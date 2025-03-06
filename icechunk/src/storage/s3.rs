use std::{
    collections::HashMap,
    fmt,
    future::ready,
    ops::Range,
    path::{Path, PathBuf},
    sync::Arc,
};

use crate::{
    config::{S3Credentials, S3CredentialsFetcher, S3Options},
    format::{ChunkId, ChunkOffset, FileTypeTag, ManifestId, ObjectId, SnapshotId},
    private, Storage, StorageError,
};
use async_trait::async_trait;
use aws_config::{
    meta::region::RegionProviderChain, retry::ProvideErrorKind, AppName, BehaviorVersion,
};
use aws_credential_types::provider::error::CredentialsError;
use aws_sdk_s3::{
    config::{
        interceptors::BeforeTransmitInterceptorContextMut, Builder, ConfigBag, Intercept,
        ProvideCredentials, Region, RuntimeComponents,
    },
    error::{BoxError, SdkError},
    operation::put_object::PutObjectError,
    primitives::ByteStream,
    types::{Delete, Object, ObjectIdentifier},
    Client,
};
use aws_smithy_types_convert::{date_time::DateTimeExt, stream::PaginationStreamExt};
use bytes::{Buf, Bytes};
use chrono::{DateTime, Utc};
use futures::{
    stream::{self, BoxStream},
    StreamExt, TryStreamExt,
};
use serde::{Deserialize, Serialize};
use tokio::{io::AsyncRead, sync::OnceCell};
use tracing::instrument;

use super::{
    DeleteObjectsResult, FetchConfigResult, GetRefResult, ListInfo, Reader, Settings,
    StorageErrorKind, StorageResult, UpdateConfigResult, VersionInfo, WriteRefResult,
    CHUNK_PREFIX, CONFIG_PATH, MANIFEST_PREFIX, REF_PREFIX, SNAPSHOT_PREFIX,
    TRANSACTION_PREFIX,
};

#[derive(Debug, Serialize, Deserialize)]
pub struct S3Storage {
    // config and credentials are stored so we are able to serialize and deserialize the struct
    config: S3Options,
    credentials: S3Credentials,
    bucket: String,
    prefix: String,
    can_write: bool,
    extra_read_headers: Vec<(String, String)>,
    extra_write_headers: Vec<(String, String)>,
    #[serde(skip)]
    /// We need to use OnceCell to allow async initialization, because serde
    /// does not support async cfunction calls from deserialization. This gives
    /// us a way to lazily initialize the client.
    client: OnceCell<Arc<Client>>,
}

impl fmt::Display for S3Storage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "S3Storage(bucket={}, prefix={}, config={})",
            self.bucket, self.prefix, self.config,
        )
    }
}
#[derive(Debug)]
struct ExtraHeadersInterceptor {
    extra_read_headers: Vec<(String, String)>,
    extra_write_headers: Vec<(String, String)>,
}

impl Intercept for ExtraHeadersInterceptor {
    fn name(&self) -> &'static str {
        "ExtraHeaders"
    }

    fn modify_before_retry_loop(
        &self,
        context: &mut BeforeTransmitInterceptorContextMut<'_>,
        _runtime_components: &RuntimeComponents,
        _cfg: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        let request = context.request_mut();
        match request.method() {
            "GET" | "HEAD" | "OPTIONS" | "TRACE" => {
                for (k, v) in self.extra_read_headers.iter() {
                    request.headers_mut().insert(k.clone(), v.clone());
                }
            }
            _ => {
                for (k, v) in self.extra_write_headers.iter() {
                    request.headers_mut().insert(k.clone(), v.clone());
                }
            }
        }
        Ok(())
    }
}

#[instrument(skip(credentials))]
pub async fn mk_client(
    config: &S3Options,
    credentials: S3Credentials,
    extra_read_headers: Vec<(String, String)>,
    extra_write_headers: Vec<(String, String)>,
) -> Client {
    let region = config
        .region
        .as_ref()
        .map(|r| RegionProviderChain::first_try(Some(Region::new(r.clone()))))
        .unwrap_or_else(RegionProviderChain::default_provider);

    let endpoint = config.endpoint_url.clone();
    let region = if endpoint.is_some() {
        // GH793, the S3 SDK requires a region even though it may not make sense
        // for S3-compatible object stores like Tigris or Ceph.
        // So we set a fake region, using the `endpoint_url` as a sign that
        // we are not talking to real S3
        region.or_else(Region::new("region-was-not-set"))
    } else {
        region
    };

    #[allow(clippy::unwrap_used)]
    let app_name = AppName::new("icechunk").unwrap();
    let mut aws_config = aws_config::defaults(BehaviorVersion::v2024_03_28())
        .region(region)
        .app_name(app_name);

    if let Some(endpoint) = endpoint {
        aws_config = aws_config.endpoint_url(endpoint)
    }

    match credentials {
        S3Credentials::FromEnv => {}
        S3Credentials::Anonymous => aws_config = aws_config.no_credentials(),
        S3Credentials::Static(credentials) => {
            aws_config =
                aws_config.credentials_provider(aws_credential_types::Credentials::new(
                    credentials.access_key_id,
                    credentials.secret_access_key,
                    credentials.session_token,
                    credentials.expires_after.map(|e| e.into()),
                    "user",
                ));
        }
        S3Credentials::Refreshable(fetcher) => {
            aws_config =
                aws_config.credentials_provider(ProvideRefreshableCredentials(fetcher));
        }
    }

    let mut s3_builder =
        Builder::from(&aws_config.load().await).force_path_style(config.force_path_style);

    if !extra_read_headers.is_empty() || !extra_write_headers.is_empty() {
        s3_builder = s3_builder.interceptor(ExtraHeadersInterceptor {
            extra_read_headers,
            extra_write_headers,
        })
    }

    let config = s3_builder.build();

    Client::from_conf(config)
}

impl S3Storage {
    pub fn new(
        config: S3Options,
        bucket: String,
        prefix: Option<String>,
        credentials: S3Credentials,
        can_write: bool,
        extra_read_headers: Vec<(String, String)>,
        extra_write_headers: Vec<(String, String)>,
    ) -> Result<S3Storage, StorageError> {
        let client = OnceCell::new();
        Ok(S3Storage {
            client,
            config,
            bucket,
            prefix: prefix.unwrap_or_default(),
            credentials,
            can_write,
            extra_read_headers,
            extra_write_headers,
        })
    }

    /// Get the client, initializing it if it hasn't been initialized yet. This is necessary because the
    /// client is not serializeable and must be initialized after deserialization. Under normal construction
    /// the original client is returned immediately.
    #[instrument(skip(self))]
    async fn get_client(&self) -> &Arc<Client> {
        self.client
            .get_or_init(|| async {
                Arc::new(
                    mk_client(
                        &self.config,
                        self.credentials.clone(),
                        self.extra_read_headers.clone(),
                        self.extra_write_headers.clone(),
                    )
                    .await,
                )
            })
            .await
    }

    fn get_path_str(&self, file_prefix: &str, id: &str) -> StorageResult<String> {
        let path = PathBuf::from_iter([self.prefix.as_str(), file_prefix, id]);
        let path_str =
            path.into_os_string().into_string().map_err(StorageErrorKind::BadPrefix)?;

        Ok(path_str.replace("\\", "/"))
    }

    fn get_path<const SIZE: usize, T: FileTypeTag>(
        &self,
        file_prefix: &str,
        id: &ObjectId<SIZE, T>,
    ) -> StorageResult<String> {
        // we serialize the url using crockford
        self.get_path_str(file_prefix, id.to_string().as_str())
    }

    fn get_config_path(&self) -> StorageResult<String> {
        self.get_path_str("", CONFIG_PATH)
    }

    fn get_snapshot_path(&self, id: &SnapshotId) -> StorageResult<String> {
        self.get_path(SNAPSHOT_PREFIX, id)
    }

    fn get_manifest_path(&self, id: &ManifestId) -> StorageResult<String> {
        self.get_path(MANIFEST_PREFIX, id)
    }

    fn get_chunk_path(&self, id: &ChunkId) -> StorageResult<String> {
        self.get_path(CHUNK_PREFIX, id)
    }

    fn get_transaction_path(&self, id: &SnapshotId) -> StorageResult<String> {
        self.get_path(TRANSACTION_PREFIX, id)
    }

    fn ref_key(&self, ref_key: &str) -> StorageResult<String> {
        let path = PathBuf::from_iter([self.prefix.as_str(), REF_PREFIX, ref_key]);
        let path_str =
            path.into_os_string().into_string().map_err(StorageErrorKind::BadPrefix)?;

        Ok(path_str.replace("\\", "/"))
    }

    async fn get_object_reader(
        &self,
        _settings: &Settings,
        key: &str,
    ) -> StorageResult<Box<dyn AsyncRead + Unpin + Send>> {
        let client = self.get_client().await;
        let b = client.get_object().bucket(self.bucket.as_str()).key(key);
        Ok(Box::new(b.send().await?.body.into_async_read()))
    }

    async fn put_object<
        I: IntoIterator<Item = (impl Into<String>, impl Into<String>)>,
    >(
        &self,
        settings: &Settings,
        key: &str,
        content_type: Option<impl Into<String>>,
        metadata: I,
        bytes: impl Into<ByteStream>,
    ) -> StorageResult<()> {
        let mut b =
            self.get_client().await.put_object().bucket(self.bucket.clone()).key(key);

        if settings.unsafe_use_metadata() {
            if let Some(ct) = content_type {
                b = b.content_type(ct)
            };
        }

        if settings.unsafe_use_metadata() {
            for (k, v) in metadata {
                b = b.metadata(k, v);
            }
        }
        b.body(bytes.into()).send().await?;
        Ok(())
    }

    fn get_ref_name<'a>(&self, key: Option<&'a str>) -> Option<&'a str> {
        let key = key?;
        let prefix = self.ref_key("").ok()?;
        let relative_key = key.strip_prefix(&prefix)?;
        let ref_name = relative_key.split('/').next()?;
        Some(ref_name)
    }
}

pub fn range_to_header(range: &Range<ChunkOffset>) -> String {
    format!("bytes={}-{}", range.start, range.end - 1)
}

impl private::Sealed for S3Storage {}

#[async_trait]
#[typetag::serde]
impl Storage for S3Storage {
    fn can_write(&self) -> bool {
        self.can_write
    }

    #[instrument(skip(self, _settings))]
    async fn fetch_config(
        &self,
        _settings: &Settings,
    ) -> StorageResult<FetchConfigResult> {
        let key = self.get_config_path()?;
        let res = self
            .get_client()
            .await
            .get_object()
            .bucket(self.bucket.clone())
            .key(key)
            .send()
            .await;

        match res {
            Ok(output) => match output.e_tag {
                Some(etag) => Ok(FetchConfigResult::Found {
                    bytes: output.body.collect().await?.into_bytes(),
                    version: VersionInfo::from_etag_only(etag),
                }),
                None => Ok(FetchConfigResult::NotFound),
            },
            Err(sdk_err) => match sdk_err.as_service_error() {
                Some(e) if e.is_no_such_key() => Ok(FetchConfigResult::NotFound),
                _ => Err(sdk_err.into()),
            },
        }
    }

    #[instrument(skip(self, settings, config))]
    async fn update_config(
        &self,
        settings: &Settings,
        config: Bytes,
        previous_version: &VersionInfo,
    ) -> StorageResult<UpdateConfigResult> {
        let key = self.get_config_path()?;
        let mut req = self
            .get_client()
            .await
            .put_object()
            .bucket(self.bucket.clone())
            .key(key)
            .body(config.into());

        if settings.unsafe_use_metadata() {
            req = req.content_type("application/yaml")
        }

        match (
            previous_version.etag(),
            settings.unsafe_use_conditional_create(),
            settings.unsafe_use_conditional_update(),
        ) {
            (None, true, _) => req = req.if_none_match("*"),
            (Some(etag), _, true) => req = req.if_match(etag),
            (_, _, _) => {}
        }

        let res = req.send().await;

        match res {
            Ok(out) => {
                let new_etag = out
                    .e_tag()
                    .ok_or(StorageErrorKind::Other(
                        "Config object should have an etag".to_string(),
                    ))?
                    .to_string();
                let new_version = VersionInfo::from_etag_only(new_etag);
                Ok(UpdateConfigResult::Updated { new_version })
            }
            // minio returns this
            Err(SdkError::ServiceError(err)) => {
                if err.err().meta().code() == Some("PreconditionFailed") {
                    Ok(UpdateConfigResult::NotOnLatestVersion)
                } else {
                    Err(StorageError::from(SdkError::<PutObjectError>::ServiceError(err)))
                }
            }
            // S3 API documents this
            Err(SdkError::ResponseError(err)) => {
                let status = err.raw().status().as_u16();
                // see https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html#API_PutObject_RequestSyntax
                if status == 409 || status == 412 {
                    Ok(UpdateConfigResult::NotOnLatestVersion)
                } else {
                    Err(StorageError::from(SdkError::<PutObjectError>::ResponseError(
                        err,
                    )))
                }
            }
            Err(err) => Err(err.into()),
        }
    }

    #[instrument(skip(self, settings))]
    async fn fetch_snapshot(
        &self,
        settings: &Settings,
        id: &SnapshotId,
    ) -> StorageResult<Box<dyn AsyncRead + Unpin + Send>> {
        let key = self.get_snapshot_path(id)?;
        self.get_object_reader(settings, key.as_str()).await
    }

    #[instrument(skip(self, settings))]
    async fn fetch_manifest_known_size(
        &self,
        settings: &Settings,
        id: &ManifestId,
        size: u64,
    ) -> StorageResult<Reader> {
        let key = self.get_manifest_path(id)?;
        self.get_object_concurrently(settings, key.as_str(), &(0..size)).await
    }

    #[instrument(skip(self, settings))]
    async fn fetch_manifest_unknown_size(
        &self,
        settings: &Settings,
        id: &ManifestId,
    ) -> StorageResult<Box<dyn AsyncRead + Unpin + Send>> {
        let key = self.get_manifest_path(id)?;
        self.get_object_reader(settings, key.as_str()).await
    }

    #[instrument(skip(self, settings))]
    async fn fetch_transaction_log(
        &self,
        settings: &Settings,
        id: &SnapshotId,
    ) -> StorageResult<Box<dyn AsyncRead + Unpin + Send>> {
        let key = self.get_transaction_path(id)?;
        self.get_object_reader(settings, key.as_str()).await
    }

    #[instrument(skip(self, settings))]
    async fn fetch_chunk(
        &self,
        settings: &Settings,
        id: &ChunkId,
        range: &Range<ChunkOffset>,
    ) -> StorageResult<Bytes> {
        let key = self.get_chunk_path(id)?;
        self.get_object_concurrently(settings, key.as_str(), range)
            .await?
            .to_bytes((range.end - range.start) as usize)
            .await
    }

    #[instrument(skip(self, settings, metadata, bytes))]
    async fn write_snapshot(
        &self,
        settings: &Settings,
        id: SnapshotId,
        metadata: Vec<(String, String)>,
        bytes: Bytes,
    ) -> StorageResult<()> {
        let key = self.get_snapshot_path(&id)?;
        self.put_object(settings, key.as_str(), None::<String>, metadata, bytes).await
    }

    #[instrument(skip(self, settings, metadata, bytes))]
    async fn write_manifest(
        &self,
        settings: &Settings,
        id: ManifestId,
        metadata: Vec<(String, String)>,
        bytes: Bytes,
    ) -> StorageResult<()> {
        let key = self.get_manifest_path(&id)?;
        self.put_object(
            settings,
            key.as_str(),
            None::<String>,
            metadata.into_iter(),
            bytes,
        )
        .await
    }

    #[instrument(skip(self, settings, metadata, bytes))]
    async fn write_transaction_log(
        &self,
        settings: &Settings,
        id: SnapshotId,
        metadata: Vec<(String, String)>,
        bytes: Bytes,
    ) -> StorageResult<()> {
        let key = self.get_transaction_path(&id)?;
        self.put_object(
            settings,
            key.as_str(),
            None::<String>,
            metadata.into_iter(),
            bytes,
        )
        .await
    }

    #[instrument(skip(self, settings, bytes))]
    async fn write_chunk(
        &self,
        settings: &Settings,
        id: ChunkId,
        bytes: bytes::Bytes,
    ) -> Result<(), StorageError> {
        let key = self.get_chunk_path(&id)?;
        //FIXME: use multipart upload
        let metadata: [(String, String); 0] = [];
        self.put_object(settings, key.as_str(), None::<String>, metadata, bytes).await
    }

    #[instrument(skip(self, _settings))]
    async fn get_ref(
        &self,
        _settings: &Settings,
        ref_key: &str,
    ) -> StorageResult<GetRefResult> {
        let key = self.ref_key(ref_key)?;
        let res = self
            .get_client()
            .await
            .get_object()
            .bucket(self.bucket.clone())
            .key(key.clone())
            .send()
            .await;

        match res {
            Ok(res) => {
                let bytes = res.body.collect().await?.into_bytes();
                if let Some(version) = res.e_tag.map(VersionInfo::from_etag_only) {
                    Ok(GetRefResult::Found { bytes, version })
                } else {
                    Ok(GetRefResult::NotFound)
                }
            }
            Err(err)
                if err
                    .as_service_error()
                    .map(|e| e.is_no_such_key())
                    .unwrap_or(false) =>
            {
                Ok(GetRefResult::NotFound)
            }
            Err(err) => Err(err.into()),
        }
    }

    #[instrument(skip(self, _settings))]
    async fn ref_names(&self, _settings: &Settings) -> StorageResult<Vec<String>> {
        let prefix = self.ref_key("")?;
        let mut paginator = self
            .get_client()
            .await
            .list_objects_v2()
            .bucket(self.bucket.clone())
            .prefix(prefix.clone())
            .into_paginator()
            .send();

        let mut res = Vec::new();

        while let Some(page) = paginator.try_next().await? {
            for obj in page.contents.unwrap_or_else(Vec::new) {
                let name = self.get_ref_name(obj.key());
                if let Some(name) = name {
                    res.push(name.to_string());
                }
            }
        }

        Ok(res)
    }

    #[instrument(skip(self, settings, bytes))]
    async fn write_ref(
        &self,
        settings: &Settings,
        ref_key: &str,
        bytes: Bytes,
        previous_version: &VersionInfo,
    ) -> StorageResult<WriteRefResult> {
        let key = self.ref_key(ref_key)?;
        let mut builder = self
            .get_client()
            .await
            .put_object()
            .bucket(self.bucket.clone())
            .key(key.clone());

        match (
            previous_version.etag(),
            settings.unsafe_use_conditional_create(),
            settings.unsafe_use_conditional_update(),
        ) {
            (None, true, _) => {
                builder = builder.if_none_match("*");
            }
            (Some(etag), _, true) => {
                builder = builder.if_match(etag);
            }
            (_, _, _) => {}
        }

        let res = builder.body(bytes.into()).send().await;

        match res {
            Ok(_) => Ok(WriteRefResult::Written),
            Err(err) => {
                let code = err.as_service_error().and_then(|e| e.code()).unwrap_or("");
                if code.contains("PreconditionFailed")
                    || code.contains("ConditionalRequestConflict")
                {
                    Ok(WriteRefResult::WontOverwrite)
                } else {
                    Err(err.into())
                }
            }
        }
    }

    #[instrument(skip(self, _settings))]
    async fn list_objects<'a>(
        &'a self,
        _settings: &Settings,
        prefix: &str,
    ) -> StorageResult<BoxStream<'a, StorageResult<ListInfo<String>>>> {
        let prefix = format!("{}/{}", self.prefix, prefix).replace("//", "/");
        let stream = self
            .get_client()
            .await
            .list_objects_v2()
            .bucket(self.bucket.clone())
            .prefix(prefix)
            .into_paginator()
            .send()
            .into_stream_03x()
            .try_filter_map(|page| {
                let contents = page.contents.map(|cont| stream::iter(cont).map(Ok));
                ready(Ok(contents))
            })
            .try_flatten()
            // TODO: we should signal error instead of filtering
            .try_filter_map(|object| ready(Ok(object_to_list_info(&object))));
        Ok(stream.boxed())
    }

    #[instrument(skip(self, batch))]
    async fn delete_batch(
        &self,
        prefix: &str,
        batch: Vec<(String, u64)>,
    ) -> StorageResult<DeleteObjectsResult> {
        let mut sizes = HashMap::new();
        let mut ids = Vec::new();
        for (id, size) in batch.into_iter() {
            if let Ok(key) = self.get_path_str(prefix, id.as_str()) {
                if let Ok(ident) = ObjectIdentifier::builder().key(key.clone()).build() {
                    ids.push(ident);
                    sizes.insert(key, size);
                }
            }
        }

        let delete = Delete::builder()
            .set_objects(Some(ids))
            .build()
            .map_err(|e| StorageErrorKind::Other(e.to_string()))?;

        let res = self
            .get_client()
            .await
            .delete_objects()
            .bucket(self.bucket.clone())
            .delete(delete)
            .send()
            .await?;

        let mut result = DeleteObjectsResult::default();
        for deleted in res.deleted() {
            if let Some(key) = deleted.key() {
                let size = sizes.get(key).unwrap_or(&0);
                result.deleted_bytes += *size;
                result.deleted_objects += 1;
            }
        }
        Ok(result)
    }

    #[instrument(skip(self, _settings))]
    async fn get_snapshot_last_modified(
        &self,
        _settings: &Settings,
        snapshot: &SnapshotId,
    ) -> StorageResult<DateTime<Utc>> {
        let key = self.get_snapshot_path(snapshot)?;
        let res = self
            .get_client()
            .await
            .head_object()
            .bucket(self.bucket.as_str())
            .key(key)
            .send()
            .await?;

        let res = res.last_modified.ok_or(StorageErrorKind::Other(
            "Object has no last_modified field".to_string(),
        ))?;
        let res = res.to_chrono_utc().map_err(|_| {
            StorageErrorKind::Other("Invalid metadata timestamp".to_string())
        })?;

        Ok(res)
    }

    #[instrument(skip(self))]
    async fn get_object_range_buf(
        &self,
        key: &str,
        range: &Range<u64>,
    ) -> StorageResult<Box<dyn Buf + Unpin + Send>> {
        let b = self
            .get_client()
            .await
            .get_object()
            .bucket(self.bucket.as_str())
            .key(key)
            .range(range_to_header(range));
        Ok(Box::new(b.send().await?.body.collect().await?))
    }

    #[instrument(skip(self))]
    async fn get_object_range_read(
        &self,
        key: &str,
        range: &Range<u64>,
    ) -> StorageResult<Box<dyn AsyncRead + Unpin + Send>> {
        let client = self.get_client().await;
        let bucket = self.bucket.clone();
        Ok(Box::new(get_object_range(client.as_ref(), bucket, key, range).await?))
    }
}

fn object_to_list_info(object: &Object) -> Option<ListInfo<String>> {
    let key = object.key()?;
    let last_modified = object.last_modified()?;
    let created_at = last_modified.to_chrono_utc().ok()?;
    let id = Path::new(key).file_name().and_then(|s| s.to_str())?.to_string();
    let size_bytes = object.size.unwrap_or(0) as u64;
    Some(ListInfo { id, created_at, size_bytes })
}

#[derive(Debug)]
struct ProvideRefreshableCredentials(Arc<dyn S3CredentialsFetcher>);

impl ProvideCredentials for ProvideRefreshableCredentials {
    fn provide_credentials<'a>(
        &'a self,
    ) -> aws_credential_types::provider::future::ProvideCredentials<'a>
    where
        Self: 'a,
    {
        aws_credential_types::provider::future::ProvideCredentials::new(self.provide())
    }
}

impl ProvideRefreshableCredentials {
    async fn provide(
        &self,
    ) -> Result<aws_credential_types::Credentials, CredentialsError> {
        let creds = self.0.get().await.map_err(CredentialsError::not_loaded)?;
        let creds = aws_credential_types::Credentials::new(
            creds.access_key_id,
            creds.secret_access_key,
            creds.session_token,
            creds.expires_after.map(|e| e.into()),
            "user",
        );
        Ok(creds)
    }
}

async fn get_object_range(
    client: &Client,
    bucket: String,
    key: &str,
    range: &Range<ChunkOffset>,
) -> StorageResult<impl AsyncRead> {
    let b = client.get_object().bucket(bucket).key(key).range(range_to_header(range));
    Ok(b.send().await?.body.into_async_read())
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use crate::config::{S3Credentials, S3Options, S3StaticCredentials};

    use super::*;

    #[tokio::test]
    async fn test_serialize_s3_storage() {
        let config = S3Options {
            region: Some("us-west-2".to_string()),
            endpoint_url: Some("http://localhost:9000".to_string()),
            allow_http: true,
            anonymous: false,
            force_path_style: false,
        };
        let credentials = S3Credentials::Static(S3StaticCredentials {
            access_key_id: "access_key_id".to_string(),
            secret_access_key: "secret_access_key".to_string(),
            session_token: Some("session_token".to_string()),
            expires_after: None,
        });
        let storage = S3Storage::new(
            config,
            "bucket".to_string(),
            Some("prefix".to_string()),
            credentials,
            true,
            Vec::new(),
            Vec::new(),
        )
        .unwrap();

        let serialized = serde_json::to_string(&storage).unwrap();

        assert_eq!(
            serialized,
            r#"{"config":{"region":"us-west-2","endpoint_url":"http://localhost:9000","anonymous":false,"allow_http":true,"force_path_style":false},"credentials":{"s3_credential_type":"static","access_key_id":"access_key_id","secret_access_key":"secret_access_key","session_token":"session_token","expires_after":null},"bucket":"bucket","prefix":"prefix","can_write":true,"extra_read_headers":[],"extra_write_headers":[]}"#
        );

        let deserialized: S3Storage = serde_json::from_str(&serialized).unwrap();
        assert_eq!(storage.config, deserialized.config);
    }

    #[tokio::test]
    async fn test_s3_paths() {
        let storage = S3Storage::new(
            S3Options {
                region: Some("us-west-2".to_string()),
                endpoint_url: None,
                allow_http: true,
                anonymous: false,
                force_path_style: false,
            },
            "bucket".to_string(),
            Some("prefix".to_string()),
            S3Credentials::FromEnv,
            true,
            Vec::new(),
            Vec::new(),
        )
        .unwrap();

        let ref_path = storage.ref_key("ref_key").unwrap();
        assert_eq!(ref_path, "prefix/refs/ref_key");

        let snapshot_id = SnapshotId::random();
        let snapshot_path = storage.get_snapshot_path(&snapshot_id).unwrap();
        assert_eq!(snapshot_path, format!("prefix/snapshots/{snapshot_id}"));

        let manifest_id = ManifestId::random();
        let manifest_path = storage.get_manifest_path(&manifest_id).unwrap();
        assert_eq!(manifest_path, format!("prefix/manifests/{manifest_id}"));

        let chunk_id = ChunkId::random();
        let chunk_path = storage.get_chunk_path(&chunk_id).unwrap();
        assert_eq!(chunk_path, format!("prefix/chunks/{chunk_id}"));

        let transaction_id = SnapshotId::random();
        let transaction_path = storage.get_transaction_path(&transaction_id).unwrap();
        assert_eq!(transaction_path, format!("prefix/transactions/{transaction_id}"));
    }
}
