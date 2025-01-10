use std::{
    future::ready,
    ops::Range,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use crate::{
    config::{CredentialsFetcher, S3Credentials, S3Options},
    format::{ChunkId, ChunkOffset, FileTypeTag, ManifestId, ObjectId, SnapshotId},
    private, Storage, StorageError,
};
use async_stream::try_stream;
use async_trait::async_trait;
use aws_config::{
    meta::region::RegionProviderChain, retry::ProvideErrorKind, AppName, BehaviorVersion,
};
use aws_credential_types::provider::error::CredentialsError;
use aws_sdk_s3::{
    config::{Builder, ProvideCredentials, Region},
    error::SdkError,
    operation::put_object::PutObjectError,
    primitives::ByteStream,
    types::{Delete, Object, ObjectIdentifier},
    Client,
};
use aws_smithy_types_convert::{date_time::DateTimeExt, stream::PaginationStreamExt};
use bytes::Bytes;
use futures::{
    stream::{self, BoxStream, FuturesOrdered},
    StreamExt, TryStreamExt,
};
use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncRead, AsyncReadExt},
    sync::OnceCell,
};

use super::{
    split_in_multiple_requests, ETag, ListInfo, Settings, StorageResult, CHUNK_PREFIX,
    CONFIG_PATH, MANIFEST_PREFIX, REF_PREFIX, SNAPSHOT_PREFIX, TRANSACTION_PREFIX,
};

#[derive(Debug, Serialize, Deserialize)]
pub struct S3Storage {
    // config and credentials are stored so we are able to serialize and deserialize the struct
    config: S3Options,
    credentials: S3Credentials,
    bucket: String,
    prefix: String,

    #[serde(skip)]
    /// We need to use OnceCell to allow async initialization, because serde
    /// does not support async cfunction calls from deserialization. This gives
    /// us a way to lazily initialize the client.
    client: OnceCell<Arc<Client>>,
}

pub async fn mk_client(config: &S3Options, credentials: S3Credentials) -> Client {
    let region = config
        .region
        .as_ref()
        .map(|r| RegionProviderChain::first_try(Some(Region::new(r.clone()))))
        .unwrap_or_else(RegionProviderChain::default_provider);

    let endpoint = config.endpoint_url.clone();
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

    let mut s3_builder = Builder::from(&aws_config.load().await);

    if config.allow_http {
        s3_builder = s3_builder.force_path_style(true);
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
    ) -> Result<S3Storage, StorageError> {
        let client = OnceCell::new();
        Ok(S3Storage {
            client,
            config,
            bucket,
            prefix: prefix.unwrap_or_default(),
            credentials,
        })
    }

    /// Get the client, initializing it if it hasn't been initialized yet. This is necessary because the
    /// client is not serializeable and must be initialized after deserialization. Under normal construction
    /// the original client is returned immediately.
    async fn get_client(&self) -> &Arc<Client> {
        self.client
            .get_or_init(|| async {
                Arc::new(mk_client(&self.config, self.credentials.clone()).await)
            })
            .await
    }

    fn get_path_str(&self, file_prefix: &str, id: &str) -> StorageResult<String> {
        let path = PathBuf::from_iter([self.prefix.as_str(), file_prefix, id]);
        path.into_os_string().into_string().map_err(StorageError::BadPrefix)
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
        path.into_os_string().into_string().map_err(StorageError::BadPrefix)
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

    async fn get_object_concurrently(
        &self,
        settings: &Settings,
        key: &str,
        range: &Range<u64>,
    ) -> StorageResult<Box<dyn AsyncRead + Unpin + Send>> {
        let client = self.get_client().await;
        let mut results = split_in_multiple_requests(
            range,
            settings.concurrency.ideal_concurrent_request_size.get(),
            settings.concurrency.max_concurrent_requests_for_object.get(),
        )
        .map(|range| async move {
            let key = key.to_string();
            let client = Arc::clone(client);
            let bucket = self.bucket.clone();
            get_object_range(client.as_ref(), bucket, &key, &range).await
        })
        .collect::<FuturesOrdered<_>>();

        let mut res: Box<dyn AsyncRead + Unpin + Send> = Box::new(tokio::io::empty());
        while let Some(read) = results.try_next().await? {
            res = Box::new(res.chain(read));
        }

        Ok(res)
    }

    async fn put_object<
        I: IntoIterator<Item = (impl Into<String>, impl Into<String>)>,
    >(
        &self,
        key: &str,
        content_type: Option<impl Into<String>>,
        metadata: I,
        bytes: impl Into<ByteStream>,
    ) -> StorageResult<()> {
        let mut b =
            self.get_client().await.put_object().bucket(self.bucket.clone()).key(key);

        if let Some(ct) = content_type {
            b = b.content_type(ct)
        };

        for (k, v) in metadata {
            b = b.metadata(k, v);
        }

        b.body(bytes.into()).send().await?;
        Ok(())
    }

    async fn delete_batch(
        &self,
        prefix: &str,
        batch: Vec<String>,
    ) -> StorageResult<usize> {
        let keys = batch
            .iter()
            // FIXME: flag errors instead of skipping them
            .filter_map(|id| {
                let key = self.get_path_str(prefix, id).ok()?;
                let ident = ObjectIdentifier::builder().key(key).build().ok()?;
                Some(ident)
            })
            .collect();

        let delete = Delete::builder()
            .set_objects(Some(keys))
            .build()
            .map_err(|e| StorageError::Other(e.to_string()))?;

        let res = self
            .get_client()
            .await
            .delete_objects()
            .bucket(self.bucket.clone())
            .delete(delete)
            .send()
            .await?;

        Ok(res.deleted().len())
    }
}

pub fn range_to_header(range: &Range<ChunkOffset>) -> String {
    format!("bytes={}-{}", range.start, range.end - 1)
}

impl private::Sealed for S3Storage {}

#[async_trait]
#[typetag::serde]
impl Storage for S3Storage {
    async fn fetch_config(
        &self,
        _settings: &Settings,
    ) -> StorageResult<Option<(Bytes, ETag)>> {
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
                Some(etag) => Ok(Some((output.body.collect().await?.into_bytes(), etag))),
                None => Err(StorageError::Other("No ETag found for config".to_string())),
            },
            Err(sdk_err) => match sdk_err.as_service_error() {
                Some(e) if e.is_no_such_key() => Ok(None),
                _ => Err(sdk_err.into()),
            },
        }
    }

    async fn update_config(
        &self,
        _settings: &Settings,
        config: Bytes,
        etag: Option<&str>,
    ) -> StorageResult<ETag> {
        let key = self.get_config_path()?;
        let mut req = self
            .get_client()
            .await
            .put_object()
            .bucket(self.bucket.clone())
            .key(key)
            .content_type("application/yaml")
            .body(config.into());

        if let Some(etag) = etag {
            req = req.if_match(etag)
        } else {
            req = req.if_none_match("*")
        }

        let res = req.send().await;

        match res {
            Ok(out) => {
                let etag = out.e_tag().ok_or(StorageError::Other(
                    "Config object should have an etag".to_string(),
                ))?;
                Ok(etag.to_string())
            }
            // minio returns this
            Err(SdkError::ServiceError(err)) => {
                if err.err().meta().code() == Some("PreconditionFailed") {
                    Err(StorageError::ConfigUpdateConflict)
                } else {
                    Err(StorageError::from(SdkError::<PutObjectError>::ServiceError(err)))
                }
            }
            // S3 API documents this
            Err(SdkError::ResponseError(err)) => {
                let status = err.raw().status().as_u16();
                // see https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html#API_PutObject_RequestSyntax
                if status == 409 || status == 412 {
                    Err(StorageError::ConfigUpdateConflict)
                } else {
                    Err(StorageError::from(SdkError::<PutObjectError>::ResponseError(
                        err,
                    )))
                }
            }
            Err(err) => Err(err.into()),
        }
    }

    async fn fetch_snapshot(
        &self,
        settings: &Settings,
        id: &SnapshotId,
    ) -> StorageResult<Box<dyn AsyncRead + Unpin + Send>> {
        let key = self.get_snapshot_path(id)?;
        self.get_object_reader(settings, key.as_str()).await
    }

    async fn fetch_manifest_splitting(
        &self,
        settings: &Settings,
        id: &ManifestId,
        size: u64,
    ) -> StorageResult<Box<dyn AsyncRead + Unpin + Send>> {
        let key = self.get_manifest_path(id)?;
        self.get_object_concurrently(settings, key.as_str(), &(0..size)).await
    }

    async fn fetch_manifest_single_request(
        &self,
        settings: &Settings,
        id: &ManifestId,
    ) -> StorageResult<Box<dyn AsyncRead + Unpin + Send>> {
        let key = self.get_manifest_path(id)?;
        self.get_object_reader(settings, key.as_str()).await
    }

    async fn fetch_transaction_log(
        &self,
        settings: &Settings,
        id: &SnapshotId,
    ) -> StorageResult<Box<dyn AsyncRead + Unpin + Send>> {
        let key = self.get_transaction_path(id)?;
        self.get_object_reader(settings, key.as_str()).await
    }

    async fn fetch_chunk(
        &self,
        settings: &Settings,
        id: &ChunkId,
        range: &Range<ChunkOffset>,
    ) -> StorageResult<Bytes> {
        let key = self.get_chunk_path(id)?;
        let mut read =
            self.get_object_concurrently(settings, key.as_str(), range).await?;
        // add some extra space to the buffer to optimize conversion to bytes
        let mut buffer = Vec::with_capacity((range.end - range.start + 16) as usize);
        tokio::io::copy(&mut read, &mut buffer).await?;
        Ok(buffer.into())
    }

    async fn write_snapshot(
        &self,
        _settings: &Settings,
        id: SnapshotId,
        metadata: Vec<(String, String)>,
        bytes: Bytes,
    ) -> StorageResult<()> {
        let key = self.get_snapshot_path(&id)?;
        self.put_object(key.as_str(), None::<String>, metadata, bytes).await
    }

    async fn write_manifest(
        &self,
        _settings: &Settings,
        id: ManifestId,
        metadata: Vec<(String, String)>,
        bytes: Bytes,
    ) -> StorageResult<()> {
        let key = self.get_manifest_path(&id)?;
        self.put_object(key.as_str(), None::<String>, metadata.into_iter(), bytes).await
    }

    async fn write_transaction_log(
        &self,
        _settings: &Settings,
        id: SnapshotId,
        metadata: Vec<(String, String)>,
        bytes: Bytes,
    ) -> StorageResult<()> {
        let key = self.get_transaction_path(&id)?;
        self.put_object(key.as_str(), None::<String>, metadata.into_iter(), bytes).await
    }

    async fn write_chunk(
        &self,
        _settings: &Settings,
        id: ChunkId,
        bytes: bytes::Bytes,
    ) -> Result<(), StorageError> {
        let key = self.get_chunk_path(&id)?;
        //FIXME: use multipart upload
        let metadata: [(String, String); 0] = [];
        self.put_object(key.as_str(), None::<String>, metadata, bytes).await
    }

    async fn get_ref(&self, _settings: &Settings, ref_key: &str) -> StorageResult<Bytes> {
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
            Ok(res) => Ok(res.body.collect().await?.into_bytes()),
            Err(err)
                if err
                    .as_service_error()
                    .map(|e| e.is_no_such_key())
                    .unwrap_or(false) =>
            {
                Err(StorageError::RefNotFound(key.to_string()))
            }
            Err(err) => Err(err.into()),
        }
    }

    async fn ref_names(&self, _settings: &Settings) -> StorageResult<Vec<String>> {
        let prefix = self.ref_key("")?;
        let mut paginator = self
            .get_client()
            .await
            .list_objects_v2()
            .bucket(self.bucket.clone())
            .prefix(prefix.clone())
            .delimiter("/")
            .into_paginator()
            .send();

        let mut res = Vec::new();

        while let Some(page) = paginator.try_next().await? {
            for common_prefix in page.common_prefixes() {
                if let Some(key) = common_prefix
                    .prefix()
                    .as_ref()
                    .and_then(|key| key.strip_prefix(prefix.as_str()))
                    .and_then(|key| key.strip_suffix('/'))
                {
                    res.push(key.to_string());
                }
            }
        }

        Ok(res)
    }

    async fn ref_versions(
        &self,
        _settings: &Settings,
        ref_name: &str,
    ) -> StorageResult<BoxStream<StorageResult<String>>> {
        let prefix = self.ref_key(ref_name)?;
        let mut paginator = self
            .get_client()
            .await
            .list_objects_v2()
            .bucket(self.bucket.clone())
            .prefix(prefix.clone())
            .into_paginator()
            .send();

        let prefix = prefix + "/";
        let stream = try_stream! {
            while let Some(page) = paginator.try_next().await? {
                for object in page.contents() {
                    if let Some(key) = object.key.as_ref().and_then(|key| key.strip_prefix(prefix.as_str())) {
                        yield key.to_string()
                    }
                }
            }
        };
        Ok(stream.boxed())
    }

    async fn write_ref(
        &self,
        _settings: &Settings,
        ref_key: &str,
        overwrite_refs: bool,
        bytes: Bytes,
    ) -> StorageResult<()> {
        let key = self.ref_key(ref_key)?;
        let mut builder = self
            .get_client()
            .await
            .put_object()
            .bucket(self.bucket.clone())
            .key(key.clone());

        if !overwrite_refs {
            builder = builder.if_none_match("*")
        }

        let res = builder.body(bytes.into()).send().await;

        match res {
            Ok(_) => Ok(()),
            Err(err) => {
                let code = err.as_service_error().and_then(|e| e.code()).unwrap_or("");
                if code.contains("PreconditionFailed")
                    || code.contains("ConditionalRequestConflict")
                {
                    Err(StorageError::RefAlreadyExists(key))
                } else {
                    Err(err.into())
                }
            }
        }
    }

    async fn list_objects<'a>(
        &'a self,
        _settings: &Settings,
        prefix: &str,
    ) -> StorageResult<BoxStream<'a, StorageResult<ListInfo<String>>>> {
        let prefix = PathBuf::from_iter([self.prefix.as_str(), prefix])
            .into_os_string()
            .into_string()
            .map_err(StorageError::BadPrefix)?;
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

    async fn delete_objects(
        &self,
        _settings: &Settings,
        prefix: &str,
        ids: BoxStream<'_, String>,
    ) -> StorageResult<usize> {
        let deleted = AtomicUsize::new(0);
        ids.chunks(1_000)
            // FIXME: configurable concurrency
            .for_each_concurrent(10, |batch| {
                let deleted = &deleted;
                async move {
                    // FIXME: handle error instead of skipping
                    let new_deletes = self.delete_batch(prefix, batch).await.unwrap_or(0);
                    deleted.fetch_add(new_deletes, Ordering::Release);
                }
            })
            .await;
        Ok(deleted.into_inner())
    }
}

fn object_to_list_info(object: &Object) -> Option<ListInfo<String>> {
    let key = object.key()?;
    let last_modified = object.last_modified()?;
    let created_at = last_modified.to_chrono_utc().ok()?;
    let id = Path::new(key).file_name().and_then(|s| s.to_str())?.to_string();
    Some(ListInfo { id, created_at })
}

#[derive(Debug)]
struct ProvideRefreshableCredentials(Arc<dyn CredentialsFetcher>);

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
        )
        .unwrap();

        let serialized = serde_json::to_string(&storage).unwrap();

        assert_eq!(
            serialized,
            r#"{"config":{"region":"us-west-2","endpoint_url":"http://localhost:9000","anonymous":false,"allow_http":true},"credentials":{"type":"static","access_key_id":"access_key_id","secret_access_key":"secret_access_key","session_token":"session_token","expires_after":null},"bucket":"bucket","prefix":"prefix"}"#
        );

        let deserialized: S3Storage = serde_json::from_str(&serialized).unwrap();
        assert_eq!(storage.config, deserialized.config);
    }
}
