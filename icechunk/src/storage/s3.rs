use std::{
    collections::HashMap,
    fmt,
    future::ready,
    ops::Range,
    path::{Path, PathBuf},
    sync::Arc,
};

use crate::{
    Storage, StorageError,
    config::{S3Credentials, S3CredentialsFetcher, S3Options},
    format::ChunkOffset,
    private,
};
use async_trait::async_trait;
use aws_config::{
    AppName, BehaviorVersion, meta::region::RegionProviderChain, retry::RetryConfig,
};
use aws_credential_types::provider::error::CredentialsError;
use aws_sdk_s3::{
    Client,
    config::{
        Builder, ConfigBag, IdentityCache, Intercept, ProvideCredentials, Region,
        RuntimeComponents, StalledStreamProtectionConfig,
        interceptors::BeforeTransmitInterceptorContextMut,
    },
    error::{BoxError, SdkError},
    operation::put_object::PutObjectError,
    primitives::ByteStream,
    types::{CompletedMultipartUpload, CompletedPart, Delete, Object, ObjectIdentifier},
};
use aws_smithy_types_convert::{date_time::DateTimeExt, stream::PaginationStreamExt};
use bytes::{Buf, Bytes};
use chrono::{DateTime, Utc};
use futures::{
    StreamExt, TryStreamExt,
    stream::{self, BoxStream, FuturesOrdered},
};
use serde::{Deserialize, Serialize};
use tokio::{io::AsyncRead, sync::OnceCell};
use tracing::{error, instrument};

use super::{
    DeleteObjectsResult, ListInfo, REF_PREFIX, Settings, StorageErrorKind, StorageResult,
    VersionInfo, VersionedFetchResult, VersionedUpdateResult,
    split_in_multiple_equal_requests,
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
    settings: &Settings,
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
    let mut aws_config = aws_config::defaults(BehaviorVersion::v2025_01_17())
        .region(region)
        .app_name(app_name);

    if let Some(endpoint) = endpoint {
        aws_config = aws_config.endpoint_url(endpoint)
    }

    let stalled_stream = if config.network_stream_timeout_seconds == Some(0) {
        StalledStreamProtectionConfig::disabled()
    } else {
        StalledStreamProtectionConfig::enabled()
            .grace_period(std::time::Duration::from_secs(
                config.network_stream_timeout_seconds.unwrap_or(60) as u64,
            ))
            .build()
    };
    aws_config = aws_config.stalled_stream_protection(stalled_stream);

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

    let retry_config = RetryConfig::standard()
        .with_max_attempts(settings.retries().max_tries().get() as u32)
        .with_initial_backoff(core::time::Duration::from_millis(
            settings.retries().initial_backoff_ms() as u64,
        ))
        .with_max_backoff(core::time::Duration::from_millis(
            settings.retries().max_backoff_ms() as u64,
        ));

    let mut s3_builder = Builder::from(&aws_config.load().await)
        .force_path_style(config.force_path_style)
        .retry_config(retry_config);

    // credentials may take a while to refresh, defaults are too strict
    let id_cache = IdentityCache::lazy()
        .load_timeout(core::time::Duration::from_secs(120))
        .buffer_time(core::time::Duration::from_secs(120))
        .build();

    s3_builder = s3_builder.identity_cache(id_cache);

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
    #[instrument(skip_all)]
    async fn get_client(&self, settings: &Settings) -> &Arc<Client> {
        self.client
            .get_or_init(|| async {
                Arc::new(
                    mk_client(
                        &self.config,
                        self.credentials.clone(),
                        self.extra_read_headers.clone(),
                        self.extra_write_headers.clone(),
                        settings,
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

    fn prefixed_path(&self, path: &str) -> String {
        format!("{}/{path}", self.prefix)
    }

    fn ref_key(&self, ref_key: &str) -> StorageResult<String> {
        let path = PathBuf::from_iter([self.prefix.as_str(), REF_PREFIX, ref_key]);
        let path_str =
            path.into_os_string().into_string().map_err(StorageErrorKind::BadPrefix)?;

        Ok(path_str.replace("\\", "/"))
    }

    async fn put_object_single<
        I: IntoIterator<Item = (impl Into<String>, impl Into<String>)>,
    >(
        &self,
        settings: &Settings,
        key: &str,
        content_type: Option<impl Into<String>>,
        metadata: I,
        storage_class: Option<&String>,
        bytes: impl Into<ByteStream>,
    ) -> StorageResult<()> {
        let mut b = self
            .get_client(settings)
            .await
            .put_object()
            .bucket(self.bucket.clone())
            .key(key);

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

        if let Some(klass) = storage_class {
            let klass = klass.as_str().into();
            b = b.storage_class(klass);
        }

        b.body(bytes.into()).send().await.map_err(Box::new)?;
        Ok(())
    }

    async fn put_object_multipart<
        I: IntoIterator<Item = (impl Into<String>, impl Into<String>)>,
    >(
        &self,
        settings: &Settings,
        key: &str,
        content_type: Option<impl Into<String>>,
        metadata: I,
        storage_class: Option<&String>,
        bytes: &Bytes,
    ) -> StorageResult<()> {
        let mut multi = self
            .get_client(settings)
            .await
            .create_multipart_upload()
            // We would like this, but it fails in MinIO
            //.checksum_type(aws_sdk_s3::types::ChecksumType::FullObject)
            //.checksum_algorithm(aws_sdk_s3::types::ChecksumAlgorithm::Crc64Nvme)
            .bucket(self.bucket.clone())
            .key(key);

        if settings.unsafe_use_metadata() {
            if let Some(ct) = content_type {
                multi = multi.content_type(ct)
            };
            for (k, v) in metadata {
                multi = multi.metadata(k, v);
            }
        }

        if let Some(klass) = storage_class {
            let klass = klass.as_str().into();
            multi = multi.storage_class(klass);
        }

        let create_res = multi.send().await.map_err(Box::new)?;
        let upload_id =
            create_res.upload_id().ok_or(StorageError::from(StorageErrorKind::Other(
                "No upload_id in create multipart upload result".to_string(),
            )))?;

        // We need to ensure all requests are the same size except for the last one, which can be
        // smaller. This is a requirement for R2 compatibility
        let parts = split_in_multiple_equal_requests(
            &(0..bytes.len() as u64),
            settings.concurrency().ideal_concurrent_request_size().get(),
            settings.concurrency().max_concurrent_requests_for_object().get(),
        )
        .collect::<Vec<_>>();

        let results = parts
            .into_iter()
            .enumerate()
            .map(|(part_idx, range)| async move {
                let body = bytes.slice(range.start as usize..range.end as usize).into();
                let idx = part_idx as i32 + 1;
                self.get_client(settings)
                    .await
                    .upload_part()
                    .upload_id(upload_id)
                    .bucket(self.bucket.clone())
                    .key(key)
                    .part_number(idx)
                    .body(body)
                    .send()
                    .await
                    .map(|res| (idx, res))
            })
            .collect::<FuturesOrdered<_>>();

        let completed_parts = results
            .map_ok(|(idx, res)| {
                let etag = res.e_tag().unwrap_or("");
                CompletedPart::builder()
                    .e_tag(strip_quotes(etag))
                    .part_number(idx)
                    .build()
            })
            .try_collect::<Vec<_>>()
            .await
            .map_err(Box::new)?;

        let completed_parts =
            CompletedMultipartUpload::builder().set_parts(Some(completed_parts)).build();

        self.get_client(settings)
            .await
            .complete_multipart_upload()
            .bucket(self.bucket.clone())
            .key(key)
            .upload_id(upload_id)
            //.checksum_type(aws_sdk_s3::types::ChecksumType::FullObject)
            .multipart_upload(completed_parts)
            .send()
            .await
            .map_err(Box::new)?;

        Ok(())
    }

    async fn put_object<
        I: IntoIterator<Item = (impl Into<String>, impl Into<String>)>,
    >(
        &self,
        settings: &Settings,
        full_key: &str,
        content_type: Option<impl Into<String>>,
        metadata: I,
        storage_class: Option<&String>,
        bytes: &Bytes,
    ) -> StorageResult<()> {
        if bytes.len() >= settings.minimum_size_for_multipart_upload() as usize {
            self.put_object_multipart(
                settings,
                full_key,
                content_type,
                metadata,
                storage_class,
                bytes,
            )
            .await
        } else {
            self.put_object_single(
                settings,
                full_key,
                content_type,
                metadata,
                storage_class,
                bytes.clone(),
            )
            .await
        }
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

    #[instrument(skip(self, settings))]
    async fn get_versioned_object(
        &self,
        path: &str,
        settings: &Settings,
    ) -> StorageResult<VersionedFetchResult<Box<dyn AsyncRead + Unpin + Send>>> {
        let key = self.prefixed_path(path);
        let res = self
            .get_client(settings)
            .await
            .get_object()
            .bucket(self.bucket.clone())
            .key(key)
            .send()
            .await;

        match res {
            Ok(output) => match output.e_tag {
                Some(etag) => Ok(VersionedFetchResult::Found {
                    result: Box::new(output.body.into_async_read()),
                    version: VersionInfo::from_etag_only(etag),
                }),
                None => Ok(VersionedFetchResult::NotFound),
            },
            Err(sdk_err) => match sdk_err.as_service_error() {
                Some(e) if e.is_no_such_key() => Ok(VersionedFetchResult::NotFound),
                Some(_)
                    if sdk_err
                        .raw_response()
                        .is_some_and(|x| x.status().as_u16() == 404) =>
                {
                    // needed for Cloudflare R2 public bucket URLs
                    // if object doesn't exist we get a 404 that isn't parsed by the AWS SDK
                    // into anything useful. So we need to parse the raw response, and match
                    // the status code.
                    Ok(VersionedFetchResult::NotFound)
                }
                _ => Err(Box::new(sdk_err).into()),
            },
        }
    }

    #[instrument(skip(self, bytes, settings))]
    async fn put_versioned_object(
        &self,
        path: &str,
        bytes: Bytes,
        content_type: Option<&str>,
        metadata: Vec<(String, String)>,
        previous_version: &VersionInfo,
        settings: &Settings,
    ) -> StorageResult<VersionedUpdateResult> {
        let key = self.prefixed_path(path);
        let mut req = self
            .get_client(settings)
            .await
            .put_object()
            .bucket(self.bucket.clone())
            .key(key)
            .body(bytes.into());

        if settings.unsafe_use_metadata() {
            if let Some(content_type) = content_type {
                req = req.content_type(content_type)
            }
            for (k, v) in metadata {
                req = req.metadata(k, v);
            }
        }

        if let Some(klass) = settings.metadata_storage_class() {
            let klass = klass.as_str().into();
            req = req.storage_class(klass);
        }

        match (
            previous_version.etag(),
            settings.unsafe_use_conditional_create(),
            settings.unsafe_use_conditional_update(),
        ) {
            (None, true, _) => req = req.if_none_match("*"),
            (Some(etag), _, true) => req = req.if_match(strip_quotes(etag)),
            (_, _, _) => {}
        }

        let res = req.send().await;

        match res {
            Ok(out) => {
                let new_etag = out
                    .e_tag()
                    .ok_or(StorageErrorKind::Other(
                        "Object should have an etag".to_string(),
                    ))?
                    .to_string();
                let new_version = VersionInfo::from_etag_only(new_etag);
                Ok(VersionedUpdateResult::Updated { new_version })
            }
            // minio returns this
            Err(SdkError::ServiceError(err)) => {
                if err.err().meta().code() == Some("PreconditionFailed") {
                    Ok(VersionedUpdateResult::NotOnLatestVersion)
                } else {
                    Err(StorageError::from(Box::new(
                        SdkError::<PutObjectError>::ServiceError(err),
                    )))
                }
            }
            // S3 API documents this
            Err(SdkError::ResponseError(err)) => {
                let status = err.raw().status().as_u16();
                // see https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html#API_PutObject_RequestSyntax
                if status == 409 || status == 412 {
                    Ok(VersionedUpdateResult::NotOnLatestVersion)
                } else {
                    Err(StorageError::from(Box::new(
                        SdkError::<PutObjectError>::ResponseError(err),
                    )))
                }
            }
            Err(err) => Err(Box::new(err).into()),
        }
    }

    async fn put_object(
        &self,
        settings: &Settings,
        path: &str,
        metadata: Vec<(String, String)>,
        bytes: Bytes,
    ) -> StorageResult<()> {
        let path = self.prefixed_path(path);
        self.put_object(
            settings,
            path.as_str(),
            None::<String>,
            metadata,
            settings.metadata_storage_class(),
            &bytes,
        )
        .await
    }

    #[instrument(skip_all)]
    async fn ref_names(&self, settings: &Settings) -> StorageResult<Vec<String>> {
        let prefix = self.ref_key("")?;
        let mut paginator = self
            .get_client(settings)
            .await
            .list_objects_v2()
            .bucket(self.bucket.clone())
            .prefix(prefix.clone())
            .into_paginator()
            .send();

        let mut res = Vec::new();

        while let Some(page) = paginator.try_next().await.map_err(Box::new)? {
            for obj in page.contents.unwrap_or_else(Vec::new) {
                let name = self.get_ref_name(obj.key());
                if let Some(name) = name {
                    res.push(name.to_string());
                } else {
                    tracing::error!(object = ?obj, "Bad ref name")
                }
            }
        }

        Ok(res)
    }

    #[instrument(skip(self, settings))]
    async fn list_objects<'a>(
        &'a self,
        settings: &Settings,
        prefix: &str,
    ) -> StorageResult<BoxStream<'a, StorageResult<ListInfo<String>>>> {
        let prefix = format!("{}/{}", self.prefix, prefix).replace("//", "/");
        let stream = self
            .get_client(settings)
            .await
            .list_objects_v2()
            .bucket(self.bucket.clone())
            .prefix(prefix)
            .into_paginator()
            .send()
            .into_stream_03x()
            .map_err(Box::new)
            .try_filter_map(|page| {
                let contents = page.contents.map(|cont| stream::iter(cont).map(Ok));
                ready(Ok(contents))
            })
            .try_flatten()
            .try_filter_map(|object| async move {
                let info = object_to_list_info(&object);
                if info.is_none() {
                    tracing::error!(object=?object, "Found bad object while listing");
                }
                Ok(info)
            });
        Ok(stream.boxed())
    }

    #[instrument(skip(self, batch))]
    async fn delete_batch(
        &self,
        settings: &Settings,
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
            .get_client(settings)
            .await
            .delete_objects()
            .bucket(self.bucket.clone())
            .delete(delete)
            .send()
            .await
            .map_err(Box::new)?;

        if let Some(err) = res.errors.as_ref().and_then(|e| e.first()) {
            tracing::error!(
                error = ?err,
                "Errors deleting objects",
            );
        }

        let mut result = DeleteObjectsResult::default();
        for deleted in res.deleted() {
            if let Some(key) = deleted.key() {
                let size = sizes.get(key).unwrap_or(&0);
                result.deleted_bytes += *size;
                result.deleted_objects += 1;
            } else {
                tracing::error!("Deleted object without key");
            }
        }
        Ok(result)
    }

    #[instrument(skip(self, settings))]
    async fn get_object_last_modified(
        &self,
        path: &str,
        settings: &Settings,
    ) -> StorageResult<DateTime<Utc>> {
        let key = self.prefixed_path(path);
        let res = self
            .get_client(settings)
            .await
            .head_object()
            .bucket(self.bucket.clone())
            .key(key)
            .send()
            .await
            .map_err(Box::new)?;

        let res = res.last_modified.ok_or(StorageErrorKind::Other(
            "Object has no last_modified field".to_string(),
        ))?;
        let res = res.to_chrono_utc().map_err(|_| {
            StorageErrorKind::Other("Invalid metadata timestamp".to_string())
        })?;

        Ok(res)
    }

    #[instrument(skip(self))]
    async fn get_object_buf(
        &self,
        settings: &Settings,
        path: &str,
        range: &Range<u64>,
    ) -> StorageResult<Box<dyn Buf + Unpin + Send>> {
        let key = self.prefixed_path(path);
        let b = self
            .get_client(settings)
            .await
            .get_object()
            .bucket(self.bucket.clone())
            .key(key)
            .range(range_to_header(range));
        Ok(Box::new(
            b.send().await.map_err(Box::new)?.body.collect().await.map_err(Box::new)?,
        ))
    }

    #[instrument(skip(self))]
    async fn get_object_read(
        &self,
        settings: &Settings,
        path: &str,
        range: Option<&Range<u64>>,
    ) -> StorageResult<Box<dyn AsyncRead + Unpin + Send>> {
        let client = self.get_client(settings).await;
        let bucket = self.bucket.clone();
        let key = self.prefixed_path(path);
        Ok(Box::new(
            get_object_range(client.as_ref(), bucket, key.as_str(), range).await?,
        ))
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
        let creds = self
            .0
            .get()
            .await
            .inspect_err(|err| error!(error = err, "Cannot load credentials"))
            .map_err(CredentialsError::not_loaded)?;
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
    full_key: &str,
    range: Option<&Range<ChunkOffset>>,
) -> StorageResult<impl AsyncRead + use<>> {
    let mut b = client.get_object().bucket(bucket).key(full_key);
    if let Some(range) = range {
        b = b.range(range_to_header(range));
    }
    Ok(b.send().await.map_err(Box::new)?.body.into_async_read())
}

fn strip_quotes(s: &str) -> &str {
    s.strip_prefix('"').and_then(|s| s.strip_suffix('"')).unwrap_or(s)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use icechunk_macros::tokio_test;

    use crate::config::{S3Credentials, S3Options, S3StaticCredentials};

    use super::*;

    #[tokio_test]
    async fn test_serialize_s3_storage() {
        let config = S3Options {
            region: Some("us-west-2".to_string()),
            endpoint_url: Some("http://localhost:9000".to_string()),
            allow_http: true,
            anonymous: false,
            force_path_style: false,
            network_stream_timeout_seconds: None,
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
            r#"{"config":{"region":"us-west-2","endpoint_url":"http://localhost:9000","anonymous":false,"allow_http":true,"force_path_style":false,"network_stream_timeout_seconds":null},"credentials":{"s3_credential_type":"static","access_key_id":"access_key_id","secret_access_key":"secret_access_key","session_token":"session_token","expires_after":null},"bucket":"bucket","prefix":"prefix","can_write":true,"extra_read_headers":[],"extra_write_headers":[]}"#
        );

        let deserialized: S3Storage = serde_json::from_str(&serialized).unwrap();
        assert_eq!(storage.config, deserialized.config);
    }

    #[tokio_test]
    async fn test_s3_paths() {
        let storage = S3Storage::new(
            S3Options {
                region: Some("us-west-2".to_string()),
                endpoint_url: None,
                allow_http: true,
                anonymous: false,
                force_path_style: false,
                network_stream_timeout_seconds: None,
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
    }
}
