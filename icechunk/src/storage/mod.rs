<<<<<<< composable-storage
#[cfg(feature = "object-store-http")]
use ::object_store::ClientConfigKey;
#[cfg(feature = "object-store-azure")]
use ::object_store::azure::AzureConfigKey;
#[cfg(feature = "object-store-gcs")]
use ::object_store::gcp::GoogleConfigKey;

#[cfg(feature = "s3")]
=======
//! Object store abstraction layer.
//!
//! The [`Storage`] trait defines generic object store operations (get, put, delete,
//! list) for persisting Icechunk data. Constructor functions like [`new_s3_storage`],
//! [`new_gcs_storage`], [`new_in_memory_storage`] create configured storage instances.

use ::object_store::{ClientConfigKey, azure::AzureConfigKey, gcp::GoogleConfigKey};
>>>>>>> main
use aws_sdk_s3::{
    config::http::HttpResponse,
    error::SdkError,
    operation::{
        complete_multipart_upload::CompleteMultipartUploadError,
        copy_object::CopyObjectError,
        create_multipart_upload::CreateMultipartUploadError,
        delete_objects::DeleteObjectsError, get_object::GetObjectError,
        head_object::HeadObjectError, list_objects_v2::ListObjectsV2Error,
        put_object::PutObjectError, upload_part::UploadPartError,
    },
    primitives::ByteStreamError,
};

use chrono::{DateTime, Utc};
use core::fmt;
use futures::{
    Stream, StreamExt, TryStreamExt,
    stream::{self, BoxStream, FuturesOrdered},
};
use itertools::Itertools;

#[cfg(feature = "s3")]
use s3::S3Storage;

use serde::{Deserialize, Serialize};
use std::{
    cmp::{max, min},
    collections::HashMap,
    ffi::OsString,
    iter,
    num::{NonZeroU16, NonZeroU64},
    ops::Range,
    path::Path,
    pin::Pin,
    str::FromStr as _,
    sync::{Arc, Mutex, OnceLock},
};
use tokio::io::AsyncBufRead;
use tokio_util::io::StreamReader;
use tracing::{instrument, warn};
use url::Url;

use async_trait::async_trait;
use bytes::Bytes;
use thiserror::Error;

#[cfg(test)]
pub mod logging;

/// Storage using the `object_store` crate (local, in-memory, Azure, GCS).
pub mod object_store;
/// HTTP redirect-based storage for read-only access.
pub mod redirect;
<<<<<<< composable-storage

#[cfg(feature = "s3")]
=======
/// Native S3 client implementation.
>>>>>>> main
pub mod s3;

pub use object_store::ObjectStorage;

use crate::{
    error::ICError,
    private,
    registry::{extract_type_tag, serialize_with_tag, storage_registry},
    storage::redirect::RedirectStorage,
};

<<<<<<< composable-storage
#[cfg(any(feature = "s3", feature = "object-store-s3"))]
use crate::config::S3Credentials;

#[cfg(any(feature = "s3", feature = "object-store-s3"))]
use crate::config::S3Options;

#[cfg(feature = "object-store-azure")]
use crate::config::AzureCredentials;

#[cfg(feature = "object-store-gcs")]
use crate::config::GcsCredentials;

=======
/// Storage operation error types.
>>>>>>> main
#[derive(Debug, Error)]
pub enum StorageErrorKind {
    #[error("object not found")]
    ObjectNotFound,
    #[error("object store error {0}")]
    ObjectStore(#[from] Box<::object_store::Error>),
    #[error("bad object store prefix {0:?}")]
    BadPrefix(OsString),

    #[cfg(feature = "s3")]
    #[error("error getting object from object store {0}")]
    S3GetObjectError(#[from] Box<SdkError<GetObjectError, HttpResponse>>),
    #[cfg(feature = "s3")]
    #[error("error writing object to object store {0}")]
    S3PutObjectError(#[from] Box<SdkError<PutObjectError, HttpResponse>>),
    #[cfg(feature = "s3")]
    #[error("error creating multipart upload {0}")]
    S3CreateMultipartUploadError(
        #[from] Box<SdkError<CreateMultipartUploadError, HttpResponse>>,
    ),
    #[cfg(feature = "s3")]
    #[error("error uploading multipart part {0}")]
    S3UploadPartError(#[from] Box<SdkError<UploadPartError, HttpResponse>>),
    #[cfg(feature = "s3")]
    #[error("error completing multipart upload {0}")]
    S3CompleteMultipartUploadError(
        #[from] Box<SdkError<CompleteMultipartUploadError, HttpResponse>>,
    ),
    #[cfg(feature = "s3")]
    #[error("error copying object in object store {0}")]
    S3CopyObjectError(#[from] Box<SdkError<CopyObjectError, HttpResponse>>),
    #[cfg(feature = "s3")]
    #[error("error getting object metadata from object store {0}")]
    S3HeadObjectError(#[from] Box<SdkError<HeadObjectError, HttpResponse>>),
    #[cfg(feature = "s3")]
    #[error("error listing objects in object store {0}")]
    S3ListObjectError(#[from] Box<SdkError<ListObjectsV2Error, HttpResponse>>),
    #[cfg(feature = "s3")]
    #[error("error deleting objects in object store {0}")]
    S3DeleteObjectError(#[from] Box<SdkError<DeleteObjectsError, HttpResponse>>),
    #[cfg(feature = "s3")]
    #[error("error streaming bytes from object store {0}")]
    S3StreamError(#[from] Box<ByteStreamError>),

    #[error("I/O error: {0}")]
    IOError(#[from] std::io::Error),
    #[error("storage configuration error: {0}")]
    R2ConfigurationError(String),
    #[error("error parsing URL: {url:?}")]
    CannotParseUrl {
        #[source]
        cause: url::ParseError,
        url: String,
    },
    #[error("Redirect Storage error: {0}")]
    BadRedirect(String),
    #[error("storage error: {0}")]
    Other(String),
}
pub type StorageError = ICError<StorageErrorKind>;

// it would be great to define this impl in error.rs, but it conflicts with the blanket
// `impl From<T> for T`
impl<E> From<E> for StorageError
where
    E: Into<StorageErrorKind>,
{
    fn from(value: E) -> Self {
        Self::new(value.into())
    }
}

pub type StorageResult<A> = Result<A, StorageError>;

#[derive(Debug)]
pub struct ListInfo<A> {
    pub id: A,
    pub created_at: DateTime<Utc>,
    pub size_bytes: u64,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone, Hash, PartialOrd, Ord)]
pub struct ETag(pub String);
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone, Default)]
pub struct Generation(pub String);

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone)]
pub struct VersionInfo {
    pub etag: Option<ETag>,
    pub generation: Option<Generation>,
}

impl VersionInfo {
    pub fn for_creation() -> Self {
        Self { etag: None, generation: None }
    }

    pub fn from_etag_only(etag: String) -> Self {
        Self { etag: Some(ETag(etag)), generation: None }
    }

    pub fn is_create(&self) -> bool {
        self.etag.is_none() && self.generation.is_none()
    }

    pub fn etag(&self) -> Option<&String> {
        self.etag.as_ref().map(|e| &e.0)
    }

    pub fn generation(&self) -> Option<&String> {
        self.generation.as_ref().map(|e| &e.0)
    }
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone, Default)]
pub struct RetriesSettings {
    pub max_tries: Option<NonZeroU16>,
    pub initial_backoff_ms: Option<u32>,
    pub max_backoff_ms: Option<u32>,
}

impl RetriesSettings {
    pub fn max_tries(&self) -> NonZeroU16 {
        self.max_tries.unwrap_or_else(|| NonZeroU16::new(10).unwrap_or(NonZeroU16::MIN))
    }

    pub fn initial_backoff_ms(&self) -> u32 {
        self.initial_backoff_ms.unwrap_or(100)
    }

    pub fn max_backoff_ms(&self) -> u32 {
        self.max_backoff_ms.unwrap_or(3 * 60 * 1000)
    }

    pub fn merge(&self, other: Self) -> Self {
        Self {
            max_tries: other.max_tries.or(self.max_tries),
            initial_backoff_ms: other.initial_backoff_ms.or(self.initial_backoff_ms),
            max_backoff_ms: other.max_backoff_ms.or(self.max_backoff_ms),
        }
    }
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone, Default)]
pub struct ConcurrencySettings {
    pub max_concurrent_requests_for_object: Option<NonZeroU16>,
    pub ideal_concurrent_request_size: Option<NonZeroU64>,
}

impl ConcurrencySettings {
    // AWS recommendations: https://docs.aws.amazon.com/whitepapers/latest/s3-optimizing-performance-best-practices/horizontal-scaling-and-request-parallelization-for-high-throughput.html
    // 8-16 MB requests
    // 85-90 MB/s per request
    // these numbers would saturate a 12.5 Gbps network

    pub fn max_concurrent_requests_for_object(&self) -> NonZeroU16 {
        self.max_concurrent_requests_for_object
            .unwrap_or_else(|| NonZeroU16::new(18).unwrap_or(NonZeroU16::MIN))
    }
    pub fn ideal_concurrent_request_size(&self) -> NonZeroU64 {
        self.ideal_concurrent_request_size.unwrap_or_else(|| {
            NonZeroU64::new(12 * 1024 * 1024).unwrap_or(NonZeroU64::MIN)
        })
    }

    pub fn merge(&self, other: Self) -> Self {
        Self {
            max_concurrent_requests_for_object: other
                .max_concurrent_requests_for_object
                .or(self.max_concurrent_requests_for_object),
            ideal_concurrent_request_size: other
                .ideal_concurrent_request_size
                .or(self.ideal_concurrent_request_size),
        }
    }
}

/// Configuration for storage operations (retries, concurrency, storage classes).
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone, Default)]
pub struct Settings {
    #[serde(default)]
    pub concurrency: Option<ConcurrencySettings>,

    #[serde(default)]
    pub retries: Option<RetriesSettings>,

    #[serde(default)]
    pub unsafe_use_conditional_update: Option<bool>,

    #[serde(default)]
    pub unsafe_use_conditional_create: Option<bool>,

    #[serde(default)]
    pub unsafe_use_metadata: Option<bool>,

    #[serde(default)]
    pub storage_class: Option<String>,

    #[serde(default)]
    pub metadata_storage_class: Option<String>,

    #[serde(default)]
    pub chunks_storage_class: Option<String>,

    #[serde(default)]
    pub minimum_size_for_multipart_upload: Option<u64>,
}

static DEFAULT_CONCURRENCY: OnceLock<ConcurrencySettings> = OnceLock::new();
static DEFAULT_RETRIES: OnceLock<RetriesSettings> = OnceLock::new();

impl Settings {
    pub fn concurrency(&self) -> &ConcurrencySettings {
        self.concurrency
            .as_ref()
            .unwrap_or_else(|| DEFAULT_CONCURRENCY.get_or_init(Default::default))
    }

    pub fn retries(&self) -> &RetriesSettings {
        self.retries
            .as_ref()
            .unwrap_or_else(|| DEFAULT_RETRIES.get_or_init(Default::default))
    }

    pub fn unsafe_use_conditional_create(&self) -> bool {
        self.unsafe_use_conditional_create.unwrap_or(true)
    }

    pub fn unsafe_use_conditional_update(&self) -> bool {
        self.unsafe_use_conditional_update.unwrap_or(true)
    }

    pub fn unsafe_use_metadata(&self) -> bool {
        self.unsafe_use_metadata.unwrap_or(true)
    }

    pub fn metadata_storage_class(&self) -> Option<&String> {
        self.metadata_storage_class.as_ref().or(self.storage_class.as_ref())
    }

    pub fn storage_class(&self) -> Option<&String> {
        self.storage_class.as_ref()
    }

    pub fn chunks_storage_class(&self) -> Option<&String> {
        self.chunks_storage_class.as_ref().or(self.storage_class.as_ref())
    }

    pub fn minimum_size_for_multipart_upload(&self) -> u64 {
        // per AWS  recommendation: 100 MB
        self.minimum_size_for_multipart_upload.unwrap_or(100 * 1024 * 1024)
    }

    pub fn merge(&self, other: Self) -> Self {
        Self {
            concurrency: match (&self.concurrency, other.concurrency) {
                (None, None) => None,
                (None, Some(c)) => Some(c),
                (Some(c), None) => Some(c.clone()),
                (Some(mine), Some(theirs)) => Some(mine.merge(theirs)),
            },
            retries: match (&self.retries, other.retries) {
                (None, None) => None,
                (None, Some(c)) => Some(c),
                (Some(c), None) => Some(c.clone()),
                (Some(mine), Some(theirs)) => Some(mine.merge(theirs)),
            },
            unsafe_use_conditional_create: match (
                &self.unsafe_use_conditional_create,
                other.unsafe_use_conditional_create,
            ) {
                (None, None) => None,
                (None, Some(c)) => Some(c),
                (Some(c), None) => Some(*c),
                (Some(_), Some(theirs)) => Some(theirs),
            },
            unsafe_use_conditional_update: match (
                &self.unsafe_use_conditional_update,
                other.unsafe_use_conditional_update,
            ) {
                (None, None) => None,
                (None, Some(c)) => Some(c),
                (Some(c), None) => Some(*c),
                (Some(_), Some(theirs)) => Some(theirs),
            },
            unsafe_use_metadata: match (
                &self.unsafe_use_metadata,
                other.unsafe_use_metadata,
            ) {
                (None, None) => None,
                (None, Some(c)) => Some(c),
                (Some(c), None) => Some(*c),
                (Some(_), Some(theirs)) => Some(theirs),
            },
            storage_class: match (&self.storage_class, other.storage_class) {
                (None, None) => None,
                (None, Some(c)) => Some(c),
                (Some(c), None) => Some(c.clone()),
                (Some(_), Some(theirs)) => Some(theirs),
            },
            metadata_storage_class: match (
                &self.metadata_storage_class,
                other.metadata_storage_class,
            ) {
                (None, None) => None,
                (None, Some(c)) => Some(c),
                (Some(c), None) => Some(c.clone()),
                (Some(_), Some(theirs)) => Some(theirs),
            },
            chunks_storage_class: match (
                &self.chunks_storage_class,
                other.chunks_storage_class,
            ) {
                (None, None) => None,
                (None, Some(c)) => Some(c),
                (Some(c), None) => Some(c.clone()),
                (Some(_), Some(theirs)) => Some(theirs),
            },
            minimum_size_for_multipart_upload: match (
                &self.minimum_size_for_multipart_upload,
                other.minimum_size_for_multipart_upload,
            ) {
                (None, None) => None,
                (None, Some(c)) => Some(c),
                (Some(c), None) => Some(*c),
                (Some(_), Some(theirs)) => Some(theirs),
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VersionedUpdateResult {
    Updated { new_version: VersionInfo },
    NotOnLatestVersion,
}

impl VersionedUpdateResult {
    pub fn must_write(self) -> StorageResult<VersionInfo> {
        match self {
            VersionedUpdateResult::Updated { new_version } => Ok(new_version),
            VersionedUpdateResult::NotOnLatestVersion => {
                Err(StorageErrorKind::ObjectNotFound.into())
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct DeleteObjectsResult {
    pub deleted_objects: u64,
    pub deleted_bytes: u64,
}

impl DeleteObjectsResult {
    pub fn merge(&mut self, other: &Self) {
        self.deleted_objects += other.deleted_objects;
        self.deleted_bytes += other.deleted_bytes;
    }
}

/// Fetch and write the parquet files that represent the repository in object store
///
/// Different implementation can cache the files differently, or not at all.
/// Implementations are free to assume files are never overwritten.
#[async_trait]
pub trait Storage: fmt::Debug + fmt::Display + private::Sealed + Sync + Send {
    /// Returns the type tag used for serialization/deserialization.
    fn type_tag(&self) -> &'static str;

    /// Serialize self for use with the registry pattern.
    fn as_serialize(&self) -> &dyn erased_serde::Serialize;
    async fn default_settings(&self) -> StorageResult<Settings> {
        Ok(Default::default())
    }

    async fn can_write(&self) -> StorageResult<bool>;

    async fn get_object(
        &self,
        settings: &Settings,
        path: &str,
        range: Option<&Range<u64>>,
    ) -> StorageResult<(Pin<Box<dyn AsyncBufRead + Send>>, VersionInfo)> {
        if let Some(range) = range {
            self.get_object_concurrently(settings, path, range).await
        } else {
            self.get_object_range_read(settings, path, range).await
        }
    }

    async fn get_object_range_read(
        &self,
        settings: &Settings,
        path: &str,
        range: Option<&Range<u64>>,
    ) -> StorageResult<(Pin<Box<dyn AsyncBufRead + Send>>, VersionInfo)> {
        let (stream, version) = self.get_object_range(settings, path, range).await?;
        let reader = StreamReader::new(stream.map_err(std::io::Error::other));
        Ok((Box::pin(reader), version))
    }

    async fn get_object_range(
        &self,
        settings: &Settings,
        path: &str,
        range: Option<&Range<u64>>,
    ) -> StorageResult<(
        Pin<Box<dyn Stream<Item = Result<Bytes, StorageError>> + Send>>,
        VersionInfo,
    )>;

    async fn put_object(
        &self,
        settings: &Settings,
        path: &str,
        bytes: Bytes,
        content_type: Option<&str>,
        metadata: Vec<(String, String)>,
        previous_version: Option<&VersionInfo>,
    ) -> StorageResult<VersionedUpdateResult>;

    async fn copy_object(
        &self,
        settings: &Settings,
        from: &str,
        to: &str,
        content_type: Option<&str>,
        version: &VersionInfo,
    ) -> StorageResult<VersionedUpdateResult>;

    async fn list_objects<'a>(
        &'a self,
        settings: &Settings,
        prefix: &str,
    ) -> StorageResult<BoxStream<'a, StorageResult<ListInfo<String>>>>;

    async fn delete_batch(
        &self,
        settings: &Settings,
        prefix: &str,
        batch: Vec<(String, u64)>,
    ) -> StorageResult<DeleteObjectsResult>;

    async fn get_object_last_modified(
        &self,
        path: &str,
        settings: &Settings,
    ) -> StorageResult<DateTime<Utc>>;

    /// Delete a stream of objects, by their id string representations
    /// Input stream includes sizes to get as result the total number of bytes deleted
    #[instrument(skip(self, settings, ids))]
    async fn delete_objects(
        &self,
        settings: &Settings,
        prefix: &str,
        ids: BoxStream<'_, (String, u64)>,
    ) -> StorageResult<DeleteObjectsResult> {
        let res = Arc::new(Mutex::new(DeleteObjectsResult::default()));
        ids.chunks(1_000)
            // FIXME: configurable concurrency
            .for_each_concurrent(10, |batch| {
                let res = Arc::clone(&res);
                async move {
                    let new_deletes = self
                        .delete_batch(settings, prefix, batch)
                        .await
                        .unwrap_or_else(|_| {
                            // FIXME: handle error instead of skipping
                            warn!("ignoring error in Storage::delete_batch");
                            Default::default()
                        });
                    #[allow(clippy::expect_used)]
                    res.lock().expect("Bug in delete objects").merge(&new_deletes);
                }
            })
            .await;
        #[allow(clippy::expect_used)]
        let res = res.lock().expect("Bug in delete objects");
        Ok(res.clone())
    }

    async fn root_is_clean(&self) -> StorageResult<bool> {
        match self.list_objects(&Settings::default(), "").await?.next().await {
            None => Ok(true),
            Some(Ok(_)) => Ok(false),
            Some(Err(err)) => Err(err),
        }
    }

    async fn get_object_concurrently_multiple(
        &self,
        settings: &Settings,
        key: &str,
        parts: Vec<Range<u64>>,
    ) -> StorageResult<(Pin<Box<dyn AsyncBufRead + Send>>, VersionInfo)> {
        let settings2 = settings.clone();
        let key2 = key.to_string();
        let results = parts
            .into_iter()
            .map(move |range| {
                let key = key2.clone();
                let settings = settings2.clone();
                async move {
                    let (stream, version) = self
                        .get_object_range(&settings, key.as_ref(), Some(&range))
                        .await?;
                    let all_bytes: Vec<_> = stream.try_collect().await?;
                    Ok::<_, StorageError>((all_bytes, version))
                }
            })
            .collect::<FuturesOrdered<_>>();

        let results = results.peekable();
        tokio::pin!(results);
        let version = match results.as_mut().peek().await {
            Some(Ok((_, version))) => version.clone(),
            _ => VersionInfo::for_creation(),
        };
        let all_bytes = results
            .map_ok(|(all_bytes, _)| stream::iter(all_bytes).map(Ok::<_, StorageError>))
            .try_flatten()
            .map_err(std::io::Error::other)
            .try_collect::<Vec<_>>()
            .await?;

        let res = StreamReader::new(stream::iter(all_bytes).map(Ok::<_, std::io::Error>));
        Ok((Box::pin(res), version))
    }

    async fn get_object_concurrently(
        &self,
        settings: &Settings,
        key: &str,
        range: &Range<u64>,
    ) -> StorageResult<(Pin<Box<dyn AsyncBufRead + Send>>, VersionInfo)> {
        let parts = split_in_multiple_requests(
            range,
            settings.concurrency().ideal_concurrent_request_size().get(),
            settings.concurrency().max_concurrent_requests_for_object().get(),
        )
        .collect::<Vec<_>>();

        let res: (Pin<Box<dyn AsyncBufRead + Send>>, VersionInfo) = match parts.len() {
            0 => (Box::pin(tokio::io::empty()), VersionInfo::for_creation()),
            1 => self.get_object_range_read(settings, key, Some(range)).await?,
            _ => self.get_object_concurrently_multiple(settings, key, parts).await?,
        };
        Ok(res)
    }
}

/// Split an object request into multiple byte range requests
///
/// Returns tuples of Range for each request.
///
/// It generates requests that are as similar as possible in size, this means no more than 1 byte
/// difference between the requests.
///
/// It tries to generate ceil(size/ideal_req_size) requests, but never exceeds max_requests.
///
/// ideal_req_size and max_requests must be > 0
pub fn split_in_multiple_requests(
    range: &Range<u64>,
    ideal_req_size: u64,
    max_requests: u16,
) -> impl Iterator<Item = Range<u64>> + use<> {
    let size = max(0, range.end - range.start);
    // we do a ceiling division, rounding always up
    let num_parts = size.div_ceil(ideal_req_size);
    // no more than max_parts, so we limit
    let num_parts = max(1, min(num_parts, max_requests as u64));

    // we split the total size into request that are as similar as possible in size
    // this means, we are going to have a few requests that are 1 byte larger
    let big_parts = size % num_parts;
    let small_parts_size = size / num_parts;
    let big_parts_size = small_parts_size + 1;

    iter::successors(Some((1, range.start..range.start)), move |(index, prev_range)| {
        let size = if *index <= big_parts { big_parts_size } else { small_parts_size };
        Some((index + 1, prev_range.end..prev_range.end + size))
    })
    .dropping(1)
    .take(num_parts as usize)
    .map(|(_, range)| range)
}

/// Split an object request into multiple byte range requests ensuring only the last request is
/// smaller
///
/// Returns tuples of Range for each request.
///
/// It tries to generate ceil(size/ideal_req_size) requests, but never exceeds max_requests.
///
/// ideal_req_size and max_requests must be > 0
pub fn split_in_multiple_equal_requests(
    range: &Range<u64>,
    ideal_req_size: u64,
    max_requests: u16,
) -> impl Iterator<Item = Range<u64>> + use<> {
    let size = max(0, range.end - range.start);
    // we do a ceiling division, rounding always up
    let num_parts = size.div_ceil(ideal_req_size);
    // no more than max_parts, so we limit
    let num_parts = max(1, min(num_parts, max_requests as u64));

    let big_parts = num_parts - 1;
    let big_parts_size = size / max(1, big_parts);
    let small_part_size = size - big_parts_size * big_parts;

    iter::successors(Some((1, range.start..range.start)), move |(index, prev_range)| {
        let size = if *index <= big_parts { big_parts_size } else { small_part_size };
        Some((index + 1, prev_range.end..prev_range.end + size))
    })
    .dropping(1)
    .take(num_parts as usize)
    .map(|(_, range)| range)
}

#[cfg(feature = "s3")]
pub fn new_s3_storage(
    config: S3Options,
    bucket: String,
    prefix: Option<String>,
    credentials: Option<S3Credentials>,
) -> StorageResult<Arc<dyn Storage>> {
    if let Some(endpoint) = &config.endpoint_url
        && (endpoint.contains("fly.storage.tigris.dev")
            || endpoint.contains("t3.storage.dev"))
    {
        return Err(StorageError::from(StorageErrorKind::Other("Tigris Storage is not S3 compatible, use the Tigris specific constructor instead".to_string())));
    }

    let st = S3Storage::new(
        config,
        bucket,
        prefix,
        credentials.unwrap_or(S3Credentials::FromEnv),
        true,
        Vec::new(),
        Vec::new(),
    )?;
    Ok(Arc::new(st))
}

#[cfg(feature = "s3")]
pub fn new_r2_storage(
    config: S3Options,
    bucket: Option<String>,
    prefix: Option<String>,
    account_id: Option<String>,
    credentials: Option<S3Credentials>,
) -> StorageResult<Arc<dyn Storage>> {
    let (bucket, prefix) = match (bucket, prefix) {
        (Some(bucket), Some(prefix)) => (bucket, Some(prefix)),
        (None, Some(prefix)) => match prefix.split_once("/") {
            Some((bucket, prefix)) => (bucket.to_string(), Some(prefix.to_string())),
            None => (prefix, None),
        },
        (Some(bucket), None) => (bucket, None),
        (None, None) => {
            return Err(StorageErrorKind::R2ConfigurationError(
                "Either bucket or prefix must be provided.".to_string(),
            )
            .into());
        }
    };

    if config.endpoint_url.is_none() && account_id.is_none() {
        return Err(StorageErrorKind::R2ConfigurationError(
            "Either endpoint_url or account_id must be provided.".to_string(),
        )
        .into());
    }

    let config = S3Options {
        region: config.region.or(Some("auto".to_string())),
        endpoint_url: config
            .endpoint_url
            .or(account_id.map(|x| format!("https://{x}.r2.cloudflarestorage.com"))),
        force_path_style: true,
        ..config
    };
    let st = S3Storage::new(
        config,
        bucket,
        prefix,
        credentials.unwrap_or(S3Credentials::FromEnv),
        true,
        Vec::new(),
        Vec::new(),
    )?;
    Ok(Arc::new(st))
}

#[cfg(feature = "s3")]
pub fn new_tigris_storage(
    config: S3Options,
    bucket: String,
    prefix: Option<String>,
    credentials: Option<S3Credentials>,
    use_weak_consistency: bool,
) -> StorageResult<Arc<dyn Storage>> {
    let config = S3Options {
        endpoint_url: Some(
            config.endpoint_url.unwrap_or("https://t3.storage.dev".to_string()),
        ),
        ..config
    };
    let mut extra_write_headers = Vec::with_capacity(2);
    let mut extra_read_headers = Vec::with_capacity(3);

    if !use_weak_consistency {
        // TODO: Tigris will need more than this to offer good eventually consistent behavior
        // For example: we should use no-cache for branches and config file
        if let Some(region) = config.region.as_ref() {
            extra_write_headers.push(("X-Tigris-Regions".to_string(), region.clone()));
            extra_write_headers
                .push(("X-Tigris-Consistent".to_string(), "true".to_string()));

            extra_read_headers.push(("X-Tigris-Regions".to_string(), region.clone()));
            extra_read_headers
                .push(("Cache-Control".to_string(), "no-cache".to_string()));
            extra_read_headers
                .push(("X-Tigris-Consistent".to_string(), "true".to_string()));
        } else {
            return Err(StorageErrorKind::Other("Tigris storage requires a region to provide full consistency. Either set the region for the bucket or use the read-only, eventually consistent storage by passing `use_weak_consistency=True` (experts only)".to_string()).into());
        }
    }
    let st = S3Storage::new(
        config,
        bucket,
        prefix,
        credentials.unwrap_or(S3Credentials::FromEnv),
        !use_weak_consistency, // notice eventually consistent storage can't do writes
        extra_read_headers,
        extra_write_headers,
    )?;
    Ok(Arc::new(st))
}

pub async fn new_in_memory_storage() -> StorageResult<Arc<dyn Storage>> {
    let st = ObjectStorage::new_in_memory().await?;
    Ok(Arc::new(st))
}

#[cfg(feature = "object-store-local")]
pub async fn new_local_filesystem_storage(
    path: &Path,
) -> StorageResult<Arc<dyn Storage>> {
    let st = ObjectStorage::new_local_filesystem(path).await?;
    Ok(Arc::new(st))
}

#[cfg(feature = "object-store-http")]
pub fn new_http_storage(
    base_url: &str,
    config: Option<HashMap<String, String>>,
) -> StorageResult<Arc<dyn Storage>> {
    let base_url = Url::parse(base_url).map_err(|e| {
        StorageErrorKind::CannotParseUrl { cause: e, url: base_url.to_string() }
    })?;
    let config = config
        .unwrap_or_default()
        .iter()
        .filter_map(|(k, v)| {
            ClientConfigKey::from_str(k).ok().map(|key| (key, v.clone()))
        })
        .collect();
    let st = ObjectStorage::new_http(base_url, Some(config))?;
    Ok(Arc::new(st))
}

pub fn new_redirect_storage(base_url: &str) -> StorageResult<Arc<dyn Storage>> {
    let base_url = Url::parse(base_url).map_err(|e| {
        StorageErrorKind::CannotParseUrl { cause: e, url: base_url.to_string() }
    })?;
    Ok(Arc::new(RedirectStorage::new(base_url)))
}

#[cfg(feature = "object-store-s3")]
pub async fn new_s3_object_store_storage(
    config: S3Options,
    bucket: String,
    prefix: Option<String>,
    credentials: Option<S3Credentials>,
) -> StorageResult<Arc<dyn Storage>> {
    if let Some(endpoint) = &config.endpoint_url
        && (endpoint.contains("fly.storage.tigris.dev")
            || endpoint.contains("t3.storage.dev"))
    {
        return Err(StorageError::from(StorageErrorKind::Other("Tigris Storage is not S3 compatible, use the Tigris specific constructor instead".to_string())));
    }
    let storage =
        ObjectStorage::new_s3(bucket, prefix, credentials, Some(config)).await?;
    Ok(Arc::new(storage))
}

#[cfg(feature = "object-store-azure")]
pub async fn new_azure_blob_storage(
    account: String,
    container: String,
    prefix: Option<String>,
    credentials: Option<AzureCredentials>,
    config: Option<HashMap<String, String>>,
) -> StorageResult<Arc<dyn Storage>> {
    let config = config
        .unwrap_or_default()
        .into_iter()
        .filter_map(|(key, value)| key.parse::<AzureConfigKey>().map(|k| (k, value)).ok())
        .collect();
    let storage =
        ObjectStorage::new_azure(account, container, prefix, credentials, Some(config))
            .await?;
    Ok(Arc::new(storage))
}

#[cfg(feature = "object-store-gcs")]
pub fn new_gcs_storage(
    bucket: String,
    prefix: Option<String>,
    credentials: Option<GcsCredentials>,
    config: Option<HashMap<String, String>>,
) -> StorageResult<Arc<dyn Storage>> {
    let config = config
        .unwrap_or_default()
        .into_iter()
        .filter_map(|(key, value)| {
            key.parse::<GoogleConfigKey>().map(|k| (k, value)).ok()
        })
        .collect();
    let storage = ObjectStorage::new_gcs(bucket, prefix, credentials, Some(config))?;
    Ok(Arc::new(storage))
}

// =============================================================================
// Serde helper module for Arc<dyn Storage>
// =============================================================================

/// Serde helper module for serializing/deserializing Arc<dyn Storage>.
/// Use with `#[serde(with = "storage_serde")]` on fields.
pub mod storage_serde {
    use super::*;

    pub fn serialize<S>(
        storage: &Arc<dyn Storage + Send + Sync>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serialize_with_tag(serializer, "type", storage.type_tag(), storage.as_serialize())
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Arc<dyn Storage + Send + Sync>, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = rmpv::Value::deserialize(deserializer)?;
        let type_tag =
            extract_type_tag(&value, "type").map_err(serde::de::Error::custom)?;

        let registry = storage_registry();
        let deserialize_fn = registry.get(&type_tag).ok_or_else(|| {
            serde::de::Error::custom(format!(
                "Unknown Storage type: {}. Available types depend on enabled features.",
                type_tag
            ))
        })?;

        // The registry returns Arc<dyn Storage>, but since Storage: Send + Sync,
        // we can safely convert it to Arc<dyn Storage + Send + Sync>
        let storage = deserialize_fn(value).map_err(serde::de::Error::custom)?;
        // SAFETY: Storage trait requires Send + Sync, so this cast is always valid
        Ok(storage as Arc<dyn Storage + Send + Sync>)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::panic)]
mod tests {

    use std::{collections::HashSet, fs::File, io::Write, path::PathBuf};

    use crate::config::{GcsBearerCredential, GcsStaticCredentials};

    use super::*;
    use icechunk_macros::tokio_test;
    use proptest::prelude::*;
    use tempfile::TempDir;

    #[tokio_test]
    async fn test_is_clean() {
        let repo_dir = TempDir::new().unwrap();
        let s = new_local_filesystem_storage(repo_dir.path()).await.unwrap();
        assert!(s.root_is_clean().await.unwrap());

        let mut file = File::create(repo_dir.path().join("foo.txt")).unwrap();
        write!(file, "hello").unwrap();
        assert!(!s.root_is_clean().await.unwrap());

        let inside_existing =
            PathBuf::from_iter([repo_dir.path().as_os_str().to_str().unwrap(), "foo"]);
        let s = new_local_filesystem_storage(&inside_existing).await.unwrap();
        assert!(s.root_is_clean().await.unwrap());
    }

    #[tokio_test]
    /// Regression test: we can deserialize a GCS credential with token
    async fn test_gcs_session_serialization() {
        // Use a wrapper struct to test serialization since we use serde(with = ...) pattern
        #[derive(Serialize, Deserialize)]
        struct StorageWrapper {
            #[serde(with = "super::storage_serde")]
            storage: Arc<dyn Storage + Send + Sync>,
        }

        let storage = new_gcs_storage(
            "bucket".to_string(),
            Some("prefix".to_string()),
            Some(GcsCredentials::Static(GcsStaticCredentials::BearerToken(
                GcsBearerCredential {
                    bearer: "the token".to_string(),
                    expires_after: None,
                },
            ))),
            None,
        )
        .unwrap();
        let wrapper = StorageWrapper { storage };
        let bytes = rmp_serde::to_vec(&wrapper).unwrap();
        let dese: Result<StorageWrapper, _> = rmp_serde::from_slice(&bytes);
        assert!(dese.is_ok())
    }

    proptest! {
        #![proptest_config(ProptestConfig {
            cases: 999, .. ProptestConfig::default()
        })]

        #[icechunk_macros::test]
        fn test_split_equal_requests(offset in 0..1_000_000u64, size in 1..3_000_000_000u64, part_size in 1..16_000_000u64, max_parts in 1..100u16 ) {
            let res: Vec<_> = split_in_multiple_equal_requests(&(offset..offset+size), part_size, max_parts).collect();
            // there is always at least 1 request
            prop_assert!(!res.is_empty());

            // it does as many requests as possible
            prop_assert!(res.len() as u64 >= min(max_parts as u64, size / part_size));

            // there are never more than max_parts requests
            prop_assert!(res.len() <= max_parts as usize);

            // the request sizes add up to total size
            prop_assert_eq!(res.iter().map(|range| range.end - range.start).sum::<u64>(), size);

            let sizes: Vec<_> = res.iter().map(|range| range.end - range.start).collect();
            if sizes.len() > 1 {
                // all but last request have the same size
                assert_eq!(sizes.iter().rev().skip(1).unique().count(), 1);

                // last element is smaller or equal
                assert!(sizes.last().unwrap() <= sizes.first().unwrap());
            }

            // we split as much as possible
            assert!(res.len() >= min((size / part_size) as usize, max_parts as usize) );

            // there are no holes in the requests, nor bytes that are requested more than once
            let mut iter = res.iter();
            iter.next();
            prop_assert!(res.iter().zip(iter).all(|(r1,r2)| r1.end == r2.start));
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig {
            cases: 999, .. ProptestConfig::default()
        })]
        #[icechunk_macros::test]
        fn test_split_requests(offset in 0..1_000_000u64, size in 1..3_000_000_000u64, part_size in 1..16_000_000u64, max_parts in 1..100u16 ) {
            let res: Vec<_> = split_in_multiple_requests(&(offset..offset+size), part_size, max_parts).collect();

            // there is always at least 1 request
            prop_assert!(!res.is_empty());

            // it does as many requests as possible
            prop_assert!(res.len() as u64 >= min(max_parts as u64, size / part_size));

            // there are never more than max_parts requests
            prop_assert!(res.len() <= max_parts as usize);

            // the request sizes add up to total size
            prop_assert_eq!(res.iter().map(|range| range.end - range.start).sum::<u64>(), size);

            // there are only two request sizes
            let sizes: HashSet<_> = res.iter().map(|range| range.end - range.start).collect();
            prop_assert!(sizes.len() <= 2); // only last element is smaller
            if sizes.len() > 1 {
                // the smaller request size is one less than the big ones
                prop_assert_eq!(sizes.iter().min().unwrap() + 1, *sizes.iter().max().unwrap() );
            }

            // we split as much as possible
            assert!(res.len() >= min((size / part_size) as usize, max_parts as usize) );

            // there are no holes in the requests, nor bytes that are requested more than once
            let mut iter = res.iter();
            iter.next();
            prop_assert!(res.iter().zip(iter).all(|(r1,r2)| r1.end == r2.start));
        }

    }

    #[icechunk_macros::test]
    fn test_split_examples() {
        assert_eq!(
            split_in_multiple_requests(&(0..4), 4, 100,).collect::<Vec<_>>(),
            vec![0..4]
        );
        assert_eq!(
            split_in_multiple_requests(&(10..14), 4, 100,).collect::<Vec<_>>(),
            vec![10..14]
        );
        assert_eq!(
            split_in_multiple_requests(&(20..23), 1, 100,).collect::<Vec<_>>(),
            vec![(20..21), (21..22), (22..23),]
        );
        assert_eq!(
            split_in_multiple_requests(&(10..16), 5, 100,).collect::<Vec<_>>(),
            vec![(10..13), (13..16)]
        );
        assert_eq!(
            split_in_multiple_requests(&(10..21), 5, 100,).collect::<Vec<_>>(),
            vec![(10..14), (14..18), (18..21)]
        );
        assert_eq!(
            split_in_multiple_requests(&(0..13), 5, 2,).collect::<Vec<_>>(),
            vec![(0..7), (7..13)]
        );
        assert_eq!(
            split_in_multiple_requests(&(0..100), 5, 3,).collect::<Vec<_>>(),
            vec![(0..34), (34..67), (67..100)]
        );
        // this is data from a real example
        assert_eq!(
            split_in_multiple_requests(&(0..19579213), 12_000_000, 18)
                .collect::<Vec<_>>(),
            vec![(0..9789607), (9789607..19579213)]
        );
    }
}
