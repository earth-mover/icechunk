//! Core storage trait and shared types.

use chrono::{DateTime, Utc};
use core::fmt;
use futures::{
    Stream, StreamExt as _, TryStreamExt as _,
    stream::{self, BoxStream, FuturesOrdered},
};
use itertools::Itertools as _;
use serde::{Deserialize, Serialize};
use std::{
    cmp::{max, min},
    ffi::OsString,
    fmt::Display,
    iter,
    num::{NonZeroU16, NonZeroU64},
    ops::Range,
    pin::Pin,
    sync::{Arc, Mutex, OnceLock},
};
use tokio::io::AsyncBufRead;
use tokio_util::io::StreamReader;
use tracing::{instrument, warn};

use async_trait::async_trait;
use bytes::Bytes;
use thiserror::Error;

use crate::ICError;
use crate::sealed;

/// Storage operation error types.
#[derive(Debug, Error)]
pub enum StorageErrorKind {
    #[error("object not found")]
    ObjectNotFound,
    #[error("bad object store prefix {0:?}")]
    BadPrefix(OsString),
    #[error("object store error {0}")]
    ObjectStore(#[from] Box<dyn std::error::Error + Send + Sync + 'static>),
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

pub type StorageResult<A> = Result<A, StorageError>;

pub fn obj_store_error_res<T>(
    err: impl std::error::Error + Send + Sync + 'static,
) -> StorageResult<T> {
    Err(obj_store_error(err))
}

pub fn obj_store_error(
    err: impl std::error::Error + Send + Sync + 'static,
) -> StorageError {
    StorageError::capture(StorageErrorKind::ObjectStore(Box::new(err)))
}

pub fn obj_not_found_res<T>() -> StorageResult<T> {
    Err(StorageError::capture(StorageErrorKind::ObjectNotFound))
}

pub fn other_error(s: impl Into<String>) -> StorageError {
    StorageError::capture(StorageErrorKind::Other(s.into()))
}

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

impl Display for VersionInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match (&self.etag, &self.generation) {
            (Some(etag), Some(generation)) => {
                write!(f, "etag={}, generation={}", etag.0, generation.0)
            }
            (Some(etag), None) => write!(f, "etag={}", etag.0),
            (None, Some(generation)) => write!(f, "generation={}", generation.0),
            (None, None) => write!(f, "new"),
        }
    }
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone, Copy, Default)]
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

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone, Copy, Default)]
pub struct TimeoutSettings {
    pub connect_timeout_ms: Option<u32>,
    pub read_timeout_ms: Option<u32>,
    pub operation_timeout_ms: Option<u32>,
    pub operation_attempt_timeout_ms: Option<u32>,
}

impl TimeoutSettings {
    pub fn merge(&self, other: Self) -> Self {
        Self {
            connect_timeout_ms: other.connect_timeout_ms.or(self.connect_timeout_ms),
            read_timeout_ms: other.read_timeout_ms.or(self.read_timeout_ms),
            operation_timeout_ms: other
                .operation_timeout_ms
                .or(self.operation_timeout_ms),
            operation_attempt_timeout_ms: other
                .operation_attempt_timeout_ms
                .or(self.operation_attempt_timeout_ms),
        }
    }
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone, Copy, Default)]
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
    pub timeouts: Option<TimeoutSettings>,

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

    pub fn timeouts(&self) -> Option<&TimeoutSettings> {
        self.timeouts.as_ref()
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
                (Some(c), None) => Some(*c),
                (Some(mine), Some(theirs)) => Some(mine.merge(theirs)),
            },
            retries: match (&self.retries, other.retries) {
                (None, None) => None,
                (None, Some(c)) => Some(c),
                (Some(c), None) => Some(*c),
                (Some(mine), Some(theirs)) => Some(mine.merge(theirs)),
            },
            timeouts: match (&self.timeouts, other.timeouts) {
                (None, None) => None,
                (None, Some(c)) => Some(c),
                (Some(c), None) => Some(*c),
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
                Err(StorageError::capture(StorageErrorKind::ObjectNotFound))
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

pub enum GetModifiedResult {
    Modified { data: Pin<Box<dyn AsyncBufRead + Send>>, new_version: VersionInfo },
    OnLatestVersion,
}

impl fmt::Debug for GetModifiedResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Modified { new_version, .. } => {
                f.debug_struct("Modified").field("new_version", new_version).finish()
            }
            Self::OnLatestVersion => write!(f, "OnLatestVersion"),
        }
    }
}

impl Display for GetModifiedResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Modified { new_version, .. } => {
                write!(f, "Modified(new_version={new_version})")
            }
            Self::OnLatestVersion => write!(f, "OnLatestVersion"),
        }
    }
}

/// Fetch and write the parquet files that represent the repository in object store
///
/// Different implementation can cache the files differently, or not at all.
/// Implementations are free to assume files are never overwritten.
#[async_trait]
#[typetag::serde(tag = "type")]
pub trait Storage: fmt::Debug + Display + sealed::Sealed + Sync + Send {
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

    /// List objects in storage whose keys start with the given prefix.
    ///
    /// Returns a stream of [`ListInfo`] entries, each containing the object's key and size in bytes.
    /// Pass an empty prefix to list all objects in the repository's storage root.
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

    async fn get_object_conditional(
        &self,
        settings: &Settings,
        path: &str,
        previous_version: Option<&VersionInfo>,
    ) -> StorageResult<GetModifiedResult>;

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
                    #[expect(clippy::expect_used)]
                    res.lock().expect("Bug in delete objects").merge(&new_deletes);
                }
            })
            .await;
        #[expect(clippy::expect_used)]
        let res = res.lock().expect("Bug in delete objects");
        Ok(res.clone())
    }

    async fn root_is_clean(&self, settings: &Settings) -> StorageResult<bool> {
        match self.list_objects(settings, "").await?.next().await {
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
/// It tries to generate `ceil(size/ideal_req_size)` requests, but never exceeds `max_requests`.
///
/// `ideal_req_size` and `max_requests` must be > 0
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
/// It tries to generate `ceil(size/ideal_req_size)` requests, but never exceeds `max_requests`.
///
/// `ideal_req_size` and `max_requests` must be > 0
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

pub fn strip_quotes(s: &str) -> &str {
    s.strip_prefix('"').and_then(|s| s.strip_suffix('"')).unwrap_or(s)
}
