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
    task::{Context, Poll},
};
use tokio::io::{AsyncBufRead, AsyncRead, ReadBuf};
use tokio_util::io::StreamReader;
use tracing::{instrument, warn};

use async_trait::async_trait;
use bytes::Bytes;
use thiserror::Error;

use crate::ICError;
use crate::governor::{
    Direction, IoOutcome, IoPermit, IoResult, MemoryPermit, ObjectRange, StorageContext,
};
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

pub use icechunk_types::ETag;
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

    /// Whether to stamp objects with user metadata. Defaults to `true`.
    ///
    /// Disabling this while leaving `unsafe_use_conditional_*` enabled
    /// silently neutralises the lost-response recovery for conditional
    /// PUTs: the conditional headers still go out, but with no write-id
    /// stamped a transient PUT failure can surface as a spurious
    /// `NotOnLatestVersion` even when the write landed. Only disable for
    /// backends that genuinely don't support user metadata.
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
/// Structured metadata about a storage backend, for display/repr purposes.
#[derive(Debug, Clone)]
pub struct StorageInfo {
    /// Human-readable backend type, e.g. "S3", "GCS", "in-memory".
    pub backend_type: &'static str,
    /// Key-value pairs of relevant configuration (bucket, prefix, path, etc.).
    pub fields: Vec<(&'static str, String)>,
}

/// Whether a repository may be created at a storage location.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RepositoryCreation {
    /// A new repository may be created here.
    Allowed,
    /// Refused: this is a cloud object store addressed at an empty prefix (the
    /// bucket root). New empty-prefix repositories are no longer supported,
    /// pre-existing ones can still be opened and updated.
    RefusedEmptyPrefix,
}

/// A reader carrying the [`IoPermit`] of the request that produced it.
///
/// Counts every byte handed to the consumer and completes the permit at
/// EOF; a read error completes it with [`IoResult::Error`]; dropping the
/// reader before EOF reports an abort (via the permit's `Drop`).
pub struct PermitTrackedReader {
    inner: Pin<Box<dyn AsyncBufRead + Send>>,
    permit: Option<IoPermit>,
    bytes: u64,
}

impl PermitTrackedReader {
    pub fn new(inner: Pin<Box<dyn AsyncBufRead + Send>>, permit: IoPermit) -> Self {
        Self { inner, permit: Some(permit), bytes: 0 }
    }

    fn finish(&mut self, result: IoResult) {
        if let Some(permit) = self.permit.take() {
            permit.complete(IoOutcome { bytes: self.bytes, result });
        }
    }
}

impl fmt::Debug for PermitTrackedReader {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PermitTrackedReader")
            .field("permit", &self.permit)
            .field("bytes", &self.bytes)
            .finish_non_exhaustive()
    }
}

impl AsyncRead for PermitTrackedReader {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();
        // zero bytes into a zero-capacity buffer is not EOF
        let had_capacity = buf.remaining() > 0;
        let before = buf.filled().len();
        match this.inner.as_mut().poll_read(cx, buf) {
            Poll::Ready(Ok(())) => {
                let n = (buf.filled().len() - before) as u64;
                this.bytes += n;
                if n == 0 && had_capacity {
                    this.finish(IoResult::Ok);
                }
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(err)) => {
                this.finish(IoResult::Error);
                Poll::Ready(Err(err))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl AsyncBufRead for PermitTrackedReader {
    fn poll_fill_buf(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<std::io::Result<&[u8]>> {
        let this = self.get_mut();
        let poll = this.inner.as_mut().poll_fill_buf(cx);
        // only disjoint fields below: `poll` still borrows `inner`
        match &poll {
            Poll::Ready(Ok([])) => {
                if let Some(permit) = this.permit.take() {
                    permit
                        .complete(IoOutcome { bytes: this.bytes, result: IoResult::Ok });
                }
            }
            Poll::Ready(Err(_)) => {
                if let Some(permit) = this.permit.take() {
                    permit.complete(IoOutcome {
                        bytes: this.bytes,
                        result: IoResult::Error,
                    });
                }
            }
            _ => {}
        }
        poll
    }

    fn consume(self: Pin<&mut Self>, amt: usize) {
        let this = self.get_mut();
        this.bytes += amt as u64;
        this.inner.as_mut().consume(amt);
    }
}

/// Implementations are free to assume files are never overwritten.
///
/// # Governor integration
///
/// Every method that issues HTTP requests admits them through
/// `ctx.governor.acquire`, one permit per request. For most operations that
/// happens in provided front-door methods that then delegate to a `*_raw`
/// required method ([`Storage::copy_object`] → [`Storage::copy_object_raw`],
/// etc.), so implementations don't need to (and must not) consult the
/// governor themselves. The exception is [`Storage::put_object`]: it stays
/// a required method and acquires inside each implementation, because
/// multipart uploads fan out into several requests only the implementation
/// can see.
#[async_trait]
#[typetag::serde(tag = "type")]
pub trait Storage: fmt::Debug + Display + sealed::Sealed + Sync + Send {
    /// Return structured metadata about this storage backend for display/repr.
    fn storage_info(&self) -> StorageInfo;

    async fn default_settings(&self) -> StorageResult<Settings> {
        Ok(Default::default())
    }

    async fn can_write(&self) -> StorageResult<bool>;

    /// Whether a repository may be created at this storage location.
    async fn can_create_repository(&self) -> StorageResult<RepositoryCreation> {
        Ok(RepositoryCreation::Allowed)
    }

    /// Ensure the storage location is ready to receive writes.
    ///
    /// Called by [`Repository::create`] before any other I/O. The default
    /// implementation is a no-op; backends that need to materialize the
    /// location (e.g. the local filesystem creating the directory) override
    /// this. Read paths like `open` never call it, so a missing location
    /// stays missing.
    async fn create_location_if_needed(&self) -> StorageResult<()> {
        Ok(())
    }

    async fn get_object(
        &self,
        ctx: &StorageContext<'_>,
        path: &str,
        target: ObjectRange<'_>,
    ) -> StorageResult<(Pin<Box<dyn AsyncBufRead + Send>>, VersionInfo)> {
        match target {
            ObjectRange::Ranged(range) => {
                self.get_object_concurrently(ctx, path, range).await
            }
            ObjectRange::Whole(_) => self.get_object_range_read(ctx, path, target).await,
        }
    }

    /// Issue a single governed GET.
    ///
    /// The request permit rides inside the returned [`PermitTrackedReader`]
    /// and reports the outcome to the governor when the reader hits EOF or
    /// is dropped.
    async fn get_object_range_read(
        &self,
        ctx: &StorageContext<'_>,
        path: &str,
        target: ObjectRange<'_>,
    ) -> StorageResult<(Pin<Box<dyn AsyncBufRead + Send>>, VersionInfo)> {
        let permit = ctx
            .governor
            .acquire(ctx.io_class(Direction::Read), target.expected_bytes())
            .await;
        match self.get_object_range(ctx, path, target).await {
            Ok((stream, version)) => {
                let reader = StreamReader::new(stream.map_err(std::io::Error::other));
                let reader = PermitTrackedReader::new(Box::pin(reader), permit);
                Ok((Box::pin(reader), version))
            }
            Err(err) => {
                permit.complete(IoOutcome { bytes: 0, result: IoResult::Error });
                Err(err)
            }
        }
    }

    /// The raw read primitive: one GET request, *not* consulting the
    /// governor. All reads ultimately flow through here; callers go through
    /// the governed [`Storage::get_object`] /
    /// [`Storage::get_object_range_read`] /
    /// [`Storage::get_object_concurrently`] front doors instead.
    ///
    /// Implementations report a whole-object response's total size through
    /// [`ObjectRange::observe_total_size`].
    async fn get_object_range(
        &self,
        ctx: &StorageContext<'_>,
        path: &str,
        target: ObjectRange<'_>,
    ) -> StorageResult<(
        Pin<Box<dyn Stream<Item = Result<Bytes, StorageError>> + Send>>,
        VersionInfo,
    )>;

    /// Write an object.
    ///
    /// Unlike the other operations, implementations acquire the governor
    /// permits themselves (see the trait-level docs): a single-shot upload
    /// acquires once, a multipart upload once per request it issues.
    async fn put_object(
        &self,
        ctx: &StorageContext<'_>,
        path: &str,
        bytes: Bytes,
        content_type: Option<&str>,
        metadata: Vec<(String, String)>,
        previous_version: Option<&VersionInfo>,
    ) -> StorageResult<VersionedUpdateResult>;

    /// Governed front door for [`Storage::copy_object_raw`].
    async fn copy_object(
        &self,
        ctx: &StorageContext<'_>,
        from: &str,
        to: &str,
        content_type: Option<&str>,
        version: &VersionInfo,
    ) -> StorageResult<VersionedUpdateResult> {
        let permit = ctx.governor.acquire(ctx.io_class(Direction::Write), None).await;
        let res = self.copy_object_raw(ctx, from, to, content_type, version).await;
        permit.complete_result(&res, 0);
        res
    }

    async fn copy_object_raw(
        &self,
        ctx: &StorageContext<'_>,
        from: &str,
        to: &str,
        content_type: Option<&str>,
        version: &VersionInfo,
    ) -> StorageResult<VersionedUpdateResult>;

    /// List objects in storage whose keys start with the given prefix.
    ///
    /// Returns a stream of [`ListInfo`] entries, each containing the object's key and size in bytes.
    /// Pass an empty prefix to list all objects in the repository's storage root.
    ///
    /// Governed front door for [`Storage::list_objects_raw`]. The governor
    /// admits one request per listing; later result pages are not metered
    /// (known v1 gap).
    async fn list_objects<'a>(
        &'a self,
        ctx: &StorageContext<'_>,
        prefix: &str,
    ) -> StorageResult<BoxStream<'a, StorageResult<ListInfo<String>>>> {
        let permit = ctx.governor.acquire(ctx.io_class(Direction::Read), None).await;
        let res = self.list_objects_raw(ctx, prefix).await;
        permit.complete_result(&res, 0);
        res
    }

    async fn list_objects_raw<'a>(
        &'a self,
        ctx: &StorageContext<'_>,
        prefix: &str,
    ) -> StorageResult<BoxStream<'a, StorageResult<ListInfo<String>>>>;

    /// Governed front door for [`Storage::delete_batch_raw`].
    async fn delete_batch(
        &self,
        ctx: &StorageContext<'_>,
        prefix: &str,
        batch: Vec<(String, u64)>,
    ) -> StorageResult<DeleteObjectsResult> {
        let permit = ctx.governor.acquire(ctx.io_class(Direction::Write), None).await;
        let res = self.delete_batch_raw(ctx, prefix, batch).await;
        permit.complete_result(&res, 0);
        res
    }

    async fn delete_batch_raw(
        &self,
        ctx: &StorageContext<'_>,
        prefix: &str,
        batch: Vec<(String, u64)>,
    ) -> StorageResult<DeleteObjectsResult>;

    /// Governed front door for [`Storage::get_object_last_modified_raw`].
    async fn get_object_last_modified(
        &self,
        ctx: &StorageContext<'_>,
        path: &str,
    ) -> StorageResult<DateTime<Utc>> {
        let permit = ctx.governor.acquire(ctx.io_class(Direction::Read), None).await;
        let res = self.get_object_last_modified_raw(ctx, path).await;
        permit.complete_result(&res, 0);
        res
    }

    async fn get_object_last_modified_raw(
        &self,
        ctx: &StorageContext<'_>,
        path: &str,
    ) -> StorageResult<DateTime<Utc>>;

    /// Governed front door for [`Storage::get_object_conditional_raw`]. On
    /// [`GetModifiedResult::Modified`] the permit rides inside the returned
    /// reader and completes at EOF.
    ///
    /// A conditional GET is always whole-object, so it takes the logical
    /// fetch's memory reservation for the response to true up
    /// ([`MemoryPermit::unmetered`] for deliberately unmetered fetches).
    async fn get_object_conditional(
        &self,
        ctx: &StorageContext<'_>,
        path: &str,
        reservation: &MemoryPermit,
        previous_version: Option<&VersionInfo>,
    ) -> StorageResult<GetModifiedResult> {
        let permit = ctx.governor.acquire(ctx.io_class(Direction::Read), None).await;
        match self
            .get_object_conditional_raw(ctx, path, reservation, previous_version)
            .await
        {
            Ok(GetModifiedResult::Modified { data, new_version }) => {
                let data = Box::pin(PermitTrackedReader::new(data, permit));
                Ok(GetModifiedResult::Modified { data, new_version })
            }
            res => {
                permit.complete_result(&res, 0);
                res
            }
        }
    }

    async fn get_object_conditional_raw(
        &self,
        ctx: &StorageContext<'_>,
        path: &str,
        reservation: &MemoryPermit,
        previous_version: Option<&VersionInfo>,
    ) -> StorageResult<GetModifiedResult>;

    /// Delete a stream of objects, by their id string representations
    /// Input stream includes sizes to get as result the total number of bytes deleted
    #[instrument(skip(self, ctx, ids))]
    async fn delete_objects(
        &self,
        ctx: &StorageContext<'_>,
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
                        .delete_batch(ctx, prefix, batch)
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

    async fn root_is_clean(&self, ctx: &StorageContext<'_>) -> StorageResult<bool> {
        match self.list_objects(ctx, "").await {
            Ok(mut stream) => match stream.next().await {
                None => Ok(true),
                Some(Ok(_)) => Ok(false),
                Some(Err(err)) => Err(err),
            },
            Err(StorageError { kind: StorageErrorKind::ObjectNotFound, .. }) => Ok(true),
            Err(err) => Err(err),
        }
    }

    async fn get_object_concurrently_multiple(
        &self,
        ctx: &StorageContext<'_>,
        key: &str,
        parts: Vec<Range<u64>>,
    ) -> StorageResult<(Pin<Box<dyn AsyncBufRead + Send>>, VersionInfo)> {
        let settings2 = ctx.settings.clone();
        let governor2 = Arc::clone(ctx.governor);
        let asset = ctx.asset;
        let key2 = key.to_string();
        let results = parts
            .into_iter()
            .map(move |range| {
                let key = key2.clone();
                let settings = settings2.clone();
                let governor = Arc::clone(&governor2);
                async move {
                    let ctx = StorageContext {
                        settings: &settings,
                        governor: &governor,
                        asset,
                    };
                    let expected = range.end - range.start;
                    let permit = governor
                        .acquire(ctx.io_class(Direction::Read), Some(expected))
                        .await;
                    // an early `?` drops the permit, reporting an abort
                    let (stream, version) = self
                        .get_object_range(&ctx, key.as_ref(), ObjectRange::Ranged(&range))
                        .await?;
                    let all_bytes: Vec<_> = stream.try_collect().await?;
                    let bytes = all_bytes.iter().map(|b| b.len() as u64).sum();
                    permit.complete(IoOutcome { bytes, result: IoResult::Ok });
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
        ctx: &StorageContext<'_>,
        key: &str,
        range: &Range<u64>,
    ) -> StorageResult<(Pin<Box<dyn AsyncBufRead + Send>>, VersionInfo)> {
        let parts = split_in_multiple_requests(
            range,
            ctx.settings.concurrency().ideal_concurrent_request_size().get(),
            ctx.settings.concurrency().max_concurrent_requests_for_object().get(),
        )
        .collect::<Vec<_>>();

        let res: (Pin<Box<dyn AsyncBufRead + Send>>, VersionInfo) = match parts.len() {
            0 => (Box::pin(tokio::io::empty()), VersionInfo::for_creation()),
            1 => self.get_object_range_read(ctx, key, ObjectRange::Ranged(range)).await?,
            _ => self.get_object_concurrently_multiple(ctx, key, parts).await?,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::governor::PermitState;
    use futures::executor::block_on;
    use tokio::io::{AsyncBufReadExt as _, AsyncReadExt as _};

    /// What the permit reported: `Ok(outcome)` for complete, `Err(())` for abort.
    type Report = Arc<Mutex<Option<Result<IoOutcome, ()>>>>;

    #[derive(Debug)]
    struct Recorder(Report);

    impl PermitState for Recorder {
        fn complete(self: Box<Self>, outcome: IoOutcome) {
            *self.0.lock().unwrap() = Some(Ok(outcome));
        }
        fn abort(self: Box<Self>) {
            *self.0.lock().unwrap() = Some(Err(()));
        }
    }

    fn tracked_reader(
        chunks: Vec<StorageResult<Bytes>>,
    ) -> (PermitTrackedReader, Report) {
        let report = Report::default();
        let permit = IoPermit::new(Box::new(Recorder(Arc::clone(&report))));
        let stream = stream::iter(chunks).map_err(std::io::Error::other);
        let reader =
            PermitTrackedReader::new(Box::pin(StreamReader::new(stream)), permit);
        (reader, report)
    }

    fn reported(report: &Report) -> Option<Result<IoOutcome, ()>> {
        *report.lock().unwrap()
    }

    #[test]
    fn tracked_reader_completes_at_eof() {
        block_on(async {
            let (mut reader, report) = tracked_reader(vec![
                Ok(Bytes::from_static(b"hello ")),
                Ok(Bytes::from_static(b"world")),
            ]);
            let mut data = Vec::new();
            reader.read_to_end(&mut data).await.unwrap();
            assert_eq!(data, b"hello world");
            let outcome = reported(&report).unwrap().unwrap();
            assert_eq!(outcome.bytes, 11);
            assert_eq!(outcome.result, IoResult::Ok);

            // dropping after EOF must not overwrite the completion with an abort
            drop(reader);
            assert!(reported(&report).unwrap().is_ok());
        });
    }

    #[test]
    fn tracked_reader_drop_before_eof_aborts() {
        block_on(async {
            let (mut reader, report) =
                tracked_reader(vec![Ok(Bytes::from_static(b"hello world"))]);
            let mut buf = [0u8; 4];
            reader.read_exact(&mut buf).await.unwrap();
            assert_eq!(reported(&report), None, "must not report before EOF");
            drop(reader);
            assert_eq!(reported(&report), Some(Err(())));
        });
    }

    #[test]
    fn tracked_reader_error_completes_with_error() {
        block_on(async {
            let (mut reader, report) = tracked_reader(vec![
                Ok(Bytes::from_static(b"abc")),
                Err(other_error("stream broke")),
            ]);
            let mut data = Vec::new();
            reader.read_to_end(&mut data).await.unwrap_err();
            let outcome = reported(&report).unwrap().unwrap();
            assert_eq!(outcome.bytes, 3);
            assert_eq!(outcome.result, IoResult::Error);
        });
    }

    #[test]
    fn tracked_reader_counts_bufread_consumption() {
        block_on(async {
            let (mut reader, report) = tracked_reader(vec![
                Ok(Bytes::from_static(b"hello ")),
                Ok(Bytes::from_static(b"world")),
            ]);
            // read_until with an absent delimiter drives poll_fill_buf/consume
            let mut data = Vec::new();
            reader.read_until(0, &mut data).await.unwrap();
            assert_eq!(data, b"hello world");
            let outcome = reported(&report).unwrap().unwrap();
            assert_eq!(outcome.bytes, 11);
            assert_eq!(outcome.result, IoResult::Ok);
        });
    }
}
