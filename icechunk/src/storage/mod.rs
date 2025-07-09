use ::object_store::{azure::AzureConfigKey, gcp::GoogleConfigKey};
use aws_sdk_s3::{
    config::http::HttpResponse,
    error::SdkError,
    operation::{
        complete_multipart_upload::CompleteMultipartUploadError,
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
    stream::{BoxStream, FuturesOrdered},
};
use itertools::Itertools;
use s3::S3Storage;
use serde::{Deserialize, Serialize};
use std::{
    cmp::{max, min},
    collections::HashMap,
    ffi::OsString,
    io::Read,
    iter,
    num::{NonZeroU16, NonZeroU64},
    ops::Range,
    path::Path,
    sync::{Arc, Mutex, OnceLock},
};
use tokio::io::AsyncRead;
use tokio_util::io::SyncIoBridge;
use tracing::{instrument, warn};

use async_trait::async_trait;
use bytes::{Buf, Bytes};
use thiserror::Error;

#[cfg(test)]
pub mod logging;

pub mod object_store;
pub mod s3;

pub use object_store::ObjectStorage;

use crate::{
    config::{AzureCredentials, GcsCredentials, S3Credentials, S3Options},
    error::ICError,
    format::{ChunkId, ChunkOffset, ManifestId, SnapshotId},
    private,
};

#[derive(Debug, Error)]
pub enum StorageErrorKind {
    #[error("object store error {0}")]
    ObjectStore(#[from] Box<::object_store::Error>),
    #[error("bad object store prefix {0:?}")]
    BadPrefix(OsString),
    #[error("error getting object from object store {0}")]
    S3GetObjectError(#[from] Box<SdkError<GetObjectError, HttpResponse>>),
    #[error("error writing object to object store {0}")]
    S3PutObjectError(#[from] Box<SdkError<PutObjectError, HttpResponse>>),
    #[error("error creating multipart upload {0}")]
    S3CreateMultipartUploadError(
        #[from] Box<SdkError<CreateMultipartUploadError, HttpResponse>>,
    ),
    #[error("error uploading multipart part {0}")]
    S3UploadPartError(#[from] Box<SdkError<UploadPartError, HttpResponse>>),
    #[error("error completing multipart upload {0}")]
    S3CompleteMultipartUploadError(
        #[from] Box<SdkError<CompleteMultipartUploadError, HttpResponse>>,
    ),
    #[error("error getting object metadata from object store {0}")]
    S3HeadObjectError(#[from] Box<SdkError<HeadObjectError, HttpResponse>>),
    #[error("error listing objects in object store {0}")]
    S3ListObjectError(#[from] Box<SdkError<ListObjectsV2Error, HttpResponse>>),
    #[error("error deleting objects in object store {0}")]
    S3DeleteObjectError(#[from] Box<SdkError<DeleteObjectsError, HttpResponse>>),
    #[error("error streaming bytes from object store {0}")]
    S3StreamError(#[from] Box<ByteStreamError>),
    #[error("I/O error: {0}")]
    IOError(#[from] std::io::Error),
    #[error("storage configuration error: {0}")]
    R2ConfigurationError(String),
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
pub struct ListInfo<Id> {
    pub id: Id,
    pub created_at: DateTime<Utc>,
    pub size_bytes: u64,
}

const SNAPSHOT_PREFIX: &str = "snapshots/";
const MANIFEST_PREFIX: &str = "manifests/";
// const ATTRIBUTES_PREFIX: &str = "attributes/";
const CHUNK_PREFIX: &str = "chunks/";
const REF_PREFIX: &str = "refs";
const TRANSACTION_PREFIX: &str = "transactions/";
const CONFIG_PATH: &str = "config.yaml";

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

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone, Default)]
pub struct Settings {
    pub concurrency: Option<ConcurrencySettings>,
    pub retries: Option<RetriesSettings>,
    pub unsafe_use_conditional_update: Option<bool>,
    pub unsafe_use_conditional_create: Option<bool>,
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

pub enum Reader {
    Asynchronous(Box<dyn AsyncRead + Unpin + Send>),
    Synchronous(Box<dyn Buf + Unpin + Send>),
}

impl Reader {
    pub async fn to_bytes(self, expected_size: usize) -> StorageResult<Bytes> {
        match self {
            Reader::Asynchronous(mut read) => {
                // add some extra space to the buffer to optimize conversion to bytes
                let mut buffer = Vec::with_capacity(expected_size + 16);
                tokio::io::copy(&mut read, &mut buffer)
                    .await
                    .map_err(StorageErrorKind::IOError)?;
                Ok(buffer.into())
            }
            Reader::Synchronous(mut buf) => Ok(buf.copy_to_bytes(buf.remaining())),
        }
    }

    /// Notice this Read can only be used in non async contexts, for example, calling tokio::task::spawn_blocking
    pub fn into_read(self) -> Box<dyn Read + Unpin + Send> {
        match self {
            Reader::Asynchronous(read) => Box::new(SyncIoBridge::new(read)),
            Reader::Synchronous(buf) => Box::new(buf.reader()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FetchConfigResult {
    Found { bytes: Bytes, version: VersionInfo },
    NotFound,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UpdateConfigResult {
    Updated { new_version: VersionInfo },
    NotOnLatestVersion,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GetRefResult {
    Found { bytes: Bytes, version: VersionInfo },
    NotFound,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WriteRefResult {
    Written,
    WontOverwrite,
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
#[typetag::serde(tag = "type")]
pub trait Storage: fmt::Debug + fmt::Display + private::Sealed + Sync + Send {
    fn default_settings(&self) -> Settings {
        Default::default()
    }

    fn can_write(&self) -> bool;

    async fn fetch_config(&self, settings: &Settings)
    -> StorageResult<FetchConfigResult>;
    async fn update_config(
        &self,
        settings: &Settings,
        config: Bytes,
        previous_version: &VersionInfo,
    ) -> StorageResult<UpdateConfigResult>;
    async fn fetch_snapshot(
        &self,
        settings: &Settings,
        id: &SnapshotId,
    ) -> StorageResult<Box<dyn AsyncRead + Unpin + Send>>;
    /// Returns whatever reader is more efficient.
    ///
    /// For example, if processed with multiple requests, it will return a synchronous `Buf`
    /// instance pointing the different parts. If it was executed in a single request, it's more
    /// efficient to return the network `AsyncRead` directly
    async fn fetch_manifest_known_size(
        &self,
        settings: &Settings,
        id: &ManifestId,
        size: u64,
    ) -> StorageResult<Reader>;
    async fn fetch_manifest_unknown_size(
        &self,
        settings: &Settings,
        id: &ManifestId,
    ) -> StorageResult<Box<dyn AsyncRead + Unpin + Send>>;
    async fn fetch_chunk(
        &self,
        settings: &Settings,
        id: &ChunkId,
        range: &Range<ChunkOffset>,
    ) -> StorageResult<Bytes>; // FIXME: format flags
    async fn fetch_transaction_log(
        &self,
        settings: &Settings,
        id: &SnapshotId,
    ) -> StorageResult<Box<dyn AsyncRead + Unpin + Send>>;

    async fn write_snapshot(
        &self,
        settings: &Settings,
        id: SnapshotId,
        metadata: Vec<(String, String)>,
        bytes: Bytes,
    ) -> StorageResult<()>;
    async fn write_manifest(
        &self,
        settings: &Settings,
        id: ManifestId,
        metadata: Vec<(String, String)>,
        bytes: Bytes,
    ) -> StorageResult<()>;
    async fn write_chunk(
        &self,
        settings: &Settings,
        id: ChunkId,
        bytes: Bytes,
    ) -> StorageResult<()>;
    async fn write_transaction_log(
        &self,
        settings: &Settings,
        id: SnapshotId,
        metadata: Vec<(String, String)>,
        bytes: Bytes,
    ) -> StorageResult<()>;

    async fn get_ref(
        &self,
        settings: &Settings,
        ref_key: &str,
    ) -> StorageResult<GetRefResult>;
    async fn ref_names(&self, settings: &Settings) -> StorageResult<Vec<String>>;
    async fn write_ref(
        &self,
        settings: &Settings,
        ref_key: &str,
        bytes: Bytes,
        previous_version: &VersionInfo,
    ) -> StorageResult<WriteRefResult>;

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

    async fn get_snapshot_last_modified(
        &self,
        settings: &Settings,
        snapshot: &SnapshotId,
    ) -> StorageResult<DateTime<Utc>>;

    async fn root_is_clean(&self) -> StorageResult<bool> {
        match self.list_objects(&Settings::default(), "").await?.next().await {
            None => Ok(true),
            Some(Ok(_)) => Ok(false),
            Some(Err(err)) => Err(err),
        }
    }

    async fn list_chunks(
        &self,
        settings: &Settings,
    ) -> StorageResult<BoxStream<StorageResult<ListInfo<ChunkId>>>> {
        Ok(translate_list_infos(self.list_objects(settings, CHUNK_PREFIX).await?))
    }

    async fn list_manifests(
        &self,
        settings: &Settings,
    ) -> StorageResult<BoxStream<StorageResult<ListInfo<ManifestId>>>> {
        Ok(translate_list_infos(self.list_objects(settings, MANIFEST_PREFIX).await?))
    }

    async fn list_snapshots(
        &self,
        settings: &Settings,
    ) -> StorageResult<BoxStream<StorageResult<ListInfo<SnapshotId>>>> {
        Ok(translate_list_infos(self.list_objects(settings, SNAPSHOT_PREFIX).await?))
    }

    async fn list_transaction_logs(
        &self,
        settings: &Settings,
    ) -> StorageResult<BoxStream<StorageResult<ListInfo<SnapshotId>>>> {
        Ok(translate_list_infos(self.list_objects(settings, TRANSACTION_PREFIX).await?))
    }

    async fn delete_chunks(
        &self,
        settings: &Settings,
        chunks: BoxStream<'_, (ChunkId, u64)>,
    ) -> StorageResult<DeleteObjectsResult> {
        self.delete_objects(
            settings,
            CHUNK_PREFIX,
            chunks.map(|(id, size)| (id.to_string(), size)).boxed(),
        )
        .await
    }

    async fn delete_manifests(
        &self,
        settings: &Settings,
        manifests: BoxStream<'_, (ManifestId, u64)>,
    ) -> StorageResult<DeleteObjectsResult> {
        self.delete_objects(
            settings,
            MANIFEST_PREFIX,
            manifests.map(|(id, size)| (id.to_string(), size)).boxed(),
        )
        .await
    }

    async fn delete_snapshots(
        &self,
        settings: &Settings,
        snapshots: BoxStream<'_, (SnapshotId, u64)>,
    ) -> StorageResult<DeleteObjectsResult> {
        self.delete_objects(
            settings,
            SNAPSHOT_PREFIX,
            snapshots.map(|(id, size)| (id.to_string(), size)).boxed(),
        )
        .await
    }

    async fn delete_transaction_logs(
        &self,
        settings: &Settings,
        transaction_logs: BoxStream<'_, (SnapshotId, u64)>,
    ) -> StorageResult<DeleteObjectsResult> {
        self.delete_objects(
            settings,
            TRANSACTION_PREFIX,
            transaction_logs.map(|(id, size)| (id.to_string(), size)).boxed(),
        )
        .await
    }

    async fn delete_refs(
        &self,
        settings: &Settings,
        refs: BoxStream<'_, String>,
    ) -> StorageResult<u64> {
        let refs = refs.map(|s| (s, 0)).boxed();
        Ok(self.delete_objects(settings, REF_PREFIX, refs).await?.deleted_objects)
    }

    async fn get_object_range_buf(
        &self,
        settings: &Settings,
        key: &str,
        range: &Range<u64>,
    ) -> StorageResult<Box<dyn Buf + Unpin + Send>>;

    async fn get_object_range_read(
        &self,
        settings: &Settings,
        key: &str,
        range: &Range<u64>,
    ) -> StorageResult<Box<dyn AsyncRead + Unpin + Send>>;

    async fn get_object_concurrently_multiple(
        &self,
        settings: &Settings,
        key: &str,
        parts: Vec<Range<u64>>,
    ) -> StorageResult<Box<dyn Buf + Send + Unpin>> {
        let results =
            parts
                .into_iter()
                .map(|range| async move {
                    self.get_object_range_buf(settings, key, &range).await
                })
                .collect::<FuturesOrdered<_>>();

        let init: Box<dyn Buf + Unpin + Send> = Box::new(&[][..]);
        let buf = results
            .try_fold(init, |prev, buf| async {
                let res: Box<dyn Buf + Unpin + Send> = Box::new(prev.chain(buf));
                Ok(res)
            })
            .await?;

        Ok(Box::new(buf))
    }

    async fn get_object_concurrently(
        &self,
        settings: &Settings,
        key: &str,
        range: &Range<u64>,
    ) -> StorageResult<Reader> {
        let parts = split_in_multiple_requests(
            range,
            settings.concurrency().ideal_concurrent_request_size().get(),
            settings.concurrency().max_concurrent_requests_for_object().get(),
        )
        .collect::<Vec<_>>();

        let res = match parts.len() {
            0 => Reader::Asynchronous(Box::new(tokio::io::empty())),
            1 => Reader::Asynchronous(
                self.get_object_range_read(settings, key, range).await?,
            ),
            _ => Reader::Synchronous(
                self.get_object_concurrently_multiple(settings, key, parts).await?,
            ),
        };
        Ok(res)
    }
}

fn convert_list_item<Id>(item: ListInfo<String>) -> Option<ListInfo<Id>>
where
    Id: for<'b> TryFrom<&'b str>,
{
    let id = Id::try_from(item.id.as_str()).ok()?;
    let created_at = item.created_at;
    Some(ListInfo { created_at, id, size_bytes: item.size_bytes })
}

fn translate_list_infos<'a, Id>(
    s: impl Stream<Item = StorageResult<ListInfo<String>>> + Send + 'a,
) -> BoxStream<'a, StorageResult<ListInfo<Id>>>
where
    Id: for<'b> TryFrom<&'b str> + Send + std::fmt::Debug + 'a,
{
    s.try_filter_map(|info| async move {
        let info = convert_list_item(info);
        if info.is_none() {
            tracing::error!(list_info=?info, "Error processing list item metadata");
        }
        Ok(info)
    })
    .boxed()
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

pub fn new_s3_storage(
    config: S3Options,
    bucket: String,
    prefix: Option<String>,
    credentials: Option<S3Credentials>,
) -> StorageResult<Arc<dyn Storage>> {
    if let Some(endpoint) = &config.endpoint_url {
        if endpoint.contains("fly.storage.tigris.dev") {
            return Err(StorageError::from(StorageErrorKind::Other("Tigris Storage is not S3 compatible, use the Tigris specific constructor instead".to_string())));
        }
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

pub fn new_tigris_storage(
    config: S3Options,
    bucket: String,
    prefix: Option<String>,
    credentials: Option<S3Credentials>,
    use_weak_consistency: bool,
) -> StorageResult<Arc<dyn Storage>> {
    let config = S3Options {
        endpoint_url: Some(
            config.endpoint_url.unwrap_or("https://fly.storage.tigris.dev".to_string()),
        ),
        ..config
    };
    let mut extra_write_headers = Vec::with_capacity(1);
    let mut extra_read_headers = Vec::with_capacity(2);

    if !use_weak_consistency {
        // TODO: Tigris will need more than this to offer good eventually consistent behavior
        // For example: we should use no-cache for branches and config file
        if let Some(region) = config.region.as_ref() {
            extra_write_headers.push(("X-Tigris-Region".to_string(), region.clone()));
            extra_read_headers.push(("X-Tigris-Region".to_string(), region.clone()));
            extra_read_headers
                .push(("Cache-Control".to_string(), "no-cache".to_string()));
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

pub async fn new_local_filesystem_storage(
    path: &Path,
) -> StorageResult<Arc<dyn Storage>> {
    let st = ObjectStorage::new_local_filesystem(path).await?;
    Ok(Arc::new(st))
}

pub async fn new_s3_object_store_storage(
    config: S3Options,
    bucket: String,
    prefix: Option<String>,
    credentials: Option<S3Credentials>,
) -> StorageResult<Arc<dyn Storage>> {
    if let Some(endpoint) = &config.endpoint_url {
        if endpoint.contains("fly.storage.tigris.dev") {
            return Err(StorageError::from(StorageErrorKind::Other("Tigris Storage is not S3 compatible, use the Tigris specific constructor instead".to_string())));
        }
    }
    let storage =
        ObjectStorage::new_s3(bucket, prefix, credentials, Some(config)).await?;
    Ok(Arc::new(storage))
}

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

pub async fn new_gcs_storage(
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
    let storage =
        ObjectStorage::new_gcs(bucket, prefix, credentials, Some(config)).await?;
    Ok(Arc::new(storage))
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
        .await
        .unwrap();
        let bytes = rmp_serde::to_vec(&storage).unwrap();
        let dese: Result<Arc<dyn Storage>, _> = rmp_serde::from_slice(&bytes);
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

            let sizes: Vec<_> = res.iter().map(|range| (range.end - range.start)).collect();
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
            let sizes: HashSet<_> = res.iter().map(|range| (range.end - range.start)).collect();
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
