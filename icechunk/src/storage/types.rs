use bytes::{Buf, Bytes};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::{
    io::Read,
    num::{NonZeroU16, NonZeroU64},
    sync::OnceLock,
};
use tokio::io::AsyncRead;
#[cfg(not(target_arch = "wasm32"))]
use tokio_util::io::SyncIoBridge;

use super::StorageResult;

pub const SNAPSHOT_PREFIX: &str = "snapshots/";
pub const MANIFEST_PREFIX: &str = "manifests/";
pub const CHUNK_PREFIX: &str = "chunks/";
pub const REF_PREFIX: &str = "refs";
pub const TRANSACTION_PREFIX: &str = "transactions/";
pub const CONFIG_PATH: &str = "config.yaml";

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
                    .map_err(super::StorageErrorKind::IOError)?;
                Ok(buffer.into())
            }
            Reader::Synchronous(mut buf) => Ok(buf.copy_to_bytes(buf.remaining())),
        }
    }

    /// Notice this Read can only be used in non async contexts, for example, calling tokio::task::spawn_blocking
    pub fn into_read(self) -> Box<dyn Read + Unpin + Send> {
        match self {
            #[cfg(not(target_arch = "wasm32"))]
            Reader::Asynchronous(read) => Box::new(SyncIoBridge::new(read)),
            #[cfg(target_arch = "wasm32")]
            Reader::Asynchronous(_) => panic!("SyncIoBridge not available on WASM"),
            Reader::Synchronous(buf) => Box::new(buf.reader()),
        }
    }
}

#[derive(Debug)]
pub struct ListInfo<Id> {
    pub id: Id,
    pub created_at: DateTime<Utc>,
    pub size_bytes: u64,
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