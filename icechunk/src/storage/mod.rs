use ::object_store::{
    azure::AzureConfigKey,
    gcp::{GoogleCloudStorageBuilder, GoogleConfigKey},
};
use aws_sdk_s3::{
    config::http::HttpResponse,
    error::SdkError,
    operation::{
        delete_objects::DeleteObjectsError, get_object::GetObjectError,
        head_object::HeadObjectError, list_objects_v2::ListObjectsV2Error,
        put_object::PutObjectError,
    },
    primitives::ByteStreamError,
};
use chrono::{DateTime, Utc};
use core::fmt;
use futures::{
    stream::{BoxStream, FuturesOrdered},
    Stream, StreamExt, TryStreamExt,
};
use itertools::Itertools;
use object_store::ObjectStorageConfig;
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
    sync::{Arc, OnceLock},
};
use tokio::io::AsyncRead;
use tokio_util::io::SyncIoBridge;

use async_trait::async_trait;
use bytes::{Buf, Bytes};
use thiserror::Error;

#[cfg(test)]
pub mod logging;

pub mod object_store;
pub mod s3;

pub use object_store::ObjectStorage;

use crate::{
    config::{
        AzureCredentials, AzureStaticCredentials, GcsCredentials, GcsStaticCredentials,
        S3Credentials, S3Options,
    },
    error::ICError,
    format::{ChunkId, ChunkOffset, ManifestId, SnapshotId},
    private,
};

#[derive(Debug, Error)]
pub enum StorageErrorKind {
    #[error("object store error {0}")]
    ObjectStore(#[from] ::object_store::Error),
    #[error("bad object store prefix {0:?}")]
    BadPrefix(OsString),
    #[error("error getting object from object store {0}")]
    S3GetObjectError(#[from] SdkError<GetObjectError, HttpResponse>),
    #[error("error writing object to object store {0}")]
    S3PutObjectError(#[from] SdkError<PutObjectError, HttpResponse>),
    #[error("error getting object metadata from object store {0}")]
    S3HeadObjectError(#[from] SdkError<HeadObjectError, HttpResponse>),
    #[error("error listing objects in object store {0}")]
    S3ListObjectError(#[from] SdkError<ListObjectsV2Error, HttpResponse>),
    #[error("error deleting objects in object store {0}")]
    S3DeleteObjectError(#[from] SdkError<DeleteObjectsError, HttpResponse>),
    #[error("error streaming bytes from object store {0}")]
    S3StreamError(#[from] ByteStreamError),
    #[error("I/O error: {0}")]
    IOError(#[from] std::io::Error),
    #[error("unknown storage error: {0}")]
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
}

const SNAPSHOT_PREFIX: &str = "snapshots/";
const MANIFEST_PREFIX: &str = "manifests/";
// const ATTRIBUTES_PREFIX: &str = "attributes/";
const CHUNK_PREFIX: &str = "chunks/";
const REF_PREFIX: &str = "refs";
const TRANSACTION_PREFIX: &str = "transactions/";
const CONFIG_PATH: &str = "config.yaml";

pub type ETag = String;

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
}

static DEFAULT_CONCURRENCY: OnceLock<ConcurrencySettings> = OnceLock::new();

impl Settings {
    pub fn concurrency(&self) -> &ConcurrencySettings {
        self.concurrency
            .as_ref()
            .unwrap_or_else(|| DEFAULT_CONCURRENCY.get_or_init(Default::default))
    }

    pub fn merge(&self, other: Self) -> Self {
        Self {
            concurrency: match (&self.concurrency, other.concurrency) {
                (None, None) => None,
                (None, Some(c)) => Some(c),
                (Some(c), None) => Some(c.clone()),
                (Some(mine), Some(theirs)) => Some(mine.merge(theirs)),
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
    Found { bytes: Bytes, etag: ETag },
    NotFound,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UpdateConfigResult {
    Updated { new_etag: ETag },
    NotOnLatestVersion,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GetRefResult {
    Found { bytes: Bytes },
    NotFound,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WriteRefResult {
    Written,
    WontOverwrite,
}

/// Fetch and write the parquet files that represent the repository in object store
///
/// Different implementation can cache the files differently, or not at all.
/// Implementations are free to assume files are never overwritten.
#[async_trait]
#[typetag::serde(tag = "type")]
pub trait Storage: fmt::Debug + private::Sealed + Sync + Send {
    fn default_settings(&self) -> Settings {
        Default::default()
    }
    async fn fetch_config(&self, settings: &Settings)
        -> StorageResult<FetchConfigResult>;
    async fn update_config(
        &self,
        settings: &Settings,
        config: Bytes,
        etag: Option<&str>,
    ) -> StorageResult<UpdateConfigResult>;
    async fn fetch_snapshot(
        &self,
        settings: &Settings,
        id: &SnapshotId,
    ) -> StorageResult<Box<dyn AsyncRead + Unpin + Send>>;
    /// Returns whatever reader is more efficient.
    ///
    /// For example, if processesed with multiple requests, it will return a synchronous `Buf`
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
    async fn ref_versions(
        &self,
        settings: &Settings,
        ref_name: &str,
    ) -> StorageResult<BoxStream<StorageResult<String>>>;
    async fn write_ref(
        &self,
        settings: &Settings,
        ref_key: &str,
        overwrite_refs: bool,
        bytes: Bytes,
    ) -> StorageResult<WriteRefResult>;

    async fn list_objects<'a>(
        &'a self,
        settings: &Settings,
        prefix: &str,
    ) -> StorageResult<BoxStream<'a, StorageResult<ListInfo<String>>>>;

    /// Delete a stream of objects, by their id string representations
    async fn delete_objects(
        &self,
        settings: &Settings,
        prefix: &str,
        ids: BoxStream<'_, String>,
    ) -> StorageResult<usize>;

    async fn get_snapshot_last_modified(
        &self,
        settings: &Settings,
        snapshot: &SnapshotId,
    ) -> StorageResult<DateTime<Utc>>;

    async fn root_is_clean(&self) -> StorageResult<bool>;

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
        chunks: BoxStream<'_, ChunkId>,
    ) -> StorageResult<usize> {
        self.delete_objects(
            settings,
            CHUNK_PREFIX,
            chunks.map(|id| id.to_string()).boxed(),
        )
        .await
    }

    async fn delete_manifests(
        &self,
        settings: &Settings,
        manifests: BoxStream<'_, ManifestId>,
    ) -> StorageResult<usize> {
        self.delete_objects(
            settings,
            MANIFEST_PREFIX,
            manifests.map(|id| id.to_string()).boxed(),
        )
        .await
    }

    async fn delete_snapshots(
        &self,
        settings: &Settings,
        snapshots: BoxStream<'_, SnapshotId>,
    ) -> StorageResult<usize> {
        self.delete_objects(
            settings,
            SNAPSHOT_PREFIX,
            snapshots.map(|id| id.to_string()).boxed(),
        )
        .await
    }

    async fn delete_transaction_logs(
        &self,
        settings: &Settings,
        transaction_logs: BoxStream<'_, SnapshotId>,
    ) -> StorageResult<usize> {
        self.delete_objects(
            settings,
            TRANSACTION_PREFIX,
            transaction_logs.map(|id| id.to_string()).boxed(),
        )
        .await
    }

    async fn delete_refs(
        &self,
        settings: &Settings,
        refs: BoxStream<'_, String>,
    ) -> StorageResult<usize> {
        self.delete_objects(settings, REF_PREFIX, refs).await
    }

    async fn get_object_range_buf(
        &self,
        key: &str,
        range: &Range<u64>,
    ) -> StorageResult<Box<dyn Buf + Unpin + Send>>;

    async fn get_object_range_read(
        &self,
        key: &str,
        range: &Range<u64>,
    ) -> StorageResult<Box<dyn AsyncRead + Unpin + Send>>;

    async fn get_object_concurrently_multiple(
        &self,
        key: &str,
        parts: Vec<Range<u64>>,
    ) -> StorageResult<Box<dyn Buf + Send + Unpin>> {
        let results = parts
            .into_iter()
            .map(|range| async move { self.get_object_range_buf(key, &range).await })
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
            1 => Reader::Asynchronous(self.get_object_range_read(key, range).await?),
            _ => Reader::Synchronous(
                self.get_object_concurrently_multiple(key, parts).await?,
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
    Some(ListInfo { created_at, id })
}

fn translate_list_infos<'a, Id>(
    s: impl Stream<Item = StorageResult<ListInfo<String>>> + Send + 'a,
) -> BoxStream<'a, StorageResult<ListInfo<Id>>>
where
    Id: for<'b> TryFrom<&'b str> + Send + 'a,
{
    // FIXME: flag error, don't skip
    s.try_filter_map(|info| async move { Ok(convert_list_item(info)) }).boxed()
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
) -> impl Iterator<Item = Range<u64>> {
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

pub fn new_s3_storage(
    config: S3Options,
    bucket: String,
    prefix: Option<String>,
    credentials: Option<S3Credentials>,
) -> StorageResult<Arc<dyn Storage>> {
    let st = S3Storage::new(
        config,
        bucket,
        prefix,
        credentials.unwrap_or(S3Credentials::FromEnv),
    )?;
    Ok(Arc::new(st))
}

pub fn new_tigris_storage(
    config: S3Options,
    bucket: String,
    prefix: Option<String>,
    credentials: Option<S3Credentials>,
) -> StorageResult<Arc<dyn Storage>> {
    let config = S3Options {
        endpoint_url: Some(
            config.endpoint_url.unwrap_or("https://fly.storage.tigris.dev".to_string()),
        ),
        ..config
    };
    let st = S3Storage::new(
        config,
        bucket,
        prefix,
        credentials.unwrap_or(S3Credentials::FromEnv),
    )?;
    Ok(Arc::new(st))
}

pub fn new_in_memory_storage() -> StorageResult<Arc<dyn Storage>> {
    let st = ObjectStorage::new_in_memory()?;
    Ok(Arc::new(st))
}

pub fn new_local_filesystem_storage(path: &Path) -> StorageResult<Arc<dyn Storage>> {
    let st = ObjectStorage::new_local_filesystem(path)?;
    Ok(Arc::new(st))
}

pub fn new_azure_blob_storage(
    container: String,
    prefix: String,
    credentials: Option<AzureCredentials>,
    config: Option<HashMap<String, String>>,
) -> StorageResult<Arc<dyn Storage>> {
    let url = format!("azure://{}/{}", container, prefix);
    let mut options = config.unwrap_or_default().into_iter().collect::<Vec<_>>();
    // Either the account name should be provided or user_emulator should be set to true to use the default account
    if !options.iter().any(|(k, _)| k == AzureConfigKey::AccountName.as_ref()) {
        options
            .push((AzureConfigKey::UseEmulator.as_ref().to_string(), "true".to_string()));
    }

    match credentials {
        Some(AzureCredentials::Static(AzureStaticCredentials::AccessKey(key))) => {
            options.push((AzureConfigKey::AccessKey.as_ref().to_string(), key));
        }
        Some(AzureCredentials::Static(AzureStaticCredentials::SASToken(token))) => {
            options.push((AzureConfigKey::SasKey.as_ref().to_string(), token));
        }
        Some(AzureCredentials::Static(AzureStaticCredentials::BearerToken(token))) => {
            options.push((AzureConfigKey::Token.as_ref().to_string(), token));
        }
        None | Some(AzureCredentials::FromEnv) => {
            let builder = ::object_store::azure::MicrosoftAzureBuilder::from_env();

            for key in &[
                AzureConfigKey::AccessKey,
                AzureConfigKey::SasKey,
                AzureConfigKey::Token,
            ] {
                if let Some(value) = builder.get_config_value(key) {
                    options.push((key.as_ref().to_string(), value));
                }
            }
        }
    };

    let config = ObjectStorageConfig {
        url,
        prefix: "".to_string(), // it's embedded in the url
        options,
    };

    Ok(Arc::new(ObjectStorage::from_config(config)?))
}

pub fn new_gcs_storage(
    bucket: String,
    prefix: Option<String>,
    credentials: Option<GcsCredentials>,
    config: Option<HashMap<String, String>>,
) -> StorageResult<Arc<dyn Storage>> {
    let url = format!(
        "gs://{}{}",
        bucket,
        prefix.map(|p| format!("/{}", p)).unwrap_or("".to_string())
    );
    let mut options = config.unwrap_or_default().into_iter().collect::<Vec<_>>();

    match credentials {
        Some(GcsCredentials::Static(GcsStaticCredentials::ServiceAccount(path))) => {
            options.push((
                GoogleConfigKey::ServiceAccount.as_ref().to_string(),
                path.into_os_string().into_string().map_err(|_| {
                    StorageErrorKind::Other("invalid service account path".to_string())
                })?,
            ));
        }
        Some(GcsCredentials::Static(GcsStaticCredentials::ServiceAccountKey(key))) => {
            options.push((GoogleConfigKey::ServiceAccountKey.as_ref().to_string(), key));
        }
        Some(GcsCredentials::Static(GcsStaticCredentials::ApplicationCredentials(
            path,
        ))) => {
            options.push((
                GoogleConfigKey::ApplicationCredentials.as_ref().to_string(),
                path.into_os_string().into_string().map_err(|_| {
                    StorageErrorKind::Other(
                        "invalid application credentials path".to_string(),
                    )
                })?,
            ));
        }
        None | Some(GcsCredentials::FromEnv) => {
            let builder = GoogleCloudStorageBuilder::from_env();

            for key in &[
                GoogleConfigKey::ServiceAccount,
                GoogleConfigKey::ServiceAccountKey,
                GoogleConfigKey::ApplicationCredentials,
            ] {
                if let Some(value) = builder.get_config_value(key) {
                    options.push((key.as_ref().to_string(), value));
                }
            }
        }
    };

    let config = ObjectStorageConfig {
        url,
        prefix: "".to_string(), // it's embedded in the url
        options,
    };

    Ok(Arc::new(ObjectStorage::from_config(config)?))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::panic)]
mod tests {

    use std::collections::HashSet;

    use super::*;
    use proptest::prelude::*;

    proptest! {
        #![proptest_config(ProptestConfig {
            cases: 999, .. ProptestConfig::default()
        })]
        #[test]
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

    #[test]
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
