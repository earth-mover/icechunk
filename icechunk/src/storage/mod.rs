use ::object_store::gcp::{GoogleCloudStorageBuilder, GoogleConfigKey};
use aws_sdk_s3::{
    config::http::HttpResponse,
    error::SdkError,
    operation::{
        delete_objects::DeleteObjectsError, get_object::GetObjectError,
        list_objects_v2::ListObjectsV2Error, put_object::PutObjectError,
    },
    primitives::ByteStreamError,
};
use chrono::{DateTime, Utc};
use core::fmt;
use futures::{stream::BoxStream, Stream, StreamExt, TryStreamExt};
use object_store::ObjectStorageConfig;
use s3::S3Storage;
use std::{
    cmp::{max, min},
    collections::HashMap,
    ffi::OsString,
    iter,
    num::{NonZeroU16, NonZeroU64},
    path::Path,
    sync::Arc,
};
use tokio::task::JoinError;

use async_trait::async_trait;
use bytes::Bytes;
use thiserror::Error;

pub mod caching;

#[cfg(test)]
pub mod logging;

pub mod object_store;
pub mod s3;

pub use caching::MemCachingStorage;
pub use object_store::ObjectStorage;

use crate::{
    config::{GcsCredentials, GcsStaticCredentials, S3Credentials, S3Options},
    format::{
        attributes::AttributesTable, manifest::Manifest, snapshot::Snapshot,
        transaction_log::TransactionLog, AttributesId, ByteRange, ChunkId, ManifestId,
        SnapshotId,
    },
    private,
};

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("error contacting object store {0}")]
    ObjectStore(#[from] ::object_store::Error),
    #[error("bad object store prefix {0:?}")]
    BadPrefix(OsString),
    #[error("error getting object from object store {0}")]
    S3GetObjectError(#[from] SdkError<GetObjectError, HttpResponse>),
    #[error("error writing object to object store {0}")]
    S3PutObjectError(#[from] SdkError<PutObjectError, HttpResponse>),
    #[error("error listing objects in object store {0}")]
    S3ListObjectError(#[from] SdkError<ListObjectsV2Error, HttpResponse>),
    #[error("error deleting objects in object store {0}")]
    S3DeleteObjectError(#[from] SdkError<DeleteObjectsError, HttpResponse>),
    #[error("error streaming bytes from object store {0}")]
    S3StreamError(#[from] ByteStreamError),
    #[error("messagepack decode error: {0}")]
    MsgPackDecodeError(#[from] rmp_serde::decode::Error),
    #[error("messagepack encode error: {0}")]
    MsgPackEncodeError(#[from] rmp_serde::encode::Error),
    #[error("cannot overwrite ref: {0}")]
    RefAlreadyExists(String),
    #[error("ref not found: {0}")]
    RefNotFound(String),
    #[error("the etag does not match")]
    ConfigUpdateConflict,
    #[error("a concurrent task failed {0}")]
    ConcurrencyError(JoinError),
    #[error("unknown storage error: {0}")]
    Other(String),
}

pub type StorageResult<A> = Result<A, StorageError>;

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

/// Fetch and write the parquet files that represent the repository in object store
///
/// Different implementation can cache the files differently, or not at all.
/// Implementations are free to assume files are never overwritten.
#[async_trait]
#[typetag::serde(tag = "type")]
pub trait Storage: fmt::Debug + private::Sealed + Sync + Send {
    async fn fetch_config(&self) -> StorageResult<Option<(Bytes, ETag)>>;
    async fn update_config(
        &self,
        config: Bytes,
        etag: Option<&str>,
    ) -> StorageResult<ETag>;
    async fn fetch_snapshot(&self, id: &SnapshotId) -> StorageResult<Arc<Snapshot>>;
    async fn fetch_attributes(
        &self,
        id: &AttributesId,
    ) -> StorageResult<Arc<AttributesTable>>; // FIXME: format flags
    async fn fetch_manifests(
        &self,
        id: &ManifestId,
        size: u64,
    ) -> StorageResult<Arc<Manifest>>; // FIXME: format flags
    async fn fetch_chunk(&self, id: &ChunkId, range: &ByteRange) -> StorageResult<Bytes>; // FIXME: format flags
    async fn fetch_transaction_log(
        &self,
        id: &SnapshotId,
    ) -> StorageResult<Arc<TransactionLog>>; // FIXME: format flags

    async fn write_snapshot(
        &self,
        id: SnapshotId,
        table: Arc<Snapshot>,
    ) -> StorageResult<()>;
    async fn write_attributes(
        &self,
        id: AttributesId,
        table: Arc<AttributesTable>,
    ) -> StorageResult<()>;
    async fn write_manifests(
        &self,
        id: ManifestId,
        table: Arc<Manifest>,
    ) -> StorageResult<u64>;
    async fn write_chunk(&self, id: ChunkId, bytes: Bytes) -> StorageResult<()>;
    async fn write_transaction_log(
        &self,
        id: SnapshotId,
        log: Arc<TransactionLog>,
    ) -> StorageResult<()>;

    async fn get_ref(&self, ref_key: &str) -> StorageResult<Bytes>;
    async fn ref_names(&self) -> StorageResult<Vec<String>>;
    async fn ref_versions(
        &self,
        ref_name: &str,
    ) -> StorageResult<BoxStream<StorageResult<String>>>;
    async fn write_ref(
        &self,
        ref_key: &str,
        overwrite_refs: bool,
        bytes: Bytes,
    ) -> StorageResult<()>;

    async fn list_objects<'a>(
        &'a self,
        prefix: &str,
    ) -> StorageResult<BoxStream<'a, StorageResult<ListInfo<String>>>>;

    /// Delete a stream of objects, by their id string representations
    async fn delete_objects(
        &self,
        prefix: &str,
        ids: BoxStream<'_, String>,
    ) -> StorageResult<usize>;

    async fn list_chunks(
        &self,
    ) -> StorageResult<BoxStream<StorageResult<ListInfo<ChunkId>>>> {
        Ok(translate_list_infos(self.list_objects(CHUNK_PREFIX).await?))
    }

    async fn list_manifests(
        &self,
    ) -> StorageResult<BoxStream<StorageResult<ListInfo<ManifestId>>>> {
        Ok(translate_list_infos(self.list_objects(MANIFEST_PREFIX).await?))
    }

    async fn list_snapshots(
        &self,
    ) -> StorageResult<BoxStream<StorageResult<ListInfo<SnapshotId>>>> {
        Ok(translate_list_infos(self.list_objects(SNAPSHOT_PREFIX).await?))
    }

    async fn delete_chunks(
        &self,
        chunks: BoxStream<'_, ChunkId>,
    ) -> StorageResult<usize> {
        self.delete_objects(CHUNK_PREFIX, chunks.map(|id| id.to_string()).boxed()).await
    }

    async fn delete_manifests(
        &self,
        chunks: BoxStream<'_, ManifestId>,
    ) -> StorageResult<usize> {
        self.delete_objects(MANIFEST_PREFIX, chunks.map(|id| id.to_string()).boxed())
            .await
    }

    async fn delete_snapshots(
        &self,
        chunks: BoxStream<'_, SnapshotId>,
    ) -> StorageResult<usize> {
        self.delete_objects(SNAPSHOT_PREFIX, chunks.map(|id| id.to_string()).boxed())
            .await
    }

    async fn delete_refs(&self, refs: BoxStream<'_, String>) -> StorageResult<usize> {
        self.delete_objects(REF_PREFIX, refs).await
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
/// Returns tuples of (offset, size) for each request. It tries to generate the maximum number of
/// requests possible, not generating more than `max_parts` requests, and each request not being
/// smaller than `min_part_size`. Note that the size of the last request is >= the preceding one.
fn split_in_multiple_requests(
    size: u64,
    min_part_size: u64,
    max_parts: u16,
) -> impl Iterator<Item = (u64, u64)> {
    let min_part_size = max(min_part_size, 1);
    let num_parts = size / min_part_size;
    let num_parts = max(1, min(num_parts, max_parts as u64));
    let equal_parts = num_parts - 1;
    let equal_parts_size = size / num_parts;
    let last_part_size = size - equal_parts * equal_parts_size;
    let equal_requests =
        iter::successors(Some((0, equal_parts_size)), move |(off, _)| {
            Some((off + equal_parts_size, equal_parts_size))
        });
    let last_request = iter::once((equal_parts * equal_parts_size, last_part_size));
    equal_requests.take(equal_parts as usize).chain(last_request)
}

pub fn new_s3_storage(
    config: S3Options,
    bucket: String,
    prefix: Option<String>,
    credentials: Option<S3Credentials>,
    max_concurrent_requests_for_object: Option<NonZeroU16>,
    min_concurrent_request_size: Option<NonZeroU64>,
) -> StorageResult<Arc<dyn Storage>> {
    let st = S3Storage::new(
        config,
        bucket,
        prefix,
        credentials.unwrap_or(S3Credentials::FromEnv),
        max_concurrent_requests_for_object,
        min_concurrent_request_size,
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

pub fn new_gcs_storage(
    bucket: String,
    prefix: Option<String>,
    credentials: Option<GcsCredentials>,
    config: Option<HashMap<String, String>>,
    max_concurrent_requests_for_object: Option<u16>,
    min_concurrent_request_size: Option<u64>,
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
                    StorageError::Other("invalid service account path".to_string())
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
                    StorageError::Other(
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
        // AWS recommendations: https://docs.aws.amazon.com/whitepapers/latest/s3-optimizing-performance-best-practices/horizontal-scaling-and-request-parallelization-for-high-throughput.html
        // 8-16 MB requests
        // 85-90 MB/s per request
        // these numbers would saturate a 12.5 Gbps network
        max_concurrent_requests_for_object: max_concurrent_requests_for_object
            .unwrap_or(18),
        min_concurrent_request_size: min_concurrent_request_size
            .unwrap_or(12 * 1024 * 1024),
        options,
    };

    Ok(Arc::new(ObjectStorage::from_config(config)?))
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {

    use super::*;
    use itertools::Itertools;
    use proptest::prelude::*;

    proptest! {
        #![proptest_config(ProptestConfig {
            cases: 999, .. ProptestConfig::default()
        })]
        #[test]
        fn test_split_requests(size in 1..3_000_000_000u64, min_part_size in 0..16_000_000u64, max_parts in 1..100u16 ) {
            let res: Vec<_> = split_in_multiple_requests(size, min_part_size, max_parts).collect();

            // there is always at least 1 request
            prop_assert!(!res.is_empty());

            // it does as many requests as possible
            prop_assert!(res.len() as u64 >= min(max_parts as u64, size / min_part_size));

            // there are never more than max_parts requests
            prop_assert!(res.len() <= max_parts as usize);

            // the request sizes add up to total size
            prop_assert_eq!(res.iter().map(|(_, size)| size).sum::<u64>(), size);

            // no more than one request smaller than minimum size
            prop_assert!(res.iter().filter(|(_, size)| *size < min_part_size).count() <= 1);

            // if there is a request smaller than the minimum size it is because the total size is
            // smaller than minimum request size
            if res.iter().any(|(_, size)| *size < min_part_size) {
                prop_assert!(size < min_part_size)
            }

            // there are only two request sizes
            let counts = res.iter().map(|(_, size)| size).counts();
            prop_assert!(counts.len() <= 2); // only last element is smaller
            if counts.len() > 1 {
                // the smaller request size happens only once
                prop_assert_eq!(counts.values().min(), Some(&1usize));
            }

            // there are no holes in the requests, nor bytes that are requested more than once
            let mut iter = res.iter();
            iter.next();
            prop_assert!(res.iter().zip(iter).all(|((off1,size),(off2,_))| off1 + size == *off2));
        }

    }

    #[test]
    fn test_split_examples() {
        assert_eq!(
            split_in_multiple_requests(4, 4, 100,).collect::<Vec<_>>(),
            vec![(0, 4)]
        );
        assert_eq!(
            split_in_multiple_requests(3, 1, 100,).collect::<Vec<_>>(),
            vec![(0, 1), (1, 1), (2, 1),]
        );
        assert_eq!(
            split_in_multiple_requests(6, 5, 100,).collect::<Vec<_>>(),
            vec![(0, 6)]
        );
        assert_eq!(
            split_in_multiple_requests(11, 5, 100,).collect::<Vec<_>>(),
            vec![(0, 5), (5, 6)]
        );
        assert_eq!(
            split_in_multiple_requests(13, 5, 2,).collect::<Vec<_>>(),
            vec![(0, 6), (6, 7)]
        );
        assert_eq!(
            split_in_multiple_requests(100, 5, 3,).collect::<Vec<_>>(),
            vec![(0, 33), (33, 33), (66, 34)]
        );
    }
}
