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
use serde::{Deserialize, Serialize};
use std::{
    cmp::{max, min},
    collections::HashMap,
    ffi::OsString,
    iter,
    num::{NonZeroU16, NonZeroU64},
    ops::Range,
    path::Path,
    sync::Arc,
};
use tokio::io::AsyncRead;

use async_trait::async_trait;
use bytes::Bytes;
use thiserror::Error;

#[cfg(test)]
pub mod logging;

pub mod object_store;
pub mod s3;

pub use object_store::ObjectStorage;

use crate::{
    config::{GcsCredentials, GcsStaticCredentials, S3Credentials, S3Options},
    format::{ChunkId, ChunkOffset, ManifestId, SnapshotId},
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
    #[error("cannot overwrite ref: {0}")]
    RefAlreadyExists(String),
    #[error("ref not found: {0}")]
    RefNotFound(String),
    #[error("the etag does not match")]
    ConfigUpdateConflict,
    #[error("I/O error: {0}")]
    IOError(#[from] std::io::Error),
    #[error("unknown storage error: {0}")]
    Other(String),
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

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone)]
pub struct ConcurrencySettings {
    pub max_concurrent_requests_for_object: NonZeroU16,
    pub min_concurrent_request_size: NonZeroU64,
}

// AWS recommendations: https://docs.aws.amazon.com/whitepapers/latest/s3-optimizing-performance-best-practices/horizontal-scaling-and-request-parallelization-for-high-throughput.html
// 8-16 MB requests
// 85-90 MB/s per request
// these numbers would saturate a 12.5 Gbps network
impl Default for ConcurrencySettings {
    fn default() -> Self {
        Self {
            max_concurrent_requests_for_object: NonZeroU16::new(18)
                .unwrap_or(NonZeroU16::MIN),
            min_concurrent_request_size: NonZeroU64::new(12 * 1024 * 1024)
                .unwrap_or(NonZeroU64::MIN),
        }
    }
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone, Default)]
pub struct Settings {
    pub concurrency: ConcurrencySettings,
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
    async fn fetch_config(
        &self,
        settings: &Settings,
    ) -> StorageResult<Option<(Bytes, ETag)>>;
    async fn update_config(
        &self,
        settings: &Settings,
        config: Bytes,
        etag: Option<&str>,
    ) -> StorageResult<ETag>;
    async fn fetch_snapshot(
        &self,
        settings: &Settings,
        id: &SnapshotId,
    ) -> StorageResult<Box<dyn AsyncRead + Unpin + Send>>;
    async fn fetch_manifest(
        &self,
        settings: &Settings,
        id: &ManifestId,
        size: u64,
    ) -> StorageResult<Box<dyn AsyncRead + Unpin + Send>> {
        if size == 0 {
            self.fetch_manifest_single_request(settings, id).await
        } else {
            self.fetch_manifest_splitting(settings, id, size).await
        }
    }
    async fn fetch_manifest_splitting(
        &self,
        settings: &Settings,
        id: &ManifestId,
        size: u64,
    ) -> StorageResult<Box<dyn AsyncRead + Unpin + Send>>;
    async fn fetch_manifest_single_request(
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

    async fn get_ref(&self, settings: &Settings, ref_key: &str) -> StorageResult<Bytes>;
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
    ) -> StorageResult<()>;

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
        chunks: BoxStream<'_, ManifestId>,
    ) -> StorageResult<usize> {
        self.delete_objects(
            settings,
            MANIFEST_PREFIX,
            chunks.map(|id| id.to_string()).boxed(),
        )
        .await
    }

    async fn delete_snapshots(
        &self,
        settings: &Settings,
        chunks: BoxStream<'_, SnapshotId>,
    ) -> StorageResult<usize> {
        self.delete_objects(
            settings,
            SNAPSHOT_PREFIX,
            chunks.map(|id| id.to_string()).boxed(),
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
/// Returns tuples of Range for each request. It tries to generate the maximum number of
/// requests possible, not generating more than `max_parts` requests, and each request not being
/// smaller than `min_part_size`. Note that the size of the last request is >= the preceding one.
fn split_in_multiple_requests(
    range: &Range<u64>,
    min_part_size: u64,
    max_parts: u16,
) -> impl Iterator<Item = Range<u64>> {
    let size = range.end - range.start;
    let min_part_size = max(min_part_size, 1);
    let num_parts = size / min_part_size;
    let num_parts = max(1, min(num_parts, max_parts as u64));
    let equal_parts = num_parts - 1;
    let equal_parts_size = size / num_parts;
    let last_part_size = size - equal_parts * equal_parts_size;
    let equal_requests = iter::successors(
        Some(range.start..range.start + equal_parts_size),
        move |range| Some(range.end..range.end + equal_parts_size),
    );
    let last_part_offset = range.start + equal_parts * equal_parts_size;
    let last_request = iter::once(last_part_offset..last_part_offset + last_part_size);
    equal_requests.take(equal_parts as usize).chain(last_request)
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
        fn test_split_requests(offset in 0..1_000_000u64, size in 1..3_000_000_000u64, min_part_size in 1..16_000_000u64, max_parts in 1..100u16 ) {
            let res: Vec<_> = split_in_multiple_requests(&(offset..offset+size), min_part_size, max_parts).collect();

            // there is always at least 1 request
            prop_assert!(!res.is_empty());

            // it does as many requests as possible
            prop_assert!(res.len() as u64 >= min(max_parts as u64, size / min_part_size));

            // there are never more than max_parts requests
            prop_assert!(res.len() <= max_parts as usize);

            // the request sizes add up to total size
            prop_assert_eq!(res.iter().map(|range| range.end - range.start).sum::<u64>(), size);

            // no more than one request smaller than minimum size
            prop_assert!(res.iter().filter(|range| (range.end - range.start) < min_part_size).count() <= 1);

            // if there is a request smaller than the minimum size it is because the total size is
            // smaller than minimum request size
            if res.iter().any(|range| (range.end - range.start) < min_part_size) {
                prop_assert!(size < min_part_size)
            }

            // there are only two request sizes
            let counts = res.iter().map(|range| (range.end - range.start)).counts();
            prop_assert!(counts.len() <= 2); // only last element is smaller
            if counts.len() > 1 {
                // the smaller request size happens only once
                prop_assert_eq!(counts.values().min(), Some(&1usize));
            }

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
            vec![(10..16)]
        );
        assert_eq!(
            split_in_multiple_requests(&(10..21), 5, 100,).collect::<Vec<_>>(),
            vec![(10..15), (15..21)]
        );
        assert_eq!(
            split_in_multiple_requests(&(0..13), 5, 2,).collect::<Vec<_>>(),
            vec![(0..6), (6..13)]
        );
        assert_eq!(
            split_in_multiple_requests(&(0..100), 5, 3,).collect::<Vec<_>>(),
            vec![(0..33), (33..66), (66..100)]
        );
    }
}
