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
use s3::S3Storage;
use std::{ffi::OsString, path::Path, sync::Arc};

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
    async fn fetch_manifests(&self, id: &ManifestId) -> StorageResult<Arc<Manifest>>; // FIXME: format flags
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
    ) -> StorageResult<()>;
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
) -> StorageResult<Arc<dyn Storage>> {
    let url = format!(
        "gs://{}{}",
        bucket,
        prefix.map(|p| format!("/{}", p)).unwrap_or("".to_string())
    );
    let mut options = vec![];

    match credentials {
        Some(GcsCredentials::FromEnv) => (),
        Some(GcsCredentials::Static(GcsStaticCredentials::ServiceAccount(path))) => {
            options.push((
                "google_service_account".to_string(),
                path.into_os_string().into_string().map_err(|_| {
                    StorageError::Other("invalid service account path".to_string())
                })?,
            ));
        }
        Some(GcsCredentials::Static(GcsStaticCredentials::ServiceAccountKey(key))) => {
            options.push(("google_service_account_key".to_string(), key));
        }
        Some(GcsCredentials::Static(GcsStaticCredentials::ApplicationCredentials(
            path,
        ))) => {
            options.push((
                "google_application_credentials".to_string(),
                path.into_os_string().into_string().map_err(|_| {
                    StorageError::Other(
                        "invalid application credentials path".to_string(),
                    )
                })?,
            ));
        }
        None => {}
    };

    Ok(Arc::new(ObjectStorage::from_url(&url, options)?))
}
