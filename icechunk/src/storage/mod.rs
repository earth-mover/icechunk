use core::fmt;
use futures::stream::BoxStream;
use parquet::errors as parquet_errors;
use std::{ops::Range, sync::Arc};

use async_trait::async_trait;
use bytes::Bytes;
use thiserror::Error;

pub mod caching;

#[cfg(test)]
pub mod logging;

pub mod object_store;

pub use caching::MemCachingStorage;
pub use object_store::ObjectStorage;

use crate::format::{
    attributes::AttributesTable, manifest::ManifestsTable, snapshot::SnapshotTable,
    ChunkOffset, ObjectId, Path,
};

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("object not found `{0:?}`")]
    NotFound(ObjectId),
    #[error("error contacting object store {0}")]
    ObjectStore(#[from] ::object_store::Error),
    #[error("parquet file error: {0}")]
    ParquetError(#[from] parquet_errors::ParquetError),
    #[error("error parsing RecordBatch from parquet file {0}.")]
    BadRecordBatchRead(Path),
    #[error("cannot overwrite ref: {0}")]
    RefAlreadyExists(String),
    #[error("ref not found: {0}")]
    RefNotFound(String),
    #[error("I/O error: {0}")]
    IO(#[from] futures::io::Error),
    #[error("generic storage error: {0}")]
    OtherError(#[from] Arc<dyn std::error::Error + Sync + Send>),
    #[error("unknown storage error: {0}")]
    Other(String),
}

pub type StorageResult<A> = Result<A, StorageError>;

/// Fetch and write the parquet files that represent the dataset in object store
///
/// Different implementation can cache the files differently, or not at all.
/// Implementations are free to assume files are never overwritten.
#[async_trait]
pub trait Storage: fmt::Debug {
    async fn fetch_snapshot(&self, id: &ObjectId) -> StorageResult<Arc<SnapshotTable>>;
    async fn fetch_attributes(
        &self,
        id: &ObjectId,
    ) -> StorageResult<Arc<AttributesTable>>; // FIXME: format flags
    async fn fetch_manifests(&self, id: &ObjectId) -> StorageResult<Arc<ManifestsTable>>; // FIXME: format flags
    async fn fetch_manifest(
        &self,
        id: &ObjectId,
    ) -> StorageResult<BoxStream<StorageResult<Bytes>>>; // FIXME: format flags
    async fn fetch_chunk(
        &self,
        id: &ObjectId,
        range: &Option<Range<ChunkOffset>>,
    ) -> StorageResult<Bytes>; // FIXME: format flags

    async fn write_snapshot(
        &self,
        id: ObjectId,
        table: Arc<SnapshotTable>,
    ) -> StorageResult<()>;
    async fn write_attributes(
        &self,
        id: ObjectId,
        table: Arc<AttributesTable>,
    ) -> StorageResult<()>;
    async fn write_manifests(
        &self,
        id: ObjectId,
        table: Arc<ManifestsTable>,
    ) -> StorageResult<()>;
    async fn write_chunk(&self, id: ObjectId, bytes: Bytes) -> StorageResult<()>;

    async fn get_ref(&self, ref_key: &str) -> StorageResult<Bytes>;
    async fn ref_names(&self) -> StorageResult<Vec<String>>;
    async fn ref_versions(&self, ref_name: &str) -> BoxStream<StorageResult<String>>;
    async fn write_ref(&self, ref_key: &str, bytes: Bytes) -> StorageResult<()>;
}
