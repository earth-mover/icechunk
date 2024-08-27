use core::fmt;
use parquet::errors as parquet_errors;
use std::{ops::Range, sync::Arc};

use async_trait::async_trait;
use bytes::Bytes;
use thiserror::Error;

pub mod caching;
pub mod in_memory;

#[cfg(test)]
pub mod logging;

pub mod object_store;

pub use caching::MemCachingStorage;
pub use in_memory::InMemoryStorage;
pub use object_store::ObjectStorage;

use crate::format::{
    attributes::AttributesTable, manifest::ManifestsTable, structure::StructureTable,
    ChunkOffset, ObjectId,
};

/// Fetch and write the parquet files that represent the dataset in object store
///
/// Different implementation can cache the files differently, or not at all.
/// Implementations are free to assume files are never overwritten.

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("object not found `{0:?}`")]
    NotFound(ObjectId),
    #[error("synchronization error on the Storage instance")]
    Deadlock,
    #[error("error contacting object store {0}")]
    ObjectStore(#[from] ::object_store::Error),
    #[error("error reading or writing to/from parquet files: {0}")]
    ParquetError(#[from] parquet_errors::ParquetError),
    #[error("error reading RecordBatch from parquet file {0}.")]
    BadRecordBatchRead(String),
}

#[async_trait]
pub trait Storage: fmt::Debug {
    async fn fetch_structure(
        &self,
        id: &ObjectId,
    ) -> Result<Arc<StructureTable>, StorageError>; // FIXME: format flags
    async fn fetch_attributes(
        &self,
        id: &ObjectId,
    ) -> Result<Arc<AttributesTable>, StorageError>; // FIXME: format flags
    async fn fetch_manifests(
        &self,
        id: &ObjectId,
    ) -> Result<Arc<ManifestsTable>, StorageError>; // FIXME: format flags
    async fn fetch_chunk(
        &self,
        id: &ObjectId,
        range: &Option<Range<ChunkOffset>>,
    ) -> Result<Bytes, StorageError>; // FIXME: format flags

    async fn write_structure(
        &self,
        id: ObjectId,
        table: Arc<StructureTable>,
    ) -> Result<(), StorageError>;
    async fn write_attributes(
        &self,
        id: ObjectId,
        table: Arc<AttributesTable>,
    ) -> Result<(), StorageError>;
    async fn write_manifests(
        &self,
        id: ObjectId,
        table: Arc<ManifestsTable>,
    ) -> Result<(), StorageError>;
    async fn write_chunk(&self, id: ObjectId, bytes: Bytes) -> Result<(), StorageError>;
}
