use core::fmt;
use futures::stream::BoxStream;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use thiserror::Error;

pub mod caching;

#[cfg(test)]
pub mod logging;

pub mod object_store;
pub mod virtual_ref;

pub use caching::MemCachingStorage;
pub use object_store::ObjectStorage;

use crate::format::{
    attributes::AttributesTable,
    manifest::Manifest,
    snapshot::Snapshot,
    ByteRange, ObjectId, Path,
};

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("object not found `{0:?}`")]
    NotFound(ObjectId),
    #[error("error contacting object store {0}")]
    ObjectStore(#[from] ::object_store::Error),
    #[error("messagepack decode error: {0}")]
    MsgPackDecodeError(#[from] rmp_serde::decode::Error),
    #[error("messagepack encode error: {0}")]
    MsgPackEncodeError(#[from] rmp_serde::encode::Error),
    #[error("error parsing RecordBatch from parquet file {0}.")]
    BadRecordBatchRead(Path),
    #[error("cannot overwrite ref: {0}")]
    RefAlreadyExists(String),
    #[error("ref not found: {0}")]
    RefNotFound(String),
    #[error("generic storage error: {0}")]
    OtherError(#[from] Arc<dyn std::error::Error + Sync + Send>),
    #[error("unknown storage error: {0}")]
    Other(String),
}

type StorageResult<A> = Result<A, StorageError>;

/// Fetch and write the parquet files that represent the dataset in object store
///
/// Different implementation can cache the files differently, or not at all.
/// Implementations are free to assume files are never overwritten.
#[async_trait]
pub trait Storage: fmt::Debug {
    async fn fetch_snapshot(&self, id: &ObjectId) -> StorageResult<Arc<Snapshot>>;
    async fn fetch_attributes(
        &self,
        id: &ObjectId,
    ) -> StorageResult<Arc<AttributesTable>>; // FIXME: format flags
    async fn fetch_manifests(&self, id: &ObjectId) -> StorageResult<Arc<Manifest>>; // FIXME: format flags
    async fn fetch_chunk(&self, id: &ObjectId, range: &ByteRange)
        -> StorageResult<Bytes>; // FIXME: format flags

    async fn write_snapshot(
        &self,
        id: ObjectId,
        table: Arc<Snapshot>,
    ) -> StorageResult<()>;
    async fn write_attributes(
        &self,
        id: ObjectId,
        table: Arc<AttributesTable>,
    ) -> StorageResult<()>;
    async fn write_manifests(
        &self,
        id: ObjectId,
        table: Arc<Manifest>,
    ) -> StorageResult<()>;
    async fn write_chunk(&self, id: ObjectId, bytes: Bytes) -> StorageResult<()>;

    async fn get_ref(&self, ref_key: &str) -> StorageResult<Bytes>;
    async fn ref_names(&self) -> StorageResult<Vec<String>>;
    async fn ref_versions(&self, ref_name: &str) -> BoxStream<StorageResult<String>>;
    async fn write_ref(
        &self,
        ref_key: &str,
        overwrite_refs: bool,
        bytes: Bytes,
    ) -> StorageResult<()>;
}
