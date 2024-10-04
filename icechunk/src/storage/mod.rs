use aws_sdk_s3::{
    config::http::HttpResponse,
    error::SdkError,
    operation::{
        get_object::GetObjectError, list_objects_v2::ListObjectsV2Error,
        put_object::PutObjectError,
    },
    primitives::ByteStreamError,
};
use core::fmt;
use futures::stream::BoxStream;
use std::{ffi::OsString, sync::Arc};

use async_trait::async_trait;
use bytes::Bytes;
use thiserror::Error;

pub mod caching;

#[cfg(test)]
pub mod logging;

pub mod object_store;
pub mod s3;
pub mod virtual_ref;

pub use caching::MemCachingStorage;
pub use object_store::ObjectStorage;

use crate::format::{
    attributes::AttributesTable, manifest::Manifest, snapshot::Snapshot, AttributesId,
    ByteRange, ChunkId, ManifestId, SnapshotId,
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
    #[error("unknown storage error: {0}")]
    Other(String),
}

pub type StorageResult<A> = Result<A, StorageError>;

/// Fetch and write the parquet files that represent the repository in object store
///
/// Different implementation can cache the files differently, or not at all.
/// Implementations are free to assume files are never overwritten.
#[async_trait]
pub trait Storage: fmt::Debug {
    async fn fetch_snapshot(&self, id: &SnapshotId) -> StorageResult<Arc<Snapshot>>;
    async fn fetch_attributes(
        &self,
        id: &AttributesId,
    ) -> StorageResult<Arc<AttributesTable>>; // FIXME: format flags
    async fn fetch_manifests(&self, id: &ManifestId) -> StorageResult<Arc<Manifest>>; // FIXME: format flags
    async fn fetch_chunk(&self, id: &ChunkId, range: &ByteRange) -> StorageResult<Bytes>; // FIXME: format flags

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
}
