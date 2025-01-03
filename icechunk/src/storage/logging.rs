use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use serde::{Deserialize, Serialize};

use super::{ETag, ListInfo, Storage, StorageError, StorageResult};
use crate::{
    format::{
        attributes::AttributesTable, manifest::Manifest, snapshot::Snapshot,
        AttributesId, ByteRange, ChunkId, ManifestId, SnapshotId,
    },
    private,
};

#[derive(Debug, Serialize, Deserialize)]
pub struct LoggingStorage {
    backend: Arc<dyn Storage + Send + Sync>,
    fetch_log: Mutex<Vec<(String, Vec<u8>)>>,
}

#[cfg(test)]
impl LoggingStorage {
    pub fn new(backend: Arc<dyn Storage + Send + Sync>) -> Self {
        Self { backend, fetch_log: Mutex::new(Vec::new()) }
    }

    #[allow(clippy::expect_used)] // this implementation is intended for tests only
    pub fn fetch_operations(&self) -> Vec<(String, Vec<u8>)> {
        self.fetch_log.lock().expect("poison lock").clone()
    }
}

impl private::Sealed for LoggingStorage {}

#[async_trait]
#[typetag::serde]
#[allow(clippy::expect_used)] // this implementation is intended for tests only
impl Storage for LoggingStorage {
    async fn fetch_config(&self) -> StorageResult<Option<(Bytes, ETag)>> {
        self.backend.fetch_config().await
    }
    async fn update_config(
        &self,
        config: Bytes,
        etag: Option<&str>,
    ) -> StorageResult<ETag> {
        self.backend.update_config(config, etag).await
    }
    async fn fetch_snapshot(
        &self,
        id: &SnapshotId,
    ) -> Result<Arc<Snapshot>, StorageError> {
        self.fetch_log
            .lock()
            .expect("poison lock")
            .push(("fetch_snapshot".to_string(), id.0.to_vec()));
        self.backend.fetch_snapshot(id).await
    }

    async fn fetch_transaction_log(
        &self,
        id: &SnapshotId,
    ) -> StorageResult<Arc<crate::format::transaction_log::TransactionLog>> {
        self.fetch_log
            .lock()
            .expect("poison lock")
            .push(("fetch_transaction_log".to_string(), id.0.to_vec()));
        self.backend.fetch_transaction_log(id).await
    }

    async fn fetch_attributes(
        &self,
        id: &AttributesId,
    ) -> Result<Arc<AttributesTable>, StorageError> {
        self.fetch_log
            .lock()
            .expect("poison lock")
            .push(("fetch_attributes".to_string(), id.0.to_vec()));
        self.backend.fetch_attributes(id).await
    }

    async fn fetch_manifests(
        &self,
        id: &ManifestId,
        size: u64,
    ) -> Result<Arc<Manifest>, StorageError> {
        self.fetch_log
            .lock()
            .expect("poison lock")
            .push(("fetch_manifests".to_string(), id.0.to_vec()));
        self.backend.fetch_manifests(id, size).await
    }

    async fn fetch_chunk(
        &self,
        id: &ChunkId,
        range: &ByteRange,
    ) -> Result<Bytes, StorageError> {
        self.fetch_log
            .lock()
            .expect("poison lock")
            .push(("fetch_chunk".to_string(), id.0.to_vec()));
        self.backend.fetch_chunk(id, range).await
    }

    async fn write_snapshot(
        &self,
        id: SnapshotId,
        table: Arc<Snapshot>,
    ) -> Result<(), StorageError> {
        self.backend.write_snapshot(id, table).await
    }

    async fn write_transaction_log(
        &self,
        id: SnapshotId,
        log: Arc<crate::format::transaction_log::TransactionLog>,
    ) -> StorageResult<()> {
        self.backend.write_transaction_log(id, log).await
    }

    async fn write_attributes(
        &self,
        id: AttributesId,
        table: Arc<AttributesTable>,
    ) -> Result<(), StorageError> {
        self.backend.write_attributes(id, table).await
    }

    async fn write_manifests(
        &self,
        id: ManifestId,
        table: Arc<Manifest>,
    ) -> Result<u64, StorageError> {
        self.backend.write_manifests(id, table).await
    }

    async fn write_chunk(&self, id: ChunkId, bytes: Bytes) -> Result<(), StorageError> {
        self.backend.write_chunk(id, bytes).await
    }

    async fn get_ref(&self, ref_key: &str) -> StorageResult<Bytes> {
        self.backend.get_ref(ref_key).await
    }

    async fn ref_names(&self) -> StorageResult<Vec<String>> {
        self.backend.ref_names().await
    }

    async fn write_ref(
        &self,
        ref_key: &str,
        overwrite_refs: bool,
        bytes: Bytes,
    ) -> StorageResult<()> {
        self.backend.write_ref(ref_key, overwrite_refs, bytes).await
    }

    async fn ref_versions(
        &self,
        ref_name: &str,
    ) -> StorageResult<BoxStream<StorageResult<String>>> {
        self.backend.ref_versions(ref_name).await
    }

    async fn list_objects<'a>(
        &'a self,
        prefix: &str,
    ) -> StorageResult<BoxStream<'a, StorageResult<ListInfo<String>>>> {
        self.backend.list_objects(prefix).await
    }

    async fn delete_objects(
        &self,
        prefix: &str,
        ids: BoxStream<'_, String>,
    ) -> StorageResult<usize> {
        self.backend.delete_objects(prefix, ids).await
    }
}
