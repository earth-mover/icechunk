use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;

use super::{Storage, StorageError, StorageResult};
use crate::format::{
    attributes::AttributesTable, manifest::ManifestsTable, snapshot::SnapshotTable,
    ByteRange, ObjectId,
};

#[derive(Debug)]
pub struct LoggingStorage {
    backend: Arc<dyn Storage + Send + Sync>,
    fetch_log: Mutex<Vec<(String, ObjectId)>>,
}

#[cfg(test)]
impl LoggingStorage {
    pub fn new(backend: Arc<dyn Storage + Send + Sync>) -> Self {
        Self { backend, fetch_log: Mutex::new(Vec::new()) }
    }

    #[allow(clippy::expect_used)] // this implementation is intended for tests only
    pub fn fetch_operations(&self) -> Vec<(String, ObjectId)> {
        self.fetch_log.lock().expect("poison lock").clone()
    }
}

#[async_trait]
#[allow(clippy::expect_used)] // this implementation is intended for tests only
impl Storage for LoggingStorage {
    async fn fetch_snapshot(
        &self,
        id: &ObjectId,
    ) -> Result<Arc<SnapshotTable>, StorageError> {
        self.fetch_log
            .lock()
            .expect("poison lock")
            .push(("fetch_snapshot".to_string(), id.clone()));
        self.backend.fetch_snapshot(id).await
    }

    async fn fetch_attributes(
        &self,
        id: &ObjectId,
    ) -> Result<Arc<AttributesTable>, StorageError> {
        self.fetch_log
            .lock()
            .expect("poison lock")
            .push(("fetch_attributes".to_string(), id.clone()));
        self.backend.fetch_attributes(id).await
    }

    async fn fetch_manifests(
        &self,
        id: &ObjectId,
    ) -> Result<Arc<ManifestsTable>, StorageError> {
        self.fetch_log
            .lock()
            .expect("poison lock")
            .push(("fetch_manifests".to_string(), id.clone()));
        self.backend.fetch_manifests(id).await
    }

    async fn fetch_chunk(
        &self,
        id: &ObjectId,
        range: &ByteRange,
    ) -> Result<Bytes, StorageError> {
        self.fetch_log
            .lock()
            .expect("poison lock")
            .push(("fetch_chunk".to_string(), id.clone()));
        self.backend.fetch_chunk(id, range).await
    }

    async fn write_snapshot(
        &self,
        id: ObjectId,
        table: Arc<SnapshotTable>,
    ) -> Result<(), StorageError> {
        self.backend.write_snapshot(id, table).await
    }

    async fn write_attributes(
        &self,
        id: ObjectId,
        table: Arc<AttributesTable>,
    ) -> Result<(), StorageError> {
        self.backend.write_attributes(id, table).await
    }

    async fn write_manifests(
        &self,
        id: ObjectId,
        table: Arc<ManifestsTable>,
    ) -> Result<(), StorageError> {
        self.backend.write_manifests(id, table).await
    }

    async fn write_chunk(&self, id: ObjectId, bytes: Bytes) -> Result<(), StorageError> {
        self.backend.write_chunk(id, bytes).await
    }

    async fn get_ref(&self, ref_key: &str) -> StorageResult<Bytes> {
        self.backend.get_ref(ref_key).await
    }

    async fn ref_names(&self) -> StorageResult<Vec<String>> {
        self.backend.ref_names().await
    }

    async fn write_ref(&self, ref_key: &str, bytes: Bytes) -> StorageResult<()> {
        self.backend.write_ref(ref_key, bytes).await
    }

    async fn ref_versions(&self, ref_name: &str) -> BoxStream<StorageResult<String>> {
        self.backend.ref_versions(ref_name).await
    }
}
