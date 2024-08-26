use std::{
    ops::Range,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use bytes::Bytes;

use super::{Storage, StorageError};
use crate::format::{
    attributes::AttributesTable, manifest::ManifestsTable, structure::StructureTable,
    ChunkOffset, ObjectId,
};

#[derive(Debug)]
pub struct LoggingStorage {
    backend: Arc<dyn Storage + Send + Sync>,
    fetch_log: Mutex<Vec<(String, ObjectId)>>,
}

impl LoggingStorage {
    pub fn new(backend: Arc<dyn Storage + Send + Sync>) -> Self {
        Self { backend, fetch_log: Mutex::new(Vec::new()) }
    }

    pub fn fetch_operations(&self) -> Vec<(String, ObjectId)> {
        self.fetch_log.lock().unwrap().clone()
    }
}

#[async_trait]
impl Storage for LoggingStorage {
    async fn fetch_structure(
        &self,
        id: &ObjectId,
    ) -> Result<Arc<StructureTable>, StorageError> {
        self.fetch_log.lock().unwrap().push(("fetch_structure".to_string(), id.clone()));
        self.backend.fetch_structure(id).await
    }

    async fn fetch_attributes(
        &self,
        id: &ObjectId,
    ) -> Result<Arc<AttributesTable>, StorageError> {
        self.fetch_log.lock().unwrap().push(("fetch_attributes".to_string(), id.clone()));
        self.backend.fetch_attributes(id).await
    }

    async fn fetch_manifests(
        &self,
        id: &ObjectId,
    ) -> Result<Arc<ManifestsTable>, StorageError> {
        self.fetch_log.lock().unwrap().push(("fetch_manifests".to_string(), id.clone()));
        self.backend.fetch_manifests(id).await
    }

    async fn fetch_chunk(
        &self,
        id: &ObjectId,
        range: &Option<Range<ChunkOffset>>,
    ) -> Result<Bytes, StorageError> {
        self.fetch_log.lock().unwrap().push(("fetch_chunk".to_string(), id.clone()));
        self.backend.fetch_chunk(id, range).await
    }

    async fn write_structure(
        &self,
        id: ObjectId,
        table: Arc<StructureTable>,
    ) -> Result<(), StorageError> {
        self.backend.write_structure(id, table).await
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
}
