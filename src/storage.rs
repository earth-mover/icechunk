use std::{
    collections::HashMap,
    ops::Range,
    sync::{Arc, RwLock},
};

use async_trait::async_trait;
use bytes::Bytes;

use crate::{
    AttributesTable, ChunkOffset, ManifestsTable, ObjectId, Storage, StorageError,
    StructureTable,
};

#[derive(Default)]
pub struct InMemoryStorage {
    struct_files: Arc<RwLock<HashMap<ObjectId, Arc<StructureTable>>>>,
    attr_files: Arc<RwLock<HashMap<ObjectId, Arc<AttributesTable>>>>,
    man_files: Arc<RwLock<HashMap<ObjectId, Arc<ManifestsTable>>>>,
    chunk_files: Arc<RwLock<HashMap<ObjectId, Arc<Bytes>>>>,
}

impl InMemoryStorage {
    pub fn new() -> InMemoryStorage {
        InMemoryStorage {
            struct_files: Arc::new(RwLock::new(HashMap::new())),
            attr_files: Arc::new(RwLock::new(HashMap::new())),
            man_files: Arc::new(RwLock::new(HashMap::new())),
            chunk_files: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

#[async_trait]
impl Storage for InMemoryStorage {
    async fn fetch_structure(
        &self,
        id: &ObjectId,
    ) -> Result<Arc<StructureTable>, StorageError> {
        self.struct_files
            .read()
            .or(Err(StorageError::Deadlock))?
            .get(id)
            .cloned()
            .ok_or(StorageError::NotFound(id.clone()))
    }

    async fn fetch_attributes(
        &self,
        id: &ObjectId,
    ) -> Result<Arc<AttributesTable>, StorageError> {
        self.attr_files
            .read()
            .or(Err(StorageError::Deadlock))?
            .get(id)
            .cloned()
            .ok_or(StorageError::NotFound(id.clone()))
    }

    async fn fetch_manifests(
        &self,
        id: &ObjectId,
    ) -> Result<Arc<ManifestsTable>, StorageError> {
        self.man_files
            .read()
            .or(Err(StorageError::Deadlock))?
            .get(id)
            .cloned()
            .ok_or(StorageError::NotFound(id.clone()))
    }

    async fn write_structure(
        &self,
        id: ObjectId,
        table: Arc<StructureTable>,
    ) -> Result<(), StorageError> {
        self.struct_files
            .write()
            .or(Err(StorageError::Deadlock))?
            .insert(id, Arc::clone(&table));
        Ok(())
    }

    async fn write_attributes(
        &self,
        id: ObjectId,
        table: Arc<AttributesTable>,
    ) -> Result<(), StorageError> {
        self.attr_files
            .write()
            .or(Err(StorageError::Deadlock))?
            .insert(id, Arc::clone(&table));
        Ok(())
    }

    async fn write_manifests(
        &self,
        id: ObjectId,
        table: Arc<ManifestsTable>,
    ) -> Result<(), StorageError> {
        self.man_files
            .write()
            .or(Err(StorageError::Deadlock))?
            .insert(id, Arc::clone(&table));
        Ok(())
    }

    async fn fetch_chunk(
        &self,
        id: &ObjectId,
        range: &Option<Range<ChunkOffset>>,
    ) -> Result<Arc<Bytes>, StorageError> {
        // avoid unused warning
        let chunk = self
            .chunk_files
            .read()
            .or(Err(StorageError::Deadlock))?
            .get(id)
            .cloned()
            .ok_or(StorageError::NotFound(id.clone()))?;
        if let Some(range) = range {
            Ok(Arc::new(chunk.slice((range.start as usize)..(range.end as usize))))
        } else {
            Ok(Arc::clone(&chunk))
        }
    }

    async fn write_chunk(
        &self,
        id: ObjectId,
        bytes: bytes::Bytes,
    ) -> Result<(), StorageError> {
        self.chunk_files
            .write()
            .or(Err(StorageError::Deadlock))?
            .insert(id, Arc::new(bytes));
        Ok(())
    }
}
