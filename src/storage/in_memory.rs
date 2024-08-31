use core::fmt;
use std::{
    collections::{HashMap, HashSet},
    ops::Range,
    sync::{Arc, RwLock},
};

use async_trait::async_trait;
use bytes::Bytes;

use crate::format::{
    attributes::AttributesTable, manifest::ManifestsTable, structure::StructureTable,
    ChunkOffset, ObjectId,
};

use super::{Storage, StorageError};

#[derive(Default)]
pub struct InMemoryStorage {
    struct_files: Arc<RwLock<HashMap<ObjectId, Arc<StructureTable>>>>,
    attr_files: Arc<RwLock<HashMap<ObjectId, Arc<AttributesTable>>>>,
    man_files: Arc<RwLock<HashMap<ObjectId, Arc<ManifestsTable>>>>,
    chunk_files: Arc<RwLock<HashMap<ObjectId, Bytes>>>,
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

    /// Intended for tests
    pub fn chunk_ids(&self) -> HashSet<ObjectId> {
        #[allow(clippy::unwrap_used)] // this implementation is used exclusively for tests
        self.chunk_files.read().unwrap().keys().cloned().collect()
    }
}

impl fmt::Debug for InMemoryStorage {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "InMemoryStorage at {:p}", self)
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
            .or(Err(StorageError::Other("in-memory storage deadlock".to_string())))?
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
            .or(Err(StorageError::Other("in-memory storage deadlock".to_string())))?
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
            .or(Err(StorageError::Other("in-memory storage deadlock".to_string())))?
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
            .or(Err(StorageError::Other("in-memory storage deadlock".to_string())))?
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
            .or(Err(StorageError::Other("in-memory storage deadlock".to_string())))?
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
            .or(Err(StorageError::Other("in-memory storage deadlock".to_string())))?
            .insert(id, Arc::clone(&table));
        Ok(())
    }

    async fn fetch_chunk(
        &self,
        id: &ObjectId,
        range: &Option<Range<ChunkOffset>>,
    ) -> Result<Bytes, StorageError> {
        // avoid unused warning
        let chunk = self
            .chunk_files
            .read()
            .or(Err(StorageError::Other("in-memory storage deadlock".to_string())))?
            .get(id)
            .cloned()
            .ok_or(StorageError::NotFound(id.clone()))?;
        if let Some(range) = range {
            Ok(chunk.slice((range.start as usize)..(range.end as usize)))
        } else {
            Ok(chunk.clone())
        }
    }

    async fn write_chunk(
        &self,
        id: ObjectId,
        bytes: bytes::Bytes,
    ) -> Result<(), StorageError> {
        self.chunk_files
            .write()
            .or(Err(StorageError::Other("in-memory storage deadlock".to_string())))?
            .insert(id, bytes);

        Ok(())
    }
}
