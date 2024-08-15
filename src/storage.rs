use base64::{engine::general_purpose::URL_SAFE as BASE64_URL_SAFE, Engine as _};
use std::{
    collections::HashMap,
    ops::Range,
    sync::{Arc, RwLock},
};

use arrow::array::RecordBatch;
use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use parquet::arrow::{
    async_reader::ParquetObjectReader, AsyncArrowWriter, ParquetRecordBatchStreamBuilder,
};

use crate::{
    AttributesTable, ChunkOffset, ManifestsTable, ObjectId, Storage, StorageError,
    StructureTable, StorageError::StorageLayerError,
};
use object_store::{local::LocalFileSystem, memory::InMemory, path::Path, ObjectStore};

#[allow(dead_code)]
enum FileType {
    Structure,
    Manifest,
    Attributes,
}
impl FileType {
    pub fn get_prefix(&self) -> &str {
        match self {
            FileType::Structure => "s/",
            FileType::Manifest => "m/",
            FileType::Attributes => "a/",
        }
    }
}

// #[derive(Default)]
pub struct ObjectStorage {
    store: Arc<dyn ObjectStore>,
}

impl ObjectStorage {
    pub fn new_in_memory_store() -> ObjectStorage {
        ObjectStorage { store: Arc::new(InMemory::new()) }
    }
    pub fn new_local_store(
        prefix: &std::path::Path,
    ) -> Result<ObjectStorage, StorageError> {
        Ok(ObjectStorage {
            store: Arc::new(
                LocalFileSystem::new_with_prefix(prefix)
                    .map_err(|err| StorageLayerError(Box::new(err)))?,
            ),
        })
    }
    pub fn new_s3_store_from_env(
        bucket_name: String,
    ) -> Result<ObjectStorage, StorageError> {
        use object_store::aws::AmazonS3Builder;
        let store = AmazonS3Builder::from_env()
            .with_bucket_name(bucket_name)
            .build()
            .map_err(|err| StorageError::UrlParseError(Box::new(err)))?;
        Ok(ObjectStorage { store: Arc::new(store) })
    }

    pub fn new_s3_store_with_config(
        bucket_name: String,
    ) -> Result<ObjectStorage, StorageError> {
        use object_store::aws::AmazonS3Builder;
        let store = AmazonS3Builder::new()
            // TODO: Generalize the auth config
            .with_access_key_id("minio123")
            .with_secret_access_key("minio123")
            .with_endpoint("http://localhost:9000")
            .with_allow_http(true)
            .with_bucket_name(bucket_name)
            .build()
            .map_err(|err| StorageError::UrlParseError(Box::new(err)))?;
        Ok(ObjectStorage { store: Arc::new(store) })
    }

    fn get_path(filetype: FileType, id: &ObjectId) -> Path {
        let ObjectId(asu8) = id;
        let prefix = filetype.get_prefix();
        // TODO: be careful about allocation here
        let path = format!("{}/{}", prefix, BASE64_URL_SAFE.encode(asu8));
        Path::from(path)
    }

    async fn read_parquet(&self, path: &Path) -> Result<RecordBatch, StorageError> {
        let meta = self
            .store
            .head(path)
            .await
            .map_err(|err| StorageError::ParquetReadError(Box::new(err)))?;
        let reader = ParquetObjectReader::new(Arc::clone(&self.store), meta);
        let mut builder = ParquetRecordBatchStreamBuilder::new(reader)
            .await
            .map_err(|err| StorageError::ParquetReadError(Box::new(err)))?
            .build()
            .map_err(|err| StorageError::ParquetReadError(Box::new(err)))?;

        // only one batch ever? Assert that
        // Use `if let`;
        let batch = builder.next().await.unwrap().unwrap();
        Ok(batch)
    }

    async fn write_parquet(
        &self,
        path: &Path,
        batch: &RecordBatch,
    ) -> Result<(), StorageError> {
        let mut buffer = Vec::new();
        let mut writer = AsyncArrowWriter::try_new(&mut buffer, batch.schema(), None)
            .map_err(|err| StorageLayerError(Box::new(err)))?;
        writer.write(batch).await.map_err(|err| StorageLayerError(Box::new(err)))?;
        writer.close().await.map_err(|err| StorageLayerError(Box::new(err)))?;

        // TODO: find object_store streaming interface
        let payload = object_store::PutPayload::from(buffer);
        self.store
            .put(path, payload)
            .await
            .map_err(|err| StorageLayerError(Box::new(err)))?;
        Ok(())
    }
}

#[async_trait]
impl Storage for ObjectStorage {
    async fn fetch_structure(
        &self,
        id: &ObjectId,
    ) -> Result<Arc<StructureTable>, StorageError> {
        let path = ObjectStorage::get_path(FileType::Structure, id);
        let batch = self.read_parquet(&path).await?;
        Ok(Arc::new(StructureTable { batch }))
    }

    async fn fetch_attributes(
        &self,
        _id: &ObjectId,
    ) -> Result<Arc<AttributesTable>, StorageError> {
        todo!();
    }

    async fn fetch_manifests(
        &self,
        id: &ObjectId,
    ) -> Result<Arc<ManifestsTable>, StorageError> {
        let path = ObjectStorage::get_path(FileType::Manifest, id);
        let batch = self.read_parquet(&path).await?;
        Ok(Arc::new(ManifestsTable { batch }))
    }

    async fn write_structure(
        &self,
        id: ObjectId,
        table: Arc<StructureTable>,
    ) -> Result<(), StorageError> {
        let path = ObjectStorage::get_path(FileType::Structure, &id);
        self.write_parquet(&path, &table.batch).await?;
        Ok(())
    }

    async fn write_attributes(
        &self,
        _id: ObjectId,
        _table: Arc<AttributesTable>,
    ) -> Result<(), StorageError> {
        todo!()
        // let path = ObjectStorage::get_path(FileType::Structure, &id);
        // self.write_parquet(&path, &table.batch).await?;
        // Ok(())
    }

    async fn write_manifests(
        &self,
        id: ObjectId,
        table: Arc<ManifestsTable>,
    ) -> Result<(), StorageError> {
        let path = ObjectStorage::get_path(FileType::Manifest, &id);
        self.write_parquet(&path, &table.batch).await?;
        Ok(())
    }

    async fn fetch_chunk(
        &self,
        _x_id: &ObjectId,
        _range: &Option<std::ops::Range<crate::ChunkOffset>>,
    ) -> Result<Arc<Bytes>, StorageError> {
        todo!()
    }

    async fn write_chunk(
        &self,
        _id: ObjectId,
        _bytes: bytes::Bytes,
    ) -> Result<(), StorageError> {
        todo!()
    }
}

//////#########

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
