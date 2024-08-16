use base64::{engine::general_purpose::URL_SAFE as BASE64_URL_SAFE, Engine as _};
use std::{
    collections::HashMap,
    fs::create_dir_all,
    ops::Range,
    sync::{Arc, RwLock},
};

use crate::{
    AttributesTable, ChunkOffset, ManifestsTable, ObjectId, Storage, StorageError,
    StorageError::StorageLayerError, StructureTable,
};
use arrow::array::RecordBatch;
use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
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
    prefix: String,
}

impl ObjectStorage {
    pub fn new_in_memory_store() -> ObjectStorage {
        ObjectStorage { store: Arc::new(InMemory::new()), prefix: "".into() }
    }
    pub fn new_local_store(
        prefix: std::path::PathBuf,
    ) -> Result<ObjectStorage, StorageError> {
        create_dir_all(prefix.as_path())
            .map_err(|err| StorageLayerError(Arc::new(err)))?;
        Ok(ObjectStorage {
            store: Arc::new(
                LocalFileSystem::new_with_prefix(&prefix)
                    .map_err(|err| StorageLayerError(Arc::new(err)))?,
            ),
            // We rely on `new_with_prefix` to create the `prefix` directory
            // if it doesn't exist. It will also add the prefix to any path
            // so we set ObjectStorate::prefix to an empty string.
            prefix: "".to_string(),
        })
    }
    pub fn new_s3_store_from_env(
        bucket_name: impl Into<String>,
        prefix: impl Into<String>,
    ) -> Result<ObjectStorage, StorageError> {
        use object_store::aws::AmazonS3Builder;
        let store = AmazonS3Builder::from_env()
            .with_bucket_name(bucket_name.into())
            .build()
            .map_err(|err| StorageError::ObjectStoreError(Arc::new(err)))?;
        Ok(ObjectStorage { store: Arc::new(store), prefix: prefix.into() })
    }

    pub fn new_s3_store_with_config(
        bucket_name: impl Into<String>,
        prefix: impl Into<String>,
    ) -> Result<ObjectStorage, StorageError> {
        use object_store::aws::AmazonS3Builder;
        let store = AmazonS3Builder::new()
            // TODO: Generalize the auth config
            .with_access_key_id("minio123")
            .with_secret_access_key("minio123")
            .with_endpoint("http://localhost:9000")
            .with_allow_http(true)
            .with_bucket_name(bucket_name.into())
            .build()
            .map_err(|err| StorageError::ObjectStoreError(Arc::new(err)))?;
        Ok(ObjectStorage { store: Arc::new(store), prefix: prefix.into() })
    }

    fn get_path(&self, filetype: FileType, ObjectId(asu8): &ObjectId) -> Path {
        let type_prefix = filetype.get_prefix();
        // TODO: be careful about allocation here
        let path = format!(
            "{}/{}/{}.parquet",
            self.prefix,
            type_prefix,
            BASE64_URL_SAFE.encode(asu8)
        );
        Path::from(path)
    }

    async fn read_parquet(&self, path: &Path) -> Result<RecordBatch, StorageError> {
        use crate::StorageError::ParquetError;
        use parquet::arrow::{
            async_reader::ParquetObjectReader, ParquetRecordBatchStreamBuilder,
        };

        // TODO: avoid this read since we are always reading the whole thing.
        let meta = self
            .store
            .head(path)
            .await
            .map_err(|err| StorageError::ParquetError(Arc::new(err)))?;
        let reader = ParquetObjectReader::new(Arc::clone(&self.store), meta);
        let mut builder = ParquetRecordBatchStreamBuilder::new(reader)
            .await
            .map_err(|err| StorageError::ParquetError(Arc::new(err)))?
            .build()
            .map_err(|err| StorageError::ParquetError(Arc::new(err)))?;

        // TODO: do we always have only one batch ever? Assert that
        let maybe_batch = builder.next().await;
        if let Some(batch) = maybe_batch {
            batch.map_err(|err| ParquetError(Arc::new(err)))
        } else {
            Err(StorageError::MiscError(
                "ParquetError:No more record batches".to_string(),
            ))
        }
    }

    async fn write_parquet(
        &self,
        path: &Path,
        batch: &RecordBatch,
    ) -> Result<(), StorageError> {
        use crate::StorageError::ParquetError;
        use parquet::arrow::async_writer::AsyncArrowWriter;
        let mut buffer = Vec::new();
        let mut writer = AsyncArrowWriter::try_new(&mut buffer, batch.schema(), None)
            .map_err(|err| ParquetError(Arc::new(err)))?;
        writer.write(batch).await.map_err(|err| ParquetError(Arc::new(err)))?;
        writer.close().await.map_err(|err| ParquetError(Arc::new(err)))?;

        // TODO: find object_store streaming interface
        let payload = object_store::PutPayload::from(buffer);
        self.store
            .put(path, payload)
            .await
            .map_err(|err| StorageLayerError(Arc::new(err)))?;
        Ok(())
    }
}

#[async_trait]
impl Storage for ObjectStorage {
    async fn fetch_structure(
        &self,
        id: &ObjectId,
    ) -> Result<Arc<StructureTable>, StorageError> {
        let path = self.get_path(FileType::Structure, id);
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
        let path = self.get_path(FileType::Manifest, id);
        let batch = self.read_parquet(&path).await?;
        Ok(Arc::new(ManifestsTable { batch }))
    }

    async fn write_structure(
        &self,
        id: ObjectId,
        table: Arc<StructureTable>,
    ) -> Result<(), StorageError> {
        let path = self.get_path(FileType::Structure, &id);
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
        let path = self.get_path(FileType::Manifest, &id);
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::ObjectId;
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use rand;
    use rand::distributions::Alphanumeric;
    use rand::Rng;

    use super::{FileType, ObjectStorage};

    fn make_record_batch() -> RecordBatch {
        let id_array = Int32Array::from(vec![1, 2, 3, 4, 5]);
        let schema = Schema::new(vec![Field::new("id", DataType::Int32, false)]);

        RecordBatch::try_new(Arc::new(schema), vec![Arc::new(id_array)]).unwrap()
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_read_write_parquet_object_storage() {
        // simple test to make sure we can speak to all stores
        let batch = make_record_batch();
        let prefix: String = rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(7)
            .map(char::from)
            .collect();

        for store in [
            ObjectStorage::new_in_memory_store(),
            ObjectStorage::new_local_store(prefix.clone().into()).unwrap(),
            // Arc::new(ObjectStorage::new_s3_store_from_env("testbucket".to_string()).unwrap()),
            ObjectStorage::new_s3_store_with_config("testbucket".to_string(), prefix)
                .unwrap(),
        ] {
            let id = ObjectId::random();
            let path = store.get_path(FileType::Manifest, &id);
            store.write_parquet(&path, &batch).await.unwrap();
            let actual = store.read_parquet(&path).await.unwrap();
            assert_eq!(actual, batch)
        }
    }
}
