use base64::{engine::general_purpose::URL_SAFE as BASE64_URL_SAFE, Engine as _};
use std::{
    collections::HashMap,
    fs::create_dir_all,
    ops::Range,
    sync::{Arc, RwLock},
};

use crate::{
    AttributesTable, ChunkOffset, ManifestsTable, ObjectId, Path, Storage, StorageError,
    StructureTable,
};
use arrow::array::RecordBatch;
use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use object_store::{
    local::LocalFileSystem, memory::InMemory, path::Path as ObjectPath, ObjectStore,
};
use parquet::arrow::async_writer::AsyncArrowWriter;
use parquet::arrow::{
    async_reader::ParquetObjectReader, ParquetRecordBatchStreamBuilder,
};

const STRUCTURE_PREFIX: &str = "s/";
const MANIFEST_PREFIX: &str = "m/";
// const ATTRIBUTES_PREFIX: &str = "a/";
const CHUNK_PREFIX: &str = "c/";

pub struct ObjectStorage {
    store: Arc<dyn ObjectStore>,
    prefix: String,
}

impl ObjectStorage {
    pub fn new_in_memory_store() -> ObjectStorage {
        ObjectStorage { store: Arc::new(InMemory::new()), prefix: "".into() }
    }
    pub fn new_local_store(prefix: &Path) -> Result<ObjectStorage, StorageError> {
        create_dir_all(prefix.as_path())?;
        let prefix = prefix.to_str().ok_or(StorageError::BadPath(prefix.to_owned()))?;
        Ok(ObjectStorage {
            store: Arc::new(LocalFileSystem::new()),
            prefix: prefix.to_owned().to_string(),
        })
    }
    pub fn new_s3_store_from_env(
        bucket_name: impl Into<String>,
        prefix: impl Into<String>,
    ) -> Result<ObjectStorage, StorageError> {
        use object_store::aws::AmazonS3Builder;
        let store =
            AmazonS3Builder::from_env().with_bucket_name(bucket_name.into()).build()?;
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
            .build()?;
        Ok(ObjectStorage { store: Arc::new(store), prefix: prefix.into() })
    }

    fn get_path(&self, file_prefix: &str, ObjectId(asu8): &ObjectId) -> ObjectPath {
        // TODO: be careful about allocation here
        let path = format!(
            "{}/{}/{}.parquet",
            self.prefix,
            file_prefix,
            BASE64_URL_SAFE.encode(asu8)
        );
        ObjectPath::from(path)
    }

    async fn read_parquet(&self, path: &ObjectPath) -> Result<RecordBatch, StorageError> {
        // FIXME: avoid this metadata read since we are always reading the whole thing.
        let meta = self.store.head(path).await?;
        let reader = ParquetObjectReader::new(Arc::clone(&self.store), meta);
        let mut builder = ParquetRecordBatchStreamBuilder::new(reader).await?.build()?;
        // TODO: do we always have only one batch ever? Assert that
        let maybe_batch = builder.next().await;
        Ok(maybe_batch.ok_or(StorageError::BadRecordBatchRead(path.to_string()))??)
    }

    async fn write_parquet(
        &self,
        path: &ObjectPath,
        batch: &RecordBatch,
    ) -> Result<(), StorageError> {
        let mut buffer = Vec::new();
        let mut writer = AsyncArrowWriter::try_new(&mut buffer, batch.schema(), None)?;
        writer.write(batch).await?;
        writer.close().await?;

        // TODO: find object_store streaming interface
        let payload = object_store::PutPayload::from(buffer);
        self.store.put(path, payload).await?;
        Ok(())
    }
}

#[async_trait]
impl Storage for ObjectStorage {
    async fn fetch_structure(
        &self,
        id: &ObjectId,
    ) -> Result<Arc<StructureTable>, StorageError> {
        let path = self.get_path(STRUCTURE_PREFIX, id);
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
        let path = self.get_path(MANIFEST_PREFIX, id);
        let batch = self.read_parquet(&path).await?;
        Ok(Arc::new(ManifestsTable { batch }))
    }

    async fn write_structure(
        &self,
        id: ObjectId,
        table: Arc<StructureTable>,
    ) -> Result<(), StorageError> {
        let path = self.get_path(STRUCTURE_PREFIX, &id);
        self.write_parquet(&path, &table.batch).await?;
        Ok(())
    }

    async fn write_attributes(
        &self,
        _id: ObjectId,
        _table: Arc<AttributesTable>,
    ) -> Result<(), StorageError> {
        todo!()
        // let path = ObjectStorage::get_path(ATTRIBUTES_PREFIX, &id);
        // self.write_parquet(&path, &table.batch).await?;
        // Ok(())
    }

    async fn write_manifests(
        &self,
        id: ObjectId,
        table: Arc<ManifestsTable>,
    ) -> Result<(), StorageError> {
        let path = self.get_path(MANIFEST_PREFIX, &id);
        self.write_parquet(&path, &table.batch).await?;
        Ok(())
    }

    async fn fetch_chunk(
        &self,
        id: &ObjectId,
        range: &Option<std::ops::Range<crate::ChunkOffset>>,
    ) -> Result<Bytes, StorageError> {
        let path = self.get_path(CHUNK_PREFIX, id);
        // TODO: shall we split `range` into multiple ranges and use get_ranges?
        // I can't tell that `get_range` does splitting
        if let Some(range) = range {
            Ok(self
                .store
                .get_range(&path, (range.start as usize)..(range.end as usize))
                .await?)
        } else {
            // TODO: Can't figure out if `get` is the most efficient way to get the whole object.
            Ok(self.store.get(&path).await?.bytes().await?)
        }
    }

    async fn write_chunk(
        &self,
        id: ObjectId,
        bytes: bytes::Bytes,
    ) -> Result<(), StorageError> {
        let path = self.get_path(CHUNK_PREFIX, &id);
        let upload = self.store.put_multipart(&path).await?;
        // TODO: new_with_chunk_size?
        let mut write = object_store::WriteMultipart::new(upload);
        write.write(&bytes);
        write.finish().await?;
        Ok(())
    }
}

//////#########

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
    ) -> Result<Bytes, StorageError> {
        // avoid unused warning
        let chunk = self
            .chunk_files
            .read()
            .or(Err(StorageError::Deadlock))?
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
        self.chunk_files.write().or(Err(StorageError::Deadlock))?.insert(id, bytes);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::env::temp_dir;
    use std::sync::Arc;

    use crate::ObjectId;
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use rand;
    use rand::distributions::Alphanumeric;
    use rand::Rng;

    use super::ObjectStorage;

    fn make_record_batch() -> RecordBatch {
        let id_array = Int32Array::from(vec![1, 2, 3, 4, 5]);
        let schema = Schema::new(vec![Field::new("id", DataType::Int32, false)]);

        RecordBatch::try_new(Arc::new(schema), vec![Arc::new(id_array)]).unwrap()
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_read_write_parquet_object_storage() {
        // simple test to make sure we can speak to all stores
        let batch = make_record_batch();
        let mut prefix = temp_dir().to_str().unwrap().to_string();
        let rdms: String = rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(7)
            .map(char::from)
            .collect();
        prefix.push_str(rdms.as_str());

        for store in [
            ObjectStorage::new_in_memory_store(),
            // FIXME: figure out local and minio tests on CI
            // ObjectStorage::new_local_store(&prefix.into()).unwrap(),
            // ObjectStorage::new_s3_store_from_env("testbucket".to_string()).unwrap(),
            // ObjectStorage::new_s3_store_with_config("testbucket".to_string(), prefix)
            //     .unwrap(),
        ] {
            let id = ObjectId::random();
            let path = store.get_path("foo_prefix/", &id);
            store.write_parquet(&path, &batch).await.unwrap();
            let actual = store.read_parquet(&path).await.unwrap();
            assert_eq!(actual, batch)
        }
    }
}
