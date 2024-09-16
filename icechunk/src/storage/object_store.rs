use core::fmt;
use std::{fs::create_dir_all, future::ready, sync::Arc};

use arrow::array::RecordBatch;
use async_trait::async_trait;
use base64::{engine::general_purpose::URL_SAFE as BASE64_URL_SAFE, Engine as _};
use bytes::Bytes;
use futures::{stream::BoxStream, StreamExt, TryStreamExt};
use object_store::{
    buffered::BufWriter, local::LocalFileSystem, memory::InMemory,
    path::Path as ObjectPath, GetOptions, GetRange, ObjectStore, PutMode, PutOptions,
    PutPayload,
};
use parquet::arrow::{
    async_reader::ParquetObjectReader, async_writer::ParquetObjectWriter,
    AsyncArrowWriter, ParquetRecordBatchStreamBuilder,
};

use crate::format::{
    attributes::AttributesTable, manifest::ManifestsTable, snapshot::SnapshotTable,
    ByteRange, ObjectId, Path,
};

use super::{Storage, StorageError, StorageResult};

// Get Range is object_store specific, keep it with this module
impl From<&ByteRange> for Option<GetRange> {
    fn from(value: &ByteRange) -> Self {
        match value {
            ByteRange::Range((start, end)) => {
                Some(GetRange::Bounded(*start as usize..*end as usize))
            }
            ByteRange::From(start) => Some(GetRange::Offset(*start as usize)),
            ByteRange::To(end) => Some(GetRange::Suffix(*end as usize)),
            ByteRange::All => None,
        }
    }
}

const SNAPSHOT_PREFIX: &str = "s/";
const MANIFEST_PREFIX: &str = "m/";
// const ATTRIBUTES_PREFIX: &str = "a/";
const CHUNK_PREFIX: &str = "c/";
const REF_PREFIX: &str = "r";

pub struct ObjectStorage {
    store: Arc<dyn ObjectStore>,
    prefix: String,
}

impl ObjectStorage {
    pub fn new_in_memory_store() -> ObjectStorage {
        ObjectStorage { store: Arc::new(InMemory::new()), prefix: "".into() }
    }
    pub fn new_local_store(prefix: &Path) -> Result<ObjectStorage, std::io::Error> {
        create_dir_all(prefix.as_path())?;
        let prefix = prefix.display().to_string();
        let store = Arc::new(LocalFileSystem::new_with_prefix(prefix.clone())?);
        Ok(ObjectStorage { store, prefix: "".to_string() })
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
        access_key_id: impl Into<String>,
        secret_access_key: impl Into<String>,
        endpoint: Option<impl Into<String>>,
    ) -> Result<ObjectStorage, StorageError> {
        use object_store::aws::AmazonS3Builder;
        let builder = AmazonS3Builder::new()
            .with_access_key_id(access_key_id)
            .with_secret_access_key(secret_access_key);

        let builder = if let Some(endpoint) = endpoint {
            // TODO: Check if HTTP is allowed always or based on endpoint
            builder.with_endpoint(endpoint).with_allow_http(true)
        } else {
            builder
        };

        let store = builder.with_bucket_name(bucket_name.into()).build()?;
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
        Ok(maybe_batch
            .ok_or(StorageError::BadRecordBatchRead(path.as_ref().into()))??)
    }

    async fn write_parquet(
        &self,
        path: &ObjectPath,
        batch: &RecordBatch,
    ) -> Result<(), StorageError> {
        // defaults are concurrency=8, buffer capacity=10MB
        // TODO: allow configuring these
        let writer = ParquetObjectWriter::from_buf_writer(BufWriter::new(
            Arc::clone(&self.store),
            path.clone(),
        ));
        let mut writer = AsyncArrowWriter::try_new(writer, batch.schema(), None)?;
        writer.write(batch).await?;
        writer.close().await?;
        Ok(())
    }

    fn drop_prefix(&self, prefix: &ObjectPath, path: &ObjectPath) -> Option<ObjectPath> {
        path.prefix_match(&ObjectPath::from(format!("{}/{}", self.prefix, prefix)))
            .map(|it| it.collect())
    }

    fn ref_key(&self, ref_key: &str) -> ObjectPath {
        // ObjectPath knows how to deal with empty path parts: bar//foo
        ObjectPath::from(format!("{}/{}/{}", self.prefix.as_str(), REF_PREFIX, ref_key))
    }
}

impl fmt::Debug for ObjectStorage {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "ObjectStorage, prefix={}, store={}", self.prefix, self.store)
    }
}
#[async_trait]
impl Storage for ObjectStorage {
    async fn fetch_snapshot(
        &self,
        id: &ObjectId,
    ) -> Result<Arc<SnapshotTable>, StorageError> {
        let path = self.get_path(SNAPSHOT_PREFIX, id);
        let batch = self.read_parquet(&path).await?;
        Ok(Arc::new(SnapshotTable { batch }))
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

    async fn write_snapshot(
        &self,
        id: ObjectId,
        table: Arc<SnapshotTable>,
    ) -> Result<(), StorageError> {
        let path = self.get_path(SNAPSHOT_PREFIX, &id);
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
        range: &ByteRange,
    ) -> Result<Bytes, StorageError> {
        let path = self.get_path(CHUNK_PREFIX, id);
        // TODO: shall we split `range` into multiple ranges and use get_ranges?
        // I can't tell that `get_range` does splitting
        let options =
            GetOptions { range: Option::<GetRange>::from(range), ..Default::default() };
        let chunk = self.store.get_opts(&path, options).await?.bytes().await?;
        Ok(chunk)
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

    async fn get_ref(&self, ref_key: &str) -> StorageResult<Bytes> {
        let key = self.ref_key(ref_key);
        match self.store.get(&key).await {
            Ok(res) => Ok(res.bytes().await?),
            Err(object_store::Error::NotFound { .. }) => {
                Err(StorageError::RefNotFound(key.to_string()))
            }
            Err(err) => Err(err.into()),
        }
    }

    async fn ref_names(&self) -> StorageResult<Vec<String>> {
        // FIXME: i don't think object_store's implementation of list_with_delimiter is any good
        // we need to test if it even works beyond 1k refs
        let prefix = self.ref_key("");
        let ref_prefix = ObjectPath::from(REF_PREFIX);

        Ok(self
            .store
            .list_with_delimiter(Some(prefix).as_ref())
            .await?
            .common_prefixes
            .iter()
            .filter_map(|path| {
                self.drop_prefix(&ref_prefix, path).map(|path| path.to_string())
            })
            .collect())
    }

    async fn ref_versions(&self, ref_name: &str) -> BoxStream<StorageResult<String>> {
        let prefix = self.ref_key(ref_name);
        self.store
            .list(Some(prefix.clone()).as_ref())
            .map_err(|e| e.into())
            .and_then(move |meta| {
                ready(
                    self.drop_prefix(&prefix, &meta.location)
                        .map(|path| path.to_string())
                        .ok_or(StorageError::Other(
                            "Bug in ref prefix logic".to_string(),
                        )),
                )
            })
            .boxed()
    }

    async fn write_ref(&self, ref_key: &str, bytes: Bytes) -> StorageResult<()> {
        let key = self.ref_key(ref_key);
        let opts = PutOptions { mode: PutMode::Create, ..PutOptions::default() };

        self.store
            .put_opts(&key, PutPayload::from_bytes(bytes), opts)
            .await
            .map_err(|e| match e {
                object_store::Error::AlreadyExists { path, .. } => {
                    StorageError::RefAlreadyExists(path)
                }
                _ => e.into(),
            })
            .map(|_| ())
    }
}

#[cfg(test)]
#[allow(clippy::panic, clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::env::temp_dir;
    use std::sync::Arc;

    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use rand;
    use rand::distributions::Alphanumeric;
    use rand::Rng;

    use super::*;

    fn make_record_batch(data: Vec<i32>) -> RecordBatch {
        let id_array = Int32Array::from(data);
        let schema = Schema::new(vec![Field::new("id", DataType::Int32, false)]);

        RecordBatch::try_new(Arc::new(schema), vec![Arc::new(id_array)]).unwrap()
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_read_write_parquet_object_storage() {
        // simple test to make sure we can speak to all stores
        let batch = make_record_batch(vec![1, 2, 3, 4, 5]);
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
            // ObjectStorage::new_local_store(&prefix.clone().into()).unwrap(),
            // ObjectStorage::new_s3_store_from_env("testbucket".to_string(), prefix.clone()).unwrap(),
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
