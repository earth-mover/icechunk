use std::{ops::Range, sync::Arc};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use quick_cache::{
    sync::{Cache, DefaultLifecycle},
    DefaultHashBuilder, OptionsBuilder, Weighter,
};

use crate::format::{
    attributes::AttributesTable, manifest::ManifestsTable, snapshot::SnapshotTable,
    ChunkOffset, ObjectId,
};

use super::{Storage, StorageError, StorageResult};

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
enum CacheKey {
    Snapshot(ObjectId),
    Attributes(ObjectId),
    Manifest(ObjectId),
    Chunk(ObjectId, Option<Range<ChunkOffset>>),
}

#[derive(Clone)]
enum CacheValue {
    Snapshot(Arc<SnapshotTable>),
    Attributes(Arc<AttributesTable>),
    Manifest(Arc<ManifestsTable>),
    Chunk(Bytes),
}

#[derive(Debug, Clone)]
struct CacheWeighter;

#[derive(Debug)]
pub struct MemCachingStorage {
    backend: Arc<dyn Storage + Send + Sync>,
    // We keep all objects in a single cache so we can effictively limit total memory usage while
    // maintaining the LRU semantics
    cache: Cache<CacheKey, CacheValue, CacheWeighter>,
}

impl Weighter<CacheKey, CacheValue> for CacheWeighter {
    fn weight(&self, _key: &CacheKey, val: &CacheValue) -> u64 {
        // We ignore the keys weigth
        match val {
            CacheValue::Snapshot(table) => table.batch.get_array_memory_size() as u64,
            CacheValue::Attributes(table) => table.batch.get_array_memory_size() as u64,
            CacheValue::Manifest(table) => table.batch.get_array_memory_size() as u64,
            CacheValue::Chunk(bytes) => bytes.len() as u64,
        }
    }
}

impl MemCachingStorage {
    pub fn new(
        backend: Arc<dyn Storage + Send + Sync>,
        approx_max_memory_bytes: u64,
    ) -> Self {
        let cache = Cache::with_options(
            // build only fails if estimated_items_capacity or
            // weight_capacity are not set
            #[allow(clippy::expect_used)]
            OptionsBuilder::new()
                // TODO: estimate this capacity
                .estimated_items_capacity(0)
                .weight_capacity(approx_max_memory_bytes)
                .build()
                .expect("Bug in MemCachingStorage"),
            CacheWeighter,
            DefaultHashBuilder::default(),
            DefaultLifecycle::default(),
        );
        MemCachingStorage { backend, cache }
    }
}

#[async_trait]
impl Storage for MemCachingStorage {
    async fn fetch_snapshot(
        &self,
        id: &ObjectId,
    ) -> Result<Arc<SnapshotTable>, StorageError> {
        let key = CacheKey::Snapshot(id.clone());
        match self.cache.get_value_or_guard_async(&key).await {
            Ok(CacheValue::Snapshot(table)) => Ok(table),
            Err(guard) => {
                let table = self.backend.fetch_snapshot(id).await?;
                let _fail_is_ok = guard.insert(CacheValue::Snapshot(Arc::clone(&table)));
                Ok(table)
            }
            Ok(_) => {
                debug_assert!(false, "Logic bug in MemCachingStorage");
                self.cache.remove(&key);
                self.fetch_snapshot(id).await
            }
        }
    }

    async fn fetch_attributes(
        &self,
        id: &ObjectId,
    ) -> Result<Arc<AttributesTable>, StorageError> {
        let key = CacheKey::Snapshot(id.clone());
        match self.cache.get_value_or_guard_async(&key).await {
            Ok(CacheValue::Attributes(table)) => Ok(table),
            Err(guard) => {
                let table = self.backend.fetch_attributes(id).await?;
                let _fail_is_ok =
                    guard.insert(CacheValue::Attributes(Arc::clone(&table)));
                Ok(table)
            }
            Ok(_) => {
                debug_assert!(false, "Logic bug in MemCachingStorage");
                self.cache.remove(&key);
                self.fetch_attributes(id).await
            }
        }
    }

    async fn fetch_manifests(
        &self,
        id: &ObjectId,
    ) -> Result<Arc<ManifestsTable>, StorageError> {
        let key = CacheKey::Snapshot(id.clone());
        match self.cache.get_value_or_guard_async(&key).await {
            Ok(CacheValue::Manifest(table)) => Ok(table),
            Err(guard) => {
                let table = self.backend.fetch_manifests(id).await?;
                let _fail_is_ok = guard.insert(CacheValue::Manifest(Arc::clone(&table)));
                Ok(table)
            }
            Ok(_) => {
                debug_assert!(false, "Logic bug in MemCachingStorage");
                self.cache.remove(&key);
                self.fetch_manifests(id).await
            }
        }
    }

    async fn fetch_chunk(
        &self,
        id: &ObjectId,
        range: &Option<Range<ChunkOffset>>,
    ) -> Result<Bytes, StorageError> {
        let key = CacheKey::Chunk(id.clone(), range.clone());
        match self.cache.get_value_or_guard_async(&key).await {
            Ok(CacheValue::Chunk(table)) => Ok(table),
            Err(guard) => {
                let bytes = self.backend.fetch_chunk(id, range).await?;
                let _fail_is_ok = guard.insert(CacheValue::Chunk(bytes.clone()));
                Ok(bytes)
            }
            Ok(_) => {
                debug_assert!(false, "Logic bug in MemCachingStorage");
                self.cache.remove(&key);
                self.fetch_chunk(id, range).await
            }
        }
    }

    async fn write_snapshot(
        &self,
        id: ObjectId,
        table: Arc<SnapshotTable>,
    ) -> Result<(), StorageError> {
        self.backend.write_snapshot(id.clone(), Arc::clone(&table)).await?;
        self.cache.insert(CacheKey::Snapshot(id), CacheValue::Snapshot(table));
        Ok(())
    }

    async fn write_attributes(
        &self,
        id: ObjectId,
        table: Arc<AttributesTable>,
    ) -> Result<(), StorageError> {
        self.backend.write_attributes(id.clone(), Arc::clone(&table)).await?;
        self.cache.insert(CacheKey::Attributes(id), CacheValue::Attributes(table));
        Ok(())
    }

    async fn write_manifests(
        &self,
        id: ObjectId,
        table: Arc<ManifestsTable>,
    ) -> Result<(), StorageError> {
        self.backend.write_manifests(id.clone(), Arc::clone(&table)).await?;
        self.cache.insert(CacheKey::Manifest(id), CacheValue::Manifest(table));
        Ok(())
    }

    async fn write_chunk(&self, id: ObjectId, bytes: Bytes) -> Result<(), StorageError> {
        self.backend.write_chunk(id.clone(), bytes.clone()).await?;
        // TODO: we could add the chunk also with its full range (0, size)
        self.cache.insert(CacheKey::Chunk(id, None), CacheValue::Chunk(bytes));
        Ok(())
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

#[cfg(test)]
#[allow(clippy::panic, clippy::unwrap_used, clippy::expect_used)]
mod test {
    use std::sync::Arc;

    use arrow::{
        array::{Int32Array, RecordBatch},
        datatypes::{DataType, Field, Schema},
    };
    use itertools::Itertools;

    use super::*;
    use crate::storage::{logging::LoggingStorage, ObjectStorage, Storage};

    fn make_record_batch(data: Vec<i32>) -> RecordBatch {
        let id_array = Int32Array::from(data);
        let schema = Schema::new(vec![Field::new("id", DataType::Int32, false)]);

        RecordBatch::try_new(Arc::new(schema), vec![Arc::new(id_array)]).unwrap()
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_caching_storage_caches() -> Result<(), Box<dyn std::error::Error>> {
        let backend: Arc<dyn Storage + Send + Sync> =
            Arc::new(ObjectStorage::new_in_memory_store());

        let pre_existing_id = ObjectId::random();
        let pre_exiting_table =
            Arc::new(SnapshotTable { batch: make_record_batch(vec![1]) });
        backend
            .write_snapshot(pre_existing_id.clone(), Arc::clone(&pre_exiting_table))
            .await?;

        let logging = Arc::new(LoggingStorage::new(Arc::clone(&backend)));
        let logging_c: Arc<dyn Storage + Send + Sync> = logging.clone();
        let caching = MemCachingStorage::new(Arc::clone(&logging_c), 100_000_000);

        let table = Arc::new(SnapshotTable { batch: make_record_batch(vec![2]) });
        let id = ObjectId::random();
        caching.write_snapshot(id.clone(), Arc::clone(&table)).await?;

        assert_eq!(caching.fetch_snapshot(&id).await?, table);
        assert_eq!(caching.fetch_snapshot(&id).await?, table);
        // when we insert we cache, so no fetches
        assert_eq!(logging.fetch_operations(), vec![]);

        // first time it sees an ID it calls the backend
        assert_eq!(caching.fetch_snapshot(&pre_existing_id).await?, pre_exiting_table);
        assert_eq!(
            logging.fetch_operations(),
            vec![("fetch_snapshot".to_string(), pre_existing_id.clone())]
        );

        // only calls backend once
        assert_eq!(caching.fetch_snapshot(&pre_existing_id).await?, pre_exiting_table);
        assert_eq!(
            logging.fetch_operations(),
            vec![("fetch_snapshot".to_string(), pre_existing_id.clone())]
        );

        // other walues still cached
        assert_eq!(caching.fetch_snapshot(&id).await?, table);
        assert_eq!(
            logging.fetch_operations(),
            vec![("fetch_snapshot".to_string(), pre_existing_id.clone())]
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_caching_storage_has_limit() -> Result<(), Box<dyn std::error::Error>> {
        let backend: Arc<dyn Storage + Send + Sync> =
            Arc::new(ObjectStorage::new_in_memory_store());

        let id1 = ObjectId::random();
        let table1 = Arc::new(SnapshotTable { batch: make_record_batch(vec![1, 2, 3]) });
        backend.write_snapshot(id1.clone(), Arc::clone(&table1)).await?;
        let id2 = ObjectId::random();
        let table2 = Arc::new(SnapshotTable { batch: make_record_batch(vec![4, 5, 6]) });
        backend.write_snapshot(id2.clone(), Arc::clone(&table2)).await?;
        let id3 = ObjectId::random();
        let table3 = Arc::new(SnapshotTable { batch: make_record_batch(vec![7, 8, 9]) });
        backend.write_snapshot(id3.clone(), Arc::clone(&table3)).await?;

        let logging = Arc::new(LoggingStorage::new(Arc::clone(&backend)));
        let logging_c: Arc<dyn Storage + Send + Sync> = logging.clone();
        let caching = MemCachingStorage::new(
            Arc::clone(&logging_c),
            // the cache can only fit 2 tables. TODO: This number was manually tuned
            300,
        );

        // we keep asking for all 3 items, but the cache can only fit 2
        for _ in 0..20 {
            assert_eq!(caching.fetch_snapshot(&id1).await?, table1);
            assert_eq!(caching.fetch_snapshot(&id2).await?, table2);
            assert_eq!(caching.fetch_snapshot(&id3).await?, table3);
        }
        // after the initial warming requests, we only request the file that doesn't fit in the cache
        assert_eq!(logging.fetch_operations()[10..].iter().unique().count(), 1);

        Ok(())
    }
}
