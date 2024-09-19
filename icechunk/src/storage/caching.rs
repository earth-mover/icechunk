use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use quick_cache::{
    sync::{Cache, DefaultLifecycle},
    DefaultHashBuilder, OptionsBuilder, Weighter,
};

use crate::format::{
    attributes::AttributesTable, manifest::Manifest, snapshot::Snapshot, ByteRange,
    ObjectId,
};

use super::{Storage, StorageError, StorageResult};

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
enum CacheKey {
    Snapshot(ObjectId),
    Attributes(ObjectId),
    Manifest(ObjectId),
    Chunk(ObjectId, ByteRange),
}

#[derive(Clone, Debug)]
enum CacheValue {
    Snapshot(Arc<Snapshot>),
    Attributes(Arc<AttributesTable>),
    Manifest(Arc<Manifest>),
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

/// FIXME: this is not a good idea, with the new serialization formats can no longer estimate the
/// size of objects. Also, the storage layer doesn't know enough about the format to know what to
/// cache and not cache.
impl Weighter<CacheKey, CacheValue> for CacheWeighter {
    fn weight(&self, _key: &CacheKey, val: &CacheValue) -> u64 {
        // We ignore the keys weigth
        match val {
            CacheValue::Snapshot(table) => table.estimated_size_bytes() as u64,
            CacheValue::Attributes(_) => 1,
            CacheValue::Manifest(table) => table.estimated_size_bytes() as u64,
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
    async fn fetch_snapshot(&self, id: &ObjectId) -> Result<Arc<Snapshot>, StorageError> {
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
        let key = CacheKey::Attributes(id.clone());
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
    ) -> Result<Arc<Manifest>, StorageError> {
        let key = CacheKey::Manifest(id.clone());
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
        range: &ByteRange,
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
        table: Arc<Snapshot>,
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
        table: Arc<Manifest>,
    ) -> Result<(), StorageError> {
        self.backend.write_manifests(id.clone(), Arc::clone(&table)).await?;
        dbg!("inserting", &id);
        self.cache.insert(CacheKey::Manifest(id), CacheValue::Manifest(table));
        Ok(())
    }

    async fn write_chunk(&self, id: ObjectId, bytes: Bytes) -> Result<(), StorageError> {
        self.backend.write_chunk(id.clone(), bytes.clone()).await?;
        // TODO: we could add the chunk also with its full range (0, size)
        self.cache.insert(CacheKey::Chunk(id, ByteRange::ALL), CacheValue::Chunk(bytes));
        Ok(())
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

    async fn ref_versions(&self, ref_name: &str) -> BoxStream<StorageResult<String>> {
        self.backend.ref_versions(ref_name).await
    }
}

#[cfg(test)]
#[allow(clippy::panic, clippy::unwrap_used, clippy::expect_used)]
mod test {
    use itertools::Itertools;
    use std::sync::Arc;

    use super::*;
    use crate::{
        dataset::{ChunkIndices, ChunkPayload},
        format::manifest::ChunkInfo,
        storage::{logging::LoggingStorage, ObjectStorage, Storage},
    };

    #[tokio::test(flavor = "multi_thread")]
    async fn test_caching_storage_caches() -> Result<(), Box<dyn std::error::Error>> {
        let backend: Arc<dyn Storage + Send + Sync> =
            Arc::new(ObjectStorage::new_in_memory_store());

        let ci1 = ChunkInfo {
            node: 1,
            coord: ChunkIndices(vec![]),
            payload: ChunkPayload::Inline(Bytes::copy_from_slice(b"a")),
        };
        let ci2 = ChunkInfo {
            node: 1,
            coord: ChunkIndices(vec![]),
            payload: ChunkPayload::Inline(Bytes::copy_from_slice(b"b")),
        };
        let pre_existing_id = ObjectId::random();
        let pre_exiting_table = Arc::new(vec![ci1].into_iter().collect());
        backend
            .write_manifests(pre_existing_id.clone(), Arc::clone(&pre_exiting_table))
            .await?;

        let logging = Arc::new(LoggingStorage::new(Arc::clone(&backend)));
        let logging_c: Arc<dyn Storage + Send + Sync> = logging.clone();
        let caching = MemCachingStorage::new(Arc::clone(&logging_c), 100_000_000);

        let table = Arc::new(vec![ci2].into_iter().collect());
        let id = ObjectId::random();
        caching.write_manifests(id.clone(), Arc::clone(&table)).await?;

        assert_eq!(caching.fetch_manifests(&id).await?, table);
        assert_eq!(caching.fetch_manifests(&id).await?, table);
        // when we insert we cache, so no fetches
        assert_eq!(logging.fetch_operations(), vec![]);

        // first time it sees an ID it calls the backend
        assert_eq!(caching.fetch_manifests(&pre_existing_id).await?, pre_exiting_table);
        assert_eq!(
            logging.fetch_operations(),
            vec![("fetch_manifests".to_string(), pre_existing_id.clone())]
        );

        // only calls backend once
        assert_eq!(caching.fetch_manifests(&pre_existing_id).await?, pre_exiting_table);
        assert_eq!(
            logging.fetch_operations(),
            vec![("fetch_manifests".to_string(), pre_existing_id.clone())]
        );

        // other walues still cached
        assert_eq!(caching.fetch_manifests(&id).await?, table);
        assert_eq!(
            logging.fetch_operations(),
            vec![("fetch_manifests".to_string(), pre_existing_id.clone())]
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_caching_storage_has_limit() -> Result<(), Box<dyn std::error::Error>> {
        let backend: Arc<dyn Storage + Send + Sync> =
            Arc::new(ObjectStorage::new_in_memory_store());

        let ci1 = ChunkInfo {
            node: 1,
            coord: ChunkIndices(vec![]),
            payload: ChunkPayload::Inline(Bytes::copy_from_slice(b"a")),
        };
        let ci2 = ChunkInfo { node: 2, ..ci1.clone() };
        let ci3 = ChunkInfo { node: 3, ..ci1.clone() };
        let ci4 = ChunkInfo { node: 4, ..ci1.clone() };
        let ci5 = ChunkInfo { node: 5, ..ci1.clone() };
        let ci6 = ChunkInfo { node: 6, ..ci1.clone() };
        let ci7 = ChunkInfo { node: 7, ..ci1.clone() };
        let ci8 = ChunkInfo { node: 8, ..ci1.clone() };
        let ci9 = ChunkInfo { node: 9, ..ci1.clone() };

        let id1 = ObjectId::random();
        let table1 = Arc::new(vec![ci1, ci2, ci3].into_iter().collect());
        backend.write_manifests(id1.clone(), Arc::clone(&table1)).await?;
        let id2 = ObjectId::random();
        let table2 = Arc::new(vec![ci4, ci5, ci6].into_iter().collect());
        backend.write_manifests(id2.clone(), Arc::clone(&table2)).await?;
        let id3 = ObjectId::random();
        let table3 = Arc::new(vec![ci7, ci8, ci9].into_iter().collect());
        backend.write_manifests(id3.clone(), Arc::clone(&table3)).await?;

        let logging = Arc::new(LoggingStorage::new(Arc::clone(&backend)));
        let logging_c: Arc<dyn Storage + Send + Sync> = logging.clone();
        let caching = MemCachingStorage::new(
            Arc::clone(&logging_c),
            // the cache can only fit 2 tables. TODO: This number was manually tuned
            900,
        );

        // we keep asking for all 3 items, but the cache can only fit 2
        for _ in 0..20 {
            assert_eq!(caching.fetch_manifests(&id1).await?, table1);
            assert_eq!(caching.fetch_manifests(&id2).await?, table2);
            assert_eq!(caching.fetch_manifests(&id3).await?, table3);
        }
        // after the initial warming requests, we only request the file that doesn't fit in the cache
        assert_eq!(logging.fetch_operations()[10..].iter().unique().count(), 1);

        Ok(())
    }
}
