use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use quick_cache::sync::Cache;
use serde::{Deserialize, Serialize};

use crate::{
    format::{
        attributes::AttributesTable, manifest::Manifest, snapshot::Snapshot,
        transaction_log::TransactionLog, AttributesId, ByteRange, ChunkId, ManifestId,
        SnapshotId,
    },
    private,
};

use super::{ETag, ListInfo, Settings, Storage, StorageError, StorageResult};

#[derive(Debug, Serialize)]
#[serde(transparent)]
pub struct MemCachingStorage {
    backend: Arc<dyn Storage + Send + Sync>,
    #[serde(skip)]
    snapshot_cache: Cache<SnapshotId, Arc<Snapshot>>,
    #[serde(skip)]
    manifest_cache: Cache<ManifestId, Arc<Manifest>>,
    #[serde(skip)]
    transactions_cache: Cache<SnapshotId, Arc<TransactionLog>>,
    #[serde(skip)]
    attributes_cache: Cache<AttributesId, Arc<AttributesTable>>,
    #[serde(skip)]
    chunk_cache: Cache<(ChunkId, ByteRange), Bytes>,
}

impl MemCachingStorage {
    pub fn new(
        backend: Arc<dyn Storage + Send + Sync>,
        num_snapshots: u16,
        num_manifests: u16,
        num_transactions: u16,
        num_attributes: u16,
        num_chunks: u16,
    ) -> Self {
        MemCachingStorage {
            backend,
            snapshot_cache: Cache::new(num_snapshots as usize),
            manifest_cache: Cache::new(num_manifests as usize),
            transactions_cache: Cache::new(num_transactions as usize),
            attributes_cache: Cache::new(num_attributes as usize),
            chunk_cache: Cache::new(num_chunks as usize),
        }
    }

    pub fn new_with_defaults(backend: Arc<dyn Storage + Send + Sync>) -> Self {
        Self::new(backend, 2, 2, 0, 2, 0)
    }
}

impl private::Sealed for MemCachingStorage {}

impl<'de> Deserialize<'de> for MemCachingStorage {
    fn deserialize<D>(deserializer: D) -> Result<MemCachingStorage, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let backend: Arc<dyn Storage + Sync + Send> =
            Deserialize::deserialize(deserializer)?;
        Ok(Self::new_with_defaults(backend))
    }
}

#[async_trait]
#[typetag::serde]
impl Storage for MemCachingStorage {
    fn default_settings(&self) -> Settings {
        self.backend.default_settings()
    }
    async fn fetch_config(
        &self,
        settings: &Settings,
    ) -> StorageResult<Option<(Bytes, ETag)>> {
        self.backend.fetch_config(settings).await
    }
    async fn update_config(
        &self,
        settings: &Settings,
        config: Bytes,
        etag: Option<&str>,
    ) -> StorageResult<ETag> {
        self.backend.update_config(settings, config, etag).await
    }

    async fn fetch_snapshot(
        &self,
        settings: &Settings,
        id: &SnapshotId,
    ) -> Result<Arc<Snapshot>, StorageError> {
        match self.snapshot_cache.get_value_or_guard_async(id).await {
            Ok(snapshot) => Ok(snapshot),
            Err(guard) => {
                let snapshot = self.backend.fetch_snapshot(settings, id).await?;
                let _fail_is_ok = guard.insert(Arc::clone(&snapshot));
                Ok(snapshot)
            }
        }
    }

    async fn fetch_attributes(
        &self,
        settings: &Settings,
        id: &AttributesId,
    ) -> Result<Arc<AttributesTable>, StorageError> {
        match self.attributes_cache.get_value_or_guard_async(id).await {
            Ok(table) => Ok(table),
            Err(guard) => {
                let table = self.backend.fetch_attributes(settings, id).await?;
                let _fail_is_ok = guard.insert(Arc::clone(&table));
                Ok(table)
            }
        }
    }

    async fn fetch_manifests(
        &self,
        settings: &Settings,
        id: &ManifestId,
        size: u64,
    ) -> Result<Arc<Manifest>, StorageError> {
        match self.manifest_cache.get_value_or_guard_async(id).await {
            Ok(manifest) => Ok(manifest),
            Err(guard) => {
                let manifest = self.backend.fetch_manifests(settings, id, size).await?;
                let _fail_is_ok = guard.insert(Arc::clone(&manifest));
                Ok(manifest)
            }
        }
    }

    async fn fetch_transaction_log(
        &self,
        settings: &Settings,
        id: &SnapshotId,
    ) -> StorageResult<Arc<TransactionLog>> {
        match self.transactions_cache.get_value_or_guard_async(id).await {
            Ok(log) => Ok(log),
            Err(guard) => {
                let log = self.backend.fetch_transaction_log(settings, id).await?;
                let _fail_is_ok = guard.insert(Arc::clone(&log));
                Ok(log)
            }
        }
    }

    async fn fetch_chunk(
        &self,
        settings: &Settings,
        id: &ChunkId,
        range: &ByteRange,
    ) -> Result<Bytes, StorageError> {
        let key = (id.clone(), range.clone());
        match self.chunk_cache.get_value_or_guard_async(&key).await {
            Ok(bytes) => Ok(bytes),
            Err(guard) => {
                let bytes = self.backend.fetch_chunk(settings, id, range).await?;
                let _fail_is_ok = guard.insert(bytes.clone());
                Ok(bytes)
            }
        }
    }

    async fn write_snapshot(
        &self,
        settings: &Settings,
        id: SnapshotId,
        snapshot: Arc<Snapshot>,
    ) -> Result<(), StorageError> {
        self.backend.write_snapshot(settings, id.clone(), Arc::clone(&snapshot)).await?;
        self.snapshot_cache.insert(id, snapshot);
        Ok(())
    }

    async fn write_attributes(
        &self,
        settings: &Settings,
        id: AttributesId,
        table: Arc<AttributesTable>,
    ) -> Result<(), StorageError> {
        self.backend.write_attributes(settings, id.clone(), Arc::clone(&table)).await?;
        self.attributes_cache.insert(id, table);
        Ok(())
    }

    async fn write_manifests(
        &self,
        settings: &Settings,
        id: ManifestId,
        manifest: Arc<Manifest>,
    ) -> Result<u64, StorageError> {
        let res = self
            .backend
            .write_manifests(settings, id.clone(), Arc::clone(&manifest))
            .await?;
        self.manifest_cache.insert(id, manifest);
        Ok(res)
    }

    async fn write_transaction_log(
        &self,
        settings: &Settings,
        id: SnapshotId,
        log: Arc<TransactionLog>,
    ) -> StorageResult<()> {
        self.backend
            .write_transaction_log(settings, id.clone(), Arc::clone(&log))
            .await?;
        self.transactions_cache.insert(id, log);
        Ok(())
    }

    async fn write_chunk(
        &self,
        settings: &Settings,
        id: ChunkId,
        bytes: Bytes,
    ) -> Result<(), StorageError> {
        self.backend.write_chunk(settings, id.clone(), bytes.clone()).await?;
        // we don't pre-populate the chunk cache, there are too many of them for this to be useful
        Ok(())
    }

    async fn get_ref(&self, settings: &Settings, ref_key: &str) -> StorageResult<Bytes> {
        self.backend.get_ref(settings, ref_key).await
    }

    async fn ref_names(&self, settings: &Settings) -> StorageResult<Vec<String>> {
        self.backend.ref_names(settings).await
    }

    async fn write_ref(
        &self,
        settings: &Settings,
        ref_key: &str,
        overwrite_refs: bool,
        bytes: Bytes,
    ) -> StorageResult<()> {
        self.backend.write_ref(settings, ref_key, overwrite_refs, bytes).await
    }

    async fn ref_versions(
        &self,
        settings: &Settings,
        ref_name: &str,
    ) -> StorageResult<BoxStream<StorageResult<String>>> {
        self.backend.ref_versions(settings, ref_name).await
    }

    async fn list_objects<'a>(
        &'a self,
        settings: &Settings,
        prefix: &str,
    ) -> StorageResult<BoxStream<'a, StorageResult<ListInfo<String>>>> {
        self.backend.list_objects(settings, prefix).await
    }

    async fn delete_objects(
        &self,
        settings: &Settings,
        prefix: &str,
        ids: BoxStream<'_, String>,
    ) -> StorageResult<usize> {
        self.backend.delete_objects(settings, prefix, ids).await
    }
}

#[cfg(test)]
#[allow(clippy::panic, clippy::unwrap_used, clippy::expect_used)]
mod test {
    use std::sync::Arc;

    use itertools::Itertools;

    use super::*;
    use crate::{
        format::{
            manifest::{ChunkInfo, ChunkPayload},
            ChunkIndices, NodeId,
        },
        storage::{logging::LoggingStorage, new_in_memory_storage, Storage},
    };

    #[tokio::test(flavor = "multi_thread")]
    async fn test_caching_storage_caches() -> Result<(), Box<dyn std::error::Error>> {
        let backend: Arc<dyn Storage + Send + Sync> = new_in_memory_storage()?;
        let settings = Settings::default();

        let ci1 = ChunkInfo {
            node: NodeId::random(),
            coord: ChunkIndices(vec![]),
            payload: ChunkPayload::Inline(Bytes::copy_from_slice(b"a")),
        };
        let ci2 = ChunkInfo {
            node: NodeId::random(),
            coord: ChunkIndices(vec![]),
            payload: ChunkPayload::Inline(Bytes::copy_from_slice(b"b")),
        };
        let pre_existing_id = ManifestId::random();
        let pre_exiting_manifest = Arc::new(vec![ci1].into_iter().collect());
        let pre_size = backend
            .write_manifests(
                &settings,
                pre_existing_id.clone(),
                Arc::clone(&pre_exiting_manifest),
            )
            .await?;

        let logging = Arc::new(LoggingStorage::new(Arc::clone(&backend)));
        let logging_c: Arc<dyn Storage + Send + Sync> = logging.clone();
        let caching = MemCachingStorage::new(Arc::clone(&logging_c), 0, 2, 0, 0, 0);

        let manifest = Arc::new(vec![ci2].into_iter().collect());
        let id = ManifestId::random();
        let size =
            caching.write_manifests(&settings, id.clone(), Arc::clone(&manifest)).await?;

        assert_eq!(caching.fetch_manifests(&settings, &id, size).await?, manifest);
        assert_eq!(caching.fetch_manifests(&settings, &id, size).await?, manifest);
        // when we insert we cache, so no fetches
        assert_eq!(logging.fetch_operations(), vec![]);

        // first time it sees an ID it calls the backend
        assert_eq!(
            caching.fetch_manifests(&settings, &pre_existing_id, pre_size).await?,
            pre_exiting_manifest
        );
        assert_eq!(
            logging.fetch_operations(),
            vec![("fetch_manifests".to_string(), pre_existing_id.0.to_vec())]
        );

        // only calls backend once
        assert_eq!(
            caching.fetch_manifests(&settings, &pre_existing_id, pre_size).await?,
            pre_exiting_manifest
        );
        assert_eq!(
            logging.fetch_operations(),
            vec![("fetch_manifests".to_string(), pre_existing_id.0.to_vec())]
        );

        // other walues still cached
        assert_eq!(caching.fetch_manifests(&settings, &id, size).await?, manifest);
        assert_eq!(
            logging.fetch_operations(),
            vec![("fetch_manifests".to_string(), pre_existing_id.0.to_vec())]
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_caching_storage_has_limit() -> Result<(), Box<dyn std::error::Error>> {
        let backend: Arc<dyn Storage + Send + Sync> = new_in_memory_storage()?;
        let settings = Settings::default();

        let ci1 = ChunkInfo {
            node: NodeId::random(),
            coord: ChunkIndices(vec![]),
            payload: ChunkPayload::Inline(Bytes::copy_from_slice(b"a")),
        };
        let ci2 = ChunkInfo { node: NodeId::random(), ..ci1.clone() };
        let ci3 = ChunkInfo { node: NodeId::random(), ..ci1.clone() };
        let ci4 = ChunkInfo { node: NodeId::random(), ..ci1.clone() };
        let ci5 = ChunkInfo { node: NodeId::random(), ..ci1.clone() };
        let ci6 = ChunkInfo { node: NodeId::random(), ..ci1.clone() };
        let ci7 = ChunkInfo { node: NodeId::random(), ..ci1.clone() };
        let ci8 = ChunkInfo { node: NodeId::random(), ..ci1.clone() };
        let ci9 = ChunkInfo { node: NodeId::random(), ..ci1.clone() };

        let id1 = ManifestId::random();
        let table1 = Arc::new(vec![ci1, ci2, ci3].into_iter().collect());
        let size1 =
            backend.write_manifests(&settings, id1.clone(), Arc::clone(&table1)).await?;
        let id2 = ManifestId::random();
        let table2 = Arc::new(vec![ci4, ci5, ci6].into_iter().collect());
        let size2 =
            backend.write_manifests(&settings, id2.clone(), Arc::clone(&table2)).await?;
        let id3 = ManifestId::random();
        let table3 = Arc::new(vec![ci7, ci8, ci9].into_iter().collect());
        let size3 =
            backend.write_manifests(&settings, id3.clone(), Arc::clone(&table3)).await?;

        let logging = Arc::new(LoggingStorage::new(Arc::clone(&backend)));
        let logging_c: Arc<dyn Storage + Send + Sync> = logging.clone();
        let caching = MemCachingStorage::new(
            Arc::clone(&logging_c),
            // the cache can only fit 2 manifests.
            0,
            2,
            0,
            0,
            0,
        );

        // we keep asking for all 3 items, but the cache can only fit 2
        for _ in 0..20 {
            assert_eq!(caching.fetch_manifests(&settings, &id1, size1).await?, table1);
            assert_eq!(caching.fetch_manifests(&settings, &id2, size2).await?, table2);
            assert_eq!(caching.fetch_manifests(&settings, &id3, size3).await?, table3);
        }
        // after the initial warming requests, we only request the file that doesn't fit in the cache
        assert_eq!(logging.fetch_operations()[10..].iter().unique().count(), 1);

        Ok(())
    }
}
