//! Storage wrapper that adds artificial latency (for testing).

use std::{
    fmt,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::{Stream, stream::BoxStream};
use serde::{Deserialize, Serialize};

use super::{
    DeleteObjectsResult, GetModifiedResult, ListInfo, MemoryPermit, ObjectRange,
    RepositoryCreation, Settings, Storage, StorageContext, StorageError, StorageInfo,
    StorageResult, VersionInfo, VersionedUpdateResult,
};
use icechunk_storage::sealed;

#[derive(Debug, Serialize, Deserialize)]
pub struct LatencyStorage {
    backend: Arc<dyn Storage + Send + Sync>,
    write_delay_ms: AtomicU64,
    read_delay_ms: AtomicU64,
}

impl LatencyStorage {
    pub fn new(
        backend: Arc<dyn Storage + Send + Sync>,
        write_delay_ms: u64,
        read_delay_ms: u64,
    ) -> Self {
        Self {
            backend,
            write_delay_ms: AtomicU64::new(write_delay_ms),
            read_delay_ms: AtomicU64::new(read_delay_ms),
        }
    }

    pub fn set_write_delay_ms(&self, ms: u64) {
        self.write_delay_ms.store(ms, Ordering::Relaxed);
    }

    pub fn write_delay_ms(&self) -> u64 {
        self.write_delay_ms.load(Ordering::Relaxed)
    }

    pub fn set_read_delay_ms(&self, ms: u64) {
        self.read_delay_ms.store(ms, Ordering::Relaxed);
    }

    pub fn read_delay_ms(&self) -> u64 {
        self.read_delay_ms.load(Ordering::Relaxed)
    }

    async fn sleep_for_read(&self) {
        tokio::time::sleep(Duration::from_millis(self.read_delay_ms())).await;
    }

    async fn sleep_for_write(&self) {
        tokio::time::sleep(Duration::from_millis(self.write_delay_ms())).await;
    }
}

impl fmt::Display for LatencyStorage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "LatencyStorage(backend={})", self.backend)
    }
}

impl sealed::Sealed for LatencyStorage {}

#[async_trait]
#[typetag::serde]
impl Storage for LatencyStorage {
    fn storage_info(&self) -> StorageInfo {
        self.backend.storage_info()
    }

    async fn default_settings(&self) -> StorageResult<Settings> {
        self.backend.default_settings().await
    }

    async fn can_write(&self) -> StorageResult<bool> {
        self.backend.can_write().await
    }

    async fn can_create_repository(&self) -> StorageResult<RepositoryCreation> {
        self.backend.can_create_repository().await
    }

    async fn put_object(
        &self,
        ctx: &StorageContext<'_>,
        path: &str,
        bytes: Bytes,
        content_type: Option<&str>,
        metadata: Vec<(String, String)>,
        previous_version: Option<&VersionInfo>,
    ) -> StorageResult<VersionedUpdateResult> {
        self.sleep_for_write().await;
        self.backend
            .put_object(ctx, path, bytes, content_type, metadata, previous_version)
            .await
    }

    async fn copy_object_raw(
        &self,
        ctx: &StorageContext<'_>,
        from: &str,
        to: &str,
        content_type: Option<&str>,
        version: &VersionInfo,
    ) -> StorageResult<VersionedUpdateResult> {
        self.sleep_for_write().await;
        self.backend.copy_object_raw(ctx, from, to, content_type, version).await
    }

    async fn list_objects_raw<'a>(
        &'a self,
        ctx: &StorageContext<'_>,
        prefix: &str,
    ) -> StorageResult<BoxStream<'a, StorageResult<ListInfo<String>>>> {
        // NOTE: this only sleeps on the initial call. The underlying stream
        // pages back to storage every ~1k keys without additional delays.
        self.sleep_for_read().await;
        self.backend.list_objects_raw(ctx, prefix).await
    }

    async fn delete_batch_raw(
        &self,
        ctx: &StorageContext<'_>,
        prefix: &str,
        batch: Vec<(String, u64)>,
    ) -> StorageResult<DeleteObjectsResult> {
        self.sleep_for_write().await;
        self.backend.delete_batch_raw(ctx, prefix, batch).await
    }

    async fn get_object_last_modified_raw(
        &self,
        ctx: &StorageContext<'_>,
        path: &str,
    ) -> StorageResult<DateTime<Utc>> {
        self.sleep_for_read().await;
        self.backend.get_object_last_modified_raw(ctx, path).await
    }

    async fn get_object_conditional_raw(
        &self,
        ctx: &StorageContext<'_>,
        path: &str,
        reservation: &MemoryPermit,
        previous_version: Option<&VersionInfo>,
    ) -> StorageResult<GetModifiedResult> {
        self.sleep_for_read().await;
        self.backend
            .get_object_conditional_raw(ctx, path, reservation, previous_version)
            .await
    }

    async fn get_object_range(
        &self,
        ctx: &StorageContext<'_>,
        path: &str,
        target: ObjectRange<'_>,
    ) -> StorageResult<(
        Pin<Box<dyn Stream<Item = Result<Bytes, StorageError>> + Send>>,
        VersionInfo,
    )> {
        self.sleep_for_read().await;
        self.backend.get_object_range(ctx, path, target).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::new_in_memory_storage;

    #[tokio::test]
    async fn storage_with_latency() {
        use crate::storage::{Asset, IoGovernor, UnlimitedGovernor};

        let backend = new_in_memory_storage().await.unwrap();
        let storage = LatencyStorage::new(backend, 0, 0);
        let settings = storage.default_settings().await.unwrap();
        let governor: Arc<dyn IoGovernor> = Arc::new(UnlimitedGovernor);
        let ctx = StorageContext {
            settings: &settings,
            governor: &governor,
            asset: Asset::Other,
        };

        // Add 500ms write latency, 300ms read latency
        storage.set_write_delay_ms(500);
        storage.set_read_delay_ms(300);

        let start = std::time::Instant::now();
        storage
            .put_object(&ctx, "test/key", "hello".into(), None, vec![], None)
            .await
            .unwrap();
        let write_with_latency = start.elapsed();

        let start = std::time::Instant::now();
        let _ = storage
            .get_object_range(&ctx, "test/key", ObjectRange::unmetered())
            .await
            .unwrap();
        let read_with_latency = start.elapsed();

        // Allow some wiggle room for scheduling jitter
        assert!(
            write_with_latency.as_millis() >= 500,
            "expected ~500ms write latency, got {write_with_latency:?}"
        );
        assert!(
            read_with_latency.as_millis() >= 300,
            "expected ~300ms read latency, got {read_with_latency:?}"
        );
    }
}
