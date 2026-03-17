//! Storage wrapper that adds artificial latency (for testing).

use std::{
    fmt,
    ops::Range,
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
    DeleteObjectsResult, GetModifiedResult, ListInfo, Settings, Storage, StorageError,
    StorageResult, VersionInfo, VersionedUpdateResult,
};
use crate::private;

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
}

impl fmt::Display for LatencyStorage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "LatencyStorage(backend={})", self.backend)
    }
}

impl private::Sealed for LatencyStorage {}

#[async_trait]
#[typetag::serde]
impl Storage for LatencyStorage {
    async fn default_settings(&self) -> StorageResult<Settings> {
        self.backend.default_settings().await
    }

    async fn can_write(&self) -> StorageResult<bool> {
        self.backend.can_write().await
    }

    async fn put_object(
        &self,
        settings: &Settings,
        path: &str,
        bytes: Bytes,
        content_type: Option<&str>,
        metadata: Vec<(String, String)>,
        previous_version: Option<&VersionInfo>,
    ) -> StorageResult<VersionedUpdateResult> {
        tokio::time::sleep(Duration::from_millis(self.write_delay_ms())).await;
        self.backend
            .put_object(settings, path, bytes, content_type, metadata, previous_version)
            .await
    }

    async fn copy_object(
        &self,
        settings: &Settings,
        from: &str,
        to: &str,
        content_type: Option<&str>,
        version: &VersionInfo,
    ) -> StorageResult<VersionedUpdateResult> {
        tokio::time::sleep(Duration::from_millis(self.write_delay_ms())).await;
        self.backend.copy_object(settings, from, to, content_type, version).await
    }

    async fn list_objects<'a>(
        &'a self,
        settings: &Settings,
        prefix: &str,
    ) -> StorageResult<BoxStream<'a, StorageResult<ListInfo<String>>>> {
        tokio::time::sleep(Duration::from_millis(self.read_delay_ms())).await;
        self.backend.list_objects(settings, prefix).await
    }

    async fn delete_batch(
        &self,
        settings: &Settings,
        prefix: &str,
        batch: Vec<(String, u64)>,
    ) -> StorageResult<DeleteObjectsResult> {
        tokio::time::sleep(Duration::from_millis(self.write_delay_ms())).await;
        self.backend.delete_batch(settings, prefix, batch).await
    }

    async fn get_object_last_modified(
        &self,
        path: &str,
        settings: &Settings,
    ) -> StorageResult<DateTime<Utc>> {
        tokio::time::sleep(Duration::from_millis(self.read_delay_ms())).await;
        self.backend.get_object_last_modified(path, settings).await
    }

    async fn get_object_conditional(
        &self,
        settings: &Settings,
        path: &str,
        previous_version: Option<&VersionInfo>,
    ) -> StorageResult<GetModifiedResult> {
        tokio::time::sleep(Duration::from_millis(self.read_delay_ms())).await;
        self.backend.get_object_conditional(settings, path, previous_version).await
    }

    async fn get_object_range(
        &self,
        settings: &Settings,
        path: &str,
        range: Option<&Range<u64>>,
    ) -> StorageResult<(
        Pin<Box<dyn Stream<Item = Result<Bytes, StorageError>> + Send>>,
        VersionInfo,
    )> {
        tokio::time::sleep(Duration::from_millis(self.read_delay_ms())).await;
        self.backend.get_object_range(settings, path, range).await
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::storage::new_in_memory_storage;

    #[tokio::test]
    async fn test_latency() {
        let backend = new_in_memory_storage().await.unwrap();
        let storage = LatencyStorage::new(backend, 0, 0);
        let settings = storage.default_settings().await.unwrap();

        // Write a test object so we can read it back
        storage
            .put_object(&settings, "test/key", "hello".into(), None, vec![], None)
            .await
            .unwrap();

        // Baseline: measure write and read with no latency
        let start = std::time::Instant::now();
        storage
            .put_object(&settings, "test/key", "hello".into(), None, vec![], None)
            .await
            .unwrap();
        let write_baseline = start.elapsed();

        let start = std::time::Instant::now();
        let _ = storage.get_object_range(&settings, "test/key", None).await.unwrap();
        let read_baseline = start.elapsed();

        // Add 50ms write latency, 30ms read latency
        storage.set_write_delay_ms(50);
        storage.set_read_delay_ms(30);

        let start = std::time::Instant::now();
        storage
            .put_object(&settings, "test/key", "hello".into(), None, vec![], None)
            .await
            .unwrap();
        let write_with_latency = start.elapsed();

        let start = std::time::Instant::now();
        let _ = storage.get_object_range(&settings, "test/key", None).await.unwrap();
        let read_with_latency = start.elapsed();

        let write_added = write_with_latency.saturating_sub(write_baseline);
        let read_added = read_with_latency.saturating_sub(read_baseline);

        // Allow some wiggle room for scheduling jitter
        assert!(
            write_added.as_millis() >= 45,
            "expected ~50ms write latency, got {write_added:?}"
        );
        assert!(
            read_added.as_millis() >= 25,
            "expected ~30ms read latency, got {read_added:?}"
        );
    }
}
