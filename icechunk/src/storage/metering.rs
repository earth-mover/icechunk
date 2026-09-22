//! Storage wrapper that counts requests and bytes per operation (for benchmarks).

use std::{
    collections::{BTreeMap, HashSet},
    fmt,
    ops::Range,
    pin::Pin,
    sync::{Arc, Mutex},
    time::Instant,
};

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::{Stream, StreamExt as _, stream::BoxStream};
use serde::{Deserialize, Serialize};

use super::{
    DeleteObjectsResult, GetModifiedResult, ListInfo, RepositoryCreation, Settings,
    Storage, StorageError, StorageInfo, StorageResult, VersionInfo,
    VersionedUpdateResult,
};
use icechunk_storage::sealed;

/// Counters for one `Storage` method.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct OpStats {
    pub requests: u64,
    /// Only `get_object_range` counts bytes read, as they are yielded by the stream.
    pub bytes_read: u64,
    /// Only `put_object` counts bytes written.
    pub bytes_written: u64,
    /// Only `delete_batch` counts deleted objects.
    pub objects_deleted: u64,
}

/// Activity during one wall-clock second since the wrapper was created.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct SecondStats {
    pub bytes_read: u64,
    pub requests_started: u64,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct MeteringReport {
    pub per_op: BTreeMap<&'static str, OpStats>,
    /// Index is the elapsed second.
    pub read_timeline: Vec<SecondStats>,
}

#[derive(Debug)]
struct Meter {
    /// Second 0 of the timeline. Moved by [`Meter::reset`] so a measurement
    /// can exclude the setup that preceded it.
    started: Mutex<Instant>,
    per_op: Mutex<BTreeMap<&'static str, OpStats>>,
    timeline: Mutex<Vec<SecondStats>>,
}

impl Default for Meter {
    fn default() -> Self {
        Self {
            started: Mutex::new(Instant::now()),
            per_op: Mutex::new(BTreeMap::new()),
            timeline: Mutex::new(Vec::new()),
        }
    }
}

impl Meter {
    fn with_op(&self, op: &'static str, f: impl FnOnce(&mut OpStats)) {
        let mut per_op = self.per_op.lock().unwrap_or_else(|p| p.into_inner());
        f(per_op.entry(op).or_default());
    }

    fn with_second(&self, f: impl FnOnce(&mut SecondStats)) {
        let second =
            self.started.lock().unwrap_or_else(|p| p.into_inner()).elapsed().as_secs()
                as usize;
        let mut timeline = self.timeline.lock().unwrap_or_else(|p| p.into_inner());
        if timeline.len() <= second {
            timeline.resize(second + 1, SecondStats::default());
        }
        if let Some(stats) = timeline.get_mut(second) {
            f(stats);
        }
    }

    fn request(&self, op: &'static str) {
        self.with_op(op, |s| s.requests += 1);
        self.with_second(|s| s.requests_started += 1);
    }

    fn read(&self, op: &'static str, bytes: u64) {
        self.with_op(op, |s| s.bytes_read += bytes);
        self.with_second(|s| s.bytes_read += bytes);
    }

    fn written(&self, op: &'static str, bytes: u64) {
        self.with_op(op, |s| s.bytes_written += bytes);
    }

    fn deleted(&self, op: &'static str, objects: u64) {
        self.with_op(op, |s| s.objects_deleted += objects);
    }

    fn reset(&self) {
        *self.started.lock().unwrap_or_else(|p| p.into_inner()) = Instant::now();
        self.per_op.lock().unwrap_or_else(|p| p.into_inner()).clear();
        self.timeline.lock().unwrap_or_else(|p| p.into_inner()).clear();
    }

    fn snapshot(&self) -> MeteringReport {
        MeteringReport {
            per_op: self.per_op.lock().unwrap_or_else(|p| p.into_inner()).clone(),
            read_timeline: self
                .timeline
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .clone(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MeteringStorage {
    backend: Arc<dyn Storage + Send + Sync>,
    #[serde(skip, default)]
    meter: Arc<Meter>,
}

impl MeteringStorage {
    pub fn new(backend: Arc<dyn Storage + Send + Sync>) -> Self {
        Self { backend, meter: Arc::new(Meter::default()) }
    }

    pub fn snapshot(&self) -> MeteringReport {
        self.meter.snapshot()
    }

    /// Forget everything measured so far and restart the timeline at now, so
    /// the report describes only what happens after this point.
    pub fn reset(&self) {
        self.meter.reset();
    }
}

impl fmt::Display for MeteringStorage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "MeteringStorage(backend={})", self.backend)
    }
}

impl sealed::Sealed for MeteringStorage {}

#[async_trait]
#[typetag::serde]
impl Storage for MeteringStorage {
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
        settings: &Settings,
        path: &str,
        bytes: Bytes,
        content_type: Option<&str>,
        metadata: Vec<(String, String)>,
        previous_version: Option<&VersionInfo>,
    ) -> StorageResult<VersionedUpdateResult> {
        self.meter.request("put_object");
        let written = bytes.len() as u64;
        let result = self
            .backend
            .put_object(settings, path, bytes, content_type, metadata, previous_version)
            .await;
        if result.is_ok() {
            self.meter.written("put_object", written);
        }
        result
    }

    async fn copy_object(
        &self,
        settings: &Settings,
        from: &str,
        to: &str,
        content_type: Option<&str>,
        version: &VersionInfo,
    ) -> StorageResult<VersionedUpdateResult> {
        self.meter.request("copy_object");
        self.backend.copy_object(settings, from, to, content_type, version).await
    }

    async fn list_objects<'a>(
        &'a self,
        settings: &Settings,
        prefix: &str,
    ) -> StorageResult<BoxStream<'a, StorageResult<ListInfo<String>>>> {
        self.meter.request("list_objects");
        self.backend.list_objects(settings, prefix).await
    }

    async fn list_objects_with_id_first_chars<'a>(
        &'a self,
        settings: &Settings,
        prefix: &str,
        first_chars: &HashSet<char>,
    ) -> StorageResult<BoxStream<'a, StorageResult<ListInfo<String>>>> {
        self.meter.request("list_objects_with_id_first_chars");
        self.backend.list_objects_with_id_first_chars(settings, prefix, first_chars).await
    }

    async fn sum_object_sizes(
        &self,
        settings: &Settings,
        prefixes: &[(&str, bool)],
    ) -> StorageResult<u64> {
        self.meter.request("sum_object_sizes");
        self.backend.sum_object_sizes(settings, prefixes).await
    }

    async fn delete_batch(
        &self,
        settings: &Settings,
        prefix: &str,
        batch: Vec<(String, u64)>,
    ) -> StorageResult<DeleteObjectsResult> {
        self.meter.request("delete_batch");
        // count what the backend reports deleted, not what we asked it to
        // delete: a batch can come back `Ok` with per-key failures inside
        let result = self.backend.delete_batch(settings, prefix, batch).await?;
        self.meter.deleted("delete_batch", result.deleted_objects);
        Ok(result)
    }

    async fn get_object_last_modified(
        &self,
        path: &str,
        settings: &Settings,
    ) -> StorageResult<DateTime<Utc>> {
        self.meter.request("get_object_last_modified");
        self.backend.get_object_last_modified(path, settings).await
    }

    async fn get_object_conditional(
        &self,
        settings: &Settings,
        path: &str,
        previous_version: Option<&VersionInfo>,
    ) -> StorageResult<GetModifiedResult> {
        self.meter.request("get_object_conditional");
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
        self.meter.request("get_object_range");
        let (stream, version) =
            self.backend.get_object_range(settings, path, range).await?;
        let meter = Arc::clone(&self.meter);
        let stream = stream.inspect(move |item| {
            if let Ok(bytes) = item {
                meter.read("get_object_range", bytes.len() as u64);
            }
        });
        Ok((Box::pin(stream), version))
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use futures::TryStreamExt as _;

    use super::*;
    use crate::storage::new_in_memory_storage;

    #[tokio::test]
    async fn counts_requests_and_bytes() {
        let backend = new_in_memory_storage().await.unwrap();
        let storage = MeteringStorage::new(backend);
        let settings = storage.default_settings().await.unwrap();

        storage
            .put_object(
                &settings,
                "a/b",
                Bytes::from_static(b"hello"),
                None,
                vec![],
                None,
            )
            .await
            .unwrap();
        let (stream, _) = storage.get_object_range(&settings, "a/b", None).await.unwrap();
        let chunks: Vec<Bytes> = stream.try_collect().await.unwrap();
        assert_eq!(chunks.concat(), b"hello");
        let listed: Vec<_> = storage
            .list_objects(&settings, "")
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        assert_eq!(listed.len(), 1);
        storage.delete_batch(&settings, "a", vec![("b".to_string(), 5)]).await.unwrap();

        let report = storage.snapshot();
        let put = report.per_op["put_object"];
        assert_eq!(put.requests, 1);
        assert_eq!(put.bytes_written, 5);
        let get = report.per_op["get_object_range"];
        assert_eq!(get.requests, 1);
        assert_eq!(get.bytes_read, 5);
        let del = report.per_op["delete_batch"];
        assert_eq!(del.requests, 1);
        assert_eq!(del.objects_deleted, 1);
        let list = report.per_op["list_objects"];
        assert_eq!(list.requests, 1);

        assert!(!report.read_timeline.is_empty());
        let total: u64 = report.read_timeline.iter().map(|s| s.bytes_read).sum();
        assert_eq!(total, 5);
        let started: u64 = report.read_timeline.iter().map(|s| s.requests_started).sum();
        assert_eq!(started, 4);
    }
}
