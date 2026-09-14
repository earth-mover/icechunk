//! Storage wrapper that logs all operations (for testing).

use std::{
    collections::HashSet,
    fmt,
    ops::Range,
    pin::Pin,
    sync::{Arc, Mutex},
    time::Duration,
};

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::{Stream, stream::BoxStream};
use serde::{Deserialize, Serialize};

use super::{
    DeleteObjectsResult, GetModifiedResult, ListInfo, RepositoryCreation, Settings,
    Storage, StorageError, StorageInfo, StorageResult, VersionInfo,
    VersionedUpdateResult,
};
use icechunk_storage::sealed;

/// Records how many `put_object` calls to matching paths overlap in time, and
/// slows each one so that overlap is observable.
#[derive(Debug, Default, Serialize, Deserialize)]
struct WriteProbe {
    path_filter: String,
    delay: Option<Duration>,
    in_flight: usize,
    peak: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct LoggingStorage {
    backend: Arc<dyn Storage + Send + Sync>,
    fetch_log: Mutex<Vec<(String, String)>>,
    #[serde(skip)]
    probe: Mutex<WriteProbe>,
}

#[cfg(test)]
impl LoggingStorage {
    pub fn new(backend: Arc<dyn Storage + Send + Sync>) -> Self {
        Self {
            backend,
            fetch_log: Mutex::new(Vec::new()),
            probe: Mutex::new(WriteProbe::default()),
        }
    }

    /// Delays every `put_object` whose path contains `path_filter`, and records the
    /// peak number of such writes in flight at once.
    pub fn probing_writes(
        backend: Arc<dyn Storage + Send + Sync>,
        path_filter: &str,
        delay: Duration,
    ) -> Self {
        Self {
            backend,
            fetch_log: Mutex::new(Vec::new()),
            probe: Mutex::new(WriteProbe {
                path_filter: path_filter.to_string(),
                delay: Some(delay),
                in_flight: 0,
                peak: 0,
            }),
        }
    }

    pub fn peak_concurrent_writes(&self) -> usize {
        self.probe.lock().expect("poison lock").peak
    }

    pub fn reset_peak_concurrent_writes(&self) {
        self.probe.lock().expect("poison lock").peak = 0;
    }

    pub fn fetch_operations(&self) -> Vec<(String, String)> {
        self.fetch_log.lock().expect("poison lock").clone()
    }

    pub fn clear(&self) {
        self.fetch_log.lock().expect("poison lock").clear();
    }
}

impl fmt::Display for LoggingStorage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "LoggingStorage(backend={})", self.backend)
    }
}

impl sealed::Sealed for LoggingStorage {}

#[async_trait]
#[typetag::serde]
impl Storage for LoggingStorage {
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
        self.fetch_log
            .lock()
            .expect("poison lock")
            .push(("put_object".to_string(), path.to_string()));

        let delay = {
            let mut probe = self.probe.lock().expect("poison lock");
            match probe.delay {
                Some(delay) if path.contains(&probe.path_filter) => {
                    probe.in_flight += 1;
                    probe.peak = probe.peak.max(probe.in_flight);
                    Some(delay)
                }
                _ => None,
            }
        };
        if let Some(delay) = delay {
            tokio::time::sleep(delay).await;
        }

        let result = self
            .backend
            .put_object(settings, path, bytes, content_type, metadata, previous_version)
            .await;

        if delay.is_some() {
            self.probe.lock().expect("poison lock").in_flight -= 1;
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
        self.fetch_log
            .lock()
            .expect("poison lock")
            .push(("copy_object".to_string(), format!("{from} -> {to}")));
        self.backend.copy_object(settings, from, to, content_type, version).await
    }

    async fn list_objects<'a>(
        &'a self,
        settings: &Settings,
        prefix: &str,
    ) -> StorageResult<BoxStream<'a, StorageResult<ListInfo<String>>>> {
        self.fetch_log
            .lock()
            .expect("poison lock")
            .push(("list_objects".to_string(), prefix.to_string()));
        self.backend.list_objects(settings, prefix).await
    }

    async fn list_objects_with_id_first_chars<'a>(
        &'a self,
        settings: &Settings,
        prefix: &str,
        first_chars: &HashSet<char>,
    ) -> StorageResult<BoxStream<'a, StorageResult<ListInfo<String>>>> {
        self.fetch_log
            .lock()
            .expect("poison lock")
            .push(("list_objects_with_id_first_chars".to_string(), prefix.to_string()));
        self.backend.list_objects_with_id_first_chars(settings, prefix, first_chars).await
    }

    async fn delete_batch(
        &self,
        settings: &Settings,
        prefix: &str,
        batch: Vec<(String, u64)>,
    ) -> StorageResult<DeleteObjectsResult> {
        self.fetch_log
            .lock()
            .expect("poison lock")
            .push(("delete_batch".to_string(), prefix.to_string()));
        self.backend.delete_batch(settings, prefix, batch).await
    }

    async fn get_object_last_modified(
        &self,
        path: &str,
        settings: &Settings,
    ) -> StorageResult<DateTime<Utc>> {
        self.fetch_log
            .lock()
            .expect("poison lock")
            .push(("get_object_last_modified".to_string(), path.to_string()));
        self.backend.get_object_last_modified(path, settings).await
    }

    async fn get_object_conditional(
        &self,
        settings: &Settings,
        path: &str,
        previous_version: Option<&VersionInfo>,
    ) -> StorageResult<GetModifiedResult> {
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
        self.fetch_log
            .lock()
            .expect("poison lock")
            .push(("get_object_range".to_string(), path.to_string()));
        self.backend.get_object_range(settings, path, range).await
    }
}
