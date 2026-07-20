//! Storage wrapper that logs all operations (for testing).

use std::{
    fmt,
    ops::Range,
    pin::Pin,
    sync::{Arc, Mutex},
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

#[derive(Debug, Serialize, Deserialize)]
pub struct LoggingStorage {
    backend: Arc<dyn Storage + Send + Sync>,
    fetch_log: Mutex<Vec<(String, String)>>,
}

#[cfg(test)]
impl LoggingStorage {
    pub fn new(backend: Arc<dyn Storage + Send + Sync>) -> Self {
        Self { backend, fetch_log: Mutex::new(Vec::new()) }
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

    async fn sum_object_sizes(
        &self,
        settings: &Settings,
        prefix: &str,
        shardable: bool,
    ) -> StorageResult<u64> {
        self.fetch_log
            .lock()
            .expect("poison lock")
            .push(("sum_object_sizes".to_string(), prefix.to_string()));
        self.backend.sum_object_sizes(settings, prefix, shardable).await
    }

    async fn sum_object_sizes_many(
        &self,
        settings: &Settings,
        prefixes: &[(&str, bool)],
    ) -> StorageResult<u64> {
        self.fetch_log.lock().expect("poison lock").push((
            "sum_object_sizes_many".to_string(),
            prefixes.iter().map(|(p, _)| *p).collect::<Vec<_>>().join(","),
        ));
        self.backend.sum_object_sizes_many(settings, prefixes).await
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::new_in_memory_storage;

    #[tokio::test]
    async fn sum_object_sizes_many_forwards_to_backend_override()
    -> Result<(), Box<dyn std::error::Error>> {
        let backend = new_in_memory_storage().await?;
        let settings = backend.default_settings().await?;
        backend
            .put_object(&settings, "chunks/AB", "hello".into(), None, vec![], None)
            .await?;
        backend.put_object(&settings, "refs/x", "yo".into(), None, vec![], None).await?;

        let logging = LoggingStorage::new(backend);
        let prefixes: [(&str, bool); 2] = [("chunks", true), ("refs", false)];
        let total = logging.sum_object_sizes_many(&settings, &prefixes).await?;
        assert_eq!(total, "hello".len() as u64 + "yo".len() as u64);

        let ops: Vec<String> =
            logging.fetch_operations().into_iter().map(|(op, _)| op).collect();
        // Forwarded to the backend's batch override: the wrapper records exactly
        // one sum_object_sizes_many. The inherited default drain would instead
        // fan the wrapper's own per-prefix sum_object_sizes / list_objects out,
        // so their absence proves the fast path is not being bypassed here.
        assert_eq!(
            ops.iter().filter(|op| *op == "sum_object_sizes_many").count(),
            1,
            "ops: {ops:?}"
        );
        assert!(!ops.iter().any(|op| op == "list_objects"), "ops: {ops:?}");
        assert!(!ops.iter().any(|op| op == "sum_object_sizes"), "ops: {ops:?}");
        Ok(())
    }
}
