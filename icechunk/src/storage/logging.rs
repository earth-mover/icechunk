//! Storage wrapper that logs all operations (for testing).

use std::{
    fmt,
    pin::Pin,
    sync::{Arc, Mutex},
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
        ctx: &StorageContext<'_>,
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
        self.fetch_log
            .lock()
            .expect("poison lock")
            .push(("copy_object".to_string(), format!("{from} -> {to}")));
        self.backend.copy_object_raw(ctx, from, to, content_type, version).await
    }

    async fn list_objects_raw<'a>(
        &'a self,
        ctx: &StorageContext<'_>,
        prefix: &str,
    ) -> StorageResult<BoxStream<'a, StorageResult<ListInfo<String>>>> {
        self.fetch_log
            .lock()
            .expect("poison lock")
            .push(("list_objects".to_string(), prefix.to_string()));
        self.backend.list_objects_raw(ctx, prefix).await
    }

    async fn delete_batch_raw(
        &self,
        ctx: &StorageContext<'_>,
        prefix: &str,
        batch: Vec<(String, u64)>,
    ) -> StorageResult<DeleteObjectsResult> {
        self.fetch_log
            .lock()
            .expect("poison lock")
            .push(("delete_batch".to_string(), prefix.to_string()));
        self.backend.delete_batch_raw(ctx, prefix, batch).await
    }

    async fn get_object_last_modified_raw(
        &self,
        ctx: &StorageContext<'_>,
        path: &str,
    ) -> StorageResult<DateTime<Utc>> {
        self.fetch_log
            .lock()
            .expect("poison lock")
            .push(("get_object_last_modified".to_string(), path.to_string()));
        self.backend.get_object_last_modified_raw(ctx, path).await
    }

    async fn get_object_conditional_raw(
        &self,
        ctx: &StorageContext<'_>,
        path: &str,
        reservation: &MemoryPermit,
        previous_version: Option<&VersionInfo>,
    ) -> StorageResult<GetModifiedResult> {
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
        self.fetch_log
            .lock()
            .expect("poison lock")
            .push(("get_object_range".to_string(), path.to_string()));
        self.backend.get_object_range(ctx, path, target).await
    }
}
