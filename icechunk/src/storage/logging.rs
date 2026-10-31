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
    Storage, StorageContext, StorageError, StorageInfo, StorageResult, VersionInfo,
    VersionedUpdateResult,
};
use icechunk_storage::sealed;

#[derive(Debug, Serialize, Deserialize)]
pub struct LoggingStorage {
    backend: Arc<dyn Storage + Send + Sync>,
    fetch_log: Mutex<Vec<(String, String)>>,
    /// What to report from `lists_id_prefixes_natively`: `None` defers to the
    /// backend, `Some` overrides it either way.
    #[serde(default)]
    native_id_prefixes: Option<bool>,
}

#[cfg(test)]
impl LoggingStorage {
    pub fn new(backend: Arc<dyn Storage + Send + Sync>) -> Self {
        Self { backend, fetch_log: Mutex::new(Vec::new()), native_id_prefixes: None }
    }

    /// Report `native` from `lists_id_prefixes_natively` instead of what the
    /// backend says.
    pub fn with_native_id_prefixes(mut self, native: bool) -> Self {
        self.native_id_prefixes = Some(native);
        self
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

    async fn copy_object(
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
        self.backend.copy_object(ctx, from, to, content_type, version).await
    }

    async fn list_objects<'a>(
        &'a self,
        ctx: &StorageContext<'_>,
        prefix: &str,
    ) -> StorageResult<BoxStream<'a, StorageResult<ListInfo<String>>>> {
        self.fetch_log
            .lock()
            .expect("poison lock")
            .push(("list_objects".to_string(), prefix.to_string()));
        self.backend.list_objects(ctx, prefix).await
    }

    async fn list_objects_with_id_prefixes<'a>(
        &'a self,
        ctx: &StorageContext<'_>,
        prefix: &str,
        id_prefixes: &[String],
    ) -> StorageResult<BoxStream<'a, StorageResult<ListInfo<String>>>> {
        self.fetch_log
            .lock()
            .expect("poison lock")
            .push(("list_objects_with_id_prefixes".to_string(), prefix.to_string()));
        self.backend.list_objects_with_id_prefixes(ctx, prefix, id_prefixes).await
    }

    fn lists_id_prefixes_natively(&self) -> bool {
        self.native_id_prefixes
            .unwrap_or_else(|| self.backend.lists_id_prefixes_natively())
    }

    async fn delete_batch(
        &self,
        ctx: &StorageContext<'_>,
        prefix: &str,
        batch: Vec<(String, u64)>,
    ) -> StorageResult<DeleteObjectsResult> {
        self.fetch_log
            .lock()
            .expect("poison lock")
            .push(("delete_batch".to_string(), prefix.to_string()));
        self.backend.delete_batch(ctx, prefix, batch).await
    }

    async fn get_object_last_modified(
        &self,
        ctx: &StorageContext<'_>,
        path: &str,
    ) -> StorageResult<DateTime<Utc>> {
        self.fetch_log
            .lock()
            .expect("poison lock")
            .push(("get_object_last_modified".to_string(), path.to_string()));
        self.backend.get_object_last_modified(ctx, path).await
    }

    async fn get_object_conditional(
        &self,
        ctx: &StorageContext<'_>,
        path: &str,
        previous_version: Option<&VersionInfo>,
    ) -> StorageResult<GetModifiedResult> {
        self.backend.get_object_conditional(ctx, path, previous_version).await
    }

    async fn get_object_range(
        &self,
        ctx: &StorageContext<'_>,
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
        self.backend.get_object_range(ctx, path, range).await
    }
}
