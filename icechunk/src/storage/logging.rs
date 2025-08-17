use std::{
    fmt,
    ops::Range,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use bytes::{Buf, Bytes};
use chrono::{DateTime, Utc};
use futures::stream::BoxStream;
use serde::{Deserialize, Serialize};
use tokio::io::AsyncRead;

use super::{
    DeleteObjectsResult, ListInfo, Settings, Storage, StorageResult, VersionInfo,
    VersionedFetchResult, VersionedUpdateResult,
};
use crate::private;

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

    #[allow(clippy::expect_used)] // this implementation is intended for tests only
    pub fn fetch_operations(&self) -> Vec<(String, String)> {
        self.fetch_log.lock().expect("poison lock").clone()
    }

    #[allow(clippy::expect_used)] // this implementation is intended for tests only
    pub fn clear(&self) {
        self.fetch_log.lock().expect("poison lock").clear();
    }
}

impl fmt::Display for LoggingStorage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "LoggingStorage(backend={})", self.backend)
    }
}

impl private::Sealed for LoggingStorage {}

#[async_trait]
#[typetag::serde]
#[allow(clippy::expect_used)] // this implementation is intended for tests only
impl Storage for LoggingStorage {
    fn default_settings(&self) -> Settings {
        self.backend.default_settings()
    }

    fn can_write(&self) -> bool {
        self.backend.can_write()
    }

    async fn get_versioned_object(
        &self,
        path: &str,
        settings: &Settings,
    ) -> StorageResult<VersionedFetchResult<Box<dyn AsyncRead + Unpin + Send>>> {
        self.fetch_log
            .lock()
            .expect("poison lock")
            .push(("get_versioned_object".to_string(), path.to_string()));
        self.backend.get_versioned_object(path, settings).await
    }
    async fn put_versioned_object(
        &self,
        path: &str,
        bytes: Bytes,
        content_type: Option<&str>,
        metadata: Vec<(String, String)>,
        previous_version: &VersionInfo,
        settings: &Settings,
    ) -> StorageResult<VersionedUpdateResult> {
        self.fetch_log
            .lock()
            .expect("poison lock")
            .push(("put_versioned_object".to_string(), path.to_string()));
        self.backend
            .put_versioned_object(
                path,
                bytes,
                content_type,
                metadata,
                previous_version,
                settings,
            )
            .await
    }

    async fn put_object(
        &self,
        settings: &Settings,
        path: &str,
        metadata: Vec<(String, String)>,
        bytes: Bytes,
    ) -> StorageResult<()> {
        self.fetch_log
            .lock()
            .expect("poison lock")
            .push(("put_object".to_string(), path.to_string()));
        self.backend.put_object(settings, path, metadata, bytes).await
    }

    async fn ref_names(&self, settings: &Settings) -> StorageResult<Vec<String>> {
        self.backend.ref_names(settings).await
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

    async fn get_object_buf(
        &self,
        settings: &Settings,
        key: &str,
        range: &Range<u64>,
    ) -> StorageResult<Box<dyn Buf + Unpin + Send>> {
        self.fetch_log
            .lock()
            .expect("poison lock")
            .push(("get_object_buf".to_string(), key.to_string()));
        self.backend.get_object_buf(settings, key, range).await
    }

    async fn get_object_read(
        &self,
        settings: &Settings,
        key: &str,
        range: Option<&Range<u64>>,
    ) -> StorageResult<Box<dyn AsyncRead + Unpin + Send>> {
        self.fetch_log
            .lock()
            .expect("poison lock")
            .push(("get_object_read".to_string(), key.to_string()));
        self.backend.get_object_read(settings, key, range).await
    }
}
