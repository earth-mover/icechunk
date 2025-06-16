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
    DeleteObjectsResult, FetchConfigResult, GetRefResult, ListInfo, Reader, Settings,
    Storage, StorageError, StorageResult, UpdateConfigResult, VersionInfo,
    WriteRefResult,
};
use crate::{
    format::{ChunkId, ChunkOffset, ManifestId, SnapshotId},
    private,
};

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

    async fn fetch_config(
        &self,
        settings: &Settings,
    ) -> StorageResult<FetchConfigResult> {
        self.backend.fetch_config(settings).await
    }
    async fn update_config(
        &self,
        settings: &Settings,
        config: Bytes,
        previous_version: &VersionInfo,
    ) -> StorageResult<UpdateConfigResult> {
        self.backend.update_config(settings, config, previous_version).await
    }

    async fn fetch_snapshot(
        &self,
        settings: &Settings,
        id: &SnapshotId,
    ) -> StorageResult<Box<dyn AsyncRead + Unpin + Send>> {
        self.fetch_log
            .lock()
            .expect("poison lock")
            .push(("fetch_snapshot".to_string(), id.to_string()));
        self.backend.fetch_snapshot(settings, id).await
    }

    async fn fetch_transaction_log(
        &self,
        settings: &Settings,
        id: &SnapshotId,
    ) -> StorageResult<Box<dyn AsyncRead + Unpin + Send>> {
        self.fetch_log
            .lock()
            .expect("poison lock")
            .push(("fetch_transaction_log".to_string(), id.to_string()));
        self.backend.fetch_transaction_log(settings, id).await
    }

    async fn fetch_manifest_known_size(
        &self,
        settings: &Settings,
        id: &ManifestId,
        size: u64,
    ) -> StorageResult<Reader> {
        self.fetch_log
            .lock()
            .expect("poison lock")
            .push(("fetch_manifest_splitting".to_string(), id.to_string()));
        self.backend.fetch_manifest_known_size(settings, id, size).await
    }

    async fn fetch_manifest_unknown_size(
        &self,
        settings: &Settings,
        id: &ManifestId,
    ) -> StorageResult<Box<dyn AsyncRead + Unpin + Send>> {
        self.fetch_log
            .lock()
            .expect("poison lock")
            .push(("fetch_manifest_single_request".to_string(), id.to_string()));
        self.backend.fetch_manifest_unknown_size(settings, id).await
    }

    async fn fetch_chunk(
        &self,
        settings: &Settings,
        id: &ChunkId,
        range: &Range<ChunkOffset>,
    ) -> Result<Bytes, StorageError> {
        self.fetch_log
            .lock()
            .expect("poison lock")
            .push(("fetch_chunk".to_string(), id.to_string()));
        self.backend.fetch_chunk(settings, id, range).await
    }

    async fn write_snapshot(
        &self,
        settings: &Settings,
        id: SnapshotId,
        metadata: Vec<(String, String)>,
        bytes: Bytes,
    ) -> StorageResult<()> {
        self.backend.write_snapshot(settings, id, metadata, bytes).await
    }

    async fn write_transaction_log(
        &self,
        settings: &Settings,
        id: SnapshotId,
        metadata: Vec<(String, String)>,
        bytes: Bytes,
    ) -> StorageResult<()> {
        self.backend.write_transaction_log(settings, id, metadata, bytes).await
    }

    async fn write_manifest(
        &self,
        settings: &Settings,
        id: ManifestId,
        metadata: Vec<(String, String)>,
        bytes: Bytes,
    ) -> StorageResult<()> {
        self.backend.write_manifest(settings, id, metadata, bytes).await
    }

    async fn write_chunk(
        &self,
        settings: &Settings,
        id: ChunkId,
        bytes: Bytes,
    ) -> Result<(), StorageError> {
        self.backend.write_chunk(settings, id, bytes).await
    }

    async fn get_ref(
        &self,
        settings: &Settings,
        ref_key: &str,
    ) -> StorageResult<GetRefResult> {
        self.backend.get_ref(settings, ref_key).await
    }

    async fn ref_names(&self, settings: &Settings) -> StorageResult<Vec<String>> {
        self.backend.ref_names(settings).await
    }

    async fn write_ref(
        &self,
        settings: &Settings,
        ref_key: &str,
        bytes: Bytes,
        previous_version: &VersionInfo,
    ) -> StorageResult<WriteRefResult> {
        self.backend.write_ref(settings, ref_key, bytes, previous_version).await
    }

    async fn list_objects<'a>(
        &'a self,
        settings: &Settings,
        prefix: &str,
    ) -> StorageResult<BoxStream<'a, StorageResult<ListInfo<String>>>> {
        self.backend.list_objects(settings, prefix).await
    }

    async fn delete_batch(
        &self,
        settings: &Settings,
        prefix: &str,
        batch: Vec<(String, u64)>,
    ) -> StorageResult<DeleteObjectsResult> {
        self.backend.delete_batch(settings, prefix, batch).await
    }

    async fn get_snapshot_last_modified(
        &self,
        settings: &Settings,
        snapshot: &SnapshotId,
    ) -> StorageResult<DateTime<Utc>> {
        self.backend.get_snapshot_last_modified(settings, snapshot).await
    }

    async fn get_object_range_buf(
        &self,
        settings: &Settings,
        key: &str,
        range: &Range<u64>,
    ) -> StorageResult<Box<dyn Buf + Unpin + Send>> {
        self.backend.get_object_range_buf(settings, key, range).await
    }

    async fn get_object_range_read(
        &self,
        settings: &Settings,
        key: &str,
        range: &Range<u64>,
    ) -> StorageResult<Box<dyn AsyncRead + Unpin + Send>> {
        self.backend.get_object_range_read(settings, key, range).await
    }
}
