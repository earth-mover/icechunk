use std::{
    ops::Range,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use serde::{Deserialize, Serialize};
use tokio::io::AsyncRead;

use super::{ETag, ListInfo, Settings, Storage, StorageError, StorageResult};
use crate::{
    format::{ChunkId, ChunkOffset, ManifestId, SnapshotId},
    private,
};

#[derive(Debug, Serialize, Deserialize)]
pub struct LoggingStorage {
    backend: Arc<dyn Storage + Send + Sync>,
    fetch_log: Mutex<Vec<(String, Vec<u8>)>>,
}

#[cfg(test)]
impl LoggingStorage {
    pub fn new(backend: Arc<dyn Storage + Send + Sync>) -> Self {
        Self { backend, fetch_log: Mutex::new(Vec::new()) }
    }

    #[allow(clippy::expect_used)] // this implementation is intended for tests only
    pub fn fetch_operations(&self) -> Vec<(String, Vec<u8>)> {
        self.fetch_log.lock().expect("poison lock").clone()
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
    ) -> StorageResult<Box<dyn AsyncRead + Unpin + Send>> {
        self.fetch_log
            .lock()
            .expect("poison lock")
            .push(("fetch_snapshot".to_string(), id.0.to_vec()));
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
            .push(("fetch_transaction_log".to_string(), id.0.to_vec()));
        self.backend.fetch_transaction_log(settings, id).await
    }

    async fn fetch_manifest_splitting(
        &self,
        settings: &Settings,
        id: &ManifestId,
        size: u64,
    ) -> StorageResult<Box<dyn AsyncRead + Unpin + Send>> {
        self.fetch_log
            .lock()
            .expect("poison lock")
            .push(("fetch_manifest_splitting".to_string(), id.0.to_vec()));
        self.backend.fetch_manifest_splitting(settings, id, size).await
    }

    async fn fetch_manifest_single_request(
        &self,
        settings: &Settings,
        id: &ManifestId,
    ) -> StorageResult<Box<dyn AsyncRead + Unpin + Send>> {
        self.fetch_log
            .lock()
            .expect("poison lock")
            .push(("fetch_manifest_single_request".to_string(), id.0.to_vec()));
        self.backend.fetch_manifest_single_request(settings, id).await
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
            .push(("fetch_chunk".to_string(), id.0.to_vec()));
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
