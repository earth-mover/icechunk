//! Typed I/O layer for Icechunk assets.
//!
//! Sits between [`crate::session::Session`] and [`Storage`], handling serialization
//! and caching. While [`Storage`] provides generic object store operations (bytes
//! in/out), this module works with typed Icechunk assets: snapshots, manifests,
//! transaction logs, and chunks.

use async_stream::try_stream;
use backon::{BackoffBuilder, ExponentialBuilder, Retryable};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::{Stream, StreamExt as _, TryStreamExt, stream::BoxStream};
use quick_cache::{Weighter, sync::Cache};
use serde::{Deserialize, Serialize};
use std::{
    io::{BufReader, Read},
    ops::Range,
    pin::Pin,
    sync::{Arc, atomic::AtomicBool},
    time::Duration,
};
use tokio::{
    io::{AsyncBufRead, AsyncReadExt},
    sync::Semaphore,
};
use tokio_util::io::SyncIoBridge;
use tracing::{Span, debug, instrument, trace, warn};

use crate::{
    RepositoryConfig, Storage, StorageError,
    config::CachingConfig,
    format::{
        CHUNKS_FILE_PATH, CONFIG_FILE_PATH, ChunkId, ChunkOffset,
        IcechunkFormatErrorKind, MANIFESTS_FILE_PATH, ManifestId, OVERWRITTEN_FILES_PATH,
        REPO_INFO_FILE_PATH, SNAPSHOTS_FILE_PATH, SnapshotId, TRANSACTION_LOGS_FILE_PATH,
        format_constants::{self, CompressionAlgorithmBin, FileTypeBin, SpecVersionBin},
        manifest::Manifest,
        repo_info::RepoInfo,
        serializers::{
            deserialize_manifest, deserialize_repo_info, deserialize_snapshot,
            deserialize_transaction_log, serialize_manifest, serialize_repo_info,
            serialize_snapshot, serialize_transaction_log,
        },
        snapshot::{Snapshot, SnapshotInfo},
        transaction_log::TransactionLog,
    },
    private,
    repository::{RepositoryError, RepositoryErrorKind, RepositoryResult},
    storage::{
        self, DeleteObjectsResult, ListInfo, StorageErrorKind, VersionInfo,
        VersionedUpdateResult,
    },
};

/// Reads and writes Icechunk assets with caching and concurrency control.
#[derive(Debug, Serialize, Deserialize)]
#[serde(from = "AssetManagerSerializer")]
pub struct AssetManager {
    storage: Arc<dyn Storage + Send + Sync>,
    storage_settings: storage::Settings,
    spec_version: SpecVersionBin,
    num_snapshot_nodes: u64,
    num_chunk_refs: u64,
    num_transaction_changes: u64,
    num_bytes_attributes: u64,
    num_bytes_chunks: u64,
    compression_level: u8,

    max_concurrent_requests: u16,

    #[serde(skip)]
    manifest_cache_size_warned: AtomicBool,
    #[serde(skip)]
    snapshot_cache_size_warned: AtomicBool,

    #[serde(skip)]
    snapshot_cache: Cache<SnapshotId, Arc<Snapshot>, FileWeighter>,
    #[serde(skip)]
    manifest_cache: Cache<ManifestId, Arc<Manifest>, FileWeighter>,
    #[serde(skip)]
    transactions_cache: Cache<SnapshotId, Arc<TransactionLog>, FileWeighter>,
    #[serde(skip)]
    chunk_cache: Cache<(ChunkId, Range<ChunkOffset>), Bytes, FileWeighter>,

    #[serde(skip)]
    request_semaphore: Semaphore,
}

impl private::Sealed for AssetManager {}

#[derive(Debug, Deserialize)]
struct AssetManagerSerializer {
    storage: Arc<dyn Storage + Send + Sync>,
    storage_settings: storage::Settings,
    spec_version: SpecVersionBin,
    num_snapshot_nodes: u64,
    num_chunk_refs: u64,
    num_transaction_changes: u64,
    num_bytes_attributes: u64,
    num_bytes_chunks: u64,
    compression_level: u8,
    max_concurrent_requests: u16,
}

impl From<AssetManagerSerializer> for AssetManager {
    fn from(value: AssetManagerSerializer) -> Self {
        AssetManager::new(
            value.storage,
            value.storage_settings,
            value.spec_version,
            value.num_snapshot_nodes,
            value.num_chunk_refs,
            value.num_transaction_changes,
            value.num_bytes_attributes,
            value.num_bytes_chunks,
            value.compression_level,
            value.max_concurrent_requests,
        )
    }
}

impl AssetManager {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        storage: Arc<dyn Storage + Send + Sync>,
        storage_settings: storage::Settings,
        spec_version: SpecVersionBin,
        num_snapshot_nodes: u64,
        num_chunk_refs: u64,
        num_transaction_changes: u64,
        num_bytes_attributes: u64,
        num_bytes_chunks: u64,
        compression_level: u8,
        max_concurrent_requests: u16,
    ) -> Self {
        Self {
            num_snapshot_nodes,
            num_chunk_refs,
            num_transaction_changes,
            num_bytes_attributes,
            num_bytes_chunks,
            compression_level,
            max_concurrent_requests,
            storage,
            storage_settings,
            spec_version,
            snapshot_cache: Cache::with_weighter(1, num_snapshot_nodes, FileWeighter),
            manifest_cache: Cache::with_weighter(1, num_chunk_refs, FileWeighter),
            transactions_cache: Cache::with_weighter(
                0,
                num_transaction_changes,
                FileWeighter,
            ),
            chunk_cache: Cache::with_weighter(0, num_bytes_chunks, FileWeighter),
            snapshot_cache_size_warned: AtomicBool::new(false),
            manifest_cache_size_warned: AtomicBool::new(false),
            request_semaphore: Semaphore::new(max_concurrent_requests as usize),
        }
    }

    pub fn new_no_cache(
        storage: Arc<dyn Storage + Send + Sync>,
        storage_settings: storage::Settings,
        spec_version: SpecVersionBin,
        compression_level: u8,
        max_concurrent_requests: u16,
    ) -> Self {
        Self::new(
            storage,
            storage_settings,
            spec_version,
            0,
            0,
            0,
            0,
            0,
            compression_level,
            max_concurrent_requests,
        )
    }

    pub fn new_with_config(
        storage: Arc<dyn Storage + Send + Sync>,
        storage_settings: storage::Settings,
        spec_version: SpecVersionBin,
        config: &CachingConfig,
        compression_level: u8,
        max_concurrent_requests: u16,
    ) -> Self {
        Self::new(
            storage,
            storage_settings,
            spec_version,
            config.num_snapshot_nodes(),
            config.num_chunk_refs(),
            config.num_transaction_changes(),
            config.num_bytes_attributes(),
            config.num_bytes_chunks(),
            compression_level,
            max_concurrent_requests,
        )
    }

    pub fn clone_for_spec_version(&self, spec_version: SpecVersionBin) -> Self {
        Self::new(
            Arc::clone(&self.storage),
            self.storage_settings.clone(),
            spec_version,
            self.num_snapshot_nodes,
            self.num_chunk_refs,
            self.num_transaction_changes,
            self.num_bytes_attributes,
            self.num_bytes_chunks,
            self.compression_level,
            self.max_concurrent_requests,
        )
    }

    pub fn spec_version(&self) -> SpecVersionBin {
        self.spec_version
    }

    pub fn remove_cached_snapshot(&self, snapshot_id: &SnapshotId) {
        self.snapshot_cache.remove(snapshot_id);
    }

    pub fn remove_cached_manifest(&self, manifest_id: &ManifestId) {
        self.manifest_cache.remove(manifest_id);
    }

    pub fn remove_cached_tx_log(&self, snapshot_id: &SnapshotId) {
        self.transactions_cache.remove(snapshot_id);
    }

    pub fn clear_chunk_cache(&self) {
        self.chunk_cache.clear();
    }

    pub async fn fetch_config(
        &self,
    ) -> RepositoryResult<Option<(RepositoryConfig, VersionInfo)>> {
        match self
            .storage
            .get_object(&self.storage_settings, CONFIG_FILE_PATH, None)
            .await
        {
            Ok((mut result, version)) => {
                let mut data = Vec::with_capacity(1_024);
                result.read_to_end(&mut data).await?;
                let config = serde_yaml_ng::from_slice(data.as_slice())?;
                Ok(Some((config, version)))
            }
            Err(StorageError { kind: StorageErrorKind::ObjectNotFound, .. }) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub async fn try_update_config(
        &self,
        config: &RepositoryConfig,
        previous_version: &VersionInfo,
        backup_path: Option<&str>,
    ) -> RepositoryResult<Option<VersionInfo>> {
        let bytes = Bytes::from(serde_yaml_ng::to_string(config)?);
        let content_type = Some("application/yaml");
        if let Some(backup_path) = backup_path {
            let backup_path = format!("{OVERWRITTEN_FILES_PATH}/{backup_path}");
            match self
                .storage
                .copy_object(
                    &self.storage_settings,
                    CONFIG_FILE_PATH,
                    backup_path.as_str(),
                    content_type,
                    previous_version,
                )
                .await?
            {
                VersionedUpdateResult::Updated { .. } => {}
                VersionedUpdateResult::NotOnLatestVersion => return Ok(None),
            }
        }
        match self
            .storage
            .put_object(
                &self.storage_settings,
                CONFIG_FILE_PATH,
                bytes,
                content_type,
                Vec::new(),
                Some(previous_version),
            )
            .await?
        {
            VersionedUpdateResult::Updated { new_version } => Ok(Some(new_version)),
            VersionedUpdateResult::NotOnLatestVersion => Ok(None),
        }
    }

    #[instrument(skip(self, manifest))]
    pub async fn write_manifest(&self, manifest: Arc<Manifest>) -> RepositoryResult<u64> {
        let manifest_c = Arc::clone(&manifest);
        let res = write_new_manifest(
            manifest_c,
            self.spec_version(),
            self.compression_level,
            self.storage.as_ref(),
            &self.storage_settings,
            &self.request_semaphore,
        )
        .await?;
        self.warn_if_manifest_cache_small(manifest.as_ref());
        self.manifest_cache.insert(manifest.id().clone(), manifest);
        Ok(res)
    }

    #[instrument(skip(self))]
    pub async fn fetch_manifest(
        &self,
        manifest_id: &ManifestId,
        manifest_size: u64,
    ) -> RepositoryResult<Arc<Manifest>> {
        match self.manifest_cache.get_value_or_guard_async(manifest_id).await {
            Ok(manifest) => Ok(manifest),
            Err(guard) => {
                let manifest = fetch_manifest(
                    manifest_id,
                    manifest_size,
                    self.storage.as_ref(),
                    &self.storage_settings,
                    &self.request_semaphore,
                )
                .await?;
                self.warn_if_manifest_cache_small(manifest.as_ref());
                let _fail_is_ok = guard.insert(Arc::clone(&manifest));
                Ok(manifest)
            }
        }
    }

    fn warn_if_manifest_cache_small(&self, manifest: &Manifest) {
        let manifest_weight = manifest.len();
        let capacity = self.num_chunk_refs;
        // TODO: we may need a config to silence this warning
        if manifest_weight as u64 > capacity / 2
            && !self.manifest_cache_size_warned.load(std::sync::atomic::Ordering::Relaxed)
        {
            warn!(
                "A manifest with {manifest_weight} chunk references is being loaded into the cache that can only keep {capacity} references. Consider increasing the size of the manifest cache using the num_chunk_refs field in CachingConfig"
            );
            self.manifest_cache_size_warned
                .store(true, std::sync::atomic::Ordering::Relaxed);
        }
    }

    fn warn_if_snapshot_cache_small(&self, snap: &Snapshot) {
        let snap_weight = snap.len();
        let capacity = self.num_snapshot_nodes;
        // TODO: we may need a config to silence this warning
        if snap_weight as u64 > capacity / 5
            && !self.snapshot_cache_size_warned.load(std::sync::atomic::Ordering::Relaxed)
        {
            warn!(
                "A snapshot with {snap_weight} nodes is being loaded into the cache that can only keep {capacity} nodes. Consider increasing the size of the snapshot cache using the num_snapshot_nodes field in CachingConfig"
            );
            self.snapshot_cache_size_warned
                .store(true, std::sync::atomic::Ordering::Relaxed);
        }
    }

    #[instrument(skip(self,))]
    pub async fn fetch_manifest_unknown_size(
        &self,
        manifest_id: &ManifestId,
    ) -> RepositoryResult<Arc<Manifest>> {
        self.fetch_manifest(manifest_id, 0).await
    }

    #[instrument(skip(self, snapshot))]
    pub async fn write_snapshot(&self, snapshot: Arc<Snapshot>) -> RepositoryResult<()> {
        let snapshot_c = Arc::clone(&snapshot);
        write_new_snapshot(
            snapshot_c,
            self.spec_version(),
            self.compression_level,
            self.storage.as_ref(),
            &self.storage_settings,
            &self.request_semaphore,
        )
        .await?;
        let snapshot_id = snapshot.id().clone();
        self.warn_if_snapshot_cache_small(snapshot.as_ref());
        // This line is critical for expiration:
        // When we edit snapshots in place, we need the cache to return the new version
        self.snapshot_cache.insert(snapshot_id, snapshot);
        Ok(())
    }

    #[instrument(skip(self))]
    pub async fn fetch_snapshot(
        &self,
        snapshot_id: &SnapshotId,
    ) -> RepositoryResult<Arc<Snapshot>> {
        match self.snapshot_cache.get_value_or_guard_async(snapshot_id).await {
            Ok(snapshot) => Ok(snapshot),
            Err(guard) => {
                let snapshot = fetch_snapshot(
                    snapshot_id,
                    self.storage.as_ref(),
                    &self.storage_settings,
                    &self.request_semaphore,
                )
                .await?;
                self.warn_if_snapshot_cache_small(snapshot.as_ref());
                let _fail_is_ok = guard.insert(Arc::clone(&snapshot));
                Ok(snapshot)
            }
        }
    }

    #[instrument(skip(self, log))]
    pub async fn write_transaction_log(
        &self,
        transaction_id: SnapshotId,
        log: Arc<TransactionLog>,
    ) -> RepositoryResult<()> {
        let log_c = Arc::clone(&log);
        write_new_tx_log(
            transaction_id.clone(),
            log_c,
            self.spec_version(),
            self.compression_level,
            self.storage.as_ref(),
            &self.storage_settings,
            &self.request_semaphore,
        )
        .await?;
        self.transactions_cache.insert(transaction_id, log);
        Ok(())
    }

    #[instrument(skip(self))]
    pub async fn fetch_transaction_log(
        &self,
        transaction_id: &SnapshotId,
    ) -> RepositoryResult<Arc<TransactionLog>> {
        match self.transactions_cache.get_value_or_guard_async(transaction_id).await {
            Ok(transaction) => Ok(transaction),
            Err(guard) => {
                let transaction = fetch_transaction_log(
                    transaction_id,
                    self.storage.as_ref(),
                    &self.storage_settings,
                    &self.request_semaphore,
                )
                .await?;
                let _fail_is_ok = guard.insert(Arc::clone(&transaction));
                Ok(transaction)
            }
        }
    }

    #[instrument(skip(self))]
    pub async fn fetch_repo_info(
        &self,
    ) -> RepositoryResult<(Arc<RepoInfo>, VersionInfo)> {
        self.fail_unless_spec_at_least(SpecVersionBin::V2dot0)?;
        fetch_repo_info(self.storage.as_ref(), &self.storage_settings).await
    }

    pub fn fail_unless_spec_at_least(
        &self,
        minimum_spec_version: SpecVersionBin,
    ) -> RepositoryResult<()> {
        if self.spec_version() < minimum_spec_version {
            Err(RepositoryErrorKind::BadRepoVersion { minimum_spec_version }.into())
        } else {
            Ok(())
        }
    }

    #[instrument(skip(self))]
    pub async fn fetch_repo_info_backup(
        &self,
        file_name: &str,
    ) -> RepositoryResult<(Arc<RepoInfo>, VersionInfo)> {
        fetch_repo_info_backup(self.storage.as_ref(), &self.storage_settings, file_name)
            .await
    }

    #[instrument(skip(self, info))]
    pub async fn create_repo_info(
        &self,
        info: Arc<RepoInfo>,
    ) -> RepositoryResult<VersionInfo> {
        write_repo_info(
            info,
            self.spec_version(),
            &storage::VersionInfo::for_creation(),
            self.compression_level,
            None,
            self.storage.as_ref(),
            &self.storage_settings,
            None,
        )
        .await
    }

    #[instrument(skip(self, retry_settings, update))]
    pub async fn update_repo_info(
        &self,
        retry_settings: &storage::RetriesSettings,
        mut update: impl FnMut(Arc<RepoInfo>, &str) -> RepositoryResult<Arc<RepoInfo>>,
    ) -> RepositoryResult<VersionInfo> {
        let max_attempts = retry_settings.max_tries().get() as usize;

        // The first few retries are immediate (no delay) since brief contention
        // typically resolves within one or two attempts. After that, we switch to
        // exponential backoff with jitter.
        let immediate_retries: u64 = 5;
        let mut backoff = ExponentialBuilder::new()
            .with_min_delay(std::time::Duration::from_millis(
                retry_settings.initial_backoff_ms() as u64,
            ))
            .with_max_delay(std::time::Duration::from_millis(
                retry_settings.max_backoff_ms() as u64,
            ))
            .with_max_times(max_attempts.saturating_sub(immediate_retries as usize))
            .with_jitter()
            .build();

        let mut attempts: u64 = 1;
        loop {
            let (repo_info, repo_version) = self.fetch_repo_info().await?;
            let backup_path = self.backup_path_for_repo_info();
            let new_repo = update(repo_info, backup_path.as_str())?;
            trace!(attempts, "Attempting to update repo object");
            match write_repo_info(
                Arc::clone(&new_repo),
                self.spec_version(),
                &repo_version,
                self.compression_level,
                Some(backup_path.as_str()),
                self.storage.as_ref(),
                &self.storage_settings,
                None,
            )
            .await
            {
                res @ Ok(_) => {
                    debug!(attempts, "Repo info object updated successfully");
                    return res;
                }
                Err(RepositoryError {
                    kind: RepositoryErrorKind::RepoInfoUpdated,
                    ..
                }) => {
                    if attempts <= immediate_retries {
                        debug!(
                            attempts,
                            "Repo info object was updated concurrently, retrying immediately..."
                        );
                    } else {
                        match backoff.next() {
                            Some(delay) => {
                                debug!(
                                    attempts,
                                    ?delay,
                                    "Repo info object was updated concurrently, retrying with backoff..."
                                );
                                tokio::time::sleep(delay).await;
                            }
                            None => {
                                return Err(
                                    RepositoryErrorKind::RepoUpdateAttemptsLimit(
                                        max_attempts as u64,
                                    )
                                    .into(),
                                );
                            }
                        }
                    }
                    attempts += 1;
                }
                err @ Err(_) => {
                    return err;
                }
            }
        }
    }

    #[instrument(skip(self, bytes))]
    pub async fn write_chunk(
        &self,
        chunk_id: ChunkId,
        bytes: Bytes,
    ) -> RepositoryResult<()> {
        trace!(%chunk_id, size_bytes=bytes.len(), "Writing chunk");

        let path = format!("{CHUNKS_FILE_PATH}/{chunk_id}");
        let _permit = self.request_semaphore.acquire().await?;
        let settings = storage::Settings {
            storage_class: self.storage_settings.chunks_storage_class().cloned(),
            ..self.storage_settings.clone()
        };
        // we don't pre-populate the chunk cache, there are too many of them for this to be useful
        self.storage
            .put_object(&settings, path.as_str(), bytes, None, Default::default(), None)
            .await?
            .must_write()?;
        Ok(())
    }

    #[instrument(skip(self))]
    pub async fn fetch_chunk(
        &self,
        chunk_id: &ChunkId,
        range: &Range<ChunkOffset>,
    ) -> RepositoryResult<Bytes> {
        let key = (chunk_id.clone(), range.clone());
        match self.chunk_cache.get_value_or_guard_async(&key).await {
            Ok(chunk) => Ok(chunk),
            Err(guard) => {
                trace!(%chunk_id, ?range, "Downloading chunk");
                let path = format!("{CHUNKS_FILE_PATH}/{chunk_id}");
                let chunk = (|| async {
                    let permit = self.request_semaphore.acquire().await?;
                    let (read, _) = self
                        .storage
                        .get_object(&self.storage_settings, &path, Some(range))
                        .await?;
                    let bytes_result =
                        async_reader_to_bytes(read, (range.end - range.start) as usize)
                            .await
                            .map_err(RepositoryError::from);
                    drop(permit);
                    bytes_result
                })
                .retry(
                    ExponentialBuilder::new()
                        .with_max_times(
                            self.storage_settings.retries().max_tries().get().into(),
                        )
                        .with_min_delay(Duration::from_millis(
                            self.storage_settings.retries().initial_backoff_ms().into(),
                        ))
                        .with_max_delay(Duration::from_millis(
                            self.storage_settings.retries().max_backoff_ms().into(),
                        )),
                )
                .when(|e| format!("{e:?}").contains("StreamingError"))
                .notify(|_err, duration| {
                    debug!("retrying on stalled stream error after {:?}.", duration)
                })
                .await?;
                let _fail_is_ok = guard.insert(chunk.clone());
                Ok(chunk)
            }
        }
    }

    #[instrument(skip(self))]
    pub async fn get_snapshot_last_modified(
        &self,
        snapshot_id: &SnapshotId,
    ) -> RepositoryResult<DateTime<Utc>> {
        debug!(%snapshot_id, "Getting snapshot timestamp");
        let path = format!("{SNAPSHOTS_FILE_PATH}/{snapshot_id}");
        let _permit = self.request_semaphore.acquire().await?;
        Ok(self
            .storage
            .get_object_last_modified(path.as_str(), &self.storage_settings)
            .await?)
    }

    #[instrument(skip(self))]
    pub async fn fetch_snapshot_info(
        &self,
        snapshot_id: &SnapshotId,
    ) -> RepositoryResult<SnapshotInfo> {
        let snapshot = self.fetch_snapshot(snapshot_id).await?;
        let info = snapshot.as_ref().try_into()?;
        Ok(info)
    }

    #[instrument(skip(self))]
    pub async fn list_chunks(
        &self,
    ) -> RepositoryResult<BoxStream<'_, RepositoryResult<ListInfo<ChunkId>>>> {
        Ok(translate_list_infos(
            self.storage
                .list_objects(&self.storage_settings, CHUNKS_FILE_PATH)
                .await?
                .err_into(),
        ))
    }

    #[instrument(skip(self))]
    pub async fn list_manifests(
        &self,
    ) -> RepositoryResult<BoxStream<'_, RepositoryResult<ListInfo<ManifestId>>>> {
        Ok(translate_list_infos(
            self.storage
                .list_objects(&self.storage_settings, MANIFESTS_FILE_PATH)
                .await?
                .err_into(),
        ))
    }

    #[instrument(skip(self))]
    pub async fn list_snapshots(
        &self,
    ) -> RepositoryResult<BoxStream<'_, RepositoryResult<ListInfo<SnapshotId>>>> {
        Ok(translate_list_infos(
            self.storage
                .list_objects(&self.storage_settings, SNAPSHOTS_FILE_PATH)
                .await?
                .err_into(),
        ))
    }

    #[instrument(skip(self))]
    pub async fn list_transaction_logs(
        &self,
    ) -> RepositoryResult<BoxStream<'_, RepositoryResult<ListInfo<SnapshotId>>>> {
        Ok(translate_list_infos(
            self.storage
                .list_objects(&self.storage_settings, TRANSACTION_LOGS_FILE_PATH)
                .await?
                .err_into(),
        ))
    }

    pub async fn delete_chunks(
        &self,
        chunks: BoxStream<'_, (ChunkId, u64)>,
    ) -> RepositoryResult<DeleteObjectsResult> {
        Ok(self
            .storage
            .delete_objects(
                &self.storage_settings,
                CHUNKS_FILE_PATH,
                chunks.map(|(id, size)| (id.to_string(), size)).boxed(),
            )
            .await?)
    }

    pub async fn delete_manifests(
        &self,
        manifests: BoxStream<'_, (ManifestId, u64)>,
    ) -> RepositoryResult<DeleteObjectsResult> {
        Ok(self
            .storage
            .delete_objects(
                &self.storage_settings,
                MANIFESTS_FILE_PATH,
                manifests.map(|(id, size)| (id.to_string(), size)).boxed(),
            )
            .await?)
    }

    pub async fn delete_snapshots(
        &self,
        snapshots: BoxStream<'_, (SnapshotId, u64)>,
    ) -> RepositoryResult<DeleteObjectsResult> {
        Ok(self
            .storage
            .delete_objects(
                &self.storage_settings,
                SNAPSHOTS_FILE_PATH,
                snapshots.map(|(id, size)| (id.to_string(), size)).boxed(),
            )
            .await?)
    }

    pub async fn delete_transaction_logs(
        &self,
        transaction_logs: BoxStream<'_, (SnapshotId, u64)>,
    ) -> RepositoryResult<DeleteObjectsResult> {
        Ok(self
            .storage
            .delete_objects(
                &self.storage_settings,
                TRANSACTION_LOGS_FILE_PATH,
                transaction_logs.map(|(id, size)| (id.to_string(), size)).boxed(),
            )
            .await?)
    }

    pub async fn can_write_to_storage(&self) -> RepositoryResult<bool> {
        Ok(self.storage.can_write().await?)
    }

    pub async fn list_overwritten_objects(
        &self,
    ) -> RepositoryResult<BoxStream<'_, RepositoryResult<String>>> {
        let stream = self
            .storage
            .list_objects(&self.storage_settings, OVERWRITTEN_FILES_PATH)
            .await?
            .map_ok(|li| li.id)
            .err_into()
            .boxed();
        Ok(stream)
    }

    pub fn backup_path_for_config(&self) -> String {
        backup_destination(CONFIG_FILE_PATH)
    }

    pub fn backup_path_for_repo_info(&self) -> String {
        backup_destination(REPO_INFO_FILE_PATH)
    }

    #[deprecated(
        since = "2.0.0",
        note = "Shouldn't be necessary after 2.0, only to support Icechunk 1 repos"
    )]
    pub fn storage(&self) -> &Arc<dyn Storage + Send + Sync> {
        &self.storage
    }

    #[deprecated(
        since = "2.0.0",
        note = "Shouldn't be necessary after 2.0, only to support Icechunk 1 repos"
    )]
    pub fn storage_settings(&self) -> &storage::Settings {
        &self.storage_settings
    }

    #[deprecated(
        since = "2.0.0",
        note = "Shouldn't be necessary after 2.0, only to support Icechunk 1 repos"
    )]
    pub async fn snapshot_ancestry_v1(
        self: Arc<Self>,
        snapshot_id: &SnapshotId,
    ) -> RepositoryResult<impl Stream<Item = RepositoryResult<Arc<Snapshot>>> + use<>>
    {
        let mut this = self.fetch_snapshot(snapshot_id).await?;
        let stream = try_stream! {
            yield Arc::clone(&this);
            #[allow(deprecated)]
            while let Some(parent) = this.parent_id() {
                let snap = self.fetch_snapshot(&parent).await?;
                yield Arc::clone(&snap);
                this = snap;
            }
        };
        Ok(stream)
    }
}

fn binary_file_header(
    spec_version: SpecVersionBin,
    file_type: FileTypeBin,
    compression_algorithm: CompressionAlgorithmBin,
) -> Vec<u8> {
    use format_constants::*;
    // TODO: initialize capacity
    let mut buffer = Vec::with_capacity(1024);
    // magic numbers
    buffer.extend_from_slice(ICECHUNK_FORMAT_MAGIC_BYTES);
    // implementation name
    let implementation = format!("{:<24}", &*ICECHUNK_CLIENT_NAME);
    buffer.extend_from_slice(&implementation.as_bytes()[..24]);
    // spec version
    buffer.push(spec_version as u8);
    buffer.push(file_type as u8);
    // compression
    buffer.push(compression_algorithm as u8);
    buffer
}

fn check_header(
    read: &mut dyn Read,
    file_type: FileTypeBin,
) -> RepositoryResult<(SpecVersionBin, CompressionAlgorithmBin)> {
    let mut buf = [0; 12];
    read.read_exact(&mut buf)?;
    // Magic numbers
    if format_constants::ICECHUNK_FORMAT_MAGIC_BYTES != buf {
        return Err(RepositoryErrorKind::FormatError(
            IcechunkFormatErrorKind::InvalidMagicNumbers,
        )
        .into());
    }

    let mut buf = [0; 24];
    // ignore implementation name
    read.read_exact(&mut buf)?;

    let mut spec_version = 0;
    read.read_exact(std::slice::from_mut(&mut spec_version))?;

    let spec_version = spec_version.try_into().map_err(|_| {
        RepositoryErrorKind::FormatError(IcechunkFormatErrorKind::InvalidSpecVersion)
    })?;

    let mut actual_file_type_int = 0;
    read.read_exact(std::slice::from_mut(&mut actual_file_type_int))?;

    let actual_file_type: FileTypeBin =
        actual_file_type_int.try_into().map_err(|_| {
            RepositoryErrorKind::FormatError(IcechunkFormatErrorKind::InvalidFileType {
                expected: file_type,
                got: actual_file_type_int,
            })
        })?;

    if actual_file_type != file_type {
        return Err(RepositoryErrorKind::FormatError(
            IcechunkFormatErrorKind::InvalidFileType {
                expected: file_type,
                got: actual_file_type_int,
            },
        )
        .into());
    }

    let mut compression = 0;
    read.read_exact(std::slice::from_mut(&mut compression))?;

    let compression = compression.try_into().map_err(|_| {
        RepositoryErrorKind::FormatError(
            IcechunkFormatErrorKind::InvalidCompressionAlgorithm,
        )
    })?;

    Ok((spec_version, compression))
}

async fn write_new_manifest(
    new_manifest: Arc<Manifest>,
    spec_version: SpecVersionBin,
    compression_level: u8,
    storage: &(dyn Storage + Send + Sync),
    storage_settings: &storage::Settings,
    semaphore: &Semaphore,
) -> RepositoryResult<u64> {
    use format_constants::*;
    let metadata = vec![
        (
            LATEST_ICECHUNK_FORMAT_VERSION_METADATA_KEY.to_string(),
            (spec_version as u8).to_string(),
        ),
        (ICECHUNK_CLIENT_NAME_METADATA_KEY.to_string(), ICECHUNK_CLIENT_NAME.to_string()),
        (
            ICECHUNK_FILE_TYPE_METADATA_KEY.to_string(),
            ICECHUNK_FILE_TYPE_MANIFEST.to_string(),
        ),
        (
            ICECHUNK_COMPRESSION_METADATA_KEY.to_string(),
            ICECHUNK_COMPRESSION_ZSTD.to_string(),
        ),
    ];

    let id = new_manifest.id().clone();

    let span = Span::current();
    // TODO: we should compress only when the manifest reaches a certain size
    // but then, we would need to include metadata to know if it's compressed or not
    let buffer = tokio::task::spawn_blocking(move || {
        let _entered = span.entered();
        let buffer = binary_file_header(
            spec_version,
            FileTypeBin::Manifest,
            CompressionAlgorithmBin::Zstd,
        );
        let mut compressor =
            zstd::stream::Encoder::new(buffer, compression_level as i32)?;

        serialize_manifest(new_manifest.as_ref(), spec_version, &mut compressor)?;

        compressor.finish().map_err(RepositoryErrorKind::IOError)
    })
    .await??;

    let len = buffer.len() as u64;
    debug!(%id, size_bytes=len, "Writing manifest");
    let path = format!("{MANIFESTS_FILE_PATH}/{id}");
    let settings = storage::Settings {
        storage_class: storage_settings.metadata_storage_class().cloned(),
        ..storage_settings.clone()
    };

    let _permit = semaphore.acquire().await?;
    storage
        .put_object(&settings, path.as_str(), buffer.into(), None, metadata, None)
        .await?
        .must_write()?;
    Ok(len)
}

async fn fetch_manifest(
    manifest_id: &ManifestId,
    manifest_size: u64,
    storage: &(dyn Storage + Send),
    storage_settings: &storage::Settings,
    semaphore: &Semaphore,
) -> RepositoryResult<Arc<Manifest>> {
    debug!(%manifest_id, "Downloading manifest");

    let path = format!("{MANIFESTS_FILE_PATH}/{manifest_id}");
    let range = 0..manifest_size;
    let range = if manifest_size > 0 { Some(&range) } else { None };
    let _permit = semaphore.acquire().await?;

    let (read, _) = storage.get_object(storage_settings, path.as_str(), range).await?;

    let span = Span::current();
    tokio::task::spawn_blocking(move || {
        let _entered = span.entered();
        let (spec_version, decompressor) =
            check_and_get_decompressor(read, FileTypeBin::Manifest)?;
        deserialize_manifest(spec_version, decompressor).map_err(RepositoryError::from)
    })
    .await?
    .map(Arc::new)
}

fn check_and_get_decompressor(
    read: Pin<Box<dyn AsyncBufRead + Send>>,
    file_type: FileTypeBin,
) -> RepositoryResult<(SpecVersionBin, Box<dyn Read + Send>)> {
    // TODO: use async compression
    let mut sync_read = SyncIoBridge::new(read);
    let (spec_version, compression) = check_header(&mut sync_read, file_type)?;
    debug_assert_eq!(compression, CompressionAlgorithmBin::Zstd);
    // We find a performance impact if we don't buffer here
    let decompressor =
        BufReader::with_capacity(1_024, zstd::stream::Decoder::new(sync_read)?);
    Ok((spec_version, Box::new(decompressor)))
}

async fn write_new_snapshot(
    new_snapshot: Arc<Snapshot>,
    spec_version: SpecVersionBin,
    compression_level: u8,
    storage: &(dyn Storage + Send + Sync),
    storage_settings: &storage::Settings,
    semaphore: &Semaphore,
) -> RepositoryResult<SnapshotId> {
    use format_constants::*;
    let metadata = vec![
        (
            LATEST_ICECHUNK_FORMAT_VERSION_METADATA_KEY.to_string(),
            (spec_version as u8).to_string(),
        ),
        (ICECHUNK_CLIENT_NAME_METADATA_KEY.to_string(), ICECHUNK_CLIENT_NAME.to_string()),
        (
            ICECHUNK_FILE_TYPE_METADATA_KEY.to_string(),
            ICECHUNK_FILE_TYPE_SNAPSHOT.to_string(),
        ),
        (
            ICECHUNK_COMPRESSION_METADATA_KEY.to_string(),
            ICECHUNK_COMPRESSION_ZSTD.to_string(),
        ),
    ];

    let id = new_snapshot.id().clone();
    let span = Span::current();
    let buffer = tokio::task::spawn_blocking(move || {
        let _entered = span.entered();
        let buffer = binary_file_header(
            spec_version,
            FileTypeBin::Snapshot,
            CompressionAlgorithmBin::Zstd,
        );
        let mut compressor =
            zstd::stream::Encoder::new(buffer, compression_level as i32)?;

        serialize_snapshot(new_snapshot.as_ref(), spec_version, &mut compressor)?;

        compressor.finish().map_err(RepositoryErrorKind::IOError)
    })
    .await??;

    debug!(%id, size_bytes=buffer.len(), "Writing snapshot");
    let path = format!("{SNAPSHOTS_FILE_PATH}/{id}");
    let settings = storage::Settings {
        storage_class: storage_settings.metadata_storage_class().cloned(),
        ..storage_settings.clone()
    };
    let _permit = semaphore.acquire().await?;
    storage
        .put_object(&settings, path.as_str(), buffer.into(), None, metadata, None)
        .await?
        .must_write()?;

    Ok(id)
}

async fn fetch_snapshot(
    snapshot_id: &SnapshotId,
    storage: &(dyn Storage + Send + Sync),
    storage_settings: &storage::Settings,
    semaphore: &Semaphore,
) -> RepositoryResult<Arc<Snapshot>> {
    debug!(%snapshot_id, "Downloading snapshot");
    let _permit = semaphore.acquire().await?;

    let path = format!("{SNAPSHOTS_FILE_PATH}/{snapshot_id}");
    let (read, _) = storage.get_object(storage_settings, path.as_str(), None).await?;

    let span = Span::current();
    tokio::task::spawn_blocking(move || {
        let _entered = span.entered();
        let (spec_version, decompressor) =
            check_and_get_decompressor(read, FileTypeBin::Snapshot)?;
        deserialize_snapshot(spec_version, decompressor).map_err(RepositoryError::from)
    })
    .await?
    .map(Arc::new)
}

async fn write_new_tx_log(
    transaction_id: SnapshotId,
    new_log: Arc<TransactionLog>,
    spec_version: SpecVersionBin,
    compression_level: u8,
    storage: &(dyn Storage + Send + Sync),
    storage_settings: &storage::Settings,
    semaphore: &Semaphore,
) -> RepositoryResult<()> {
    use format_constants::*;
    let metadata = vec![
        (
            LATEST_ICECHUNK_FORMAT_VERSION_METADATA_KEY.to_string(),
            (spec_version as u8).to_string(),
        ),
        (ICECHUNK_CLIENT_NAME_METADATA_KEY.to_string(), ICECHUNK_CLIENT_NAME.to_string()),
        (
            ICECHUNK_FILE_TYPE_METADATA_KEY.to_string(),
            ICECHUNK_FILE_TYPE_TRANSACTION_LOG.to_string(),
        ),
        (
            ICECHUNK_COMPRESSION_METADATA_KEY.to_string(),
            ICECHUNK_COMPRESSION_ZSTD.to_string(),
        ),
    ];

    let span = Span::current();
    let buffer = tokio::task::spawn_blocking(move || {
        let _entered = span.entered();
        let buffer = binary_file_header(
            spec_version,
            FileTypeBin::TransactionLog,
            CompressionAlgorithmBin::Zstd,
        );
        let mut compressor =
            zstd::stream::Encoder::new(buffer, compression_level as i32)?;
        serialize_transaction_log(new_log.as_ref(), spec_version, &mut compressor)?;
        compressor.finish().map_err(RepositoryErrorKind::IOError)
    })
    .await??;

    debug!(%transaction_id, size_bytes=buffer.len(), "Writing transaction log");
    let path = format!("{TRANSACTION_LOGS_FILE_PATH}/{transaction_id}");
    let settings = storage::Settings {
        storage_class: storage_settings.metadata_storage_class().cloned(),
        ..storage_settings.clone()
    };

    let _permit = semaphore.acquire().await?;
    storage
        .put_object(&settings, path.as_str(), buffer.into(), None, metadata, None)
        .await?
        .must_write()?;

    Ok(())
}

async fn fetch_transaction_log(
    transaction_id: &SnapshotId,
    storage: &(dyn Storage + Send + Sync),
    storage_settings: &storage::Settings,
    semaphore: &Semaphore,
) -> RepositoryResult<Arc<TransactionLog>> {
    debug!(%transaction_id, "Downloading transaction log");
    let path = format!("{TRANSACTION_LOGS_FILE_PATH}/{transaction_id}");
    let _permit = semaphore.acquire().await?;
    let (read, _) = storage.get_object(storage_settings, path.as_str(), None).await?;

    let span = Span::current();
    tokio::task::spawn_blocking(move || {
        let _entered = span.entered();
        let (spec_version, decompressor) =
            check_and_get_decompressor(read, FileTypeBin::TransactionLog)?;
        deserialize_transaction_log(spec_version, decompressor)
            .map_err(RepositoryError::from)
    })
    .await?
    .map(Arc::new)
}

#[allow(clippy::too_many_arguments)]
pub async fn write_repo_info(
    info: Arc<RepoInfo>,
    spec_version: SpecVersionBin,
    version: &VersionInfo,
    compression_level: u8,
    backup_path: Option<&str>,
    storage: &(dyn Storage + Send + Sync),
    storage_settings: &storage::Settings,
    path: Option<&str>,
) -> RepositoryResult<VersionInfo> {
    use format_constants::*;
    let metadata = vec![
        (
            LATEST_ICECHUNK_FORMAT_VERSION_METADATA_KEY.to_string(),
            (spec_version as u8).to_string(),
        ),
        (ICECHUNK_CLIENT_NAME_METADATA_KEY.to_string(), ICECHUNK_CLIENT_NAME.to_string()),
        (
            ICECHUNK_FILE_TYPE_METADATA_KEY.to_string(),
            ICECHUNK_FILE_TYPE_REPO_INFO.to_string(),
        ),
        (
            ICECHUNK_COMPRESSION_METADATA_KEY.to_string(),
            ICECHUNK_COMPRESSION_ZSTD.to_string(),
        ),
    ];

    let span = Span::current();
    let buffer = tokio::task::spawn_blocking(move || {
        let _entered = span.entered();
        let buffer = binary_file_header(
            spec_version,
            FileTypeBin::RepoInfo,
            CompressionAlgorithmBin::Zstd,
        );
        let mut compressor =
            zstd::stream::Encoder::new(buffer, compression_level as i32)?;
        serialize_repo_info(info.as_ref(), spec_version, &mut compressor)?;
        compressor.finish().map_err(RepositoryErrorKind::IOError)
    })
    .await??;

    debug!(size_bytes = buffer.len(), "Writing repo info");

    let repo_file_path = path.unwrap_or(REPO_INFO_FILE_PATH);
    if let Some(backup_path) = backup_path {
        let backup_path = format!("{OVERWRITTEN_FILES_PATH}/{backup_path}");
        match storage
            .copy_object(
                storage_settings,
                repo_file_path,
                backup_path.as_str(),
                None,
                version,
            )
            .await?
        {
            VersionedUpdateResult::Updated { .. } => {}
            VersionedUpdateResult::NotOnLatestVersion => {
                return Err(RepositoryErrorKind::RepoInfoUpdated.into());
            }
        }
    }

    match storage
        .put_object(
            storage_settings,
            repo_file_path,
            buffer.into(),
            None,
            metadata,
            Some(version),
        )
        .await?
    {
        storage::VersionedUpdateResult::Updated { new_version } => Ok(new_version),
        storage::VersionedUpdateResult::NotOnLatestVersion => {
            Err(RepositoryErrorKind::RepoInfoUpdated.into())
        }
    }
}

pub async fn fetch_repo_info(
    storage: &(dyn Storage + Send + Sync),
    storage_settings: &storage::Settings,
) -> RepositoryResult<(Arc<RepoInfo>, VersionInfo)> {
    fetch_repo_info_from_path(storage, storage_settings, REPO_INFO_FILE_PATH).await
}

async fn fetch_repo_info_backup(
    storage: &(dyn Storage + Send + Sync),
    storage_settings: &storage::Settings,
    file_name: &str,
) -> RepositoryResult<(Arc<RepoInfo>, VersionInfo)> {
    fetch_repo_info_from_path(
        storage,
        storage_settings,
        format!("{OVERWRITTEN_FILES_PATH}/{file_name}").as_str(),
    )
    .await
}

pub async fn fetch_repo_info_from_path(
    storage: &(dyn Storage + Send + Sync),
    storage_settings: &storage::Settings,
    path: &str,
) -> RepositoryResult<(Arc<RepoInfo>, VersionInfo)> {
    debug!("Downloading repo info");
    match storage.get_object(storage_settings, path, None).await {
        Ok((result, version)) => {
            let span = Span::current();
            tokio::task::spawn_blocking(move || {
                let _entered = span.entered();
                let (spec_version, decompressor) =
                    check_and_get_decompressor(result, FileTypeBin::RepoInfo)?;
                deserialize_repo_info(spec_version, decompressor)
                    .map(|ri| (Arc::new(ri), version))
                    .map_err(RepositoryError::from)
            })
            .await?
        }
        Err(StorageError { kind: StorageErrorKind::ObjectNotFound, .. }) => {
            Err(RepositoryError::from(RepositoryErrorKind::RepositoryDoesntExist))
        }
        Err(e) => Err(e.into()),
    }
}

#[derive(Debug, Clone)]
struct FileWeighter;

impl Weighter<ManifestId, Arc<Manifest>> for FileWeighter {
    fn weight(&self, _: &ManifestId, val: &Arc<Manifest>) -> u64 {
        val.len() as u64
    }
}

impl Weighter<SnapshotId, Arc<Snapshot>> for FileWeighter {
    fn weight(&self, _: &SnapshotId, val: &Arc<Snapshot>) -> u64 {
        val.len() as u64
    }
}

impl Weighter<(ChunkId, Range<ChunkOffset>), Bytes> for FileWeighter {
    fn weight(&self, _: &(ChunkId, Range<ChunkOffset>), val: &Bytes) -> u64 {
        val.len() as u64
    }
}

impl Weighter<SnapshotId, Arc<TransactionLog>> for FileWeighter {
    fn weight(&self, _: &SnapshotId, val: &Arc<TransactionLog>) -> u64 {
        val.len() as u64
    }
}

fn convert_list_item<Id>(item: ListInfo<String>) -> Option<ListInfo<Id>>
where
    Id: for<'b> TryFrom<&'b str>,
{
    let id = Id::try_from(item.id.as_str()).ok()?;
    let created_at = item.created_at;
    Some(ListInfo { created_at, id, size_bytes: item.size_bytes })
}

fn translate_list_infos<'a, Id>(
    s: impl Stream<Item = RepositoryResult<ListInfo<String>>> + Send + 'a,
) -> BoxStream<'a, RepositoryResult<ListInfo<Id>>>
where
    Id: for<'b> TryFrom<&'b str> + Send + std::fmt::Debug + 'a,
{
    s.try_filter_map(|info| async move {
        let info = convert_list_item(info);
        if info.is_none() {
            tracing::error!(list_info=?info, "Error processing list item metadata");
        }
        Ok(info)
    })
    .boxed()
}

pub async fn async_reader_to_bytes(
    mut read: impl AsyncBufRead + Unpin,
    expected_size: usize,
) -> Result<Bytes, std::io::Error> {
    // add some extra space to the buffer to optimize conversion to bytes
    let mut buffer = Vec::with_capacity(expected_size + 16);
    tokio::io::copy(&mut read, &mut buffer).await?;
    Ok(buffer.into())
}

fn backup_destination(source_path: &str) -> String {
    let last: u64 = 32503680000000;
    let now = Utc::now().timestamp_millis() as u64;
    let time_index = last - now;
    let random = ChunkId::random().to_string();
    format!("{source_path}.{time_index:014}.{random}")
}

#[cfg(test)]
#[allow(clippy::panic, clippy::unwrap_used, clippy::expect_used)]
mod test {

    use icechunk_macros::tokio_test;
    use itertools::{Itertools, assert_equal};

    use super::*;
    use crate::{
        format::{
            ChunkIndices, NodeId,
            manifest::{ChunkInfo, ChunkPayload},
        },
        storage::{Storage, logging::LoggingStorage, new_in_memory_storage},
    };

    #[tokio_test]
    async fn test_caching_caches() -> Result<(), Box<dyn std::error::Error>> {
        let backend: Arc<dyn Storage + Send + Sync> = new_in_memory_storage().await?;
        let settings = storage::Settings::default();
        let manager = AssetManager::new_no_cache(
            backend.clone(),
            settings.clone(),
            SpecVersionBin::default(),
            1,
            100,
        );

        let node1 = NodeId::random();
        let node2 = NodeId::random();
        let ci1 = ChunkInfo {
            node: node1.clone(),
            coord: ChunkIndices(vec![0]),
            payload: ChunkPayload::Inline(Bytes::copy_from_slice(b"a")),
        };
        let ci2 = ChunkInfo {
            node: node2.clone(),
            coord: ChunkIndices(vec![1]),
            payload: ChunkPayload::Inline(Bytes::copy_from_slice(b"b")),
        };
        let pre_existing_manifest =
            Manifest::from_iter(&ManifestId::random(), vec![ci1].into_iter())
                .await?
                .unwrap();
        let pre_existing_manifest = Arc::new(pre_existing_manifest);
        let pre_existing_id = pre_existing_manifest.id();
        let pre_size = manager.write_manifest(Arc::clone(&pre_existing_manifest)).await?;

        let logging = Arc::new(LoggingStorage::new(Arc::clone(&backend)));
        let logging_c: Arc<dyn Storage + Send + Sync> = logging.clone();
        let caching = AssetManager::new_with_config(
            Arc::clone(&logging_c),
            settings,
            SpecVersionBin::default(),
            &CachingConfig::default(),
            1,
            100,
        );

        let manifest = Arc::new(
            Manifest::from_iter(&ManifestId::random(), vec![ci2.clone()].into_iter())
                .await?
                .unwrap(),
        );
        let id = manifest.id();
        let size = caching.write_manifest(Arc::clone(&manifest)).await?;

        let fetched = caching.fetch_manifest(&id, size).await?;
        assert_eq!(fetched.len(), 1);
        assert_equal(
            fetched.iter(node2.clone()).map(|x| x.unwrap()),
            [(ci2.coord.clone(), ci2.payload.clone())],
        );

        // fetch again
        caching.fetch_manifest(&id, size).await?;
        // when we insert we cache, so no fetches
        assert_eq!(
            logging.fetch_operations(),
            vec![("put_object".to_string(), format!("{MANIFESTS_FILE_PATH}/{id}"))]
        );

        // first time it sees an ID it calls the backend
        caching.fetch_manifest(&pre_existing_id, pre_size).await?;
        assert_eq!(
            logging.fetch_operations(),
            vec![
                ("put_object".to_string(), format!("{MANIFESTS_FILE_PATH}/{id}")),
                (
                    "get_object_range".to_string(),
                    format!("{MANIFESTS_FILE_PATH}/{pre_existing_id}")
                )
            ]
        );

        // only calls backend once
        caching.fetch_manifest(&pre_existing_id, pre_size).await?;
        assert_eq!(
            logging.fetch_operations(),
            vec![
                ("put_object".to_string(), format!("{MANIFESTS_FILE_PATH}/{id}")),
                (
                    "get_object_range".to_string(),
                    format!("{MANIFESTS_FILE_PATH}/{pre_existing_id}")
                )
            ]
        );

        // other walues still cached
        caching.fetch_manifest(&id, size).await?;
        assert_eq!(
            logging.fetch_operations(),
            vec![
                ("put_object".to_string(), format!("{MANIFESTS_FILE_PATH}/{id}")),
                (
                    "get_object_range".to_string(),
                    format!("{MANIFESTS_FILE_PATH}/{pre_existing_id}")
                )
            ]
        );
        Ok(())
    }

    #[tokio_test]
    async fn test_caching_storage_has_limit() -> Result<(), Box<dyn std::error::Error>> {
        let backend: Arc<dyn Storage + Send + Sync> = new_in_memory_storage().await?;
        let settings = storage::Settings::default();
        let manager = AssetManager::new_no_cache(
            backend.clone(),
            settings.clone(),
            SpecVersionBin::default(),
            1,
            100,
        );

        let ci1 = ChunkInfo {
            node: NodeId::random(),
            coord: ChunkIndices(vec![]),
            payload: ChunkPayload::Inline(Bytes::copy_from_slice(b"a")),
        };
        let ci2 = ChunkInfo { node: NodeId::random(), ..ci1.clone() };
        let ci3 = ChunkInfo { node: NodeId::random(), ..ci1.clone() };
        let ci4 = ChunkInfo { node: NodeId::random(), ..ci1.clone() };
        let ci5 = ChunkInfo { node: NodeId::random(), ..ci1.clone() };
        let ci6 = ChunkInfo { node: NodeId::random(), ..ci1.clone() };
        let ci7 = ChunkInfo { node: NodeId::random(), ..ci1.clone() };
        let ci8 = ChunkInfo { node: NodeId::random(), ..ci1.clone() };
        let ci9 = ChunkInfo { node: NodeId::random(), ..ci1.clone() };

        let manifest1 = Arc::new(
            Manifest::from_iter(&ManifestId::random(), vec![ci1, ci2, ci3])
                .await?
                .unwrap(),
        );
        let id1 = manifest1.id();
        let size1 = manager.write_manifest(Arc::clone(&manifest1)).await?;
        let manifest2 = Arc::new(
            Manifest::from_iter(&ManifestId::random(), vec![ci4, ci5, ci6])
                .await?
                .unwrap(),
        );
        let id2 = manifest2.id();
        let size2 = manager.write_manifest(Arc::clone(&manifest2)).await?;
        let manifest3 = Arc::new(
            Manifest::from_iter(&ManifestId::random(), vec![ci7, ci8, ci9])
                .await?
                .unwrap(),
        );
        let id3 = manifest3.id();
        let size3 = manager.write_manifest(Arc::clone(&manifest3)).await?;

        let logging = Arc::new(LoggingStorage::new(Arc::clone(&backend)));
        let logging_c: Arc<dyn Storage + Send + Sync> = logging.clone();
        let caching = AssetManager::new_with_config(
            logging_c,
            settings,
            SpecVersionBin::default(),
            // the cache can only fit 6 refs.
            &CachingConfig {
                num_snapshot_nodes: Some(0),
                num_chunk_refs: Some(7),
                num_transaction_changes: Some(0),
                num_bytes_attributes: Some(0),
                num_bytes_chunks: Some(0),
            },
            1,
            100,
        );

        // we keep asking for all 3 items, but the cache can only fit 2
        for _ in 0..20 {
            caching.fetch_manifest(&id1, size1).await?;
            caching.fetch_manifest(&id2, size2).await?;
            caching.fetch_manifest(&id3, size3).await?;
        }
        // after the initial warming requests, we only request the file that doesn't fit in the cache
        assert_eq!(logging.fetch_operations()[10..].iter().unique().count(), 1);

        Ok(())
    }

    #[tokio_test]
    async fn test_dont_fetch_asset_twice() -> Result<(), Box<dyn std::error::Error>> {
        // Test that two concurrent requests for the same manifest doesn't generate two
        // object_store requests, one of them must wait
        let storage: Arc<dyn Storage + Send + Sync> = new_in_memory_storage().await?;
        let settings = storage::Settings::default();
        let manager = Arc::new(AssetManager::new_no_cache(
            storage.clone(),
            settings.clone(),
            SpecVersionBin::default(),
            1,
            100,
        ));

        // some reasonable size so it takes some time to parse
        let manifest = Manifest::from_iter(
            &ManifestId::random(),
            (0..5_000).map(|_| ChunkInfo {
                node: NodeId::random(),
                coord: ChunkIndices(Vec::from([rand::random(), rand::random()])),
                payload: ChunkPayload::Inline("hello".into()),
            }),
        )
        .await
        .unwrap()
        .unwrap();
        let manifest_id = manifest.id().clone();
        let size = manager.write_manifest(Arc::new(manifest)).await?;

        let logging = Arc::new(LoggingStorage::new(Arc::clone(&storage)));
        let logging_c: Arc<dyn Storage + Send + Sync> = logging.clone();
        let manager = Arc::new(AssetManager::new_with_config(
            logging_c.clone(),
            settings,
            SpecVersionBin::default(),
            &CachingConfig::default(),
            1,
            100,
        ));

        let manager_c = Arc::new(manager);
        let manager_cc = Arc::clone(&manager_c);
        let manifest_id_c = manifest_id.clone();

        let res1 = tokio::task::spawn(async move {
            manager_c.fetch_manifest(&manifest_id_c, size).await
        });
        let res2 = tokio::task::spawn(async move {
            manager_cc.fetch_manifest(&manifest_id, size).await
        });

        assert!(res1.await?.is_ok());
        assert!(res2.await?.is_ok());
        assert_eq!(logging.fetch_operations().len(), 1);
        Ok(())
    }
}
