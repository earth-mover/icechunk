//! Typed I/O layer for Icechunk assets.
//!
//! Sits between [`crate::session::Session`] and [`Storage`], handling serialization
//! and caching. While [`Storage`] provides generic object store operations (bytes
//! in/out), this module works with typed Icechunk assets: snapshots, manifests,
//! transaction logs, and chunks.

use async_stream::try_stream;
use backon::{BackoffBuilder as _, ConstantBuilder, ExponentialBuilder, Retryable as _};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::{Stream, StreamExt as _, TryStreamExt as _, stream::BoxStream};
use icechunk_types::{ICResultExt as _, error::ICResultCtxExt as _};
use itertools::Itertools as _;
use quick_cache::{Weighter, sync::Cache};
use serde::{Deserialize, Serialize};
use std::sync::{LazyLock, RwLock};
use std::{
    num::NonZeroU16,
    ops::Range,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

#[expect(clippy::unwrap_used)]
static RETRYABLE_ERROR: LazyLock<regex::Regex> = LazyLock::new(|| {
    regex::Regex::new(
        "(?i)StreamingError|streaming error|DispatchFailure|ConnectorError|IncompleteMessage|connection reset",
    )
    .unwrap()
});
use tokio::{
    io::{AsyncBufRead, AsyncReadExt as _},
    sync::Semaphore,
};
use tracing::{debug, instrument, trace, warn};

use crate::format::repo_info::RepoAvailability;
use crate::storage::GetModifiedResult;
use crate::{
    RepositoryConfig, Storage, StorageError,
    config::{CachingConfig, RepoUpdateRetryConfig},
    format::{
        CHUNKS_FILE_PATH, CONFIG_FILE_PATH, ChunkId, ChunkOffset, IcechunkFormatError,
        IcechunkFormatErrorKind, MANIFESTS_FILE_PATH, ManifestId, OVERWRITTEN_FILES_PATH,
        Path, REPO_INFO_FILE_PATH, SNAPSHOTS_FILE_PATH, SnapshotId,
        TRANSACTION_LOGS_FILE_PATH,
        envelope::{compress_with_header, decompress_frame},
        format_constants::{
            self, FileHeader, FileTypeBin, SpecVersionBin, parse_file_header,
        },
        manifest::Manifest,
        repo_info::{RepoInfo, WritableShapeMode},
        repo_info_file,
        serializers::{
            deserialize_manifest, deserialize_snapshot, deserialize_transaction_log,
        },
        snapshot::{Snapshot, SnapshotInfo},
        transaction_log::TransactionLog,
        writer_features::{FeatureName, min_version, missing_features},
    },
    private,
    repository::{RepositoryError, RepositoryErrorKind, RepositoryResult},
    storage::{
        self, Attribution, AttributionLabels, ListInfo, PutCondition, RequestAttribution,
        StorageContext, StorageErrorKind, StorageWriterFeatures, VersionInfo,
        VersionedUpdateResult, WriteHint, listing,
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
    max_concurrent_decodes: u16,

    #[serde(default = "_default_true")]
    use_repo_info_cache: bool,

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
    #[serde(skip)]
    decode_semaphore: Arc<Semaphore>,

    #[serde(skip)]
    repo_cache: RwLock<Option<(Arc<RepoInfo>, VersionInfo)>>,

    #[serde(default)]
    attribution: Attribution,
    /// Rendered from `attribution` at construction.
    #[serde(skip)]
    attribution_labels: AttributionLabels,
}

const fn _default_true() -> bool {
    true
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
    #[serde(default = "crate::config::default_max_concurrent_decodes")]
    max_concurrent_decodes: u16,
    use_repo_info_cache: bool,
    #[serde(default)]
    attribution: Attribution,
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
            value.max_concurrent_decodes,
            value.use_repo_info_cache,
        )
        .with_attribution(value.attribution)
    }
}

impl AssetManager {
    #[expect(clippy::too_many_arguments)]
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
        max_concurrent_decodes: u16,
        use_repo_info_cache: bool,
    ) -> Self {
        Self {
            num_snapshot_nodes,
            num_chunk_refs,
            num_transaction_changes,
            num_bytes_attributes,
            num_bytes_chunks,
            compression_level,
            max_concurrent_requests,
            max_concurrent_decodes,
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
            decode_semaphore: Arc::new(Semaphore::new(max_concurrent_decodes as usize)),
            repo_cache: RwLock::new(None),
            use_repo_info_cache,
            attribution: Attribution::default(),
            attribution_labels: AttributionLabels::default(),
        }
    }

    pub fn with_attribution(mut self, attribution: Attribution) -> Self {
        self.attribution_labels = AttributionLabels::from(&attribution);
        self.attribution = attribution;
        self
    }

    pub fn attribution(&self) -> &Attribution {
        &self.attribution
    }

    pub fn attribution_labels(&self) -> &AttributionLabels {
        &self.attribution_labels
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
            crate::config::default_max_concurrent_decodes(),
            false,
        )
    }

    pub fn new_with_config(
        storage: Arc<dyn Storage + Send + Sync>,
        storage_settings: storage::Settings,
        spec_version: SpecVersionBin,
        config: &CachingConfig,
        compression_level: u8,
        max_concurrent_requests: u16,
        max_concurrent_decodes: u16,
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
            max_concurrent_decodes,
            true,
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
            self.max_concurrent_decodes,
            true,
        )
        .with_attribution(self.attribution.clone())
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

    pub fn clear_manifest_cache(&self) {
        self.manifest_cache.clear();
    }

    pub fn clear_snapshot_cache(&self) {
        self.snapshot_cache.clear();
    }

    #[instrument(skip_all)]
    pub async fn fetch_config(
        &self,
    ) -> RepositoryResult<Option<(RepositoryConfig, VersionInfo)>> {
        match self
            .storage
            .get_object_conditional(&self.storage_context(), CONFIG_FILE_PATH, None)
            .await
        {
            Ok(GetModifiedResult::Modified { mut data, new_version }) => {
                let mut bytes = Vec::with_capacity(1_024);
                data.read_to_end(&mut bytes).await.capture()?;
                let config = serde_yaml_ng::from_slice(bytes.as_slice()).capture()?;
                Ok(Some((config, new_version)))
            }
            Ok(GetModifiedResult::OnLatestVersion) => Err(icechunk_storage::other_error(
                "backend returned \"on latest version\" for a conditional get without a previous version",
            )
            .inject()),
            Err(StorageError { kind: StorageErrorKind::ObjectNotFound, .. }) => Ok(None),
            Err(e) => Err(e.inject()),
        }
    }

    pub async fn try_update_config(
        &self,
        config: &RepositoryConfig,
        previous_version: &VersionInfo,
        backup_path: Option<&str>,
    ) -> RepositoryResult<Option<VersionInfo>> {
        let bytes = Bytes::from(serde_yaml_ng::to_string(config).capture()?);
        let content_type = Some("application/yaml");
        if let Some(backup_path) = backup_path {
            let backup_path = format!("{OVERWRITTEN_FILES_PATH}/{backup_path}");
            match self
                .storage
                .copy_object(
                    &self.storage_context(),
                    CONFIG_FILE_PATH,
                    backup_path.as_str(),
                    content_type,
                    previous_version,
                )
                .await
                .inject()?
            {
                VersionedUpdateResult::Updated { .. } => {}
                VersionedUpdateResult::NotOnLatestVersion => return Ok(None),
            }
        }
        // IC1 config versions use `for_creation()` as "doesn't exist yet".
        let condition = if previous_version.is_create() {
            PutCondition::Create
        } else {
            PutCondition::IfVersion(previous_version)
        };
        match self
            .storage
            .put_object_conditional(
                &self.storage_context(),
                CONFIG_FILE_PATH,
                bytes,
                content_type,
                Vec::new(),
                condition,
            )
            .await
            .inject()?
        {
            VersionedUpdateResult::Updated { new_version } => Ok(Some(new_version)),
            VersionedUpdateResult::NotOnLatestVersion => Ok(None),
        }
    }

    #[instrument(skip(self, manifest))]
    pub async fn write_manifest(
        &self,
        array: &str,
        manifest: Arc<Manifest>,
    ) -> RepositoryResult<u64> {
        let manifest_c = Arc::clone(&manifest);
        let res = write_new_manifest(
            manifest_c,
            self.spec_version(),
            self.compression_level,
            self.storage.as_ref(),
            &self.node_context(&self.storage_settings, array, None),
            &self.request_semaphore,
        )
        .await?;
        self.warn_if_manifest_cache_small(manifest.as_ref());
        self.manifest_cache.insert(manifest.id().clone(), manifest);
        Ok(res)
    }

    pub async fn fetch_manifest(
        &self,
        array: &str,
        manifest_id: &ManifestId,
        manifest_size: u64,
    ) -> RepositoryResult<Arc<Manifest>> {
        self.fetch_manifest_with_context(
            &self.node_context(&self.storage_settings, array, None),
            manifest_id,
            manifest_size,
        )
        .await
    }

    /// For tooling that has a manifest id but no node: carries the labels but
    /// no array.
    pub async fn fetch_manifest_without_node(
        &self,
        manifest_id: &ManifestId,
    ) -> RepositoryResult<Arc<Manifest>> {
        self.fetch_manifest_with_context(&self.storage_context(), manifest_id, 0).await
    }

    async fn fetch_manifest_with_context(
        &self,
        ctx: &StorageContext<'_>,
        manifest_id: &ManifestId,
        manifest_size: u64,
    ) -> RepositoryResult<Arc<Manifest>> {
        match self.manifest_cache.get_value_or_guard_async(manifest_id).await {
            Ok(manifest) => Ok(manifest),
            Err(guard) => {
                let (manifest, _header) = fetch_manifest(
                    manifest_id,
                    manifest_size,
                    self.storage.as_ref(),
                    ctx,
                    &self.request_semaphore,
                    &self.decode_semaphore,
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
            && !self.manifest_cache_size_warned.load(Ordering::Relaxed)
        {
            warn!(
                "A manifest with {manifest_weight} chunk references is being loaded into the cache that can only keep {capacity} references. Consider increasing the size of the manifest cache using the num_chunk_refs field in CachingConfig"
            );
            self.manifest_cache_size_warned.store(true, Ordering::Relaxed);
        }
    }

    fn warn_if_snapshot_cache_small(&self, snap: &Snapshot) {
        let snap_weight = snap.len();
        let capacity = self.num_snapshot_nodes;
        // TODO: we may need a config to silence this warning
        if snap_weight as u64 > capacity / 5
            && !self.snapshot_cache_size_warned.load(Ordering::Relaxed)
        {
            warn!(
                "A snapshot with {snap_weight} nodes is being loaded into the cache that can only keep {capacity} nodes. Consider increasing the size of the snapshot cache using the num_snapshot_nodes field in CachingConfig"
            );
            self.snapshot_cache_size_warned.store(true, Ordering::Relaxed);
        }
    }

    pub async fn fetch_manifest_unknown_size(
        &self,
        array: &str,
        manifest_id: &ManifestId,
    ) -> RepositoryResult<Arc<Manifest>> {
        self.fetch_manifest(array, manifest_id, 0).await
    }

    /// Compressed manifest bytes straight from storage: no cache, no
    /// request or decode semaphore. For bulk traversals that own their
    /// own concurrency limits.
    #[instrument(skip(self))]
    pub async fn fetch_manifest_bytes(
        &self,
        manifest_id: &ManifestId,
        size_bytes: u64,
    ) -> RepositoryResult<Vec<u8>> {
        debug!(%manifest_id, size_bytes, "Downloading manifest");
        let path = format!("{MANIFESTS_FILE_PATH}/{manifest_id}");
        let range = 0..size_bytes;
        let range = if size_bytes > 0 { Some(&range) } else { None };
        let read = self
            .storage
            .get_object(&self.storage_context(), path.as_str(), range)
            .await
            .inject()?;
        read_all(read, size_bytes).await
    }

    pub fn max_concurrent_decodes(&self) -> u16 {
        self.max_concurrent_decodes
    }

    #[instrument(skip(self, snapshot))]
    pub async fn write_snapshot(
        &self,
        snapshot: Arc<Snapshot>,
        hint: WriteHint,
    ) -> RepositoryResult<()> {
        self.put_snapshot(snapshot, PutMode::Overwrite(hint)).await?;
        Ok(())
    }

    /// Writes `snapshot` only if no snapshot file has its id, and returns the
    /// snapshot stored under that id: `snapshot`, or the existing one read
    /// back.
    #[instrument(skip(self, snapshot))]
    pub async fn create_snapshot(
        &self,
        snapshot: Arc<Snapshot>,
    ) -> RepositoryResult<Arc<Snapshot>> {
        let id = snapshot.id();
        match self.put_snapshot(Arc::clone(&snapshot), PutMode::Create).await? {
            PutOutcome::Written => Ok(snapshot),
            PutOutcome::Exists => self.fetch_snapshot(&id).await,
        }
    }

    async fn put_snapshot(
        &self,
        snapshot: Arc<Snapshot>,
        mode: PutMode,
    ) -> RepositoryResult<PutOutcome> {
        let outcome = write_new_snapshot(
            Arc::clone(&snapshot),
            self.spec_version(),
            self.compression_level,
            self.storage.as_ref(),
            &self.storage_context(),
            &self.request_semaphore,
            mode,
        )
        .await?;
        if outcome == PutOutcome::Written {
            let snapshot_id = snapshot.id().clone();
            self.warn_if_snapshot_cache_small(snapshot.as_ref());
            // This line is critical for expiration:
            // When we edit snapshots in place, we need the cache to return the new version
            self.snapshot_cache.insert(snapshot_id, snapshot);
        }
        Ok(outcome)
    }

    pub async fn fetch_snapshot(
        &self,
        snapshot_id: &SnapshotId,
    ) -> RepositoryResult<Arc<Snapshot>> {
        match self.snapshot_cache.get_value_or_guard_async(snapshot_id).await {
            Ok(snapshot) => Ok(snapshot),
            Err(guard) => {
                let (snapshot, _header) = fetch_snapshot(
                    snapshot_id,
                    self.storage.as_ref(),
                    &self.storage_context(),
                    &self.request_semaphore,
                    &self.decode_semaphore,
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
        hint: WriteHint,
    ) -> RepositoryResult<()> {
        self.put_transaction_log(transaction_id, log, PutMode::Overwrite(hint)).await?;
        Ok(())
    }

    /// Writes `log` only if no transaction log file has this id; an existing
    /// one is kept.
    #[instrument(skip(self, log))]
    pub async fn create_transaction_log(
        &self,
        transaction_id: SnapshotId,
        log: Arc<TransactionLog>,
    ) -> RepositoryResult<()> {
        self.put_transaction_log(transaction_id, log, PutMode::Create).await?;
        Ok(())
    }

    async fn put_transaction_log(
        &self,
        transaction_id: SnapshotId,
        log: Arc<TransactionLog>,
        mode: PutMode,
    ) -> RepositoryResult<PutOutcome> {
        let outcome = write_new_tx_log(
            transaction_id.clone(),
            Arc::clone(&log),
            self.spec_version(),
            self.compression_level,
            self.storage.as_ref(),
            &self.storage_context(),
            &self.request_semaphore,
            mode,
        )
        .await?;
        if outcome == PutOutcome::Written {
            self.transactions_cache.insert(transaction_id, log);
        }
        Ok(outcome)
    }

    pub async fn fetch_transaction_log(
        &self,
        transaction_id: &SnapshotId,
    ) -> RepositoryResult<Arc<TransactionLog>> {
        match self.transactions_cache.get_value_or_guard_async(transaction_id).await {
            Ok(transaction) => Ok(transaction),
            Err(guard) => {
                let (transaction, _header) = fetch_transaction_log(
                    transaction_id,
                    self.storage.as_ref(),
                    &self.storage_context(),
                    &self.request_semaphore,
                    &self.decode_semaphore,
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
        self.fail_unless_spec_at_least(SpecVersionBin::V2)?;

        let repo_cache = if self.use_repo_info_cache {
            // Cloning here so lock is immediately released
            self.repo_cache
                .read()
                .map_err(|_| RepositoryErrorKind::PoisonLock)
                .capture()?
                .clone()
        } else {
            None
        };

        trace!(
            "Fetching repo info {} cache",
            if repo_cache.is_some() { "with" } else { "without" }
        );

        match fetch_repo_info_from_path(
            self.storage.as_ref(),
            &self.storage_context(),
            REPO_INFO_FILE_PATH,
            repo_cache.as_ref().map(|(_, info)| info),
            &self.decode_semaphore,
        )
        .await
        {
            Ok(Some((repo_info, version_info, _header))) => {
                if self.use_repo_info_cache {
                    trace!(
                        "Repo info cache wasn't latest, updating from {} to {}",
                        repo_cache
                            .map(|(_, old)| old.to_string())
                            .unwrap_or_else(|| "none".to_string()),
                        version_info
                    );
                    let mut repo_cache = self
                        .repo_cache
                        .write()
                        .map_err(|_| RepositoryErrorKind::PoisonLock)
                        .capture()?;
                    *repo_cache = Some((Arc::clone(&repo_info), version_info.clone()));
                }

                return Ok((repo_info, version_info));
            }
            Ok(None) => {
                if self.use_repo_info_cache {
                    trace!("Reusing cached repo info object since it's latest version");
                    #[expect(clippy::expect_used)]
                    return Ok(repo_cache.expect(
                        "Logic bug in fetch_repo_info, repo_cache should exist here",
                    ));
                } else {
                    // It is very surprising to get to this branch:
                    // - repo info cache is not being used
                    // - but when conditionally retrieving the repo info
                    //   without a version, which should fetch and return
                    //   the repo info, we got back a result saying it
                    //   didn't change!
                    // This is very much panic or unreachable territory...
                    unreachable!()
                }
            }
            Err(e) => return Err(e),
        }
    }

    #[cfg(test)]
    pub(crate) fn cached_repo_info(&self) -> Option<(Arc<RepoInfo>, VersionInfo)> {
        self.repo_cache.read().ok()?.clone()
    }

    pub fn fail_unless_spec_at_least(
        &self,
        minimum_spec_version: SpecVersionBin,
    ) -> RepositoryResult<()> {
        if self.spec_version() < minimum_spec_version {
            Err(RepositoryError::capture(RepositoryErrorKind::BadRepoVersion {
                minimum_spec_version,
            }))
        } else {
            Ok(())
        }
    }

    #[instrument(skip(self))]
    pub async fn fetch_repo_info_backup(
        &self,
        file_name: &str,
    ) -> RepositoryResult<(Arc<RepoInfo>, VersionInfo)> {
        fetch_repo_info_backup(
            self.storage.as_ref(),
            &self.storage_context(),
            file_name,
            &self.decode_semaphore,
        )
        .await
    }

    /// Read-only inspection: fetch just the [`FileHeader`] of the metadata file at
    /// `path` with a cheap range GET of the first few bytes.
    ///
    /// This is a pure inspection primitive: it reports whatever file type it finds
    /// in the header and does **not** assert that the type matches the path. The
    /// per-type wrappers ([`Self::fetch_snapshot_header`] etc.) only build the path.
    #[instrument(skip(self))]
    pub async fn fetch_header(&self, path: &str) -> RepositoryResult<FileHeader> {
        let len = format_constants::ICECHUNK_FILE_HEADER_LEN;
        let range = 0u64..(len as u64);
        let _permit = self.request_semaphore.acquire().await.capture()?;
        let read = self
            .storage
            .get_object(&self.storage_context(), path, Some(&range))
            .await
            .inject()?;
        let bytes = async_reader_to_bytes(read, len).await.capture()?;
        parse_file_header(&bytes).inject()
    }

    pub async fn fetch_repo_info_header(&self) -> RepositoryResult<FileHeader> {
        self.fetch_header(REPO_INFO_FILE_PATH).await
    }

    pub async fn fetch_repo_info_backup_header(
        &self,
        file_name: &str,
    ) -> RepositoryResult<FileHeader> {
        self.fetch_header(&format!("{OVERWRITTEN_FILES_PATH}/{file_name}")).await
    }

    pub async fn fetch_snapshot_header(
        &self,
        snapshot_id: &SnapshotId,
    ) -> RepositoryResult<FileHeader> {
        self.fetch_header(&format!("{SNAPSHOTS_FILE_PATH}/{snapshot_id}")).await
    }

    pub async fn fetch_manifest_header(
        &self,
        manifest_id: &ManifestId,
    ) -> RepositoryResult<FileHeader> {
        self.fetch_header(&format!("{MANIFESTS_FILE_PATH}/{manifest_id}")).await
    }

    pub async fn fetch_transaction_log_header(
        &self,
        transaction_id: &SnapshotId,
    ) -> RepositoryResult<FileHeader> {
        self.fetch_header(&format!("{TRANSACTION_LOGS_FILE_PATH}/{transaction_id}")).await
    }

    /// Fetch a snapshot together with its [`FileHeader`].
    ///
    /// Notice that unlike [`Self::fetch_snapshot`], this bypasses the value cache
    /// and always performs a fresh download+decode.
    pub async fn fetch_snapshot_with_header(
        &self,
        snapshot_id: &SnapshotId,
    ) -> RepositoryResult<(Arc<Snapshot>, FileHeader)> {
        // TODO: widen the cache to store the FileHeader so we can serve from cache
        fetch_snapshot(
            snapshot_id,
            self.storage.as_ref(),
            &self.storage_context(),
            &self.request_semaphore,
            &self.decode_semaphore,
        )
        .await
    }

    /// Fetch a manifest together with its [`FileHeader`].
    ///
    /// Notice that unlike [`Self::fetch_manifest`], this bypasses the value cache
    /// and always performs a fresh download+decode.
    pub async fn fetch_manifest_with_header(
        &self,
        manifest_id: &ManifestId,
        manifest_size: u64,
    ) -> RepositoryResult<(Arc<Manifest>, FileHeader)> {
        // TODO: widen the cache to store the FileHeader so we can serve from cache
        fetch_manifest(
            manifest_id,
            manifest_size,
            self.storage.as_ref(),
            &self.storage_context(),
            &self.request_semaphore,
            &self.decode_semaphore,
        )
        .await
    }

    /// Fetch a transaction log together with its [`FileHeader`].
    ///
    /// Notice that unlike [`Self::fetch_transaction_log`], this bypasses the value
    /// cache and always performs a fresh download+decode.
    pub async fn fetch_transaction_log_with_header(
        &self,
        transaction_id: &SnapshotId,
    ) -> RepositoryResult<(Arc<TransactionLog>, FileHeader)> {
        // TODO: widen the cache to store the FileHeader so we can serve from cache
        fetch_transaction_log(
            transaction_id,
            self.storage.as_ref(),
            &self.storage_context(),
            &self.request_semaphore,
            &self.decode_semaphore,
        )
        .await
    }

    /// Fetch the repo info together with its [`FileHeader`].
    ///
    /// Notice that unlike [`Self::fetch_repo_info`], this bypasses the repo info
    /// cache and always performs a fresh download+decode.
    pub async fn fetch_repo_info_with_header(
        &self,
    ) -> RepositoryResult<(Arc<RepoInfo>, VersionInfo, FileHeader)> {
        self.fail_unless_spec_at_least(SpecVersionBin::V2)?;
        // TODO: widen the cache to store the FileHeader so we can serve from cache
        let fetched = fetch_repo_info_from_path(
            self.storage.as_ref(),
            &self.storage_context(),
            REPO_INFO_FILE_PATH,
            None,
            &self.decode_semaphore,
        )
        .await?;
        // unreachable: we get either the object or a not-found error above.
        #[expect(clippy::expect_used)]
        Ok(fetched
            .expect("repo info must be present when fetched with no previous version"))
    }

    /// Fetch a repo info backup together with its [`FileHeader`].
    ///
    /// Like [`Self::fetch_repo_info_backup`], this is uncached and always performs
    /// a fresh download+decode.
    pub async fn fetch_repo_info_backup_with_header(
        &self,
        file_name: &str,
    ) -> RepositoryResult<(Arc<RepoInfo>, VersionInfo, FileHeader)> {
        let fetched = fetch_repo_info_from_path(
            self.storage.as_ref(),
            &self.storage_context(),
            format!("{OVERWRITTEN_FILES_PATH}/{file_name}").as_str(),
            None,
            &self.decode_semaphore,
        )
        .await?;
        // unreachable: we get either the object or a not-found error above.
        #[expect(clippy::expect_used)]
        Ok(fetched
            .expect("repo info must be present when fetched with no previous version"))
    }

    pub async fn write_repo_info(
        &self,
        info: Arc<RepoInfo>,
        condition: PutCondition<'_>,
        backup_path: Option<&str>,
    ) -> RepositoryResult<VersionInfo> {
        let new_version = write_repo_info(
            Arc::clone(&info),
            self.spec_version(),
            condition,
            self.compression_level,
            backup_path,
            self.storage.as_ref(),
            &self.storage_context(),
            None,
        )
        .await?;

        if self.use_repo_info_cache {
            {
                *self
                    .repo_cache
                    .write()
                    .map_err(|_| RepositoryErrorKind::PoisonLock)
                    .capture()? = Some((info, new_version.clone()));
            }
        }

        Ok(new_version)
    }

    /// The checks before a modification, against the repo info it replaces:
    /// the storage implements every listed writer feature, every feature the
    /// storage requires is listed, the shape is well formed and, unless the
    /// modification only changes the status, the status is `Online`. `op`
    /// prefixes the error of a status that isn't `Online`.
    pub async fn ensure_modifiable(
        &self,
        repo_info: &RepoInfo,
        modification: Modification,
        op: Option<&str>,
    ) -> RepositoryResult<()> {
        let features = self.storage.writer_features().await.inject()?;
        check_modifiable(repo_info, &features, modification, op)
    }

    #[instrument(skip(self, info))]
    pub async fn create_repo_info(
        &self,
        info: Arc<RepoInfo>,
    ) -> RepositoryResult<VersionInfo> {
        self.write_repo_info(info, PutCondition::Create, None).await
    }

    #[instrument(skip(self, retries, update))]
    pub async fn update_repo_info(
        &self,
        retries: &RepoUpdateRetryConfig,
        update: impl FnMut(
            Arc<RepoInfo>,
            &str,
            VersionInfo,
        ) -> RepositoryResult<Arc<RepoInfo>>,
    ) -> RepositoryResult<VersionInfo> {
        self.update_repo_info_internal(retries, update, Modification::Any).await
    }

    /// For changes of the status alone: skips the check that the status is
    /// `Online`, and accepts a malformed shape (the change writes a well-formed
    /// status). The writer-feature checks still apply.
    ///
    /// # Safety
    ///
    /// `update` must change only the status.
    #[expect(unsafe_code)]
    #[instrument(skip(self, retries, update))]
    pub async unsafe fn update_repo_info_unchecked(
        &self,
        retries: &RepoUpdateRetryConfig,
        update: impl FnMut(
            Arc<RepoInfo>,
            &str,
            VersionInfo,
        ) -> RepositoryResult<Arc<RepoInfo>>,
    ) -> RepositoryResult<VersionInfo> {
        self.update_repo_info_internal(retries, update, Modification::StatusOnly).await
    }

    #[instrument(skip(self, retries, update))]
    async fn update_repo_info_internal(
        &self,
        retries: &RepoUpdateRetryConfig,
        mut update: impl FnMut(
            Arc<RepoInfo>,
            &str,
            VersionInfo,
        ) -> RepositoryResult<Arc<RepoInfo>>,
        modification: Modification,
    ) -> RepositoryResult<VersionInfo> {
        let storage_features = self.storage.writer_features().await.inject()?;
        let retry_settings = retries.retries();
        let max_attempts = retry_settings.max_tries().get() as usize;
        let max_retries = max_attempts - 1;

        // The first few retries are immediate (no delay) since brief contention
        // typically resolves within one or two attempts. After that, we switch to
        // exponential backoff with jitter.
        let immediate_retries = max_retries.min(5);
        let const_backoff = ConstantBuilder::new()
            .with_delay(Duration::ZERO)
            .with_max_times(immediate_retries)
            .build();
        let exp_backoff = ExponentialBuilder::new()
            .with_min_delay(Duration::from_millis(
                retry_settings.initial_backoff_ms() as u64
            ))
            .with_max_delay(Duration::from_millis(retry_settings.max_backoff_ms() as u64))
            .with_max_times(max_retries - immediate_retries)
            .with_jitter()
            .build();
        let mut backoff = const_backoff.chain(exp_backoff);

        let mut attempts: u64 = 1;
        loop {
            let (repo_info, repo_version) = self.fetch_repo_info().await?;
            check_modifiable(&repo_info, &storage_features, modification, None)?;
            let backup_path = self.backup_path_for_repo_info();
            let new_repo = update(repo_info, backup_path.as_str(), repo_version.clone())?;
            trace!(attempts, "Attempting to update repo object");
            match write_repo_info(
                Arc::clone(&new_repo),
                self.spec_version(),
                PutCondition::IfVersion(&repo_version),
                self.compression_level,
                Some(backup_path.as_str()),
                self.storage.as_ref(),
                &self.storage_context(),
                None,
            )
            .await
            {
                Ok(new_version) => {
                    debug!(attempts, "Repo info object updated successfully");
                    if self.use_repo_info_cache {
                        *self
                            .repo_cache
                            .write()
                            .map_err(|_| RepositoryErrorKind::PoisonLock)
                            .capture()? = Some((new_repo, new_version.clone()));
                    }
                    return Ok(new_version);
                }
                Err(RepositoryError {
                    kind: RepositoryErrorKind::RepoInfoUpdated,
                    ..
                }) => {
                    // A conflict means our cached version is stale; with backends
                    // whose listing lags (FilesystemStorage answers
                    // `OnLatestVersion` for an older version), keeping it could loop.
                    if self.use_repo_info_cache {
                        *self
                            .repo_cache
                            .write()
                            .map_err(|_| RepositoryErrorKind::PoisonLock)
                            .capture()? = None;
                    }
                    let delay = backoff
                        .next()
                        .ok_or(RepositoryErrorKind::RepoUpdateAttemptsLimit(
                            max_attempts as u64,
                        ))
                        .capture()?;
                    debug!(
                        attempts,
                        ?delay,
                        "Repo info updated concurrently, retrying..."
                    );
                    tokio::time::sleep(delay).await;
                    attempts += 1;
                }
                Err(RepositoryError {
                    kind:
                        RepositoryErrorKind::UpdateDurableButRepositoryForked {
                            new_version,
                            detail,
                        },
                    context,
                }) => {
                    // The change landed: cache it, and never retry it.
                    if self.use_repo_info_cache {
                        *self
                            .repo_cache
                            .write()
                            .map_err(|_| RepositoryErrorKind::PoisonLock)
                            .capture()? = Some((new_repo, (*new_version).clone()));
                    }
                    return Err(RepositoryError {
                        kind: RepositoryErrorKind::UpdateDurableButRepositoryForked {
                            new_version,
                            detail,
                        },
                        context,
                    });
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
        array: &str,
        chunk: &[u32],
        chunk_id: ChunkId,
        bytes: Bytes,
    ) -> RepositoryResult<()> {
        trace!(%chunk_id, size_bytes=bytes.len(), "Writing chunk");
        if !self.storage.can_write().await.inject()? {
            return Err(RepositoryErrorKind::ReadonlyStorage(
                "Cannot write chunk".to_string(),
            ))
            .capture();
        }

        let path = format!("{CHUNKS_FILE_PATH}/{chunk_id}");
        let _permit = self.request_semaphore.acquire().await.capture()?;
        let settings = storage::Settings {
            storage_class: self.storage_settings.chunks_storage_class().cloned(),
            ..self.storage_settings.clone()
        };
        let ctx = self.node_context(&settings, array, Some(chunk));
        // we don't pre-populate the chunk cache, there are too many of them for this to be useful
        self.storage
            .put_object(
                &ctx,
                path.as_str(),
                bytes,
                None,
                Default::default(),
                WriteHint::NewObject,
            )
            .await
            .inject()?;
        Ok(())
    }

    #[instrument(skip(self))]
    pub async fn fetch_chunk(
        &self,
        array: &str,
        chunk: &[u32],
        chunk_id: &ChunkId,
        range: &Range<ChunkOffset>,
    ) -> RepositoryResult<Bytes> {
        let key = (chunk_id.clone(), range.clone());
        match self.chunk_cache.get_value_or_guard_async(&key).await {
            Ok(chunk) => Ok(chunk),
            Err(guard) => {
                trace!(%chunk_id, ?range, "Downloading chunk");
                let path = format!("{CHUNKS_FILE_PATH}/{chunk_id}");
                let retry = (|| async {
                    let permit = self.request_semaphore.acquire().await.capture()?;
                    let ctx =
                        self.node_context(&self.storage_settings, array, Some(chunk));
                    let read = self
                        .storage
                        .get_object(&ctx, &path, Some(range))
                        .await
                        .inject()?;
                    let bytes_result =
                        async_reader_to_bytes(read, (range.end - range.start) as usize)
                            .await
                            .capture();
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
                );
                #[cfg(feature = "napi-send-contract")]
                let retry = retry.sleep(tokio::time::sleep);
                let chunk = retry
                    .when(|e| RETRYABLE_ERROR.is_match(&format!("{e:?}")))
                    .notify(|err, duration| {
                        debug!(
                            ?err,
                            "retrying on streaming/connection error after {duration:?}"
                        );
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
        let _permit = self.request_semaphore.acquire().await.capture()?;
        self.storage
            .get_object_last_modified(&self.storage_context(), path.as_str())
            .await
            .inject()
    }

    #[instrument(skip(self))]
    pub async fn fetch_snapshot_info(
        &self,
        snapshot_id: &SnapshotId,
    ) -> RepositoryResult<SnapshotInfo> {
        let snapshot = self.fetch_snapshot(snapshot_id).await?;
        let info = SnapshotInfo::from_snapshot_file(snapshot.as_ref()).inject()?;
        Ok(info)
    }

    #[instrument(skip(self))]
    pub async fn list_chunks(
        &self,
    ) -> RepositoryResult<BoxStream<'_, RepositoryResult<ListInfo<ChunkId>>>> {
        self.list_chunks_with_concurrency(listing::default_list_concurrency()).await
    }

    #[instrument(skip(self))]
    pub async fn list_chunks_with_concurrency(
        &self,
        concurrency: NonZeroU16,
    ) -> RepositoryResult<BoxStream<'_, RepositoryResult<ListInfo<ChunkId>>>> {
        self.list_object_ids(CHUNKS_FILE_PATH, concurrency).await
    }

    #[instrument(skip(self))]
    pub async fn list_manifests(
        &self,
    ) -> RepositoryResult<BoxStream<'_, RepositoryResult<ListInfo<ManifestId>>>> {
        self.list_manifests_with_concurrency(listing::default_list_concurrency()).await
    }

    #[instrument(skip(self))]
    pub async fn list_manifests_with_concurrency(
        &self,
        concurrency: NonZeroU16,
    ) -> RepositoryResult<BoxStream<'_, RepositoryResult<ListInfo<ManifestId>>>> {
        self.list_object_ids(MANIFESTS_FILE_PATH, concurrency).await
    }

    #[instrument(skip(self))]
    pub async fn list_snapshots(
        &self,
    ) -> RepositoryResult<BoxStream<'_, RepositoryResult<ListInfo<SnapshotId>>>> {
        self.list_snapshots_with_concurrency(listing::default_list_concurrency()).await
    }

    #[instrument(skip(self))]
    pub async fn list_snapshots_with_concurrency(
        &self,
        concurrency: NonZeroU16,
    ) -> RepositoryResult<BoxStream<'_, RepositoryResult<ListInfo<SnapshotId>>>> {
        self.list_object_ids(SNAPSHOTS_FILE_PATH, concurrency).await
    }

    #[instrument(skip(self))]
    pub async fn list_transaction_logs(
        &self,
    ) -> RepositoryResult<BoxStream<'_, RepositoryResult<ListInfo<SnapshotId>>>> {
        self.list_transaction_logs_with_concurrency(listing::default_list_concurrency())
            .await
    }

    #[instrument(skip(self))]
    pub async fn list_transaction_logs_with_concurrency(
        &self,
        concurrency: NonZeroU16,
    ) -> RepositoryResult<BoxStream<'_, RepositoryResult<ListInfo<SnapshotId>>>> {
        self.list_object_ids(TRANSACTION_LOGS_FILE_PATH, concurrency).await
    }

    async fn list_object_ids<'a, Id>(
        &'a self,
        prefix: &str,
        concurrency: NonZeroU16,
    ) -> RepositoryResult<BoxStream<'a, RepositoryResult<ListInfo<Id>>>>
    where
        Id: listing::ListedId + Send + std::fmt::Debug + 'static,
    {
        listing::list_object_ids(
            &self.storage,
            &self.storage_context(),
            prefix,
            concurrency,
        )
        .await
    }

    pub async fn can_write_to_storage(&self) -> RepositoryResult<bool> {
        self.storage.can_write().await.inject()
    }

    pub async fn list_overwritten_objects(
        &self,
    ) -> RepositoryResult<BoxStream<'_, RepositoryResult<String>>> {
        let stream = self
            .storage
            .list_objects(&self.storage_context(), OVERWRITTEN_FILES_PATH)
            .await
            .inject()?
            .map_ok(|li| li.id)
            .map(|r| r.inject())
            .boxed();
        Ok(stream)
    }

    pub fn backup_path_for_config(&self) -> String {
        repo_info_file::new_backup_path_of(CONFIG_FILE_PATH)
    }

    pub fn backup_path_for_repo_info(&self) -> String {
        repo_info_file::new_backup_path()
    }

    pub fn storage(&self) -> &Arc<dyn Storage + Send + Sync> {
        &self.storage
    }

    pub fn storage_settings(&self) -> &storage::Settings {
        &self.storage_settings
    }

    /// Context for a request about no particular node.
    pub fn storage_context(&self) -> StorageContext<'_> {
        StorageContext::without_node(&self.storage_settings, &self.attribution_labels)
    }

    fn node_context<'a>(
        &'a self,
        settings: &'a storage::Settings,
        array: &'a str,
        chunk: Option<&'a [u32]>,
    ) -> StorageContext<'a> {
        StorageContext::new(
            settings,
            RequestAttribution {
                labels: &self.attribution_labels,
                array: Some(array),
                chunk,
            },
        )
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
            #[expect(deprecated)]
            while let Some(parent) = this.parent_id() {
                let snap = self.fetch_snapshot(&parent).await?;
                yield Arc::clone(&snap);
                this = snap;
            }
        };
        Ok(stream)
    }
}

/// The `array` attribution of requests about the node at `path`.
pub(crate) fn array_label(path: &Path) -> &str {
    path.as_str().trim_start_matches('/')
}

/// Starting buffer capacity for objects without a known size, to avoid a few
/// initial reallocations while `read_to_end` grows the buffer.
const DEFAULT_PREALLOC_HINT: usize = 8192;

/// Validates that `header.file_type` matches the expected `file_type`.
fn check_file_type(header: &FileHeader, file_type: FileTypeBin) -> RepositoryResult<()> {
    if header.file_type != file_type {
        return Err(RepositoryErrorKind::FormatError(
            IcechunkFormatErrorKind::InvalidFileType {
                expected: file_type,
                got: header.file_type as u8,
            },
        ))
        .capture();
    }
    Ok(())
}

async fn write_new_manifest(
    new_manifest: Arc<Manifest>,
    spec_version: SpecVersionBin,
    compression_level: u8,
    storage: &(dyn Storage + Send + Sync),
    ctx: &StorageContext<'_>,
    semaphore: &Semaphore,
) -> RepositoryResult<u64> {
    if !storage.can_write().await.inject()? {
        return Err(RepositoryErrorKind::ReadonlyStorage(
            "Cannot write manifest".to_string(),
        ))
        .capture();
    }
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

    // TODO: we should compress only when the manifest reaches a certain size
    // but then, we would need to include metadata to know if it's compressed or not
    let buffer = compress_with_header(
        new_manifest.bytes(),
        spec_version,
        FileTypeBin::Manifest,
        compression_level,
    )
    .inject()?;

    let len = buffer.len() as u64;
    debug!(%id, size_bytes=len, "Writing manifest");
    let path = format!("{MANIFESTS_FILE_PATH}/{id}");
    let settings = storage::Settings {
        storage_class: ctx.settings.metadata_storage_class().cloned(),
        ..ctx.settings.clone()
    };
    let ctx = StorageContext::new(&settings, ctx.attribution);

    let _permit = semaphore.acquire().await.capture()?;
    storage
        .put_object(
            &ctx,
            path.as_str(),
            buffer.into(),
            None,
            metadata,
            WriteHint::NewObject,
        )
        .await
        .inject()?;
    Ok(len)
}

#[instrument(skip(storage, ctx, semaphore, decode_semaphore))]
async fn fetch_manifest(
    manifest_id: &ManifestId,
    manifest_size: u64,
    storage: &(dyn Storage + Send),
    ctx: &StorageContext<'_>,
    semaphore: &Semaphore,
    decode_semaphore: &Arc<Semaphore>,
) -> RepositoryResult<(Arc<Manifest>, FileHeader)> {
    debug!(%manifest_id, "Downloading manifest");

    let path = format!("{MANIFESTS_FILE_PATH}/{manifest_id}");
    let range = 0..manifest_size;
    let range = if manifest_size > 0 { Some(&range) } else { None };
    let _permit = semaphore.acquire().await.capture()?;

    let read = storage.get_object(ctx, path.as_str(), range).await.inject()?;
    fetch_and_decode(
        read,
        manifest_size,
        FileTypeBin::Manifest,
        Arc::clone(decode_semaphore),
        |spec_version, buffer| deserialize_manifest(spec_version, buffer).map(Arc::new),
    )
    .await
}

/// Read a whole object into memory, preallocating `size_hint` bytes.
async fn read_all(
    mut read: Pin<Box<dyn AsyncBufRead + Send>>,
    size_hint: u64,
) -> RepositoryResult<Vec<u8>> {
    let mut bytes = Vec::with_capacity(size_hint as usize);
    read.read_to_end(&mut bytes).await.capture()?;
    Ok(bytes)
}

/// Parse the icechunk file header and check that it announces `file_type`.
/// Parsing rejects every compression but Zstd.
fn checked_header(
    compressed: &[u8],
    file_type: FileTypeBin,
) -> RepositoryResult<FileHeader> {
    let header = parse_file_header(compressed).inject()?;
    check_file_type(&header, file_type)?;
    Ok(header)
}

/// Decompress the body that follows a [`checked_header`] and deserialize it.
/// `decoded_size_hint` sizes the buffer when the frame does not declare its
/// size. CPU bound: the caller runs this on a blocking thread.
fn decode_body<T, F>(
    header: &FileHeader,
    compressed: &[u8],
    decoded_size_hint: Option<usize>,
    deserialize: F,
) -> RepositoryResult<T>
where
    F: FnOnce(SpecVersionBin, Vec<u8>) -> Result<T, IcechunkFormatError>,
{
    let decompressed = decompress_frame(
        &compressed[format_constants::ICECHUNK_FILE_HEADER_LEN..],
        decoded_size_hint,
    )
    .capture()?;
    deserialize(header.spec_version, decompressed).inject()
}

/// Decode the compressed bytes of a manifest file, as written by
/// `write_manifest`. The caller runs this on a blocking thread.
/// `decoded_size_hint` sizes the buffer when the frame does not declare its size.
pub fn decode_manifest(
    compressed: &[u8],
    decoded_size_hint: Option<usize>,
) -> RepositoryResult<Manifest> {
    let header = checked_header(compressed, FileTypeBin::Manifest)?;
    decode_body(&header, compressed, decoded_size_hint, deserialize_manifest)
}

/// Read compressed bytes (async IO), then decompress+deserialize on a gated blocking thread.
///
/// Returns the deserialized value together with the parsed [`FileHeader`].
async fn fetch_and_decode<T, F>(
    read: Pin<Box<dyn AsyncBufRead + Send>>,
    compressed_size_hint: u64,
    file_type: FileTypeBin,
    decode_semaphore: Arc<Semaphore>,
    deserialize: F,
) -> RepositoryResult<(T, FileHeader)>
where
    T: Send + 'static,
    F: FnOnce(SpecVersionBin, Vec<u8>) -> Result<T, IcechunkFormatError> + Send + 'static,
{
    let compressed = read_all(read, compressed_size_hint).await?;
    // the caller's `#[instrument]` span carries which asset this is
    debug!(?file_type, size_bytes = compressed.len(), "Downloaded asset");

    // on the async task: fail fast (no permit) and keep span ancestry on header errors
    let header = checked_header(&compressed, file_type)?;

    decode_blocking(decode_semaphore, move || {
        let value = decode_body(&header, &compressed, None, deserialize)?;
        Ok((value, header))
    })
    .await
}

/// Run the CPU bound `decode` on a blocking thread, gated by `decode_semaphore`.
async fn decode_blocking<T, F>(
    #[cfg_attr(feature = "shuttle", expect(unused))] decode_semaphore: Arc<Semaphore>,
    decode: F,
) -> RepositoryResult<T>
where
    T: Send + 'static,
    F: FnOnce() -> RepositoryResult<T> + Send + 'static,
{
    // keep error span ancestry on the blocking thread
    let span = tracing::Span::current();
    tokio::task::spawn_blocking(move || -> RepositoryResult<T> {
        let _entered = span.entered();
        // Acquired here, not on the async task: an unpolled async waiter never takes the permit it is handed.
        // release on decode completion, not on cancelled-future drop
        #[cfg(not(feature = "shuttle"))]
        let _decode_permit =
            futures::executor::block_on(decode_semaphore.acquire()).capture()?;
        decode()
    })
    .await
    .capture()?
}

/// The same object layout, with a frame that does not declare its decoded
/// size: what every writer before the frame size was recorded produced.
/// Test only.
#[cfg(test)]
pub(crate) fn compress_without_content_size(
    data: &[u8],
    spec_version: SpecVersionBin,
    file_type: FileTypeBin,
    compression_level: u8,
) -> RepositoryResult<Vec<u8>> {
    use crate::format::{
        envelope::binary_file_header, format_constants::CompressionAlgorithmBin,
    };
    use std::io::Write as _;
    let mut buffer =
        binary_file_header(spec_version, file_type, CompressionAlgorithmBin::Zstd);
    let mut encoder =
        zstd::stream::write::Encoder::new(&mut buffer, i32::from(compression_level))
            .capture()?;
    encoder.write_all(data).capture()?;
    encoder.finish().capture()?;
    Ok(buffer)
}

async fn write_new_snapshot(
    new_snapshot: Arc<Snapshot>,
    spec_version: SpecVersionBin,
    compression_level: u8,
    storage: &(dyn Storage + Send + Sync),
    ctx: &StorageContext<'_>,
    semaphore: &Semaphore,
    mode: PutMode,
) -> RepositoryResult<PutOutcome> {
    if !storage.can_write().await.inject()? {
        return Err(RepositoryErrorKind::ReadonlyStorage(
            "Cannot write snapshot".to_string(),
        ))
        .capture();
    }
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
    let buffer = compress_with_header(
        new_snapshot.bytes(),
        spec_version,
        FileTypeBin::Snapshot,
        compression_level,
    )
    .inject()?;

    debug!(%id, size_bytes=buffer.len(), "Writing snapshot");
    let path = format!("{SNAPSHOTS_FILE_PATH}/{id}");
    let settings = storage::Settings {
        storage_class: ctx.settings.metadata_storage_class().cloned(),
        ..ctx.settings.clone()
    };
    let ctx = StorageContext::new(&settings, ctx.attribution);
    let _permit = semaphore.acquire().await.capture()?;
    put_metadata_file(storage, &ctx, path.as_str(), buffer, metadata, mode).await
}

#[instrument(skip(storage, ctx, semaphore, decode_semaphore))]
async fn fetch_snapshot(
    snapshot_id: &SnapshotId,
    storage: &(dyn Storage + Send + Sync),
    ctx: &StorageContext<'_>,
    semaphore: &Semaphore,
    decode_semaphore: &Arc<Semaphore>,
) -> RepositoryResult<(Arc<Snapshot>, FileHeader)> {
    debug!(%snapshot_id, "Downloading snapshot");
    let _permit = semaphore.acquire().await.capture()?;

    let path = format!("{SNAPSHOTS_FILE_PATH}/{snapshot_id}");
    let read = storage.get_object(ctx, path.as_str(), None).await.inject()?;
    fetch_and_decode(
        read,
        DEFAULT_PREALLOC_HINT as u64,
        FileTypeBin::Snapshot,
        Arc::clone(decode_semaphore),
        |spec_version, buffer| deserialize_snapshot(spec_version, buffer).map(Arc::new),
    )
    .await
}

#[expect(clippy::too_many_arguments)]
async fn write_new_tx_log(
    transaction_id: SnapshotId,
    new_log: Arc<TransactionLog>,
    spec_version: SpecVersionBin,
    compression_level: u8,
    storage: &(dyn Storage + Send + Sync),
    ctx: &StorageContext<'_>,
    semaphore: &Semaphore,
    mode: PutMode,
) -> RepositoryResult<PutOutcome> {
    if !storage.can_write().await.inject()? {
        return Err(RepositoryErrorKind::ReadonlyStorage(
            "Cannot write transaction log".to_string(),
        ))
        .capture();
    }
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

    let buffer = compress_with_header(
        new_log.bytes(),
        spec_version,
        FileTypeBin::TransactionLog,
        compression_level,
    )
    .inject()?;

    debug!(%transaction_id, size_bytes=buffer.len(), "Writing transaction log");
    let path = format!("{TRANSACTION_LOGS_FILE_PATH}/{transaction_id}");
    let settings = storage::Settings {
        storage_class: ctx.settings.metadata_storage_class().cloned(),
        ..ctx.settings.clone()
    };
    let ctx = StorageContext::new(&settings, ctx.attribution);

    let _permit = semaphore.acquire().await.capture()?;
    put_metadata_file(storage, &ctx, path.as_str(), buffer, metadata, mode).await
}

/// How a snapshot or transaction log file is written.
#[derive(Debug, Clone, Copy)]
enum PutMode {
    Overwrite(WriteHint),
    /// Only if no object has the name. Storages whose settings disable
    /// conditional creates write unconditionally.
    Create,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PutOutcome {
    Written,
    /// Another writer created the name first (only with [`PutMode::Create`]).
    Exists,
}

async fn put_metadata_file(
    storage: &(dyn Storage + Send + Sync),
    ctx: &StorageContext<'_>,
    path: &str,
    buffer: Vec<u8>,
    metadata: Vec<(String, String)>,
    mode: PutMode,
) -> RepositoryResult<PutOutcome> {
    match mode {
        PutMode::Overwrite(hint) => {
            storage
                .put_object(ctx, path, buffer.into(), None, metadata, hint)
                .await
                .inject()?;
            Ok(PutOutcome::Written)
        }
        PutMode::Create => {
            match storage
                .put_object_conditional(
                    ctx,
                    path,
                    buffer.into(),
                    None,
                    metadata,
                    PutCondition::Create,
                )
                .await
                .inject()?
            {
                VersionedUpdateResult::Updated { .. } => Ok(PutOutcome::Written),
                VersionedUpdateResult::NotOnLatestVersion => Ok(PutOutcome::Exists),
            }
        }
    }
}

#[instrument(skip(storage, ctx, semaphore, decode_semaphore))]
async fn fetch_transaction_log(
    transaction_id: &SnapshotId,
    storage: &(dyn Storage + Send + Sync),
    ctx: &StorageContext<'_>,
    semaphore: &Semaphore,
    decode_semaphore: &Arc<Semaphore>,
) -> RepositoryResult<(Arc<TransactionLog>, FileHeader)> {
    debug!(%transaction_id, "Downloading transaction log");
    let path = format!("{TRANSACTION_LOGS_FILE_PATH}/{transaction_id}");
    let _permit = semaphore.acquire().await.capture()?;
    let read = storage.get_object(ctx, path.as_str(), None).await.inject()?;
    fetch_and_decode(
        read,
        DEFAULT_PREALLOC_HINT as u64,
        FileTypeBin::TransactionLog,
        Arc::clone(decode_semaphore),
        |spec_version, buffer| {
            deserialize_transaction_log(spec_version, buffer).map(Arc::new)
        },
    )
    .await
}

async fn prepare_repo_info(
    info: Arc<RepoInfo>,
    spec_version: SpecVersionBin,
    compression_level: u8,
    storage: &(dyn Storage + Send + Sync),
) -> RepositoryResult<(Vec<u8>, Vec<(String, String)>)> {
    if !storage.can_write().await.inject()? {
        return Err(RepositoryErrorKind::ReadonlyStorage(
            "Cannot write repo info".to_string(),
        ))
        .capture();
    }
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

    let buffer =
        repo_info_file::encode(&info, spec_version, compression_level).inject()?;

    Ok((buffer, metadata))
}

/// What a modification changes, for the checks before it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Modification {
    /// Anything other than the status alone.
    Any,
    /// The status alone.
    StatusOnly,
}

/// See [`AssetManager::ensure_modifiable`].
pub fn check_modifiable(
    repo_info: &RepoInfo,
    storage_features: &StorageWriterFeatures,
    modification: Modification,
    op: Option<&str>,
) -> RepositoryResult<()> {
    let listed = repo_info.writer_features().inject()?;
    let missing = missing_features(&listed, storage_features.iter().copied());
    if let Some(min_version) = min_version(&missing) {
        return Err(RepositoryError::capture(
            RepositoryErrorKind::MissingWriterFeatures { missing, min_version },
        ));
    }

    let unlisted: Vec<_> = storage_features
        .iter()
        .filter(|feature| !listed.contains(&FeatureName::from(**feature)))
        .collect();
    if !unlisted.is_empty() {
        return Err(RepositoryError::capture(RepositoryErrorKind::StorageError(
            StorageErrorKind::LayoutConversionInProgress(format!(
                "the repo info doesn't list {}, which this storage requires",
                unlisted.iter().join(", ")
            )),
        )));
    }

    let shape_mode = match modification {
        Modification::Any => WritableShapeMode::AnyChange,
        Modification::StatusOnly => WritableShapeMode::StatusOnlyChange,
    };
    repo_info.check_writable_shape(shape_mode).inject()?;

    if modification == Modification::Any {
        let status = repo_info.status().inject()?;
        if status.availability != RepoAvailability::Online {
            let reason = match op {
                Some(op) => format!("{op}; {}", status.error_msg()),
                None => status.error_msg(),
            };
            return Err(RepositoryError::capture(
                RepositoryErrorKind::ReadonlyRepository(reason),
            ));
        }
    }
    Ok(())
}

#[expect(clippy::too_many_arguments)]
pub async fn write_repo_info(
    info: Arc<RepoInfo>,
    spec_version: SpecVersionBin,
    condition: PutCondition<'_>,
    compression_level: u8,
    backup_path: Option<&str>,
    storage: &(dyn Storage + Send + Sync),
    ctx: &StorageContext<'_>,
    path: Option<&str>,
) -> RepositoryResult<VersionInfo> {
    let (buffer, metadata) =
        prepare_repo_info(info, spec_version, compression_level, storage).await?;

    debug!(size_bytes = buffer.len(), "Writing repo info");

    let repo_file_path = path.unwrap_or(REPO_INFO_FILE_PATH);
    // A create has no previous object to back up.
    if let (Some(backup_path), PutCondition::IfVersion(version)) =
        (backup_path, condition)
    {
        let backup_path = format!("{OVERWRITTEN_FILES_PATH}/{backup_path}");
        match storage
            .copy_object(ctx, repo_file_path, backup_path.as_str(), None, version)
            .await
            .inject()?
        {
            VersionedUpdateResult::Updated { .. } => {}
            VersionedUpdateResult::NotOnLatestVersion => {
                return Err(RepositoryError::capture(
                    RepositoryErrorKind::RepoInfoUpdated,
                ));
            }
        }
    }

    match storage
        .put_object_conditional(
            ctx,
            repo_file_path,
            buffer.into(),
            None,
            metadata,
            condition,
        )
        .await
        .inject()?
    {
        VersionedUpdateResult::Updated { new_version } => Ok(new_version),
        VersionedUpdateResult::NotOnLatestVersion => {
            Err(RepositoryError::capture(RepositoryErrorKind::RepoInfoUpdated))
        }
    }
}

/// Unconditionally overwrite repo info, bypassing version checks and backups.
///
/// # Safety
/// This is not memory-unsafe, but it bypasses the optimistic concurrency
/// control that `write_repo_info` provides. Concurrent writers can silently
/// clobber each other.
#[expect(unsafe_code)]
pub async unsafe fn force_write_repo_info(
    info: Arc<RepoInfo>,
    spec_version: SpecVersionBin,
    compression_level: u8,
    storage: &(dyn Storage + Send + Sync),
    ctx: &StorageContext<'_>,
    path: Option<&str>,
) -> RepositoryResult<()> {
    let (buffer, metadata) =
        prepare_repo_info(info, spec_version, compression_level, storage).await?;

    debug!(size_bytes = buffer.len(), "Force-writing repo info");

    let repo_file_path = path.unwrap_or(REPO_INFO_FILE_PATH);

    storage
        .put_object(ctx, repo_file_path, buffer.into(), None, metadata, WriteHint::Atomic)
        .await
        .inject()
}

#[instrument(skip_all)]
pub async fn fetch_repo_info(
    storage: &(dyn Storage + Send + Sync),
    ctx: &StorageContext<'_>,
    decode_semaphore: &Arc<Semaphore>,
) -> RepositoryResult<(Arc<RepoInfo>, VersionInfo)> {
    fetch_repo_info_from_path(storage, ctx, REPO_INFO_FILE_PATH, None, decode_semaphore)
        .await
        .map(|fetched| {
            // Since we didn't give a previous version, there must be a result here
            #[expect(clippy::expect_used)]
            let (info, version, _header) =
                fetched.expect("Logic bug, must have a repo_info here");
            (info, version)
        })
}

#[instrument(skip_all)]
async fn fetch_repo_info_backup(
    storage: &(dyn Storage + Send + Sync),
    ctx: &StorageContext<'_>,
    file_name: &str,
    decode_semaphore: &Arc<Semaphore>,
) -> RepositoryResult<(Arc<RepoInfo>, VersionInfo)> {
    fetch_repo_info_from_path(
        storage,
        ctx,
        format!("{OVERWRITTEN_FILES_PATH}/{file_name}").as_str(),
        None,
        decode_semaphore,
    )
    .await
    .map(|fetched| {
        // Since we didn't give a previous version, there must be a result here
        #[expect(clippy::expect_used)]
        let (info, version, _header) =
            fetched.expect("Logic bug, must have a repo_info here");
        (info, version)
    })
}

#[instrument(skip_all)]
pub async fn fetch_repo_info_from_path(
    storage: &(dyn Storage + Send + Sync),
    ctx: &StorageContext<'_>,
    path: &str,
    previous_version: Option<&VersionInfo>,
    decode_semaphore: &Arc<Semaphore>,
) -> RepositoryResult<Option<(Arc<RepoInfo>, VersionInfo, FileHeader)>> {
    debug!("Downloading repo info");
    match storage.get_object_conditional(ctx, path, previous_version).await {
        Ok(GetModifiedResult::Modified { data, new_version }) => {
            let bytes = read_all(data, DEFAULT_PREALLOC_HINT as u64).await?;
            debug!(size_bytes = bytes.len(), "Downloaded repo info");
            // on the async task: fail fast (no permit), and name the filesystem format
            let header = checked_header(&bytes, FileTypeBin::RepoInfo)?;
            let repo_info = decode_blocking(Arc::clone(decode_semaphore), move || {
                repo_info_file::decode(&bytes).inject()
            })
            .await?;
            Ok(Some((Arc::new(repo_info), new_version, header)))
        }
        Ok(GetModifiedResult::OnLatestVersion) => Ok(None),
        Err(StorageError { kind: StorageErrorKind::ObjectNotFound, .. }) => {
            Err(RepositoryError::capture(RepositoryErrorKind::RepositoryDoesntExist))
        }
        Err(e) => Err(e.inject()),
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

pub async fn async_reader_to_bytes(
    mut read: impl AsyncBufRead + Unpin,
    expected_size: usize,
) -> Result<Bytes, std::io::Error> {
    // add some extra space to the buffer to optimize conversion to bytes
    let mut buffer = Vec::with_capacity(expected_size + 16);
    tokio::io::copy(&mut read, &mut buffer).await?;
    Ok(buffer.into())
}

#[cfg(test)]
mod test {

    use icechunk_macros::tokio_test;
    use itertools::assert_equal;

    use super::*;
    use crate::{
        Repository,
        config::ManifestVirtualChunkLocationCompressionConfig,
        format::{
            ChunkIndices, NodeId, Path,
            envelope::{binary_file_header, frame_content_size},
            format_constants::CompressionAlgorithmBin,
            manifest::{ChunkInfo, ChunkPayload, LocationCompressionConfig},
            snapshot::ArrayShape,
        },
        storage::{
            Storage,
            intercepting::{Intercept, InterceptingStorage},
            logging::LoggingStorage,
            new_in_memory_storage,
        },
    };

    /// A repo info file header with its file type and compression bytes replaced.
    fn repo_info_header_with(file_type: u8, compression: u8) -> Vec<u8> {
        let mut bytes = binary_file_header(
            SpecVersionBin::current(),
            FileTypeBin::RepoInfo,
            CompressionAlgorithmBin::Zstd,
        );
        bytes[format_constants::ICECHUNK_FILE_TYPE_OFFSET] = file_type;
        bytes[format_constants::ICECHUNK_COMPRESSION_OFFSET] = compression;
        bytes
    }

    #[icechunk_macros::test]
    fn checked_header_rejects_file_type_7() {
        let err = checked_header(&repo_info_header_with(7, 1), FileTypeBin::RepoInfo)
            .expect_err("7 is not a file type");
        assert!(
            matches!(
                err.kind,
                RepositoryErrorKind::FormatError(
                    IcechunkFormatErrorKind::UnknownFileType { got: 7 }
                )
            ),
            "{err}"
        );
    }

    #[icechunk_macros::test]
    fn checked_header_rejects_compression_0() {
        let err = checked_header(&repo_info_header_with(6, 0), FileTypeBin::RepoInfo)
            .expect_err("0 is not a compression");
        assert!(
            matches!(
                err.kind,
                RepositoryErrorKind::FormatError(
                    IcechunkFormatErrorKind::InvalidCompressionAlgorithm
                )
            ),
            "{err}"
        );
    }

    /// A manifest of one inline chunk ref.
    async fn one_chunk_manifest() -> Result<Manifest, Box<dyn std::error::Error>> {
        Ok(Manifest::from_iter(
            &ManifestId::random(),
            vec![ChunkInfo {
                node: NodeId::random(),
                coord: ChunkIndices(vec![0]),
                payload: ChunkPayload::Inline("hello".into()),
            }],
            None,
        )
        .await?
        .ok_or("manifest is empty")?)
    }

    #[tokio_test]
    async fn compressed_objects_declare_their_decoded_size_and_still_stream()
    -> Result<(), Box<dyn std::error::Error>> {
        let manifest = one_chunk_manifest().await?;
        let spec_version = SpecVersionBin::current();
        let bytes = compress_with_header(
            manifest.bytes(),
            spec_version,
            FileTypeBin::Manifest,
            3,
        )?;
        let header_len = format_constants::ICECHUNK_FILE_HEADER_LEN;
        assert_eq!(
            &bytes[..header_len],
            binary_file_header(
                spec_version,
                FileTypeBin::Manifest,
                CompressionAlgorithmBin::Zstd
            )
            .as_slice()
        );
        let frame = &bytes[header_len..];
        assert_eq!(frame_content_size(frame), Some(manifest.bytes().len() as u64));
        // frame header descriptor: no content checksum and no dictionary id,
        // the same settings a single-shot frame of the same data carries
        let bulk = zstd::bulk::compress(manifest.bytes(), 3)?;
        assert_eq!(frame[4] & 0x04, 0, "a content checksum appeared");
        assert_eq!(frame[4] & 0x04, bulk[4] & 0x04);
        assert_eq!(frame[4] & 0x03, bulk[4] & 0x03);
        // the streaming path a reader that ignores the declared size takes
        let mut streamed = Vec::new();
        zstd::stream::copy_decode(frame, &mut streamed)?;
        assert_eq!(streamed.as_slice(), manifest.bytes());
        let decoded = decode_manifest(&bytes, None)?;
        assert_eq!(decoded.id(), manifest.id());
        assert_eq!(decoded.bytes(), manifest.bytes());
        Ok(())
    }

    #[test]
    fn an_empty_frame_is_not_a_manifest() -> Result<(), Box<dyn std::error::Error>> {
        let mut bytes = binary_file_header(
            SpecVersionBin::current(),
            FileTypeBin::Manifest,
            CompressionAlgorithmBin::Zstd,
        );
        bytes.extend_from_slice(&zstd::bulk::compress(b"", 3)?);
        let frame = &bytes[format_constants::ICECHUNK_FILE_HEADER_LEN..];
        assert_eq!(frame_content_size(frame), Some(0));
        assert!(decompress_frame(frame, None)?.is_empty());
        let err = decode_manifest(&bytes, None)
            .err()
            .ok_or("nothing decodes to an empty manifest")?;
        assert!(matches!(err.kind, RepositoryErrorKind::FormatError(_)), "{err}");
        Ok(())
    }

    #[tokio_test]
    async fn written_objects_record_their_decompressed_size()
    -> Result<(), Box<dyn std::error::Error>> {
        // a manifest written through the normal path must carry a frame content size
        let (repo, snap_id) = repo_with_one_manifest().await?;
        let am = repo.asset_manager();
        let snapshot = am.fetch_snapshot(&snap_id).await?;
        let info = snapshot.manifest_files().next().expect("a manifest")?;
        let bytes = am.fetch_manifest_bytes(&info.id, info.size_bytes).await?;
        let frame = &bytes[format_constants::ICECHUNK_FILE_HEADER_LEN..];
        let declared =
            frame_content_size(frame).ok_or("writer must record the content size")?;
        let manifest = decode_manifest(&bytes, None)?;
        assert_eq!(declared as usize, manifest.bytes().len());
        Ok(())
    }

    #[tokio_test]
    async fn test_caching_caches() -> Result<(), Box<dyn std::error::Error>> {
        let backend: Arc<dyn Storage + Send + Sync> = new_in_memory_storage().await?;
        let settings = storage::Settings::default();
        let manager = AssetManager::new_no_cache(
            Arc::clone(&backend),
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
            Manifest::from_iter(&ManifestId::random(), vec![ci1].into_iter(), None)
                .await?
                .unwrap();
        let pre_existing_manifest = Arc::new(pre_existing_manifest);
        let pre_existing_id = pre_existing_manifest.id();
        let pre_size =
            manager.write_manifest("a", Arc::clone(&pre_existing_manifest)).await?;

        let logging = Arc::new(LoggingStorage::new(Arc::clone(&backend)));
        let logging_c = Arc::clone(&logging);
        let logging_c: Arc<dyn Storage + Send + Sync> = logging_c;
        let caching = AssetManager::new_with_config(
            Arc::clone(&logging_c),
            settings,
            SpecVersionBin::default(),
            &CachingConfig::default(),
            1,
            100,
            8,
        );

        let compression: LocationCompressionConfig =
            (&ManifestVirtualChunkLocationCompressionConfig::default()).into();
        let manifest = Arc::new(
            Manifest::from_iter(
                &ManifestId::random(),
                vec![ci2.clone()].into_iter(),
                Some(&compression),
            )
            .await?
            .unwrap(),
        );
        let id = manifest.id();
        let size = caching.write_manifest("a", Arc::clone(&manifest)).await?;

        let fetched = caching.fetch_manifest("a", &id, size).await?;
        assert_eq!(fetched.len(), 1);
        assert_equal(
            fetched.iter(node2.clone()).unwrap().map(|x| x.unwrap()),
            [(ci2.coord.clone(), ci2.payload.clone())],
        );

        // fetch again
        caching.fetch_manifest("a", &id, size).await?;
        // when we insert we cache, so no fetches
        assert_eq!(
            logging.fetch_operations(),
            vec![("put_object".to_string(), format!("{MANIFESTS_FILE_PATH}/{id}"))]
        );

        // first time it sees an ID it calls the backend
        caching.fetch_manifest("a", &pre_existing_id, pre_size).await?;
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
        caching.fetch_manifest("a", &pre_existing_id, pre_size).await?;
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

        // other values still cached
        caching.fetch_manifest("a", &id, size).await?;
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
            Arc::clone(&backend),
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
            Manifest::from_iter(&ManifestId::random(), vec![ci1, ci2, ci3], None)
                .await?
                .unwrap(),
        );
        let id1 = manifest1.id();
        let size1 = manager.write_manifest("a", Arc::clone(&manifest1)).await?;
        let compression2: LocationCompressionConfig =
            (&ManifestVirtualChunkLocationCompressionConfig::default()).into();
        let manifest2 = Arc::new(
            Manifest::from_iter(
                &ManifestId::random(),
                vec![ci4, ci5, ci6],
                Some(&compression2),
            )
            .await?
            .unwrap(),
        );
        let id2 = manifest2.id();
        let size2 = manager.write_manifest("a", Arc::clone(&manifest2)).await?;
        let manifest3 = Arc::new(
            Manifest::from_iter(&ManifestId::random(), vec![ci7, ci8, ci9], None)
                .await?
                .unwrap(),
        );
        let id3 = manifest3.id();
        let size3 = manager.write_manifest("a", Arc::clone(&manifest3)).await?;

        let logging = Arc::new(LoggingStorage::new(Arc::clone(&backend)));
        let logging_c = Arc::clone(&logging);
        let logging_c: Arc<dyn Storage + Send + Sync> = logging_c;
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
            8,
        );

        // we keep asking for all 3 items, but the cache can only fit 2
        for _ in 0..20 {
            caching.fetch_manifest("a", &id1, size1).await?;
            caching.fetch_manifest("a", &id2, size2).await?;
            caching.fetch_manifest("a", &id3, size3).await?;
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
            Arc::clone(&storage),
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
            None,
        )
        .await
        .unwrap()
        .unwrap();
        let manifest_id = manifest.id().clone();
        let size = manager.write_manifest("a", Arc::new(manifest)).await?;

        let logging = Arc::new(LoggingStorage::new(Arc::clone(&storage)));
        let logging_c = Arc::clone(&logging);
        let logging_c: Arc<dyn Storage + Send + Sync> = logging_c;
        let manager = Arc::new(AssetManager::new_with_config(
            Arc::clone(&logging_c),
            settings,
            SpecVersionBin::default(),
            &CachingConfig::default(),
            1,
            100,
            8,
        ));

        let manager_c = Arc::new(manager);
        let manager_cc = Arc::clone(&manager_c);
        let manifest_id_c = manifest_id.clone();

        let res1 = tokio::task::spawn(async move {
            manager_c.fetch_manifest("a", &manifest_id_c, size).await
        });
        let res2 = tokio::task::spawn(async move {
            manager_cc.fetch_manifest("a", &manifest_id, size).await
        });

        assert!(res1.await?.is_ok());
        assert!(res2.await?.is_ok());
        assert_eq!(logging.fetch_operations().len(), 1);
        Ok(())
    }

    #[tokio_test]
    async fn test_repo_info_caching_no_cache() -> Result<(), Box<dyn std::error::Error>> {
        let backend: Arc<dyn Storage + Send + Sync> = new_in_memory_storage().await?;
        let settings = storage::Settings::default();
        let manager = AssetManager::new_no_cache(
            Arc::clone(&backend),
            settings.clone(),
            SpecVersionBin::default(),
            1,
            100,
        );
        let initial = Snapshot::initial(SpecVersionBin::current()).unwrap();
        let repo_info = Arc::new(RepoInfo::initial(
            SpecVersionBin::current(),
            SnapshotInfo::from_snapshot_file(&initial)?,
            100,
            None::<&()>,
            None,
            &[],
        ));
        manager.create_repo_info(Arc::clone(&repo_info)).await?;

        assert!(!manager.use_repo_info_cache);
        assert_eq!(*manager.repo_cache.read().unwrap(), None);

        let (fetched_repo_info, _) = manager.fetch_repo_info().await?;

        assert!(!manager.use_repo_info_cache);
        assert_eq!(*manager.repo_cache.read().unwrap(), None);

        assert_eq!(Arc::clone(&repo_info), fetched_repo_info);

        Ok(())
    }

    #[tokio_test]
    async fn test_repo_info_caching_with_cache() -> Result<(), Box<dyn std::error::Error>>
    {
        let backend: Arc<dyn Storage + Send + Sync> = new_in_memory_storage().await?;
        let settings = storage::Settings::default();
        let manager = AssetManager::new_with_config(
            Arc::clone(&backend),
            settings.clone(),
            SpecVersionBin::default(),
            &CachingConfig::default(),
            1,
            100,
            8,
        );
        let initial = Snapshot::initial(SpecVersionBin::current()).unwrap();
        let repo_info = Arc::new(RepoInfo::initial(
            SpecVersionBin::current(),
            SnapshotInfo::from_snapshot_file(&initial)?,
            100,
            None::<&()>,
            None,
            &[],
        ));
        let new_version = manager.create_repo_info(Arc::clone(&repo_info)).await?;

        let cached = Some((Arc::clone(&repo_info), new_version.clone()));

        assert!(manager.use_repo_info_cache);
        assert_eq!(*manager.repo_cache.read().unwrap(), cached);

        let (fetched_repo_info, version) = manager.fetch_repo_info().await?;

        assert!(manager.use_repo_info_cache);
        assert_eq!(*manager.repo_cache.read().unwrap(), cached);

        assert_eq!(Arc::clone(&repo_info), fetched_repo_info);
        assert_eq!(new_version, version);

        Ok(())
    }

    /// Calls `update_repo_info` with `max_tries`, letting a rival writer update
    /// the repo info under it during each of its first `conflicts` attempts.
    /// Returns how many attempts it made and how it ended.
    async fn update_with_conflicts(
        max_tries: u16,
        conflicts: usize,
    ) -> Result<(usize, RepositoryResult<VersionInfo>), Box<dyn std::error::Error>> {
        let backend: Arc<dyn Storage + Send + Sync> = new_in_memory_storage().await?;
        let new_manager = || {
            AssetManager::new_no_cache(
                Arc::clone(&backend),
                storage::Settings::default(),
                SpecVersionBin::default(),
                1,
                100,
            )
        };
        let (manager, rival) = (new_manager(), new_manager());
        let initial = Snapshot::initial(SpecVersionBin::current())?;
        manager
            .create_repo_info(Arc::new(RepoInfo::initial(
                SpecVersionBin::current(),
                SnapshotInfo::from_snapshot_file(&initial)?,
                100,
                None::<&()>,
                None,
                &[],
            )))
            .await?;

        let retries = RepoUpdateRetryConfig {
            default: Some(storage::RetriesSettings {
                max_tries: NonZeroU16::new(max_tries),
                initial_backoff_ms: Some(1),
                max_backoff_ms: Some(1),
            }),
        };
        // the update closure is sync, so the rival writes from its own runtime
        let rival_update = || {
            std::thread::scope(|s| {
                s.spawn(|| {
                    tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .expect("a runtime for the rival writer")
                        .block_on(rival.update_repo_info(
                            &RepoUpdateRetryConfig::default(),
                            |repo_info, _, _| Ok(repo_info),
                        ))
                })
                .join()
                .expect("the rival writer must not panic")
            })
        };
        let mut attempts = 0;
        let res = manager
            .update_repo_info(&retries, |repo_info, _, _| {
                attempts += 1;
                if attempts <= conflicts {
                    rival_update()?;
                }
                Ok(repo_info)
            })
            .await;
        Ok((attempts, res))
    }

    #[tokio_test]
    async fn test_update_repo_info_makes_at_most_max_tries_attempts()
    -> Result<(), Box<dyn std::error::Error>> {
        // 8 goes past the immediate retries into exponential backoff
        for max_tries in [1, 2, 6, 8] {
            let (attempts, res) = update_with_conflicts(max_tries, usize::MAX).await?;
            assert_eq!(attempts, max_tries as usize);
            let err = res.expect_err("every attempt conflicted");
            assert!(
                matches!(err.kind, RepositoryErrorKind::RepoUpdateAttemptsLimit(n) if n == max_tries as u64),
                "{err}"
            );
        }
        Ok(())
    }

    #[tokio_test]
    async fn test_update_repo_info_succeeds_on_the_last_try()
    -> Result<(), Box<dyn std::error::Error>> {
        for max_tries in [1, 2, 8] {
            let (attempts, res) =
                update_with_conflicts(max_tries, max_tries as usize - 1).await?;
            assert_eq!(attempts, max_tries as usize);
            res?;
        }
        Ok(())
    }

    /// Every metadata file written by this library carries the same `ic-<version>`
    /// client string in its header; assert the parsed header reflects that.
    fn assert_current_ic_header(header: &FileHeader, file_type: FileTypeBin) {
        use format_constants::*;
        assert_eq!(header.app_name, "ic");
        assert_eq!(header.app_version.as_deref(), Some(ICECHUNK_LIB_VERSION));
        assert_eq!(header.impl_name, *ICECHUNK_CLIENT_NAME);
        assert_eq!(header.file_type, file_type);
        assert_eq!(header.spec_version, SpecVersionBin::current());
        assert_eq!(header.compression, CompressionAlgorithmBin::Zstd);
    }

    /// Make an in-memory repo so every metadata file kind exists on storage,
    /// then exercise the `AssetManager` header-inspection API against them.
    #[tokio_test]
    async fn test_header_methods() -> Result<(), Box<dyn std::error::Error>> {
        let storage: Arc<dyn Storage + Send + Sync> = new_in_memory_storage().await?;
        let repo = Repository::create(Arc::clone(&storage))
            .config(RepositoryConfig {
                // force a non-inline chunk so a manifest file is written
                inline_chunk_threshold_bytes: Some(0),
                // tiny batch size so a couple commits leave a repo-info backup chain
                num_updates_per_repo_info_file: Some(1),
                ..Default::default()
            })
            .spec_version(SpecVersionBin::current())
            .execute()
            .await?;

        // First commit: group + array + a chunk, producing a manifest.
        let mut session = repo.writable_session("main").await?;
        session.add_group(Path::root(), Bytes::copy_from_slice(b"")).await?;
        let array_path: Path = "/array".to_string().try_into().unwrap();
        let shape = ArrayShape::new(vec![(4, 4)]).unwrap();
        session
            .add_array(
                array_path.clone(),
                shape,
                Some(vec!["t".into()]),
                Bytes::from_static(br#"{"this":"array"}"#),
            )
            .await?;
        let payload = session.get_chunk_writer(&array_path, &ChunkIndices(vec![0]))?(
            Bytes::copy_from_slice(&42i8.to_be_bytes()),
        )
        .await?;
        session
            .set_chunk_ref(array_path.clone(), ChunkIndices(vec![0]), Some(payload))
            .await?;
        let snap_id =
            session.commit("first commit").max_concurrent_nodes(8).execute().await?;

        // A couple more commits so the repo-info file overflows and leaves a backup
        // reachable through `repo_before_updates`.
        for (i, group) in ["/g2", "/g3"].iter().enumerate() {
            let mut session = repo.writable_session("main").await?;
            let path: Path = group.to_string().try_into().unwrap();
            session.add_group(path, Bytes::copy_from_slice(b"")).await?;
            session
                .commit(format!("commit {i}"))
                .max_concurrent_nodes(8)
                .execute()
                .await?;
        }

        let am = repo.asset_manager();

        // --- snapshot ---
        let header = am.fetch_snapshot_header(&snap_id).await?;
        assert_current_ic_header(&header, FileTypeBin::Snapshot);
        let (snapshot, with_header) = am.fetch_snapshot_with_header(&snap_id).await?;
        assert_eq!(with_header, header);
        assert_eq!(snapshot.id(), snap_id);

        let raw = am.fetch_header(&format!("{SNAPSHOTS_FILE_PATH}/{snap_id}")).await?;
        assert_eq!(raw.file_type, FileTypeBin::Snapshot);

        // --- manifest ---
        let manifest_info =
            snapshot.manifest_files().next().expect("commit must write a manifest")?;
        let header = am.fetch_manifest_header(&manifest_info.id).await?;
        assert_current_ic_header(&header, FileTypeBin::Manifest);
        let (manifest, with_header) = am
            .fetch_manifest_with_header(&manifest_info.id, manifest_info.size_bytes)
            .await?;
        assert_eq!(with_header, header);
        assert_eq!(manifest.id(), manifest_info.id);

        // --- transaction log ---
        let header = am.fetch_transaction_log_header(&snap_id).await?;
        assert_current_ic_header(&header, FileTypeBin::TransactionLog);
        let (_tx, with_header) = am.fetch_transaction_log_with_header(&snap_id).await?;
        assert_eq!(with_header, header);

        // --- repo info ---
        let header = am.fetch_repo_info_header().await?;
        assert_current_ic_header(&header, FileTypeBin::RepoInfo);
        let (info, _version, with_header) = am.fetch_repo_info_with_header().await?;
        assert_eq!(with_header, header);

        // --- repo info backup ---
        let backup_name = info
            .repo_before_updates()?
            .expect("multiple commits should leave a repo-info backup");
        let header = am.fetch_repo_info_backup_header(backup_name).await?;
        assert_current_ic_header(&header, FileTypeBin::RepoInfo);
        let (_backup_info, _v, with_header) =
            am.fetch_repo_info_backup_with_header(backup_name).await?;
        assert_eq!(with_header, header);

        Ok(())
    }

    /// A repo whose single commit wrote one manifest of 4 chunk refs.
    async fn repo_with_one_manifest()
    -> Result<(Repository, SnapshotId), Box<dyn std::error::Error>> {
        let storage: Arc<dyn Storage + Send + Sync> = new_in_memory_storage().await?;
        let repo = Repository::create(storage)
            .config(RepositoryConfig {
                // force non-inline chunks so a manifest file is written
                inline_chunk_threshold_bytes: Some(0),
                ..Default::default()
            })
            .execute()
            .await?;
        let mut session = repo.writable_session("main").await?;
        session.add_group(Path::root(), Bytes::new()).await?;
        let array_path: Path = "/a".to_string().try_into()?;
        session
            .add_array(
                array_path.clone(),
                ArrayShape::new(vec![(4, 4)]).unwrap(),
                None,
                Bytes::from_static(br#"{"zarr_format":3}"#),
            )
            .await?;
        for i in 0..4u32 {
            let payload = session
                .get_chunk_writer(&array_path, &ChunkIndices(vec![i]))?(
                Bytes::from(vec![i as u8; 100]),
            )
            .await?;
            session
                .set_chunk_ref(array_path.clone(), ChunkIndices(vec![i]), Some(payload))
                .await?;
        }
        let snap_id = session.commit("c").max_concurrent_nodes(8).execute().await?;
        Ok((repo, snap_id))
    }

    /// The raw bytes of the current repo info file of `repo`.
    async fn raw_repo_info_bytes(
        repo: &Repository,
    ) -> Result<Bytes, Box<dyn std::error::Error>> {
        let am = repo.asset_manager();
        let read = am
            .storage()
            .get_object(&am.storage_context(), REPO_INFO_FILE_PATH, None)
            .await?;
        Ok(async_reader_to_bytes(read, 0).await?)
    }

    #[tokio_test]
    async fn repo_info_files_core_writes_round_trip_through_repo_info_file()
    -> Result<(), Box<dyn std::error::Error>> {
        let (repo, snap_id) = repo_with_one_manifest().await?;
        repo.create_branch("dev", &snap_id).await?;
        repo.create_tag("t", &snap_id).await?;
        let bytes = raw_repo_info_bytes(&repo).await?;
        let info = repo_info_file::decode(&bytes)?;
        let (fetched, _) = repo.asset_manager().fetch_repo_info().await?;
        assert_eq!(info.bytes(), fetched.bytes());
        let level = repo.config().compression().level();
        let encoded = repo_info_file::encode(&info, repo.spec_version(), level)?;
        assert_eq!(encoded.as_slice(), bytes.as_ref());
        Ok(())
    }

    #[tokio_test]
    async fn num_updates_per_repo_info_file_reads_the_stored_configuration()
    -> Result<(), Box<dyn std::error::Error>> {
        let storage: Arc<dyn Storage + Send + Sync> = new_in_memory_storage().await?;
        let repo = Repository::create(storage)
            .config(RepositoryConfig {
                num_updates_per_repo_info_file: Some(7),
                ..Default::default()
            })
            .execute()
            .await?;
        let (info, _) = repo.asset_manager().fetch_repo_info().await?;
        assert_eq!(info.num_updates_per_repo_info_file()?, Some(7));

        let storage: Arc<dyn Storage + Send + Sync> = new_in_memory_storage().await?;
        let repo = Repository::create(storage).execute().await?;
        let (info, _) = repo.asset_manager().fetch_repo_info().await?;
        assert_eq!(info.num_updates_per_repo_info_file()?, None);
        Ok(())
    }

    #[tokio_test]
    async fn raw_manifest_bytes_decode_to_the_cached_manifest()
    -> Result<(), Box<dyn std::error::Error>> {
        let (repo, snap_id) = repo_with_one_manifest().await?;
        let am = repo.asset_manager();
        let snapshot = am.fetch_snapshot(&snap_id).await?;
        let info = snapshot.manifest_files().next().expect("a manifest")?;

        let bytes = am.fetch_manifest_bytes(&info.id, info.size_bytes).await?;
        assert_eq!(bytes.len() as u64, info.size_bytes);
        let decoded = decode_manifest(&bytes, None)?;
        let cached = am.fetch_manifest("a", &info.id, info.size_bytes).await?;
        assert_eq!(decoded.id(), cached.id());
        assert_eq!(decoded.len(), cached.len());
        assert_eq!(decoded.len(), 4);
        assert!(am.max_concurrent_decodes() >= 1);
        Ok(())
    }

    /// Claims every conditional read is on the latest version, like a
    /// backend whose listing lags behind the caller's version.
    #[derive(Debug)]
    struct FalseHits;

    #[async_trait::async_trait]
    impl Intercept for FalseHits {
        async fn get_object_conditional(
            &self,
            backend: &(dyn Storage + Send + Sync),
            ctx: &StorageContext<'_>,
            path: &str,
            previous_version: Option<&VersionInfo>,
        ) -> storage::StorageResult<GetModifiedResult> {
            match previous_version {
                Some(_) => Ok(GetModifiedResult::OnLatestVersion),
                None => backend.get_object_conditional(ctx, path, None).await,
            }
        }
    }

    #[tokio_test]
    async fn conflict_drops_the_cached_repo_info()
    -> Result<(), Box<dyn std::error::Error>> {
        let backend: Arc<dyn Storage + Send + Sync> = new_in_memory_storage().await?;
        let false_hits: Arc<dyn Storage + Send + Sync> =
            InterceptingStorage::new(Arc::clone(&backend), Arc::new(FalseHits));
        let settings = storage::Settings::default();
        let cached = AssetManager::new_with_config(
            false_hits,
            settings.clone(),
            SpecVersionBin::default(),
            &CachingConfig::default(),
            1,
            100,
            8,
        );
        let other = AssetManager::new_no_cache(
            backend,
            settings,
            SpecVersionBin::default(),
            1,
            100,
        );
        let initial = Snapshot::initial(SpecVersionBin::current())?;
        let snap = initial.id().clone();
        cached
            .create_repo_info(Arc::new(RepoInfo::initial(
                SpecVersionBin::current(),
                SnapshotInfo::from_snapshot_file(&initial)?,
                100,
                None::<&()>,
                None,
                &[],
            )))
            .await?;

        let add_tag = |name: &'static str| {
            let snap = snap.clone();
            move |info: Arc<RepoInfo>, backup_path: &str, _| {
                Ok(Arc::new(
                    info.add_tag(
                        SpecVersionBin::current(),
                        name,
                        &snap,
                        backup_path,
                        100,
                    )
                    .inject()?,
                ))
            }
        };
        let retries = RepoUpdateRetryConfig {
            default: Some(storage::RetriesSettings {
                max_tries: NonZeroU16::new(3),
                initial_backoff_ms: Some(1),
                max_backoff_ms: Some(1),
            }),
        };
        // Outdates the version `cached` holds.
        other.update_repo_info(&retries, add_tag("other")).await?;
        cached.update_repo_info(&retries, add_tag("cached")).await?;

        let (info, _) = other.fetch_repo_info().await?;
        assert_eq!(info.tag_names()?.sorted().collect::<Vec<_>>(), ["cached", "other"]);
        Ok(())
    }
}
