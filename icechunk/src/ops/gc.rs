use std::{
    collections::HashSet,
    future::ready,
    num::{NonZeroU16, NonZeroUsize},
    sync::{Arc, Mutex},
};

use chrono::{DateTime, Utc};
use futures::{Stream, StreamExt, TryStream, TryStreamExt, stream};
use tokio::{
    pin,
    task::{self},
};
use tracing::instrument;

use crate::{
    Storage, StorageError,
    asset_manager::AssetManager,
    format::{
        ChunkId, FileTypeTag, IcechunkFormatError, ManifestId, ObjectId, SnapshotId,
        manifest::{ChunkPayload, Manifest},
        snapshot::{ManifestFileInfo, Snapshot},
    },
    ops::pointed_snapshots,
    refs::{Ref, RefError, delete_branch, delete_tag, list_refs},
    repository::{RepositoryError, RepositoryErrorKind, RepositoryResult},
    storage::{self, DeleteObjectsResult, ListInfo},
    stream_utils::{StreamLimiter, try_unique_stream},
};

#[derive(Debug, PartialEq, Eq)]
pub enum Action {
    Keep,
    DeleteIfCreatedBefore(DateTime<Utc>),
}

#[derive(Debug)]
pub struct GCConfig {
    extra_roots: HashSet<SnapshotId>,
    dangling_chunks: Action,
    dangling_manifests: Action,
    dangling_attributes: Action,
    dangling_transaction_logs: Action,
    dangling_snapshots: Action,

    max_snapshots_in_memory: NonZeroU16,
    max_compressed_manifest_mem_bytes: NonZeroUsize,
    max_concurrent_manifest_fetches: NonZeroU16,

    dry_run: bool,
}

impl GCConfig {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        extra_roots: HashSet<SnapshotId>,
        dangling_chunks: Action,
        dangling_manifests: Action,
        dangling_attributes: Action,
        dangling_transaction_logs: Action,
        dangling_snapshots: Action,
        max_snapshots_in_memory: NonZeroU16,
        max_compressed_manifest_mem_bytes: NonZeroUsize,
        max_concurrent_manifest_fetches: NonZeroU16,
        dry_run: bool,
    ) -> Self {
        GCConfig {
            extra_roots,
            dangling_chunks,
            dangling_manifests,
            dangling_attributes,
            dangling_transaction_logs,
            dangling_snapshots,
            max_snapshots_in_memory,
            max_compressed_manifest_mem_bytes,
            max_concurrent_manifest_fetches,
            dry_run,
        }
    }
    pub fn clean_all(
        chunks_age: DateTime<Utc>,
        metadata_age: DateTime<Utc>,
        extra_roots: Option<HashSet<SnapshotId>>,
        max_snapshots_in_memory: NonZeroU16,
        max_compressed_manifest_mem_bytes: NonZeroUsize,
        max_concurrent_manifest_fetches: NonZeroU16,
        dry_run: bool,
    ) -> Self {
        use Action::DeleteIfCreatedBefore as D;
        Self::new(
            extra_roots.unwrap_or_default(),
            D(chunks_age),
            D(metadata_age),
            D(metadata_age),
            D(metadata_age),
            D(metadata_age),
            max_snapshots_in_memory,
            max_compressed_manifest_mem_bytes,
            max_concurrent_manifest_fetches,
            dry_run,
        )
    }

    fn action_needed(&self) -> bool {
        [
            &self.dangling_chunks,
            &self.dangling_manifests,
            &self.dangling_attributes,
            &self.dangling_transaction_logs,
            &self.dangling_snapshots,
        ]
        .into_iter()
        .any(|action| action != &Action::Keep)
    }

    pub fn deletes_chunks(&self) -> bool {
        self.dangling_chunks != Action::Keep
    }

    pub fn deletes_manifests(&self) -> bool {
        self.dangling_manifests != Action::Keep
    }

    pub fn deletes_attributes(&self) -> bool {
        self.dangling_attributes != Action::Keep
    }

    pub fn deletes_transaction_logs(&self) -> bool {
        self.dangling_transaction_logs != Action::Keep
    }

    pub fn deletes_snapshots(&self) -> bool {
        self.dangling_snapshots != Action::Keep
    }

    fn must_delete_chunk(&self, chunk: &ListInfo<ChunkId>) -> bool {
        match self.dangling_chunks {
            Action::DeleteIfCreatedBefore(before) => chunk.created_at < before,
            _ => false,
        }
    }

    fn must_delete_manifest(&self, manifest: &ListInfo<ManifestId>) -> bool {
        match self.dangling_manifests {
            Action::DeleteIfCreatedBefore(before) => manifest.created_at < before,
            _ => false,
        }
    }

    fn must_delete_snapshot(&self, snapshot: &ListInfo<SnapshotId>) -> bool {
        match self.dangling_snapshots {
            Action::DeleteIfCreatedBefore(before) => snapshot.created_at < before,
            _ => false,
        }
    }

    fn must_delete_transaction_log(&self, tx_log: &ListInfo<SnapshotId>) -> bool {
        match self.dangling_transaction_logs {
            Action::DeleteIfCreatedBefore(before) => tx_log.created_at < before,
            _ => false,
        }
    }
}

#[derive(Debug, PartialEq, Eq, Default)]
pub struct GCSummary {
    pub bytes_deleted: u64,
    pub chunks_deleted: u64,
    pub manifests_deleted: u64,
    pub snapshots_deleted: u64,
    pub attributes_deleted: u64,
    pub transaction_logs_deleted: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum GCError {
    #[error("ref error {0}")]
    Ref(#[from] RefError),
    #[error("storage error {0}")]
    Storage(#[from] StorageError),
    #[error("repository error {0}")]
    Repository(#[from] RepositoryError),
    #[error("format error {0}")]
    FormatError(#[from] IcechunkFormatError),
}

pub type GCResult<A> = Result<A, GCError>;

async fn snapshot_retained(
    keep_snapshots: Arc<Mutex<HashSet<SnapshotId>>>,
    snap: Arc<Snapshot>,
) -> RepositoryResult<impl TryStream<Ok = ManifestFileInfo, Error = RepositoryError>> {
    // TODO: this could be slightly optimized by not collecting all manifest info records into a vec
    // but we don't expect too many, and they are small anyway
    keep_snapshots
        .lock()
        .map_err(|_| {
            RepositoryError::from(RepositoryErrorKind::Other(
                "can't lock retained snapshots mutex".to_string(),
            ))
        })?
        .insert(snap.id());
    Ok(stream::iter(
        snap.manifest_files().map(Ok::<_, RepositoryError>).collect::<Vec<_>>(),
    ))
}

async fn manifest_retained(
    keep_manifests: Arc<Mutex<HashSet<ManifestId>>>,
    asset_manager: Arc<AssetManager>,
    minfo: ManifestFileInfo,
) -> RepositoryResult<(Arc<Manifest>, ManifestFileInfo)> {
    keep_manifests
        .lock()
        .map_err(|_| {
            RepositoryError::from(RepositoryErrorKind::Other(
                "can't lock retained manifests mutex".to_string(),
            ))
        })?
        .insert(minfo.id.clone());
    let manifest = asset_manager.fetch_manifest(&minfo.id, minfo.size_bytes).await?;
    Ok((manifest, minfo))
}

async fn chunks_retained(
    keep_chunks: Arc<Mutex<HashSet<ChunkId>>>,
    manifest: Arc<Manifest>,
    minfo: ManifestFileInfo,
) -> RepositoryResult<ManifestFileInfo> {
    task::spawn_blocking(move || {
        let chunk_ids = manifest.chunk_payloads().filter_map(|payload| match payload {
            Ok(ChunkPayload::Ref(chunk_ref)) => Some(chunk_ref.id.clone()),
            Ok(_) => None,
            Err(err) => {
                tracing::error!(
                    error = %err,
                    "Error in chunk payload iterator"
                );
                None
            }
        });
        keep_chunks
            .lock()
            .map_err(|_| {
                RepositoryError::from(RepositoryErrorKind::Other(
                    "can't lock retained chunks mutex".to_string(),
                ))
            })?
            .extend(chunk_ids);
        Ok::<_, RepositoryError>(())
    })
    .await??;
    Ok(minfo)
}

#[instrument(skip_all)]
async fn find_retained(
    storage: &(dyn Storage + Send + Sync),
    storage_settings: &storage::Settings,
    asset_manager: Arc<AssetManager>,
    config: &GCConfig,
) -> GCResult<(HashSet<ChunkId>, HashSet<ManifestId>, HashSet<SnapshotId>)> {
    let all_snaps = pointed_snapshots(
        storage,
        storage_settings,
        Arc::clone(&asset_manager),
        &config.extra_roots,
    )
    .await?;

    let keep_chunks = Arc::new(Mutex::new(HashSet::new()));
    let keep_manifests = Arc::new(Mutex::new(HashSet::new()));
    let keep_snapshots = Arc::new(Mutex::new(HashSet::new()));

    let all_manifest_infos = all_snaps
        .map(ready)
        .buffer_unordered(config.max_snapshots_in_memory.get() as usize)
        .and_then(|snap| snapshot_retained(Arc::clone(&keep_snapshots), snap))
        .try_flatten();

    let manifest_infos = try_unique_stream(|mi| mi.id.clone(), all_manifest_infos);

    // we want to fetch many manifests in parallel, but not more than memory allows
    // for this we use the StreamLimiter using the manifest size in bytes for usage
    let limiter = &Arc::new(StreamLimiter::new(
        "garbage_collect".to_string(),
        config.max_compressed_manifest_mem_bytes.get(),
    ));

    let keep_chunks_ref = &keep_chunks;
    let compute_stream = limiter
        .limit_stream(manifest_infos, |minfo| minfo.size_bytes as usize)
        .map_ok(|m| {
            manifest_retained(Arc::clone(&keep_manifests), Arc::clone(&asset_manager), m)
        })
        // Now we can buffer a bunch of fetch_manifest operations. Because we are using
        // StreamLimiter we know memory is not going to blow up
        .try_buffer_unordered(config.max_concurrent_manifest_fetches.get() as usize)
        .and_then(move |(manifest, minfo)| {
            chunks_retained(Arc::clone(keep_chunks_ref), manifest, minfo)
        });

    limiter
        .unlimit_stream(compute_stream, |minfo| minfo.size_bytes as usize)
        .try_for_each(|_| ready(Ok(())))
        .await?;

    debug_assert_eq!(limiter.current_usage(), 0);

    #[allow(clippy::expect_used)]
    Ok((
        Arc::try_unwrap(keep_chunks)
            .expect("Logic error: multiple owners to retained chunks")
            .into_inner()
            .expect("Logic error: multiple owners to retained chunks"),
        Arc::try_unwrap(keep_manifests)
            .expect("Logic error: multiple owners to retained manifests")
            .into_inner()
            .expect("Logic error: multiple owners to retained manifests"),
        Arc::try_unwrap(keep_snapshots)
            .expect("Logic error: multiple owners to retained chunks")
            .into_inner()
            .expect("Logic error: multiple owners to retained chunks"),
    ))
}

pub async fn garbage_collect(
    storage: &(dyn Storage + Send + Sync),
    storage_settings: &storage::Settings,
    asset_manager: Arc<AssetManager>,
    config: &GCConfig,
) -> GCResult<GCSummary> {
    if !storage.can_write() {
        return Err(GCError::Repository(
            RepositoryErrorKind::ReadonlyStorage("Cannot garbage collect".to_string())
                .into(),
        ));
    }

    // TODO: this function could have much more parallelism
    if !config.action_needed() {
        tracing::info!("No action requested");
        return Ok(GCSummary::default());
    }

    tracing::info!("Finding GC roots");
    let (keep_chunks, keep_manifests, keep_snapshots) =
        find_retained(storage, storage_settings, Arc::clone(&asset_manager), config)
            .await?;

    tracing::info!(
        snapshots = keep_snapshots.len(),
        manifests = keep_manifests.len(),
        chunks = keep_chunks.len(),
        "Retained objects collected"
    );

    let mut summary = GCSummary::default();

    tracing::info!("Starting deletes");

    // TODO: this could use more parallelization.
    // The trivial approach of parallelizing the deletes of the different types of objects doesn't
    // work: we want to dolete snapshots before deleting chunks, etc
    if config.deletes_snapshots() {
        let res = gc_snapshots(
            asset_manager.as_ref(),
            storage,
            storage_settings,
            config,
            &keep_snapshots,
        )
        .await?;
        summary.snapshots_deleted = res.deleted_objects;
        summary.bytes_deleted += res.deleted_bytes;
    }
    if config.deletes_transaction_logs() {
        let res = gc_transaction_logs(
            asset_manager.as_ref(),
            storage,
            storage_settings,
            config,
            &keep_snapshots,
        )
        .await?;
        summary.transaction_logs_deleted = res.deleted_objects;
        summary.bytes_deleted += res.deleted_bytes;
    }
    if config.deletes_manifests() {
        let res = gc_manifests(
            asset_manager.as_ref(),
            storage,
            storage_settings,
            config,
            &keep_manifests,
        )
        .await?;
        summary.manifests_deleted = res.deleted_objects;
        summary.bytes_deleted += res.deleted_bytes;
    }
    if config.deletes_chunks() {
        asset_manager.clear_chunk_cache();
        let res = gc_chunks(storage, storage_settings, config, &keep_chunks).await?;
        summary.chunks_deleted = res.deleted_objects;
        summary.bytes_deleted += res.deleted_bytes;
    }

    Ok(summary)
}

async fn fake_delete_result<const SIZE: usize, T: FileTypeTag>(
    to_delete: impl Stream<Item = (ObjectId<SIZE, T>, u64)>,
) -> DeleteObjectsResult {
    to_delete
        .fold(DeleteObjectsResult::default(), |mut res, (_, size)| {
            res.deleted_objects += 1;
            res.deleted_bytes += size;
            ready(res)
        })
        .await
}

#[instrument(skip(storage, storage_settings, config, keep_ids), fields(keep_ids.len = keep_ids.len()))]
async fn gc_chunks(
    storage: &(dyn Storage + Send + Sync),
    storage_settings: &storage::Settings,
    config: &GCConfig,
    keep_ids: &HashSet<ChunkId>,
) -> GCResult<DeleteObjectsResult> {
    tracing::info!("Deleting chunks");
    let to_delete = storage
        .list_chunks(storage_settings)
        .await?
        // TODO: don't skip over errors
        .filter_map(move |chunk| {
            ready(chunk.ok().and_then(|chunk| {
                if config.must_delete_chunk(&chunk) && !keep_ids.contains(&chunk.id) {
                    Some((chunk.id.clone(), chunk.size_bytes))
                } else {
                    None
                }
            }))
        })
        .boxed();
    if config.dry_run {
        Ok(fake_delete_result(to_delete).await)
    } else {
        Ok(storage.delete_chunks(storage_settings, to_delete).await?)
    }
}

#[instrument(skip(asset_manager, storage, storage_settings, config, keep_ids), fields(keep_ids.len = keep_ids.len()))]
async fn gc_manifests(
    asset_manager: &AssetManager,
    storage: &(dyn Storage + Send + Sync),
    storage_settings: &storage::Settings,
    config: &GCConfig,
    keep_ids: &HashSet<ManifestId>,
) -> GCResult<DeleteObjectsResult> {
    tracing::info!("Deleting manifests");
    let to_delete = storage
        .list_manifests(storage_settings)
        .await?
        // TODO: don't skip over errors
        .filter_map(move |manifest| {
            ready(manifest.ok().and_then(|manifest| {
                if config.must_delete_manifest(&manifest)
                    && !keep_ids.contains(&manifest.id)
                {
                    asset_manager.remove_cached_manifest(&manifest.id);
                    Some((manifest.id.clone(), manifest.size_bytes))
                } else {
                    None
                }
            }))
        })
        .boxed();
    if config.dry_run {
        Ok(fake_delete_result(to_delete).await)
    } else {
        Ok(storage.delete_manifests(storage_settings, to_delete).await?)
    }
}

#[instrument(skip(asset_manager, storage, storage_settings, config, keep_ids), fields(keep_ids.len = keep_ids.len()))]
async fn gc_snapshots(
    asset_manager: &AssetManager,
    storage: &(dyn Storage + Send + Sync),
    storage_settings: &storage::Settings,
    config: &GCConfig,
    keep_ids: &HashSet<SnapshotId>,
) -> GCResult<DeleteObjectsResult> {
    tracing::info!("Deleting snapshots");
    let to_delete = storage
        .list_snapshots(storage_settings)
        .await?
        // TODO: don't skip over errors
        .filter_map(move |snapshot| {
            ready(snapshot.ok().and_then(|snapshot| {
                if config.must_delete_snapshot(&snapshot)
                    && !keep_ids.contains(&snapshot.id)
                {
                    asset_manager.remove_cached_snapshot(&snapshot.id);
                    Some((snapshot.id.clone(), snapshot.size_bytes))
                } else {
                    None
                }
            }))
        })
        .boxed();
    if config.dry_run {
        Ok(fake_delete_result(to_delete).await)
    } else {
        Ok(storage.delete_snapshots(storage_settings, to_delete).await?)
    }
}

#[instrument(skip(asset_manager, storage, storage_settings, config, keep_ids), fields(keep_ids.len = keep_ids.len()))]
async fn gc_transaction_logs(
    asset_manager: &AssetManager,
    storage: &(dyn Storage + Send + Sync),
    storage_settings: &storage::Settings,
    config: &GCConfig,
    keep_ids: &HashSet<SnapshotId>,
) -> GCResult<DeleteObjectsResult> {
    tracing::info!("Deleting transaction logs");
    let to_delete = storage
        .list_transaction_logs(storage_settings)
        .await?
        // TODO: don't skip over errors
        .filter_map(move |tx| {
            ready(tx.ok().and_then(|tx| {
                if config.must_delete_transaction_log(&tx) && !keep_ids.contains(&tx.id) {
                    asset_manager.remove_cached_tx_log(&tx.id);
                    Some((tx.id.clone(), tx.size_bytes))
                } else {
                    None
                }
            }))
        })
        .boxed();
    if config.dry_run {
        Ok(fake_delete_result(to_delete).await)
    } else {
        Ok(storage.delete_transaction_logs(storage_settings, to_delete).await?)
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum ExpireRefResult {
    NothingToDo {
        ref_is_expired: bool,
    },
    Done {
        released_snapshots: HashSet<SnapshotId>,
        edited_snapshot: SnapshotId,
        ref_is_expired: bool,
    },
}

/// Expire snapshots older than a threshold.
///
/// This only processes snapshots found by navigating `reference`
/// ancestry. Any other snapshots are not touched.
///
/// The operation will edit in place the oldest non-expired snapshot,
/// changing its parent to be the root of the repo.
///
/// For this reasons, it's recommended to invalidate any snapshot
/// caches before traversing history againg. The cache in the
/// passed `asset_manager` is invalidated here, but other caches
/// may exist, for example, in [`Repository`] instances.
///
/// Returns the ids of all snapshots considered expired and skipped
/// from history. Notice that this snapshot are not necessarily
/// available for garbage collection, they could still be pointed by
/// ether refs.
///
/// See: https://github.com/earth-mover/icechunk/blob/main/design-docs/007-basic-expiration.md
#[instrument(skip(asset_manager, storage))]
pub async fn expire_ref(
    storage: &(dyn Storage + Send + Sync),
    storage_settings: &storage::Settings,
    asset_manager: Arc<AssetManager>,
    reference: &Ref,
    older_than: DateTime<Utc>,
) -> GCResult<ExpireRefResult> {
    let snap_id = reference
        .fetch(storage, storage_settings)
        .await
        .map(|ref_data| ref_data.snapshot)?;

    tracing::info!("Starting expiration at ref {}", snap_id);

    // the algorithm works by finding the oldest non expired snap and the root of the repo
    // we do that in a single pass through the ancestry
    // we keep two "pointer" the last editable_snap and the root, and we keep
    // updating them as we navigate the ancestry
    let mut editable_snap = snap_id.clone();
    let mut root = snap_id.clone();

    // here we'll populate the results of every expired snapshot
    let mut released = HashSet::new();
    let ancestry =
        Arc::clone(&asset_manager).snapshot_info_ancestry(&snap_id).await?.peekable();

    pin!(ancestry);

    let mut ref_is_expired = false;
    if let Some(Ok(info)) = ancestry.as_mut().peek().await
        && info.flushed_at < older_than
    {
        tracing::debug!(flushed_at = %info.flushed_at, "Ref flagged as expired");
        ref_is_expired = true;
    }

    while let Some(parent) = ancestry.try_next().await? {
        if parent.flushed_at >= older_than {
            tracing::debug!(snap = %parent.id, flushed_at = %parent.flushed_at, "Processing non expired snapshot");
            // we are navigating non-expired snaps, last will be kept in editable_snap
            editable_snap = parent.id;
        } else {
            tracing::debug!(snap = %parent.id, flushed_at = %parent.flushed_at, "Processing expired snapshot");
            released.insert(parent.id.clone());
            root = parent.id;
        }
    }

    // we counted the root as released, but it's not
    released.remove(&root);

    let editable_snap = asset_manager.fetch_snapshot(&editable_snap).await?;

    let old_parent_id = editable_snap.parent_id();
    if editable_snap.id() == root    // only root can be expired
        || Some(&root) == old_parent_id.as_ref()
        // we never found an expirable snap
        || root == snap_id
    {
        // Either the reference is the root, or it is pointing to the root as first parent
        // Nothing to do
        tracing::info!("Nothing to expire for this ref");
        return Ok(ExpireRefResult::NothingToDo { ref_is_expired });
    }

    let root = asset_manager.fetch_snapshot(&root).await?;
    // we don't want to create loops, so:
    // we never edit the root of a tree
    assert!(editable_snap.parent_id().is_some());
    // and, we only set a root as parent
    assert!(root.parent_id().is_none());

    tracing::info!(root = %root.id(), editable_snap=%editable_snap.id(), "Expiration needed for this ref");

    // TODO: add properties to the snapshot that tell us it was history edited
    let new_snapshot = Arc::new(root.adopt(&editable_snap)?);
    asset_manager.write_snapshot(new_snapshot).await?;
    tracing::info!("Snapshot overwritten");

    Ok(ExpireRefResult::Done {
        released_snapshots: released,
        edited_snapshot: editable_snap.id().clone(),
        ref_is_expired,
    })
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum ExpiredRefAction {
    Delete,
    Ignore,
}

#[derive(Debug, PartialEq, Eq, Clone, Default)]
pub struct ExpireResult {
    pub released_snapshots: HashSet<SnapshotId>,
    pub edited_snapshots: HashSet<SnapshotId>,
    pub deleted_refs: HashSet<Ref>,
}

/// Expire all snapshots older than a threshold.
///
/// This processes snapshots found by navigating all references in
/// the repo, tags first, branches leter, both in lexicographical order.
///
/// The operation will edit in place the oldest non-expired snapshot,
/// in every ancestry, changing its parent to be the root of the repo.
///
/// For this reasons, it's recommended to invalidate any snapshot
/// caches before traversing history againg. The cache in the
/// passed `asset_manager` is invalidated here, but other caches
/// may exist, for example, in [`Repository`] instances.
///
/// Notice that the snapshot returned as released, are not necessarily
/// available for garbage collection, they could still be pointed by
/// ether refs.
///
/// See: https://github.com/earth-mover/icechunk/blob/main/design-docs/007-basic-expiration.md
#[instrument(skip(asset_manager, storage))]
pub async fn expire(
    storage: &(dyn Storage + Send + Sync),
    storage_settings: &storage::Settings,
    asset_manager: Arc<AssetManager>,
    older_than: DateTime<Utc>,
    expired_branches: ExpiredRefAction,
    expired_tags: ExpiredRefAction,
) -> GCResult<ExpireResult> {
    if !storage.can_write() {
        return Err(GCError::Repository(
            RepositoryErrorKind::ReadonlyStorage("Cannot expire snapshots".to_string())
                .into(),
        ));
    }

    let all_refs = stream::iter(list_refs(storage, storage_settings).await?);
    let asset_manager = Arc::clone(&asset_manager.clone());

    all_refs
        .then(move |r| {
            let asset_manager = asset_manager.clone();
            async move {
                let ref_result =
                    expire_ref(storage, storage_settings, asset_manager, &r, older_than)
                        .await?;
                Ok::<(Ref, ExpireRefResult), GCError>((r, ref_result))
            }
        })
        .try_fold(ExpireResult::default(), |mut result, (r, ref_result)| async move {
            let ref_is_expired = match ref_result {
                ExpireRefResult::Done {
                    released_snapshots,
                    edited_snapshot,
                    ref_is_expired,
                } => {
                    result.released_snapshots.extend(released_snapshots.into_iter());
                    result.edited_snapshots.insert(edited_snapshot);
                    ref_is_expired
                }
                ExpireRefResult::NothingToDo { ref_is_expired } => ref_is_expired,
            };
            if ref_is_expired {
                match &r {
                    Ref::Tag(name) => {
                        if expired_tags == ExpiredRefAction::Delete {
                            tracing::info!(name, "Deleting expired tag");
                            delete_tag(storage, storage_settings, name.as_str())
                                .await
                                .map_err(GCError::Ref)?;
                            result.deleted_refs.insert(r);
                        }
                    }
                    Ref::Branch(name) => {
                        if expired_branches == ExpiredRefAction::Delete
                            && name != Ref::DEFAULT_BRANCH
                        {
                            tracing::info!(name, "Deleting expired branch");
                            delete_branch(storage, storage_settings, name.as_str())
                                .await
                                .map_err(GCError::Ref)?;
                            result.deleted_refs.insert(r);
                        }
                    }
                }
            }
            Ok(result)
        })
        .await
}
