use std::{collections::HashSet, future::ready, sync::Arc};

use chrono::{DateTime, Utc};
use futures::{stream, Stream, StreamExt, TryStreamExt};
use tokio::pin;

use crate::{
    asset_manager::AssetManager,
    format::{
        manifest::ChunkPayload, ChunkId, IcechunkFormatError, ManifestId, SnapshotId,
    },
    refs::{list_refs, Ref, RefError},
    repository::RepositoryError,
    storage::{self, ListInfo},
    Storage, StorageError,
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
}

impl GCConfig {
    pub fn new(
        extra_roots: HashSet<SnapshotId>,
        dangling_chunks: Action,
        dangling_manifests: Action,
        dangling_attributes: Action,
        dangling_transaction_logs: Action,
        dangling_snapshots: Action,
    ) -> Self {
        GCConfig {
            extra_roots,
            dangling_chunks,
            dangling_manifests,
            dangling_attributes,
            dangling_transaction_logs,
            dangling_snapshots,
        }
    }
    pub fn clean_all(
        chunks_age: DateTime<Utc>,
        metadata_age: DateTime<Utc>,
        extra_roots: Option<HashSet<SnapshotId>>,
    ) -> Self {
        use Action::DeleteIfCreatedBefore as D;
        Self::new(
            extra_roots.unwrap_or_default(),
            D(chunks_age),
            D(metadata_age),
            D(metadata_age),
            D(metadata_age),
            D(metadata_age),
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
}

#[derive(Debug, PartialEq, Eq, Default)]
pub struct GCSummary {
    pub chunks_deleted: usize,
    pub manifests_deleted: usize,
    pub snapshots_deleted: usize,
    pub attributes_deleted: usize,
    pub transaction_logs_deleted: usize,
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

pub async fn garbage_collect(
    storage: &(dyn Storage + Send + Sync),
    storage_settings: &storage::Settings,
    asset_manager: &AssetManager,
    config: &GCConfig,
) -> GCResult<GCSummary> {
    // TODO: this function could have much more parallelism
    if !config.action_needed() {
        return Ok(GCSummary::default());
    }

    let all_snaps =
        pointed_snapshots(storage, storage_settings, asset_manager, &config.extra_roots)
            .await?;

    // FIXME: add attribute files
    // FIXME: add transaction log files
    let mut keep_chunks = HashSet::new();
    let mut keep_manifests = HashSet::new();
    let mut keep_snapshots = HashSet::new();

    pin!(all_snaps);
    while let Some(snap_id) = all_snaps.try_next().await? {
        let snap = asset_manager.fetch_snapshot(&snap_id).await?;
        if config.deletes_snapshots() {
            keep_snapshots.insert(snap_id);
        }

        if config.deletes_manifests() {
            keep_manifests.extend(snap.manifest_files().keys().cloned());
        }

        if config.deletes_chunks() {
            for manifest_id in snap.manifest_files().keys() {
                let manifest_info = snap.manifest_info(manifest_id).ok_or_else(|| {
                    IcechunkFormatError::ManifestInfoNotFound {
                        manifest_id: manifest_id.clone(),
                    }
                })?;
                let manifest = asset_manager
                    .fetch_manifest(manifest_id, manifest_info.size_bytes)
                    .await?;
                let chunk_ids =
                    manifest.chunk_payloads().filter_map(|payload| match payload {
                        ChunkPayload::Ref(chunk_ref) => Some(chunk_ref.id.clone()),
                        _ => None,
                    });
                keep_chunks.extend(chunk_ids);
            }
        }
    }

    let mut summary = GCSummary::default();

    if config.deletes_snapshots() {
        summary.snapshots_deleted =
            gc_snapshots(storage, storage_settings, config, keep_snapshots).await?;
    }
    if config.deletes_manifests() {
        summary.manifests_deleted =
            gc_manifests(storage, storage_settings, config, keep_manifests).await?;
    }
    if config.deletes_chunks() {
        summary.chunks_deleted =
            gc_chunks(storage, storage_settings, config, keep_chunks).await?;
    }

    Ok(summary)
}

async fn all_roots<'a>(
    storage: &'a (dyn Storage + Send + Sync),
    storage_settings: &'a storage::Settings,
    extra_roots: &'a HashSet<SnapshotId>,
) -> GCResult<impl Stream<Item = GCResult<SnapshotId>> + 'a> {
    let all_refs = list_refs(storage, storage_settings).await?;
    // TODO: this could be optimized by not following the ancestry of snapshots that we have
    // already seen
    let roots = stream::iter(all_refs)
        .then(move |r| async move {
            r.fetch(storage, storage_settings).await.map(|ref_data| ref_data.snapshot)
        })
        .err_into()
        .chain(stream::iter(extra_roots.iter().cloned()).map(Ok));
    Ok(roots)
}

async fn pointed_snapshots<'a>(
    storage: &'a (dyn Storage + Send + Sync),
    storage_settings: &'a storage::Settings,
    asset_manager: &'a AssetManager,
    extra_roots: &'a HashSet<SnapshotId>,
) -> GCResult<impl Stream<Item = GCResult<SnapshotId>> + 'a> {
    let roots = all_roots(storage, storage_settings, extra_roots).await?;
    Ok(roots
        .and_then(move |snap_id| async move {
            let snap = asset_manager.fetch_snapshot(&snap_id).await?;
            let parents = asset_manager
                .snapshot_ancestry(snap.id())
                .await?
                .map_ok(|parent| parent.id)
                .err_into();
            Ok(stream::once(ready(Ok(snap_id))).chain(parents))
        })
        .try_flatten())
}

async fn gc_chunks(
    storage: &(dyn Storage + Send + Sync),
    storage_settings: &storage::Settings,
    config: &GCConfig,
    keep_ids: HashSet<ChunkId>,
) -> GCResult<usize> {
    let to_delete = storage
        .list_chunks(storage_settings)
        .await?
        // TODO: don't skip over errors
        .filter_map(move |chunk| {
            ready(chunk.ok().and_then(|chunk| {
                if config.must_delete_chunk(&chunk) && !keep_ids.contains(&chunk.id) {
                    Some(chunk.id.clone())
                } else {
                    None
                }
            }))
        })
        .boxed();
    Ok(storage.delete_chunks(storage_settings, to_delete).await?)
}

async fn gc_manifests(
    storage: &(dyn Storage + Send + Sync),
    storage_settings: &storage::Settings,
    config: &GCConfig,
    keep_ids: HashSet<ManifestId>,
) -> GCResult<usize> {
    let to_delete = storage
        .list_manifests(storage_settings)
        .await?
        // TODO: don't skip over errors
        .filter_map(move |manifest| {
            ready(manifest.ok().and_then(|manifest| {
                if config.must_delete_manifest(&manifest)
                    && !keep_ids.contains(&manifest.id)
                {
                    Some(manifest.id.clone())
                } else {
                    None
                }
            }))
        })
        .boxed();
    Ok(storage.delete_manifests(storage_settings, to_delete).await?)
}

async fn gc_snapshots(
    storage: &(dyn Storage + Send + Sync),
    storage_settings: &storage::Settings,
    config: &GCConfig,
    keep_ids: HashSet<SnapshotId>,
) -> GCResult<usize> {
    let to_delete = storage
        .list_snapshots(storage_settings)
        .await?
        // TODO: don't skip over errors
        .filter_map(move |snapshot| {
            ready(snapshot.ok().and_then(|snapshot| {
                if config.must_delete_snapshot(&snapshot)
                    && !keep_ids.contains(&snapshot.id)
                {
                    Some(snapshot.id.clone())
                } else {
                    None
                }
            }))
        })
        .boxed();
    Ok(storage.delete_snapshots(storage_settings, to_delete).await?)
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
pub async fn expire_ref(
    storage: &(dyn Storage + Send + Sync),
    storage_settings: &storage::Settings,
    asset_manager: &AssetManager,
    reference: &Ref,
    older_than: DateTime<Utc>,
) -> GCResult<HashSet<SnapshotId>> {
    let snap_id = reference
        .fetch(storage, storage_settings)
        .await
        .map(|ref_data| ref_data.snapshot)?;

    // the algorithm works by finding the oldest non expired snap and the root of the repo
    // we do that in a single pass through the ancestry
    // we keep two "pointer" the last editable_snap and the root, and we keep
    // updating them as we navigate the ancestry
    let mut editable_snap = snap_id.clone();
    let mut root = snap_id.clone();

    // here we'll populate the results of every expired snapshot
    let mut released = HashSet::new();
    let ancestry = asset_manager.snapshot_ancestry(&snap_id).await?.peekable();

    pin!(ancestry);

    // If we point to an expired snapshot already, there is nothing to do
    if let Some(Ok(info)) = ancestry.as_mut().peek().await {
        if info.flushed_at < older_than {
            return Ok(HashSet::new());
        }
    }

    while let Some(parent) = ancestry.try_next().await? {
        if parent.flushed_at >= older_than {
            // we are navigating non-expired snaps, last will be kept in editable_snap
            editable_snap = parent.id;
        } else {
            released.insert(parent.id.clone());
            root = parent.id;
        }
    }

    // we counted the root as released, but it's not
    released.remove(&root);

    let editable_snap = asset_manager.fetch_snapshot(&editable_snap).await?;
    let parent_id = editable_snap.parent_id();
    if editable_snap.id() == &root || Some(&root) == parent_id.as_ref() {
        // Either the reference is the root, or it is pointing to the root as first parent
        // Nothing to do
        return Ok(released);
    }

    let root = asset_manager.fetch_snapshot(&root).await?;
    // TODO: add properties to the snapshot that tell us it was history edited
    let new_snapshot = Arc::new(editable_snap.adopt(root.as_ref()));
    asset_manager.write_snapshot(new_snapshot).await?;

    Ok(released)
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
/// Returns the ids of all snapshots considered expired and skipped
/// from history. Notice that this snapshot are not necessarily
/// available for garbage collection, they could still be pointed by
/// ether refs.
///
/// See: https://github.com/earth-mover/icechunk/blob/main/design-docs/007-basic-expiration.md
pub async fn expire(
    storage: &(dyn Storage + Send + Sync),
    storage_settings: &storage::Settings,
    asset_manager: &AssetManager,
    older_than: DateTime<Utc>,
) -> GCResult<HashSet<SnapshotId>> {
    let all_refs = stream::iter(list_refs(storage, storage_settings).await?);

    all_refs
        .then(move |r| async move {
            let released_snaps =
                expire_ref(storage, storage_settings, asset_manager, &r, older_than)
                    .await?;
            Ok::<HashSet<SnapshotId>, GCError>(released_snaps)
        })
        .try_fold(HashSet::new(), |mut accum, new_set| async move {
            accum.extend(new_set);
            Ok(accum)
        })
        .await
}
