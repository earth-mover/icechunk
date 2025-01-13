use std::{collections::HashSet, future::ready, iter};

use chrono::{DateTime, Utc};
use futures::{stream, Stream, StreamExt, TryStreamExt};
use tokio::pin;

use crate::{
    asset_manager::AssetManager,
    format::{
        manifest::ChunkPayload, ChunkId, IcechunkFormatError, ManifestId, SnapshotId,
    },
    refs::{list_refs, RefError},
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
            keep_manifests.extend(snap.manifest_files.iter().map(|mf| mf.id.clone()));
        }

        if config.deletes_chunks() {
            for manifest_file in snap.manifest_files.iter() {
                let manifest_id = &manifest_file.id;
                let manifest_info = snap.manifest_info(manifest_id).ok_or_else(|| {
                    IcechunkFormatError::ManifestInfoNotFound {
                        manifest_id: manifest_id.clone(),
                    }
                })?;
                let manifest =
                    asset_manager.fetch_manifest(manifest_id, manifest_info.size).await?;
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
            // FIXME: this should be global ancestry, not local
            let parents = snap.local_ancestry().map(|parent| parent.id);
            Ok(stream::iter(iter::once(snap_id).chain(parents))
                .map(Ok::<SnapshotId, GCError>))
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
