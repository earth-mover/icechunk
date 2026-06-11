//! Debugging utilities for examining repository state.
//!
//! Provides [`snapshot_json`] and [`repo_info_json`] for serializing
//! repository information to JSON.

use std::collections::BTreeMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use itertools::Itertools as _;
use serde::{Deserialize, Serialize};

use crate::{
    asset_manager::AssetManager,
    format::{
        IcechunkFormatError, IcechunkFormatErrorKind, ManifestId, SnapshotId,
        format_constants::SpecVersionBin,
        manifest::{ChunkPayload, ManifestRef},
        repo_info::UpdateType,
        snapshot::{
            ManifestFileInfo, NodeData, NodeSnapshot, NodeType, SnapshotProperties,
        },
        transaction_log::TransactionLog,
    },
    repository::{RepositoryError, RepositoryErrorKind, RepositoryResult},
    storage::StorageErrorKind,
};
use icechunk_types::{ICResultExt as _, error::ICResultCtxExt as _};

#[derive(Debug, Serialize, Deserialize)]
struct ManifestFileInfoInspect {
    id: String,
    size_bytes: u64,
    num_chunk_refs: u32,
}

impl From<ManifestFileInfo> for ManifestFileInfoInspect {
    fn from(value: ManifestFileInfo) -> Self {
        Self {
            id: value.id.to_string(),
            size_bytes: value.size_bytes,
            num_chunk_refs: value.num_chunk_refs,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct ManifestRefInspect {
    id: String,
    extents: Vec<(u32, u32)>,
}

impl From<ManifestRef> for ManifestRefInspect {
    fn from(value: ManifestRef) -> Self {
        Self {
            id: value.object_id.to_string(),
            extents: value.extents.iter().map(|r| (r.start, r.end)).collect(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct NodeSnapshotInspect {
    id: String,
    path: String,
    node_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    manifest_refs: Option<Vec<ManifestRefInspect>>,
}

impl From<NodeSnapshot> for NodeSnapshotInspect {
    fn from(value: NodeSnapshot) -> Self {
        Self {
            id: value.id.to_string(),
            path: value.path.to_string(),
            node_type: match value.node_type() {
                NodeType::Group => "group".to_string(),
                NodeType::Array => "array".to_string(),
            },
            manifest_refs: match value.node_data {
                NodeData::Array { manifests, .. } => {
                    let ms = manifests.into_iter().map(|m| m.into()).collect();
                    Some(ms)
                }
                NodeData::Group => None,
            },
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct SnapshotInfoInspect {
    // TODO: add fields
    //path: String,
    //size_bytes: u64,
    id: String,
    flushed_at: DateTime<Utc>,
    commit_message: String,
    metadata: SnapshotProperties,

    manifests: Vec<ManifestFileInfoInspect>,
    nodes: Vec<NodeSnapshotInspect>,
}

async fn inspect_snapshot(
    asset_manager: &AssetManager,
    id: &SnapshotId,
) -> RepositoryResult<SnapshotInfoInspect> {
    let snap = asset_manager.fetch_snapshot(id).await?;
    let res = SnapshotInfoInspect {
        id: snap.id().to_string(),
        flushed_at: snap.flushed_at().inject()?,
        commit_message: snap.message(),
        metadata: snap.metadata().inject()?,
        manifests: snap.manifest_files().map_ok(|f| f.into()).try_collect().inject()?,
        nodes: snap.iter().map_ok(|n| n.into()).try_collect().inject()?,
    };

    Ok(res)
}

pub async fn snapshot_json(
    asset_manager: &AssetManager,
    id: &SnapshotId,
    pretty: bool,
) -> RepositoryResult<String> {
    let info = inspect_snapshot(asset_manager, id).await?;
    let res = if pretty {
        serde_json::to_string_pretty(&info)
    } else {
        serde_json::to_string(&info)
    }
    .map_err(|e| RepositoryErrorKind::Other(e.to_string()))
    .capture()?;
    Ok(res)
}

#[derive(Debug, Serialize, Deserialize)]
struct UpdateTypeInspect {
    #[serde(rename = "type")]
    type_name: String,
    #[serde(flatten)]
    details: serde_json::Value,
}

impl From<UpdateType> for UpdateTypeInspect {
    fn from(value: UpdateType) -> Self {
        match value {
            UpdateType::RepoInitializedUpdate => Self {
                type_name: "RepoInitializedUpdate".into(),
                details: serde_json::json!({}),
            },
            UpdateType::RepoMigratedUpdate { from_version, to_version } => Self {
                type_name: "RepoMigratedUpdate".into(),
                details: serde_json::json!({
                    "from_version": from_version.to_string(),
                    "to_version": to_version.to_string(),
                }),
            },
            UpdateType::RepoStatusChangedUpdate { status } => Self {
                type_name: "RepoStatusChangedUpdate".into(),
                details: serde_json::json!({ "status": status }),
            },
            UpdateType::ConfigChangedUpdate => Self {
                type_name: "ConfigChangedUpdate".into(),
                details: serde_json::json!({}),
            },
            UpdateType::MetadataChangedUpdate => Self {
                type_name: "MetadataChangedUpdate".into(),
                details: serde_json::json!({}),
            },
            UpdateType::TagCreatedUpdate { name } => Self {
                type_name: "TagCreatedUpdate".into(),
                details: serde_json::json!({ "name": name }),
            },
            UpdateType::TagDeletedUpdate { name, previous_snap_id } => Self {
                type_name: "TagDeletedUpdate".into(),
                details: serde_json::json!({
                    "name": name,
                    "previous_snap_id": previous_snap_id.to_string(),
                }),
            },
            UpdateType::BranchCreatedUpdate { name } => Self {
                type_name: "BranchCreatedUpdate".into(),
                details: serde_json::json!({ "name": name }),
            },
            UpdateType::BranchDeletedUpdate { name, previous_snap_id } => Self {
                type_name: "BranchDeletedUpdate".into(),
                details: serde_json::json!({
                    "name": name,
                    "previous_snap_id": previous_snap_id.to_string(),
                }),
            },
            UpdateType::BranchResetUpdate { name, previous_snap_id } => Self {
                type_name: "BranchResetUpdate".into(),
                details: serde_json::json!({
                    "name": name,
                    "previous_snap_id": previous_snap_id.to_string(),
                }),
            },
            UpdateType::NewCommitUpdate { branch, new_snap_id } => Self {
                type_name: "NewCommitUpdate".into(),
                details: serde_json::json!({
                    "branch": branch,
                    "new_snap_id": new_snap_id.to_string(),
                }),
            },
            UpdateType::CommitAmendedUpdate { branch, previous_snap_id, new_snap_id } => {
                Self {
                    type_name: "CommitAmendedUpdate".into(),
                    details: serde_json::json!({
                        "branch": branch,
                        "previous_snap_id": previous_snap_id.to_string(),
                        "new_snap_id": new_snap_id.to_string(),
                    }),
                }
            }
            UpdateType::NewDetachedSnapshotUpdate { new_snap_id } => Self {
                type_name: "NewDetachedSnapshotUpdate".into(),
                details: serde_json::json!({
                    "new_snap_id": new_snap_id.to_string(),
                }),
            },
            UpdateType::GCRanUpdate => {
                Self { type_name: "GCRanUpdate".into(), details: serde_json::json!({}) }
            }
            UpdateType::ExpirationRanUpdate => Self {
                type_name: "ExpirationRanUpdate".into(),
                details: serde_json::json!({}),
            },
            UpdateType::FeatureFlagChanged { id, new_value } => Self {
                type_name: "FeatureFlagChanged".into(),
                details: serde_json::json!({"id": id, "new_value": new_value}),
            },
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct UpdateInspect {
    update_type: UpdateTypeInspect,
    updated_at: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    backup_path: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct RepoInfoSnapshotInspect {
    id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    parent_id: Option<String>,
    flushed_at: DateTime<Utc>,
    message: String,
    metadata: SnapshotProperties,
}

#[derive(Debug, Serialize, Deserialize)]
struct RepoInfoInspect {
    spec_version: String,
    branches: BTreeMap<String, String>,
    tags: BTreeMap<String, String>,
    deleted_tags: Vec<String>,
    snapshots: Vec<RepoInfoSnapshotInspect>,
    metadata: SnapshotProperties,
    latest_updates: Vec<UpdateInspect>,
    #[serde(skip_serializing_if = "Option::is_none")]
    repo_before_updates: Option<String>,
}

async fn inspect_repo_info(
    asset_manager: &AssetManager,
) -> RepositoryResult<RepoInfoInspect> {
    let (info, _) = asset_manager.fetch_repo_info().await?;

    let branches = info
        .branches()
        .inject()?
        .map(|(branch, snapshot)| (branch.to_string(), snapshot.to_string()))
        .collect::<BTreeMap<_, _>>();

    let tags = info
        .tags()
        .inject()?
        .map(|(tag, snapshot)| (tag.to_string(), snapshot.to_string()))
        .collect::<BTreeMap<_, _>>();

    let deleted_tags =
        info.deleted_tags().inject()?.map(|s| s.to_string()).collect::<Vec<_>>();

    let snapshots = info
        .all_snapshots()
        .inject()?
        .map_ok(|snap| RepoInfoSnapshotInspect {
            id: snap.id.to_string(),
            parent_id: snap.parent_id.map(|x| x.to_string()),
            flushed_at: snap.flushed_at,
            message: snap.message,
            metadata: snap.metadata,
        })
        .collect::<Result<Vec<RepoInfoSnapshotInspect>, _>>()
        .inject()?;

    let latest_updates = info
        .latest_updates()
        .inject()?
        .map_ok(|(update_type, at, path)| UpdateInspect {
            update_type: update_type.into(),
            updated_at: at,
            backup_path: path.map(|x| x.to_string()),
        })
        .collect::<Result<Vec<UpdateInspect>, _>>()
        .inject()?;

    Ok(RepoInfoInspect {
        spec_version: info.spec_version().inject()?.to_string(),
        branches,
        tags,
        deleted_tags,
        snapshots,
        metadata: info.metadata().inject()?,
        latest_updates,
        repo_before_updates: info.repo_before_updates().inject()?.map(|x| x.to_string()),
    })
}

pub async fn repo_info_json(
    asset_manager: &AssetManager,
    pretty: bool,
) -> RepositoryResult<String> {
    let info = inspect_repo_info(asset_manager).await?;
    let res = if pretty {
        serde_json::to_string_pretty(&info)
    } else {
        serde_json::to_string(&info)
    }
    .map_err(|e| RepositoryErrorKind::Other(e.to_string()))
    .capture()?;
    Ok(res)
}

#[derive(Debug, Serialize, Deserialize)]
struct ArrayManifestInspect {
    node_id: String,
    num_chunk_refs: usize,
    num_inline: usize,
    num_native: usize,
    num_virtual: usize,
}

#[derive(Debug, Serialize, Deserialize)]
struct ManifestVirtualChunksCompressionInspect {
    uses_location_compression: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    dictionary_size_bytes: Option<usize>,
    num_compressed_refs: usize,
}

#[derive(Debug, Serialize, Deserialize)]
struct ManifestInspect {
    id: String,
    size_bytes: usize,
    num_arrays: usize,
    total_chunk_refs: usize,
    total_inline: usize,
    total_native: usize,
    total_virtual: usize,
    arrays: Vec<ArrayManifestInspect>,
    compression: ManifestVirtualChunksCompressionInspect,
}

async fn inspect_manifest(
    asset_manager: &AssetManager,
    manifest_id: &ManifestId,
) -> RepositoryResult<ManifestInspect> {
    let manifest = asset_manager.fetch_manifest_unknown_size(manifest_id).await?;
    let node_ids: Vec<_> = manifest.arrays().collect();
    let mut arrays = Vec::with_capacity(node_ids.len());
    for node_id in node_ids {
        let (mut num_inline, mut num_native, mut num_virtual) = (0, 0, 0);
        let node_id_str = node_id.to_string();
        for payload in Arc::clone(&manifest).iter(node_id).inject()? {
            match payload.inject()? {
                (_, ChunkPayload::Inline(_)) => num_inline += 1,
                (_, ChunkPayload::Ref(_)) => num_native += 1,
                (_, ChunkPayload::Virtual(_)) => num_virtual += 1,
                (_, _) => {}
            }
        }
        arrays.push(ArrayManifestInspect {
            node_id: node_id_str,
            num_chunk_refs: num_inline + num_native + num_virtual,
            num_inline,
            num_native,
            num_virtual,
        });
    }
    let (total_inline, total_native, total_virtual) =
        arrays.iter().fold((0, 0, 0), |(i, n, v), a| {
            (i + a.num_inline, n + a.num_native, v + a.num_virtual)
        });
    let compression = ManifestVirtualChunksCompressionInspect {
        uses_location_compression: manifest.uses_location_compression(),
        dictionary_size_bytes: manifest.location_dictionary_size(),
        num_compressed_refs: manifest.num_compressed_refs(),
    };
    Ok(ManifestInspect {
        id: manifest.id().to_string(),
        size_bytes: manifest.bytes().len(),
        num_arrays: arrays.len(),
        total_chunk_refs: total_inline + total_native + total_virtual,
        total_inline,
        total_native,
        total_virtual,
        arrays,
        compression,
    })
}

pub async fn manifest_json(
    asset_manager: &AssetManager,
    manifest_id: &ManifestId,
    pretty: bool,
) -> RepositoryResult<String> {
    let info = inspect_manifest(asset_manager, manifest_id).await?;
    let res = if pretty {
        serde_json::to_string_pretty(&info)
    } else {
        serde_json::to_string(&info)
    }
    .map_err(|e| RepositoryErrorKind::Other(e.to_string()))
    .capture()?;
    Ok(res)
}

#[derive(Debug, Serialize, Deserialize)]
struct MoveInspect {
    from: String,
    to: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct UpdatedChunksInspect {
    node_id: String,
    chunks: Vec<Vec<u32>>,
}

/// Marks a [`TransactionLogInspect`] that is not a single on-disk log but a
/// synthetic merge, because the snapshot's ancestry was collapsed by expiration.
#[derive(Debug, Serialize, Deserialize)]
struct SyntheticCompositeInspect {
    note: String,
    /// Pruned-ancestor transaction logs merged into this view, oldest first.
    merged_pruned_ancestor_tx_logs: Vec<String>,
    /// Referenced pruned-ancestor logs absent from storage, so skipped. Expected
    /// only when an older GC (predating pruned-log retention) deleted them.
    missing_tx_logs: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct TransactionLogInspect {
    new_groups: Vec<String>,
    new_arrays: Vec<String>,
    deleted_groups: Vec<String>,
    deleted_arrays: Vec<String>,
    updated_groups: Vec<String>,
    updated_arrays: Vec<String>,
    updated_chunks: Vec<UpdatedChunksInspect>,
    moved_nodes: Vec<MoveInspect>,
    #[serde(skip_serializing_if = "Option::is_none")]
    synthetic_composite: Option<SyntheticCompositeInspect>,
}

/// Pure transform of a single transaction log into its inspect representation.
fn inspect_transaction_log(
    tx: &TransactionLog,
) -> RepositoryResult<TransactionLogInspect> {
    Ok(TransactionLogInspect {
        new_groups: tx.new_groups().map(|id| id.to_string()).collect(),
        new_arrays: tx.new_arrays().map(|id| id.to_string()).collect(),
        deleted_groups: tx.deleted_groups().map(|id| id.to_string()).collect(),
        deleted_arrays: tx.deleted_arrays().map(|id| id.to_string()).collect(),
        updated_groups: tx.updated_groups().map(|id| id.to_string()).collect(),
        updated_arrays: tx.updated_arrays().map(|id| id.to_string()).collect(),
        updated_chunks: tx
            .updated_chunks()
            .map(|(node_id, chunks)| UpdatedChunksInspect {
                node_id: node_id.to_string(),
                chunks: chunks.map(|c| c.0.clone()).collect(),
            })
            .collect(),
        moved_nodes: tx
            .moves()
            .map(|m| {
                let m = m.inject()?;
                Ok(MoveInspect { from: m.from.to_string(), to: m.to.to_string() })
            })
            .try_collect()?,
        synthetic_composite: None,
    })
}

/// The transaction-log ids of ancestors expiration pruned from under `id`.
/// Returns empty if the snapshot id is not found, because it means it was
/// itself expired, so its transaction log has no pruned children
/// (they were moved already)
async fn pruned_ancestor_tx_logs(
    asset_manager: &AssetManager,
    id: &SnapshotId,
) -> RepositoryResult<Vec<SnapshotId>> {
    if asset_manager.spec_version() <= SpecVersionBin::V1 {
        return Ok(Vec::new());
    }
    let (repo_info, _) = asset_manager.fetch_repo_info().await?;
    match repo_info.find_snapshot(id) {
        Ok(si) => Ok(si.pruned_ancestor_tx_logs),
        Err(IcechunkFormatError {
            kind: IcechunkFormatErrorKind::SnapshotIdNotFound { .. },
            ..
        }) => Ok(Vec::new()),
        Err(e) => Err(e.inject()),
    }
}

async fn inspect_snapshot_tx_logs(
    asset_manager: &AssetManager,
    id: &SnapshotId,
) -> RepositoryResult<TransactionLogInspect> {
    let own = asset_manager.fetch_transaction_log(id).await?;
    let pruned_ids = pruned_ancestor_tx_logs(asset_manager, id).await?;
    if pruned_ids.is_empty() {
        return inspect_transaction_log(&own);
    }

    let mut pruned_logs = Vec::with_capacity(pruned_ids.len());
    let mut merged_ids = Vec::new();
    let mut missing = Vec::new();
    // TODO: performance, concurrency opportunity here, we are fetching sequentially
    for pruned_id in &pruned_ids {
        match asset_manager.fetch_transaction_log(pruned_id).await {
            Ok(log) => {
                pruned_logs.push(log);
                merged_ids.push(pruned_id.to_string());
            }
            Err(RepositoryError {
                kind: RepositoryErrorKind::StorageError(StorageErrorKind::ObjectNotFound),
                ..
            }) => {
                tracing::warn!(
                    snapshot_id = %pruned_id,
                    "Transaction log of an expiration-pruned ancestor is missing \
                     (likely deleted by an older Icechunk GC); the composite log view \
                     will be incomplete"
                );
                missing.push(pruned_id.to_string());
            }
            Err(e) => return Err(e),
        }
    }

    // pruned logs oldest first, then this snapshot's own log
    let to_merge = pruned_logs.iter().map(Arc::as_ref).chain([own.as_ref()]);
    // FIXME: this can (and will for large repos with expiration) blow the tx log buffer limit
    let merged = TransactionLog::merge(id, to_merge).inject()?;
    let mut info = inspect_transaction_log(&merged)?;
    info.synthetic_composite = Some(SyntheticCompositeInspect {
        note: "Synthetic composite: this snapshot's ancestry was collapsed by \
               expiration; the log below merges its pruned-ancestor logs with its \
               own log."
            .to_string(),
        merged_pruned_ancestor_tx_logs: merged_ids,
        missing_tx_logs: missing,
    });
    Ok(info)
}

pub async fn transaction_log_json(
    asset_manager: &AssetManager,
    id: &SnapshotId,
    pretty: bool,
) -> RepositoryResult<String> {
    let info = inspect_snapshot_tx_logs(asset_manager, id).await?;
    let res = if pretty {
        serde_json::to_string_pretty(&info)
    } else {
        serde_json::to_string(&info)
    }
    .map_err(|e| RepositoryErrorKind::Other(e.to_string()))
    .capture()?;
    Ok(res)
}

#[cfg(all(test, feature = "object-store-fs"))]
mod tests {
    use super::*;
    use crate::{ObjectStorage, Repository, repository::VersionInfo};
    use futures::{StreamExt as _, TryStreamExt as _};
    use std::{path::PathBuf, sync::Arc};

    #[icechunk_macros::tokio_test]
    async fn test_print_snapshot() -> Result<(), Box<dyn std::error::Error>> {
        let st = Arc::new(
            ObjectStorage::new_local_filesystem(&PathBuf::from(
                "../icechunk-python/tests/data/split-repo-v2",
            ))
            .await?,
        );
        let repo = Repository::open(None, st, Default::default()).await?;
        let snap_id = repo
            .ancestry(&VersionInfo::BranchTipRef("main".to_string()))
            .await?
            .boxed()
            .try_next()
            .await?
            .unwrap()
            .id;

        let json = snapshot_json(repo.asset_manager(), &snap_id, true).await?;
        let info: SnapshotInfoInspect = serde_json::from_str(json.as_str())?;
        assert!(info.id == snap_id.to_string());

        Ok(())
    }

    #[icechunk_macros::tokio_test]
    async fn test_inspect_repo_info() -> Result<(), Box<dyn std::error::Error>> {
        let st = Arc::new(
            ObjectStorage::new_local_filesystem(&PathBuf::from(
                "../icechunk-python/tests/data/split-repo-v2",
            ))
            .await?,
        );
        let repo = Repository::open(None, st, Default::default()).await?;

        let json = repo_info_json(repo.asset_manager(), true).await?;
        let info: RepoInfoInspect = serde_json::from_str(json.as_str())?;
        assert!(info.branches.contains_key("main"));
        assert!(!info.snapshots.is_empty());

        // compact mode has no newlines
        let compact = repo_info_json(repo.asset_manager(), false).await?;
        assert!(!compact.contains('\n'));

        Ok(())
    }
}
