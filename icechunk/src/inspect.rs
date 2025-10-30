use chrono::{DateTime, Utc};
use itertools::Itertools;
use serde::{Deserialize, Serialize};

use crate::{
    asset_manager::AssetManager,
    format::{
        IcechunkFormatError, ManifestId, SnapshotId,
        manifest::{Manifest, ManifestRef},
        snapshot::{
            ManifestFileInfo, NodeData, NodeSnapshot, NodeType, SnapshotProperties,
        },
    },
    repository::{RepositoryErrorKind, RepositoryResult},
};

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
    #[serde(skip_serializing_if = "Option::is_none")]
    parent_id: Option<String>,
    flushed_at: DateTime<Utc>,
    commit_message: String,
    metadata: SnapshotProperties,

    manifests: Vec<ManifestFileInfoInspect>,
    nodes: Vec<NodeSnapshotInspect>,
}

#[derive(Debug, Serialize, Deserialize)]
struct ArrayManifestInspect {
    node_id: String,
    num_chunk_refs: u64,
}

#[derive(Debug, Serialize, Deserialize)]
struct ManifestInfoInspect {
    id: String,
    size_bytes: u64,
    total_chunk_refs: u64,
    arrays: Vec<ArrayManifestInspect>,
}

impl TryFrom<&Manifest> for ManifestInfoInspect {
    type Error = IcechunkFormatError;

    fn try_from(value: &Manifest) -> Result<Self, Self::Error> {
        let arrays = value
            .nodes()
            .filter_map(|node_id| {
                value.node_refs(&node_id).map(|num_chunk_refs| ArrayManifestInspect {
                    node_id: node_id.to_string(),
                    num_chunk_refs: num_chunk_refs as u64,
                })
            })
            .collect();
        Ok(Self {
            id: value.id().to_string(),
            size_bytes: value.bytes().len() as u64,
            total_chunk_refs: value.len() as u64,
            arrays,
        })
    }
}

async fn inspect_manifest(
    asset_manager: &AssetManager,
    id: &ManifestId,
) -> RepositoryResult<ManifestInfoInspect> {
    let manifest = asset_manager.fetch_manifest_unknown_size(id).await?;
    Ok(manifest.as_ref().try_into()?)
}

pub async fn manifest_json(
    asset_manager: &AssetManager,
    id: &ManifestId,
    pretty: bool,
) -> RepositoryResult<String> {
    let info = inspect_manifest(asset_manager, id).await?;
    let res = if pretty {
        serde_json::to_string_pretty(&info)
    } else {
        serde_json::to_string(&info)
    }
    .map_err(|e| RepositoryErrorKind::Other(e.to_string()))?;
    Ok(res)
}

async fn inspect_snapshot(
    asset_manager: &AssetManager,
    id: &SnapshotId,
) -> RepositoryResult<SnapshotInfoInspect> {
    let snap = asset_manager.fetch_snapshot(id).await?;
    let res = SnapshotInfoInspect {
        id: snap.id().to_string(),
        parent_id: snap.parent_id().map(|p| p.to_string()),
        flushed_at: snap.flushed_at()?,
        commit_message: snap.message(),
        metadata: snap.metadata()?,
        manifests: snap.manifest_files().map(|f| f.into()).collect(),
        nodes: snap.iter().map_ok(|n| n.into()).try_collect()?,
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
    .map_err(|e| RepositoryErrorKind::Other(e.to_string()))?;
    Ok(res)
}

#[cfg(test)]
#[allow(clippy::panic, clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use crate::{ObjectStorage, Repository, repository::VersionInfo};
    use futures::{StreamExt, TryStreamExt};
    use std::{path::PathBuf, sync::Arc};

    #[icechunk_macros::tokio_test]
    async fn test_print_snapshot() -> Result<(), Box<dyn std::error::Error>> {
        let st = Arc::new(
            ObjectStorage::new_local_filesystem(&PathBuf::from(
                "../icechunk-python/tests/data/split-repo",
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
    async fn test_print_manifest() -> Result<(), Box<dyn std::error::Error>> {
        let st = Arc::new(
            ObjectStorage::new_local_filesystem(&PathBuf::from(
                "../icechunk-python/tests/data/split-repo",
            ))
            .await?,
        );
        let repo = Repository::open(None, st, Default::default()).await?;
        let snap = repo
            .ancestry(&VersionInfo::BranchTipRef("main".to_string()))
            .await?
            .boxed()
            .try_next()
            .await?
            .unwrap();

        let manifest_id = &snap.manifests[0].id;

        let json = manifest_json(repo.asset_manager(), manifest_id, true).await?;
        let info: ManifestInfoInspect = serde_json::from_str(json.as_str())?;
        assert!(info.id == manifest_id.to_string());

        Ok(())
    }
}
