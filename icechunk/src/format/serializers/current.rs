use std::collections::{BTreeMap, HashMap, HashSet};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::format::{
    manifest::{ChunkPayload, Manifest},
    snapshot::{
        AttributeFileInfo, ManifestFileInfo, NodeSnapshot, Snapshot, SnapshotProperties,
    },
    transaction_log::TransactionLog,
    ChunkIndices, ManifestId, NodeId, Path, SnapshotId,
};

#[derive(Debug, Deserialize)]
pub struct SnapshotDeserializer {
    id: SnapshotId,
    parent_id: Option<SnapshotId>,
    flushed_at: DateTime<Utc>,
    message: String,
    metadata: SnapshotProperties,
    manifest_files: Vec<ManifestFileInfo>,
    attribute_files: Vec<AttributeFileInfo>,
    nodes: BTreeMap<Path, NodeSnapshot>,
}

#[derive(Debug, Serialize)]
pub struct SnapshotSerializer<'a> {
    id: &'a SnapshotId,
    parent_id: &'a Option<SnapshotId>,
    flushed_at: &'a DateTime<Utc>,
    message: &'a String,
    metadata: &'a SnapshotProperties,
    manifest_files: Vec<ManifestFileInfo>,
    attribute_files: &'a Vec<AttributeFileInfo>,
    nodes: &'a BTreeMap<Path, NodeSnapshot>,
}

impl From<SnapshotDeserializer> for Snapshot {
    fn from(value: SnapshotDeserializer) -> Self {
        Self::from_fields(
            value.id,
            value.parent_id,
            value.flushed_at,
            value.message,
            value.metadata,
            value.manifest_files.into_iter().map(|fi| (fi.id.clone(), fi)).collect(),
            value.attribute_files,
            value.nodes,
        )
    }
}

impl<'a> From<&'a Snapshot> for SnapshotSerializer<'a> {
    fn from(value: &'a Snapshot) -> Self {
        Self {
            id: value.id(),
            parent_id: value.parent_id(),
            flushed_at: value.flushed_at(),
            message: value.message(),
            metadata: value.metadata(),
            manifest_files: value.manifest_files().values().cloned().collect(),
            attribute_files: value.attribute_files(),
            nodes: value.nodes(),
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct ManifestDeserializer {
    id: ManifestId,
    chunks: BTreeMap<NodeId, BTreeMap<ChunkIndices, ChunkPayload>>,
}

#[derive(Debug, Serialize)]
pub struct ManifestSerializer<'a> {
    id: &'a ManifestId,
    chunks: &'a BTreeMap<NodeId, BTreeMap<ChunkIndices, ChunkPayload>>,
}

impl From<ManifestDeserializer> for Manifest {
    fn from(value: ManifestDeserializer) -> Self {
        Self { id: value.id, chunks: value.chunks }
    }
}

impl<'a> From<&'a Manifest> for ManifestSerializer<'a> {
    fn from(value: &'a Manifest) -> Self {
        Self { id: &value.id, chunks: &value.chunks }
    }
}

#[derive(Debug, Deserialize)]
pub struct TransactionLogDeserializer {
    new_groups: HashSet<NodeId>,
    new_arrays: HashSet<NodeId>,
    deleted_groups: HashSet<NodeId>,
    deleted_arrays: HashSet<NodeId>,
    updated_user_attributes: HashSet<NodeId>,
    updated_zarr_metadata: HashSet<NodeId>,
    updated_chunks: HashMap<NodeId, HashSet<ChunkIndices>>,
}

#[derive(Debug, Serialize)]
pub struct TransactionLogSerializer<'a> {
    new_groups: &'a HashSet<NodeId>,
    new_arrays: &'a HashSet<NodeId>,
    deleted_groups: &'a HashSet<NodeId>,
    deleted_arrays: &'a HashSet<NodeId>,
    updated_user_attributes: &'a HashSet<NodeId>,
    updated_zarr_metadata: &'a HashSet<NodeId>,
    updated_chunks: &'a HashMap<NodeId, HashSet<ChunkIndices>>,
}

impl From<TransactionLogDeserializer> for TransactionLog {
    fn from(value: TransactionLogDeserializer) -> Self {
        Self {
            new_groups: value.new_groups,
            new_arrays: value.new_arrays,
            deleted_groups: value.deleted_groups,
            deleted_arrays: value.deleted_arrays,
            updated_user_attributes: value.updated_user_attributes,
            updated_zarr_metadata: value.updated_zarr_metadata,
            updated_chunks: value.updated_chunks,
        }
    }
}

impl<'a> From<&'a TransactionLog> for TransactionLogSerializer<'a> {
    fn from(value: &'a TransactionLog) -> Self {
        Self {
            new_groups: &value.new_groups,
            new_arrays: &value.new_arrays,
            deleted_groups: &value.deleted_groups,
            deleted_arrays: &value.deleted_arrays,
            updated_user_attributes: &value.updated_user_attributes,
            updated_zarr_metadata: &value.updated_zarr_metadata,
            updated_chunks: &value.updated_chunks,
        }
    }
}
