use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::format::{
    manifest::{ChunkPayload, Manifest},
    snapshot::{
        AttributeFileInfo, ManifestFileInfo, NodeSnapshot, Snapshot, SnapshotMetadata,
        SnapshotProperties,
    },
    transaction_log::TransactionLog,
    ChunkIndices, IcechunkFormatVersion, NodeId, Path,
};

#[derive(Debug, Deserialize)]
pub struct SnapshotDeserializer {
    manifest_files: Vec<ManifestFileInfo>,
    attribute_files: Vec<AttributeFileInfo>,
    total_parents: u32,
    short_term_parents: u16,
    short_term_history: VecDeque<SnapshotMetadata>,
    metadata: SnapshotMetadata,
    started_at: DateTime<Utc>,
    properties: SnapshotProperties,
    nodes: BTreeMap<Path, NodeSnapshot>,
}

#[derive(Debug, Serialize)]
pub struct SnapshotSerializer<'a> {
    manifest_files: &'a Vec<ManifestFileInfo>,
    attribute_files: &'a Vec<AttributeFileInfo>,
    total_parents: u32,
    short_term_parents: u16,
    short_term_history: &'a VecDeque<SnapshotMetadata>,
    metadata: &'a SnapshotMetadata,
    started_at: &'a DateTime<Utc>,
    properties: &'a SnapshotProperties,
    nodes: &'a BTreeMap<Path, NodeSnapshot>,
}

impl From<SnapshotDeserializer> for Snapshot {
    fn from(value: SnapshotDeserializer) -> Self {
        Self {
            manifest_files: value.manifest_files,
            nodes: value.nodes,
            attribute_files: value.attribute_files,
            total_parents: value.total_parents,
            short_term_parents: value.short_term_parents,
            short_term_history: value.short_term_history,
            metadata: value.metadata,
            started_at: value.started_at,
            properties: value.properties,
        }
    }
}

impl<'a> From<&'a Snapshot> for SnapshotSerializer<'a> {
    fn from(value: &'a Snapshot) -> Self {
        Self {
            manifest_files: &value.manifest_files,
            attribute_files: &value.attribute_files,
            nodes: &value.nodes,
            total_parents: value.total_parents,
            short_term_parents: value.short_term_parents,
            short_term_history: &value.short_term_history,
            metadata: &value.metadata,
            started_at: &value.started_at,
            properties: &value.properties,
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct ManifestDeserializer {
    icechunk_manifest_format_version: IcechunkFormatVersion,
    icechunk_manifest_format_flags: BTreeMap<String, rmpv::Value>,
    chunks: BTreeMap<NodeId, BTreeMap<ChunkIndices, ChunkPayload>>,
}

#[derive(Debug, Serialize)]
pub struct ManifestSerializer<'a> {
    icechunk_manifest_format_version: &'a IcechunkFormatVersion,
    icechunk_manifest_format_flags: &'a BTreeMap<String, rmpv::Value>,
    chunks: &'a BTreeMap<NodeId, BTreeMap<ChunkIndices, ChunkPayload>>,
}

impl From<ManifestDeserializer> for Manifest {
    fn from(value: ManifestDeserializer) -> Self {
        Self {
            icechunk_manifest_format_version: value.icechunk_manifest_format_version,
            icechunk_manifest_format_flags: value.icechunk_manifest_format_flags,
            chunks: value.chunks,
        }
    }
}

impl<'a> From<&'a Manifest> for ManifestSerializer<'a> {
    fn from(value: &'a Manifest) -> Self {
        Self {
            icechunk_manifest_format_version: &value.icechunk_manifest_format_version,
            icechunk_manifest_format_flags: &value.icechunk_manifest_format_flags,
            chunks: &value.chunks,
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct TransactionLogDeserializer {
    icechunk_transaction_log_format_version: IcechunkFormatVersion,
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
    icechunk_transaction_log_format_version: &'a IcechunkFormatVersion,
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
            icechunk_transaction_log_format_version: value
                .icechunk_transaction_log_format_version,
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
            icechunk_transaction_log_format_version: &value
                .icechunk_transaction_log_format_version,
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
