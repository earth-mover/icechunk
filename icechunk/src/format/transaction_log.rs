use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use crate::change_set::ChangeSet;

use super::{ChunkIndices, NodeId, Path};

#[derive(Clone, Debug, PartialEq, Default)]
pub struct TransactionLog {
    // FIXME: better, more stable on-disk format
    pub new_groups: HashSet<NodeId>,
    pub new_arrays: HashSet<NodeId>,
    pub deleted_groups: HashSet<NodeId>,
    pub deleted_arrays: HashSet<NodeId>,
    pub updated_user_attributes: HashSet<NodeId>,
    pub updated_zarr_metadata: HashSet<NodeId>,
    pub updated_chunks: HashMap<NodeId, HashSet<ChunkIndices>>,
}

impl TransactionLog {
    pub fn new(cs: &ChangeSet) -> Self {
        let new_groups = cs.new_groups().map(|(_, node_id)| node_id).cloned().collect();
        let new_arrays = cs.new_arrays().map(|(_, node_id)| node_id).cloned().collect();
        let deleted_groups = cs.deleted_groups().map(|(_, id)| id.clone()).collect();
        let deleted_arrays = cs.deleted_arrays().map(|(_, id)| id.clone()).collect();

        let updated_user_attributes =
            cs.user_attributes_updated_nodes().cloned().collect();
        let updated_zarr_metadata = cs.zarr_updated_arrays().cloned().collect();
        let updated_chunks = cs
            .chunk_changes()
            .map(|(k, v)| (k.clone(), v.keys().cloned().collect()))
            .collect();

        Self {
            new_groups,
            new_arrays,
            deleted_groups,
            deleted_arrays,
            updated_user_attributes,
            updated_zarr_metadata,
            updated_chunks,
        }
    }

    pub fn len(&self) -> usize {
        self.new_groups.len()
            + self.new_arrays.len()
            + self.deleted_groups.len()
            + self.deleted_arrays.len()
            + self.updated_user_attributes.len()
            + self.updated_zarr_metadata.len()
            + self.updated_chunks.values().map(|s| s.len()).sum::<usize>()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn merge(&mut self, other: &TransactionLog) {
        self.new_groups.extend(other.new_groups.iter().cloned());
        self.new_arrays.extend(other.new_arrays.iter().cloned());
        self.deleted_groups.extend(other.deleted_groups.iter().cloned());
        self.deleted_arrays.extend(other.deleted_arrays.iter().cloned());
        self.updated_user_attributes
            .extend(other.updated_user_attributes.iter().cloned());
        self.updated_zarr_metadata.extend(other.updated_zarr_metadata.iter().cloned());
        for (node, chunks) in other.updated_chunks.iter() {
            self.updated_chunks
                .entry(node.clone())
                .and_modify(|set| set.extend(chunks.iter().cloned()))
                .or_insert_with(|| chunks.clone());
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct Diff {
    pub new_groups: BTreeSet<Path>,
    pub new_arrays: BTreeSet<Path>,
    pub deleted_groups: BTreeSet<Path>,
    pub deleted_arrays: BTreeSet<Path>,
    pub updated_user_attributes: BTreeSet<Path>,
    pub updated_zarr_metadata: BTreeSet<Path>,
    pub updated_chunks: BTreeMap<Path, BTreeSet<ChunkIndices>>,
}

impl Diff {
    pub fn from_transaction_log(
        tx: &TransactionLog,
        nodes: HashMap<NodeId, Path>,
    ) -> Self {
        let new_groups = tx
            .new_groups
            .iter()
            .flat_map(|node_id| nodes.get(node_id))
            .cloned()
            .collect();
        let new_arrays = tx
            .new_arrays
            .iter()
            .flat_map(|node_id| nodes.get(node_id))
            .cloned()
            .collect();
        let deleted_groups = tx
            .deleted_groups
            .iter()
            .flat_map(|node_id| nodes.get(node_id))
            .cloned()
            .collect();
        let deleted_arrays = tx
            .deleted_arrays
            .iter()
            .flat_map(|node_id| nodes.get(node_id))
            .cloned()
            .collect();
        let updated_user_attributes = tx
            .updated_user_attributes
            .iter()
            .flat_map(|node_id| nodes.get(node_id))
            .cloned()
            .collect();
        let updated_zarr_metadata = tx
            .updated_zarr_metadata
            .iter()
            .flat_map(|node_id| nodes.get(node_id))
            .cloned()
            .collect();
        let updated_chunks = tx
            .updated_chunks
            .iter()
            .flat_map(|(node_id, chunks)| {
                nodes.get(node_id).map(|n| (n.clone(), chunks.iter().cloned().collect()))
            })
            .collect();
        Self {
            new_groups,
            new_arrays,
            deleted_groups,
            deleted_arrays,
            updated_user_attributes,
            updated_zarr_metadata,
            updated_chunks,
        }
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.new_groups.is_empty()
            && self.new_arrays.is_empty()
            && self.deleted_groups.is_empty()
            && self.deleted_arrays.is_empty()
            && self.updated_user_attributes.is_empty()
            && self.updated_user_attributes.is_empty()
            && self.updated_chunks.is_empty()
    }
}
