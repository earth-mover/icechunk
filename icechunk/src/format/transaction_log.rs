use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};

use crate::change_set::ChangeSet;

use super::{
    format_constants,
    snapshot::{NodeSnapshot, NodeType},
    ChunkIndices, IcechunkFormatVersion, NodeId,
};

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct TransactionLog {
    // FIXME: better, more stable on-disk format
    pub icechunk_transaction_log_format_version: IcechunkFormatVersion,
    pub new_groups: HashSet<NodeId>,
    pub new_arrays: HashSet<NodeId>,
    pub deleted_groups: HashSet<NodeId>,
    pub deleted_arrays: HashSet<NodeId>,
    pub updated_user_attributes: HashSet<NodeId>,
    pub updated_zarr_metadata: HashSet<NodeId>,
    pub updated_chunks: HashMap<NodeId, HashSet<ChunkIndices>>,
}

impl TransactionLog {
    pub fn new<'a>(
        cs: &ChangeSet,
        parent_nodes: impl Iterator<Item = &'a NodeSnapshot>,
        child_nodes: impl Iterator<Item = &'a NodeSnapshot>,
    ) -> Self {
        //let all_paths: HashMap<&Path, &NodeId> =
        //   nodes.map(|node| (&node.path, &node.id)).collect();
        let new_groups = cs.new_groups.values().cloned().collect();
        let new_arrays = cs.new_arrays.values().map(|(node, _)| node.clone()).collect();
        let parent_nodes =
            parent_nodes.map(|n| (n.id.clone(), n.node_type())).collect::<HashSet<_>>();
        let child_nodes =
            child_nodes.map(|n| (n.id.clone(), n.node_type())).collect::<HashSet<_>>();
        let mut deleted_groups = HashSet::new();
        let mut deleted_arrays = HashSet::new();

        for (node_id, node_type) in parent_nodes.difference(&child_nodes) {
            // TODO: we shouldn't need the following clones
            match node_type {
                NodeType::Group => {
                    deleted_groups.insert(node_id.clone());
                }
                NodeType::Array => {
                    deleted_arrays.insert(node_id.clone());
                }
            }
        }

        let updated_user_attributes = cs.updated_attributes.keys().cloned().collect();
        let updated_zarr_metadata = cs.updated_arrays.keys().cloned().collect();
        let updated_chunks = cs
            .set_chunks
            .iter()
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
            icechunk_transaction_log_format_version:
                format_constants::LATEST_ICECHUNK_TRANSACTION_LOG_FORMAT,
        }
    }
}
