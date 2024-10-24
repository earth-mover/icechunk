use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};

use crate::change_set::ChangeSet;

use super::{format_constants, ChunkIndices, IcechunkFormatVersion, NodeId, Path};

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct TransactionLog {
    // FIXME: better, more stable on-disk format
    pub icechunk_transaction_log_format_version: IcechunkFormatVersion,
    pub new_groups: HashMap<Path, NodeId>,
    pub new_arrays: HashMap<Path, NodeId>,
    pub updated_user_attributes: HashSet<NodeId>,
    pub updated_zarr_metadata: HashSet<NodeId>,
    pub updated_chunks: HashMap<NodeId, HashSet<ChunkIndices>>,
    pub deleted_paths: HashSet<Path>,
    pub deleted_groups: HashMap<NodeId, Path>,
    pub deleted_arrays: HashMap<NodeId, Path>,
}

impl From<&ChangeSet> for TransactionLog {
    fn from(value: &ChangeSet) -> Self {
        let new_groups = value.new_groups.clone();
        let new_arrays =
            value.new_arrays.iter().map(|(k, (v, _))| (k.clone(), *v)).collect();
        let updated_user_attributes = value.updated_attributes.keys().copied().collect();
        let updated_zarr_metadata = value.updated_arrays.keys().copied().collect();
        let updated_chunks = value
            .set_chunks
            .iter()
            .map(|(k, v)| (*k, v.keys().cloned().collect()))
            .collect();
        let deleted_paths =
            value.deleted_arrays.union(&value.deleted_groups).cloned().collect();

        Self {
            icechunk_transaction_log_format_version:
                format_constants::LATEST_ICECHUNK_TRANSACTION_LOG_FORMAT,
            new_groups,
            new_arrays,
            updated_user_attributes,
            updated_zarr_metadata,
            updated_chunks,
            deleted_paths,
            // FIXME: implement
            deleted_arrays: HashMap::new(),
            deleted_groups: HashMap::new(),
        }
    }
}
