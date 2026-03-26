use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use icechunk_format::Move;
use itertools::Itertools as _;

use crate::{
    format::{ChunkIndices, NodeId, Path, transaction_log::TransactionLog},
    session::{Session, SessionResult},
};

#[derive(Debug, Default)]
pub struct DiffBuilder {
    new_groups: HashSet<NodeId>,
    new_arrays: HashSet<NodeId>,
    deleted_groups: HashSet<NodeId>,
    deleted_arrays: HashSet<NodeId>,
    updated_groups: HashSet<NodeId>,
    updated_arrays: HashSet<NodeId>,
    // we use sorted set here to simply move it to a diff without having to rebuild
    updated_chunks: HashMap<NodeId, BTreeSet<ChunkIndices>>,
    moved_nodes: Vec<Move>,
}

impl DiffBuilder {
    pub fn add_changes(&mut self, tx: &TransactionLog) {
        self.new_groups.extend(tx.new_groups());
        self.new_arrays.extend(tx.new_arrays());
        self.deleted_groups.extend(tx.deleted_groups());
        self.deleted_arrays.extend(tx.deleted_arrays());
        self.updated_groups.extend(tx.updated_groups());
        self.updated_arrays.extend(tx.updated_arrays());
        self.moved_nodes.extend(tx.moves());

        for (node, chunks) in tx.updated_chunks() {
            match self.updated_chunks.get_mut(&node) {
                Some(all_chunks) => {
                    all_chunks.extend(chunks);
                }
                None => {
                    self.updated_chunks.insert(node, BTreeSet::from_iter(chunks));
                }
            }
        }
    }

    pub async fn to_diff(self, from: &Session, to: &Session) -> SessionResult<Diff> {
        let nodes: HashMap<NodeId, Path> = from
            .list_nodes(&Path::root())
            .await?
            .chain(to.list_nodes(&Path::root()).await?)
            .map_ok(|n| (n.id, n.path))
            .try_collect()?;
        Ok(Diff::from_diff_builder(self, &nodes))
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct Diff {
    pub new_groups: BTreeSet<Path>,
    pub new_arrays: BTreeSet<Path>,
    pub deleted_groups: BTreeSet<Path>,
    pub deleted_arrays: BTreeSet<Path>,
    pub updated_groups: BTreeSet<Path>,
    pub updated_arrays: BTreeSet<Path>,
    pub updated_chunks: BTreeMap<Path, BTreeSet<ChunkIndices>>,
    pub moved_nodes: Vec<Move>,
}

impl Diff {
    fn from_diff_builder(builder: DiffBuilder, nodes: &HashMap<NodeId, Path>) -> Self {
        let new_groups = builder
            .new_groups
            .iter()
            .flat_map(|node_id| nodes.get(node_id))
            .cloned()
            .collect();
        let new_arrays = builder
            .new_arrays
            .iter()
            .flat_map(|node_id| nodes.get(node_id))
            .cloned()
            .collect();
        let deleted_groups = builder
            .deleted_groups
            .iter()
            .flat_map(|node_id| nodes.get(node_id))
            .cloned()
            .collect();
        let deleted_arrays = builder
            .deleted_arrays
            .iter()
            .flat_map(|node_id| nodes.get(node_id))
            .cloned()
            .collect();
        let updated_groups = builder
            .updated_groups
            .iter()
            .flat_map(|node_id| nodes.get(node_id))
            .cloned()
            .collect();
        let updated_arrays = builder
            .updated_arrays
            .iter()
            .flat_map(|node_id| nodes.get(node_id))
            .cloned()
            .collect();
        let updated_chunks = builder
            .updated_chunks
            .into_iter()
            .flat_map(|(node_id, chunks)| {
                let path = nodes.get(&node_id).cloned()?;
                Some((path, chunks))
            })
            .collect();
        Self {
            new_groups,
            new_arrays,
            deleted_groups,
            deleted_arrays,
            updated_groups,
            updated_arrays,
            updated_chunks,
            moved_nodes: builder.moved_nodes,
        }
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.new_groups.is_empty()
            && self.new_arrays.is_empty()
            && self.deleted_groups.is_empty()
            && self.deleted_arrays.is_empty()
            && self.updated_groups.is_empty()
            && self.updated_arrays.is_empty()
            && self.updated_chunks.is_empty()
            && self.moved_nodes.is_empty()
    }
}
