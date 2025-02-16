use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    iter,
};

use flatbuffers::VerifierOptions;
use itertools::{Either, Itertools as _};

use crate::{
    change_set::ChangeSet,
    format::flatbuffers::gen::ObjectId12,
    session::{Session, SessionResult},
};

use super::{flatbuffers::gen, ChunkIndices, IcechunkResult, NodeId, Path, SnapshotId};

#[derive(Clone, Debug, PartialEq, Default)]
pub struct TransactionLog {
    buffer: Vec<u8>,
}

impl TransactionLog {
    pub fn new(id: &SnapshotId, cs: &ChangeSet) -> Self {
        let mut new_groups: Vec<_> =
            cs.new_groups().map(|(_, id)| gen::ObjectId8::new(&id.0)).collect();
        let mut new_arrays: Vec<_> =
            cs.new_arrays().map(|(_, id)| gen::ObjectId8::new(&id.0)).collect();
        let mut deleted_groups: Vec<_> =
            cs.deleted_groups().map(|(_, id)| gen::ObjectId8::new(&id.0)).collect();
        let mut deleted_arrays: Vec<_> =
            cs.deleted_arrays().map(|(_, id)| gen::ObjectId8::new(&id.0)).collect();

        let mut updated_user_attributes: Vec<_> = cs
            .user_attributes_updated_nodes()
            .map(|id| gen::ObjectId8::new(&id.0))
            .collect();
        let mut updated_zarr_metadata: Vec<_> =
            cs.zarr_updated_arrays().map(|id| gen::ObjectId8::new(&id.0)).collect();

        // TODO: what's a good capacity?
        let mut builder = flatbuffers::FlatBufferBuilder::with_capacity(1_024 * 1_024);

        // these come sorted from the change set
        let updated_chunks = cs
            .chunk_changes()
            .map(|(node_id, chunks)| {
                let node_id = gen::ObjectId8::new(&node_id.0);
                let node_id = Some(&node_id);
                let chunks = chunks
                    .keys()
                    .map(|indices| {
                        let coords = Some(builder.create_vector(indices.0.as_slice()));
                        gen::ChunkIndices::create(
                            &mut builder,
                            &gen::ChunkIndicesArgs { coords },
                        )
                    })
                    .collect::<Vec<_>>();
                let chunks = Some(builder.create_vector(chunks.as_slice()));
                gen::ArrayUpdatedChunks::create(
                    &mut builder,
                    &gen::ArrayUpdatedChunksArgs { node_id, chunks },
                )
            })
            .collect::<Vec<_>>();
        let updated_chunks = builder.create_vector(updated_chunks.as_slice());
        let updated_chunks = Some(updated_chunks);

        new_groups.sort_by(|a, b| a.0.cmp(&b.0));
        new_arrays.sort_by(|a, b| a.0.cmp(&b.0));
        deleted_groups.sort_by(|a, b| a.0.cmp(&b.0));
        deleted_arrays.sort_by(|a, b| a.0.cmp(&b.0));
        updated_user_attributes.sort_by(|a, b| a.0.cmp(&b.0));
        updated_zarr_metadata.sort_by(|a, b| a.0.cmp(&b.0));

        let new_groups = Some(builder.create_vector(new_groups.as_slice()));
        let new_arrays = Some(builder.create_vector(new_arrays.as_slice()));
        let deleted_groups = Some(builder.create_vector(deleted_groups.as_slice()));
        let deleted_arrays = Some(builder.create_vector(deleted_arrays.as_slice()));
        let updated_user_attributes =
            Some(builder.create_vector(updated_user_attributes.as_slice()));
        let updated_zarr_metadata =
            Some(builder.create_vector(updated_zarr_metadata.as_slice()));

        let id = ObjectId12::new(&id.0);
        let id = Some(&id);
        let tx = gen::TransactionLog::create(
            &mut builder,
            &gen::TransactionLogArgs {
                id,
                new_groups,
                new_arrays,
                deleted_groups,
                deleted_arrays,
                updated_user_attributes,
                updated_zarr_metadata,
                updated_chunks,
            },
        );

        builder.finish(tx, Some("Ichk"));
        let (mut buffer, offset) = builder.collapse();
        buffer.drain(0..offset);
        buffer.shrink_to_fit();
        Self { buffer }
    }

    pub fn from_buffer(buffer: Vec<u8>) -> IcechunkResult<TransactionLog> {
        let _ = flatbuffers::root_with_opts::<gen::TransactionLog>(
            &ROOT_OPTIONS,
            buffer.as_slice(),
        )?;
        Ok(TransactionLog { buffer })
    }

    pub fn new_groups(&self) -> impl Iterator<Item = NodeId> + '_ {
        self.root().new_groups().iter().map(From::from)
    }

    pub fn new_arrays(&self) -> impl Iterator<Item = NodeId> + '_ {
        self.root().new_arrays().iter().map(From::from)
    }

    pub fn deleted_groups(&self) -> impl Iterator<Item = NodeId> + '_ {
        self.root().deleted_groups().iter().map(From::from)
    }

    pub fn deleted_arrays(&self) -> impl Iterator<Item = NodeId> + '_ {
        self.root().deleted_arrays().iter().map(From::from)
    }

    pub fn updated_user_attributes(&self) -> impl Iterator<Item = NodeId> + '_ {
        self.root().updated_user_attributes().iter().map(From::from)
    }

    pub fn updated_zarr_metadata(&self) -> impl Iterator<Item = NodeId> + '_ {
        self.root().updated_zarr_metadata().iter().map(From::from)
    }

    pub fn updated_chunks(
        &self,
    ) -> impl Iterator<Item = (NodeId, impl Iterator<Item = ChunkIndices> + '_)> + '_
    {
        self.root().updated_chunks().iter().map(|arr_chunks| {
            let id: NodeId = arr_chunks.node_id().into();
            let chunks = arr_chunks.chunks().iter().map(|idx| idx.into());
            (id, chunks)
        })
    }

    pub fn updated_chunks_for(
        &self,
        node: &NodeId,
    ) -> impl Iterator<Item = ChunkIndices> + '_ {
        let arr = self
            .root()
            .updated_chunks()
            .lookup_by_key(node.0, |a, b| a.node_id().0.cmp(b));

        match arr {
            Some(arr) => Either::Left(arr.chunks().iter().map(From::from)),
            None => Either::Right(iter::empty()),
        }
    }

    pub fn group_created(&self, id: &NodeId) -> bool {
        self.root().new_groups().lookup_by_key(id.0, |a, b| a.0.cmp(b)).is_some()
    }

    pub fn array_created(&self, id: &NodeId) -> bool {
        self.root().new_arrays().lookup_by_key(id.0, |a, b| a.0.cmp(b)).is_some()
    }

    pub fn group_deleted(&self, id: &NodeId) -> bool {
        self.root().deleted_groups().lookup_by_key(id.0, |a, b| a.0.cmp(b)).is_some()
    }

    pub fn array_deleted(&self, id: &NodeId) -> bool {
        self.root().deleted_arrays().lookup_by_key(id.0, |a, b| a.0.cmp(b)).is_some()
    }

    pub fn user_attributes_updated(&self, id: &NodeId) -> bool {
        self.root()
            .updated_user_attributes()
            .lookup_by_key(id.0, |a, b| a.0.cmp(b))
            .is_some()
    }

    pub fn chunks_updated(&self, id: &NodeId) -> bool {
        self.root()
            .updated_chunks()
            .lookup_by_key(id.0, |a, b| a.node_id().0.cmp(b))
            .is_some()
    }

    pub fn zarr_metadata_updated(&self, id: &NodeId) -> bool {
        self.root()
            .updated_zarr_metadata()
            .lookup_by_key(id.0, |a, b| a.0.cmp(b))
            .is_some()
    }

    fn root(&self) -> gen::TransactionLog {
        // without the unsafe version this is too slow
        // if we try to keep the root in the TransactionLog struct, we would need a lifetime
        unsafe { flatbuffers::root_unchecked::<gen::TransactionLog>(&self.buffer) }
    }

    pub fn bytes(&self) -> &[u8] {
        self.buffer.as_slice()
    }

    pub fn len(&self) -> usize {
        let root = self.root();
        root.new_groups().len()
            + root.new_arrays().len()
            + root.deleted_groups().len()
            + root.deleted_arrays().len()
            + root.updated_user_attributes().len()
            + root.updated_zarr_metadata().len()
            + root.updated_chunks().iter().map(|s| s.chunks().len()).sum::<usize>()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    //pub fn merge(&mut self, other: &TransactionLog) {
    //    self.new_groups.extend(other.new_groups.iter().cloned());
    //    self.new_arrays.extend(other.new_arrays.iter().cloned());
    //    self.deleted_groups.extend(other.deleted_groups.iter().cloned());
    //    self.deleted_arrays.extend(other.deleted_arrays.iter().cloned());
    //    self.updated_user_attributes
    //        .extend(other.updated_user_attributes.iter().cloned());
    //    self.updated_zarr_metadata.extend(other.updated_zarr_metadata.iter().cloned());
    //    for (node, chunks) in other.updated_chunks.iter() {
    //        self.updated_chunks
    //            .entry(node.clone())
    //            .and_modify(|set| set.extend(chunks.iter().cloned()))
    //            .or_insert_with(|| chunks.clone());
    //    }
    //}
}

static ROOT_OPTIONS: VerifierOptions = VerifierOptions {
    max_depth: 64,
    max_tables: 50_000_000,
    max_apparent_size: 1 << 31, // taken from the default
    ignore_missing_null_terminator: true,
};

#[derive(Debug, Default)]
pub struct DiffBuilder {
    new_groups: HashSet<NodeId>,
    new_arrays: HashSet<NodeId>,
    deleted_groups: HashSet<NodeId>,
    deleted_arrays: HashSet<NodeId>,
    updated_user_attributes: HashSet<NodeId>,
    updated_zarr_metadata: HashSet<NodeId>,
    // we use sorted set here to simply move it to a diff without having to rebuild
    updated_chunks: HashMap<NodeId, BTreeSet<ChunkIndices>>,
}

impl DiffBuilder {
    pub fn add_changes(&mut self, tx: &TransactionLog) {
        self.new_groups.extend(tx.new_groups());
        self.new_arrays.extend(tx.new_arrays());
        self.deleted_groups.extend(tx.deleted_groups());
        self.deleted_arrays.extend(tx.deleted_arrays());
        self.updated_user_attributes.extend(tx.updated_user_attributes());
        self.updated_zarr_metadata.extend(tx.updated_zarr_metadata());

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
            .list_nodes()
            .await?
            .chain(to.list_nodes().await?)
            .map_ok(|n| (n.id, n.path))
            .try_collect()?;
        Ok(Diff::from_diff_builder(self, nodes))
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
    fn from_diff_builder(tx: DiffBuilder, nodes: HashMap<NodeId, Path>) -> Self {
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
