use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    iter,
};

use flatbuffers::{VerifierOptions, WIPOffset};
use itertools::{Either, Itertools as _};

use crate::{
    change_set::{ChangeSet, Move},
    session::{Session, SessionResult},
};

use super::{
    ChunkIndices, IcechunkResult, NodeId, Path, SnapshotId,
    flatbuffers::generated::{self, MoveOperation, MoveOperationArgs},
};

#[derive(Clone, Debug, PartialEq, Default)]
pub struct TransactionLog {
    buffer: Vec<u8>,
}

impl TransactionLog {
    pub fn new(id: &SnapshotId, cs: &ChangeSet) -> Self {
        // TODO: what's a good capacity?
        let mut builder = flatbuffers::FlatBufferBuilder::with_capacity(1_024 * 1_024);

        let mut new_groups: Vec<_> =
            cs.new_groups().map(|(_, id)| generated::ObjectId8::new(&id.0)).collect();
        let mut new_arrays: Vec<_> =
            cs.new_arrays().map(|(_, id)| generated::ObjectId8::new(&id.0)).collect();
        let mut deleted_groups: Vec<_> =
            cs.deleted_groups().map(|(_, id)| generated::ObjectId8::new(&id.0)).collect();
        let mut deleted_arrays: Vec<_> =
            cs.deleted_arrays().map(|(_, id)| generated::ObjectId8::new(&id.0)).collect();

        let mut updated_arrays: Vec<_> =
            cs.updated_arrays().map(|id| generated::ObjectId8::new(&id.0)).collect();
        let mut updated_groups: Vec<_> =
            cs.updated_groups().map(|id| generated::ObjectId8::new(&id.0)).collect();
        let moved_nodes: Vec<_> = cs
            .moves()
            .map(|Move { from, to }| {
                let from = builder.create_string(from.to_string().as_str());
                let to = builder.create_string(to.to_string().as_str());
                let args = MoveOperationArgs { from: Some(from), to: Some(to) };
                MoveOperation::create(&mut builder, &args)
            })
            .collect();

        // these come sorted from the change set
        let updated_chunks = cs
            .changed_chunks()
            .map(|(node_id, chunks)| {
                let node_id = generated::ObjectId8::new(&node_id.0);
                let node_id = Some(&node_id);
                let chunks = chunks
                    .map(|indices| {
                        let coords = Some(builder.create_vector(indices.0.as_slice()));
                        generated::ChunkIndices::create(
                            &mut builder,
                            &generated::ChunkIndicesArgs { coords },
                        )
                    })
                    .collect::<Vec<_>>();
                let chunks = Some(builder.create_vector(chunks.as_slice()));
                generated::ArrayUpdatedChunks::create(
                    &mut builder,
                    &generated::ArrayUpdatedChunksArgs { node_id, chunks },
                )
            })
            .collect::<Vec<_>>();
        let updated_chunks = builder.create_vector(updated_chunks.as_slice());
        let updated_chunks = Some(updated_chunks);

        new_groups.sort_by(|a, b| a.0.cmp(&b.0));
        new_arrays.sort_by(|a, b| a.0.cmp(&b.0));
        deleted_groups.sort_by(|a, b| a.0.cmp(&b.0));
        deleted_arrays.sort_by(|a, b| a.0.cmp(&b.0));
        updated_groups.sort_by(|a, b| a.0.cmp(&b.0));
        updated_arrays.sort_by(|a, b| a.0.cmp(&b.0));

        let new_groups = Some(builder.create_vector(new_groups.as_slice()));
        let new_arrays = Some(builder.create_vector(new_arrays.as_slice()));
        let deleted_groups = Some(builder.create_vector(deleted_groups.as_slice()));
        let deleted_arrays = Some(builder.create_vector(deleted_arrays.as_slice()));
        let updated_groups = Some(builder.create_vector(updated_groups.as_slice()));
        let updated_arrays = Some(builder.create_vector(updated_arrays.as_slice()));

        let id = generated::ObjectId12::new(&id.0);
        let id = Some(&id);
        let moved_nodes = Some(builder.create_vector(moved_nodes.as_slice()));
        let tx = generated::TransactionLog::create(
            &mut builder,
            &generated::TransactionLogArgs {
                id,
                new_groups,
                new_arrays,
                deleted_groups,
                deleted_arrays,
                updated_groups,
                updated_arrays,
                updated_chunks,
                moved_nodes,
            },
        );

        builder.finish(tx, Some("Ichk"));
        let (mut buffer, offset) = builder.collapse();
        buffer.drain(0..offset);
        buffer.shrink_to_fit();
        Self { buffer }
    }

    /// Low level method that creates a tx log from its parts
    /// Intended to be used only by library creators
    #[allow(clippy::too_many_arguments)]
    pub fn new_from_parts(
        id: &SnapshotId,
        sorted_new_groups: impl ExactSizeIterator<Item = NodeId> + DoubleEndedIterator,
        sorted_new_arrays: impl ExactSizeIterator<Item = NodeId> + DoubleEndedIterator,
        sorted_deleted_groups: impl ExactSizeIterator<Item = NodeId> + DoubleEndedIterator,
        sorted_deleted_arrays: impl ExactSizeIterator<Item = NodeId> + DoubleEndedIterator,
        sorted_updated_groups: impl ExactSizeIterator<Item = NodeId> + DoubleEndedIterator,
        sorted_updated_arrays: impl ExactSizeIterator<Item = NodeId> + DoubleEndedIterator,
        sorted_updated_chunks: impl ExactSizeIterator<
            Item = (NodeId, impl Iterator<Item = ChunkIndices>),
        > + DoubleEndedIterator,
        sorted_moves: impl Iterator<Item = Move>,
    ) -> Self {
        // TODO: what's a good capacity?
        let mut builder = flatbuffers::FlatBufferBuilder::with_capacity(1_024 * 1_024);

        //let new_groups = Some(builder.create_vector(sorted_new_groups.as_slice()));
        let new_groups = Some(builder.create_vector_from_iter(
            sorted_new_groups.map(|id| generated::ObjectId8::new(&id.0)),
        ));
        let new_arrays = Some(builder.create_vector_from_iter(
            sorted_new_arrays.map(|id| generated::ObjectId8::new(&id.0)),
        ));
        let deleted_groups = Some(builder.create_vector_from_iter(
            sorted_deleted_groups.map(|id| generated::ObjectId8::new(&id.0)),
        ));
        let deleted_arrays = Some(builder.create_vector_from_iter(
            sorted_deleted_arrays.map(|id| generated::ObjectId8::new(&id.0)),
        ));
        let updated_groups = Some(builder.create_vector_from_iter(
            sorted_updated_groups.map(|id| generated::ObjectId8::new(&id.0)),
        ));
        let updated_arrays = Some(builder.create_vector_from_iter(
            sorted_updated_arrays.map(|id| generated::ObjectId8::new(&id.0)),
        ));

        let id = generated::ObjectId12::new(&id.0);
        let id = Some(&id);
        // TODO: very inefficient
        let updated_chunks = sorted_updated_chunks
            .map(|(node_id, chunks)| {
                let node_id = generated::ObjectId8::new(&node_id.0);
                let node_id = Some(&node_id);
                let chunks = chunks
                    .map(|indices| {
                        let coords = Some(builder.create_vector(indices.0.as_slice()));
                        generated::ChunkIndices::create(
                            &mut builder,
                            &generated::ChunkIndicesArgs { coords },
                        )
                    })
                    .collect::<Vec<_>>();
                let chunks = Some(builder.create_vector(chunks.as_slice()));
                generated::ArrayUpdatedChunks::create(
                    &mut builder,
                    &generated::ArrayUpdatedChunksArgs { node_id, chunks },
                )
            })
            .collect::<Vec<_>>();
        let updated_chunks = builder.create_vector(updated_chunks.as_slice());
        let updated_chunks = Some(updated_chunks);

        let moved_nodes: Vec<_> = sorted_moves
            .map(|Move { from, to }| {
                let from = builder.create_string(from.to_string().as_str());
                let to = builder.create_string(to.to_string().as_str());
                let args = MoveOperationArgs { from: Some(from), to: Some(to) };
                MoveOperation::create(&mut builder, &args)
            })
            .collect();

        let moved_nodes = Some(builder.create_vector(moved_nodes.as_slice()));

        let tx = generated::TransactionLog::create(
            &mut builder,
            &generated::TransactionLogArgs {
                id,
                new_groups,
                new_arrays,
                deleted_groups,
                deleted_arrays,
                updated_groups,
                updated_arrays,
                updated_chunks,
                moved_nodes,
            },
        );

        builder.finish(tx, Some("Ichk"));
        let (mut buffer, offset) = builder.collapse();
        buffer.drain(0..offset);
        buffer.shrink_to_fit();
        Self { buffer }
    }

    pub fn from_buffer(buffer: Vec<u8>) -> IcechunkResult<Self> {
        let _ = flatbuffers::root_with_opts::<generated::TransactionLog>(
            &ROOT_OPTIONS,
            buffer.as_slice(),
        )?;
        Ok(Self { buffer })
    }

    pub fn new_groups(
        &self,
    ) -> impl ExactSizeIterator<Item = NodeId> + DoubleEndedIterator + '_ {
        self.root().new_groups().iter().map(From::from)
    }

    pub fn new_arrays(
        &self,
    ) -> impl ExactSizeIterator<Item = NodeId> + DoubleEndedIterator + '_ {
        self.root().new_arrays().iter().map(From::from)
    }

    pub fn deleted_groups(
        &self,
    ) -> impl ExactSizeIterator<Item = NodeId> + DoubleEndedIterator + '_ {
        self.root().deleted_groups().iter().map(From::from)
    }

    pub fn deleted_arrays(
        &self,
    ) -> impl ExactSizeIterator<Item = NodeId> + DoubleEndedIterator + '_ {
        self.root().deleted_arrays().iter().map(From::from)
    }

    pub fn updated_groups(
        &self,
    ) -> impl ExactSizeIterator<Item = NodeId> + DoubleEndedIterator + '_ {
        self.root().updated_groups().iter().map(From::from)
    }

    pub fn updated_arrays(
        &self,
    ) -> impl ExactSizeIterator<Item = NodeId> + DoubleEndedIterator + '_ {
        self.root().updated_arrays().iter().map(From::from)
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

    pub fn moves(&self) -> impl Iterator<Item = Move> + '_ {
        let it = match self.root().moved_nodes() {
            Some(it) => Either::Left(it.iter()),
            None => Either::Right(iter::empty()),
        };
        it.filter_map(|m| {
            let from = Path::new(m.from()).ok()?;
            let to = Path::new(m.to()).ok()?;
            Some(Move { from, to })
        })
    }

    pub fn updated_chunks_for(
        &self,
        node: &NodeId,
    ) -> impl Iterator<Item = ChunkIndices> + '_ + use<'_> {
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

    pub fn group_updated(&self, id: &NodeId) -> bool {
        self.root().updated_groups().lookup_by_key(id.0, |a, b| a.0.cmp(b)).is_some()
    }

    pub fn array_updated(&self, id: &NodeId) -> bool {
        self.root().updated_arrays().lookup_by_key(id.0, |a, b| a.0.cmp(b)).is_some()
    }

    pub fn chunks_updated(&self, id: &NodeId) -> bool {
        self.root()
            .updated_chunks()
            .lookup_by_key(id.0, |a, b| a.node_id().0.cmp(b))
            .is_some()
    }

    fn root(&self) -> generated::TransactionLog<'_> {
        // without the unsafe version this is too slow
        // if we try to keep the root in the TransactionLog struct, we would need a lifetime
        unsafe { flatbuffers::root_unchecked::<generated::TransactionLog>(&self.buffer) }
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
            + root.updated_groups().len()
            + root.updated_arrays().len()
            + root.updated_chunks().iter().map(|s| s.chunks().len()).sum::<usize>()
            + root.moved_nodes().map(|v| v.len()).unwrap_or_default()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn merge<'a, T: IntoIterator<Item = &'a TransactionLog>>(
        id: &SnapshotId,
        iter: T,
    ) -> Self {
        let txs = Vec::from_iter(iter);

        // TODO: what's a good capacity?
        let mut builder = flatbuffers::FlatBufferBuilder::with_capacity(1_024 * 1_024);

        let new_groups = {
            let new_groups =
                BTreeSet::from_iter(txs.iter().flat_map(|tx| tx.new_groups()));
            let new_groups = Vec::from_iter(
                new_groups.into_iter().map(|id| generated::ObjectId8::new(&id.0)),
            );
            Some(builder.create_vector(new_groups.as_slice()))
        };
        let new_arrays = {
            let new_arrays =
                BTreeSet::from_iter(txs.iter().flat_map(|tx| tx.new_arrays()));
            let new_arrays = Vec::from_iter(
                new_arrays.into_iter().map(|id| generated::ObjectId8::new(&id.0)),
            );
            Some(builder.create_vector(new_arrays.as_slice()))
        };
        let deleted_groups = {
            let deleted_groups =
                BTreeSet::from_iter(txs.iter().flat_map(|tx| tx.deleted_groups()));
            let deleted_groups = Vec::from_iter(
                deleted_groups.into_iter().map(|id| generated::ObjectId8::new(&id.0)),
            );
            Some(builder.create_vector(deleted_groups.as_slice()))
        };
        let deleted_arrays = {
            let deleted_arrays =
                BTreeSet::from_iter(txs.iter().flat_map(|tx| tx.deleted_arrays()));
            let deleted_arrays = Vec::from_iter(
                deleted_arrays.into_iter().map(|id| generated::ObjectId8::new(&id.0)),
            );
            Some(builder.create_vector(deleted_arrays.as_slice()))
        };
        let updated_groups = {
            let updated_groups =
                BTreeSet::from_iter(txs.iter().flat_map(|tx| tx.updated_groups()));
            let updated_groups = Vec::from_iter(
                updated_groups.into_iter().map(|id| generated::ObjectId8::new(&id.0)),
            );
            Some(builder.create_vector(updated_groups.as_slice()))
        };
        let updated_arrays = {
            let updated_arrays =
                BTreeSet::from_iter(txs.iter().flat_map(|tx| tx.updated_arrays()));
            let updated_arrays = Vec::from_iter(
                updated_arrays.into_iter().map(|id| generated::ObjectId8::new(&id.0)),
            );
            Some(builder.create_vector(updated_arrays.as_slice()))
        };
        let updated_chunks = {
            let updated_chunks = txs.iter().fold(BTreeMap::new(), |res, tx| {
                tx.updated_chunks().fold(res, |mut res, (node_id, chunks_it)| {
                    let set: &mut BTreeSet<_> = res.entry(node_id).or_default();
                    set.extend(chunks_it);
                    res
                })
            });

            let updated_chunks = updated_chunks
                .into_iter()
                .map(|(node_id, chunks)| {
                    let node_id = generated::ObjectId8::new(&node_id.0);
                    let node_id = Some(&node_id);
                    let chunks = chunks
                        .into_iter()
                        .map(|indices| {
                            let coords =
                                Some(builder.create_vector(indices.0.as_slice()));
                            generated::ChunkIndices::create(
                                &mut builder,
                                &generated::ChunkIndicesArgs { coords },
                            )
                        })
                        .collect::<Vec<_>>();
                    let chunks = Some(builder.create_vector(chunks.as_slice()));
                    generated::ArrayUpdatedChunks::create(
                        &mut builder,
                        &generated::ArrayUpdatedChunksArgs { node_id, chunks },
                    )
                })
                .collect::<Vec<_>>();

            let updated_chunks = builder.create_vector(updated_chunks.as_slice());
            Some(updated_chunks)
        };

        let id = generated::ObjectId12::new(&id.0);
        let id = Some(&id);
        // FIXME: verify no moves in origins
        let moved_nodes: &[WIPOffset<_>] = &[];
        let moved_nodes = Some(builder.create_vector(moved_nodes)); // FIXME:
        let tx = generated::TransactionLog::create(
            &mut builder,
            &generated::TransactionLogArgs {
                id,
                new_groups,
                new_arrays,
                deleted_groups,
                deleted_arrays,
                updated_groups,
                updated_arrays,
                updated_chunks,
                moved_nodes,
            },
        );

        builder.finish(tx, Some("Ichk"));
        let (mut buffer, offset) = builder.collapse();
        buffer.drain(0..offset);
        buffer.shrink_to_fit();
        Self { buffer }
    }
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
        Ok(Diff::from_diff_builder(self, nodes))
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
    fn from_diff_builder(builder: DiffBuilder, nodes: HashMap<NodeId, Path>) -> Self {
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

#[cfg(test)]
#[allow(clippy::panic, clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::collections::HashSet;

    use bytes::Bytes;
    use itertools::Itertools;

    use crate::{
        change_set::{ArrayData, ChangeSet},
        format::{
            ChunkIndices, NodeId, SnapshotId,
            manifest::{ChunkPayload, ManifestExtents, ManifestSplits},
            snapshot::ArrayShape,
            transaction_log::TransactionLog,
        },
    };

    #[icechunk_macros::test]
    fn test_merge() -> Result<(), Box<dyn std::error::Error>> {
        let mut cs1 = ChangeSet::for_edits();
        let added_group = NodeId::random();
        let added_array = NodeId::random();
        let deleted_group = NodeId::random();
        let deleted_array = NodeId::random();
        let updated_group = NodeId::random();
        let chunk_added = NodeId::random();
        cs1.add_group("/g1".try_into().unwrap(), added_group.clone(), Bytes::new())?;
        cs1.delete_group("/g2".try_into().unwrap(), &deleted_group)?;
        cs1.add_array(
            "/a1".try_into().unwrap(),
            added_array.clone(),
            ArrayData {
                shape: ArrayShape::new([(0, 10)]).unwrap(),
                dimension_names: None,
                user_data: Bytes::new(),
            },
        )?;
        cs1.delete_array("/a2".try_into().unwrap(), &deleted_array)?;
        cs1.update_group(&updated_group, &"/g3".try_into().unwrap(), Bytes::new())?;
        cs1.set_chunk_ref(
            chunk_added.clone(),
            ChunkIndices(vec![0]),
            Some(ChunkPayload::Inline(Bytes::new())),
            &ManifestSplits::from_extents(vec![ManifestExtents::new(&[0], &[100])]),
        )?;

        let t1 = TransactionLog::new(&SnapshotId::random(), &cs1);
        let t2 = TransactionLog::new(&SnapshotId::random(), &cs1);

        let tx = TransactionLog::merge(&SnapshotId::random(), [&t1, &t2]);
        assert!(tx.new_groups().eq([added_group.clone()]));
        assert!(tx.new_arrays().eq([added_array.clone()]));
        assert!(tx.deleted_groups().eq([deleted_group.clone()]));
        assert!(tx.deleted_arrays().eq([deleted_array.clone()]));
        assert!(tx.updated_groups().eq([updated_group.clone()]));
        let chunks =
            Vec::from_iter(tx.updated_chunks().map(|(id, it)| (id, Vec::from_iter(it))));
        assert_eq!(chunks, vec![(chunk_added.clone(), vec![ChunkIndices(vec![0])])]);

        let added_group2 = NodeId::random();
        let deleted_group2 = NodeId::random();
        let deleted_array2 = NodeId::random();
        let updated_group2 = NodeId::random();
        let chunk_added2 = NodeId::random();
        let mut cs2 = ChangeSet::for_edits();
        cs2.add_group("/g1".try_into().unwrap(), added_group2.clone(), Bytes::new())?;
        cs2.delete_group("/g2".try_into().unwrap(), &deleted_group2)?;
        cs2.add_array(
            "/a1".try_into().unwrap(),
            added_array.clone(),
            ArrayData {
                shape: ArrayShape::new([(0, 10)]).unwrap(),
                dimension_names: None,
                user_data: Bytes::new(),
            },
        )?;
        cs2.delete_array("/a2".try_into().unwrap(), &deleted_array2)?;
        cs2.update_group(&updated_group2, &"/g3".try_into().unwrap(), Bytes::new())?;
        cs2.set_chunk_ref(
            chunk_added.clone(),
            ChunkIndices(vec![0]),
            None,
            &ManifestSplits::from_extents(vec![ManifestExtents::new(&[0], &[100])]),
        )?;
        cs2.set_chunk_ref(
            chunk_added.clone(),
            ChunkIndices(vec![1]),
            Some(ChunkPayload::Inline(Bytes::new())),
            &ManifestSplits::from_extents(vec![ManifestExtents::new(&[0], &[100])]),
        )?;
        cs2.set_chunk_ref(
            chunk_added.clone(),
            ChunkIndices(vec![42]),
            None,
            &ManifestSplits::from_extents(vec![ManifestExtents::new(&[0], &[100])]),
        )?;
        cs2.set_chunk_ref(
            chunk_added2.clone(),
            ChunkIndices(vec![7]),
            Some(ChunkPayload::Inline(Bytes::new())),
            &ManifestSplits::from_extents(vec![ManifestExtents::new(&[0], &[100])]),
        )?;

        let t3 = TransactionLog::new(&SnapshotId::random(), &cs2);
        let tx_id = SnapshotId::random();
        let tx = TransactionLog::merge(&tx_id, [&t1, &t2, &t3]);

        assert!(
            tx.new_groups()
                .sorted()
                .eq([added_group.clone(), added_group2.clone()].into_iter().sorted())
        );
        assert!(tx.new_arrays().eq([added_array.clone()]));
        assert!(
            tx.deleted_groups().sorted().eq([
                deleted_group.clone(),
                deleted_group2.clone()
            ]
            .into_iter()
            .sorted())
        );
        assert!(
            tx.deleted_arrays().sorted().eq([
                deleted_array.clone(),
                deleted_array2.clone()
            ]
            .into_iter()
            .sorted())
        );
        assert!(
            tx.updated_groups().sorted().eq([
                updated_group.clone(),
                updated_group2.clone()
            ]
            .into_iter()
            .sorted())
        );
        let chunks = HashSet::from_iter(
            tx.updated_chunks().map(|(id, it)| (id, Vec::from_iter(it))),
        );
        assert_eq!(
            chunks,
            HashSet::from([
                (
                    chunk_added.clone(),
                    vec![
                        ChunkIndices(vec![0]),
                        ChunkIndices(vec![1]),
                        ChunkIndices(vec![42])
                    ]
                ),
                (chunk_added2.clone(), vec![ChunkIndices(vec![7]),])
            ])
        );
        Ok(())
    }
}
