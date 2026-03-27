//! Change records for commits, enabling conflict detection during rebase.

use std::{
    collections::{BTreeMap, BTreeSet},
    iter,
};

use flatbuffers::{VerifierOptions, WIPOffset};
use itertools::Either;

use crate::{
    ChunkIndices, IcechunkResult, Move, NodeId, Path, SnapshotId,
    flatbuffers::generated::{self, MoveOperation, MoveOperationArgs},
};
use icechunk_types::ICResultExt as _;

#[derive(Clone, Debug, PartialEq, Default)]
pub struct TransactionLog {
    buffer: Vec<u8>,
}

impl TransactionLog {
    /// Low level method that creates a tx log from its parts
    /// Intended to be used only by library creators
    #[expect(clippy::too_many_arguments)]
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
            .map(|Move { from, to, node_id, node_type }| {
                let from = builder.create_string(from.to_string().as_str());
                let to = builder.create_string(to.to_string().as_str());
                let node_id = generated::ObjectId8::new(&node_id.0);
                let node_id = Some(&node_id);
                let node_type: generated::NodeType = node_type.into();
                let args = MoveOperationArgs {
                    from: Some(from),
                    to: Some(to),
                    node_id,
                    node_type,
                };
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
                ..Default::default()
            },
        );

        builder.finish(tx, Some("Ichk"));
        let (mut buffer, offset) = builder.collapse();
        buffer.drain(0..offset);
        buffer.shrink_to_fit();
        Self { buffer }
    }

    pub fn from_buffer(buffer: Vec<u8>) -> IcechunkResult<Self> {
        let _ = flatbuffers::root_with_opts::<generated::TransactionLog<'_>>(
            &ROOT_OPTIONS,
            buffer.as_slice(),
        )
        .capture()?;
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
            let from = Path::new(
                m.from().expect("from is optional in flatbuffers, but required by spec"),
            )
            .ok()?;
            let to = Path::new(
                m.to().expect("from is optional in flatbuffers, but required by spec"),
            )
            .ok()?;
            let node_id: NodeId = m
                .node_id()
                .expect("from is optional in flatbuffers, but required by spec")
                .into();
            let node_type = m.node_type().into();
            Some(Move { from, to, node_id, node_type })
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

    pub fn updated_chunks_counts(
        &self,
    ) -> impl Iterator<Item = (NodeId, u64)> + '_ + use<'_> {
        self.root().updated_chunks().iter().map(|arr_chunks| {
            let id: NodeId = arr_chunks.node_id().into();
            let n = arr_chunks.chunks().len();
            (id, n as u64)
        })
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

    #[expect(unsafe_code)]
    fn root(&self) -> generated::TransactionLog<'_> {
        // SAFETY: self.buffer was serialized by our own flatbuffers serialization code.
        // We skip validation for performance; a corrupt buffer here indicates
        // file corruption or a bad Icechunk implementation, not a caller error.
        unsafe {
            flatbuffers::root_unchecked::<generated::TransactionLog<'_>>(&self.buffer)
        }
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

    pub fn has_moves(&self) -> bool {
        self.root().moved_nodes().map(|v| !v.is_empty()).unwrap_or(false)
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
                ..Default::default()
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

// Tests for TransactionLog depend on ChangeSet which lives in the icechunk crate.
// They are kept in icechunk's test suite instead.
#[cfg(any())]
mod tests {
    use std::collections::HashSet;

    use bytes::Bytes;
    use itertools::Itertools as _;

    use crate::{
        change_set::{ArrayData, ChangeSet, transaction_log_from_change_set},
        format::{
            ChunkIndices, NodeId, SnapshotId, manifest::ChunkPayload,
            snapshot::ArrayShape, transaction_log::TransactionLog,
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
        )?;

        let t1 = transaction_log_from_change_set(&SnapshotId::random(), &cs1);
        let t2 = transaction_log_from_change_set(&SnapshotId::random(), &cs1);

        let tx = TransactionLog::merge(&SnapshotId::random(), [&t1, &t2]);
        assert!(tx.new_groups().eq([added_group.clone()]));
        assert!(tx.new_arrays().eq([added_array.clone()]));
        assert!(tx.deleted_groups().eq([deleted_group.clone()]));
        assert!(tx.deleted_arrays().eq([deleted_array.clone()]));
        assert!(tx.updated_groups().eq([updated_group.clone()]));
        let chunks =
            Vec::from_iter(tx.updated_chunks().map(|(id, it)| (id, Vec::from_iter(it))));
        assert_eq!(chunks, vec![(chunk_added.clone(), vec![ChunkIndices(vec![0])])]);
        assert_eq!(
            tx.updated_chunks_counts().collect::<Vec<_>>(),
            vec![(chunk_added.clone(), 1)]
        );

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
        cs2.set_chunk_ref(chunk_added.clone(), ChunkIndices(vec![0]), None)?;
        cs2.set_chunk_ref(
            chunk_added.clone(),
            ChunkIndices(vec![1]),
            Some(ChunkPayload::Inline(Bytes::new())),
        )?;
        cs2.set_chunk_ref(chunk_added.clone(), ChunkIndices(vec![42]), None)?;
        cs2.set_chunk_ref(
            chunk_added2.clone(),
            ChunkIndices(vec![7]),
            Some(ChunkPayload::Inline(Bytes::new())),
        )?;

        let t3 = transaction_log_from_change_set(&SnapshotId::random(), &cs2);
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

        assert_eq!(
            tx.updated_chunks_counts().collect::<HashSet<_>>(),
            HashSet::from([(chunk_added.clone(), 3), (chunk_added2, 1)])
        );
        Ok(())
    }
}
