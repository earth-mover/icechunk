use std::{
    collections::{BTreeMap, HashMap, HashSet},
    iter,
    mem::take,
};

use bytes::Bytes;
use itertools::{Either, Itertools as _};
use serde::{Deserialize, Serialize};

use crate::{
    format::{
        manifest::{ChunkInfo, ChunkPayload},
        snapshot::{ArrayShape, DimensionName, NodeData, NodeSnapshot},
        ChunkIndices, NodeId, Path,
    },
    session::SessionResult,
};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArrayData {
    pub shape: ArrayShape,
    pub dimension_names: Option<Vec<DimensionName>>,
    pub user_data: Bytes,
}

#[derive(Clone, Debug, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct ChangeSet {
    new_groups: HashMap<Path, (NodeId, Bytes)>,
    new_arrays: HashMap<Path, (NodeId, ArrayData)>,
    updated_arrays: HashMap<NodeId, ArrayData>,
    updated_groups: HashMap<NodeId, Bytes>,
    // It's important we keep these sorted, we use this fact in TransactionLog creation
    set_chunks: BTreeMap<NodeId, BTreeMap<ChunkIndices, Option<ChunkPayload>>>,
    deleted_groups: HashSet<(Path, NodeId)>,
    deleted_arrays: HashSet<(Path, NodeId)>,
}

impl ChangeSet {
    pub fn deleted_arrays(&self) -> impl Iterator<Item = &(Path, NodeId)> {
        self.deleted_arrays.iter()
    }

    pub fn deleted_groups(&self) -> impl Iterator<Item = &(Path, NodeId)> {
        self.deleted_groups.iter()
    }

    pub fn updated_arrays(&self) -> impl Iterator<Item = &NodeId> {
        self.updated_arrays.keys()
    }

    pub fn updated_groups(&self) -> impl Iterator<Item = &NodeId> {
        self.updated_groups.keys()
    }

    pub fn array_is_deleted(&self, path_and_id: &(Path, NodeId)) -> bool {
        self.deleted_arrays.contains(path_and_id)
    }

    pub fn chunk_changes(
        &self,
    ) -> impl Iterator<Item = (&NodeId, &BTreeMap<ChunkIndices, Option<ChunkPayload>>)>
    {
        self.set_chunks.iter()
    }

    pub fn has_chunk_changes(&self, node: &NodeId) -> bool {
        self.set_chunks.get(node).map(|m| !m.is_empty()).unwrap_or(false)
    }

    pub fn arrays_with_chunk_changes(&self) -> impl Iterator<Item = &NodeId> {
        self.chunk_changes().map(|(node, _)| node)
    }

    pub fn is_empty(&self) -> bool {
        self == &ChangeSet::default()
    }

    pub fn add_group(&mut self, path: Path, node_id: NodeId, definition: Bytes) {
        debug_assert!(!self.updated_groups.contains_key(&node_id));
        self.new_groups.insert(path, (node_id, definition));
    }

    pub fn get_group(&self, path: &Path) -> Option<&(NodeId, Bytes)> {
        self.new_groups.get(path)
    }

    pub fn get_array(&self, path: &Path) -> Option<&(NodeId, ArrayData)> {
        self.new_arrays.get(path)
    }

    /// IMPORTANT: This method does not delete children. The caller
    /// is responsible for doing that
    pub fn delete_group(&mut self, path: Path, node_id: &NodeId) {
        self.updated_groups.remove(node_id);
        if self.new_groups.remove(&path).is_none() {
            // it's an old group, we need to flag it as deleted
            self.deleted_groups.insert((path, node_id.clone()));
        }
    }

    pub fn add_array(&mut self, path: Path, node_id: NodeId, array_data: ArrayData) {
        self.new_arrays.insert(path, (node_id, array_data));
    }

    pub fn update_array(&mut self, node_id: &NodeId, path: &Path, array_data: ArrayData) {
        match self.new_arrays.get(path) {
            Some((id, _)) => {
                debug_assert!(!self.updated_arrays.contains_key(id));
                self.new_arrays.insert(path.clone(), (node_id.clone(), array_data));
            }
            None => {
                self.updated_arrays.insert(node_id.clone(), array_data);
            }
        }
    }

    pub fn update_group(&mut self, node_id: &NodeId, path: &Path, definition: Bytes) {
        match self.new_groups.get(path) {
            Some((id, _)) => {
                debug_assert!(!self.updated_groups.contains_key(id));
                self.new_groups.insert(path.clone(), (node_id.clone(), definition));
            }
            None => {
                self.updated_groups.insert(node_id.clone(), definition);
            }
        }
    }

    pub fn delete_array(&mut self, path: Path, node_id: &NodeId) {
        // if deleting a new array created in this session, just remove the entry
        // from new_arrays
        let node_and_meta = self.new_arrays.remove(&path);
        let is_new_array = node_and_meta.is_some();
        debug_assert!(
            !is_new_array || node_and_meta.map(|n| n.0).as_ref() == Some(node_id)
        );

        self.updated_arrays.remove(node_id);
        self.set_chunks.remove(node_id);
        if !is_new_array {
            self.deleted_arrays.insert((path, node_id.clone()));
        }
    }

    pub fn is_deleted(&self, path: &Path, node_id: &NodeId) -> bool {
        let key = (path.clone(), node_id.clone());
        self.deleted_groups.contains(&key) || self.deleted_arrays.contains(&key)
    }

    //pub fn has_updated_definition(&self, node_id: &NodeId) -> bool {
    //    self.updated_definitions.contains_key(node_id)
    //}

    pub fn get_updated_array(&self, node_id: &NodeId) -> Option<&ArrayData> {
        self.updated_arrays.get(node_id)
    }

    pub fn get_updated_group(&self, node_id: &NodeId) -> Option<&Bytes> {
        self.updated_groups.get(node_id)
    }

    pub fn set_chunk_ref(
        &mut self,
        node_id: NodeId,
        coord: ChunkIndices,
        data: Option<ChunkPayload>,
    ) {
        // this implementation makes delete idempotent
        // it allows deleting a deleted chunk by repeatedly setting None.
        self.set_chunks
            .entry(node_id)
            .and_modify(|h| {
                h.insert(coord.clone(), data.clone());
            })
            .or_insert(BTreeMap::from([(coord, data)]));
    }

    pub fn get_chunk_ref(
        &self,
        node_id: &NodeId,
        coords: &ChunkIndices,
    ) -> Option<&Option<ChunkPayload>> {
        self.set_chunks.get(node_id).and_then(|h| h.get(coords))
    }

    /// Drop the updated chunk references for the node.
    /// This will only drop the references for which `predicate` returns true
    pub fn drop_chunk_changes(
        &mut self,
        node_id: &NodeId,
        predicate: impl Fn(&ChunkIndices) -> bool,
    ) {
        if let Some(changes) = self.set_chunks.get_mut(node_id) {
            changes.retain(|coord, _| !predicate(coord));
        }
    }

    pub fn array_chunks_iterator(
        &self,
        node_id: &NodeId,
        node_path: &Path,
    ) -> impl Iterator<Item = (&ChunkIndices, &Option<ChunkPayload>)> {
        if self.is_deleted(node_path, node_id) {
            return Either::Left(iter::empty());
        }
        match self.set_chunks.get(node_id) {
            None => Either::Left(iter::empty()),
            Some(h) => Either::Right(h.iter()),
        }
    }

    pub fn new_arrays_chunk_iterator(
        &self,
    ) -> impl Iterator<Item = (Path, ChunkInfo)> + '_ {
        self.new_arrays.iter().flat_map(|(path, (node_id, _))| {
            self.new_array_chunk_iterator(node_id, path).map(|ci| (path.clone(), ci))
        })
    }

    pub fn new_array_chunk_iterator<'a>(
        &'a self,
        node_id: &'a NodeId,
        node_path: &Path,
    ) -> impl Iterator<Item = ChunkInfo> + 'a {
        self.array_chunks_iterator(node_id, node_path).filter_map(
            move |(coords, payload)| {
                payload.as_ref().map(|p| ChunkInfo {
                    node: node_id.clone(),
                    coord: coords.clone(),
                    payload: p.clone(),
                })
            },
        )
    }

    pub fn new_nodes(&self) -> impl Iterator<Item = (&Path, &NodeId)> {
        self.new_groups().chain(self.new_arrays())
    }

    pub fn new_groups(&self) -> impl Iterator<Item = (&Path, &NodeId)> {
        self.new_groups.iter().map(|(path, (node_id, _))| (path, node_id))
    }

    pub fn new_arrays(&self) -> impl Iterator<Item = (&Path, &NodeId)> {
        self.new_arrays.iter().map(|(path, (node_id, _))| (path, node_id))
    }

    pub fn take_chunks(
        &mut self,
    ) -> BTreeMap<NodeId, BTreeMap<ChunkIndices, Option<ChunkPayload>>> {
        take(&mut self.set_chunks)
    }

    pub fn set_chunks(
        &mut self,
        chunks: BTreeMap<NodeId, BTreeMap<ChunkIndices, Option<ChunkPayload>>>,
    ) {
        self.set_chunks = chunks
    }

    /// Merge this ChangeSet with `other`.
    ///
    /// Results of the merge are applied to `self`. Changes present in `other` take precedence over
    /// `self` changes.
    pub fn merge(&mut self, other: ChangeSet) {
        // FIXME: this should detect conflict, for example, if different writers added on the same
        // path, different objects, or if the same path is added and deleted, etc.
        // TODO: optimize
        self.new_groups.extend(other.new_groups);
        self.new_arrays.extend(other.new_arrays);
        self.updated_groups.extend(other.updated_groups);
        self.updated_arrays.extend(other.updated_arrays);
        self.deleted_groups.extend(other.deleted_groups);
        self.deleted_arrays.extend(other.deleted_arrays);

        for (node, other_chunks) in other.set_chunks.into_iter() {
            match self.set_chunks.remove(&node) {
                Some(mut old_value) => {
                    old_value.extend(other_chunks);
                    self.set_chunks.insert(node, old_value);
                }
                None => {
                    self.set_chunks.insert(node, other_chunks);
                }
            }
        }
    }

    pub fn merge_many<T: IntoIterator<Item = ChangeSet>>(&mut self, others: T) {
        others.into_iter().fold(self, |res, change_set| {
            res.merge(change_set);
            res
        });
    }

    /// Serialize this ChangeSet
    ///
    /// This is intended to help with marshalling distributed writers back to the coordinator
    pub fn export_to_bytes(&self) -> SessionResult<Vec<u8>> {
        Ok(rmp_serde::to_vec(self)?)
    }

    /// Deserialize a ChangeSet
    ///
    /// This is intended to help with marshalling distributed writers back to the coordinator
    pub fn import_from_bytes(bytes: &[u8]) -> SessionResult<Self> {
        Ok(rmp_serde::from_slice(bytes)?)
    }

    pub fn update_existing_chunks<'a, E>(
        &'a self,
        node: NodeId,
        chunks: impl Iterator<Item = Result<ChunkInfo, E>> + 'a,
    ) -> impl Iterator<Item = Result<ChunkInfo, E>> + 'a {
        chunks.filter_map_ok(move |chunk| match self.get_chunk_ref(&node, &chunk.coord) {
            None => Some(chunk),
            Some(new_payload) => {
                new_payload.clone().map(|pl| ChunkInfo { payload: pl, ..chunk })
            }
        })
    }

    pub fn get_new_node(&self, path: &Path) -> Option<NodeSnapshot> {
        self.get_new_array(path).or(self.get_new_group(path))
    }

    pub fn get_new_array(&self, path: &Path) -> Option<NodeSnapshot> {
        self.get_array(path).map(|(id, array_data)| {
            debug_assert!(!self.updated_arrays.contains_key(id));
            NodeSnapshot {
                id: id.clone(),
                path: path.clone(),
                user_data: array_data.user_data.clone(),
                // We put no manifests in new arrays, see get_chunk_ref to understand how chunks get
                // fetched for those arrays
                node_data: NodeData::Array {
                    shape: array_data.shape.clone(),
                    dimension_names: array_data.dimension_names.clone(),
                    manifests: vec![],
                },
            }
        })
    }

    pub fn get_new_group(&self, path: &Path) -> Option<NodeSnapshot> {
        self.get_group(path).map(|(id, definition)| {
            debug_assert!(!self.updated_groups.contains_key(id));
            NodeSnapshot {
                id: id.clone(),
                path: path.clone(),
                user_data: definition.clone(),
                node_data: NodeData::Group,
            }
        })
    }

    pub fn new_nodes_iterator(&self) -> impl Iterator<Item = NodeSnapshot> + '_ {
        self.new_nodes().filter_map(move |(path, node_id)| {
            if self.is_deleted(path, node_id) {
                return None;
            }
            // we should be able to create the full node because we
            // know it's a new node
            #[allow(clippy::expect_used)]
            let node = self.get_new_node(path).expect("Bug in new_nodes implementation");
            Some(node)
        })
    }

    // Applies the changeset to an existing node, yielding a new node if it hasn't been deleted
    pub fn update_existing_node(&self, node: NodeSnapshot) -> Option<NodeSnapshot> {
        if self.is_deleted(&node.path, &node.id) {
            return None;
        }

        match node.node_data {
            NodeData::Group => {
                let new_definition =
                    self.updated_groups.get(&node.id).cloned().unwrap_or(node.user_data);
                Some(NodeSnapshot { user_data: new_definition, ..node })
            }
            NodeData::Array { shape, dimension_names, manifests } => {
                let new_data =
                    self.updated_arrays.get(&node.id).cloned().unwrap_or_else(|| {
                        ArrayData { shape, dimension_names, user_data: node.user_data }
                    });
                Some(NodeSnapshot {
                    user_data: new_data.user_data,
                    node_data: NodeData::Array {
                        shape: new_data.shape,
                        dimension_names: new_data.dimension_names,
                        manifests,
                    },
                    ..node
                })
            }
        }
    }

    pub fn undo_update(&mut self, node_id: &NodeId) {
        self.updated_arrays.remove(node_id);
        self.updated_groups.remove(node_id);
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use bytes::Bytes;
    use itertools::Itertools;

    use super::ChangeSet;

    use crate::{
        change_set::ArrayData,
        format::{
            manifest::{ChunkInfo, ChunkPayload},
            snapshot::ArrayShape,
            ChunkIndices, NodeId,
        },
    };

    #[test]
    fn test_new_arrays_chunk_iterator() {
        let mut change_set = ChangeSet::default();
        assert_eq!(None, change_set.new_arrays_chunk_iterator().next());

        let shape = ArrayShape::new(vec![(2, 1), (2, 1), (2, 1)]).unwrap();
        let dimension_names = Some(vec!["x".into(), "y".into(), "t".into()]);

        let node_id1 = NodeId::random();
        let node_id2 = NodeId::random();
        let array_data = ArrayData {
            shape: shape.clone(),
            dimension_names: dimension_names.clone(),
            user_data: Bytes::from_static(b"foobar"),
        };
        change_set.add_array(
            "/foo/bar".try_into().unwrap(),
            node_id1.clone(),
            array_data.clone(),
        );
        change_set.add_array(
            "/foo/baz".try_into().unwrap(),
            node_id2.clone(),
            array_data.clone(),
        );
        assert_eq!(None, change_set.new_arrays_chunk_iterator().next());

        change_set.set_chunk_ref(node_id1.clone(), ChunkIndices(vec![0, 1]), None);
        assert_eq!(None, change_set.new_arrays_chunk_iterator().next());

        change_set.set_chunk_ref(
            node_id1.clone(),
            ChunkIndices(vec![1, 0]),
            Some(ChunkPayload::Inline("bar1".into())),
        );
        change_set.set_chunk_ref(
            node_id1.clone(),
            ChunkIndices(vec![1, 1]),
            Some(ChunkPayload::Inline("bar2".into())),
        );
        change_set.set_chunk_ref(
            node_id2.clone(),
            ChunkIndices(vec![0]),
            Some(ChunkPayload::Inline("baz1".into())),
        );
        change_set.set_chunk_ref(
            node_id2.clone(),
            ChunkIndices(vec![1]),
            Some(ChunkPayload::Inline("baz2".into())),
        );

        {
            let all_chunks: Vec<_> = change_set
                .new_arrays_chunk_iterator()
                .sorted_by_key(|c| c.1.coord.clone())
                .collect();
            let expected_chunks: Vec<_> = [
                (
                    "/foo/baz".try_into().unwrap(),
                    ChunkInfo {
                        node: node_id2.clone(),
                        coord: ChunkIndices(vec![0]),
                        payload: ChunkPayload::Inline("baz1".into()),
                    },
                ),
                (
                    "/foo/baz".try_into().unwrap(),
                    ChunkInfo {
                        node: node_id2.clone(),
                        coord: ChunkIndices(vec![1]),
                        payload: ChunkPayload::Inline("baz2".into()),
                    },
                ),
                (
                    "/foo/bar".try_into().unwrap(),
                    ChunkInfo {
                        node: node_id1.clone(),
                        coord: ChunkIndices(vec![1, 0]),
                        payload: ChunkPayload::Inline("bar1".into()),
                    },
                ),
                (
                    "/foo/bar".try_into().unwrap(),
                    ChunkInfo {
                        node: node_id1.clone(),
                        coord: ChunkIndices(vec![1, 1]),
                        payload: ChunkPayload::Inline("bar2".into()),
                    },
                ),
            ]
            .into();
            assert_eq!(all_chunks, expected_chunks);
        }
    }
}
