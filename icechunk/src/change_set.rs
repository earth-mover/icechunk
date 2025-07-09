use std::{
    collections::{BTreeMap, HashMap, HashSet},
    iter,
};

use bytes::Bytes;
use itertools::{Either, Itertools as _};
use serde::{Deserialize, Serialize};

use crate::{
    format::{
        ChunkIndices, NodeId, Path,
        manifest::{ChunkInfo, ChunkPayload, ManifestExtents, ManifestSplits, Overlap},
        snapshot::{ArrayShape, DimensionName, NodeData, NodeSnapshot},
    },
    session::{SessionResult, find_coord},
};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArrayData {
    pub shape: ArrayShape,
    pub dimension_names: Option<Vec<DimensionName>>,
    pub user_data: Bytes,
}

type SplitManifest = BTreeMap<ChunkIndices, Option<ChunkPayload>>;
#[derive(Clone, Debug, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct ChangeSet {
    new_groups: HashMap<Path, (NodeId, Bytes)>,
    new_arrays: HashMap<Path, (NodeId, ArrayData)>,
    updated_arrays: HashMap<NodeId, ArrayData>,
    updated_groups: HashMap<NodeId, Bytes>,
    set_chunks: BTreeMap<NodeId, HashMap<ManifestExtents, SplitManifest>>,
    // This map keeps track of any chunk deletes that are
    // outside the domain of the current array shape. This is needed to handle
    // the very unlikely case of multiple resizes in the same session.
    deleted_chunks_outside_bounds: BTreeMap<NodeId, HashSet<ChunkIndices>>,
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

    pub fn changed_chunks(
        &self,
    ) -> impl Iterator<Item = (&NodeId, impl Iterator<Item = &ChunkIndices>)> {
        self.set_chunks.iter().map(|(node_id, split_map)| {
            (node_id, split_map.values().flat_map(|x| x.keys()))
        })
    }

    pub fn is_updated_array(&self, node: &NodeId) -> bool {
        self.updated_arrays.contains_key(node)
    }

    pub fn has_chunk_changes(&self, node: &NodeId) -> bool {
        self.set_chunks.get(node).map(|m| !m.is_empty()).unwrap_or(false)
    }

    pub fn arrays_with_chunk_changes(&self) -> impl Iterator<Item = &NodeId> {
        self.set_chunks.keys()
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

    pub fn update_array(
        &mut self,
        node_id: &NodeId,
        path: &Path,
        array_data: ArrayData,
        new_splits: &ManifestSplits,
    ) {
        match self.new_arrays.get(path) {
            Some((id, _)) => {
                debug_assert!(!self.updated_arrays.contains_key(id));
                self.new_arrays.insert(path.clone(), (node_id.clone(), array_data));
            }
            None => {
                self.updated_arrays.insert(node_id.clone(), array_data);
            }
        }

        // update existing splits
        let mut to_remove = HashSet::<ChunkIndices>::new();
        if let Some(manifests) = self.set_chunks.remove(node_id) {
            let mut new_deleted_chunks = HashSet::<ChunkIndices>::new();
            let mut new_manifests =
                HashMap::<ManifestExtents, SplitManifest>::with_capacity(
                    new_splits.len(),
                );
            for (old_extents, mut chunks) in manifests.into_iter() {
                for new_extents in new_splits.iter() {
                    if old_extents.overlap_with(new_extents) == Overlap::None {
                        continue;
                    }

                    // TODO: replace with `BTreeMap.drain_filter` after it is stable.
                    let mut extracted =
                        BTreeMap::<ChunkIndices, Option<ChunkPayload>>::new();
                    chunks.retain(|coord, payload| {
                        let cond = new_extents.contains(coord.0.as_slice());
                        if cond {
                            extracted.insert(coord.clone(), payload.clone());
                        }
                        !cond
                    });
                    new_manifests
                        .entry(new_extents.clone())
                        .or_default()
                        .extend(extracted);
                }
                new_deleted_chunks.extend(
                    chunks.into_iter().filter_map(|(coord, payload)| {
                        payload.is_none().then_some(coord)
                    }),
                );
            }

            // bring back any previously tracked deletes
            if let Some(deletes) = self.deleted_chunks_outside_bounds.get_mut(node_id) {
                for coord in deletes.iter() {
                    if let Some(extents) = new_splits.find(coord) {
                        new_manifests
                            .entry(extents.clone())
                            .or_default()
                            .insert(coord.clone(), None);
                        to_remove.insert(coord.clone());
                    };
                }
                deletes.retain(|item| !to_remove.contains(item));
                to_remove.drain();
            };
            self.set_chunks.insert(node_id.clone(), new_manifests);

            // keep track of any deletes not inserted in to set_chunks
            self.deleted_chunks_outside_bounds
                .entry(node_id.clone())
                .or_default()
                .extend(new_deleted_chunks);
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
        splits: &ManifestSplits,
    ) {
        #[allow(clippy::expect_used)]
        let extent = splits.find(&coord).expect("logic bug. Trying to set chunk ref but can't find the appropriate split manifest.");
        // this implementation makes delete idempotent
        // it allows deleting a deleted chunk by repeatedly setting None.
        self.set_chunks
            .entry(node_id)
            .or_insert_with(|| {
                HashMap::<
                    ManifestExtents,
                    BTreeMap<ChunkIndices, Option<ChunkPayload>>,
                >::with_capacity(splits.len())
            })
            .entry(extent.clone())
            .or_default()
            .insert(coord, data);
    }

    pub fn get_chunk_ref(
        &self,
        node_id: &NodeId,
        coords: &ChunkIndices,
    ) -> Option<&Option<ChunkPayload>> {
        self.set_chunks.get(node_id).and_then(|node_chunks| {
            find_coord(node_chunks.keys(), coords).and_then(|(_, extent)| {
                node_chunks.get(extent).and_then(|s| s.get(coords))
            })
        })
    }

    /// Drop the updated chunk references for the node.
    /// This will only drop the references for which `predicate` returns true
    pub fn drop_chunk_changes(
        &mut self,
        node_id: &NodeId,
        predicate: impl Fn(&ChunkIndices) -> bool,
    ) {
        if let Some(changes) = self.set_chunks.get_mut(node_id) {
            for split in changes.values_mut() {
                split.retain(|coord, _| !predicate(coord));
            }
        }
    }

    pub fn deleted_chunks_iterator(
        &self,
        node_id: &NodeId,
    ) -> impl Iterator<Item = &ChunkIndices> {
        match self.deleted_chunks_outside_bounds.get(node_id) {
            Some(deletes) => Either::Right(deletes.iter()),
            None => Either::Left(iter::empty()),
        }
    }

    pub fn array_chunks_iterator(
        &self,
        node_id: &NodeId,
        node_path: &Path,
        extent: ManifestExtents,
    ) -> impl Iterator<Item = (&ChunkIndices, &Option<ChunkPayload>)> + use<'_> {
        if self.is_deleted(node_path, node_id) {
            return Either::Left(iter::empty());
        }
        match self.set_chunks.get(node_id) {
            None => Either::Left(iter::empty()),
            Some(h) => Either::Right(
                h.iter()
                    .filter(move |(manifest_extent, _)| extent.matches(manifest_extent))
                    .flat_map(|(_, manifest)| manifest.iter()),
            ),
        }
    }

    pub fn new_arrays_chunk_iterator(
        &self,
    ) -> impl Iterator<Item = (Path, ChunkInfo)> + use<'_> {
        self.new_arrays.iter().flat_map(|(path, (node_id, _))| {
            self.new_array_chunk_iterator(node_id, path, ManifestExtents::ALL)
                .map(|ci| (path.clone(), ci))
        })
    }

    pub fn new_array_chunk_iterator<'a>(
        &'a self,
        node_id: &'a NodeId,
        node_path: &Path,
        extent: ManifestExtents,
    ) -> impl Iterator<Item = ChunkInfo> + use<'a> {
        self.array_chunks_iterator(node_id, node_path, extent).filter_map(
            move |(coords, payload)| {
                payload.as_ref().map(|p| ChunkInfo {
                    node: node_id.clone(),
                    coord: coords.clone(),
                    payload: p.clone(),
                })
            },
        )
    }

    pub fn modified_manifest_extents_iterator(
        &self,
        node_id: &NodeId,
        node_path: &Path,
    ) -> impl Iterator<Item = &ManifestExtents> + use<'_> {
        if self.is_deleted(node_path, node_id) {
            return Either::Left(iter::empty());
        }
        match self.set_chunks.get(node_id) {
            None => Either::Left(iter::empty()),
            Some(h) => Either::Right(h.keys()),
        }
    }

    pub fn array_manifest(
        &self,
        node_id: &NodeId,
        extent: &ManifestExtents,
    ) -> Option<&SplitManifest> {
        self.set_chunks.get(node_id).and_then(|x| x.get(extent))
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
        // FIXME: do we even test this?
        self.deleted_chunks_outside_bounds.extend(other.deleted_chunks_outside_bounds);

        other.set_chunks.into_iter().for_each(|(node, other_splits)| {
            let manifests = self.set_chunks.entry(node).or_insert_with(|| {
                HashMap::<ManifestExtents, SplitManifest>::with_capacity(
                    other_splits.len(),
                )
            });
            other_splits.into_iter().for_each(|(extent, their_manifest)| {
                manifests.entry(extent).or_default().extend(their_manifest)
            })
        });
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
        Ok(rmp_serde::to_vec(self).map_err(Box::new)?)
    }

    /// Deserialize a ChangeSet
    ///
    /// This is intended to help with marshalling distributed writers back to the coordinator
    pub fn import_from_bytes(bytes: &[u8]) -> SessionResult<Self> {
        Ok(rmp_serde::from_slice(bytes).map_err(Box::new)?)
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

    pub fn new_nodes_iterator(&self) -> impl Iterator<Item = NodeSnapshot> {
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
            ChunkIndices, NodeId,
            manifest::{ChunkInfo, ChunkPayload, ManifestSplits},
            snapshot::ArrayShape,
        },
    };

    #[icechunk_macros::test]
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

        let splits1 = ManifestSplits::from_edges(vec![vec![0, 10], vec![0, 10]]);

        change_set.set_chunk_ref(
            node_id1.clone(),
            ChunkIndices(vec![0, 1]),
            None,
            &splits1,
        );
        assert_eq!(None, change_set.new_arrays_chunk_iterator().next());

        change_set.set_chunk_ref(
            node_id1.clone(),
            ChunkIndices(vec![1, 0]),
            Some(ChunkPayload::Inline("bar1".into())),
            &splits1,
        );
        change_set.set_chunk_ref(
            node_id1.clone(),
            ChunkIndices(vec![1, 1]),
            Some(ChunkPayload::Inline("bar2".into())),
            &splits1,
        );

        let splits2 = ManifestSplits::from_edges(vec![vec![0, 10]]);
        change_set.set_chunk_ref(
            node_id2.clone(),
            ChunkIndices(vec![0]),
            Some(ChunkPayload::Inline("baz1".into())),
            &splits2,
        );
        change_set.set_chunk_ref(
            node_id2.clone(),
            ChunkIndices(vec![1]),
            Some(ChunkPayload::Inline("baz2".into())),
            &splits2,
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
