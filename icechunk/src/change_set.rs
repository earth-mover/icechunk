use std::{
    collections::{HashMap, HashSet},
    iter,
    mem::take,
};

use itertools::Either;
use serde::{Deserialize, Serialize};

use crate::{
    format::{
        manifest::{ChunkInfo, ChunkPayload, ManifestExtents, ManifestRef},
        snapshot::{NodeData, NodeSnapshot, UserAttributesSnapshot, ZarrArrayMetadata},
        ChunkIndices, ManifestId, NodeId, Path,
    },
    metadata::UserAttributes,
    session::SessionResult,
};

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct ChangeSet {
    new_groups: HashMap<Path, NodeId>,
    new_arrays: HashMap<Path, (NodeId, ZarrArrayMetadata)>,
    updated_arrays: HashMap<NodeId, ZarrArrayMetadata>,
    // These paths may point to Arrays or Groups,
    // since both Groups and Arrays support UserAttributes
    updated_attributes: HashMap<NodeId, Option<UserAttributes>>,
    // FIXME: issue with too many inline chunks kept in mem
    set_chunks: HashMap<NodeId, HashMap<ChunkIndices, Option<ChunkPayload>>>,
    deleted_groups: HashSet<(Path, NodeId)>,
    deleted_arrays: HashSet<(Path, NodeId)>,
}

impl ChangeSet {
    pub fn zarr_updated_arrays(&self) -> impl Iterator<Item = &NodeId> {
        self.updated_arrays.keys()
    }

    pub fn deleted_arrays(&self) -> impl Iterator<Item = &(Path, NodeId)> {
        self.deleted_arrays.iter()
    }

    pub fn deleted_groups(&self) -> impl Iterator<Item = &(Path, NodeId)> {
        self.deleted_groups.iter()
    }

    pub fn user_attributes_updated_nodes(&self) -> impl Iterator<Item = &NodeId> {
        self.updated_attributes.keys()
    }

    pub fn array_is_deleted(&self, path_and_id: &(Path, NodeId)) -> bool {
        self.deleted_arrays.contains(path_and_id)
    }

    pub fn chunk_changes(
        &self,
    ) -> impl Iterator<Item = (&NodeId, &HashMap<ChunkIndices, Option<ChunkPayload>>)>
    {
        self.set_chunks.iter()
    }

    pub fn arrays_with_chunk_changes(&self) -> impl Iterator<Item = &NodeId> {
        self.chunk_changes().map(|(node, _)| node)
    }

    pub fn is_empty(&self) -> bool {
        self == &ChangeSet::default()
    }

    pub fn add_group(&mut self, path: Path, node_id: NodeId) {
        self.new_groups.insert(path, node_id);
    }

    pub fn get_group(&self, path: &Path) -> Option<&NodeId> {
        self.new_groups.get(path)
    }

    pub fn get_array(&self, path: &Path) -> Option<&(NodeId, ZarrArrayMetadata)> {
        self.new_arrays.get(path)
    }

    /// IMPORTANT: This method does not delete children. The caller
    /// is responsible for doing that
    pub fn delete_group(&mut self, path: Path, node_id: &NodeId) {
        self.updated_attributes.remove(node_id);
        if self.new_groups.remove(&path).is_none() {
            // it's an old group, we need to flag it as deleted
            self.deleted_groups.insert((path, node_id.clone()));
        }
    }

    pub fn add_array(
        &mut self,
        path: Path,
        node_id: NodeId,
        metadata: ZarrArrayMetadata,
    ) {
        self.new_arrays.insert(path, (node_id, metadata));
    }

    pub fn update_array(&mut self, node_id: NodeId, metadata: ZarrArrayMetadata) {
        self.updated_arrays.insert(node_id, metadata);
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
        self.updated_attributes.remove(node_id);
        self.set_chunks.remove(node_id);
        if !is_new_array {
            self.deleted_arrays.insert((path, node_id.clone()));
        }
    }

    pub fn is_deleted(&self, path: &Path, node_id: &NodeId) -> bool {
        let key = (path.clone(), node_id.clone());
        self.deleted_groups.contains(&key) || self.deleted_arrays.contains(&key)
    }

    pub fn has_updated_attributes(&self, node_id: &NodeId) -> bool {
        self.updated_attributes.contains_key(node_id)
    }

    pub fn get_updated_zarr_metadata(
        &self,
        node_id: &NodeId,
    ) -> Option<&ZarrArrayMetadata> {
        self.updated_arrays.get(node_id)
    }

    pub fn update_user_attributes(
        &mut self,
        node_id: NodeId,
        atts: Option<UserAttributes>,
    ) {
        self.updated_attributes.insert(node_id, atts);
    }

    pub fn get_user_attributes(
        &self,
        node_id: &NodeId,
    ) -> Option<&Option<UserAttributes>> {
        self.updated_attributes.get(node_id)
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
            .or_insert(HashMap::from([(coord, data)]));
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
            self.array_chunks_iterator(node_id, path).filter_map(|(coords, payload)| {
                payload.as_ref().map(|p| {
                    (
                        path.clone(),
                        ChunkInfo {
                            node: node_id.clone(),
                            coord: coords.clone(),
                            payload: p.clone(),
                        },
                    )
                })
            })
        })
    }

    pub fn new_nodes(&self) -> impl Iterator<Item = (&Path, &NodeId)> {
        self.new_groups().chain(self.new_arrays())
    }

    pub fn new_groups(&self) -> impl Iterator<Item = (&Path, &NodeId)> {
        self.new_groups.iter()
    }

    pub fn new_arrays(&self) -> impl Iterator<Item = (&Path, &NodeId)> {
        self.new_arrays.iter().map(|(path, (node_id, _))| (path, node_id))
    }

    pub fn take_chunks(
        &mut self,
    ) -> HashMap<NodeId, HashMap<ChunkIndices, Option<ChunkPayload>>> {
        take(&mut self.set_chunks)
    }

    pub fn set_chunks(
        &mut self,
        chunks: HashMap<NodeId, HashMap<ChunkIndices, Option<ChunkPayload>>>,
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
        self.updated_arrays.extend(other.updated_arrays);
        self.updated_attributes.extend(other.updated_attributes);
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

    pub fn update_existing_chunks<'a>(
        &'a self,
        node: NodeId,
        chunks: impl Iterator<Item = ChunkInfo> + 'a,
    ) -> impl Iterator<Item = ChunkInfo> + 'a {
        chunks.filter_map(move |chunk| match self.get_chunk_ref(&node, &chunk.coord) {
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
        self.get_array(path).map(|(id, meta)| {
            let meta = self.get_updated_zarr_metadata(id).unwrap_or(meta).clone();
            let atts = self.get_user_attributes(id).cloned();
            NodeSnapshot {
                id: id.clone(),
                path: path.clone(),
                user_attributes: atts.flatten().map(UserAttributesSnapshot::Inline),
                // We put no manifests in new arrays, see get_chunk_ref to understand how chunks get
                // fetched for those arrays
                node_data: NodeData::Array(meta.clone(), vec![]),
            }
        })
    }

    pub fn get_new_group(&self, path: &Path) -> Option<NodeSnapshot> {
        self.get_group(path).map(|id| {
            let atts = self.get_user_attributes(id).cloned();
            NodeSnapshot {
                id: id.clone(),
                path: path.clone(),
                user_attributes: atts.flatten().map(UserAttributesSnapshot::Inline),
                node_data: NodeData::Group,
            }
        })
    }

    pub fn new_nodes_iterator<'a>(
        &'a self,
        manifest_id: Option<&'a ManifestId>,
    ) -> impl Iterator<Item = NodeSnapshot> + 'a {
        self.new_nodes().filter_map(move |(path, node_id)| {
            if self.is_deleted(path, node_id) {
                return None;
            }
            // we should be able to create the full node because we
            // know it's a new node
            #[allow(clippy::expect_used)]
            let node = self.get_new_node(path).expect("Bug in new_nodes implementation");
            match node.node_data {
                NodeData::Group => Some(node),
                NodeData::Array(meta, _no_manifests_yet) => {
                    let new_manifests = manifest_id
                        .map(|mid| {
                            vec![ManifestRef {
                                object_id: mid.clone(),
                                extents: ManifestExtents(vec![]),
                            }]
                        })
                        .unwrap_or_default();
                    Some(NodeSnapshot {
                        node_data: NodeData::Array(meta, new_manifests),
                        ..node
                    })
                }
            }
        })
    }

    pub fn update_existing_node(
        &self,
        node: NodeSnapshot,
        new_manifests: Option<Vec<ManifestRef>>,
    ) -> Option<NodeSnapshot> {
        if self.is_deleted(&node.path, &node.id) {
            return None;
        }

        let session_atts = self
            .get_user_attributes(&node.id)
            .cloned()
            .map(|a| a.map(UserAttributesSnapshot::Inline));
        let new_atts = session_atts.unwrap_or(node.user_attributes);
        match node.node_data {
            NodeData::Group => Some(NodeSnapshot { user_attributes: new_atts, ..node }),
            NodeData::Array(old_zarr_meta, _) => {
                let new_zarr_meta = self
                    .get_updated_zarr_metadata(&node.id)
                    .cloned()
                    .unwrap_or(old_zarr_meta);

                Some(NodeSnapshot {
                    node_data: NodeData::Array(
                        new_zarr_meta,
                        new_manifests.unwrap_or_default(),
                    ),
                    user_attributes: new_atts,
                    ..node
                })
            }
        }
    }

    pub fn undo_user_attributes_update(&mut self, node_id: &NodeId) {
        self.updated_attributes.remove(node_id);
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use itertools::Itertools;

    use super::ChangeSet;

    use crate::{
        format::{
            manifest::{ChunkInfo, ChunkPayload},
            snapshot::ZarrArrayMetadata,
            ChunkIndices, NodeId,
        },
        metadata::{
            ChunkKeyEncoding, ChunkShape, Codec, DataType, FillValue, StorageTransformer,
        },
    };

    #[test]
    fn test_new_arrays_chunk_iterator() {
        let mut change_set = ChangeSet::default();
        assert_eq!(None, change_set.new_arrays_chunk_iterator().next());

        let zarr_meta = ZarrArrayMetadata {
            shape: vec![2, 2, 2],
            data_type: DataType::Int32,
            chunk_shape: ChunkShape(vec![
                NonZeroU64::new(1).unwrap(),
                NonZeroU64::new(1).unwrap(),
                NonZeroU64::new(1).unwrap(),
            ]),
            chunk_key_encoding: ChunkKeyEncoding::Slash,
            fill_value: FillValue::Int32(0),
            codecs: vec![Codec { name: "mycodec".to_string(), configuration: None }],
            storage_transformers: Some(vec![StorageTransformer {
                name: "mytransformer".to_string(),
                configuration: None,
            }]),
            dimension_names: Some(vec![
                Some("x".to_string()),
                Some("y".to_string()),
                Some("t".to_string()),
            ]),
        };

        let node_id1 = NodeId::random();
        let node_id2 = NodeId::random();
        change_set.add_array(
            "/foo/bar".try_into().unwrap(),
            node_id1.clone(),
            zarr_meta.clone(),
        );
        change_set.add_array("/foo/baz".try_into().unwrap(), node_id2.clone(), zarr_meta);
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
