use std::{
    collections::{HashMap, HashSet},
    iter,
    mem::take,
};

use itertools::Either;

use crate::{
    format::{manifest::ChunkInfo, NodeId},
    metadata::UserAttributes,
    repository::{ChunkIndices, ChunkPayload, Path, ZarrArrayMetadata},
};

#[derive(Clone, Debug, PartialEq, Default)]
pub struct ChangeSet {
    new_groups: HashMap<Path, NodeId>,
    new_arrays: HashMap<Path, (NodeId, ZarrArrayMetadata)>,
    updated_arrays: HashMap<NodeId, ZarrArrayMetadata>,
    // These paths may point to Arrays or Groups,
    // since both Groups and Arrays support UserAttributes
    updated_attributes: HashMap<NodeId, Option<UserAttributes>>,
    // FIXME: issue with too many inline chunks kept in mem
    set_chunks: HashMap<NodeId, HashMap<ChunkIndices, Option<ChunkPayload>>>,
    deleted_groups: HashSet<NodeId>,
    deleted_arrays: HashSet<NodeId>,
}

impl ChangeSet {
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

    pub fn delete_group(&mut self, path: &Path, node_id: NodeId) {
        let new_node_id = self.new_groups.remove(path);
        let is_new_group = new_node_id.is_some();
        debug_assert!(!is_new_group || new_node_id == Some(node_id));

        self.updated_attributes.remove(&node_id);
        if !is_new_group {
            self.deleted_groups.insert(node_id);
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

    pub fn delete_array(&mut self, path: &Path, node_id: NodeId) {
        // if deleting a new array created in this session, just remove the entry
        // from new_arrays
        let node_and_meta = self.new_arrays.remove(path);
        let is_new_array = node_and_meta.is_some();
        debug_assert!(!is_new_array || node_and_meta.map(|n| n.0) == Some(node_id));

        self.updated_arrays.remove(&node_id);
        self.updated_attributes.remove(&node_id);
        self.set_chunks.remove(&node_id);
        if !is_new_array {
            self.deleted_arrays.insert(node_id);
        }
    }

    pub fn is_deleted(&self, node_id: &NodeId) -> bool {
        self.deleted_groups.contains(node_id) || self.deleted_arrays.contains(node_id)
    }

    pub fn has_updated_attributes(&self, node_id: &NodeId) -> bool {
        self.updated_attributes.contains_key(node_id)
    }

    pub fn get_updated_zarr_metadata(
        &self,
        node_id: NodeId,
    ) -> Option<&ZarrArrayMetadata> {
        self.updated_arrays.get(&node_id)
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
        node_id: NodeId,
    ) -> Option<&Option<UserAttributes>> {
        self.updated_attributes.get(&node_id)
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
        node_id: NodeId,
        coords: &ChunkIndices,
    ) -> Option<&Option<ChunkPayload>> {
        self.set_chunks.get(&node_id).and_then(|h| h.get(coords))
    }

    pub fn array_chunks_iterator(
        &self,
        node_id: NodeId,
    ) -> impl Iterator<Item = (&ChunkIndices, &Option<ChunkPayload>)> {
        match self.set_chunks.get(&node_id) {
            None => Either::Left(iter::empty()),
            Some(h) => Either::Right(h.iter()),
        }
    }

    pub fn new_arrays_chunk_iterator(
        &self,
    ) -> impl Iterator<Item = (Path, ChunkInfo)> + '_ {
        self.new_arrays.iter().flat_map(|(path, (node_id, _))| {
            self.array_chunks_iterator(*node_id).filter_map(|(coords, payload)| {
                payload.as_ref().map(|p| {
                    (
                        path.clone(),
                        ChunkInfo {
                            node: *node_id,
                            coord: coords.clone(),
                            payload: p.clone(),
                        },
                    )
                })
            })
        })
    }

    pub fn new_nodes(&self) -> impl Iterator<Item = &Path> {
        self.new_groups.keys().chain(self.new_arrays.keys())
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
}
