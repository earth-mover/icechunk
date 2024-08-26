use std::{
    collections::{HashMap, HashSet},
    iter,
    sync::Arc,
};

use bytes::Bytes;
use futures::{Stream, StreamExt};
use itertools::Either;
use thiserror::Error;

use crate::{
    manifest::mk_manifests_table, structure::mk_structure_table, AddNodeError,
    ArrayIndices, ChangeSet, ChunkInfo, ChunkPayload, ChunkRef, Dataset, DatasetConfig,
    DeleteNodeError, Flags, GetNodeError, ManifestExtents, ManifestRef, NodeData, NodeId,
    NodeStructure, ObjectId, Path, Storage, StorageError, TableRegion, UpdateNodeError,
    UserAttributes, UserAttributesStructure, ZarrArrayMetadata,
};

impl ChangeSet {
    fn add_group(&mut self, path: Path, node_id: NodeId) {
        self.new_groups.insert(path, node_id);
    }

    fn get_group(&self, path: &Path) -> Option<&NodeId> {
        self.new_groups.get(path)
    }

    fn delete_group(
        &mut self,
        path: Path,
        node_id: NodeId,
    ) -> Result<(), DeleteNodeError> {
        let was_new = self.new_groups.remove(&path).is_some();
        self.updated_attributes.remove(&path);
        if !was_new {
            self.deleted_groups.insert(path, node_id);
        }
        Ok(())
    }

    fn add_array(&mut self, path: Path, node_id: NodeId, metadata: ZarrArrayMetadata) {
        self.new_arrays.insert(path, (node_id, metadata));
    }

    fn get_array(
        &self,
        path: &Path,
    ) -> Result<&(NodeId, ZarrArrayMetadata), GetNodeError> {
        if self.deleted_arrays.contains_key(path) {
            Err(GetNodeError::NotFound(path.clone()))
        } else {
            self.new_arrays.get(path).ok_or(GetNodeError::NotFound(path.clone()))
        }
    }

    fn update_array(&mut self, path: Path, metadata: ZarrArrayMetadata) {
        // TODO: consider only inserting for arrays not present in `self.new_arrays`?
        self.updated_arrays.insert(path, metadata);
    }

    fn delete_array(
        &mut self,
        path: Path,
        node_id: NodeId,
    ) -> Result<(), DeleteNodeError> {
        // if deleting a new array created in this session, just remove the entry
        // from new_arrays
        let was_new = self.new_arrays.remove(&path).is_some();
        self.updated_arrays.remove(&path);
        self.updated_attributes.remove(&path);
        self.set_chunks.remove(&path);
        if !was_new {
            self.deleted_arrays.insert(path, node_id);
        }
        Ok(())
    }

    fn get_updated_zarr_metadata(&self, path: &Path) -> Option<&ZarrArrayMetadata> {
        self.updated_arrays.get(path)
    }

    fn update_user_attributes(&mut self, path: Path, atts: Option<UserAttributes>) {
        self.updated_attributes.insert(path, atts);
    }

    fn get_user_attributes(&self, path: &Path) -> Option<&Option<UserAttributes>> {
        self.updated_attributes.get(path)
    }

    fn set_chunk_ref(
        &mut self,
        path: Path,
        coord: ArrayIndices,
        data: Option<ChunkPayload>,
    ) {
        // this implementation makes delete idempotent
        // it allows deleting a deleted chunk by repeatedly setting None.
        self.set_chunks
            .entry(path)
            .and_modify(|h| {
                h.insert(coord.clone(), data.clone());
            })
            .or_insert(HashMap::from([(coord, data)]));
    }

    fn get_chunk_ref(
        &self,
        path: &Path,
        coords: &ArrayIndices,
    ) -> Option<&Option<ChunkPayload>> {
        self.set_chunks.get(path).and_then(|h| h.get(coords))
    }

    fn array_chunks_iterator(
        &self,
        path: &Path,
    ) -> impl Iterator<Item = (&ArrayIndices, &Option<ChunkPayload>)> {
        match self.set_chunks.get(path) {
            None => Either::Left(iter::empty()),
            Some(h) => Either::Right(h.iter()),
        }
    }

    fn new_arrays_chunk_iterator(&self) -> impl Iterator<Item = ChunkInfo> + '_ {
        self.new_arrays.iter().flat_map(|(path, (node_id, _))| {
            self.array_chunks_iterator(path).filter_map(|(coords, payload)| {
                payload.as_ref().map(|p| ChunkInfo {
                    node: *node_id,
                    coord: coords.clone(),
                    payload: p.clone(),
                })
            })
        })
    }

    fn new_nodes(&self) -> impl Iterator<Item = &Path> {
        self.new_groups.keys().chain(self.new_arrays.keys())
    }
}

#[derive(Debug, Clone)]
pub struct DatasetBuilder {
    config: DatasetConfig,
    storage: Arc<dyn Storage + Send + Sync>,
    structure_id: Option<ObjectId>,
}

impl DatasetBuilder {
    fn new(
        storage: Arc<dyn Storage + Send + Sync>,
        structure_id: Option<ObjectId>,
    ) -> Self {
        Self { config: DatasetConfig::default(), structure_id, storage }
    }

    pub fn with_inline_threshold_bytes(&mut self, threshold: u16) -> &mut Self {
        self.config.inline_threshold_bytes = threshold;
        self
    }

    pub fn build(&self) -> Dataset {
        Dataset::new(self.config.clone(), self.storage.clone(), self.structure_id.clone())
    }
}
/// FIXME: what do we want to do with implicit groups?
///
impl Dataset {
    pub fn create(storage: Arc<dyn Storage + Send + Sync>) -> DatasetBuilder {
        DatasetBuilder::new(storage, None)
    }

    pub fn update(
        storage: Arc<dyn Storage + Send + Sync>,
        previous_version_structure_id: ObjectId,
    ) -> DatasetBuilder {
        DatasetBuilder::new(storage, Some(previous_version_structure_id))
    }

    fn new(
        config: DatasetConfig,
        storage: Arc<dyn Storage + Send + Sync>,
        previous_version_structure_id: Option<ObjectId>,
    ) -> Self {
        Dataset {
            structure_id: previous_version_structure_id,
            config,
            storage,
            last_node_id: None,
            change_set: ChangeSet::default(),
        }
    }

    /// Add a group to the store.
    ///
    /// Calling this only records the operation in memory, doesn't have any consequence on the storage
    pub async fn add_group(&mut self, path: Path) -> Result<(), AddNodeError> {
        if self.get_node(&path).await.is_err() {
            let id = self.reserve_node_id().await;
            self.change_set.add_group(path.clone(), id);
            Ok(())
        } else {
            Err(AddNodeError::AlreadyExists(path))
        }
    }

    pub async fn delete_group(&mut self, path: Path) -> Result<(), DeleteNodeError> {
        let node = self
            .get_node(&path)
            .await
            .map_err(|_| DeleteNodeError::NotFound(path.clone()))?;

        match node.node_data {
            NodeData::Group => {
                self.change_set.delete_group(path, node.id)?;
                Ok(())
            }
            NodeData::Array(_, _) => Err(DeleteNodeError::NotAGroup(path)),
        }
    }

    /// Add an array to the store.
    ///
    /// Calling this only records the operation in memory, doesn't have any consequence on the storage
    pub async fn add_array(
        &mut self,
        path: Path,
        metadata: ZarrArrayMetadata,
    ) -> Result<(), AddNodeError> {
        if self.get_node(&path).await.is_err() {
            let id = self.reserve_node_id().await;
            self.change_set.add_array(path, id, metadata);
            Ok(())
        } else {
            Err(AddNodeError::AlreadyExists(path))
        }
    }

    // Updates an array Zarr metadata
    ///
    /// Calling this only records the operation in memory, doesn't have any consequence on the storage
    pub async fn update_array(
        &mut self,
        path: Path,
        metadata: ZarrArrayMetadata,
    ) -> Result<(), UpdateNodeError> {
        match self
            .get_node(&path)
            .await
            .map_err(|_| UpdateNodeError::NotFound(path.clone()))?
        {
            NodeStructure { node_data: NodeData::Array(..), .. } => {
                self.change_set.update_array(path, metadata);
                Ok(())
            }
            _ => Err(UpdateNodeError::NotAnArray(path)),
        }
    }

    pub async fn delete_array(&mut self, path: Path) -> Result<(), DeleteNodeError> {
        // TODO: add a cheaper `get_node_id_and_type`?
        let node = self
            .get_node(&path)
            .await
            .map_err(|_| DeleteNodeError::NotFound(path.clone()))?;

        match node.node_data {
            NodeData::Array(_, _) => {
                self.change_set.delete_array(path, node.id)?;
                Ok(())
            }
            NodeData::Group => Err(DeleteNodeError::NotAnArray(path)),
        }
    }

    /// Record the write or delete of user attributes to array or group
    pub async fn set_user_attributes(
        &mut self,
        path: Path,
        atts: Option<UserAttributes>,
    ) -> Result<(), UpdateNodeError> {
        self.get_node(&path)
            .await
            .map_err(|_| UpdateNodeError::NotFound(path.clone()))?;
        self.change_set.update_user_attributes(path, atts);
        Ok(())
    }

    // Record the write, referenceing or delete of a chunk
    //
    // Caller has to write the chunk before calling this.
    pub async fn set_chunk_ref(
        &mut self,
        path: Path,
        coord: ArrayIndices,
        data: Option<ChunkPayload>,
    ) -> Result<(), UpdateNodeError> {
        match self.get_node(&path).await {
            Ok(NodeStructure { node_data: NodeData::Array(..), .. }) => {
                self.change_set.set_chunk_ref(path, coord, data);
                Ok(())
            }
            Err(GetNodeError::NotFound(path)) => Err(UpdateNodeError::NotFound(path)),
            _ => Err(UpdateNodeError::NotAnArray(path)),
        }
    }

    async fn compute_last_node_id(&self) -> NodeId {
        // FIXME: errors
        match &self.structure_id {
            None => 0,
            Some(id) => self
                .storage
                .fetch_structure(id)
                .await
                // FIXME: bubble up the error
                .ok()
                .and_then(|structure| structure.iter().max_by_key(|s| s.id))
                .map_or(0, |node| node.id),
        }
    }

    async fn reserve_node_id(&mut self) -> NodeId {
        let last = self.last_node_id.unwrap_or(self.compute_last_node_id().await);
        let new = last + 1;
        self.last_node_id = Some(new);
        new
    }

    // FIXME: add list, moves

    pub async fn get_node(&self, path: &Path) -> Result<NodeStructure, GetNodeError> {
        // We need to look for nodes in self.change_set and the structure file
        let new_node = self.get_new_node(path);
        if new_node.is_err() {
            if self.change_set.deleted_groups.contains_key(path)
                || self.change_set.deleted_arrays.contains_key(path)
            {
                Err(GetNodeError::NotFound(path.clone()))
            } else {
                self.get_existing_node(path).await
            }
        } else {
            new_node
        }
    }

    pub async fn get_array(&self, path: &Path) -> Result<NodeStructure, GetNodeError> {
        match self.get_node(path).await {
            res @ Ok(NodeStructure { node_data: NodeData::Array(..), .. }) => res,
            Ok(NodeStructure { node_data: NodeData::Group, .. }) => {
                Err(GetNodeError::NotFound(path.clone()))
            }
            other => other,
        }
    }

    pub async fn get_group(&self, path: &Path) -> Result<NodeStructure, GetNodeError> {
        match self.get_node(path).await {
            res @ Ok(NodeStructure { node_data: NodeData::Group, .. }) => res,
            Ok(NodeStructure { node_data: NodeData::Array(..), .. }) => {
                Err(GetNodeError::NotFound(path.clone()))
            }
            other => other,
        }
    }

    async fn get_existing_node(
        &self,
        path: &Path,
    ) -> Result<NodeStructure, GetNodeError> {
        // An existing node is one that is present in a structure file on storage
        let structure_id =
            self.structure_id.as_ref().ok_or(GetNodeError::NotFound(path.clone()))?;
        let structure = self.storage.fetch_structure(structure_id).await?;

        let session_atts = self
            .change_set
            .get_user_attributes(path)
            .cloned()
            .map(|a| a.map(UserAttributesStructure::Inline));
        let res = structure.get_node(path).ok_or(GetNodeError::NotFound(path.clone()))?;
        let res = NodeStructure {
            user_attributes: session_atts.unwrap_or(res.user_attributes),
            ..res
        };
        if let Some(session_meta) =
            self.change_set.get_updated_zarr_metadata(path).cloned()
        {
            if let NodeData::Array(_, manifests) = res.node_data {
                Ok(NodeStructure {
                    node_data: NodeData::Array(session_meta, manifests),
                    ..res
                })
            } else {
                Ok(res)
            }
        } else {
            Ok(res)
        }
    }

    fn get_new_node(&self, path: &Path) -> Result<NodeStructure, GetNodeError> {
        self.get_new_array(path).or_else(|_| self.get_new_group(path))
    }

    fn get_new_array(&self, path: &Path) -> Result<NodeStructure, GetNodeError> {
        self.change_set
            .get_array(path)
            .ok()
            .map(|(id, meta)| {
                let meta = self
                    .change_set
                    .get_updated_zarr_metadata(path)
                    .unwrap_or(meta)
                    .clone();
                let atts = self.change_set.get_user_attributes(path).cloned();
                NodeStructure {
                    id: *id,
                    path: path.clone(),
                    user_attributes: atts.flatten().map(UserAttributesStructure::Inline),
                    // We put no manifests in new arrays, see get_chunk_ref to understand how chunks get
                    // fetched for those arrays
                    node_data: NodeData::Array(meta.clone(), vec![]),
                }
            })
            .ok_or(GetNodeError::NotFound(path.clone()))
    }

    fn get_new_group(&self, path: &Path) -> Result<NodeStructure, GetNodeError> {
        self.change_set
            .get_group(path)
            .map(|id| {
                let atts = self.change_set.get_user_attributes(path).cloned();
                NodeStructure {
                    id: *id,
                    path: path.clone(),
                    user_attributes: atts.flatten().map(UserAttributesStructure::Inline),
                    node_data: NodeData::Group,
                }
            })
            .ok_or(GetNodeError::NotFound(path.clone()))
    }

    pub async fn get_chunk_ref(
        &self,
        path: &Path,
        coords: &ArrayIndices,
    ) -> Option<ChunkPayload> {
        // FIXME: better error type
        let node = self.get_node(path).await.ok()?;
        match node.node_data {
            NodeData::Group => None,
            NodeData::Array(_, manifests) => {
                // check the chunks modified in this session first
                // TODO: I hate rust forces me to clone to search in a hashmap. How to do better?
                let session_chunk = self.change_set.get_chunk_ref(path, coords).cloned();
                // If session_chunk is not None we have to return it, because is the update the
                // user made in the current session
                // If session_chunk == None, user hasn't modified the chunk in this session and we
                // need to fallback to fetching the manifests
                session_chunk
                    .unwrap_or(self.get_old_chunk(manifests.as_slice(), coords).await)
            }
        }
    }

    pub async fn get_chunk(&self, path: &Path, coords: &ArrayIndices) -> Option<Bytes> {
        match self.get_chunk_ref(path, coords).await? {
            ChunkPayload::Ref(ChunkRef { id, .. }) => {
                // FIXME: handle error
                // TODO: we don't have a way to distinguish if we want to pass a range or not
                self.storage.fetch_chunk(&id, &None).await.ok()
            }
            ChunkPayload::Inline(bytes) => Some(bytes),
            //FIXME: implement virtual fetch
            ChunkPayload::Virtual(_) => todo!(),
        }
    }

    pub async fn set_chunk(
        &mut self,
        path: &Path,
        coord: &ArrayIndices,
        data: Bytes,
    ) -> Result<(), UpdateNodeError> {
        match self.get_array(path).await {
            Ok(_) => {
                let payload = if data.len() > self.config.inline_threshold_bytes as usize
                {
                    new_materialized_chunk(self.storage.as_ref(), data).await?
                } else {
                    new_inline_chunk(data)
                };
                self.change_set.set_chunk_ref(path.clone(), coord.clone(), Some(payload));
                Ok(())
            }
            Err(_) => Err(UpdateNodeError::NotFound(path.clone())),
        }
    }

    async fn get_old_chunk(
        &self,
        manifests: &[ManifestRef],
        coords: &ArrayIndices,
    ) -> Option<ChunkPayload> {
        // FIXME: use manifest extents
        for manifest in manifests {
            let manifest_structure =
                self.storage.fetch_manifests(&manifest.object_id).await.ok()?;
            if let Some(payload) = manifest_structure
                .get_chunk_info(coords, &manifest.location)
                .map(|info| info.payload)
            {
                return Some(payload);
            }
        }
        None
    }

    async fn updated_chunk_iterator(&self) -> impl Stream<Item = ChunkInfo> + '_ {
        match self.structure_id.as_ref() {
            None => futures::future::Either::Left(futures::stream::empty()),
            Some(structure_id) => {
                // FIXME: error handling
                let structure = self.storage.fetch_structure(structure_id).await.unwrap();
                let nodes = futures::stream::iter(structure.iter_arc());
                futures::future::Either::Right(
                    nodes
                        .then(move |node| async move {
                            self.node_chunk_iterator(node).await
                        })
                        .flatten(),
                )
            }
        }
    }

    async fn node_chunk_iterator(
        &self,
        node: NodeStructure,
    ) -> impl Stream<Item = ChunkInfo> + '_ {
        match node.node_data {
            NodeData::Group => futures::future::Either::Left(futures::stream::empty()),
            NodeData::Array(_, manifests) => {
                let new_chunk_indices: Box<HashSet<&ArrayIndices>> = Box::new(
                    self.change_set
                        .array_chunks_iterator(&node.path)
                        .map(|(idx, _)| idx)
                        .collect(),
                );

                let new_chunks = self
                    .change_set
                    .array_chunks_iterator(&node.path)
                    .filter_map(move |(idx, payload)| {
                        payload.as_ref().map(|payload| ChunkInfo {
                            node: node.id,
                            coord: idx.clone(),
                            payload: payload.clone(),
                        })
                    });

                futures::future::Either::Right(
                    futures::stream::iter(new_chunks).chain(
                        futures::stream::iter(manifests)
                            .then(move |manifest_ref| {
                                let path = node.path.clone();
                                let new_chunk_indices = new_chunk_indices.clone();
                                async move {
                                    let manifest = self
                                        .storage
                                        .fetch_manifests(&manifest_ref.object_id)
                                        .await
                                        .unwrap();

                                    let old_chunks = manifest
                                        .iter(
                                            Some(manifest_ref.location.0),
                                            Some(manifest_ref.location.1),
                                        )
                                        .filter(move |c| {
                                            !new_chunk_indices.contains(&c.coord)
                                        });

                                    let old_chunks = self
                                        .update_existing_chunks(path.clone(), old_chunks);
                                    //FIXME: error handling
                                    futures::stream::iter(old_chunks)
                                }
                            })
                            .flatten(),
                    ),
                )
            }
        }
    }

    fn update_existing_chunks<'a>(
        &'a self,
        path: Path,
        chunks: impl Iterator<Item = ChunkInfo> + 'a,
    ) -> impl Iterator<Item = ChunkInfo> + 'a {
        chunks.filter_map(move |chunk| {
            match self.change_set.get_chunk_ref(&path, &chunk.coord) {
                None => Some(chunk),
                Some(new_payload) => {
                    new_payload.clone().map(|pl| ChunkInfo { payload: pl, ..chunk })
                }
            }
        })
    }

    async fn updated_existing_nodes<'a>(
        &'a self,
        manifest_id: &'a ObjectId,
        manifest_tracker: Option<&'a TableRegionTracker>,
    ) -> impl Iterator<Item = NodeStructure> + 'a {
        // TODO: solve this duplication, there is always the possibility of this being the first
        // version
        match &self.structure_id {
            None => Either::Left(iter::empty()),
            Some(id) => Either::Right(
                self.storage
                    .fetch_structure(id)
                    .await
                    // FIXME: bubble up the error
                    .unwrap()
                    .iter_arc()
                    .map(move |node| {
                        let region = manifest_tracker.and_then(|t| t.region(node.id));
                        let new_manifests = region.map(|r| {
                            if r.0 == r.1 {
                                vec![]
                            } else {
                                vec![ManifestRef {
                                    object_id: manifest_id.clone(),
                                    location: r.clone(),
                                    flags: Flags(),
                                    extents: ManifestExtents(vec![]),
                                }]
                            }
                        });
                        self.update_existing_node(node, new_manifests)
                    }),
            ),
        }
    }

    fn new_nodes<'a>(
        &'a self,
        manifest_id: &'a ObjectId,
        manifest_tracker: Option<&'a TableRegionTracker>,
    ) -> impl Iterator<Item = NodeStructure> + 'a {
        // FIXME: unwrap
        self.change_set.new_nodes().map(move |path| {
            let node = self.get_new_node(path).unwrap();
            match node.node_data {
                NodeData::Group => node,
                NodeData::Array(meta, _no_manifests_yet) => {
                    let region = manifest_tracker.and_then(|t| t.region(node.id));
                    let new_manifests = region.map(|r| {
                        if r.0 == r.1 {
                            vec![]
                        } else {
                            vec![ManifestRef {
                                object_id: manifest_id.clone(),
                                location: r.clone(),
                                flags: Flags(),
                                extents: ManifestExtents(vec![]),
                            }]
                        }
                    });
                    NodeStructure {
                        node_data: NodeData::Array(
                            meta,
                            new_manifests.unwrap_or_default(),
                        ),
                        ..node
                    }
                }
            }
        })
    }

    async fn updated_nodes<'a>(
        &'a self,
        manifest_id: &'a ObjectId,
        manifest_tracker: Option<&'a TableRegionTracker>,
    ) -> impl Iterator<Item = NodeStructure> + 'a {
        self.updated_existing_nodes(manifest_id, manifest_tracker)
            .await
            .chain(self.new_nodes(manifest_id, manifest_tracker))
    }

    fn update_existing_node(
        &self,
        node: NodeStructure,
        new_manifests: Option<Vec<ManifestRef>>,
    ) -> NodeStructure {
        let session_atts = self
            .change_set
            .get_user_attributes(&node.path)
            .cloned()
            .map(|a| a.map(UserAttributesStructure::Inline));
        let new_atts = session_atts.unwrap_or(node.user_attributes);
        match node.node_data {
            NodeData::Group => NodeStructure { user_attributes: new_atts, ..node },
            NodeData::Array(old_zarr_meta, _) => {
                let new_zarr_meta = self
                    .change_set
                    .get_updated_zarr_metadata(&node.path)
                    .cloned()
                    .unwrap_or(old_zarr_meta);

                NodeStructure {
                    // FIXME: bad option type, change
                    node_data: NodeData::Array(
                        new_zarr_meta,
                        new_manifests.unwrap_or_default(),
                    ),
                    user_attributes: new_atts,
                    ..node
                }
            }
        }
    }

    pub async fn list_nodes(&self) -> impl Iterator<Item = NodeStructure> + '_ {
        self.updated_nodes(&ObjectId::FAKE, None).await
    }

    /// After changes to the dasate have been made, this generates and writes to `Storage` the updated datastructures.
    ///
    /// After calling this, changes are reset and the [Dataset] can continue to be used for further
    /// changes.
    ///
    /// Returns the `ObjectId` of the new structure file. It's the callers responsibility to commit
    /// this id change.
    pub async fn flush(&mut self) -> Result<ObjectId, FlushError> {
        let mut region_tracker = TableRegionTracker::default();
        let existing_array_chunks = self.updated_chunk_iterator().await;
        let new_array_chunks =
            futures::stream::iter(self.change_set.new_arrays_chunk_iterator());
        let all_chunks = existing_array_chunks.chain(new_array_chunks).map(|chunk| {
            region_tracker.update(&chunk);
            chunk
        });
        let new_manifest = mk_manifests_table(all_chunks).await;
        let new_manifest_id = ObjectId::random();
        self.storage
            .write_manifests(new_manifest_id.clone(), Arc::new(new_manifest))
            .await?;

        let all_nodes = self.updated_nodes(&new_manifest_id, Some(&region_tracker)).await;
        let new_structure = mk_structure_table(all_nodes);
        let new_structure_id = ObjectId::random();
        self.storage
            .write_structure(new_structure_id.clone(), Arc::new(new_structure))
            .await?;

        self.structure_id = Some(new_structure_id.clone());
        self.change_set = ChangeSet::default();
        Ok(new_structure_id)
    }
}

#[derive(Debug, Clone, Default)]
struct TableRegionTracker(HashMap<NodeId, TableRegion>, u32);

impl TableRegionTracker {
    fn update(&mut self, chunk: &ChunkInfo) {
        self.0
            .entry(chunk.node)
            .and_modify(|tr| tr.1 = self.1 + 1)
            .or_insert(TableRegion(self.1, self.1 + 1));
        self.1 += 1;
    }

    fn region(&self, node: NodeId) -> Option<&TableRegion> {
        self.0.get(&node)
    }
}

#[derive(Debug, Error)]
pub enum FlushError {
    #[error("no changes made to the dataset")]
    NoChangesToFlush,
    #[error("error contacting storage")]
    StorageError(#[from] StorageError),
}

async fn new_materialized_chunk(
    storage: &(dyn Storage + Send + Sync),
    data: Bytes,
) -> Result<ChunkPayload, UpdateNodeError> {
    let new_id = ObjectId::random();
    storage
        .write_chunk(new_id.clone(), data.clone())
        .await
        .map_err(UpdateNodeError::StorageError)?;
    Ok(ChunkPayload::Ref(ChunkRef { id: new_id, offset: 0, length: data.len() as u64 }))
}

fn new_inline_chunk(data: Bytes) -> ChunkPayload {
    ChunkPayload::Inline(data)
}

#[cfg(test)]
mod tests {

    use std::{error::Error, num::NonZeroU64, path::PathBuf};

    use crate::{
        manifest::mk_manifests_table, storage::InMemoryStorage, strategies::*,
        structure::mk_structure_table, ChunkInfo, ChunkKeyEncoding, ChunkRef, ChunkShape,
        Codec, DataType, FillValue, Flags, ManifestExtents, StorageTransformer,
        TableRegion,
    };

    use super::*;
    use itertools::Itertools;
    use pretty_assertions::assert_eq;
    use proptest::prelude::{prop_assert, prop_assert_eq};
    use test_strategy::proptest;

    #[proptest(async = "tokio")]
    async fn test_add_delete_group(
        #[strategy(node_paths())] path: Path,
        #[strategy(empty_datasets())] mut dataset: Dataset,
    ) {
        // getting any path from an empty dataset must fail
        prop_assert!(dataset.get_node(&path).await.is_err());

        // adding a new group must succeed
        prop_assert!(dataset.add_group(path.clone()).await.is_ok());

        // Getting a group just added must succeed
        let node = dataset.get_node(&path).await;
        prop_assert!(node.is_ok());

        // Getting the group twice must be equal
        prop_assert_eq!(node.unwrap(), dataset.get_node(&path).await.unwrap());

        // adding an existing group fails
        prop_assert_eq!(
            dataset.add_group(path.clone()).await.unwrap_err(),
            AddNodeError::AlreadyExists(path.clone())
        );

        // deleting the added group must succeed
        prop_assert!(dataset.delete_group(path.clone()).await.is_ok());

        // deleting twice must fail
        prop_assert_eq!(
            dataset.delete_group(path.clone()).await.unwrap_err(),
            DeleteNodeError::NotFound(path.clone())
        );

        // getting a deleted group must fail
        prop_assert!(dataset.get_node(&path).await.is_err());

        // adding again must succeed
        prop_assert!(dataset.add_group(path.clone()).await.is_ok());

        // deleting again must succeed
        prop_assert!(dataset.delete_group(path.clone()).await.is_ok());
    }

    #[proptest(async = "tokio")]
    async fn test_add_delete_array(
        #[strategy(node_paths())] path: Path,
        #[strategy(zarr_array_metadata())] metadata: ZarrArrayMetadata,
        #[strategy(empty_datasets())] mut dataset: Dataset,
    ) {
        // new array must always succeed
        prop_assert!(dataset.add_array(path.clone(), metadata.clone()).await.is_ok());

        // adding to the same path must fail
        prop_assert!(dataset.add_array(path.clone(), metadata.clone()).await.is_err());

        // first delete must succeed
        prop_assert!(dataset.delete_array(path.clone()).await.is_ok());

        // deleting twice must fail
        prop_assert_eq!(
            dataset.delete_array(path.clone()).await.unwrap_err(),
            DeleteNodeError::NotFound(path.clone())
        );

        // adding again must succeed
        prop_assert!(dataset.add_array(path.clone(), metadata.clone()).await.is_ok());

        // deleting again must succeed
        prop_assert!(dataset.delete_array(path.clone()).await.is_ok());
    }

    #[proptest(async = "tokio")]
    async fn test_add_array_group_clash(
        #[strategy(node_paths())] path: Path,
        #[strategy(zarr_array_metadata())] metadata: ZarrArrayMetadata,
        #[strategy(empty_datasets())] mut dataset: Dataset,
    ) {
        // adding a group at an existing array node must fail
        prop_assert!(dataset.add_array(path.clone(), metadata.clone()).await.is_ok());
        prop_assert_eq!(
            dataset.add_group(path.clone()).await.unwrap_err(),
            AddNodeError::AlreadyExists(path.clone())
        );
        prop_assert_eq!(
            dataset.delete_group(path.clone()).await.unwrap_err(),
            DeleteNodeError::NotAGroup(path.clone())
        );
        prop_assert!(dataset.delete_array(path.clone()).await.is_ok());

        // adding an array at an existing group node must fail
        prop_assert!(dataset.add_group(path.clone()).await.is_ok());
        prop_assert_eq!(
            dataset.add_array(path.clone(), metadata.clone()).await.unwrap_err(),
            AddNodeError::AlreadyExists(path.clone())
        );
        prop_assert_eq!(
            dataset.delete_array(path.clone()).await.unwrap_err(),
            DeleteNodeError::NotAnArray(path.clone())
        );
        prop_assert!(dataset.delete_group(path.clone()).await.is_ok());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_dataset_with_updates() -> Result<(), Box<dyn Error>> {
        let storage = InMemoryStorage::new();

        let array_id = 2;
        let chunk1 = ChunkInfo {
            node: array_id,
            coord: ArrayIndices(vec![0, 0, 0]),
            payload: ChunkPayload::Ref(ChunkRef {
                id: ObjectId::random(),
                offset: 0,
                length: 4,
            }),
        };

        let chunk2 = ChunkInfo {
            node: array_id,
            coord: ArrayIndices(vec![0, 0, 1]),
            payload: ChunkPayload::Inline("hello".into()),
        };

        let manifest = Arc::new(
            mk_manifests_table(futures::stream::iter(vec![
                chunk1.clone(),
                chunk2.clone(),
            ]))
            .await,
        );
        let manifest_id = ObjectId::random();
        storage.write_manifests(manifest_id.clone(), manifest).await?;

        let zarr_meta1 = ZarrArrayMetadata {
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
        let manifest_ref = ManifestRef {
            object_id: manifest_id,
            location: TableRegion(0, 2),
            flags: Flags(),
            extents: ManifestExtents(vec![]),
        };
        let array1_path: PathBuf = "/array1".to_string().into();
        let nodes = vec![
            NodeStructure {
                path: "/".into(),
                id: 1,
                user_attributes: None,
                node_data: NodeData::Group,
            },
            NodeStructure {
                path: array1_path.clone(),
                id: array_id,
                user_attributes: Some(UserAttributesStructure::Inline(
                    UserAttributes::try_new(br#"{"foo":1}"#).unwrap(),
                )),
                node_data: NodeData::Array(zarr_meta1.clone(), vec![manifest_ref]),
            },
        ];

        let structure = Arc::new(mk_structure_table(nodes.clone()));
        let structure_id = ObjectId::random();
        storage.write_structure(structure_id.clone(), structure).await?;
        let mut ds = Dataset::update(Arc::new(storage), structure_id).build();

        // retrieve the old array node
        let node = ds.get_node(&array1_path).await;
        assert_eq!(nodes.get(1).unwrap(), node.as_ref().unwrap());

        let group_name = "/tbd-group".to_string();
        ds.add_group(group_name.clone().into()).await?;
        ds.delete_group(group_name.clone().into()).await?;
        assert!(ds.delete_group(group_name.clone().into()).await.is_err());
        assert!(ds.get_node(&group_name.into()).await.is_err());

        // add a new array and retrieve its node
        ds.add_group("/group".to_string().into()).await?;

        let zarr_meta2 = ZarrArrayMetadata {
            shape: vec![3],
            data_type: DataType::Int32,
            chunk_shape: ChunkShape(vec![NonZeroU64::new(2).unwrap()]),
            chunk_key_encoding: ChunkKeyEncoding::Slash,
            fill_value: FillValue::Int32(0),
            codecs: vec![Codec { name: "mycodec".to_string(), configuration: None }],
            storage_transformers: Some(vec![StorageTransformer {
                name: "mytransformer".to_string(),
                configuration: None,
            }]),
            dimension_names: Some(vec![Some("t".to_string())]),
        };

        let new_array_path: PathBuf = "/group/array2".to_string().into();
        ds.add_array(new_array_path.clone(), zarr_meta2.clone()).await?;

        ds.delete_array(new_array_path.clone()).await?;
        // Delete a non-existent array
        assert!(ds.delete_array(new_array_path.clone()).await.is_err());
        assert!(ds.delete_array(new_array_path.clone()).await.is_err());
        assert!(ds.get_node(&new_array_path.clone()).await.is_err());

        ds.add_array(new_array_path.clone(), zarr_meta2.clone()).await?;

        let node = ds.get_node(&new_array_path).await;
        assert_eq!(
            node.ok(),
            Some(NodeStructure {
                path: new_array_path.clone(),
                id: 6,
                user_attributes: None,
                node_data: NodeData::Array(zarr_meta2.clone(), vec![]),
            }),
        );

        // set user attributes for the new array and retrieve them
        ds.set_user_attributes(
            new_array_path.clone(),
            Some(UserAttributes::try_new(br#"{"n":42}"#).unwrap()),
        )
        .await?;
        let node = ds.get_node(&new_array_path).await;
        assert_eq!(
            node.ok(),
            Some(NodeStructure {
                path: "/group/array2".into(),
                id: 6,
                user_attributes: Some(UserAttributesStructure::Inline(
                    UserAttributes::try_new(br#"{"n":42}"#).unwrap()
                )),
                node_data: NodeData::Array(zarr_meta2.clone(), vec![]),
            }),
        );

        ds.set_chunk(
            &new_array_path,
            &ArrayIndices(vec![0]),
            Bytes::copy_from_slice(b"foo"),
        )
        .await?;

        let chunk = ds.get_chunk_ref(&new_array_path, &ArrayIndices(vec![0])).await;
        assert_eq!(chunk, Some(ChunkPayload::Inline("foo".into())));

        // retrieve a non initialized chunk of the new array
        let non_chunk = ds.get_chunk_ref(&new_array_path, &ArrayIndices(vec![1])).await;
        assert_eq!(non_chunk, None);

        // update old array use attriutes and check them
        ds.set_user_attributes(
            array1_path.clone(),
            Some(UserAttributes::try_new(br#"{"updated": true}"#).unwrap()),
        )
        .await?;
        let node = ds.get_node(&array1_path).await.unwrap();
        assert_eq!(
            node.user_attributes,
            Some(UserAttributesStructure::Inline(
                UserAttributes::try_new(br#"{"updated": true}"#).unwrap()
            ))
        );

        // update old array zarr metadata and check it
        let new_zarr_meta1 = ZarrArrayMetadata { shape: vec![2, 2, 3], ..zarr_meta1 };
        ds.update_array(array1_path.clone(), new_zarr_meta1).await?;
        let node = ds.get_node(&array1_path).await;
        if let Ok(NodeStructure {
            node_data: NodeData::Array(ZarrArrayMetadata { shape, .. }, _),
            ..
        }) = node
        {
            assert_eq!(shape, vec![2, 2, 3]);
        } else {
            panic!("Failed to update zarr metadata");
        }

        // set old array chunk and check them
        let data = Bytes::copy_from_slice(b"foo".repeat(512).as_slice());
        ds.set_chunk(&array1_path, &ArrayIndices(vec![0, 0, 0]), data.clone()).await?;

        let chunk = ds.get_chunk(&array1_path, &ArrayIndices(vec![0, 0, 0])).await;
        assert_eq!(chunk, Some(data));

        let path: Path = "/group/array2".into();
        assert!(ds.change_set.updated_attributes.contains_key(&path));
        assert!(ds.delete_array(path.clone()).await.is_ok());
        assert!(!ds.change_set.updated_attributes.contains_key(&path));

        Ok(())
    }

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

        change_set.add_array("foo/bar".into(), 1, zarr_meta.clone());
        change_set.add_array("foo/baz".into(), 2, zarr_meta);
        assert_eq!(None, change_set.new_arrays_chunk_iterator().next());

        change_set.set_chunk_ref("foo/bar".into(), ArrayIndices(vec![0, 1]), None);
        assert_eq!(None, change_set.new_arrays_chunk_iterator().next());

        change_set.set_chunk_ref(
            "foo/bar".into(),
            ArrayIndices(vec![1, 0]),
            Some(ChunkPayload::Inline("bar1".into())),
        );
        change_set.set_chunk_ref(
            "foo/bar".into(),
            ArrayIndices(vec![1, 1]),
            Some(ChunkPayload::Inline("bar2".into())),
        );
        change_set.set_chunk_ref(
            "foo/baz".into(),
            ArrayIndices(vec![0]),
            Some(ChunkPayload::Inline("baz1".into())),
        );
        change_set.set_chunk_ref(
            "foo/baz".into(),
            ArrayIndices(vec![1]),
            Some(ChunkPayload::Inline("baz2".into())),
        );

        {
            let all_chunks: Vec<_> = change_set
                .new_arrays_chunk_iterator()
                .sorted_by_key(|c| c.coord.clone())
                .collect();
            let expected_chunks: Vec<_> = [
                ChunkInfo {
                    node: 2,
                    coord: ArrayIndices(vec![0]),
                    payload: ChunkPayload::Inline("baz1".into()),
                },
                ChunkInfo {
                    node: 2,
                    coord: ArrayIndices(vec![1]),
                    payload: ChunkPayload::Inline("baz2".into()),
                },
                ChunkInfo {
                    node: 1,
                    coord: ArrayIndices(vec![1, 0]),
                    payload: ChunkPayload::Inline("bar1".into()),
                },
                ChunkInfo {
                    node: 1,
                    coord: ArrayIndices(vec![1, 1]),
                    payload: ChunkPayload::Inline("bar2".into()),
                },
            ]
            .into();
            assert_eq!(all_chunks, expected_chunks);
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_dataset_with_updates_and_writes() -> Result<(), Box<dyn Error>> {
        let storage: Arc<dyn Storage + Send + Sync> = Arc::new(InMemoryStorage::new());
        let mut ds = Dataset::create(Arc::clone(&storage)).build();

        // add a new array and retrieve its node
        ds.add_group("/".into()).await?;
        let structure_id = ds.flush().await?;

        assert_eq!(Some(structure_id), ds.structure_id);
        assert_eq!(
            ds.get_node(&"/".into()).await.ok(),
            Some(NodeStructure {
                id: 1,
                path: "/".into(),
                user_attributes: None,
                node_data: NodeData::Group
            })
        );
        ds.add_group("/group".into()).await?;
        let _structure_id = ds.flush().await?;
        assert_eq!(
            ds.get_node(&"/".into()).await.ok(),
            Some(NodeStructure {
                id: 1,
                path: "/".into(),
                user_attributes: None,
                node_data: NodeData::Group
            })
        );
        assert_eq!(
            ds.get_node(&"/group".into()).await.ok(),
            Some(NodeStructure {
                id: 2,
                path: "/group".into(),
                user_attributes: None,
                node_data: NodeData::Group
            })
        );
        let zarr_meta = ZarrArrayMetadata {
            shape: vec![1, 1, 2],
            data_type: DataType::Int32,
            chunk_shape: ChunkShape(vec![NonZeroU64::new(2).unwrap()]),
            chunk_key_encoding: ChunkKeyEncoding::Slash,
            fill_value: FillValue::Int32(0),
            codecs: vec![Codec { name: "mycodec".to_string(), configuration: None }],
            storage_transformers: Some(vec![StorageTransformer {
                name: "mytransformer".to_string(),
                configuration: None,
            }]),
            dimension_names: Some(vec![Some("t".to_string())]),
        };

        let new_array_path: PathBuf = "/group/array1".to_string().into();
        ds.add_array(new_array_path.clone(), zarr_meta.clone()).await?;

        // wo commit to test the case of a chunkless array
        let _structure_id = ds.flush().await?;

        // we set a chunk in a new array
        ds.set_chunk_ref(
            new_array_path.clone(),
            ArrayIndices(vec![0, 0, 0]),
            Some(ChunkPayload::Inline("hello".into())),
        )
        .await?;

        let _structure_id = ds.flush().await?;
        assert_eq!(
            ds.get_node(&"/".into()).await.ok(),
            Some(NodeStructure {
                id: 1,
                path: "/".into(),
                user_attributes: None,
                node_data: NodeData::Group
            })
        );
        assert_eq!(
            ds.get_node(&"/group".into()).await.ok(),
            Some(NodeStructure {
                id: 2,
                path: "/group".into(),
                user_attributes: None,
                node_data: NodeData::Group
            })
        );
        assert!(matches!(
            ds.get_node(&new_array_path).await.ok(),
            Some(NodeStructure {
                id: 3,
                path,
                user_attributes: None,
                node_data: NodeData::Array(meta, manifests)
            }) if path == new_array_path && meta == zarr_meta.clone() && manifests.len() == 1
        ));
        assert_eq!(
            ds.get_chunk_ref(&new_array_path, &ArrayIndices(vec![0, 0, 0])).await,
            Some(ChunkPayload::Inline("hello".into()))
        );

        // we modify a chunk in an existing array
        ds.set_chunk_ref(
            new_array_path.clone(),
            ArrayIndices(vec![0, 0, 0]),
            Some(ChunkPayload::Inline("bye".into())),
        )
        .await?;

        // we add a new chunk in an existing array
        ds.set_chunk_ref(
            new_array_path.clone(),
            ArrayIndices(vec![0, 0, 1]),
            Some(ChunkPayload::Inline("new chunk".into())),
        )
        .await?;

        let previous_structure_id = ds.flush().await?;
        assert_eq!(
            ds.get_chunk_ref(&new_array_path, &ArrayIndices(vec![0, 0, 0])).await,
            Some(ChunkPayload::Inline("bye".into()))
        );
        assert_eq!(
            ds.get_chunk_ref(&new_array_path, &ArrayIndices(vec![0, 0, 1])).await,
            Some(ChunkPayload::Inline("new chunk".into()))
        );

        // we delete a chunk
        ds.set_chunk_ref(new_array_path.clone(), ArrayIndices(vec![0, 0, 1]), None)
            .await?;

        let new_meta = ZarrArrayMetadata { shape: vec![1, 1, 1], ..zarr_meta };
        // we change zarr metadata
        ds.update_array(new_array_path.clone(), new_meta.clone()).await?;

        // we change user attributes metadata
        ds.set_user_attributes(
            new_array_path.clone(),
            Some(UserAttributes::try_new(br#"{"foo":42}"#).unwrap()),
        )
        .await?;

        let structure_id = ds.flush().await?;
        let ds = Dataset::update(Arc::clone(&storage), structure_id).build();

        assert_eq!(
            ds.get_chunk_ref(&new_array_path, &ArrayIndices(vec![0, 0, 0])).await,
            Some(ChunkPayload::Inline("bye".into()))
        );
        assert_eq!(
            ds.get_chunk_ref(&new_array_path, &ArrayIndices(vec![0, 0, 1])).await,
            None
        );
        assert!(matches!(
            ds.get_node(&new_array_path).await.ok(),
            Some(NodeStructure {
                id: 3,
                path,
                user_attributes: Some(atts),
                node_data: NodeData::Array(meta, manifests)
            }) if path == new_array_path && meta == new_meta.clone() &&
                    manifests.len() == 1 &&
                    atts == UserAttributesStructure::Inline(UserAttributes::try_new(br#"{"foo":42}"#).unwrap())
        ));

        //test the previous version is still alive
        let ds = Dataset::update(Arc::clone(&storage), previous_structure_id).build();
        assert_eq!(
            ds.get_chunk_ref(&new_array_path, &ArrayIndices(vec![0, 0, 0])).await,
            Some(ChunkPayload::Inline("bye".into()))
        );
        assert_eq!(
            ds.get_chunk_ref(&new_array_path, &ArrayIndices(vec![0, 0, 1])).await,
            Some(ChunkPayload::Inline("new chunk".into()))
        );

        Ok(())
    }

    #[cfg(test)]
    mod state_machine_test {
        use crate::storage::InMemoryStorage;
        use crate::{NodeData, Path, ZarrArrayMetadata};
        use futures::Future;
        // use futures::Future;
        use proptest::prelude::*;
        use proptest::sample;
        use proptest::strategy::{BoxedStrategy, Just};
        use proptest_state_machine::{
            prop_state_machine, ReferenceStateMachine, StateMachineTest,
        };
        use std::collections::HashMap;
        use std::fmt::Debug;
        use std::sync::Arc;
        use tokio::runtime::Runtime;

        use crate::Dataset;
        use proptest::test_runner::Config;

        use super::{node_paths, zarr_array_metadata};

        #[derive(Clone, Debug)]
        enum DatasetTransition {
            AddArray(Path, ZarrArrayMetadata),
            UpdateArray(Path, ZarrArrayMetadata),
            DeleteArray(Option<Path>),
            AddGroup(Path),
            DeleteGroup(Option<Path>),
        }

        /// An empty type used for the `ReferenceStateMachine` implementation.
        struct DatasetStateMachine;

        #[derive(Clone, Default, Debug)]
        struct DatasetModel {
            arrays: HashMap<Path, ZarrArrayMetadata>,
            groups: Vec<Path>,
        }

        impl ReferenceStateMachine for DatasetStateMachine {
            type State = DatasetModel;
            type Transition = DatasetTransition;

            fn init_state() -> BoxedStrategy<Self::State> {
                dbg!("======> New state");
                Just(Default::default()).boxed()
            }

            fn transitions(state: &Self::State) -> BoxedStrategy<Self::Transition> {
                dbg!("generating transitions", state.clone());

                // proptest-state-machine generates the transitions first,
                // *then* applies the preconditions to decide if that transition is valid.
                // that means we have to make sure that we are not sampling from
                // parts of the State that are empty.
                // i.e. we need to apply a precondition here :/
                let delete_arrays = {
                    if !state.arrays.is_empty() {
                        let array_keys: Vec<Path> =
                            state.arrays.keys().cloned().collect();
                        sample::select(array_keys)
                            .prop_map(|p| DatasetTransition::DeleteArray(Some(p)))
                            .boxed()
                    } else {
                        Just(DatasetTransition::DeleteArray(None)).boxed()
                    }
                };

                let delete_groups = {
                    if !state.groups.is_empty() {
                        sample::select(state.groups.clone())
                            .prop_map(|p| DatasetTransition::DeleteGroup(Some(p)))
                            .boxed()
                    } else {
                        Just(DatasetTransition::DeleteGroup(None)).boxed()
                    }
                };

                prop_oneof![
                    (node_paths(), zarr_array_metadata())
                        .prop_map(|(a, b)| DatasetTransition::AddArray(a, b)),
                    (node_paths(), zarr_array_metadata())
                        .prop_map(|(a, b)| DatasetTransition::UpdateArray(a, b)),
                    delete_arrays,
                    node_paths().prop_map(DatasetTransition::AddGroup),
                    delete_groups,
                ]
                .boxed()
            }

            fn apply(
                mut state: Self::State,
                transition: &Self::Transition,
            ) -> Self::State {
                dbg!("apply");
                match transition {
                    // Array ops
                    DatasetTransition::AddArray(path, metadata) => {
                        let res = state.arrays.insert(path.clone(), metadata.clone());
                        assert!(res.is_none());
                    }
                    DatasetTransition::UpdateArray(path, metadata) => {
                        state
                            .arrays
                            .insert(path.clone(), metadata.clone())
                            .expect("(postcondition) insertion failed");
                    }
                    DatasetTransition::DeleteArray(path) => {
                        let path = path.clone().unwrap();
                        state
                            .arrays
                            .remove(&path)
                            .expect("(postcondition) deletion failed");
                    }

                    // Group ops
                    DatasetTransition::AddGroup(path) => {
                        state.groups.push(path.clone());
                        // TODO: postcondition
                    }
                    DatasetTransition::DeleteGroup(Some(path)) => {
                        let index =
                            state.groups.iter().position(|x| x == path).expect(
                                "Attempting to delete a non-existent path: {path}",
                            );
                        state.groups.swap_remove(index);
                    }
                    _ => panic!(),
                }
                state
            }

            fn preconditions(state: &Self::State, transition: &Self::Transition) -> bool {
                dbg!("in preconditions...");
                match transition {
                    DatasetTransition::AddArray(path, _) => {
                        !state.arrays.contains_key(path) && !state.groups.contains(path)
                    }
                    DatasetTransition::UpdateArray(path, _) => {
                        state.arrays.contains_key(path)
                    }
                    DatasetTransition::DeleteArray(path) => path.is_some(),
                    DatasetTransition::AddGroup(path) => {
                        !state.arrays.contains_key(path) && !state.groups.contains(path)
                    }
                    DatasetTransition::DeleteGroup(p) => p.is_some(),
                }
            }
        }

        struct TestDataset {
            dataset: Dataset,
            runtime: Runtime,
        }
        trait BlockOnUnwrap {
            fn unwrap<F, T, E>(&self, future: F) -> T
            where
                F: Future<Output = Result<T, E>>,
                E: Debug;
        }
        impl BlockOnUnwrap for Runtime {
            fn unwrap<F, T, E>(&self, future: F) -> T
            where
                F: Future<Output = Result<T, E>>,
                E: Debug,
            {
                self.block_on(future).unwrap()
            }
        }

        impl StateMachineTest for TestDataset {
            type SystemUnderTest = Self;
            type Reference = DatasetStateMachine;

            fn init_test(
                _ref_state: &<Self::Reference as ReferenceStateMachine>::State,
            ) -> Self::SystemUnderTest {
                let storage = InMemoryStorage::new();
                TestDataset {
                    dataset: Dataset::create(Arc::new(storage)).build(),
                    runtime: Runtime::new().unwrap(),
                }
            }

            fn apply(
                mut state: Self::SystemUnderTest,
                _ref_state: &<Self::Reference as ReferenceStateMachine>::State,
                transition: DatasetTransition,
            ) -> Self::SystemUnderTest {
                let runtime = &state.runtime;
                let dataset = &mut state.dataset;
                match transition {
                    DatasetTransition::AddArray(path, metadata) => {
                        runtime.unwrap(dataset.add_array(path, metadata))
                    }
                    DatasetTransition::UpdateArray(path, metadata) => {
                        runtime.unwrap(dataset.update_array(path, metadata))
                    }
                    DatasetTransition::DeleteArray(Some(path)) => {
                        runtime.unwrap(dataset.delete_array(path))
                    }
                    DatasetTransition::AddGroup(path) => {
                        runtime.unwrap(dataset.add_group(path))
                    }
                    DatasetTransition::DeleteGroup(Some(path)) => {
                        runtime.unwrap(dataset.delete_group(path))
                    }
                    _ => panic!(),
                }
                state
            }

            fn check_invariants(
                state: &Self::SystemUnderTest,
                ref_state: &<Self::Reference as ReferenceStateMachine>::State,
            ) {
                let runtime = &state.runtime;
                for (path, metadata) in ref_state.arrays.iter() {
                    let node = runtime.unwrap(state.dataset.get_array(path));
                    let actual_metadata = match node.node_data {
                        NodeData::Array(metadata, _) => Ok(metadata),
                        _ => Err("foo"),
                    }
                    .unwrap();
                    assert_eq!(metadata, &actual_metadata);
                }

                for path in ref_state.groups.iter() {
                    let node = runtime.unwrap(state.dataset.get_group(path));
                    match node.node_data {
                        NodeData::Group => Ok(()),
                        _ => Err("foo"),
                    }
                    .unwrap();
                }
            }
        }

        prop_state_machine! {
            #![proptest_config(Config {
            verbose: 1,
            .. Config::default()
        })]

        #[test]
        fn run_dataset_state_machine_test(
            // This is a macro's keyword - only `sequential` is currently supported.
            sequential
            // The number of transitions to be generated for each case. This can
            // be a single numerical value or a range as in here.
            1..20
            // Macro's boilerplate to separate the following identifier.
            =>
            // The name of the type that implements `StateMachineTest`.
            TestDataset
        );
        }
    }
}
