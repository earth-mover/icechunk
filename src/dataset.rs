use std::sync::Arc;

use crate::{
    AddNodeError, ArrayIndices, AttributesTable, ChunkPayload, Dataset, ManifestsTable, NodeData,
    NodeStructure, Path, StructureTable, UpdateNodeError, UserAttributes, ZarrArrayMetadata,
};

/// FIXME: what do we want to do with implicit groups?
///
impl Dataset {
    /// Add a group to the store.
    ///
    /// Calling this only records the operation in memory, doesn't have any consequence on the storage
    pub async fn add_group(&mut self, path: Path) -> Result<(), AddNodeError> {
        if self.get_node(&path).await.is_none() {
            self.new_groups.insert(path);
            Ok(())
        } else {
            Err(AddNodeError::AlreadyExists)
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
        if self.get_node(&path).await.is_none() {
            self.new_arrays.insert(path, metadata);
            Ok(())
        } else {
            Err(AddNodeError::AlreadyExists)
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
        match self.get_node(&path).await {
            None => Err(UpdateNodeError::NotFound),
            Some(NodeStructure {
                node_data: NodeData::Array(..),
                ..
            }) => {
                self.updated_arrays.insert(path, metadata);
                Ok(())
            }
            Some(_) => Err(UpdateNodeError::NotAnArray),
        }
    }

    /// Record the write of user attributes to array or group
    pub async fn set_user_attributes(
        &mut self,
        path: Path,
        atts: UserAttributes,
    ) -> Result<(), UpdateNodeError> {
        match self.get_node(&path).await {
            None => Err(UpdateNodeError::NotFound),
            Some(_) => {
                self.updated_attributes.insert(path, atts);
                Ok(())
            }
        }
    }

    // Record the write or referenceing of a chunk
    //
    // Caller has to write the chunk before calling this.
    pub async fn set_chunk(
        &mut self,
        path: Path,
        coord: ArrayIndices,
        data: ChunkPayload,
    ) -> Result<(), UpdateNodeError> {
        match self.get_node(&path).await {
            None => Err(UpdateNodeError::NotFound),
            Some(NodeStructure {
                node_data: NodeData::Array(..),
                ..
            }) => {
                self.set_chunks.insert((path, coord), data);
                Ok(())
            }
            Some(_) => Err(UpdateNodeError::NotAnArray),
        }
    }

    // FIXME: add list, deletes, moves

    // FIXME: we should have errros here, not only None
    pub async fn get_node(&self, path: &Path) -> Option<NodeStructure> {
        let structure = self
            .storage
            .fetch_structure(&self.structure_id)
            .await
            .ok()?;
        structure.get_node(path)
    }

    pub async fn get_user_attributes(&self, _path: Path) -> Option<UserAttributes> {
        todo!()
    }
    pub async fn get_chunk(&self, _path: Path, _coord: ArrayIndices) -> ChunkPayload {
        todo!()
    }

    /// After all changes have been made, this generates all the structure files
    ///
    /// After calling this, changes are reset and the [Dataset] can continue to be used for further
    /// commits. The caller has presponsibility for seriliazing the output files.
    ///
    /// Files that are reused from previous commits are not returned because they don't need saving
    pub async fn consolidate(
        &mut self,
    ) -> (
        Arc<StructureTable>,
        Vec<Arc<AttributesTable>>,
        Vec<Arc<ManifestsTable>>,
    ) {
        todo!()
    }
}
