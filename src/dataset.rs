use std::{collections::HashMap, sync::Arc};

use crate::{
    AddNodeError, ArrayIndices, AttributesTable, ChunkPayload, Dataset, ManifestRef,
    ManifestsTable, NodeData, NodeId, NodeStructure, ObjectId, Path, Storage, StructureTable,
    UpdateNodeError, UserAttributes, UserAttributesStructure, ZarrArrayMetadata,
};

/// FIXME: what do we want to do with implicit groups?
///
impl Dataset {
    pub fn new(storage: Box<dyn Storage>, structure_id: ObjectId) -> Self {
        Dataset {
            structure_id,
            storage,
            last_node_id: None,
            new_groups: HashMap::new(),
            new_arrays: HashMap::new(),
            updated_arrays: HashMap::new(),
            updated_attributes: HashMap::new(),
            set_chunks: HashMap::new(),
        }
    }

    /// Add a group to the store.
    ///
    /// Calling this only records the operation in memory, doesn't have any consequence on the storage
    pub async fn add_group(&mut self, path: Path) -> Result<(), AddNodeError> {
        if self.get_node(&path).await.is_none() {
            let id = self.reserve_node_id().await;
            self.new_groups.insert(path, id);
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
            let id = self.reserve_node_id().await;
            self.new_arrays.insert(path, (id, metadata));
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

    /// Record the write or delete of user attributes to array or group
    pub async fn set_user_attributes(
        &mut self,
        path: Path,
        atts: Option<UserAttributes>,
    ) -> Result<(), UpdateNodeError> {
        match self.get_node(&path).await {
            None => Err(UpdateNodeError::NotFound),
            Some(_) => {
                self.updated_attributes.insert(path, atts);
                Ok(())
            }
        }
    }

    // Record the write, referenceing or delete of a chunk
    //
    // Caller has to write the chunk before calling this.
    pub async fn set_chunk(
        &mut self,
        path: Path,
        coord: ArrayIndices,
        data: Option<ChunkPayload>,
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

    async fn compute_last_node_id(&self) -> NodeId {
        // FIXME: errors
        match self.storage.fetch_structure(&self.structure_id).await.ok() {
            None => 0,
            Some(structure) => structure
                .iter()
                .max_by_key(|s| s.id)
                .map_or(0, |node| node.id),
        }
    }

    async fn reserve_node_id(&mut self) -> NodeId {
        let last = self
            .last_node_id
            .unwrap_or(self.compute_last_node_id().await);
        let new = last + 1;
        self.last_node_id = Some(new);
        new
    }

    // FIXME: add list, deletes, moves

    // FIXME: we should have errros here, not only None
    pub async fn get_node(&self, path: &Path) -> Option<NodeStructure> {
        self.get_new_node(path)
            .or(self.get_existing_node(path).await)
    }

    async fn get_existing_node(&self, path: &Path) -> Option<NodeStructure> {
        let structure = self
            .storage
            .fetch_structure(&self.structure_id)
            .await
            .ok()?;
        let session_atts = self
            .updated_attributes
            .get(path)
            .cloned()
            .map(|a| a.map(UserAttributesStructure::Inline));
        let res = structure.get_node(path)?;
        let res = NodeStructure {
            user_attributes: session_atts.unwrap_or(res.user_attributes),
            ..res
        };
        if let Some(session_meta) = self.updated_arrays.get(path).cloned() {
            if let NodeData::Array(_, manifests) = res.node_data {
                Some(NodeStructure {
                    node_data: NodeData::Array(session_meta, manifests),
                    ..res
                })
            } else {
                Some(res)
            }
        } else {
            Some(res)
        }
    }

    fn get_new_node(&self, path: &Path) -> Option<NodeStructure> {
        self.get_new_array(path).or(self.get_new_group(path))
    }

    fn get_new_array(&self, path: &Path) -> Option<NodeStructure> {
        self.new_arrays.get(path).map(|(id, meta)| {
            let meta = self.updated_arrays.get(path).unwrap_or(meta).clone();
            let atts = self.updated_attributes.get(path).cloned();
            NodeStructure {
                id: *id,
                path: path.clone(),
                user_attributes: atts.flatten().map(UserAttributesStructure::Inline),
                // We put no manifests in new arrays, see get_chunk to understand how chunks get
                // fetched for those arrays
                node_data: NodeData::Array(meta.clone(), vec![]),
            }
        })
    }

    fn get_new_group(&self, path: &Path) -> Option<NodeStructure> {
        self.new_groups.get(path).map(|id| {
            let atts = self.updated_attributes.get(path).cloned();
            NodeStructure {
                id: *id,
                path: path.clone(),
                user_attributes: atts.flatten().map(UserAttributesStructure::Inline),
                node_data: NodeData::Group,
            }
        })
    }

    pub async fn get_chunk(&self, path: &Path, coords: &ArrayIndices) -> Option<ChunkPayload> {
        // FIXME: better error type
        let node = self.get_node(path).await?;
        match node.node_data {
            NodeData::Group => None,
            NodeData::Array(_, manifests) => {
                // check the chunks modified in this session first
                // TODO: I hate rust forces me to clone to search in a hashmap. How to do better?
                let session_chunk = self
                    .set_chunks
                    .get(&(path.clone(), coords.clone()))
                    .cloned();
                // If session_chunk is not None we have to return it, because is the update the
                // user made in the current session
                // If session_chunk == None, user hasn't modified the chunk in this session and we
                // need to fallback to fetching the manifests
                session_chunk.unwrap_or(self.get_old_chunk(manifests.as_slice(), coords).await)
            }
        }
    }

    async fn get_old_chunk(
        &self,
        manifests: &[ManifestRef],
        coords: &ArrayIndices,
    ) -> Option<ChunkPayload> {
        // FIXME: use manifest extents
        for manifest in manifests {
            let manifest_structure = self
                .storage
                .fetch_manifests(&manifest.object_id)
                .await
                .ok()?;
            if let Some(payload) = manifest_structure
                .get_chunk_info(coords, &manifest.location)
                .map(|info| info.payload)
            {
                return Some(payload);
            }
        }
        None
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

#[cfg(test)]
mod tests {
    use std::{error::Error, num::NonZeroU64, path::PathBuf};

    use crate::{
        manifest::mk_manifests_table, storage::InMemoryStorage, structure::mk_structure_table,
        ChunkInfo, ChunkKeyEncoding, ChunkRef, ChunkShape, Codecs, DataType, FillValue, Flags,
        ManifestExtents, StorageTransformers, TableRegion,
    };

    use super::*;
    use pretty_assertions::assert_eq;

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
            payload: ChunkPayload::Inline(vec![0, 0, 0, 42]),
        };

        let manifest = Arc::new(mk_manifests_table(vec![chunk1.clone(), chunk2.clone()]));
        let manifest_id = ObjectId::random();
        storage
            .write_manifests(manifest_id.clone(), manifest)
            .await
            .map_err(|err| format!("{err:#?}"))?;

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
            codecs: Codecs("codec".to_string()),
            storage_transformers: Some(StorageTransformers("tranformers".to_string())),
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
                user_attributes: Some(UserAttributesStructure::Inline("{foo:1}".to_string())),
                node_data: NodeData::Array(zarr_meta1.clone(), vec![manifest_ref]),
            },
        ];

        let structure = Arc::new(mk_structure_table(nodes.clone()));
        let structure_id = ObjectId::random();
        storage
            .write_structure(structure_id.clone(), structure)
            .await
            .map_err(|err| format!("{err:#?}"))?;
        let mut ds = Dataset::new(Box::new(storage), structure_id);

        // retrieve the old array node
        let node = ds.get_node(&array1_path).await;
        assert_eq!(nodes.get(1), node.as_ref());

        // add a new array and retrieve its node
        ds.add_group("/group".to_string().into())
            .await
            .map_err(|err| format!("{err:#?}"))?;

        let zarr_meta2 = ZarrArrayMetadata {
            shape: vec![3],
            data_type: DataType::Int32,
            chunk_shape: ChunkShape(vec![NonZeroU64::new(2).unwrap()]),
            chunk_key_encoding: ChunkKeyEncoding::Slash,
            fill_value: FillValue::Int32(0),
            codecs: Codecs("codec".to_string()),
            storage_transformers: Some(StorageTransformers("tranformers".to_string())),
            dimension_names: Some(vec![Some("t".to_string())]),
        };

        let new_array_path: PathBuf = "/group/array2".to_string().into();
        ds.add_array(new_array_path.clone(), zarr_meta2.clone())
            .await
            .map_err(|err| format!("{err:#?}"))?;

        let node = ds.get_node(&new_array_path).await;
        assert_eq!(
            node,
            Some(NodeStructure {
                path: new_array_path.clone(),
                id: 4,
                user_attributes: None,
                node_data: NodeData::Array(zarr_meta2.clone(), vec![]),
            })
        );

        // set user attributes for the new array and retrieve them
        ds.set_user_attributes(new_array_path.clone(), Some("{n:42}".to_string()))
            .await
            .map_err(|err| format!("{err:#?}"))?;
        let node = ds.get_node(&new_array_path).await;
        assert_eq!(
            node,
            Some(NodeStructure {
                path: "/group/array2".into(),
                id: 4,
                user_attributes: Some(UserAttributesStructure::Inline("{n:42}".to_string(),)),
                node_data: NodeData::Array(zarr_meta2.clone(), vec![]),
            })
        );

        // set a chunk for the new array and  retrieve it
        ds.set_chunk(
            new_array_path.clone(),
            ArrayIndices(vec![0]),
            Some(ChunkPayload::Inline(vec![0, 0, 0, 7])),
        )
        .await
        .map_err(|err| format!("{err:#?}"))?;

        let chunk = ds.get_chunk(&new_array_path, &ArrayIndices(vec![0])).await;
        assert_eq!(chunk, Some(ChunkPayload::Inline(vec![0, 0, 0, 7])));

        // retrieve a non initialized chunk of the new array
        let non_chunk = ds.get_chunk(&new_array_path, &ArrayIndices(vec![1])).await;
        assert_eq!(non_chunk, None);

        // update old array use attriutes and check them
        ds.set_user_attributes(array1_path.clone(), Some("{updated: true}".to_string()))
            .await
            .map_err(|err| format!("{err:#?}"))?;
        let node = ds.get_node(&array1_path).await.unwrap();
        assert_eq!(
            node.user_attributes,
            Some(UserAttributesStructure::Inline(
                "{updated: true}".to_string()
            ))
        );

        // update old array zarr metadata and check it
        let new_zarr_meta1 = ZarrArrayMetadata {
            shape: vec![2, 2, 3],
            ..zarr_meta1
        };
        ds.update_array(array1_path.clone(), new_zarr_meta1)
            .await
            .map_err(|err| format!("{err:#?}"))?;
        let node = ds.get_node(&array1_path).await;
        if let Some(NodeStructure {
            node_data: NodeData::Array(ZarrArrayMetadata { shape, .. }, _),
            ..
        }) = node
        {
            assert_eq!(shape, vec![2, 2, 3]);
        } else {
            panic!("Failed to update zarr metadata");
        }

        // set old array chunk and check them
        ds.set_chunk(
            array1_path.clone(),
            ArrayIndices(vec![0, 0, 0]),
            Some(ChunkPayload::Inline(vec![0, 0, 0, 99])),
        )
        .await
        .map_err(|err| format!("{err:#?}"))?;

        let chunk = ds
            .get_chunk(&array1_path, &ArrayIndices(vec![0, 0, 0]))
            .await;
        assert_eq!(chunk, Some(ChunkPayload::Inline(vec![0, 0, 0, 99])));

        Ok(())
    }
}
