use std::{
    collections::HashSet,
    future::{ready, Future},
    pin::Pin,
    sync::Arc,
};

use bytes::Bytes;
use chrono::Utc;
use futures::{FutureExt, Stream, StreamExt, TryStreamExt};

use crate::{
    change_set::ChangeSet,
    format::{
        manifest::{
            ChunkInfo, ChunkRef, Manifest, ManifestExtents, ManifestRef, VirtualChunkRef,
        },
        snapshot::{
            ManifestFileInfo, NodeData, NodeSnapshot, NodeType, Snapshot,
            SnapshotProperties, UserAttributesSnapshot,
        },
        transaction_log::TransactionLog,
        ByteRange, ChunkIndices, IcechunkFormatError, ManifestId, NodeId, Path,
        SnapshotId,
    },
    repository::{ChunkPayload, RepositoryError, RepositoryResult},
    storage::virtual_ref::{construct_valid_byte_range, VirtualChunkResolver},
    zarr::ObjectId,
    RepositoryConfig, Storage,
};

pub struct ReadableSession {
    config: RepositoryConfig,
    storage: Arc<dyn Storage + Send + Sync>,
    snapshot_id: SnapshotId,
    change_set: ChangeSet,
    virtual_resolver: Arc<dyn VirtualChunkResolver + Send + Sync>,
}

impl ReadableSession {
    fn change_set(&self) -> &ChangeSet {
        &self.change_set
    }

    async fn get_node(&self, path: &Path) -> RepositoryResult<NodeSnapshot> {
        get_node(self.storage.as_ref(), &self.snapshot_id, &self.change_set(), path).await
    }

    async fn get_array(&self, path: &Path) -> RepositoryResult<NodeSnapshot> {
        get_array(self.storage.as_ref(), &self.snapshot_id, &self.change_set, path).await
    }

    async fn get_group(&self, path: &Path) -> RepositoryResult<NodeSnapshot> {
        get_group(self.storage.as_ref(), &self.snapshot_id, &self.change_set, path).await
    }

    async fn get_chunk_ref(
        &self,
        path: &Path,
        coords: &ChunkIndices,
    ) -> RepositoryResult<Option<ChunkPayload>> {
        get_chunk_ref(
            self.storage.as_ref(),
            &self.snapshot_id,
            &self.change_set,
            path,
            coords,
        )
        .await
    }

    /// Get a future that reads the the payload of a chunk from object store
    ///
    /// This function doesn't return [`Bytes`] directly to avoid locking the ref to self longer
    /// than needed. We want the bytes to be pulled from object store without holding a ref to the
    /// [`Repository`], that way, writes can happen concurrently.
    ///
    /// The result of calling this function is None, if the chunk reference is not present in the
    /// repository, or a [`Future`] that will fetch the bytes, possibly failing.
    ///
    /// Example usage:
    /// ```ignore
    /// get_chunk(
    ///     ds.get_chunk_reader(
    ///         &path,
    ///         &ChunkIndices(vec![0, 0, 0]),
    ///         &ByteRange::ALL,
    ///     )
    ///     .await
    ///     .unwrap(),
    /// ).await?
    /// ```
    ///
    /// The helper function [`get_chunk`] manages the pattern matching of the result and returns
    /// the bytes.
    async fn get_chunk_reader(
        &self,
        path: &Path,
        coords: &ChunkIndices,
        byte_range: &ByteRange,
    ) -> RepositoryResult<
        Option<Pin<Box<dyn Future<Output = RepositoryResult<Bytes>> + Send>>>,
    > {
        get_chunk_reader(
            &self.storage,
            &self.virtual_resolver,
            &self.snapshot_id,
            &self.change_set,
            path,
            coords,
            byte_range,
        )
        .await
    }

    async fn get_old_chunk(
        &self,
        node: NodeId,
        manifests: &[ManifestRef],
        coords: &ChunkIndices,
    ) -> RepositoryResult<Option<ChunkPayload>> {
        // FIXME: use manifest extents
        get_old_chunk(self.storage.as_ref(), node, manifests, coords).await
    }

    async fn list_nodes(
        &self,
    ) -> RepositoryResult<impl Iterator<Item = NodeSnapshot> + '_> {
        updated_nodes(self.storage.as_ref(), &self.change_set(), &self.snapshot_id, None)
            .await
    }

    async fn all_chunks(
        &self,
    ) -> RepositoryResult<impl Stream<Item = RepositoryResult<(Path, ChunkInfo)>> + '_>
    {
        all_chunks(self.storage.as_ref(), &self.change_set(), &self.snapshot_id).await
    }
}

pub async fn updated_nodes<'a>(
    storage: &(dyn Storage + Send + Sync),
    change_set: &'a ChangeSet,
    parent_id: &SnapshotId,
    manifest_id: Option<&'a ManifestId>,
) -> RepositoryResult<impl Iterator<Item = NodeSnapshot> + 'a> {
    Ok(updated_existing_nodes(storage, change_set, parent_id, manifest_id)
        .await?
        .chain(change_set.new_nodes_iterator(manifest_id)))
}

async fn updated_existing_nodes<'a>(
    storage: &(dyn Storage + Send + Sync),
    change_set: &'a ChangeSet,
    parent_id: &SnapshotId,
    manifest_id: Option<&'a ManifestId>,
) -> RepositoryResult<impl Iterator<Item = NodeSnapshot> + 'a> {
    let manifest_refs = manifest_id.map(|mid| {
        vec![ManifestRef { object_id: mid.clone(), extents: ManifestExtents(vec![]) }]
    });
    let updated_nodes =
        storage.fetch_snapshot(parent_id).await?.iter_arc().filter_map(move |node| {
            let new_manifests = if node.node_type() == NodeType::Array {
                //FIXME: it could be none for empty arrays
                manifest_refs.clone()
            } else {
                None
            };
            change_set.update_existing_node(node, new_manifests)
        });

    Ok(updated_nodes)
}

async fn get_node<'a>(
    storage: &(dyn Storage + Send + Sync),
    snapshot_id: &SnapshotId,
    change_set: &'a ChangeSet,
    path: &Path,
) -> RepositoryResult<NodeSnapshot> {
    // We need to look for nodes in self.change_set and the snapshot file
    if change_set.is_deleted(path) {
        return Err(RepositoryError::NodeNotFound {
            path: path.clone(),
            message: "getting node".to_string(),
        });
    }
    match change_set.get_new_node(path) {
        Some(node) => Ok(node),
        None => {
            let node = get_existing_node(storage, change_set, snapshot_id, path).await?;
            if change_set.is_deleted(&node.path) {
                Err(RepositoryError::NodeNotFound {
                    path: path.clone(),
                    message: "getting node".to_string(),
                })
            } else {
                Ok(node)
            }
        }
    }
}

async fn get_existing_node<'a>(
    storage: &(dyn Storage + Send + Sync),
    change_set: &'a ChangeSet,
    snapshot_id: &SnapshotId,
    path: &Path,
) -> RepositoryResult<NodeSnapshot> {
    // An existing node is one that is present in a Snapshot file on storage
    let snapshot = storage.fetch_snapshot(snapshot_id).await?;

    let node = snapshot.get_node(path).map_err(|err| match err {
        // A missing node here is not really a format error, so we need to
        // generate the correct error for repositories
        IcechunkFormatError::NodeNotFound { path } => RepositoryError::NodeNotFound {
            path,
            message: "existing node not found".to_string(),
        },
        err => RepositoryError::FormatError(err),
    })?;
    let session_atts = change_set
        .get_user_attributes(&node.id)
        .cloned()
        .map(|a| a.map(UserAttributesSnapshot::Inline));
    let res = NodeSnapshot {
        user_attributes: session_atts.unwrap_or_else(|| node.user_attributes.clone()),
        ..node.clone()
    };
    if let Some(session_meta) = change_set.get_updated_zarr_metadata(&node.id).cloned() {
        if let NodeData::Array(_, manifests) = res.node_data {
            Ok(NodeSnapshot {
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

pub async fn all_chunks<'a>(
    storage: &'a (dyn Storage + Send + Sync),
    change_set: &'a ChangeSet,
    snapshot_id: &'a SnapshotId,
) -> RepositoryResult<impl Stream<Item = RepositoryResult<(Path, ChunkInfo)>> + 'a> {
    let existing_array_chunks =
        updated_chunk_iterator(storage, change_set, snapshot_id).await?;
    let new_array_chunks =
        futures::stream::iter(change_set.new_arrays_chunk_iterator().map(Ok));
    Ok(existing_array_chunks.chain(new_array_chunks))
}

/// Warning: The presence of a single error may mean multiple missing items
async fn updated_chunk_iterator<'a>(
    storage: &'a (dyn Storage + Send + Sync),
    change_set: &'a ChangeSet,
    snapshot_id: &'a SnapshotId,
) -> RepositoryResult<impl Stream<Item = RepositoryResult<(Path, ChunkInfo)>> + 'a> {
    let snapshot = storage.fetch_snapshot(snapshot_id).await?;
    let nodes = futures::stream::iter(snapshot.iter_arc());
    let res = nodes.then(move |node| async move {
        let path = node.path.clone();
        node_chunk_iterator(storage, change_set, snapshot_id, &node.path)
            .await
            .map_ok(move |ci| (path.clone(), ci))
    });
    Ok(res.flatten())
}

/// Warning: The presence of a single error may mean multiple missing items
async fn node_chunk_iterator<'a>(
    storage: &'a (dyn Storage + Send + Sync),
    change_set: &'a ChangeSet,
    snapshot_id: &SnapshotId,
    path: &Path,
) -> impl Stream<Item = RepositoryResult<ChunkInfo>> + 'a {
    match get_node(storage, snapshot_id, change_set, path).await {
        Ok(node) => futures::future::Either::Left(
            verified_node_chunk_iterator(storage, change_set, node).await,
        ),
        Err(_) => futures::future::Either::Right(futures::stream::empty()),
    }
}

/// Warning: The presence of a single error may mean multiple missing items
async fn verified_node_chunk_iterator<'a>(
    storage: &'a (dyn Storage + Send + Sync),
    change_set: &'a ChangeSet,
    node: NodeSnapshot,
) -> impl Stream<Item = RepositoryResult<ChunkInfo>> + 'a {
    match node.node_data {
        NodeData::Group => futures::future::Either::Left(futures::stream::empty()),
        NodeData::Array(_, manifests) => {
            let new_chunk_indices: Box<HashSet<&ChunkIndices>> = Box::new(
                change_set
                    .array_chunks_iterator(&node.id, &node.path)
                    .map(|(idx, _)| idx)
                    .collect(),
            );

            let node_id_c = node.id.clone();
            let new_chunks = change_set
                .array_chunks_iterator(&node.id, &node.path)
                .filter_map(move |(idx, payload)| {
                    payload.as_ref().map(|payload| {
                        Ok(ChunkInfo {
                            node: node_id_c.clone(),
                            coord: idx.clone(),
                            payload: payload.clone(),
                        })
                    })
                });

            futures::future::Either::Right(
                futures::stream::iter(new_chunks).chain(
                    futures::stream::iter(manifests)
                        .then(move |manifest_ref| {
                            let new_chunk_indices = new_chunk_indices.clone();
                            let node_id_c = node.id.clone();
                            let node_id_c2 = node.id.clone();
                            let node_id_c3 = node.id.clone();
                            async move {
                                let manifest = storage
                                    .fetch_manifests(&manifest_ref.object_id)
                                    .await;
                                match manifest {
                                    Ok(manifest) => {
                                        let old_chunks = manifest
                                            .iter(node_id_c.clone())
                                            .filter(move |(coord, _)| {
                                                !new_chunk_indices.contains(coord)
                                            })
                                            .map(move |(coord, payload)| ChunkInfo {
                                                node: node_id_c2.clone(),
                                                coord,
                                                payload,
                                            });

                                        let old_chunks = change_set
                                            .update_existing_chunks(
                                                node_id_c3, old_chunks,
                                            );
                                        futures::future::Either::Left(
                                            futures::stream::iter(old_chunks.map(Ok)),
                                        )
                                    }
                                    // if we cannot even fetch the manifest, we generate a
                                    // single error value.
                                    Err(err) => futures::future::Either::Right(
                                        futures::stream::once(ready(Err(
                                            RepositoryError::StorageError(err),
                                        ))),
                                    ),
                                }
                            }
                        })
                        .flatten(),
                ),
            )
        }
    }
}

async fn new_materialized_chunk(
    storage: &(dyn Storage + Send + Sync),
    data: Bytes,
) -> RepositoryResult<ChunkPayload> {
    let new_id = ObjectId::random();
    storage.write_chunk(new_id.clone(), data.clone()).await?;
    Ok(ChunkPayload::Ref(ChunkRef { id: new_id, offset: 0, length: data.len() as u64 }))
}

fn new_inline_chunk(data: Bytes) -> ChunkPayload {
    ChunkPayload::Inline(data)
}

async fn flush(
    storage: &(dyn Storage + Send + Sync),
    change_set: &ChangeSet,
    parent_id: &SnapshotId,
    message: &str,
    properties: SnapshotProperties,
) -> RepositoryResult<SnapshotId> {
    if change_set.is_empty() {
        return Err(RepositoryError::NoChangesToCommit);
    }

    let chunks = all_chunks(storage, change_set, parent_id)
        .await?
        .map_ok(|(_path, chunk_info)| chunk_info);

    let new_manifest = Arc::new(Manifest::from_stream(chunks).await?);
    let new_manifest_id = if new_manifest.len() > 0 {
        let id = ObjectId::random();
        storage.write_manifests(id.clone(), Arc::clone(&new_manifest)).await?;
        Some(id)
    } else {
        None
    };

    let all_nodes =
        updated_nodes(storage, change_set, parent_id, new_manifest_id.as_ref()).await?;

    let old_snapshot = storage.fetch_snapshot(parent_id).await?;
    let mut new_snapshot = Snapshot::from_iter(
        old_snapshot.as_ref(),
        Some(properties),
        new_manifest_id
            .as_ref()
            .map(|mid| {
                vec![ManifestFileInfo {
                    id: mid.clone(),
                    format_version: new_manifest.icechunk_manifest_format_version,
                }]
            })
            .unwrap_or_default(),
        vec![],
        all_nodes,
    );
    new_snapshot.metadata.message = message.to_string();
    new_snapshot.metadata.written_at = Utc::now();

    let new_snapshot = Arc::new(new_snapshot);
    // FIXME: this should execute in a non-blocking context
    let tx_log =
        TransactionLog::new(change_set, old_snapshot.iter(), new_snapshot.iter());
    let new_snapshot_id = &new_snapshot.metadata.id;
    storage.write_snapshot(new_snapshot_id.clone(), Arc::clone(&new_snapshot)).await?;
    storage.write_transaction_log(new_snapshot_id.clone(), Arc::new(tx_log)).await?;

    Ok(new_snapshot_id.clone())
}

async fn get_old_chunk(
    storage: &(dyn Storage + Send + Sync),
    node: NodeId,
    manifests: &[ManifestRef],
    coords: &ChunkIndices,
) -> RepositoryResult<Option<ChunkPayload>> {
    // FIXME: use manifest extents
    for manifest in manifests {
        let manifest_structure = storage.fetch_manifests(&manifest.object_id).await?;
        match manifest_structure.get_chunk_payload(&node, coords.clone()) {
            Ok(payload) => {
                return Ok(Some(payload.clone()));
            }
            Err(IcechunkFormatError::ChunkCoordinatesNotFound { .. }) => {}
            Err(err) => return Err(err.into()),
        }
    }
    Ok(None)
}

async fn get_chunk_ref(
    storage: &(dyn Storage + Send + Sync),
    snapshot_id: &SnapshotId,
    change_set: &ChangeSet,
    path: &Path,
    coords: &ChunkIndices,
) -> RepositoryResult<Option<ChunkPayload>> {
    let node = get_node(storage, snapshot_id, change_set, path).await?;
    // TODO: it's ugly to have to do this destructuring even if we could be calling `get_array`
    // get_array should return the array data, not a node
    match node.node_data {
        NodeData::Group => Err(RepositoryError::NotAnArray {
            node,
            message: "getting chunk reference".to_string(),
        }),
        NodeData::Array(_, manifests) => {
            get_old_chunk(storage, node.id, manifests.as_slice(), coords).await
        }
    }
}

/// Get a future that reads the the payload of a chunk from object store
///
/// This function doesn't return [`Bytes`] directly to avoid locking the ref to self longer
/// than needed. We want the bytes to be pulled from object store without holding a ref to the
/// [`Repository`], that way, writes can happen concurrently.
///
/// The result of calling this function is None, if the chunk reference is not present in the
/// repository, or a [`Future`] that will fetch the bytes, possibly failing.
///
/// Example usage:
/// ```ignore
/// get_chunk(
///     ds.get_chunk_reader(
///         &path,
///         &ChunkIndices(vec![0, 0, 0]),
///         &ByteRange::ALL,
///     )
///     .await
///     .unwrap(),
/// ).await?
/// ```
///
/// The helper function [`get_chunk`] manages the pattern matching of the result and returns
/// the bytes.
async fn get_chunk_reader(
    storage: &Arc<dyn Storage + Send + Sync>,
    virtual_resolver: &Arc<dyn VirtualChunkResolver + Send + Sync>,
    snapshot_id: &SnapshotId,
    change_set: &ChangeSet,
    path: &Path,
    coords: &ChunkIndices,
    byte_range: &ByteRange,
) -> RepositoryResult<Option<Pin<Box<dyn Future<Output = RepositoryResult<Bytes>> + Send>>>>
{
    match get_chunk_ref(storage.as_ref(), snapshot_id, change_set, path, coords).await? {
        Some(ChunkPayload::Ref(ChunkRef { id, .. })) => {
            let storage = Arc::clone(storage);
            let byte_range = byte_range.clone();
            Ok(Some(
                async move {
                    // TODO: we don't have a way to distinguish if we want to pass a range or not
                    storage.fetch_chunk(&id, &byte_range).await.map_err(|e| e.into())
                }
                .boxed(),
            ))
        }
        Some(ChunkPayload::Inline(bytes)) => {
            Ok(Some(ready(Ok(byte_range.slice(bytes))).boxed()))
        }
        Some(ChunkPayload::Virtual(VirtualChunkRef { location, offset, length })) => {
            let byte_range = construct_valid_byte_range(byte_range, offset, length);
            let resolver = Arc::clone(virtual_resolver);
            Ok(Some(
                async move {
                    resolver
                        .fetch_chunk(&location, &byte_range)
                        .await
                        .map_err(|e| e.into())
                }
                .boxed(),
            ))
        }
        None => Ok(None),
    }
}

async fn get_array(
    storage: &(dyn Storage + Send + Sync),
    snapshot_id: &SnapshotId,
    change_set: &ChangeSet,
    path: &Path,
) -> RepositoryResult<NodeSnapshot> {
    match get_node(storage, snapshot_id, change_set, path).await {
        res @ Ok(NodeSnapshot { node_data: NodeData::Array(..), .. }) => res,
        Ok(node @ NodeSnapshot { .. }) => Err(RepositoryError::NotAnArray {
            node,
            message: "getting an array".to_string(),
        }),
        other => other,
    }
}

async fn get_group(
    storage: &(dyn Storage + Send + Sync),
    snapshot_id: &SnapshotId,
    change_set: &ChangeSet,
    path: &Path,
) -> RepositoryResult<NodeSnapshot> {
    match get_node(storage, snapshot_id, change_set, path).await {
        res @ Ok(NodeSnapshot { node_data: NodeData::Group, .. }) => res,
        Ok(node @ NodeSnapshot { .. }) => Err(RepositoryError::NotAGroup {
            node,
            message: "getting a group".to_string(),
        }),
        other => other,
    }
}
