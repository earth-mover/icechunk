use core::fmt;
use std::{
    collections::{HashMap, HashSet},
    io::ErrorKind,
    iter,
    path::{Path as StdPath, PathBuf},
    sync::{Arc, RwLock},
    time::{Instant, SystemTime, UNIX_EPOCH},
};

pub use crate::{
    format::{manifest::ChunkPayload, snapshot::ZarrArrayMetadata, ChunkIndices, Path},
    metadata::{
        ArrayShape, ChunkKeyEncoding, ChunkShape, Codec, DataType, DimensionName,
        DimensionNames, FillValue, StorageTransformer, UserAttributes,
    },
};

use ::rocksdb::OptimisticTransactionDB;
use arrow::error::ArrowError;
use bytes::Bytes;
use chrono::Utc;
use futures::{future::ready, io::AsyncWriteExt, Stream, StreamExt, TryStreamExt};
use itertools::{Either, Itertools};
use rocksdb::{SingleThreaded, WaitForCompactOptions};
use serde_json::Value;
use thiserror::Error;
use tokio::{
    pin,
    task::{self, spawn_blocking},
};
use tokio_util::io::SyncIoBridge;

use crate::{
    format::{
        manifest::{
            delete_chunk, get_chunk_payload, write_chunk_payload, ChunkInfo, ChunkRef,
            ManifestExtents, ManifestRef,
        },
        snapshot::{
            mk_snapshot_table, NodeData, NodeSnapshot, NodeType, UserAttributesSnapshot,
        },
        Flags, IcechunkFormatError, NodeId, ObjectId, TableRegion,
    },
    refs::{
        fetch_branch, fetch_branch_tip, fetch_tag, last_branch_version, update_branch,
        BranchVersion, RefError,
    },
    rocksdb::{compress_db, extract_db, open_db},
    Storage, StorageError,
};

#[derive(Clone, Debug)]
pub struct DatasetConfig {
    // Chunks smaller than this will be stored inline in the manifst
    pub inline_threshold_bytes: u16,
    pub manifest_databases_dir: PathBuf,
}

impl Default for DatasetConfig {
    fn default() -> Self {
        Self {
            inline_threshold_bytes: 512,
            manifest_databases_dir: {
                let mut tmp = std::env::temp_dir();
                tmp.push("icechunk-manifests");
                tmp.push(
                    SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_micros()
                        .to_string(),
                );
                tmp
            },
        }
    }
}

#[derive()]
pub struct Dataset {
    config: DatasetConfig,
    storage: Arc<dyn Storage + Send + Sync>,
    snapshot_id: Option<ObjectId>,
    last_node_id: Option<NodeId>,
    change_set: ChangeSet,
    manifests: RwLock<HashMap<ObjectId, Arc<OptimisticTransactionDB>>>,
    rocksdb_env: ::rocksdb::Env,
}
impl fmt::Debug for Dataset {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        #[derive(Debug)]
        struct DebuggableConfig<'a> {
            config: &'a DatasetConfig,
            storage: &'a Arc<dyn Storage + Send + Sync>,
            snapshot_id: &'a Option<ObjectId>,
            last_node_id: &'a Option<NodeId>,
            change_set: &'a ChangeSet,
            manifests: &'a RwLock<HashMap<ObjectId, Arc<OptimisticTransactionDB>>>,
        }

        let Self {
            config,
            storage,
            snapshot_id,
            last_node_id,
            change_set,
            manifests,
            rocksdb_env: _,
        } = self;

        // per Chayim Friedman’s suggestion
        fmt::Debug::fmt(
            &DebuggableConfig {
                config,
                storage,
                snapshot_id,
                last_node_id,
                change_set,
                manifests,
            },
            f,
        )
    }
}

#[derive(Clone, Debug, PartialEq, Default)]
struct ChangeSet {
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
    fn add_group(&mut self, path: Path, node_id: NodeId) {
        self.new_groups.insert(path, node_id);
    }

    fn get_group(&self, path: &Path) -> Option<&NodeId> {
        self.new_groups.get(path)
    }

    fn get_array(&self, path: &Path) -> Option<&(NodeId, ZarrArrayMetadata)> {
        self.new_arrays.get(path)
    }

    fn delete_group(&mut self, path: &Path, node_id: NodeId) {
        let new_node_id = self.new_groups.remove(path);
        let is_new_group = new_node_id.is_some();
        debug_assert!(!is_new_group || new_node_id == Some(node_id));

        self.updated_attributes.remove(&node_id);
        if !is_new_group {
            self.deleted_groups.insert(node_id);
        }
    }

    fn add_array(&mut self, path: Path, node_id: NodeId, metadata: ZarrArrayMetadata) {
        self.new_arrays.insert(path, (node_id, metadata));
    }

    fn update_array(&mut self, node_id: NodeId, metadata: ZarrArrayMetadata) {
        self.updated_arrays.insert(node_id, metadata);
    }

    fn delete_array(&mut self, path: &Path, node_id: NodeId) {
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

    fn get_updated_zarr_metadata(&self, node_id: NodeId) -> Option<&ZarrArrayMetadata> {
        self.updated_arrays.get(&node_id)
    }

    fn update_user_attributes(&mut self, node_id: NodeId, atts: Option<UserAttributes>) {
        self.updated_attributes.insert(node_id, atts);
    }

    fn get_user_attributes(&self, node_id: NodeId) -> Option<&Option<UserAttributes>> {
        self.updated_attributes.get(&node_id)
    }

    fn set_chunk_ref(
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

    fn get_chunk_ref(
        &self,
        node_id: NodeId,
        coords: &ChunkIndices,
    ) -> Option<&Option<ChunkPayload>> {
        self.set_chunks.get(&node_id).and_then(|h| h.get(coords))
    }

    fn array_chunks_iterator(
        &self,
        node_id: NodeId,
    ) -> impl Iterator<Item = (&ChunkIndices, &Option<ChunkPayload>)> {
        match self.set_chunks.get(&node_id) {
            None => Either::Left(iter::empty()),
            Some(h) => Either::Right(h.iter()),
        }
    }

    fn new_arrays_chunk_iterator(
        &self,
    ) -> impl Iterator<Item = (PathBuf, ChunkInfo)> + '_ {
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

    fn new_nodes(&self) -> impl Iterator<Item = &Path> {
        self.new_groups.keys().chain(self.new_arrays.keys())
    }
}

#[derive(Debug, Clone)]
pub struct DatasetBuilder {
    config: DatasetConfig,
    storage: Arc<dyn Storage + Send + Sync>,
    snapshot_id: Option<ObjectId>,
}

impl DatasetBuilder {
    fn new(
        storage: Arc<dyn Storage + Send + Sync>,
        snapshot_id: Option<ObjectId>,
    ) -> Self {
        Self { config: DatasetConfig::default(), snapshot_id, storage }
    }

    pub fn with_inline_threshold_bytes(&mut self, threshold: u16) -> &mut Self {
        self.config.inline_threshold_bytes = threshold;
        self
    }

    pub fn build(&self) -> Dataset {
        Dataset::new(self.config.clone(), self.storage.clone(), self.snapshot_id.clone())
    }
}

#[derive(Debug, Error)]
pub enum DatasetError {
    #[error("error contacting storage")]
    StorageError(#[from] StorageError),
    #[error("error in icechunk file")]
    FormatError(#[from] IcechunkFormatError),
    #[error("node not found at `{path}`: {message}")]
    NotFound { path: Path, message: String },
    #[error("there is not an array at `{node:?}`: {message}")]
    NotAnArray { node: NodeSnapshot, message: String },
    #[error("there is not a group at `{node:?}`: {message}")]
    NotAGroup { node: NodeSnapshot, message: String },
    #[error("node already exists at `{node:?}`: {message}")]
    AlreadyExists { node: NodeSnapshot, message: String },
    #[error("no changes made to the dataset")]
    NoChangesToFlush,
    #[error("Arrow format error: `{0}`")]
    ArrowError(#[from] ArrowError),
    #[error("ref error: `{0}`")]
    Ref(#[from] RefError),
    #[error("branch update conflict: `({expected_parent:?}) != ({actual_parent:?})`")]
    Conflict { expected_parent: Option<ObjectId>, actual_parent: Option<ObjectId> },
    #[error("i/o error: `{0}")]
    IO(#[from] std::io::Error),
    #[error("RocksDB error error: `{0}")]
    RocksDB(#[from] ::rocksdb::Error),
}

type DatasetResult<T> = Result<T, DatasetError>;

/// FIXME: what do we want to do with implicit groups?
///
impl Dataset {
    pub fn create(storage: Arc<dyn Storage + Send + Sync>) -> DatasetBuilder {
        DatasetBuilder::new(storage, None)
    }

    pub fn update(
        storage: Arc<dyn Storage + Send + Sync>,
        previous_version_snapshot_id: ObjectId,
    ) -> DatasetBuilder {
        DatasetBuilder::new(storage, Some(previous_version_snapshot_id))
    }

    pub async fn from_branch_tip(
        storage: Arc<dyn Storage + Send + Sync>,
        branch_name: &str,
    ) -> DatasetResult<DatasetBuilder> {
        let version = last_branch_version(storage.as_ref(), branch_name).await?;
        let ref_data = fetch_branch(storage.as_ref(), branch_name, &version).await?;
        Ok(Self::update(storage, ref_data.snapshot))
    }

    pub async fn from_tag(
        storage: Arc<dyn Storage + Send + Sync>,
        tag_name: &str,
    ) -> DatasetResult<DatasetBuilder> {
        let ref_data = fetch_tag(storage.as_ref(), tag_name).await?;
        Ok(Self::update(storage, ref_data.snapshot))
    }

    fn new(
        config: DatasetConfig,
        storage: Arc<dyn Storage + Send + Sync>,
        previous_version_snapshot_id: Option<ObjectId>,
    ) -> Self {
        Dataset {
            snapshot_id: previous_version_snapshot_id,
            config,
            storage,
            last_node_id: None,
            change_set: ChangeSet::default(),
            manifests: RwLock::new(HashMap::new()),
            // FIXME: unwrap
            rocksdb_env: ::rocksdb::Env::new().unwrap(),
        }
    }

    pub fn storage(&self) -> &Arc<dyn Storage + Send + Sync> {
        &self.storage
    }

    /// Add a group to the store.
    ///
    /// Calling this only records the operation in memory, doesn't have any consequence on the storage
    pub async fn add_group(&mut self, path: Path) -> DatasetResult<()> {
        match self.get_node(&path).await {
            Err(DatasetError::NotFound { .. }) => {
                let id = self.reserve_node_id().await?;
                self.change_set.add_group(path.clone(), id);
                Ok(())
            }
            Ok(node) => Err(DatasetError::AlreadyExists {
                node,
                message: "trying to add group".to_string(),
            }),
            Err(err) => Err(err),
        }
    }

    pub async fn delete_group(&mut self, path: Path) -> DatasetResult<()> {
        self.get_group(&path)
            .await
            .map(|node| self.change_set.delete_group(&node.path, node.id))
    }

    /// Add an array to the store.
    ///
    /// Calling this only records the operation in memory, doesn't have any consequence on the storage
    pub async fn add_array(
        &mut self,
        path: Path,
        metadata: ZarrArrayMetadata,
    ) -> DatasetResult<()> {
        match self.get_node(&path).await {
            Err(DatasetError::NotFound { .. }) => {
                let id = self.reserve_node_id().await?;
                self.change_set.add_array(path, id, metadata);
                Ok(())
            }
            Ok(node) => Err(DatasetError::AlreadyExists {
                node,
                message: "trying to add array".to_string(),
            }),
            Err(err) => Err(err),
        }
    }

    // Updates an array Zarr metadata
    ///
    /// Calling this only records the operation in memory, doesn't have any consequence on the storage
    pub async fn update_array(
        &mut self,
        path: Path,
        metadata: ZarrArrayMetadata,
    ) -> DatasetResult<()> {
        self.get_array(&path)
            .await
            .map(|node| self.change_set.update_array(node.id, metadata))
    }

    pub async fn delete_array(&mut self, path: Path) -> DatasetResult<()> {
        self.get_array(&path)
            .await
            .map(|node| self.change_set.delete_array(&node.path, node.id))
    }

    /// Record the write or delete of user attributes to array or group
    pub async fn set_user_attributes(
        &mut self,
        path: Path,
        atts: Option<UserAttributes>,
    ) -> DatasetResult<()> {
        let node = self.get_node(&path).await?;
        self.change_set.update_user_attributes(node.id, atts);
        Ok(())
    }

    // Record the write, referenceing or delete of a chunk
    //
    // Caller has to write the chunk before calling this.
    pub async fn set_chunk_ref(
        &mut self,
        path: Path,
        coord: ChunkIndices,
        data: Option<ChunkPayload>,
    ) -> DatasetResult<()> {
        self.get_array(&path)
            .await
            .map(|node| self.change_set.set_chunk_ref(node.id, coord, data))
    }

    async fn compute_last_node_id(&self) -> DatasetResult<NodeId> {
        match &self.snapshot_id {
            None => Ok(0),
            Some(id) => {
                let mut max_id = 0;
                for maybe_node in self.storage.fetch_snapshot(id).await?.iter() {
                    match maybe_node {
                        Ok(node) => {
                            max_id = std::cmp::max(max_id, node.id);
                        }
                        Err(err) => {
                            Err(err)?;
                        }
                    }
                }
                Ok(max_id)
            }
        }
    }

    async fn reserve_node_id(&mut self) -> DatasetResult<NodeId> {
        let last = self.last_node_id.unwrap_or(self.compute_last_node_id().await?);
        let new = last + 1;
        self.last_node_id = Some(new);
        Ok(new)
    }

    // FIXME: add moves

    pub async fn get_node(&self, path: &Path) -> DatasetResult<NodeSnapshot> {
        // We need to look for nodes in self.change_set and the snapshot file
        match self.get_new_node(path) {
            Some(node) => Ok(node),
            None => {
                let node = self.get_existing_node(path).await?;
                if self.change_set.deleted_groups.contains(&node.id)
                    || self.change_set.deleted_arrays.contains(&node.id)
                {
                    Err(DatasetError::NotFound {
                        path: path.clone(),
                        message: "getting node".to_string(),
                    })
                } else {
                    Ok(node)
                }
            }
        }
    }

    pub async fn get_array(&self, path: &Path) -> DatasetResult<NodeSnapshot> {
        match self.get_node(path).await {
            res @ Ok(NodeSnapshot { node_data: NodeData::Array(..), .. }) => res,
            Ok(node @ NodeSnapshot { .. }) => Err(DatasetError::NotAnArray {
                node,
                message: "getting an array".to_string(),
            }),
            other => other,
        }
    }

    pub async fn get_group(&self, path: &Path) -> DatasetResult<NodeSnapshot> {
        match self.get_node(path).await {
            res @ Ok(NodeSnapshot { node_data: NodeData::Group, .. }) => res,
            Ok(node @ NodeSnapshot { .. }) => Err(DatasetError::NotAGroup {
                node,
                message: "getting a group".to_string(),
            }),
            other => other,
        }
    }

    async fn get_existing_node(&self, path: &Path) -> DatasetResult<NodeSnapshot> {
        // An existing node is one that is present in a Snapshot file on storage
        let err = || DatasetError::NotFound {
            path: path.clone(),
            message: "getting existing node".to_string(),
        };

        let snapshot_id = self.snapshot_id.as_ref().ok_or(err())?;
        let snapshot = self.storage.fetch_snapshot(snapshot_id).await?;

        let node = snapshot.get_node(path).map_err(|err| match err {
            // A missing node here is not really a format error, so we need to
            // generate the correct error for datasets
            IcechunkFormatError::NodeNotFound { path } => DatasetError::NotFound {
                path,
                message: "existing node not found".to_string(),
            },
            err => DatasetError::FormatError(err),
        })?;
        let session_atts = self
            .change_set
            .get_user_attributes(node.id)
            .cloned()
            .map(|a| a.map(UserAttributesSnapshot::Inline));
        let res = NodeSnapshot {
            user_attributes: session_atts.unwrap_or(node.user_attributes),
            ..node
        };
        if let Some(session_meta) =
            self.change_set.get_updated_zarr_metadata(node.id).cloned()
        {
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

    fn get_new_node(&self, path: &Path) -> Option<NodeSnapshot> {
        self.get_new_array(path).or(self.get_new_group(path))
    }

    fn get_new_array(&self, path: &Path) -> Option<NodeSnapshot> {
        self.change_set.get_array(path).map(|(id, meta)| {
            let meta =
                self.change_set.get_updated_zarr_metadata(*id).unwrap_or(meta).clone();
            let atts = self.change_set.get_user_attributes(*id).cloned();
            NodeSnapshot {
                id: *id,
                path: path.clone(),
                user_attributes: atts.flatten().map(UserAttributesSnapshot::Inline),
                // We put no manifests in new arrays, see get_chunk_ref to understand how chunks get
                // fetched for those arrays
                node_data: NodeData::Array(meta.clone(), vec![]),
            }
        })
    }

    fn get_new_group(&self, path: &Path) -> Option<NodeSnapshot> {
        self.change_set.get_group(path).map(|id| {
            let atts = self.change_set.get_user_attributes(*id).cloned();
            NodeSnapshot {
                id: *id,
                path: path.clone(),
                user_attributes: atts.flatten().map(UserAttributesSnapshot::Inline),
                node_data: NodeData::Group,
            }
        })
    }

    pub async fn get_chunk_ref(
        &self,
        path: &Path,
        coords: &ChunkIndices,
    ) -> DatasetResult<Option<ChunkPayload>> {
        let node = self.get_node(path).await?;
        // TODO: it's ugly to have to do this destructuring even if we could be calling `get_array`
        // get_array should return the array data, not a node
        match node.node_data {
            NodeData::Group => Err(DatasetError::NotAnArray {
                node,
                message: "getting chunk reference".to_string(),
            }),
            NodeData::Array(_, manifests) => {
                // check the chunks modified in this session first
                // TODO: I hate rust forces me to clone to search in a hashmap. How to do better?
                let session_chunk =
                    self.change_set.get_chunk_ref(node.id, coords).cloned();
                // If session_chunk is not None we have to return it, because is the update the
                // user made in the current session
                // If session_chunk == None, user hasn't modified the chunk in this session and we
                // need to fallback to fetching the manifests
                match session_chunk {
                    Some(res) => Ok(res),
                    None => {
                        self.get_old_chunk(manifests.as_slice(), &node.id, coords).await
                    }
                }
            }
        }
    }

    pub async fn get_chunk(
        &self,
        path: &Path,
        coords: &ChunkIndices,
    ) -> DatasetResult<Option<Bytes>> {
        match self.get_chunk_ref(path, coords).await? {
            Some(ChunkPayload::Ref(ChunkRef { id, .. })) => {
                // TODO: we don't have a way to distinguish if we want to pass a range or not
                Ok(self.storage.fetch_chunk(&id, &None).await.map(Some)?)
            }
            Some(ChunkPayload::Inline(bytes)) => Ok(Some(bytes)),
            //FIXME: implement virtual fetch
            Some(ChunkPayload::Virtual(_)) => todo!(),
            None => Ok(None),
        }
    }

    pub async fn set_chunk(
        &mut self,
        path: &Path,
        coord: &ChunkIndices,
        data: Bytes,
    ) -> DatasetResult<()> {
        match self.get_array(path).await {
            Ok(node) => {
                let payload = if data.len() > self.config.inline_threshold_bytes as usize
                {
                    new_materialized_chunk(self.storage.as_ref(), data).await?
                } else {
                    new_inline_chunk(data)
                };
                self.change_set.set_chunk_ref(node.id, coord.clone(), Some(payload));
                Ok(())
            }
            Err(_) => Err(DatasetError::NotFound {
                path: path.clone(),
                message: "setting chunk".to_string(),
            }),
        }
    }

    fn db_dir(&self, manifest_id: &ObjectId) -> PathBuf {
        let mut dir = self.config.manifest_databases_dir.clone();
        let dir_name: String = manifest_id.into();
        dir.push(dir_name.as_str());
        dir
    }

    async fn fetch_manifest(
        &self,
        id: &ObjectId,
    ) -> DatasetResult<OptimisticTransactionDB> {
        let dir = self.db_dir(id);
        let db_bytes = self
            .storage
            .fetch_manifest(id)
            .await?
            .map_err(|e| std::io::Error::new(ErrorKind::Other, e));
        extract_db(dir.as_path(), db_bytes).await?;
        let db = open_db(dir.as_path(), &self.rocksdb_env)?;
        Ok(db)
    }

    async fn get_manifest(
        &self,
        id: &ObjectId,
    ) -> DatasetResult<Arc<OptimisticTransactionDB>> {
        let cached = {
            let m = self.manifests.read().unwrap();
            m.get(id).map(Arc::clone)
        };

        match cached {
            Some(db) => Ok(db),
            None => {
                let db = self.fetch_manifest(id).await?;
                self.manifests.write().unwrap().insert(id.clone(), Arc::new(db));
                let man = self.manifests.read().unwrap();
                let res = man.get(id).unwrap();
                Ok(Arc::clone(res))
            }
        }
    }

    //async fn get_old_chunk(
    //    &self,
    //    manifests: &[ManifestRef],
    //    coords: &ChunkIndices,
    //) -> DatasetResult<Option<ChunkPayload>> {
    //    // FIXME: use manifest extents
    //    for manifest in manifests {
    //        let manifest_structure =
    //            self.storage.fetch_manifests(&manifest.object_id).await?;
    //        match manifest_structure
    //            .get_chunk_info(coords, &manifest.location)
    //            .map(|info| info.payload)
    //        {
    //            Ok(payload) => {
    //                return Ok(Some(payload));
    //            }
    //            Err(IcechunkFormatError::ChunkCoordinatesNotFound { .. }) => {}
    //            Err(err) => return Err(err.into()),
    //        }
    //    }
    //    Ok(None)
    //}

    async fn get_old_chunk(
        &self,
        manifests: &[ManifestRef],
        node_id: &NodeId,
        coord: &ChunkIndices,
    ) -> DatasetResult<Option<ChunkPayload>> {
        // FIXME: use manifest extents
        for manifest in manifests {
            let manifest_db = self.get_manifest(&manifest.object_id).await?;
            match get_chunk_payload(manifest_db.as_ref(), node_id, coord) {
                Ok(payload) => {
                    return Ok(Some(payload));
                }
                Err(IcechunkFormatError::ChunkCoordinatesNotFound { .. }) => {}
                Err(err) => return Err(err.into()),
            }
        }
        Ok(None)
    }

    /// Warning: The presence of a single error may mean multiple missing items
    async fn updated_chunk_iterator(
        &self,
    ) -> DatasetResult<impl Stream<Item = DatasetResult<(PathBuf, ChunkInfo)>> + '_> {
        match self.snapshot_id.as_ref() {
            None => Ok(futures::future::Either::Left(futures::stream::empty())),
            Some(snapshot_id) => {
                let snapshot = self.storage.fetch_snapshot(snapshot_id).await?;
                let nodes = futures::stream::iter(snapshot.iter_arc());
                let res = nodes.and_then(move |node| async move {
                    let path = node.path.clone();
                    Ok(self
                        .node_chunk_iterator(node)
                        .await
                        .map_ok(move |ci| (path.clone(), ci)))
                });
                Ok(futures::future::Either::Right(res.try_flatten()))
            }
        }
    }

    /// Warning: The presence of a single error item may mean multiple missing items
    async fn node_chunk_iterator(
        &self,
        node: NodeSnapshot,
    ) -> impl Stream<Item = DatasetResult<ChunkInfo>> + '_ {
        match node.node_data {
            NodeData::Group => futures::future::Either::Left(futures::stream::empty()),
            NodeData::Array(_, manifests) => {
                let new_chunk_indices: Box<HashSet<&ChunkIndices>> = Box::new(
                    self.change_set
                        .array_chunks_iterator(node.id)
                        .map(|(idx, _)| idx)
                        .collect(),
                );

                let new_chunks = self
                    .change_set
                    .array_chunks_iterator(node.id)
                    .filter_map(move |(idx, payload)| {
                        payload.as_ref().map(|payload| {
                            Ok(ChunkInfo {
                                node: node.id,
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
                                async move {
                                    let manifest = self
                                        .storage
                                        .fetch_manifests(&manifest_ref.object_id)
                                        .await;
                                    match manifest {
                                        Ok(manifest) => {
                                            let old_chunks = manifest
                                                .iter(
                                                    Some(manifest_ref.location.start()),
                                                    Some(manifest_ref.location.end()),
                                                )
                                                .filter_ok(move |c| {
                                                    !new_chunk_indices.contains(&c.coord)
                                                });

                                            let old_chunks = self.update_existing_chunks(
                                                node.id, old_chunks,
                                            );
                                            futures::future::Either::Left(
                                                futures::stream::iter(old_chunks)
                                                    // convert error types
                                                    .map_err(|e| e.into()),
                                            )
                                        }
                                        // if we cannot even fetch the manifest, we generate a
                                        // single error value.
                                        Err(err) => futures::future::Either::Right(
                                            futures::stream::once(ready(Err(
                                                DatasetError::StorageError(err),
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

    fn update_existing_chunks<'a, E>(
        &'a self,
        node: NodeId,
        chunks: impl Iterator<Item = Result<ChunkInfo, E>> + 'a,
    ) -> impl Iterator<Item = Result<ChunkInfo, E>> + 'a {
        chunks.filter_map_ok(move |chunk| {
            match self.change_set.get_chunk_ref(node, &chunk.coord) {
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
    ) -> DatasetResult<impl Iterator<Item = DatasetResult<NodeSnapshot>> + 'a> {
        // TODO: solve this duplication, there is always the possibility of this being the first
        // version
        match &self.snapshot_id {
            None => Ok(Either::Left(iter::empty())),
            Some(id) => Ok(Either::Right(
                self.storage
                    .fetch_snapshot(id)
                    .await?
                    .iter_arc()
                    .map(|maybe_node| match maybe_node {
                        Ok(n) => Ok(n),
                        Err(e) => Err(e.into()),
                    })
                    .map_ok(move |node| {
                        let new_manifests = if node.node_type() == NodeType::Array {
                            //FIXME: it could be none for empty arrays
                            Some(vec![ManifestRef {
                                object_id: manifest_id.clone(),
                                location: TableRegion::new(0, 1).unwrap(),
                                flags: Flags(),
                                extents: ManifestExtents(vec![]),
                            }])
                        } else {
                            None
                        };
                        self.update_existing_node(node, new_manifests)
                    }),
            )),
        }
    }

    fn new_nodes<'a>(
        &'a self,
        manifest_id: &'a ObjectId,
    ) -> impl Iterator<Item = NodeSnapshot> + 'a {
        self.change_set.new_nodes().map(move |path| {
            // we should be able to create the full node because we
            // know it's a new node
            #[allow(clippy::expect_used)]
            let node = self.get_new_node(path).expect("Bug in new_nodes implementation");
            match node.node_data {
                NodeData::Group => node,
                NodeData::Array(meta, _no_manifests_yet) => {
                    let new_manifests = vec![ManifestRef {
                        object_id: manifest_id.clone(),
                        location: TableRegion::new(0, 1).unwrap(),
                        flags: Flags(),
                        extents: ManifestExtents(vec![]),
                    }];
                    NodeSnapshot {
                        node_data: NodeData::Array(meta, new_manifests),
                        ..node
                    }
                }
            }
        })
    }

    async fn updated_nodes<'a>(
        &'a self,
        manifest_id: &'a ObjectId,
    ) -> DatasetResult<impl Iterator<Item = DatasetResult<NodeSnapshot>> + 'a> {
        Ok(self
            .updated_existing_nodes(manifest_id)
            .await?
            .chain(self.new_nodes(manifest_id).map(Ok)))
    }

    fn update_existing_node(
        &self,
        node: NodeSnapshot,
        new_manifests: Option<Vec<ManifestRef>>,
    ) -> NodeSnapshot {
        let session_atts = self
            .change_set
            .get_user_attributes(node.id)
            .cloned()
            .map(|a| a.map(UserAttributesSnapshot::Inline));
        let new_atts = session_atts.unwrap_or(node.user_attributes);
        match node.node_data {
            NodeData::Group => NodeSnapshot { user_attributes: new_atts, ..node },
            NodeData::Array(old_zarr_meta, _) => {
                let new_zarr_meta = self
                    .change_set
                    .get_updated_zarr_metadata(node.id)
                    .cloned()
                    .unwrap_or(old_zarr_meta);

                NodeSnapshot {
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

    pub async fn list_nodes(
        &self,
    ) -> DatasetResult<impl Iterator<Item = DatasetResult<NodeSnapshot>> + '_> {
        self.updated_nodes(&ObjectId::FAKE).await
    }

    pub async fn all_chunks(
        &self,
    ) -> DatasetResult<impl Stream<Item = DatasetResult<(PathBuf, ChunkInfo)>> + '_> {
        let existing_array_chunks = self.updated_chunk_iterator().await?;
        let new_array_chunks =
            futures::stream::iter(self.change_set.new_arrays_chunk_iterator().map(Ok));
        Ok(existing_array_chunks.chain(new_array_chunks))
    }

    /// After changes to the dataset have been made, this generates and writes to `Storage` the updated datastructures.
    ///
    /// After calling this, changes are reset and the [Dataset] can continue to be used for further
    /// changes.
    ///
    /// Returns the `ObjectId` of the new Snapshot file. It's the callers responsibility to commit
    /// this id change.
    pub async fn flush(&mut self) -> DatasetResult<ObjectId> {
        let (old_manifest_id, manifest_db) = match self.snapshot_id.as_ref() {
            None => {
                let manifest_id = ObjectId::random();
                let db_path = self.db_dir(&manifest_id);
                (
                    manifest_id,
                    Arc::new(open_db::<SingleThreaded>(
                        db_path.as_path(),
                        &self.rocksdb_env,
                    )?),
                )
            }
            Some(snapshot_id) => {
                let snapshot = self.storage.fetch_snapshot(snapshot_id).await?;
                // FIXME: currently only looking for the first manifest
                let manifest_id = snapshot.iter_arc().find_map(|node| {
                    node.ok().and_then(|node| match node.node_data {
                        NodeData::Array(_, man) => {
                            // TODO: can we avoid clone
                            man.first().map(|manifest| manifest.object_id.clone())
                        }
                        NodeData::Group => None,
                    })
                });
                match manifest_id {
                    Some(ref manifest_id) => {
                        (manifest_id.clone(), self.get_manifest(manifest_id).await?)
                    }
                    None => {
                        let manifest_id = ObjectId::random();
                        let db_path = self.db_dir(&manifest_id);
                        (
                            manifest_id,
                            Arc::new(open_db(db_path.as_path(), &self.rocksdb_env)?),
                        )
                    }
                }
            }
        };

        // FIXME: this is an ugly and expensive clone, but a tricky one. We could turn change_set
        // into an Arc<Mutex<_>> instead
        let chunk_changes = self.change_set.set_chunks.clone();
        let manifest_db_c = Arc::clone(&manifest_db);

        task::spawn_blocking(move || {
            // fixme should we use stricter options for the tx?
            // fixme async
            let tx = manifest_db_c.transaction();
            for (node_id, chunks) in chunk_changes.iter() {
                for (coord, maybe_payload) in chunks.iter() {
                    match maybe_payload {
                        Some(payload) => {
                            // a chunk was updated or inserted
                            write_chunk_payload(&tx, node_id, coord, payload);
                        }
                        None => {
                            // a chunk was deleted
                            delete_chunk(&tx, node_id, coord);
                        }
                    }
                }
            }
            tx.commit();

            let mut wait_opts = WaitForCompactOptions::default();
            wait_opts.set_flush(true);
            manifest_db_c.wait_for_compact(&wait_opts);
            // FIXME: is this enough to guarantee we can tar the db?
        })
        .await
        .unwrap();

        let new_manifest_id = ObjectId::random();
        // fixme: clone
        let writer = self.storage.manifest_writer(new_manifest_id.clone()).await?;
        let writer = writer;

        let db_path = self.db_dir(&old_manifest_id);
        // fixme close database
        let mut writer = compress_db(db_path.as_ref(), writer).await?;
        writer.close().await?;

        let all_nodes = self.updated_nodes(&new_manifest_id).await?;
        let new_snapshot = match mk_snapshot_table(all_nodes) {
            Ok(str) => str,
            Err(Ok(e)) => Err(e)?,
            Err(Err(arrow_error)) => Err(DatasetError::ArrowError(arrow_error))?,
        };
        let new_snapshot_id = ObjectId::random();
        self.storage
            .write_snapshot(new_snapshot_id.clone(), Arc::new(new_snapshot))
            .await?;

        self.snapshot_id = Some(new_snapshot_id.clone());
        self.change_set = ChangeSet::default();
        Ok(new_snapshot_id)
    }

    pub async fn commit(
        &mut self,
        update_branch_name: &str,
        message: &str,
    ) -> DatasetResult<(ObjectId, BranchVersion)> {
        let current = fetch_branch_tip(self.storage.as_ref(), update_branch_name).await;
        match current {
            Err(RefError::RefNotFound(_)) => {
                if self.snapshot_id.is_none() {
                    self.do_commit(update_branch_name, message).await
                } else {
                    Err(DatasetError::Conflict {
                        expected_parent: self.snapshot_id.clone(),
                        actual_parent: None,
                    })
                }
            }
            Err(err) => Err(err.into()),
            Ok(ref_data) => {
                // we can detect there will be a conflict before generating the new snapshot
                if Some(&ref_data.snapshot) != self.snapshot_id.as_ref() {
                    Err(DatasetError::Conflict {
                        expected_parent: self.snapshot_id.clone(),
                        actual_parent: Some(ref_data.snapshot.clone()),
                    })
                } else {
                    self.do_commit(update_branch_name, message).await
                }
            }
        }
    }

    async fn do_commit(
        &mut self,
        update_branch_name: &str,
        message: &str,
    ) -> DatasetResult<(ObjectId, BranchVersion)> {
        let parent_snaphsot = self.snapshot_id.clone();
        let new_snapshot = self.flush().await?;
        let now = Utc::now();
        let properties =
            HashMap::from_iter(vec![("message".to_string(), Value::from(message))]);

        match update_branch(
            self.storage.as_ref(),
            update_branch_name,
            new_snapshot.clone(),
            parent_snaphsot.as_ref(),
            now,
            properties,
        )
        .await
        {
            Ok(branch_version) => Ok((new_snapshot, branch_version)),
            Err(RefError::Conflict { expected_parent, actual_parent }) => {
                Err(DatasetError::Conflict { expected_parent, actual_parent })
            }
            Err(err) => Err(err.into()),
        }
    }
}

async fn new_materialized_chunk(
    storage: &(dyn Storage + Send + Sync),
    data: Bytes,
) -> DatasetResult<ChunkPayload> {
    let new_id = ObjectId::random();
    storage.write_chunk(new_id.clone(), data.clone()).await?;
    Ok(ChunkPayload::Ref(ChunkRef { id: new_id, offset: 0, length: data.len() as u64 }))
}

fn new_inline_chunk(data: Bytes) -> ChunkPayload {
    ChunkPayload::Inline(data)
}

#[cfg(test)]
#[allow(clippy::panic, clippy::unwrap_used, clippy::expect_used)]
mod tests {

    use std::{convert::Infallible, error::Error, num::NonZeroU64, path::PathBuf};

    use crate::{
        format::{
            manifest::{mk_manifests_table, ChunkInfo, ChunkRef, ManifestExtents},
            snapshot::mk_snapshot_table,
            Flags, TableRegion,
        },
        metadata::{
            ChunkKeyEncoding, ChunkShape, Codec, DataType, FillValue, StorageTransformer,
        },
        refs::{fetch_ref, Ref},
        storage::ObjectStorage,
        strategies::*,
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
        let matches = matches!(
            dataset.add_group(path.clone()).await.unwrap_err(),
            DatasetError::AlreadyExists{node, ..} if node.path == path
        );
        prop_assert!(matches);

        // deleting the added group must succeed
        prop_assert!(dataset.delete_group(path.clone()).await.is_ok());

        // deleting twice must fail
        let matches = matches!(
            dataset.delete_group(path.clone()).await.unwrap_err(),
            DatasetError::NotFound{path: reported_path, ..} if reported_path == path
        );
        prop_assert!(matches);

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
        let matches = matches!(
            dataset.delete_array(path.clone()).await.unwrap_err(),
            DatasetError::NotFound{path: reported_path, ..} if reported_path == path
        );
        prop_assert!(matches);

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
        let matches = matches!(
            dataset.add_group(path.clone()).await.unwrap_err(),
            DatasetError::AlreadyExists{node, ..} if node.path == path
        );
        prop_assert!(matches);

        let matches = matches!(
            dataset.delete_group(path.clone()).await.unwrap_err(),
            DatasetError::NotAGroup{node, ..} if node.path == path
        );
        prop_assert!(matches);
        prop_assert!(dataset.delete_array(path.clone()).await.is_ok());

        // adding an array at an existing group node must fail
        prop_assert!(dataset.add_group(path.clone()).await.is_ok());
        let matches = matches!(
            dataset.add_array(path.clone(), metadata.clone()).await.unwrap_err(),
            DatasetError::AlreadyExists{node, ..} if node.path == path
        );
        prop_assert!(matches);
        let matches = matches!(
            dataset.delete_array(path.clone()).await.unwrap_err(),
            DatasetError::NotAnArray{node, ..} if node.path == path
        );
        prop_assert!(matches);
        prop_assert!(dataset.delete_group(path.clone()).await.is_ok());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_dataset_with_updates() -> Result<(), Box<dyn Error>> {
        let storage = ObjectStorage::new_in_memory_store();

        let array_id = 2;
        let chunk1 = ChunkInfo {
            node: array_id,
            coord: ChunkIndices(vec![0, 0, 0]),
            payload: ChunkPayload::Ref(ChunkRef {
                id: ObjectId::random(),
                offset: 0,
                length: 4,
            }),
        };

        let chunk2 = ChunkInfo {
            node: array_id,
            coord: ChunkIndices(vec![0, 0, 1]),
            payload: ChunkPayload::Inline("hello".into()),
        };

        let manifest = Arc::new(
            mk_manifests_table::<Infallible>(futures::stream::iter(vec![
                Ok(chunk1.clone()),
                Ok(chunk2.clone()),
            ]))
            .await
            .unwrap(),
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
            location: TableRegion::new(0, 2).unwrap(),
            flags: Flags(),
            extents: ManifestExtents(vec![]),
        };
        let array1_path: PathBuf = "/array1".to_string().into();
        let nodes = vec![
            Ok(NodeSnapshot {
                path: "/".into(),
                id: 1,
                user_attributes: None,
                node_data: NodeData::Group,
            }),
            Ok(NodeSnapshot {
                path: array1_path.clone(),
                id: array_id,
                user_attributes: Some(UserAttributesSnapshot::Inline(
                    UserAttributes::try_new(br#"{"foo":1}"#).unwrap(),
                )),
                node_data: NodeData::Array(zarr_meta1.clone(), vec![manifest_ref]),
            }),
        ];

        let snapshot = Arc::new(mk_snapshot_table::<Infallible>(nodes.clone()).unwrap());
        let snapshot_id = ObjectId::random();
        storage.write_snapshot(snapshot_id.clone(), snapshot).await?;
        let mut ds = Dataset::update(Arc::new(storage), snapshot_id)
            .with_inline_threshold_bytes(512)
            .build();

        // retrieve the old array node
        let node = ds.get_node(&array1_path).await?;
        assert_eq!(nodes.get(1).unwrap().as_ref().unwrap(), &node);

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
            Some(NodeSnapshot {
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
            Some(NodeSnapshot {
                path: "/group/array2".into(),
                id: 6,
                user_attributes: Some(UserAttributesSnapshot::Inline(
                    UserAttributes::try_new(br#"{"n":42}"#).unwrap()
                )),
                node_data: NodeData::Array(zarr_meta2.clone(), vec![]),
            }),
        );

        ds.set_chunk(
            &new_array_path,
            &ChunkIndices(vec![0]),
            Bytes::copy_from_slice(b"foo"),
        )
        .await?;

        let chunk = ds.get_chunk_ref(&new_array_path, &ChunkIndices(vec![0])).await?;
        assert_eq!(chunk, Some(ChunkPayload::Inline("foo".into())));

        // retrieve a non initialized chunk of the new array
        let non_chunk = ds.get_chunk_ref(&new_array_path, &ChunkIndices(vec![1])).await?;
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
            Some(UserAttributesSnapshot::Inline(
                UserAttributes::try_new(br#"{"updated": true}"#).unwrap()
            ))
        );

        // update old array zarr metadata and check it
        let new_zarr_meta1 = ZarrArrayMetadata { shape: vec![2, 2, 3], ..zarr_meta1 };
        ds.update_array(array1_path.clone(), new_zarr_meta1).await?;
        let node = ds.get_node(&array1_path).await;
        if let Ok(NodeSnapshot {
            node_data: NodeData::Array(ZarrArrayMetadata { shape, .. }, _),
            ..
        }) = &node
        {
            assert_eq!(shape, &vec![2, 2, 3]);
        } else {
            panic!("Failed to update zarr metadata");
        }

        // set old array chunk and check them
        let data = Bytes::copy_from_slice(b"foo".repeat(512).as_slice());
        ds.set_chunk(&array1_path, &ChunkIndices(vec![0, 0, 0]), data.clone()).await?;

        let chunk = ds.get_chunk(&array1_path, &ChunkIndices(vec![0, 0, 0])).await?;
        assert_eq!(chunk, Some(data));

        let path: Path = "/group/array2".into();
        let node = ds.get_node(&path).await;
        assert!(ds
            .change_set
            .updated_attributes
            .contains_key(&node.as_ref().unwrap().id));
        assert!(ds.delete_array(path.clone()).await.is_ok());
        assert!(!ds.change_set.updated_attributes.contains_key(&node?.id));

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

        change_set.set_chunk_ref(1, ChunkIndices(vec![0, 1]), None);
        assert_eq!(None, change_set.new_arrays_chunk_iterator().next());

        change_set.set_chunk_ref(
            1,
            ChunkIndices(vec![1, 0]),
            Some(ChunkPayload::Inline("bar1".into())),
        );
        change_set.set_chunk_ref(
            1,
            ChunkIndices(vec![1, 1]),
            Some(ChunkPayload::Inline("bar2".into())),
        );
        change_set.set_chunk_ref(
            2,
            ChunkIndices(vec![0]),
            Some(ChunkPayload::Inline("baz1".into())),
        );
        change_set.set_chunk_ref(
            2,
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
                    "foo/baz".into(),
                    ChunkInfo {
                        node: 2,
                        coord: ChunkIndices(vec![0]),
                        payload: ChunkPayload::Inline("baz1".into()),
                    },
                ),
                (
                    "foo/baz".into(),
                    ChunkInfo {
                        node: 2,
                        coord: ChunkIndices(vec![1]),
                        payload: ChunkPayload::Inline("baz2".into()),
                    },
                ),
                (
                    "foo/bar".into(),
                    ChunkInfo {
                        node: 1,
                        coord: ChunkIndices(vec![1, 0]),
                        payload: ChunkPayload::Inline("bar1".into()),
                    },
                ),
                (
                    "foo/bar".into(),
                    ChunkInfo {
                        node: 1,
                        coord: ChunkIndices(vec![1, 1]),
                        payload: ChunkPayload::Inline("bar2".into()),
                    },
                ),
            ]
            .into();
            assert_eq!(all_chunks, expected_chunks);
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_dataset_with_updates_and_writes() -> Result<(), Box<dyn Error>> {
        let storage: Arc<dyn Storage + Send + Sync> =
            Arc::new(ObjectStorage::new_in_memory_store());
        let storage: Arc<dyn Storage + Send + Sync> = Arc::new(
            ObjectStorage::new_local_store(&PathBuf::from("/tmp/test-object-store"))
                .unwrap(),
        );
        let mut ds = Dataset::create(Arc::clone(&storage)).build();

        // add a new array and retrieve its node
        ds.add_group("/".into()).await?;
        let snapshot_id = ds.flush().await?;

        assert_eq!(Some(snapshot_id), ds.snapshot_id);
        assert_eq!(
            ds.get_node(&"/".into()).await.ok(),
            Some(NodeSnapshot {
                id: 1,
                path: "/".into(),
                user_attributes: None,
                node_data: NodeData::Group
            })
        );
        ds.add_group("/group".into()).await?;
        let _snapshot_id = ds.flush().await?;
        assert_eq!(
            ds.get_node(&"/".into()).await.ok(),
            Some(NodeSnapshot {
                id: 1,
                path: "/".into(),
                user_attributes: None,
                node_data: NodeData::Group
            })
        );
        assert_eq!(
            ds.get_node(&"/group".into()).await.ok(),
            Some(NodeSnapshot {
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

        dbg!("1");
        let new_array_path: PathBuf = "/group/array1".to_string().into();
        ds.add_array(new_array_path.clone(), zarr_meta.clone()).await?;

        // no commit to test the case of a chunkless array
        let _snapshot_id = ds.flush().await?;

        // we set a chunk in a new array
        ds.set_chunk_ref(
            new_array_path.clone(),
            ChunkIndices(vec![0, 0, 0]),
            Some(ChunkPayload::Inline("hello".into())),
        )
        .await?;

        dbg!("2");
        let _snapshot_id = ds.flush().await?;
        assert_eq!(
            ds.get_node(&"/".into()).await.ok(),
            Some(NodeSnapshot {
                id: 1,
                path: "/".into(),
                user_attributes: None,
                node_data: NodeData::Group
            })
        );
        assert_eq!(
            ds.get_node(&"/group".into()).await.ok(),
            Some(NodeSnapshot {
                id: 2,
                path: "/group".into(),
                user_attributes: None,
                node_data: NodeData::Group
            })
        );
        assert!(matches!(
            ds.get_node(&new_array_path).await.ok(),
            Some(NodeSnapshot {
                id: 3,
                path,
                user_attributes: None,
                node_data: NodeData::Array(meta, manifests)
            }) if path == new_array_path && meta == zarr_meta.clone() && manifests.len() == 1
        ));
        assert_eq!(
            ds.get_chunk_ref(&new_array_path, &ChunkIndices(vec![0, 0, 0])).await?,
            Some(ChunkPayload::Inline("hello".into()))
        );

        dbg!("!!!!!!!!!!!!!!!!!!!!!!!!");
        // we modify a chunk in an existing array
        ds.set_chunk_ref(
            new_array_path.clone(),
            ChunkIndices(vec![0, 0, 0]),
            Some(ChunkPayload::Inline("bye".into())),
        )
        .await?;

        dbg!("3");
        // we add a new chunk in an existing array
        ds.set_chunk_ref(
            new_array_path.clone(),
            ChunkIndices(vec![0, 0, 1]),
            Some(ChunkPayload::Inline("new chunk".into())),
        )
        .await?;

        let previous_snapshot_id = ds.flush().await?;
        assert_eq!(
            ds.get_chunk_ref(&new_array_path, &ChunkIndices(vec![0, 0, 0])).await?,
            Some(ChunkPayload::Inline("bye".into()))
        );
        assert_eq!(
            ds.get_chunk_ref(&new_array_path, &ChunkIndices(vec![0, 0, 1])).await?,
            Some(ChunkPayload::Inline("new chunk".into()))
        );

        dbg!("4");
        // we delete a chunk
        ds.set_chunk_ref(new_array_path.clone(), ChunkIndices(vec![0, 0, 1]), None)
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
        dbg!("5");

        let snapshot_id = ds.flush().await?;
        let ds = Dataset::update(Arc::clone(&storage), snapshot_id).build();

        dbg!("6");
        assert_eq!(
            ds.get_chunk_ref(&new_array_path, &ChunkIndices(vec![0, 0, 0])).await?,
            Some(ChunkPayload::Inline("bye".into()))
        );
        dbg!("7");
        assert_eq!(
            ds.get_chunk_ref(&new_array_path, &ChunkIndices(vec![0, 0, 1])).await?,
            None
        );
        dbg!("8");
        assert!(matches!(
            ds.get_node(&new_array_path).await.ok(),
            Some(NodeSnapshot {
                id: 3,
                path,
                user_attributes: Some(atts),
                node_data: NodeData::Array(meta, manifests)
            }) if path == new_array_path && meta == new_meta.clone() &&
                    manifests.len() == 1 &&
                    atts == UserAttributesSnapshot::Inline(UserAttributes::try_new(br#"{"foo":42}"#).unwrap())
        ));

        dbg!("9");
        //test the previous version is still alive
        let ds = Dataset::update(Arc::clone(&storage), previous_snapshot_id).build();
        dbg!("10");
        assert_eq!(
            ds.get_chunk_ref(&new_array_path, &ChunkIndices(vec![0, 0, 0])).await?,
            Some(ChunkPayload::Inline("bye".into()))
        );
        dbg!("11");
        assert_eq!(
            ds.get_chunk_ref(&new_array_path, &ChunkIndices(vec![0, 0, 1])).await?,
            Some(ChunkPayload::Inline("new chunk".into()))
        );

        dbg!("12");
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_commit_and_refs() -> Result<(), Box<dyn Error>> {
        let storage: Arc<dyn Storage + Send + Sync> =
            Arc::new(ObjectStorage::new_in_memory_store());
        let mut ds = Dataset::create(Arc::clone(&storage)).build();

        // add a new array and retrieve its node
        ds.add_group("/".into()).await?;
        let (new_snapshot_id, version) = ds.commit("main", "first commit").await?;
        assert_eq!(version, BranchVersion(0));
        assert_eq!(
            new_snapshot_id,
            fetch_ref(storage.as_ref(), "main").await?.1.snapshot
        );

        assert_eq!(
            ds.get_node(&"/".into()).await.ok(),
            Some(NodeSnapshot {
                id: 1,
                path: "/".into(),
                user_attributes: None,
                node_data: NodeData::Group
            })
        );

        let mut ds =
            Dataset::from_branch_tip(Arc::clone(&storage), "main").await?.build();
        assert_eq!(
            ds.get_node(&"/".into()).await.ok(),
            Some(NodeSnapshot {
                id: 1,
                path: "/".into(),
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

        let new_array_path: PathBuf = "/array1".to_string().into();
        ds.add_array(new_array_path.clone(), zarr_meta.clone()).await?;
        ds.set_chunk_ref(
            new_array_path.clone(),
            ChunkIndices(vec![0, 0, 0]),
            Some(ChunkPayload::Inline("hello".into())),
        )
        .await?;
        let (new_snapshot_id, version) = ds.commit("main", "second commit").await?;
        assert_eq!(version, BranchVersion(1));
        let (ref_name, ref_data) = fetch_ref(storage.as_ref(), "main").await?;
        assert_eq!(ref_name, Ref::Branch("main".to_string()));
        assert_eq!(new_snapshot_id, ref_data.snapshot);
        assert_eq!("second commit", ref_data.properties["message"]);

        Ok(())
    }

    #[cfg(test)]
    mod state_machine_test {
        use crate::format::snapshot::NodeData;
        use crate::format::Path;
        use crate::Dataset;
        use crate::ObjectStorage;
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

        use proptest::test_runner::Config;

        use super::ZarrArrayMetadata;
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
                Just(Default::default()).boxed()
            }

            fn transitions(state: &Self::State) -> BoxedStrategy<Self::Transition> {
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
                let storage = ObjectStorage::new_in_memory_store();
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
            verbose: 0,
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
