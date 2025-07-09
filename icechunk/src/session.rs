use async_stream::try_stream;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use err_into::ErrorInto;
use futures::{FutureExt, Stream, StreamExt, TryStreamExt, future::Either, stream};
use itertools::{Itertools as _, enumerate, repeat_n};
use regex::bytes::Regex;
use serde::{Deserialize, Serialize};
use std::{
    cmp::min,
    collections::{HashMap, HashSet},
    convert::Infallible,
    future::{Future, ready},
    ops::Range,
    pin::Pin,
    sync::Arc,
};
use thiserror::Error;
use tokio::task::JoinError;
use tracing::{Instrument, debug, info, instrument, trace, warn};

use crate::{
    RepositoryConfig, Storage, StorageError,
    asset_manager::AssetManager,
    change_set::{ArrayData, ChangeSet},
    config::{ManifestSplitDim, ManifestSplitDimCondition, ManifestSplittingConfig},
    conflicts::{Conflict, ConflictResolution, ConflictSolver},
    error::ICError,
    format::{
        ByteRange, ChunkIndices, ChunkOffset, IcechunkFormatError,
        IcechunkFormatErrorKind, ManifestId, NodeId, ObjectId, Path, SnapshotId,
        manifest::{
            ChunkInfo, ChunkPayload, ChunkRef, Manifest, ManifestExtents, ManifestRef,
            ManifestSplits, Overlap, VirtualChunkLocation, VirtualChunkRef,
            VirtualReferenceError, VirtualReferenceErrorKind,
            uniform_manifest_split_edges,
        },
        snapshot::{
            ArrayShape, DimensionName, ManifestFileInfo, NodeData, NodeSnapshot,
            NodeType, Snapshot, SnapshotProperties,
        },
        transaction_log::{Diff, DiffBuilder, TransactionLog},
    },
    refs::{RefError, RefErrorKind, fetch_branch_tip, update_branch},
    repository::{RepositoryError, RepositoryErrorKind},
    storage::{self, StorageErrorKind},
    virtual_chunks::{VirtualChunkContainer, VirtualChunkResolver},
};

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum SessionErrorKind {
    #[error(transparent)]
    RepositoryError(RepositoryErrorKind),
    #[error(transparent)]
    StorageError(StorageErrorKind),
    #[error(transparent)]
    FormatError(IcechunkFormatErrorKind),
    #[error(transparent)]
    Ref(RefErrorKind),
    #[error(transparent)]
    VirtualReferenceError(VirtualReferenceErrorKind),

    #[error("Read only sessions cannot modify the repository")]
    ReadOnlySession,
    #[error("snapshot not found: `{id}`")]
    SnapshotNotFound { id: SnapshotId },
    #[error("no ancestor node was found for `{prefix}`")]
    AncestorNodeNotFound { prefix: Path },
    #[error("node not found at `{path}`: {message}")]
    NodeNotFound { path: Path, message: String },
    #[error("there is not an array at `{node:?}`: {message}")]
    NotAnArray { node: Box<NodeSnapshot>, message: String },
    #[error("there is not a group at `{node:?}`: {message}")]
    NotAGroup { node: Box<NodeSnapshot>, message: String },
    #[error("node already exists at `{node:?}`: {message}")]
    AlreadyExists { node: Box<NodeSnapshot>, message: String },
    #[error("cannot commit, no changes made to the session")]
    NoChangesToCommit,
    #[error("invalid snapshot timestamp ordering. parent: `{parent}`, child: `{child}` ")]
    InvalidSnapshotTimestampOrdering { parent: DateTime<Utc>, child: DateTime<Utc> },
    #[error(
        "snapshot timestamp is invalid: timestamp: `{snapshot_time}`, object store clock: `{object_store_time}` "
    )]
    InvalidSnapshotTimestamp {
        object_store_time: DateTime<Utc>,
        snapshot_time: DateTime<Utc>,
    },
    #[error("unknown flush error")]
    OtherFlushError,
    #[error("a concurrent task failed")]
    ConcurrencyError(#[from] JoinError),
    #[error("branch update conflict: `({expected_parent:?}) != ({actual_parent:?})`")]
    Conflict { expected_parent: Option<SnapshotId>, actual_parent: Option<SnapshotId> },
    #[error("cannot rebase snapshot {snapshot} on top of the branch")]
    RebaseFailed { snapshot: SnapshotId, conflicts: Vec<Conflict> },
    #[error("error in serializing config to JSON")]
    JsonSerializationError(#[from] serde_json::Error),
    #[error("error in session serialization")]
    SerializationError(#[from] Box<rmp_serde::encode::Error>),
    #[error("error in session deserialization")]
    DeserializationError(#[from] Box<rmp_serde::decode::Error>),
    #[error(
        "error finding conflicting path for node `{0}`, this probably indicades a bug in `rebase`"
    )]
    ConflictingPathNotFound(NodeId),
    #[error(
        "invalid chunk index: coordinates {coords:?} are not valid for array at {path}"
    )]
    InvalidIndex { coords: ChunkIndices, path: Path },
    #[error("invalid chunk index for splitting manifests: {coords:?}")]
    InvalidIndexForSplitManifests { coords: ChunkIndices },
    #[error("incompatible manifest splitting config when merging two sessions")]
    IncompatibleSplittingConfig {
        ours: ManifestSplittingConfig,
        theirs: ManifestSplittingConfig,
    },
    #[error("`to` snapshot ancestry doesn't include `from`")]
    BadSnapshotChainForDiff,
    #[error("failed to create manifest from chunk stream")]
    ManifestCreationError(#[from] Box<SessionError>),
}

pub type SessionError = ICError<SessionErrorKind>;

// it would be great to define this impl in error.rs, but it conflicts with the blanket
// `impl From<T> for T`
impl<E> From<E> for SessionError
where
    E: Into<SessionErrorKind>,
{
    fn from(value: E) -> Self {
        Self::new(value.into())
    }
}

impl From<StorageError> for SessionError {
    fn from(value: StorageError) -> Self {
        Self::with_context(SessionErrorKind::StorageError(value.kind), value.context)
    }
}

impl From<RepositoryError> for SessionError {
    fn from(value: RepositoryError) -> Self {
        Self::with_context(SessionErrorKind::RepositoryError(value.kind), value.context)
    }
}

impl From<RefError> for SessionError {
    fn from(value: RefError) -> Self {
        Self::with_context(SessionErrorKind::Ref(value.kind), value.context)
    }
}

impl From<IcechunkFormatError> for SessionError {
    fn from(value: IcechunkFormatError) -> Self {
        Self::with_context(SessionErrorKind::FormatError(value.kind), value.context)
    }
}

impl From<VirtualReferenceError> for SessionError {
    fn from(value: VirtualReferenceError) -> Self {
        Self::with_context(
            SessionErrorKind::VirtualReferenceError(value.kind),
            value.context,
        )
    }
}

pub type SessionResult<T> = Result<T, SessionError>;

// Returns the index of split_range that includes ChunkIndices
// This can be used at write time to split manifests based on the config
// and at read time to choose which manifest to query for chunk payload
/// It is useful to have this act on an iterator (e.g. get_chunk_ref)
/// The find method on ManifestSplits is simply a helper.
pub fn find_coord<'a, I>(
    iter: I,
    coord: &'a ChunkIndices,
) -> Option<(usize, &'a ManifestExtents)>
where
    I: Iterator<Item = &'a ManifestExtents>,
{
    // split_range[i] must bound ChunkIndices
    // 0 <= return value <= split_range.len()
    // it is possible that split_range does not include a coord. say we have 2x2 split grid
    // but only split (0,0) and split (1,1) are populated with data.
    // A coord located in (1, 0) should return Err
    // Since split_range need not form a regular grid, we must iterate through and find the first result.
    // ManifestExtents in split_range MUST NOT overlap with each other. How do we ensure this?
    // ndim must be the same
    // Note: I don't think we can distinguish between out of bounds index for the array
    //       and an index that is part of a split that hasn't been written yet.
    enumerate(iter).find(|(_, e)| e.contains(coord.0.as_slice()))
}

impl ManifestSplits {
    pub fn find<'a>(&'a self, coord: &'a ChunkIndices) -> Option<&'a ManifestExtents> {
        debug_assert_eq!(coord.0.len(), self.0[0].len());
        find_coord(self.iter(), coord).map(|x| x.1)
    }

    pub fn position(&self, coord: &ChunkIndices) -> Option<usize> {
        debug_assert_eq!(coord.0.len(), self.0[0].len());
        find_coord(self.iter(), coord).map(|x| x.0)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Session {
    config: RepositoryConfig,
    storage_settings: Arc<storage::Settings>,
    storage: Arc<dyn Storage + Send + Sync>,
    asset_manager: Arc<AssetManager>,
    virtual_resolver: Arc<VirtualChunkResolver>,
    branch_name: Option<String>,
    snapshot_id: SnapshotId,
    change_set: ChangeSet,
    default_commit_metadata: SnapshotProperties,
    // This is an optimization so that we needn't figure out the split sizes on every set.
    splits: HashMap<NodeId, ManifestSplits>,
}

impl Session {
    pub fn create_readonly_session(
        config: RepositoryConfig,
        storage_settings: storage::Settings,
        storage: Arc<dyn Storage + Send + Sync>,
        asset_manager: Arc<AssetManager>,
        virtual_resolver: Arc<VirtualChunkResolver>,
        snapshot_id: SnapshotId,
    ) -> Self {
        Self {
            config,
            storage_settings: Arc::new(storage_settings),
            storage,
            asset_manager,
            virtual_resolver,
            branch_name: None,
            snapshot_id,
            change_set: ChangeSet::default(),
            default_commit_metadata: SnapshotProperties::default(),
            // Splits are populated for a node during
            // `add_array`, `update_array`, and `set_chunk_ref`
            splits: Default::default(),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn create_writable_session(
        config: RepositoryConfig,
        storage_settings: storage::Settings,
        storage: Arc<dyn Storage + Send + Sync>,
        asset_manager: Arc<AssetManager>,
        virtual_resolver: Arc<VirtualChunkResolver>,
        branch_name: String,
        snapshot_id: SnapshotId,
        default_commit_metadata: SnapshotProperties,
    ) -> Self {
        Self {
            config,
            storage_settings: Arc::new(storage_settings),
            storage,
            asset_manager,
            virtual_resolver,
            branch_name: Some(branch_name),
            snapshot_id,
            change_set: Default::default(),
            default_commit_metadata,
            splits: Default::default(),
        }
    }

    #[instrument(skip(bytes))]
    pub fn from_bytes(bytes: Vec<u8>) -> SessionResult<Self> {
        rmp_serde::from_slice(&bytes).map_err(Box::new).err_into()
    }

    #[instrument(skip(self))]
    pub fn as_bytes(&self) -> SessionResult<Vec<u8>> {
        rmp_serde::to_vec(self).map_err(Box::new).err_into()
    }

    pub fn branch(&self) -> Option<&str> {
        self.branch_name.as_deref()
    }

    pub fn read_only(&self) -> bool {
        self.branch_name.is_none()
    }

    pub fn snapshot_id(&self) -> &SnapshotId {
        &self.snapshot_id
    }

    pub fn has_uncommitted_changes(&self) -> bool {
        !self.change_set.is_empty()
    }

    pub fn changes(&self) -> &ChangeSet {
        &self.change_set
    }

    pub fn config(&self) -> &RepositoryConfig {
        &self.config
    }

    pub fn matching_container(
        &self,
        chunk_location: &VirtualChunkLocation,
    ) -> Option<&VirtualChunkContainer> {
        self.virtual_resolver.matching_container(chunk_location.0.as_str())
    }

    /// Compute an overview of the current session changes
    pub async fn status(&self) -> SessionResult<Diff> {
        // it doesn't really matter what Id we give to the tx log, it's not going to be persisted
        let tx_log = TransactionLog::new(&SnapshotId::random(), &self.change_set);
        let from_session = Self::create_readonly_session(
            self.config().clone(),
            self.storage_settings.as_ref().clone(),
            Arc::clone(&self.storage),
            Arc::clone(&self.asset_manager),
            Arc::clone(&self.virtual_resolver),
            self.snapshot_id.clone(),
        );
        let mut builder = DiffBuilder::default();
        builder.add_changes(&tx_log);
        builder.to_diff(&from_session, self).await
    }

    /// Add a group to the store.
    ///
    /// Calling this only records the operation in memory, doesn't have any consequence on the storage
    #[instrument(skip(self, definition))]
    pub async fn add_group(
        &mut self,
        path: Path,
        definition: Bytes,
    ) -> SessionResult<()> {
        match self.get_node(&path).await {
            Err(SessionError { kind: SessionErrorKind::NodeNotFound { .. }, .. }) => {
                let id = NodeId::random();
                self.change_set.add_group(path.clone(), id, definition);
                Ok(())
            }
            Ok(node) => Err(SessionErrorKind::AlreadyExists {
                node: Box::new(node),
                message: "trying to add group".to_string(),
            }
            .into()),
            Err(err) => Err(err),
        }
    }

    #[instrument(skip(self))]
    pub async fn delete_node(&mut self, node: NodeSnapshot) -> SessionResult<()> {
        match node {
            NodeSnapshot { node_data: NodeData::Group, path: node_path, .. } => {
                Ok(self.delete_group(node_path).await?)
            }
            NodeSnapshot {
                node_data: NodeData::Array { .. }, path: node_path, ..
            } => Ok(self.delete_array(node_path).await?),
        }
    }
    /// Delete a group in the hierarchy
    ///
    /// Deletes of non existing groups will succeed.
    #[instrument(skip(self))]
    pub async fn delete_group(&mut self, path: Path) -> SessionResult<()> {
        match self.get_group(&path).await {
            Ok(parent) => {
                let nodes_iter: Vec<NodeSnapshot> = self
                    .list_nodes()
                    .await?
                    .filter_ok(|node| node.path.starts_with(&parent.path))
                    .try_collect()?;
                for node in nodes_iter {
                    match node.node_type() {
                        NodeType::Group => {
                            self.change_set.delete_group(node.path, &node.id)
                        }
                        NodeType::Array => {
                            self.change_set.delete_array(node.path, &node.id)
                        }
                    }
                }
            }
            Err(SessionError { kind: SessionErrorKind::NodeNotFound { .. }, .. }) => {}
            Err(err) => Err(err)?,
        }
        Ok(())
    }

    /// Add an array to the store.
    ///
    /// Calling this only records the operation in memory, doesn't have any consequence on the storage
    #[instrument(skip(self, user_data))]
    pub async fn add_array(
        &mut self,
        path: Path,
        shape: ArrayShape,
        dimension_names: Option<Vec<DimensionName>>,
        user_data: Bytes,
    ) -> SessionResult<()> {
        match self.get_node(&path).await {
            Err(SessionError { kind: SessionErrorKind::NodeNotFound { .. }, .. }) => {
                let id = NodeId::random();
                self.cache_splits(&id, &path, &shape, &dimension_names);
                self.change_set.add_array(
                    path,
                    id,
                    ArrayData { shape, dimension_names, user_data },
                );
                Ok(())
            }
            Ok(node) => Err(SessionErrorKind::AlreadyExists {
                node: Box::new(node),
                message: "trying to add array".to_string(),
            }
            .into()),
            Err(err) => Err(err),
        }
    }

    // Updates an array Zarr metadata
    ///
    /// Calling this only records the operation in memory, doesn't have any consequence on the storage
    #[instrument(skip(self, user_data))]
    pub async fn update_array(
        &mut self,
        path: &Path,
        shape: ArrayShape,
        dimension_names: Option<Vec<DimensionName>>,
        user_data: Bytes,
    ) -> SessionResult<()> {
        self.get_array(path).await.map(|node| {
            // needed to handle a resize for example.
            self.cache_splits(&node.id, path, &shape, &dimension_names);
            self.change_set.update_array(
                &node.id,
                path,
                ArrayData { shape, dimension_names, user_data },
                #[allow(clippy::expect_used)]
                self.splits.get(&node.id).expect("getting splits should not fail."),
            )
        })
    }

    // Updates an group Zarr metadata
    ///
    /// Calling this only records the operation in memory, doesn't have any consequence on the storage
    #[instrument(skip(self, definition))]
    pub async fn update_group(
        &mut self,
        path: &Path,
        definition: Bytes,
    ) -> SessionResult<()> {
        self.get_group(path)
            .await
            .map(|node| self.change_set.update_group(&node.id, path, definition))
    }

    /// Delete an array in the hierarchy
    ///
    /// Deletes of non existing array will succeed.
    #[instrument(skip(self))]
    pub async fn delete_array(&mut self, path: Path) -> SessionResult<()> {
        match self.get_array(&path).await {
            Ok(node) => {
                self.change_set.delete_array(node.path, &node.id);
            }
            Err(SessionError { kind: SessionErrorKind::NodeNotFound { .. }, .. }) => {}
            Err(err) => Err(err)?,
        }
        Ok(())
    }

    #[instrument(skip(self, coords))]
    pub async fn delete_chunks(
        &mut self,
        node_path: &Path,
        coords: impl IntoIterator<Item = ChunkIndices>,
    ) -> SessionResult<()> {
        let node = self.get_array(node_path).await?;
        for coord in coords {
            self.set_node_chunk_ref(node.clone(), coord, None).await?
        }
        Ok(())
    }

    // Record the write, referencing or delete of a chunk
    //
    // Caller has to write the chunk before calling this.
    #[instrument(skip(self))]
    pub async fn set_chunk_ref(
        &mut self,
        path: Path,
        coord: ChunkIndices,
        data: Option<ChunkPayload>,
    ) -> SessionResult<()> {
        let node_snapshot = self.get_array(&path).await?;
        self.set_node_chunk_ref(node_snapshot, coord, data).await
    }

    pub fn lookup_splits(&self, node_id: &NodeId) -> Option<&ManifestSplits> {
        self.splits.get(node_id)
    }

    /// This method is directly called in add_array & update_array
    /// where we know we must update the splits HashMap
    fn cache_splits(
        &mut self,
        node_id: &NodeId,
        path: &Path,
        shape: &ArrayShape,
        dimension_names: &Option<Vec<DimensionName>>,
    ) {
        // Q: What happens if we set a chunk, then change a dimension name, so
        //    that the split changes.
        // A: we reorg the existing chunk refs in the changeset to the new splits.
        let splitting = self.config.manifest().splitting();
        let splits = splitting.get_split_sizes(path, shape, dimension_names);
        self.splits.insert(node_id.clone(), splits);
    }

    fn get_splits(
        &mut self,
        node_id: &NodeId,
        path: &Path,
        shape: &ArrayShape,
        dimension_names: &Option<Vec<DimensionName>>,
    ) -> &ManifestSplits {
        self.splits.entry(node_id.clone()).or_insert_with(|| {
            self.config.manifest().splitting().get_split_sizes(
                path,
                shape,
                dimension_names,
            )
        })
    }

    // Helper function that accepts a NodeSnapshot instead of a path,
    // this lets us do bulk sets (and deletes) without repeatedly grabbing the node.
    #[instrument(skip(self))]
    async fn set_node_chunk_ref(
        &mut self,
        node: NodeSnapshot,
        coord: ChunkIndices,
        data: Option<ChunkPayload>,
    ) -> SessionResult<()> {
        if let NodeData::Array { shape, dimension_names, .. } = node.node_data {
            if shape.valid_chunk_coord(&coord) {
                let splits = self
                    .get_splits(&node.id, &node.path, &shape, &dimension_names)
                    .clone();
                self.change_set.set_chunk_ref(node.id, coord, data, &splits);
                Ok(())
            } else {
                Err(SessionErrorKind::InvalidIndex {
                    coords: coord,
                    path: node.path.clone(),
                }
                .into())
            }
        } else {
            Err(SessionErrorKind::NotAnArray {
                node: Box::new(node.clone()),
                message: "getting an array".to_string(),
            }
            .into())
        }
    }

    #[instrument(skip(self))]
    pub async fn get_closest_ancestor_node(
        &self,
        path: &Path,
    ) -> SessionResult<NodeSnapshot> {
        let mut ancestors = path.ancestors();
        // the first element is the `path` itself, which we have already tested
        ancestors.next();
        for parent in ancestors {
            let node = self.get_node(&parent).await;
            if node.is_ok() {
                return node;
            }
        }
        Err(SessionErrorKind::AncestorNodeNotFound { prefix: path.clone() }.into())
    }

    #[instrument(skip(self))]
    pub async fn get_node(&self, path: &Path) -> SessionResult<NodeSnapshot> {
        get_node(&self.asset_manager, &self.change_set, self.snapshot_id(), path).await
    }

    pub async fn get_array(&self, path: &Path) -> SessionResult<NodeSnapshot> {
        match self.get_node(path).await {
            res @ Ok(NodeSnapshot { node_data: NodeData::Array { .. }, .. }) => res,
            Ok(node @ NodeSnapshot { .. }) => Err(SessionErrorKind::NotAnArray {
                node: Box::new(node),
                message: "getting an array".to_string(),
            }
            .into()),
            other => other,
        }
    }

    pub async fn get_group(&self, path: &Path) -> SessionResult<NodeSnapshot> {
        match self.get_node(path).await {
            res @ Ok(NodeSnapshot { node_data: NodeData::Group, .. }) => res,
            Ok(node @ NodeSnapshot { .. }) => Err(SessionErrorKind::NotAGroup {
                node: Box::new(node),
                message: "getting a group".to_string(),
            }
            .into()),
            other => other,
        }
    }

    #[instrument(skip(self))]
    pub async fn array_chunk_iterator<'a>(
        &'a self,
        path: &Path,
    ) -> impl Stream<Item = SessionResult<ChunkInfo>> + 'a + use<'a> {
        node_chunk_iterator(
            &self.asset_manager,
            &self.change_set,
            &self.snapshot_id,
            path,
            ManifestExtents::ALL,
        )
        .await
    }

    #[instrument(skip(self))]
    pub async fn get_chunk_ref(
        &self,
        path: &Path,
        coords: &ChunkIndices,
    ) -> SessionResult<Option<ChunkPayload>> {
        let node = self.get_node(path).await?;
        // TODO: it's ugly to have to do this destructuring even if we could be calling `get_array`
        // get_array should return the array data, not a node
        match node.node_data {
            NodeData::Group => Err(SessionErrorKind::NotAnArray {
                node: Box::new(node),
                message: "getting chunk reference".to_string(),
            }
            .into()),
            NodeData::Array { shape, manifests, .. } => {
                if !shape.valid_chunk_coord(coords) {
                    // this chunk ref cannot exist
                    return Ok(None);
                }

                // check the chunks modified in this session first
                // TODO: I hate rust forces me to clone to search in a hashmap. How to do better?
                let session_chunk =
                    self.change_set.get_chunk_ref(&node.id, coords).cloned();

                // If session_chunk is not None we have to return it, because is the update the
                // user made in the current session
                // If session_chunk == None, user hasn't modified the chunk in this session and we
                // need to fallback to fetching the manifests
                match session_chunk {
                    Some(res) => Ok(res),
                    None => {
                        self.get_old_chunk(node.id, manifests.as_slice(), coords).await
                    }
                }
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
    #[instrument(skip(self))]
    #[allow(clippy::type_complexity)]
    pub async fn get_chunk_reader(
        &self,
        path: &Path,
        coords: &ChunkIndices,
        byte_range: &ByteRange,
    ) -> SessionResult<Option<Pin<Box<dyn Future<Output = SessionResult<Bytes>> + Send>>>>
    {
        match self.get_chunk_ref(path, coords).await? {
            Some(ChunkPayload::Ref(ChunkRef { id, offset, length })) => {
                let byte_range = byte_range.clone();
                let asset_manager = Arc::clone(&self.asset_manager);
                let byte_range = construct_valid_byte_range(&byte_range, offset, length);
                Ok(Some(
                    async move {
                        // TODO: we don't have a way to distinguish if we want to pass a range or not
                        asset_manager
                            .fetch_chunk(&id, &byte_range)
                            .await
                            .map_err(|e| e.into())
                    }
                    .boxed(),
                ))
            }
            Some(ChunkPayload::Inline(bytes)) => {
                Ok(Some(ready(Ok(byte_range.slice(bytes))).boxed()))
            }
            Some(ChunkPayload::Virtual(VirtualChunkRef {
                location,
                offset,
                length,
                checksum,
            })) => {
                let byte_range = construct_valid_byte_range(byte_range, offset, length);
                let resolver = Arc::clone(&self.virtual_resolver);
                Ok(Some(
                    async move {
                        resolver
                            .fetch_chunk(
                                location.0.as_str(),
                                &byte_range,
                                checksum.as_ref(),
                            )
                            .await
                            .map_err(|e| e.into())
                    }
                    .boxed(),
                ))
            }
            None => Ok(None),
        }
    }

    /// Returns a function that can be used to asynchronously write chunk bytes to object store
    ///
    /// The reason to use this design, instead of simple pass the [`Bytes`] is to avoid holding a
    /// reference to the repository while the payload is uploaded to object store. This way, the
    /// reference is hold very briefly, and then an owned object is obtained which can do the actual
    /// upload without holding any [`Repository`] references.
    ///
    /// Example usage:
    /// ```ignore
    /// repository.get_chunk_writer()(Bytes::copy_from_slice(b"hello")).await?
    /// ```
    ///
    /// As shown, the result of the returned function must be awaited to finish the upload.
    #[instrument(skip(self))]
    pub fn get_chunk_writer(
        &self,
    ) -> impl FnOnce(
        Bytes,
    )
        -> Pin<Box<dyn Future<Output = SessionResult<ChunkPayload>> + Send>>
    + use<> {
        let threshold = self.config().inline_chunk_threshold_bytes() as usize;
        let asset_manager = Arc::clone(&self.asset_manager);
        move |data: Bytes| {
            async move {
                let payload = if data.len() > threshold {
                    new_materialized_chunk(asset_manager.as_ref(), data).await?
                } else {
                    new_inline_chunk(data)
                };
                Ok(payload)
            }
            .boxed()
        }
    }

    #[instrument(skip(self))]
    pub async fn clear(&mut self) -> SessionResult<()> {
        // TODO: can this be a delete_group("/") instead?
        let to_delete: Vec<(NodeType, Path)> = self
            .list_nodes()
            .await?
            .map_ok(|node| (node.node_type(), node.path))
            .try_collect()?;

        for (t, p) in to_delete {
            match t {
                NodeType::Group => self.delete_group(p).await?,
                NodeType::Array => self.delete_array(p).await?,
            }
        }
        Ok(())
    }

    async fn get_old_chunk(
        &self,
        node: NodeId,
        manifests: &[ManifestRef],
        coords: &ChunkIndices,
    ) -> SessionResult<Option<ChunkPayload>> {
        if manifests.is_empty() {
            // no chunks have been written, and the requested coords was not
            // in the changeset, return None to Zarr.
            return Ok(None);
        }

        let index = match find_coord(manifests.iter().map(|m| &m.extents), coords) {
            Some((index, _)) => index,
            // for an invalid coordinate, we bail.
            // This happens for two cases:
            // (1) the "coords" is out-of-range for the array shape
            // (2) the "coords" belongs to a shard that hasn't been written yet.
            None => return Ok(None),
        };

        let manifest = self.fetch_manifest(&manifests[index].object_id).await?;
        match manifest.get_chunk_payload(&node, coords) {
            Ok(payload) => {
                return Ok(Some(payload.clone()));
            }
            Err(IcechunkFormatError {
                kind: IcechunkFormatErrorKind::ChunkCoordinatesNotFound { .. },
                ..
            }) => {}
            Err(err) => return Err(err.into()),
        }
        Ok(None)
    }

    async fn fetch_manifest(&self, id: &ManifestId) -> SessionResult<Arc<Manifest>> {
        fetch_manifest(id, self.snapshot_id(), self.asset_manager.as_ref()).await
    }

    #[instrument(skip(self))]
    pub async fn list_nodes(
        &self,
    ) -> SessionResult<impl Iterator<Item = SessionResult<NodeSnapshot>> + '_> {
        updated_nodes(&self.asset_manager, &self.change_set, &self.snapshot_id).await
    }

    #[instrument(skip(self))]
    pub async fn all_chunks(
        &self,
    ) -> SessionResult<impl Stream<Item = SessionResult<(Path, ChunkInfo)>> + '_> {
        all_chunks(&self.asset_manager, &self.change_set, self.snapshot_id()).await
    }

    #[instrument(skip(self))]
    pub async fn chunk_coordinates<'a, 'b: 'a>(
        &'a self,
        array_path: &'b Path,
    ) -> SessionResult<impl Stream<Item = SessionResult<ChunkIndices>> + 'a + use<'a>>
    {
        let node = self.get_array(array_path).await?;
        let updated_chunks = updated_node_chunks_iterator(
            self.asset_manager.as_ref(),
            &self.change_set,
            &self.snapshot_id,
            node.clone(),
            ManifestExtents::ALL,
        )
        .await
        .map_ok(|(_path, chunk_info)| chunk_info.coord);

        let res = try_stream! {
            let new_chunks = stream::iter(
                self.change_set
                    .new_array_chunk_iterator(&node.id, array_path, ManifestExtents::ALL)
                    .map(|chunk_info| Ok::<ChunkIndices, SessionError>(chunk_info.coord)),
            );

            for await maybe_coords in updated_chunks.chain(new_chunks) {
                match maybe_coords {
                    Ok(coords) => {yield coords;}
                    Err(err) => Err(err)?
                }
            }
        };
        Ok(res)
    }

    #[instrument(skip(self))]
    pub async fn all_virtual_chunk_locations(
        &self,
    ) -> SessionResult<impl Stream<Item = SessionResult<String>> + '_> {
        let stream =
            self.all_chunks().await?.try_filter_map(|(_, info)| match info.payload {
                ChunkPayload::Virtual(reference) => ready(Ok(Some(reference.location.0))),
                _ => ready(Ok(None)),
            });
        Ok(stream)
    }

    /// Discard all uncommitted changes and return them as a `ChangeSet`
    #[instrument(skip(self))]
    pub fn discard_changes(&mut self) -> ChangeSet {
        std::mem::take(&mut self.change_set)
    }

    /// Merge a set of `ChangeSet`s into the repository without committing them
    #[instrument(skip(self, other))]
    pub async fn merge(&mut self, other: Session) -> SessionResult<()> {
        if self.read_only() {
            return Err(SessionErrorKind::ReadOnlySession.into());
        }
        let Session { splits: other_splits, change_set, .. } = other;

        if self.splits.iter().any(|(node, our_splits)| {
            other_splits
                .get(node)
                .is_some_and(|their_splits| !our_splits.compatible_with(their_splits))
        }) {
            let ours = self.config().manifest().splitting().clone();
            let theirs = self.config().manifest().splitting().clone();
            return Err(
                SessionErrorKind::IncompatibleSplittingConfig { ours, theirs }.into()
            );
        }

        // Session.splits is _complete_ in that it will include every possible split.
        // So a simple `extend` is fine, if the same node appears in two sessions,
        // it must have the same splits and overwriting is fine.
        self.splits.extend(other_splits);
        self.change_set.merge(change_set);
        Ok(())
    }

    #[instrument(skip(self, properties))]
    pub async fn commit(
        &mut self,
        message: &str,
        properties: Option<SnapshotProperties>,
    ) -> SessionResult<SnapshotId> {
        self._commit(message, properties, false).await
    }

    #[instrument(skip(self, properties))]
    pub async fn rewrite_manifests(
        &mut self,
        message: &str,
        properties: Option<SnapshotProperties>,
    ) -> SessionResult<SnapshotId> {
        let nodes = self.list_nodes().await?.collect::<Vec<_>>();
        // We need to populate the `splits` before calling `commit`.
        // In the normal chunk setting workflow, that would've been done by `set_chunk_ref`
        for node in nodes.into_iter().flatten() {
            if let NodeSnapshot {
                id,
                path,
                node_data: NodeData::Array { shape, dimension_names, .. },
                ..
            } = node
            {
                self.get_splits(&id, &path, &shape, &dimension_names);
            }
        }

        let splitting_config_serialized =
            serde_json::to_value(self.config.manifest().splitting())?;
        let mut properties = properties.unwrap_or_default();
        properties.insert("splitting_config".to_string(), splitting_config_serialized);
        self._commit(message, Some(properties), true).await
    }

    #[instrument(skip(self, properties))]
    async fn _commit(
        &mut self,
        message: &str,
        properties: Option<SnapshotProperties>,
        rewrite_manifests: bool,
    ) -> SessionResult<SnapshotId> {
        let Some(branch_name) = &self.branch_name else {
            return Err(SessionErrorKind::ReadOnlySession.into());
        };

        let default_metadata = self.default_commit_metadata.clone();
        let properties = properties
            .map(|p| {
                let mut merged = default_metadata.clone();
                merged.extend(p.into_iter());
                merged
            })
            .unwrap_or(default_metadata);

        let current = fetch_branch_tip(
            self.storage.as_ref(),
            self.storage_settings.as_ref(),
            branch_name,
        )
        .await;

        let id = match current {
            Err(RefError { kind: RefErrorKind::RefNotFound(_), .. }) => {
                do_commit(
                    self.storage.as_ref(),
                    Arc::clone(&self.asset_manager),
                    self.storage_settings.as_ref(),
                    branch_name,
                    &self.snapshot_id,
                    &self.change_set,
                    message,
                    Some(properties),
                    &self.splits,
                    rewrite_manifests,
                )
                .await
            }
            Err(err) => Err(err.into()),
            Ok(ref_data) => {
                // we can detect there will be a conflict before generating the new snapshot
                if ref_data.snapshot != self.snapshot_id {
                    Err(SessionErrorKind::Conflict {
                        expected_parent: Some(self.snapshot_id.clone()),
                        actual_parent: Some(ref_data.snapshot.clone()),
                    }
                    .into())
                } else {
                    do_commit(
                        self.storage.as_ref(),
                        Arc::clone(&self.asset_manager),
                        self.storage_settings.as_ref(),
                        branch_name,
                        &self.snapshot_id,
                        &self.change_set,
                        message,
                        Some(properties),
                        &self.splits,
                        rewrite_manifests,
                    )
                    .await
                }
            }
        }?;

        // if the commit was successful, we update the session to be
        // a read only session pointed at the new snapshot
        self.change_set = ChangeSet::default();
        self.snapshot_id = id.clone();
        // Once committed, the session is now read only, which we control
        // by setting the branch_name to None (you can only write to a branch session)
        self.branch_name = None;

        Ok(id)
    }

    pub async fn commit_rebasing<F1, F2, Fut1, Fut2>(
        &mut self,
        solver: &dyn ConflictSolver,
        rebase_attempts: u16,
        message: &str,
        properties: Option<SnapshotProperties>,
        // We would prefer to make this argument optional, but passing None
        // for this argument is so hard. Callers should just pass noop closure like
        // |_| async {},
        before_rebase: F1,
        after_rebase: F2,
    ) -> SessionResult<SnapshotId>
    where
        F1: Fn(u16) -> Fut1,
        F2: Fn(u16) -> Fut2,
        Fut1: Future<Output = ()>,
        Fut2: Future<Output = ()>,
    {
        for attempt in 0..rebase_attempts {
            match self.commit(message, properties.clone()).await {
                Ok(snap) => return Ok(snap),
                Err(SessionError { kind: SessionErrorKind::Conflict { .. }, .. }) => {
                    before_rebase(attempt + 1).await;
                    self.rebase(solver).await?;
                    after_rebase(attempt + 1).await;
                }
                Err(other_err) => return Err(other_err),
            }
        }
        self.commit(message, properties).await
    }

    /// Detect and optionally fix conflicts between the current [`ChangeSet`] (or session) and
    /// the tip of the branch.
    ///
    /// When [`Repository::commit`] method is called, the system validates that the tip of the
    /// passed branch is exactly the same as the `snapshot_id` for the current session. If that
    /// is not the case, the commit operation fails with [`SessionError::Conflict`].
    ///
    /// In that situation, the user has two options:
    /// 1. Abort the session and start a new one with using the new branch tip as a parent.
    /// 2. Use [`Repository::rebase`] to try to "fast-forward" the session through the new
    ///    commits.
    ///
    /// The issue with option 1 is that all the writes that have been done in the session,
    /// including the chunks, will be lost and they need to be written again. But, restarting
    /// the session is always the safest option. It's the only way to guarantee that
    /// any reads done during the session were actually reading the latest data.
    ///
    /// User that understands the tradeoffs, can use option 2. This is useful, for example
    /// when different "jobs" modify different arrays, or different parts of an array.
    /// In situations like that, "merging" the two changes is pretty trivial. But what
    /// happens when there are conflicts. For example, what happens when the current session
    /// and a new commit both wrote to the same chunk, or both updated user attributes for
    /// the same group.
    ///
    /// This is what [`Repository::rebase`] helps with. It can detect conflicts to let
    /// the user fix them manually, or it can attempt to fix conflicts based on a policy.
    ///
    /// Example:
    /// ```ignore
    /// let repo = ...
    /// let payload = repo.get_chunk_writer()(Bytes::copy_from_slice(b"foo")).await?;
    /// repo.set_chunk_ref(array_path, ChunkIndices(vec![0]), Some(payload)).await?;
    ///
    /// // the commit fails with a conflict because some other writer committed once or more before us
    /// let error = repo.commit("main", "wrote a chunk").await.unwrap_err();
    ///
    /// // let's inspect what are the conflicts
    /// if let Err(RebaseFailed {conflicts, ..}) = repo2.rebase(&ConflictDetector, "main").await.unwrap_err() {
    ///    // inspect the list of conflicts and fix them manually
    ///    // ...
    ///
    ///    // once fixed we can commit again
    ///
    ///    repo.commit("main", "wrote a chunk").await?;
    /// }
    /// ```
    ///
    /// Instead of fixing the conflicts manually, the user can try rebasing with an automated
    /// policy, configured to their needs:
    ///
    /// ```ignore
    /// let solver = BasicConflictSolver {
    ///    on_chunk_conflict: VersionSelection::UseOurs,
    ///    ..Default::default()
    /// };
    /// repo2.rebase(&solver, "main").await?
    /// ```
    ///
    /// When there are more than one commit between the parent snapshot and the tip of
    /// the branch, `rebase` iterates over all of them, older first, trying to fast-forward.
    /// If at some point it finds a conflict it cannot recover from, `rebase` leaves the
    /// `Repository` in a consistent state, that would successfully commit on top
    /// of the latest successfully fast-forwarded commit.
    #[instrument(skip(self, solver))]
    pub async fn rebase(&mut self, solver: &dyn ConflictSolver) -> SessionResult<()> {
        let Some(branch_name) = &self.branch_name else {
            return Err(SessionErrorKind::ReadOnlySession.into());
        };

        debug!("Rebase started");
        let ref_data = fetch_branch_tip(
            self.storage.as_ref(),
            self.storage_settings.as_ref(),
            branch_name,
        )
        .await?;

        if ref_data.snapshot == self.snapshot_id {
            // nothing to do, commit should work without rebasing
            warn!(
                branch = &self.branch_name,
                "No rebase is needed, parent snapshot is at the top of the branch. Aborting rebase."
            );
            Ok(())
        } else {
            let current_snapshot =
                self.asset_manager.fetch_snapshot(&ref_data.snapshot).await?;
            let ancestry = Arc::clone(&self.asset_manager)
                .snapshot_ancestry(&current_snapshot.id())
                .await?
                .map_ok(|meta| meta.id);
            let new_commits =
                stream::once(ready(Ok(ref_data.snapshot.clone())))
                    .chain(ancestry.try_take_while(|snap_id| {
                        ready(Ok(snap_id != &self.snapshot_id))
                    }))
                    .try_collect::<Vec<_>>()
                    .await?;
            trace!("Found {} commits to rebase", new_commits.len());

            // TODO: this clone is expensive
            // we currently need it to be able to process commits one by one without modifying the
            // changeset in case of failure
            // let mut changeset = self.change_set.clone();

            // we need to reverse the iterator to process them in order of oldest first
            for snap_id in new_commits.into_iter().rev() {
                debug!("Rebasing snapshot {}", &snap_id);
                let tx_log = self.asset_manager.fetch_transaction_log(&snap_id).await?;

                let session = Self::create_readonly_session(
                    self.config.clone(),
                    self.storage_settings.as_ref().clone(),
                    Arc::clone(&self.storage),
                    Arc::clone(&self.asset_manager),
                    Arc::clone(&self.virtual_resolver),
                    ref_data.snapshot.clone(),
                );

                let change_set = std::mem::take(&mut self.change_set);
                // TODO: this should probably execute in a worker thread
                match solver.solve(&tx_log, &session, change_set, self).await? {
                    ConflictResolution::Patched(patched_changeset) => {
                        trace!("Snapshot rebased");
                        self.change_set = patched_changeset;
                        self.snapshot_id = snap_id;
                    }
                    ConflictResolution::Unsolvable { reason, unmodified } => {
                        warn!("Snapshot cannot be rebased. Aborting rebase.");
                        self.change_set = unmodified;
                        return Err(SessionErrorKind::RebaseFailed {
                            snapshot: snap_id,
                            conflicts: reason,
                        }
                        .into());
                    }
                }
            }
            debug!("Rebase done");
            Ok(())
        }
    }
}

/// Warning: The presence of a single error may mean multiple missing items
async fn updated_chunk_iterator<'a>(
    asset_manager: &'a AssetManager,
    change_set: &'a ChangeSet,
    snapshot_id: &'a SnapshotId,
) -> SessionResult<impl Stream<Item = SessionResult<(Path, ChunkInfo)>> + 'a> {
    let snapshot = asset_manager.fetch_snapshot(snapshot_id).await?;
    let nodes = futures::stream::iter(snapshot.iter_arc());
    let res = nodes.and_then(move |node| async move {
        // Note: Confusingly, these NodeSnapshot instances have the metadata stored in the snapshot.
        // We have not applied any changeset updates. At the moment, the downstream code only
        // use node.id so there is no need to update yet.

        Ok(updated_node_chunks_iterator(
            asset_manager,
            change_set,
            snapshot_id,
            node,
            ManifestExtents::ALL,
        )
        .await)
    });
    Ok(res.try_flatten())
}

async fn updated_node_chunks_iterator<'a>(
    asset_manager: &'a AssetManager,
    change_set: &'a ChangeSet,
    snapshot_id: &'a SnapshotId,
    node: NodeSnapshot,
    extent: ManifestExtents,
) -> impl Stream<Item = SessionResult<(Path, ChunkInfo)>> + 'a {
    // This iterator should yield chunks for existing arrays + any updates.
    // we check for deletion here in the case that `path` exists in the snapshot,
    // and was deleted and then recreated in this changeset.
    if change_set.is_deleted(&node.path, &node.id) {
        Either::Left(stream::empty())
    } else {
        let path = node.path.clone();
        Either::Right(
            // TODO: avoid clone
            verified_node_chunk_iterator(
                asset_manager,
                snapshot_id,
                change_set,
                node,
                extent,
            )
            .await
            .map_ok(move |ci| (path.clone(), ci)),
        )
    }
}

/// Warning: The presence of a single error may mean multiple missing items
async fn node_chunk_iterator<'a>(
    asset_manager: &'a AssetManager,
    change_set: &'a ChangeSet,
    snapshot_id: &'a SnapshotId,
    path: &Path,
    extent: ManifestExtents,
) -> impl Stream<Item = SessionResult<ChunkInfo>> + 'a + use<'a> {
    match get_node(asset_manager, change_set, snapshot_id, path).await {
        Ok(node) => futures::future::Either::Left(
            verified_node_chunk_iterator(
                asset_manager,
                snapshot_id,
                change_set,
                node,
                extent,
            )
            .await,
        ),
        Err(_) => futures::future::Either::Right(futures::stream::empty()),
    }
}

/// Warning: The presence of a single error may mean multiple missing items
async fn verified_node_chunk_iterator<'a>(
    asset_manager: &'a AssetManager,
    snapshot_id: &'a SnapshotId,
    change_set: &'a ChangeSet,
    node: NodeSnapshot,
    extent: ManifestExtents,
) -> impl Stream<Item = SessionResult<ChunkInfo>> + 'a {
    match node.node_data {
        NodeData::Group => futures::future::Either::Left(futures::stream::empty()),
        NodeData::Array { manifests, .. } => {
            let new_chunk_indices: Box<HashSet<&ChunkIndices>> = Box::new(
                change_set
                    .array_chunks_iterator(&node.id, &node.path, extent.clone())
                    .map(|(idx, _)| idx)
                    // by chaining here, we make sure we don't pull from the manifest
                    // any chunks that were deleted prior to resizing in this session
                    .chain(change_set.deleted_chunks_iterator(&node.id))
                    .collect(),
            );

            let node_id_c = node.id.clone();
            let extent_c = extent.clone();
            let new_chunks = change_set
                .array_chunks_iterator(&node.id, &node.path, extent.clone())
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
                        .filter(move |manifest_ref| {
                            futures::future::ready(
                                extent.overlap_with(&manifest_ref.extents)
                                    != Overlap::None,
                            )
                        })
                        .then(move |manifest_ref| {
                            let new_chunk_indices = new_chunk_indices.clone();
                            let node_id_c = node.id.clone();
                            let node_id_c2 = node.id.clone();
                            let node_id_c3 = node.id.clone();
                            let extent_c2 = extent_c.clone();
                            async move {
                                let manifest = fetch_manifest(
                                    &manifest_ref.object_id,
                                    snapshot_id,
                                    asset_manager,
                                )
                                .await;
                                match manifest {
                                    Ok(manifest) => {
                                        let old_chunks = manifest
                                            .iter(node_id_c.clone())
                                            .filter_ok(move |(coord, _)| {
                                                !new_chunk_indices.contains(coord)
                                                    // If the manifest we are parsing partially overlaps with `extent`,
                                                    // we need to filter all coordinates
                                                    && extent_c2.contains(coord.0.as_slice())
                                            })
                                            .map_ok(move |(coord, payload)| ChunkInfo {
                                                node: node_id_c2.clone(),
                                                coord,
                                                payload,
                                            });

                                        let old_chunks = change_set
                                            .update_existing_chunks(
                                                node_id_c3, old_chunks,
                                            );
                                        futures::future::Either::Left(
                                            futures::stream::iter(old_chunks)
                                                .map_err(|e| e.into()),
                                        )
                                    }
                                    // if we cannot even fetch the manifest, we generate a
                                    // single error value.
                                    Err(err) => futures::future::Either::Right(
                                        futures::stream::once(ready(Err(err))),
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

impl From<Session> for ChangeSet {
    fn from(val: Session) -> Self {
        val.change_set
    }
}

pub fn is_prefix_match(key: &str, prefix: &str) -> bool {
    let tomatch =
        if prefix != String::from('/') { key.strip_prefix(prefix) } else { Some(key) };
    match tomatch {
        None => false,
        Some(rest) => {
            // we have a few cases
            prefix.is_empty()   // if prefix was empty anything matches
                || rest.is_empty()  // if stripping prefix left empty we have a match
                || rest.starts_with('/') // next component so we match
            // what we don't include is other matches,
            // we want to catch prefix/foo but not prefix-foo
        }
    }
}

async fn new_materialized_chunk(
    asset_manager: &AssetManager,
    data: Bytes,
) -> SessionResult<ChunkPayload> {
    let new_id = ObjectId::random();
    asset_manager.write_chunk(new_id.clone(), data.clone()).await?;
    Ok(ChunkPayload::Ref(ChunkRef { id: new_id, offset: 0, length: data.len() as u64 }))
}

fn new_inline_chunk(data: Bytes) -> ChunkPayload {
    ChunkPayload::Inline(data)
}

pub async fn get_chunk(
    reader: Option<Pin<Box<dyn Future<Output = SessionResult<Bytes>> + Send>>>,
) -> SessionResult<Option<Bytes>> {
    match reader {
        Some(reader) => Ok(Some(reader.await?)),
        None => Ok(None),
    }
}

/// Yields nodes in the base snapshot, applying any relevant updates in the changeset
async fn updated_existing_nodes<'a>(
    asset_manager: &AssetManager,
    change_set: &'a ChangeSet,
    parent_id: &SnapshotId,
) -> SessionResult<impl Iterator<Item = SessionResult<NodeSnapshot>> + 'a + use<'a>> {
    let updated_nodes = asset_manager
        .fetch_snapshot(parent_id)
        .await?
        .iter_arc()
        .filter_map_ok(move |node| change_set.update_existing_node(node))
        .map(|n| match n {
            Ok(n) => Ok(n),
            Err(err) => Err(SessionError::from(err)),
        });

    Ok(updated_nodes)
}

/// Yields nodes with the snapshot, applying any relevant updates in the changeset,
/// *and* new nodes in the changeset
async fn updated_nodes<'a>(
    asset_manager: &AssetManager,
    change_set: &'a ChangeSet,
    parent_id: &SnapshotId,
) -> SessionResult<impl Iterator<Item = SessionResult<NodeSnapshot>> + 'a + use<'a>> {
    Ok(updated_existing_nodes(asset_manager, change_set, parent_id)
        .await?
        .chain(change_set.new_nodes_iterator().map(Ok)))
}

async fn get_node(
    asset_manager: &AssetManager,
    change_set: &ChangeSet,
    snapshot_id: &SnapshotId,
    path: &Path,
) -> SessionResult<NodeSnapshot> {
    match change_set.get_new_node(path) {
        Some(node) => Ok(node),
        None => {
            let node =
                get_existing_node(asset_manager, change_set, snapshot_id, path).await?;
            if change_set.is_deleted(&node.path, &node.id) {
                Err(SessionErrorKind::NodeNotFound {
                    path: path.clone(),
                    message: "getting node".to_string(),
                }
                .into())
            } else {
                Ok(node)
            }
        }
    }
}

async fn get_existing_node(
    asset_manager: &AssetManager,
    change_set: &ChangeSet,
    snapshot_id: &SnapshotId,
    path: &Path,
) -> SessionResult<NodeSnapshot> {
    // An existing node is one that is present in a Snapshot file on storage
    let snapshot = asset_manager.fetch_snapshot(snapshot_id).await?;

    let node = snapshot.get_node(path).map_err(|err| match err {
        // A missing node here is not really a format error, so we need to
        // generate the correct error for repositories
        IcechunkFormatError {
            kind: IcechunkFormatErrorKind::NodeNotFound { path },
            ..
        } => SessionErrorKind::NodeNotFound {
            path,
            message: "existing node not found".to_string(),
        }
        .into(),
        err => SessionError::from(err),
    })?;

    match node.node_data {
        NodeData::Array { ref manifests, .. } => {
            if let Some(new_data) = change_set.get_updated_array(&node.id) {
                let node_data = NodeData::Array {
                    shape: new_data.shape.clone(),
                    dimension_names: new_data.dimension_names.clone(),
                    manifests: manifests.clone(),
                };
                Ok(NodeSnapshot {
                    user_data: new_data.user_data.clone(),
                    node_data,
                    ..node
                })
            } else {
                Ok(node)
            }
        }
        NodeData::Group => {
            if let Some(updated_definition) = change_set.get_updated_group(&node.id) {
                Ok(NodeSnapshot { user_data: updated_definition.clone(), ..node })
            } else {
                Ok(node)
            }
        }
    }
}

async fn all_chunks<'a>(
    asset_manager: &'a AssetManager,
    change_set: &'a ChangeSet,
    snapshot_id: &'a SnapshotId,
) -> SessionResult<impl Stream<Item = SessionResult<(Path, ChunkInfo)>> + 'a> {
    let existing_array_chunks =
        updated_chunk_iterator(asset_manager, change_set, snapshot_id).await?;
    let new_array_chunks =
        futures::stream::iter(change_set.new_arrays_chunk_iterator().map(Ok));
    Ok(existing_array_chunks.chain(new_array_chunks))
}

pub async fn raise_if_invalid_snapshot_id(
    storage: &(dyn Storage + Send + Sync),
    storage_settings: &storage::Settings,
    snapshot_id: &SnapshotId,
) -> SessionResult<()> {
    storage
        .fetch_snapshot(storage_settings, snapshot_id)
        .await
        .map_err(|_| SessionErrorKind::SnapshotNotFound { id: snapshot_id.clone() })?;
    Ok(())
}

// Converts the requested ByteRange to a valid ByteRange appropriate
// to the chunk reference of known `offset` and `length`.
pub fn construct_valid_byte_range(
    request: &ByteRange,
    chunk_offset: u64,
    chunk_length: u64,
) -> Range<ChunkOffset> {
    // TODO: error for offset<0
    // TODO: error if request.start > offset + length
    match request {
        ByteRange::Bounded(std::ops::Range { start: req_start, end: req_end }) => {
            let new_start =
                min(chunk_offset + req_start, chunk_offset + chunk_length - 1);
            let new_end = min(chunk_offset + req_end, chunk_offset + chunk_length);
            new_start..new_end
        }
        ByteRange::From(n) => {
            let new_start = min(chunk_offset + n, chunk_offset + chunk_length - 1);
            new_start..chunk_offset + chunk_length
        }
        ByteRange::Until(n) => {
            let new_end = chunk_offset + chunk_length;
            let new_start = new_end - n;
            new_start..new_end
        }
        ByteRange::Last(n) => {
            let new_end = chunk_offset + chunk_length;
            let new_start = new_end - n;
            new_start..new_end
        }
    }
}

struct FlushProcess<'a> {
    asset_manager: Arc<AssetManager>,
    change_set: &'a ChangeSet,
    parent_id: &'a SnapshotId,
    splits: &'a HashMap<NodeId, ManifestSplits>,
    manifest_refs: HashMap<NodeId, Vec<ManifestRef>>,
    manifest_files: HashSet<ManifestFileInfo>,
}

impl<'a> FlushProcess<'a> {
    fn new(
        asset_manager: Arc<AssetManager>,
        change_set: &'a ChangeSet,
        parent_id: &'a SnapshotId,
        splits: &'a HashMap<NodeId, ManifestSplits>,
    ) -> Self {
        Self {
            asset_manager,
            change_set,
            parent_id,
            splits,
            manifest_refs: Default::default(),
            manifest_files: Default::default(),
        }
    }

    async fn write_manifest_for_updated_chunks(
        &mut self,
        node: &NodeSnapshot,
        extent: &ManifestExtents,
    ) -> SessionResult<Option<ManifestRef>> {
        let asset_manager = Arc::clone(&self.asset_manager);
        let updated_chunks = updated_node_chunks_iterator(
            asset_manager.as_ref(),
            self.change_set,
            self.parent_id,
            node.clone(),
            extent.clone(),
        )
        .await
        .map_ok(|(_path, chunk_info)| chunk_info);
        self.write_manifest_from_iterator(updated_chunks).await
    }

    async fn write_manifest_from_iterator(
        &mut self,
        chunks: impl Stream<Item = SessionResult<ChunkInfo>>,
    ) -> SessionResult<Option<ManifestRef>> {
        let mut from = vec![];
        let mut to = vec![];
        let chunks = aggregate_extents(&mut from, &mut to, chunks, |ci| &ci.coord);

        if let Some(new_manifest) = Manifest::from_stream(chunks)
            .await
            .map_err(|e| SessionErrorKind::ManifestCreationError(Box::new(e)))?
        {
            let new_manifest = Arc::new(new_manifest);
            let new_manifest_size =
                self.asset_manager.write_manifest(Arc::clone(&new_manifest)).await?;

            let file_info =
                ManifestFileInfo::new(new_manifest.as_ref(), new_manifest_size);
            self.manifest_files.insert(file_info);

            let new_ref = ManifestRef {
                object_id: new_manifest.id().clone(),
                extents: ManifestExtents::new(&from, &to),
            };
            Ok(Some(new_ref))
        } else {
            Ok(None)
        }
    }

    /// Write a manifest for a node that was created in this session
    /// It doesn't need to look at previous manifests because the node is new
    async fn write_manifest_for_new_node(
        &mut self,
        node_id: &NodeId,
        node_path: &Path,
    ) -> SessionResult<()> {
        #[allow(clippy::expect_used)]
        let splits =
            self.splits.get(node_id).expect("getting split for node unexpectedly failed");

        for extent in splits.iter() {
            if self.change_set.array_manifest(node_id, extent).is_some() {
                let chunks = stream::iter(
                    self.change_set
                        .new_array_chunk_iterator(node_id, node_path, extent.clone())
                        .map(Ok),
                );
                #[allow(clippy::expect_used)]
                let new_ref = self.write_manifest_from_iterator(chunks).await.expect(
                    "logic bug. for a new node, we must always write the manifest",
                );
                // new_ref is None if there were no chunks in the iterator
                if let Some(new_ref) = new_ref {
                    self.manifest_refs.entry(node_id.clone()).or_default().push(new_ref);
                }
            }
        }
        Ok(())
    }

    /// Write a manifest for a node that was modified in this session
    /// It needs to update the chunks according to the change set
    /// and record the new manifest
    async fn write_manifest_for_existing_node(
        &mut self,
        node: &NodeSnapshot,
        existing_manifests: Vec<ManifestRef>,
        old_snapshot: &Snapshot,
        rewrite_manifests: bool,
    ) -> SessionResult<()> {
        #[allow(clippy::expect_used)]
        let splits =
            self.splits.get(&node.id).expect("splits should exist for this node.");
        let mut refs =
            HashMap::<ManifestExtents, Vec<ManifestRef>>::with_capacity(splits.len());

        let on_disk_extents =
            existing_manifests.iter().map(|m| m.extents.clone()).collect::<Vec<_>>();

        let modified_splits = self
            .change_set
            .modified_manifest_extents_iterator(&node.id, &node.path)
            .collect::<HashSet<_>>();

        // ``modified_splits`` (i.e. splits used in this session)
        // must be a subset of ``splits`` (the splits set in the config)
        debug_assert!(modified_splits.is_subset(&splits.iter().collect::<HashSet<_>>()));

        for extent in splits.iter() {
            if rewrite_manifests || modified_splits.contains(extent) {
                // this split was modified in this session, rewrite it completely
                self.write_manifest_for_updated_chunks(node, extent)
                    .await?
                    .map(|new_ref| refs.insert(extent.clone(), vec![new_ref]));
            } else {
                // intersection of the current split with extents on disk
                let on_disk_bbox = on_disk_extents
                    .iter()
                    .filter_map(|e| e.intersection(extent))
                    .reduce(|a, b| a.union(&b));

                // split was unmodified in this session. Let's look at the current manifests
                // and see what we need to do with them
                for old_ref in existing_manifests.iter() {
                    // Remember that the extents written to disk are the `from`:`to` ranges
                    // of populated chunks
                    match old_ref.extents.overlap_with(extent) {
                        Overlap::Complete => {
                            debug_assert!(on_disk_bbox.is_some());
                            // Just propagate this ref again, no rewriting necessary
                            refs.entry(extent.clone()).or_default().push(old_ref.clone());
                            // OK to unwrap here since this manifest file must exist in the old snapshot
                            #[allow(clippy::expect_used)]
                            self.manifest_files.insert(
                                old_snapshot.manifest_info(&old_ref.object_id).expect("logic bug. creating manifest file info for an existing manifest failed."),
                            );
                        }
                        Overlap::Partial => {
                            // the splits have changed, but no refs in this split have been written in this session
                            // same as `if` block above
                            debug_assert!(on_disk_bbox.is_some());
                            if let Some(new_ref) = self
                                .write_manifest_for_updated_chunks(node, extent)
                                .await?
                            {
                                refs.entry(extent.clone()).or_default().push(new_ref);
                            }
                        }
                        Overlap::None => {
                            // Nothing to do
                        }
                    };
                }
            }
        }

        // FIXME: Assert that bboxes in refs don't overlap

        self.manifest_refs
            .entry(node.id.clone())
            .or_default()
            .extend(refs.into_values().flatten());
        Ok(())
    }

    /// Record the previous manifests for an array that was not modified in the session
    fn copy_previous_manifest(&mut self, node: &NodeSnapshot, old_snapshot: &Snapshot) {
        match &node.node_data {
            NodeData::Array { manifests: array_refs, .. } => {
                self.manifest_files.extend(array_refs.iter().map(|mr| {
                    // It's ok to unwrap here, the snapshot had the node, it has to have the
                    // manifest file info
                    #[allow(clippy::expect_used)]
                    old_snapshot
                        .get_manifest_file(&mr.object_id)
                        .expect(
                            "Bug in flush function, no manifest file found in snapshot",
                        )
                        .clone()
                }));
                for mr in array_refs.iter() {
                    let new_ref = mr.clone();
                    self.manifest_refs
                        .entry(node.id.clone())
                        .and_modify(|v| v.push(new_ref.clone()))
                        .or_insert_with(|| vec![new_ref]);
                }
            }
            NodeData::Group => {}
        }
    }
}

impl ManifestSplitDimCondition {
    fn matches(&self, axis: usize, dimname: Option<String>) -> bool {
        match self {
            ManifestSplitDimCondition::Axis(ax) => ax == &axis,
            ManifestSplitDimCondition::DimensionName(regex) => dimname
                .map(|name| {
                    Regex::new(regex)
                        .map(|regex| regex.is_match(name.as_bytes()))
                        .unwrap_or(false)
                })
                .unwrap_or(false),
            ManifestSplitDimCondition::Any => true,
        }
    }
}

impl ManifestSplittingConfig {
    pub fn get_split_sizes(
        &self,
        path: &Path,
        shape: &ArrayShape,
        dimension_names: &Option<Vec<DimensionName>>,
    ) -> ManifestSplits {
        let ndim = shape.len();
        let num_chunks = shape.num_chunks().collect::<Vec<_>>();
        let mut edges: Vec<Vec<u32>> =
            (0..ndim).map(|axis| vec![0, num_chunks[axis]]).collect();

        // This is ugly but necessary to handle:
        //   - path: *
        //     manifest-split-size:
        //     - t : 10
        //   - path: *
        //     manifest-split-size:
        //     - y : 2
        // which is now identical to:
        //   - path: *
        //     manifest-split-size:
        //     - t : 10
        //     - y : 2
        let mut already_matched: HashSet<usize> = HashSet::new();

        #[allow(clippy::expect_used)]
        let split_sizes = self
            .split_sizes
            .clone()
            .or_else(|| Self::default().split_sizes)
            .expect("logic bug in grabbing split sizes from ManifestSplittingConfig");

        for (condition, dim_specs) in split_sizes.iter() {
            if condition.matches(path) {
                let dimension_names = dimension_names
                    .clone()
                    .unwrap_or(repeat_n(DimensionName::NotSpecified, ndim).collect());
                for (axis, dimname) in itertools::enumerate(dimension_names) {
                    if already_matched.contains(&axis) {
                        continue;
                    }
                    for ManifestSplitDim {
                        condition: dim_condition,
                        num_chunks: split_size,
                    } in dim_specs.iter()
                    {
                        if dim_condition.matches(axis, dimname.clone().into()) {
                            edges[axis] = uniform_manifest_split_edges(
                                num_chunks[axis],
                                split_size,
                            );
                            already_matched.insert(axis);
                            break;
                        };
                    }
                }
            }
        }
        ManifestSplits::from_edges(edges)
    }
}

async fn flush(
    mut flush_data: FlushProcess<'_>,
    message: &str,
    properties: SnapshotProperties,
    rewrite_manifests: bool,
) -> SessionResult<SnapshotId> {
    if !rewrite_manifests && flush_data.change_set.is_empty() {
        return Err(SessionErrorKind::NoChangesToCommit.into());
    }

    let old_snapshot =
        flush_data.asset_manager.fetch_snapshot(flush_data.parent_id).await?;

    // We first go through all existing nodes to see if we need to rewrite any manifests

    for node in old_snapshot.iter().filter_ok(|node| node.node_type() == NodeType::Array)
    {
        let node = node?;
        trace!(path=%node.path, "Flushing node");
        let node_id = &node.id;

        if flush_data.change_set.array_is_deleted(&(node.path.clone(), node_id.clone())) {
            trace!(path=%node.path, "Node deleted, not writing a manifest");
            continue;
        }

        if rewrite_manifests
        // metadata change might have shrunk the array
        || flush_data.change_set.is_updated_array(node_id)
            || flush_data.change_set.has_chunk_changes(node_id)
        {
            trace!(path=%node.path, "Node has changes, writing a new manifest");
            // Array wasn't deleted and has changes in this session
            // get the new node to handle changes in size, e.g. appends.
            let new_node = get_existing_node(
                flush_data.asset_manager.as_ref(),
                flush_data.change_set,
                flush_data.parent_id,
                &node.path,
            )
            .await?;
            if let NodeData::Array { manifests, .. } = new_node.node_data {
                flush_data
                    .write_manifest_for_existing_node(
                        &node,
                        manifests,
                        old_snapshot.as_ref(),
                        rewrite_manifests,
                    )
                    .await?;
            }
        } else {
            trace!(path=%node.path, "Node has no changes, keeping the previous manifest");
            // Array wasn't deleted but has no changes in this session
            flush_data.copy_previous_manifest(&node, old_snapshot.as_ref());
        }
    }

    // Now we need to go through all the new arrays, and generate manifests for them

    for (node_path, node_id) in flush_data.change_set.new_arrays() {
        trace!(path=%node_path, "New node, writing a manifest");
        flush_data.write_manifest_for_new_node(node_id, node_path).await?;
    }

    // manifest_files & manifest_refs _must_ be consistent
    debug_assert_eq!(
        flush_data.manifest_files.iter().map(|x| x.id.clone()).collect::<HashSet<_>>(),
        flush_data
            .manifest_refs
            .values()
            .flatten()
            .map(|x| x.object_id.clone())
            .collect::<HashSet<_>>(),
    );

    trace!("Building new snapshot");
    // gather and sort nodes:
    // this is a requirement for Snapshot::from_iter
    let mut all_nodes: Vec<_> = updated_nodes(
        flush_data.asset_manager.as_ref(),
        flush_data.change_set,
        flush_data.parent_id,
    )
    .await?
    .map_ok(|node| {
        let id = &node.id;
        // TODO: many clones
        if let NodeData::Array { shape, dimension_names, .. } = node.node_data {
            NodeSnapshot {
                node_data: NodeData::Array {
                    shape,
                    dimension_names,
                    manifests: flush_data
                        .manifest_refs
                        .get(id)
                        .cloned()
                        .unwrap_or_default(),
                },
                ..node
            }
        } else {
            node
        }
    })
    .try_collect()?;

    all_nodes.sort_by(|a, b| a.path.cmp(&b.path));

    let new_snapshot = Snapshot::from_iter(
        None,
        Some(old_snapshot.id().clone()),
        message.to_string(),
        Some(properties),
        flush_data.manifest_files.into_iter().collect(),
        None,
        all_nodes.into_iter().map(Ok::<_, Infallible>),
    )?;

    let new_ts = new_snapshot.flushed_at()?;
    let old_ts = old_snapshot.flushed_at()?;
    if new_ts <= old_ts {
        tracing::error!(
            new_timestamp = %new_ts,
            old_timestamp = %old_ts,
            "Snapshot timestamp older than parent, aborting commit"
        );
        return Err(SessionErrorKind::InvalidSnapshotTimestampOrdering {
            parent: old_ts,
            child: new_ts,
        }
        .into());
    }

    let new_snapshot = Arc::new(new_snapshot);
    let new_snapshot_c = Arc::clone(&new_snapshot);
    let asset_manager = Arc::clone(&flush_data.asset_manager);
    let snapshot_timestamp = tokio::spawn(
        async move {
            asset_manager.write_snapshot(Arc::clone(&new_snapshot_c)).await?;
            asset_manager.get_snapshot_last_modified(&new_snapshot_c.id()).await
        }
        .in_current_span(),
    );

    trace!(transaction_log_id = %new_snapshot.id(), "Creating transaction log");
    let new_snapshot_id = new_snapshot.id();
    // FIXME: this should execute in a non-blocking context
    let tx_log = TransactionLog::new(&new_snapshot_id, flush_data.change_set);

    flush_data
        .asset_manager
        .write_transaction_log(new_snapshot_id.clone(), Arc::new(tx_log))
        .await?;

    let snapshot_timestamp = snapshot_timestamp
        .await
        .map_err(SessionError::from)?
        .map_err(SessionError::from)?;

    // Fail if there is too much clock difference with the object store
    // This is to prevent issues with snapshot ordering and expiration
    if (snapshot_timestamp - new_ts).num_seconds().abs() > 600 {
        tracing::error!(
            snapshot_timestamp = %new_ts,
            object_store_timestamp = %snapshot_timestamp,
            "Snapshot timestamp drifted from object store clock, aborting commit"
        );
        return Err(SessionErrorKind::InvalidSnapshotTimestamp {
            object_store_time: snapshot_timestamp,
            snapshot_time: new_ts,
        }
        .into());
    }

    Ok(new_snapshot_id.clone())
}

#[allow(clippy::too_many_arguments)]
async fn do_commit(
    storage: &(dyn Storage + Send + Sync),
    asset_manager: Arc<AssetManager>,
    storage_settings: &storage::Settings,
    branch_name: &str,
    snapshot_id: &SnapshotId,
    change_set: &ChangeSet,
    message: &str,
    properties: Option<SnapshotProperties>,
    splits: &HashMap<NodeId, ManifestSplits>,
    rewrite_manifests: bool,
) -> SessionResult<SnapshotId> {
    info!(branch_name, old_snapshot_id=%snapshot_id, "Commit started");
    let parent_snapshot = snapshot_id.clone();
    let properties = properties.unwrap_or_default();
    let flush_data = FlushProcess::new(asset_manager, change_set, snapshot_id, splits);
    let new_snapshot = flush(flush_data, message, properties, rewrite_manifests).await?;

    debug!(branch_name, new_snapshot_id=%new_snapshot, "Updating branch");
    let id = match update_branch(
        storage,
        storage_settings,
        branch_name,
        new_snapshot.clone(),
        Some(&parent_snapshot),
    )
    .await
    {
        Ok(_) => Ok(new_snapshot.clone()),
        Err(RefError {
            kind: RefErrorKind::Conflict { expected_parent, actual_parent },
            ..
        }) => Err(SessionError::from(SessionErrorKind::Conflict {
            expected_parent,
            actual_parent,
        })),
        Err(err) => Err(err.into()),
    }?;

    info!(branch_name, old_snapshot_id=%snapshot_id, new_snapshot_id=%new_snapshot, "Commit done");
    Ok(id)
}
async fn fetch_manifest(
    manifest_id: &ManifestId,
    snapshot_id: &SnapshotId,
    asset_manager: &AssetManager,
) -> SessionResult<Arc<Manifest>> {
    let snapshot = asset_manager.fetch_snapshot(snapshot_id).await?;
    let manifest_info = snapshot.manifest_info(manifest_id).ok_or_else(|| {
        IcechunkFormatError::from(IcechunkFormatErrorKind::ManifestInfoNotFound {
            manifest_id: manifest_id.clone(),
        })
    })?;
    Ok(asset_manager.fetch_manifest(manifest_id, manifest_info.size_bytes).await?)
}

/// Map the iterator to accumulate the extents of the chunks traversed
///
/// As we are processing chunks to create a manifest, we need to keep track
/// of the extents of the manifests. This means, for each coordinate, we need
/// to record its minimum and maximum values.
///
/// This very ugly code does that, without having to traverse the iterator twice.
/// It adapts the stream using [`StreamExt::map_ok`] and keeps a running min/max
/// for each coordinate.
///
/// When the iterator is fully traversed, the min and max values will be
/// available in `from` and `to` arguments.
///
/// Yes, this is horrible.
fn aggregate_extents<'a, T: std::fmt::Debug, E>(
    from: &'a mut Vec<u32>,
    to: &'a mut Vec<u32>,
    it: impl Stream<Item = Result<T, E>> + 'a,
    extract_index: impl for<'b> Fn(&'b T) -> &'b ChunkIndices + 'a,
) -> impl Stream<Item = Result<T, E>> + 'a {
    // we initialize the destination with an empty array, because we don't know
    // the dimensions of the array yet. On the first element we will re-initialize
    *from = Vec::new();
    *to = Vec::new();
    it.map_ok(move |t| {
        // these are the coordinates for the chunk
        let idx = extract_index(&t);

        // we need to initialize the mins/maxes the first time
        // we initialize with the value of the first element
        // this obviously doesn't work for empty streams
        // but we never generate manifests for them
        if from.is_empty() {
            *from = idx.0.clone();
            // important to remember that `to` is not inclusive, so we need +1
            *to = idx.0.iter().map(|n| n + 1).collect();
        } else {
            // We need to iterate over coordinates, and update the
            // minimum and maximum for each if needed
            for (coord_idx, value) in idx.0.iter().enumerate() {
                if let Some(from_current) = from.get_mut(coord_idx) {
                    if value < from_current {
                        *from_current = *value
                    }
                }
                if let Some(to_current) = to.get_mut(coord_idx) {
                    let range_value = value + 1;
                    if range_value > *to_current {
                        *to_current = range_value
                    }
                }
            }
        }
        t
    })
}

#[cfg(test)]
#[allow(clippy::panic, clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::{
        collections::HashMap,
        error::Error,
        sync::atomic::{AtomicU16, Ordering},
    };

    use crate::{
        ObjectStorage, Repository,
        config::{ManifestConfig, ManifestSplitCondition},
        conflicts::{
            basic_solver::{BasicConflictSolver, VersionSelection},
            detector::ConflictDetector,
        },
        format::manifest::{ManifestExtents, ManifestSplits},
        refs::{Ref, fetch_tag},
        repository::VersionInfo,
        storage::new_in_memory_storage,
        strategies::{
            ShapeDim, chunk_indices, empty_writable_session, node_paths, shapes_and_dims,
        },
    };

    use super::*;
    use icechunk_macros::tokio_test;
    use itertools::Itertools;
    use pretty_assertions::assert_eq;
    use proptest::prelude::{prop_assert, prop_assert_eq};
    use storage::logging::LoggingStorage;
    use test_strategy::proptest;
    use tokio::sync::Barrier;

    async fn create_memory_store_repository() -> Repository {
        let storage =
            new_in_memory_storage().await.expect("failed to create in-memory store");
        Repository::create(None, storage, HashMap::new()).await.unwrap()
    }

    #[proptest(async = "tokio")]
    async fn test_add_delete_group(
        #[strategy(node_paths())] path: Path,
        #[strategy(empty_writable_session())] mut session: Session,
    ) {
        let user_data = Bytes::new();

        // getting any path from an empty repository must fail
        prop_assert!(session.get_node(&path).await.is_err());

        // adding a new group must succeed
        prop_assert!(session.add_group(path.clone(), user_data.clone()).await.is_ok());

        // Getting a group just added must succeed
        let node = session.get_node(&path).await;
        prop_assert!(node.is_ok());

        // Getting the group twice must be equal
        prop_assert_eq!(node.unwrap(), session.get_node(&path).await.unwrap());

        // adding an existing group fails
        let matches = matches!(
            session.add_group(path.clone(), user_data.clone()).await.unwrap_err(),
            SessionError{kind: SessionErrorKind::AlreadyExists{node, ..},..} if node.path == path
        );
        prop_assert!(matches);

        // deleting the added group must succeed
        prop_assert!(session.delete_group(path.clone()).await.is_ok());

        // deleting twice must succeed
        prop_assert!(session.delete_group(path.clone()).await.is_ok());

        // getting a deleted group must fail
        prop_assert!(session.get_node(&path).await.is_err());

        // adding again must succeed
        prop_assert!(session.add_group(path.clone(), user_data.clone()).await.is_ok());

        // deleting again must succeed
        prop_assert!(session.delete_group(path.clone()).await.is_ok());
    }

    #[proptest(async = "tokio")]
    async fn test_add_delete_array(
        #[strategy(node_paths())] path: Path,
        #[strategy(shapes_and_dims(None))] metadata: ShapeDim,
        #[strategy(empty_writable_session())] mut session: Session,
    ) {
        // new array must always succeed
        prop_assert!(
            session
                .add_array(
                    path.clone(),
                    metadata.shape.clone(),
                    metadata.dimension_names.clone(),
                    Bytes::new()
                )
                .await
                .is_ok()
        );

        // adding to the same path must fail
        prop_assert!(
            session
                .add_array(
                    path.clone(),
                    metadata.shape.clone(),
                    metadata.dimension_names.clone(),
                    Bytes::new()
                )
                .await
                .is_err()
        );

        // first delete must succeed
        prop_assert!(session.delete_array(path.clone()).await.is_ok());

        // deleting twice must succeed
        prop_assert!(session.delete_array(path.clone()).await.is_ok());

        // adding again must succeed
        prop_assert!(
            session
                .add_array(
                    path.clone(),
                    metadata.shape.clone(),
                    metadata.dimension_names.clone(),
                    Bytes::new()
                )
                .await
                .is_ok()
        );

        // deleting again must succeed
        prop_assert!(session.delete_array(path.clone()).await.is_ok());
    }

    #[proptest(async = "tokio")]
    async fn test_add_array_group_clash(
        #[strategy(node_paths())] path: Path,
        #[strategy(shapes_and_dims(None))] metadata: ShapeDim,
        #[strategy(empty_writable_session())] mut session: Session,
    ) {
        // adding a group at an existing array node must fail
        prop_assert!(
            session
                .add_array(
                    path.clone(),
                    metadata.shape.clone(),
                    metadata.dimension_names.clone(),
                    Bytes::new()
                )
                .await
                .is_ok()
        );
        let matches = matches!(
            session.add_group(path.clone(), Bytes::new()).await.unwrap_err(),
            SessionError{kind: SessionErrorKind::AlreadyExists{node, ..},..} if node.path == path
        );
        prop_assert!(matches);

        let matches = matches!(
            session.delete_group(path.clone()).await.unwrap_err(),
            SessionError{kind: SessionErrorKind::NotAGroup{node, ..},..} if node.path == path
        );
        prop_assert!(matches);
        prop_assert!(session.delete_array(path.clone()).await.is_ok());

        // adding an array at an existing group node must fail
        prop_assert!(session.add_group(path.clone(), Bytes::new()).await.is_ok());
        let matches = matches!(
            session.add_array(path.clone(),
                metadata.shape.clone(),
                metadata.dimension_names.clone(),
                Bytes::new()
        ).await.unwrap_err(),
            SessionError{kind: SessionErrorKind::AlreadyExists{node, ..},..} if node.path == path
        );
        prop_assert!(matches);
        let matches = matches!(
            session.delete_array(path.clone()).await.unwrap_err(),
            SessionError{kind: SessionErrorKind::NotAnArray{node, ..},..}if node.path == path
        );
        prop_assert!(matches);
        prop_assert!(session.delete_group(path.clone()).await.is_ok());
    }

    #[proptest(async = "tokio")]
    async fn test_aggregate_extents(
        #[strategy(proptest::collection::vec(chunk_indices(3, 0..1_000_000), 1..50))]
        indices: Vec<ChunkIndices>,
    ) {
        let mut from = vec![];
        let mut to = vec![];

        let expected_from = vec![
            indices.iter().map(|i| i.0[0]).min().unwrap(),
            indices.iter().map(|i| i.0[1]).min().unwrap(),
            indices.iter().map(|i| i.0[2]).min().unwrap(),
        ];
        let expected_to = vec![
            indices.iter().map(|i| i.0[0]).max().unwrap() + 1,
            indices.iter().map(|i| i.0[1]).max().unwrap() + 1,
            indices.iter().map(|i| i.0[2]).max().unwrap() + 1,
        ];

        let _ = aggregate_extents(
            &mut from,
            &mut to,
            stream::iter(indices.into_iter().map(Ok::<ChunkIndices, Infallible>)),
            |idx| idx,
        )
        .count()
        .await;

        prop_assert_eq!(from, expected_from);
        prop_assert_eq!(to, expected_to);
    }

    #[tokio::test]
    async fn test_which_split() -> Result<(), Box<dyn Error>> {
        let splits = ManifestSplits::from_edges(vec![vec![0, 10, 20]]);

        assert_eq!(splits.position(&ChunkIndices(vec![1])), Some(0));
        assert_eq!(splits.position(&ChunkIndices(vec![11])), Some(1));

        let edges = vec![vec![0, 10, 20], vec![0, 10, 20]];

        let splits = ManifestSplits::from_edges(edges);
        assert_eq!(splits.position(&ChunkIndices(vec![1, 1])), Some(0));
        assert_eq!(splits.position(&ChunkIndices(vec![1, 10])), Some(1));
        assert_eq!(splits.position(&ChunkIndices(vec![1, 11])), Some(1));
        assert!(splits.position(&ChunkIndices(vec![21, 21])).is_none());

        Ok(())
    }

    #[tokio_test]
    async fn test_repository_with_default_commit_metadata() -> Result<(), Box<dyn Error>>
    {
        let mut repo = create_memory_store_repository().await;
        let mut ds = repo.writable_session("main").await?;
        ds.add_group(Path::root(), Bytes::new()).await?;
        let snapshot = ds.commit("commit", None).await?;

        // Verify that the first commit has no metadata
        let ancestry = repo.snapshot_ancestry(&snapshot).await?;
        let snapshot_infos = ancestry.try_collect::<Vec<_>>().await?;
        assert!(snapshot_infos[0].metadata.is_empty());

        // Set some default metadata
        let mut default_metadata = SnapshotProperties::default();
        default_metadata.insert("author".to_string(), "John Doe".to_string().into());
        default_metadata.insert("project".to_string(), "My Project".to_string().into());
        repo.set_default_commit_metadata(default_metadata.clone());

        let mut ds = repo.writable_session("main").await?;
        ds.add_group("/group".try_into().unwrap(), Bytes::new()).await?;
        let snapshot = ds.commit("commit", None).await?;

        let snapshot_info = repo.snapshot_ancestry(&snapshot).await?;
        let snapshot_infos = snapshot_info.try_collect::<Vec<_>>().await?;
        assert_eq!(snapshot_infos[0].metadata, default_metadata);

        // Check that metadata is merged with users provided metadata taking precedence
        let mut metadata = SnapshotProperties::default();
        metadata.insert("author".to_string(), "Jane Doe".to_string().into());
        metadata.insert("id".to_string(), "ideded".to_string().into());
        let mut ds = repo.writable_session("main").await?;
        ds.add_group("/group2".try_into().unwrap(), Bytes::new()).await?;
        let snapshot = ds.commit("commit", Some(metadata.clone())).await?;

        let snapshot_info = repo.snapshot_ancestry(&snapshot).await?;
        let snapshot_infos = snapshot_info.try_collect::<Vec<_>>().await?;
        let mut expected_result = SnapshotProperties::default();
        expected_result.insert("author".to_string(), "Jane Doe".to_string().into());
        expected_result.insert("project".to_string(), "My Project".to_string().into());
        expected_result.insert("id".to_string(), "ideded".to_string().into());
        assert_eq!(snapshot_infos[0].metadata, expected_result);

        Ok(())
    }

    #[tokio_test]
    async fn test_repository_with_splits_and_resizes() -> Result<(), Box<dyn Error>> {
        let storage: Arc<dyn Storage + Send + Sync> = new_in_memory_storage().await?;

        let split_sizes = Some(vec![(
            ManifestSplitCondition::PathMatches { regex: r".*".to_string() },
            vec![ManifestSplitDim {
                condition: ManifestSplitDimCondition::Any,
                num_chunks: 2,
            }],
        )]);

        let man_config = ManifestConfig {
            splitting: Some(ManifestSplittingConfig { split_sizes }),
            ..ManifestConfig::default()
        };

        let repo = Repository::create(
            Some(RepositoryConfig {
                inline_chunk_threshold_bytes: Some(0),
                manifest: Some(man_config),
                ..Default::default()
            }),
            storage,
            HashMap::new(),
        )
        .await?;
        let mut session = repo.writable_session("main").await?;
        session.add_group(Path::root(), Bytes::copy_from_slice(b"")).await?;

        let array_path: Path = "/array".to_string().try_into().unwrap();
        let shape = ArrayShape::new(vec![(4, 1)]).unwrap();
        let dimension_names = Some(vec!["t".into()]);
        let array_def = Bytes::from_static(br#"{"this":"other array"}"#);

        session
            .add_array(
                array_path.clone(),
                shape.clone(),
                dimension_names.clone(),
                array_def.clone(),
            )
            .await?;

        let bytes = Bytes::copy_from_slice(&42i8.to_be_bytes());
        for idx in [0, 2] {
            let payload = session.get_chunk_writer()(bytes.clone()).await?;
            session
                .set_chunk_ref(array_path.clone(), ChunkIndices(vec![idx]), Some(payload))
                .await?;
        }
        session.commit("None", None).await?;

        let mut session = repo.writable_session("main").await?;
        // This is how Zarr resizes
        // first, delete any out of bounds chunks
        session.set_chunk_ref(array_path.clone(), ChunkIndices(vec![2]), None).await?;
        // second, update metadata
        let shape2 = ArrayShape::new(vec![(2, 1)]).unwrap();
        session
            .update_array(
                &array_path,
                shape2.clone(),
                dimension_names.clone(),
                array_def.clone(),
            )
            .await?;

        assert!(
            session.get_chunk_ref(&array_path, &ChunkIndices(vec![2])).await?.is_none()
        );

        // resize back to original shape
        session
            .update_array(
                &array_path,
                shape.clone(),
                dimension_names.clone(),
                array_def.clone(),
            )
            .await?;

        // should still be deleted
        assert!(
            session.get_chunk_ref(&array_path, &ChunkIndices(vec![2])).await?.is_none()
        );

        // set another chunk in this split
        let payload = session.get_chunk_writer()(bytes.clone()).await?;
        session
            .set_chunk_ref(array_path.clone(), ChunkIndices(vec![3]), Some(payload))
            .await?;
        // should still be deleted
        assert!(
            session.get_chunk_ref(&array_path, &ChunkIndices(vec![2])).await?.is_none()
        );
        // new ref should be present
        assert!(
            session.get_chunk_ref(&array_path, &ChunkIndices(vec![3])).await?.is_some()
        );

        // write manifests, check number of references in manifest
        session.commit("updated", None).await?;

        // should still be deleted
        assert!(
            session.get_chunk_ref(&array_path, &ChunkIndices(vec![2])).await?.is_none()
        );
        // new ref should be present
        assert!(
            session.get_chunk_ref(&array_path, &ChunkIndices(vec![3])).await?.is_some()
        );

        Ok(())
    }

    #[tokio_test]
    async fn test_repository_with_updates() -> Result<(), Box<dyn Error>> {
        let storage: Arc<dyn Storage + Send + Sync> = new_in_memory_storage().await?;
        let storage_settings = storage.default_settings();
        let asset_manager =
            AssetManager::new_no_cache(Arc::clone(&storage), storage_settings.clone(), 1);

        let array_id = NodeId::random();
        let chunk1 = ChunkInfo {
            node: array_id.clone(),
            coord: ChunkIndices(vec![0, 0, 0]),
            payload: ChunkPayload::Ref(ChunkRef {
                id: ObjectId::random(),
                offset: 0,
                length: 4,
            }),
        };

        let chunk2 = ChunkInfo {
            node: array_id.clone(),
            coord: ChunkIndices(vec![0, 0, 1]),
            payload: ChunkPayload::Inline("hello".into()),
        };

        let manifest =
            Manifest::from_iter(vec![chunk1.clone(), chunk2.clone()]).await?.unwrap();
        let manifest = Arc::new(manifest);
        let manifest_id = manifest.id();
        let manifest_size = asset_manager.write_manifest(Arc::clone(&manifest)).await?;

        let shape = ArrayShape::new(vec![(2, 1), (2, 1), (2, 1)]).unwrap();
        let dimension_names = Some(vec!["x".into(), "y".into(), "t".into()]);

        let manifest_ref = ManifestRef {
            object_id: manifest_id.clone(),
            extents: ManifestExtents::new(&[0, 0, 0], &[1, 1, 2]),
        };

        let group_def = Bytes::from_static(br#"{"some":"group"}"#);
        let array_def = Bytes::from_static(br#"{"this":"array"}"#);

        let array1_path: Path = "/array1".try_into().unwrap();
        let node_id = NodeId::random();
        let nodes = vec![
            NodeSnapshot {
                path: Path::root(),
                id: node_id,
                node_data: NodeData::Group,
                user_data: group_def.clone(),
            },
            NodeSnapshot {
                path: array1_path.clone(),
                id: array_id.clone(),
                node_data: NodeData::Array {
                    shape: shape.clone(),
                    dimension_names: dimension_names.clone(),
                    manifests: vec![manifest_ref.clone()],
                },
                user_data: array_def.clone(),
            },
        ];

        let initial = Snapshot::initial().unwrap();
        let manifests = vec![ManifestFileInfo::new(manifest.as_ref(), manifest_size)];
        let snapshot = Arc::new(Snapshot::from_iter(
            None,
            Some(initial.id().clone()),
            "message".to_string(),
            None,
            manifests,
            None,
            nodes.iter().cloned().map(Ok::<NodeSnapshot, Infallible>),
        )?);
        asset_manager.write_snapshot(Arc::clone(&snapshot)).await?;
        update_branch(
            storage.as_ref(),
            &storage_settings,
            "main",
            snapshot.id().clone(),
            None,
        )
        .await?;
        Repository::store_config(
            storage.as_ref(),
            &RepositoryConfig::default(),
            &storage::VersionInfo::for_creation(),
        )
        .await?;

        let repo = Repository::open(None, storage, HashMap::new()).await?;
        let mut ds = repo.writable_session("main").await?;

        // retrieve the old array node
        let node = ds.get_node(&array1_path).await?;
        assert_eq!(nodes.get(1).unwrap(), &node);

        let group_name = "/tbd-group".to_string();
        ds.add_group(
            group_name.clone().try_into().unwrap(),
            Bytes::copy_from_slice(b"somedef"),
        )
        .await?;
        ds.delete_group(group_name.clone().try_into().unwrap()).await?;
        // deleting non-existing is no-op
        assert!(ds.delete_group(group_name.clone().try_into().unwrap()).await.is_ok());
        assert!(ds.get_node(&group_name.try_into().unwrap()).await.is_err());

        // add a new array and retrieve its node
        ds.add_group("/group".try_into().unwrap(), Bytes::copy_from_slice(b"somedef2"))
            .await?;

        let shape2 = ArrayShape::new(vec![(2, 2)]).unwrap();
        let dimension_names2 = Some(vec!["t".into()]);

        let array_def2 = Bytes::from_static(br#"{"this":"other array"}"#);

        let new_array_path: Path = "/group/array2".to_string().try_into().unwrap();
        ds.add_array(
            new_array_path.clone(),
            shape2.clone(),
            dimension_names2.clone(),
            array_def2.clone(),
        )
        .await?;

        ds.delete_array(new_array_path.clone()).await?;
        // Delete a non-existent array is no-op
        assert!(ds.delete_array(new_array_path.clone()).await.is_ok());
        assert!(ds.get_node(&new_array_path.clone()).await.is_err());

        ds.add_array(
            new_array_path.clone(),
            shape2.clone(),
            dimension_names2.clone(),
            array_def2.clone(),
        )
        .await?;

        let node = ds.get_node(&new_array_path).await;
        assert!(matches!(
            node.ok(),
            Some(NodeSnapshot {path,node_data, user_data, .. })
                if path== new_array_path.clone() &&
                    user_data == array_def2 &&
                    node_data == NodeData::Array{shape:shape2, dimension_names:dimension_names2, manifests: vec![]}
        ));

        // update the array definition
        let shape3 = ArrayShape::new(vec![(4, 3)]).unwrap();
        let dimension_names3 = Some(vec!["tt".into()]);

        let array_def3 = Bytes::from_static(br#"{"this":"yet other array"}"#);
        ds.update_array(
            &new_array_path.clone(),
            shape3.clone(),
            dimension_names3.clone(),
            array_def3.clone(),
        )
        .await?;
        let node = ds.get_node(&new_array_path).await;
        assert!(matches!(
            node.ok(),
            Some(NodeSnapshot {path,node_data, user_data, .. })
                if path == "/group/array2".try_into().unwrap() &&
                    user_data == array_def3 &&
                    node_data == NodeData::Array { shape:shape3, dimension_names: dimension_names3, manifests: vec![] }
        ));

        let payload = ds.get_chunk_writer()(Bytes::copy_from_slice(b"foo")).await?;
        ds.set_chunk_ref(new_array_path.clone(), ChunkIndices(vec![0]), Some(payload))
            .await?;

        let chunk = ds.get_chunk_ref(&new_array_path, &ChunkIndices(vec![0])).await?;
        assert_eq!(chunk, Some(ChunkPayload::Inline("foo".into())));

        // retrieve a non initialized chunk of the new array
        let non_chunk = ds.get_chunk_ref(&new_array_path, &ChunkIndices(vec![1])).await?;
        assert_eq!(non_chunk, None);

        // update old array zarr metadata and check it
        let shape3 = ArrayShape::new(vec![(8, 3)]).unwrap();
        let dimension_names3 = Some(vec!["tt".into()]);

        let array_def3 = Bytes::from_static(br#"{"this":"more arrays"}"#);
        ds.update_array(
            &new_array_path.clone(),
            shape3.clone(),
            dimension_names3.clone(),
            array_def3.clone(),
        )
        .await?;
        let node = ds.get_node(&new_array_path).await;
        if let Ok(NodeSnapshot { node_data: NodeData::Array { shape, .. }, .. }) = &node {
            assert_eq!(shape, &shape3);
        } else {
            panic!("Failed to update zarr metadata");
        }

        // set old array chunk and check them
        let data = Bytes::copy_from_slice(b"foo".repeat(512).as_slice());
        let payload = ds.get_chunk_writer()(data.clone()).await?;
        ds.set_chunk_ref(new_array_path.clone(), ChunkIndices(vec![0]), Some(payload))
            .await?;

        let chunk = get_chunk(
            ds.get_chunk_reader(&new_array_path, &ChunkIndices(vec![0]), &ByteRange::ALL)
                .await
                .unwrap(),
        )
        .await?;
        assert_eq!(chunk, Some(data));

        // reduce size of dimension
        // // update old array zarr metadata and check it
        let shape4 = ArrayShape::new(vec![(6, 3)]).unwrap();
        let array_def3 = Bytes::from_static(br#"{"this":"more arrays"}"#);
        ds.update_array(
            &new_array_path.clone(),
            shape4.clone(),
            dimension_names3.clone(),
            array_def3.clone(),
        )
        .await?;
        let node = ds.get_node(&new_array_path).await;
        if let Ok(NodeSnapshot { node_data: NodeData::Array { shape, .. }, .. }) = &node {
            assert_eq!(shape, &shape4);
        } else {
            panic!("Failed to update zarr metadata");
        }

        // set old array chunk and check them
        let data = Bytes::copy_from_slice(b"old".repeat(512).as_slice());
        let payload = ds.get_chunk_writer()(data.clone()).await?;
        ds.set_chunk_ref(new_array_path.clone(), ChunkIndices(vec![0]), Some(payload))
            .await?;
        let data = Bytes::copy_from_slice(b"new".repeat(512).as_slice());
        let payload = ds.get_chunk_writer()(data.clone()).await?;
        ds.set_chunk_ref(new_array_path.clone(), ChunkIndices(vec![1]), Some(payload))
            .await?;

        let chunk = get_chunk(
            ds.get_chunk_reader(&new_array_path, &ChunkIndices(vec![1]), &ByteRange::ALL)
                .await
                .unwrap(),
        )
        .await?;
        assert_eq!(chunk, Some(data.clone()));

        ds.commit("commit", Some(SnapshotProperties::default())).await?;

        let chunk = get_chunk(
            ds.get_chunk_reader(&new_array_path, &ChunkIndices(vec![1]), &ByteRange::ALL)
                .await
                .unwrap(),
        )
        .await?;
        assert_eq!(chunk, Some(data));
        Ok(())
    }

    #[tokio_test]
    async fn test_repository_with_updates_and_writes() -> Result<(), Box<dyn Error>> {
        let backend: Arc<dyn Storage + Send + Sync> = new_in_memory_storage().await?;

        let logging = Arc::new(LoggingStorage::new(Arc::clone(&backend)));
        let logging_c: Arc<dyn Storage + Send + Sync> = logging.clone();
        let storage = Arc::clone(&logging_c);

        let repository = Repository::create(None, storage, HashMap::new()).await?;

        let mut ds = repository.writable_session("main").await?;

        let initial_snapshot = repository.lookup_branch("main").await?;

        let diff = ds.status().await?;
        assert!(diff.is_empty());

        let user_data = Bytes::copy_from_slice(b"foo");
        // add a new array and retrieve its node
        ds.add_group(Path::root(), user_data.clone()).await?;
        let diff = ds.status().await?;
        assert!(!diff.is_empty());
        assert_eq!(diff.new_groups, [Path::root()].into());

        let first_commit =
            ds.commit("commit", Some(SnapshotProperties::default())).await?;

        // We need a new session after the commit
        let mut ds = repository.writable_session("main").await?;

        //let node_id3 = NodeId::random();
        assert_eq!(first_commit, ds.snapshot_id);
        assert!(matches!(
            ds.get_node(&Path::root()).await.ok(),
            Some(NodeSnapshot { path, user_data: actual_user_data, node_data, .. })
              if path == Path::root() && user_data == actual_user_data && node_data == NodeData::Group
        ));

        let user_data2 = Bytes::copy_from_slice(b"bar");
        ds.add_group("/group".try_into().unwrap(), user_data2.clone()).await?;
        let _snapshot_id =
            ds.commit("commit", Some(SnapshotProperties::default())).await?;

        let mut ds = repository.writable_session("main").await?;
        assert!(matches!(
            ds.get_node(&Path::root()).await.ok(),
            Some(NodeSnapshot { path, user_data: actual_user_data, node_data, .. })
              if path == Path::root() && user_data==actual_user_data && node_data == NodeData::Group
        ));

        assert!(matches!(
            ds.get_node(&"/group".try_into().unwrap()).await.ok(),
            Some(NodeSnapshot { path, user_data:actual_user_data, node_data, .. })
              if path == "/group".try_into().unwrap() && user_data2 == actual_user_data && node_data == NodeData::Group
        ));

        let shape = ArrayShape::new([(1, 1), (1, 1), (2, 1)]).unwrap();
        let dimension_names = Some(vec!["x".into(), "y".into(), "z".into()]);
        let array_user_data = Bytes::copy_from_slice(b"array");

        let new_array_path: Path = "/group/array1".try_into().unwrap();
        ds.add_array(
            new_array_path.clone(),
            shape.clone(),
            dimension_names.clone(),
            array_user_data.clone(),
        )
        .await?;

        let diff = ds.status().await?;
        assert!(!diff.is_empty());
        assert_eq!(diff.new_arrays, [new_array_path.clone()].into());

        // wo commit to test the case of a chunkless array
        let _snapshot_id =
            ds.commit("commit", Some(SnapshotProperties::default())).await?;

        let mut ds = repository.writable_session("main").await?;

        let new_new_array_path: Path = "/group/array2".try_into().unwrap();
        ds.add_array(
            new_new_array_path.clone(),
            shape.clone(),
            dimension_names.clone(),
            array_user_data.clone(),
        )
        .await?;

        assert!(ds.has_uncommitted_changes());
        let changes = ds.discard_changes();
        assert!(!changes.is_empty());
        assert!(!ds.has_uncommitted_changes());

        // we set a chunk in a new array
        ds.set_chunk_ref(
            new_array_path.clone(),
            ChunkIndices(vec![0, 0, 0]),
            Some(ChunkPayload::Inline("hello".into())),
        )
        .await?;

        let diff = ds.status().await?;
        assert!(!diff.is_empty());
        assert_eq!(
            diff.updated_chunks,
            [(new_array_path.clone(), [ChunkIndices(vec![0, 0, 0])].into())].into()
        );

        let _snapshot_id =
            ds.commit("commit", Some(SnapshotProperties::default())).await?;

        let mut ds = repository.writable_session("main").await?;
        assert!(matches!(
            ds.get_node(&Path::root()).await.ok(),
            Some(NodeSnapshot { path, user_data: actual_user_data, node_data, .. })
              if path == Path::root() && user_data == actual_user_data && node_data == NodeData::Group
        ));
        assert!(matches!(
            ds.get_node(&"/group".try_into().unwrap()).await.ok(),
            Some(NodeSnapshot { path, user_data: actual_user_data, node_data, .. })
              if path == "/group".try_into().unwrap()  && user_data2 == actual_user_data &&  node_data == NodeData::Group
        ));
        assert!(matches!(
            ds.get_node(&new_array_path).await.ok(),
            Some(NodeSnapshot {
                path,
                user_data: actual_user_data,
                node_data: NodeData::Array{ shape: actual_shape, dimension_names: actual_dim, manifests },
                ..
            }) if path == new_array_path
                    && actual_user_data == array_user_data
                    && actual_shape == shape
                    && actual_dim == dimension_names
                    && manifests.len() == 1
        ));
        assert_eq!(
            ds.get_chunk_ref(&new_array_path, &ChunkIndices(vec![0, 0, 0])).await?,
            Some(ChunkPayload::Inline("hello".into()))
        );

        // we modify a chunk in an existing array
        ds.set_chunk_ref(
            new_array_path.clone(),
            ChunkIndices(vec![0, 0, 0]),
            Some(ChunkPayload::Inline("bye".into())),
        )
        .await?;

        // we add a new chunk in an existing array
        ds.set_chunk_ref(
            new_array_path.clone(),
            ChunkIndices(vec![0, 0, 1]),
            Some(ChunkPayload::Inline("new chunk".into())),
        )
        .await?;

        let previous_snapshot_id = ds.commit("commit", None).await?;

        let mut ds = repository.writable_session("main").await?;

        let snap =
            repository.asset_manager().fetch_snapshot(&previous_snapshot_id).await?;
        match &snap.get_node(&new_array_path)?.node_data {
            NodeData::Array { manifests, .. } => {
                assert_eq!(
                    manifests.first().unwrap().extents,
                    ManifestExtents::new(&[0, 0, 0], &[1, 1, 2])
                );
            }
            NodeData::Group => {
                panic!("not an array")
            }
        }

        assert_eq!(
            ds.get_chunk_ref(&new_array_path, &ChunkIndices(vec![0, 0, 0])).await?,
            Some(ChunkPayload::Inline("bye".into()))
        );
        assert_eq!(
            ds.get_chunk_ref(&new_array_path, &ChunkIndices(vec![0, 0, 1])).await?,
            Some(ChunkPayload::Inline("new chunk".into()))
        );

        // we delete a chunk
        ds.set_chunk_ref(new_array_path.clone(), ChunkIndices(vec![0, 0, 1]), None)
            .await?;

        let new_shape = ArrayShape::new([(1, 1), (1, 1), (1, 1)]).unwrap();
        let new_dimension_names = Some(vec!["X".into(), "X".into(), "Z".into()]);
        let new_user_data = Bytes::copy_from_slice(b"new data");
        // we change zarr metadata
        ds.update_array(
            &new_array_path.clone(),
            new_shape.clone(),
            new_dimension_names.clone(),
            new_user_data.clone(),
        )
        .await?;

        let snapshot_id = ds.commit("commit", None).await?;

        let snap = repository.asset_manager().fetch_snapshot(&snapshot_id).await?;
        match &snap.get_node(&new_array_path)?.node_data {
            NodeData::Array { manifests, .. } => {
                assert_eq!(
                    manifests.first().unwrap().extents,
                    ManifestExtents::new(&[0, 0, 0], &[1, 1, 1])
                );
            }
            NodeData::Group => {
                panic!("not an array")
            }
        }

        let ds = repository
            .readonly_session(&VersionInfo::BranchTipRef("main".to_string()))
            .await?;

        assert_eq!(
            ds.get_chunk_ref(&new_array_path, &ChunkIndices(vec![0, 0, 0])).await?,
            Some(ChunkPayload::Inline("bye".into()))
        );
        assert_eq!(
            ds.get_chunk_ref(&new_array_path, &ChunkIndices(vec![0, 0, 1])).await?,
            None
        );
        assert!(matches!(
            ds.get_node(&new_array_path).await.ok(),
            Some(NodeSnapshot {
                path,
                user_data: actual_user_data,
                node_data: NodeData::Array{shape: actual_shape, dimension_names: actual_dims,  manifests},
                ..
                }) if path == new_array_path &&
                    actual_user_data == new_user_data &&
                    manifests.len() == 1 &&
                    actual_shape == new_shape &&
                    actual_dims == new_dimension_names
        ));

        // since we wrote every asset we should only have one fetch for the initial snapshot
        // TODO: this could be better, we should need none
        let ops = logging.fetch_operations();
        assert_eq!(ops.len(), 1);
        assert_eq!(ops[0].0, "fetch_snapshot");

        //test the previous version is still alive
        let ds = repository
            .readonly_session(&VersionInfo::SnapshotId(previous_snapshot_id))
            .await?;
        assert_eq!(
            ds.get_chunk_ref(&new_array_path, &ChunkIndices(vec![0, 0, 0])).await?,
            Some(ChunkPayload::Inline("bye".into()))
        );
        assert_eq!(
            ds.get_chunk_ref(&new_array_path, &ChunkIndices(vec![0, 0, 1])).await?,
            Some(ChunkPayload::Inline("new chunk".into()))
        );

        let diff = repository
            .diff(
                &VersionInfo::SnapshotId(initial_snapshot),
                &VersionInfo::BranchTipRef("main".to_string()),
            )
            .await?;

        assert!(diff.deleted_groups.is_empty());
        assert!(diff.deleted_arrays.is_empty());
        assert_eq!(
            &diff.new_groups,
            &["/".try_into().unwrap(), "/group".try_into().unwrap()].into()
        );
        assert_eq!(
            &diff.new_arrays,
            &[new_array_path.clone()].into() // we never committed array2
        );
        assert_eq!(
            &diff.updated_chunks,
            &[(
                new_array_path.clone(),
                [ChunkIndices(vec![0, 0, 0]), ChunkIndices(vec![0, 0, 1])].into()
            )]
            .into()
        );
        assert_eq!(&diff.updated_arrays, &[new_array_path.clone()].into());
        assert_eq!(&diff.updated_groups, &[].into());

        let diff = repository
            .diff(
                &VersionInfo::SnapshotId(first_commit),
                &VersionInfo::BranchTipRef("main".to_string()),
            )
            .await?;

        // Diff should not include the changes in `from`
        assert!(diff.deleted_groups.is_empty());
        assert!(diff.deleted_arrays.is_empty());
        assert_eq!(&diff.new_groups, &["/group".try_into().unwrap()].into());
        assert_eq!(
            &diff.new_arrays,
            &[new_array_path.clone()].into() // we never committed array2
        );
        assert_eq!(
            &diff.updated_chunks,
            &[(
                new_array_path.clone(),
                [ChunkIndices(vec![0, 0, 0]), ChunkIndices(vec![0, 0, 1])].into()
            )]
            .into()
        );
        assert_eq!(&diff.updated_arrays, &[new_array_path.clone()].into());
        assert_eq!(&diff.updated_groups, &[].into());
        Ok(())
    }

    #[tokio_test]
    async fn test_basic_delete_and_flush() -> Result<(), Box<dyn Error>> {
        let repository = create_memory_store_repository().await;
        let mut ds = repository.writable_session("main").await?;
        ds.add_group(Path::root(), Bytes::copy_from_slice(b"")).await?;
        ds.add_group("/1".try_into().unwrap(), Bytes::copy_from_slice(b"")).await?;
        ds.delete_group("/1".try_into().unwrap()).await?;
        assert_eq!(ds.list_nodes().await?.count(), 1);
        ds.commit("commit", None).await?;

        let ds = repository
            .readonly_session(&VersionInfo::BranchTipRef("main".to_string()))
            .await?;
        assert!(ds.get_group(&Path::root()).await.is_ok());
        assert!(ds.get_group(&"/1".try_into().unwrap()).await.is_err());
        assert_eq!(ds.list_nodes().await?.count(), 1);
        Ok(())
    }

    #[tokio_test]
    async fn test_basic_delete_after_flush() -> Result<(), Box<dyn Error>> {
        let repository = create_memory_store_repository().await;
        let mut ds = repository.writable_session("main").await?;
        ds.add_group(Path::root(), Bytes::copy_from_slice(b"")).await?;
        ds.add_group("/1".try_into().unwrap(), Bytes::copy_from_slice(b"")).await?;
        ds.commit("commit", None).await?;

        let mut ds = repository.writable_session("main").await?;
        ds.delete_group("/1".try_into().unwrap()).await?;
        assert!(ds.get_group(&Path::root()).await.is_ok());
        assert!(ds.get_group(&"/1".try_into().unwrap()).await.is_err());
        assert_eq!(ds.list_nodes().await?.count(), 1);
        Ok(())
    }

    #[tokio_test]
    async fn test_commit_after_deleting_old_node() -> Result<(), Box<dyn Error>> {
        let repository = create_memory_store_repository().await;
        let mut ds = repository.writable_session("main").await?;
        ds.add_group(Path::root(), Bytes::copy_from_slice(b"")).await?;
        ds.commit("commit", None).await?;

        let mut ds = repository.writable_session("main").await?;
        ds.delete_group(Path::root()).await?;
        ds.commit("commit", None).await?;

        let ds = repository
            .readonly_session(&VersionInfo::BranchTipRef("main".to_string()))
            .await?;
        assert_eq!(ds.list_nodes().await?.count(), 0);
        Ok(())
    }

    #[tokio_test]
    async fn test_delete_children() -> Result<(), Box<dyn Error>> {
        let def = Bytes::copy_from_slice(b"");
        let repository = create_memory_store_repository().await;
        let mut ds = repository.writable_session("main").await?;
        ds.add_group(Path::root(), def.clone()).await?;
        ds.commit("initialize", None).await?;

        let mut ds = repository.writable_session("main").await?;
        ds.add_group("/a".try_into().unwrap(), def.clone()).await?;
        ds.add_group("/b".try_into().unwrap(), def.clone()).await?;
        ds.add_group("/b/bb".try_into().unwrap(), def.clone()).await?;

        ds.delete_group("/b".try_into().unwrap()).await?;
        assert!(ds.get_group(&"/b".try_into().unwrap()).await.is_err());
        assert!(ds.get_group(&"/b/bb".try_into().unwrap()).await.is_err());

        ds.delete_group("/a".try_into().unwrap()).await?;
        assert!(ds.change_set.is_empty());

        // try deleting the child group again, since this was a group that was already
        // deleted, it should be a no-op
        ds.delete_group("/b/bb".try_into().unwrap()).await?;
        ds.delete_group("/a".try_into().unwrap()).await?;
        assert!(ds.change_set.is_empty());
        Ok(())
    }

    #[tokio_test]
    async fn test_delete_children_of_old_nodes() -> Result<(), Box<dyn Error>> {
        let repository = create_memory_store_repository().await;
        let mut ds = repository.writable_session("main").await?;
        let def = Bytes::copy_from_slice(b"");
        ds.add_group(Path::root(), def.clone()).await?;
        ds.add_group("/a".try_into().unwrap(), def.clone()).await?;
        ds.add_group("/b".try_into().unwrap(), def.clone()).await?;
        ds.add_group("/b/bb".try_into().unwrap(), def.clone()).await?;
        ds.commit("commit", None).await?;

        let mut ds = repository.writable_session("main").await?;
        ds.delete_group("/b".try_into().unwrap()).await?;
        assert!(ds.get_group(&"/b".try_into().unwrap()).await.is_err());
        assert!(ds.get_group(&"/b/bb".try_into().unwrap()).await.is_err());
        Ok(())
    }

    #[tokio_test(flavor = "multi_thread")]
    async fn test_all_chunks_iterator() -> Result<(), Box<dyn Error>> {
        let storage: Arc<dyn Storage + Send + Sync> = new_in_memory_storage().await?;
        let repo = Repository::create(None, storage, HashMap::new()).await?;
        let mut ds = repo.writable_session("main").await?;
        let def = Bytes::copy_from_slice(b"");

        // add a new array and retrieve its node
        ds.add_group(Path::root(), def.clone()).await?;

        let shape = ArrayShape::new(vec![(4, 2), (2, 1), (4, 2)]).unwrap();
        let dimension_names = Some(vec!["t".into()]);

        let new_array_path: Path = "/array".try_into().unwrap();
        ds.add_array(
            new_array_path.clone(),
            shape.clone(),
            dimension_names.clone(),
            def.clone(),
        )
        .await?;
        // we 3 chunks
        ds.set_chunk_ref(
            new_array_path.clone(),
            ChunkIndices(vec![0, 0, 0]),
            Some(ChunkPayload::Inline("hello".into())),
        )
        .await?;
        ds.set_chunk_ref(
            new_array_path.clone(),
            ChunkIndices(vec![0, 0, 1]),
            Some(ChunkPayload::Inline("hello".into())),
        )
        .await?;
        ds.set_chunk_ref(
            new_array_path.clone(),
            ChunkIndices(vec![1, 0, 0]),
            Some(ChunkPayload::Inline("hello".into())),
        )
        .await?;
        ds.set_chunk_ref(
            new_array_path.clone(),
            ChunkIndices(vec![0, 1, 0]),
            Some(ChunkPayload::Inline("hello".into())),
        )
        .await?;
        let snapshot_id = ds.commit("commit", None).await?;
        let ds = repo.readonly_session(&VersionInfo::SnapshotId(snapshot_id)).await?;

        let coords = ds
            .all_chunks()
            .await?
            .map_ok(|(_, chunk)| chunk.coord)
            .try_collect::<HashSet<_>>()
            .await?;
        assert_eq!(
            coords,
            vec![
                ChunkIndices(vec![0, 0, 0]),
                ChunkIndices(vec![0, 0, 1]),
                ChunkIndices(vec![1, 0, 0]),
                ChunkIndices(vec![0, 1, 0])
            ]
            .into_iter()
            .collect()
        );
        Ok(())
    }

    #[tokio_test]
    async fn test_manifests_shrink() -> Result<(), Box<dyn Error>> {
        let in_mem_storage = Arc::new(ObjectStorage::new_in_memory().await?);
        let storage: Arc<dyn Storage + Send + Sync> = in_mem_storage.clone();
        let repo = Repository::create(None, Arc::clone(&storage), HashMap::new()).await?;

        // there should be no manifests yet
        assert!(
            !in_mem_storage.all_keys().await?.iter().any(|key| key.contains("manifest"))
        );

        // initialization creates one snapshot
        assert_eq!(
            1,
            in_mem_storage
                .all_keys()
                .await?
                .iter()
                .filter(|key| key.contains("snapshot"))
                .count(),
        );

        let mut ds = repo.writable_session("main").await?;
        let def = Bytes::copy_from_slice(b"");

        let shape = ArrayShape::new(vec![(5, 2), (5, 1)]).unwrap();
        let dimension_names = Some(vec!["t".into()]);

        ds.add_group(Path::root(), def.clone()).await?;
        let a1path: Path = "/array1".try_into()?;
        let a2path: Path = "/array2".try_into()?;

        ds.add_array(a1path.clone(), shape.clone(), dimension_names.clone(), def.clone())
            .await?;
        ds.add_array(a2path.clone(), shape.clone(), dimension_names.clone(), def.clone())
            .await?;

        let _ = ds.commit("first commit", None).await?;

        // there should be no manifests yet because we didn't add any chunks
        assert_eq!(
            0,
            in_mem_storage
                .all_keys()
                .await?
                .iter()
                .filter(|key| key.contains("manifest"))
                .count(),
        );
        // there should be two snapshots, one for the initialization commit and one for the real
        // commit
        assert_eq!(
            2,
            in_mem_storage
                .all_keys()
                .await?
                .iter()
                .filter(|key| key.contains("snapshot"))
                .count(),
        );

        let mut ds = repo.writable_session("main").await?;

        // add 3 chunks
        ds.set_chunk_ref(
            a1path.clone(),
            ChunkIndices(vec![0, 0]),
            Some(ChunkPayload::Inline("hello1".into())),
        )
        .await?;
        ds.set_chunk_ref(
            a1path.clone(),
            ChunkIndices(vec![0, 1]),
            Some(ChunkPayload::Inline("hello2".into())),
        )
        .await?;
        ds.set_chunk_ref(
            a2path.clone(),
            ChunkIndices(vec![1, 0]),
            Some(ChunkPayload::Inline("hello3".into())),
        )
        .await?;

        let _snap_id = ds.commit("commit", None).await?;

        // there should be two manifest now, one per array
        assert_eq!(
            2,
            in_mem_storage
                .all_keys()
                .await?
                .iter()
                .filter(|key| key.contains("manifest"))
                .count()
        );

        let manifest_id = match ds.get_array(&a1path).await?.node_data {
            NodeData::Array { manifests, .. } => {
                manifests.first().as_ref().unwrap().object_id.clone()
            }
            NodeData::Group => panic!("must be an array"),
        };
        let manifest =
            repo.asset_manager().fetch_manifest_unknown_size(&manifest_id).await?;
        let initial_size = manifest.len();

        // we wrote two chunks to array 1
        assert_eq!(initial_size, 2);

        let mut ds = repo.writable_session("main").await?;
        ds.delete_array(a2path).await?;
        let _snap_id = ds.commit("array2 deleted", None).await?;

        // we should still have two manifests, the same as before because only array deletes happened
        assert_eq!(
            2,
            in_mem_storage
                .all_keys()
                .await?
                .iter()
                .filter(|key| key.contains("manifest"))
                .count()
        );

        let manifest_id = match ds.get_array(&a1path).await?.node_data {
            NodeData::Array { manifests, .. } => {
                manifests.first().as_ref().unwrap().object_id.clone()
            }
            NodeData::Group => panic!("must be an array"),
        };
        let manifest =
            repo.asset_manager().fetch_manifest_unknown_size(&manifest_id).await?;
        let size_after_delete = manifest.len();

        // it's the same manifest
        assert!(size_after_delete == initial_size);

        // delete a chunk
        let mut ds = repo.writable_session("main").await?;
        ds.set_chunk_ref(a1path.clone(), ChunkIndices(vec![0, 0]), None).await?;
        let _snap_id = ds.commit("chunk deleted", None).await?;

        // there should be three manifests
        assert_eq!(
            3,
            in_mem_storage
                .all_keys()
                .await?
                .iter()
                .filter(|key| key.contains("manifest"))
                .count()
        );
        // there should be five snapshots
        assert_eq!(
            5,
            in_mem_storage
                .all_keys()
                .await?
                .iter()
                .filter(|key| key.contains("snapshot"))
                .count(),
        );

        let manifest_id = match ds.get_array(&a1path).await?.node_data {
            NodeData::Array { manifests, .. } => {
                manifests.first().as_ref().unwrap().object_id.clone()
            }
            NodeData::Group => panic!("must be an array"),
        };
        let manifest =
            repo.asset_manager().fetch_manifest_unknown_size(&manifest_id).await?;
        let size_after_chunk_delete = manifest.len();
        assert!(size_after_chunk_delete < size_after_delete);

        // delete the second chunk, now there are no chunks, so there should be no manifests either
        let mut ds = repo.writable_session("main").await?;
        ds.set_chunk_ref(a1path.clone(), ChunkIndices(vec![0, 1]), None).await?;
        let _snap_id = ds.commit("chunk deleted", None).await?;

        let manifests = match ds.get_array(&a1path).await?.node_data {
            NodeData::Array { manifests, .. } => manifests,
            NodeData::Group => panic!("must be an array"),
        };
        assert!(manifests.is_empty());

        // there should be three manifests (unchanged)
        assert_eq!(
            3,
            in_mem_storage
                .all_keys()
                .await?
                .iter()
                .filter(|key| key.contains("manifest"))
                .count()
        );
        // there should be six snapshots
        assert_eq!(
            6,
            in_mem_storage
                .all_keys()
                .await?
                .iter()
                .filter(|key| key.contains("snapshot"))
                .count(),
        );

        Ok(())
    }

    #[tokio_test(flavor = "multi_thread")]
    async fn test_commit_and_refs() -> Result<(), Box<dyn Error>> {
        let repo = create_memory_store_repository().await;
        let storage = Arc::clone(repo.storage());
        let storage_settings = storage.default_settings();
        let mut ds = repo.writable_session("main").await?;

        let def = Bytes::copy_from_slice(b"");

        // add a new array and retrieve its node
        ds.add_group(Path::root(), def.clone()).await?;
        let new_snapshot_id = ds.commit("first commit", None).await?;
        assert_eq!(
            new_snapshot_id,
            fetch_branch_tip(storage.as_ref(), &storage_settings, "main").await?.snapshot
        );
        assert_eq!(&new_snapshot_id, ds.snapshot_id());

        repo.create_tag("v1", &new_snapshot_id).await?;
        let ref_data = fetch_tag(storage.as_ref(), &storage_settings, "v1").await?;
        assert_eq!(new_snapshot_id, ref_data.snapshot);

        assert!(matches!(
                ds.get_node(&Path::root()).await.ok(),
                Some(NodeSnapshot { node_data, path, ..})
                    if path == Path::root()  && node_data == NodeData::Group
        ));

        let mut ds = repo.writable_session("main").await?;

        assert!(matches!(
                ds.get_node(&Path::root()).await.ok(),
                Some(NodeSnapshot { path, node_data, ..})
                        if path == Path::root()  && node_data == NodeData::Group
        ));

        let shape = ArrayShape::new(vec![(1, 1), (2, 1), (4, 2)]).unwrap();
        let dimension_names = Some(vec!["t".into()]);

        let new_array_path: Path = "/array1".try_into().unwrap();
        ds.add_array(
            new_array_path.clone(),
            shape.clone(),
            dimension_names.clone(),
            def.clone(),
        )
        .await?;
        ds.set_chunk_ref(
            new_array_path.clone(),
            ChunkIndices(vec![0, 0, 0]),
            Some(ChunkPayload::Inline("hello".into())),
        )
        .await?;
        let new_snapshot_id = ds.commit("second commit", None).await?;
        let ref_data =
            fetch_branch_tip(storage.as_ref(), &storage_settings, Ref::DEFAULT_BRANCH)
                .await?;
        assert_eq!(new_snapshot_id, ref_data.snapshot);

        let parents = repo
            .ancestry(&VersionInfo::SnapshotId(new_snapshot_id))
            .await?
            .try_collect::<Vec<_>>()
            .await?;
        assert_eq!(parents[0].message, "second commit");
        assert_eq!(parents[1].message, "first commit");
        assert_eq!(parents[2].message, Snapshot::INITIAL_COMMIT_MESSAGE);
        assert_eq!(parents[2].id, Snapshot::INITIAL_SNAPSHOT_ID);
        itertools::assert_equal(
            parents.iter().sorted_by_key(|m| m.flushed_at).rev(),
            parents.iter(),
        );

        Ok(())
    }

    #[tokio_test]
    async fn test_no_double_commit() -> Result<(), Box<dyn Error>> {
        let repository = create_memory_store_repository().await;

        let mut ds1 = repository.writable_session("main").await?;
        let mut ds2 = repository.writable_session("main").await?;

        let def = Bytes::copy_from_slice(b"");
        ds1.add_group("/a".try_into().unwrap(), def.clone()).await?;
        ds2.add_group("/b".try_into().unwrap(), def.clone()).await?;

        let barrier = Arc::new(Barrier::new(2));
        let barrier_c = Arc::clone(&barrier);
        let barrier_cc = Arc::clone(&barrier);
        let handle1 = tokio::spawn(async move {
            let _ = barrier_c.wait().await;
            ds1.commit("from 1", None).await
        });

        let handle2 = tokio::spawn(async move {
            let _ = barrier_cc.wait().await;
            ds2.commit("from 2", None).await
        });

        let res1 = handle1.await.unwrap();
        let res2 = handle2.await.unwrap();

        // We check there is one error and one success, and that the error points to the right
        // conflicting commit
        let ok = match (&res1, &res2) {
            (
                Ok(new_snap),
                Err(SessionError {
                    kind: SessionErrorKind::Conflict { expected_parent: _, actual_parent },
                    ..
                }),
            ) if Some(new_snap) == actual_parent.as_ref() => true,
            (
                Err(SessionError {
                    kind: SessionErrorKind::Conflict { expected_parent: _, actual_parent },
                    ..
                }),
                Ok(new_snap),
            ) if Some(new_snap) == actual_parent.as_ref() => true,
            _ => false,
        };
        assert!(ok);

        let ds = repository
            .readonly_session(&VersionInfo::BranchTipRef("main".to_string()))
            .await?;
        let parents = repository
            .ancestry(&VersionInfo::SnapshotId(ds.snapshot_id))
            .await?
            .try_collect::<Vec<_>>()
            .await?;
        assert_eq!(parents.len(), 2);
        let msg = parents[0].message.as_str();
        assert!(msg == "from 1" || msg == "from 2");

        assert_eq!(parents[1].message.as_str(), Snapshot::INITIAL_COMMIT_MESSAGE);
        assert_eq!(parents[1].id, Snapshot::INITIAL_SNAPSHOT_ID);
        Ok(())
    }

    #[tokio_test]
    async fn test_setting_w_invalid_coords() -> Result<(), Box<dyn Error>> {
        let in_mem_storage = new_in_memory_storage().await?;
        let storage: Arc<dyn Storage + Send + Sync> = in_mem_storage.clone();
        let repo = Repository::create(None, Arc::clone(&storage), HashMap::new()).await?;
        let mut ds = repo.writable_session("main").await?;

        let shape = ArrayShape::new(vec![(5, 2), (5, 2)]).unwrap();
        ds.add_group(Path::root(), Bytes::new()).await?;

        let apath: Path = "/array1".try_into()?;

        ds.add_array(apath.clone(), shape, None, Bytes::new()).await?;

        ds.commit("first commit", None).await?;

        // add 3 chunks
        // First 2 chunks are valid, third will be invalid chunk indices

        let mut ds = repo.writable_session("main").await?;

        assert!(
            ds.set_chunk_ref(
                apath.clone(),
                ChunkIndices(vec![0, 0]),
                Some(ChunkPayload::Inline("hello".into())),
            )
            .await
            .is_ok()
        );
        assert!(
            ds.set_chunk_ref(
                apath.clone(),
                ChunkIndices(vec![2, 2]),
                Some(ChunkPayload::Inline("hello".into())),
            )
            .await
            .is_ok()
        );

        let bad_result = ds
            .set_chunk_ref(
                apath.clone(),
                ChunkIndices(vec![3, 0]),
                Some(ChunkPayload::Inline("hello".into())),
            )
            .await;

        match bad_result {
            Err(SessionError {
                kind: SessionErrorKind::InvalidIndex { coords, path },
                ..
            }) => {
                assert_eq!(coords, ChunkIndices(vec![3, 0]));
                assert_eq!(path, apath);
            }
            _ => panic!("Expected InvalidIndex Error"),
        }
        Ok(())
    }

    /// Construct two repos on the same storage, with a commit, a group and an array
    ///
    /// Group: /foo/bar
    /// Array: /foo/bar/some-array
    async fn get_repo_for_conflict() -> Result<Repository, Box<dyn Error>> {
        let repository = create_memory_store_repository().await;
        let mut ds = repository.writable_session("main").await?;

        ds.add_group("/foo/bar".try_into().unwrap(), Bytes::new()).await?;
        ds.add_array(
            "/foo/bar/some-array".try_into().unwrap(),
            basic_shape(),
            None,
            Bytes::new(),
        )
        .await?;
        ds.commit("create directory", None).await?;

        Ok(repository)
    }
    async fn get_sessions_for_conflict() -> Result<(Session, Session), Box<dyn Error>> {
        let repository = get_repo_for_conflict().await?;

        let ds = repository.writable_session("main").await?;
        let ds2 = repository.writable_session("main").await?;

        Ok((ds, ds2))
    }

    fn basic_shape() -> ArrayShape {
        ArrayShape::new(vec![(5, 1)]).unwrap()
    }

    fn user_data() -> Bytes {
        Bytes::new()
    }

    fn assert_has_conflict(conflict: &Conflict, rebase_result: SessionResult<()>) {
        match rebase_result {
            Err(SessionError {
                kind: SessionErrorKind::RebaseFailed { conflicts, .. },
                ..
            }) => {
                assert!(conflicts.contains(conflict));
            }
            other => panic!("test failed, expected conflict, got {other:?}"),
        }
    }

    #[tokio_test]
    /// Test conflict detection
    ///
    /// This session: add array
    /// Previous commit: add group on same path
    async fn test_conflict_detection_node_conflict_with_existing_node()
    -> Result<(), Box<dyn Error>> {
        let (mut ds1, mut ds2) = get_sessions_for_conflict().await?;

        let conflict_path: Path = "/foo/bar/conflict".try_into().unwrap();
        ds1.add_group(conflict_path.clone(), user_data()).await?;
        ds1.commit("create group", None).await?;

        ds2.add_array(conflict_path.clone(), basic_shape(), None, user_data()).await?;
        ds2.commit("create array", None).await.unwrap_err();
        assert_has_conflict(
            &Conflict::NewNodeConflictsWithExistingNode(conflict_path),
            ds2.rebase(&ConflictDetector).await,
        );
        Ok(())
    }

    #[tokio_test]
    /// Test conflict detection
    ///
    /// This session: add array
    /// Previous commit: add array in implicit path to the session array
    async fn test_conflict_detection_node_conflict_in_path() -> Result<(), Box<dyn Error>>
    {
        let (mut ds1, mut ds2) = get_sessions_for_conflict().await?;

        let conflict_path: Path = "/foo/bar/conflict".try_into().unwrap();
        ds1.add_array(conflict_path.clone(), basic_shape(), None, user_data()).await?;
        ds1.commit("create array", None).await?;

        let inner_path: Path = "/foo/bar/conflict/inner".try_into().unwrap();
        ds2.add_array(inner_path.clone(), basic_shape(), None, user_data()).await?;
        ds2.commit("create inner array", None).await.unwrap_err();
        assert_has_conflict(
            &Conflict::NewNodeInInvalidGroup(conflict_path),
            ds2.rebase(&ConflictDetector).await,
        );
        Ok(())
    }

    #[tokio_test]
    /// Test conflict detection
    ///
    /// This session: update array metadata
    /// Previous commit: update array metadata
    async fn test_conflict_detection_double_zarr_metadata_edit()
    -> Result<(), Box<dyn Error>> {
        let (mut ds1, mut ds2) = get_sessions_for_conflict().await?;

        let path: Path = "/foo/bar/some-array".try_into().unwrap();
        ds1.update_array(&path.clone(), basic_shape(), None, user_data()).await?;
        ds1.commit("update array", None).await?;

        ds2.update_array(&path.clone(), basic_shape(), None, user_data()).await?;
        ds2.commit("update array again", None).await.unwrap_err();
        assert_has_conflict(
            &Conflict::ZarrMetadataDoubleUpdate(path),
            ds2.rebase(&ConflictDetector).await,
        );
        Ok(())
    }

    #[tokio_test]
    /// Test conflict detection
    ///
    /// This session: delete array
    /// Previous commit: update same array metadata
    async fn test_conflict_detection_metadata_edit_of_deleted()
    -> Result<(), Box<dyn Error>> {
        let (mut ds1, mut ds2) = get_sessions_for_conflict().await?;

        let path: Path = "/foo/bar/some-array".try_into().unwrap();
        ds1.delete_array(path.clone()).await?;
        ds1.commit("delete array", None).await?;

        ds2.update_array(&path.clone(), basic_shape(), None, user_data()).await?;
        ds2.commit("update array again", None).await.unwrap_err();
        assert_has_conflict(
            &Conflict::ZarrMetadataUpdateOfDeletedArray(path),
            ds2.rebase(&ConflictDetector).await,
        );
        Ok(())
    }

    #[tokio_test]
    /// Test conflict detection
    ///
    /// This session: delete array
    /// Previous commit: update same array metadata
    async fn test_conflict_detection_delete_when_array_metadata_updated()
    -> Result<(), Box<dyn Error>> {
        let (mut ds1, mut ds2) = get_sessions_for_conflict().await?;

        let path: Path = "/foo/bar/some-array".try_into().unwrap();
        ds1.update_array(&path.clone(), basic_shape(), None, user_data()).await?;
        ds1.commit("update array", None).await?;

        let node = ds2.get_node(&path).await.unwrap();
        ds2.delete_array(path.clone()).await?;
        ds2.commit("delete array", None).await.unwrap_err();
        assert_has_conflict(
            &Conflict::DeleteOfUpdatedArray { path, node_id: node.id },
            ds2.rebase(&ConflictDetector).await,
        );
        Ok(())
    }

    #[tokio_test]
    /// Test conflict detection
    ///
    /// This session: delete array
    /// Previous commit: update same array chunks
    async fn test_conflict_detection_delete_when_chunks_updated()
    -> Result<(), Box<dyn Error>> {
        let (mut ds1, mut ds2) = get_sessions_for_conflict().await?;

        let path: Path = "/foo/bar/some-array".try_into().unwrap();
        ds1.set_chunk_ref(
            path.clone(),
            ChunkIndices(vec![0]),
            Some(ChunkPayload::Inline("hello".into())),
        )
        .await?;
        ds1.commit("update chunks", None).await?;

        let node = ds2.get_node(&path).await.unwrap();
        ds2.delete_array(path.clone()).await?;
        ds2.commit("delete array", None).await.unwrap_err();
        assert_has_conflict(
            &Conflict::DeleteOfUpdatedArray { path, node_id: node.id },
            ds2.rebase(&ConflictDetector).await,
        );
        Ok(())
    }

    #[tokio_test]
    /// Test conflict detection
    ///
    /// This session: delete group
    /// Previous commit: update same group user attributes
    async fn test_conflict_detection_delete_when_group_user_data_updated()
    -> Result<(), Box<dyn Error>> {
        let (mut ds1, mut ds2) = get_sessions_for_conflict().await?;

        let path: Path = "/foo/bar".try_into().unwrap();
        ds1.update_group(&path, Bytes::new()).await?;
        ds1.commit("update user attributes", None).await?;

        let node = ds2.get_node(&path).await.unwrap();
        ds2.delete_group(path.clone()).await?;
        ds2.commit("delete group", None).await.unwrap_err();
        assert_has_conflict(
            &Conflict::DeleteOfUpdatedGroup { path, node_id: node.id },
            ds2.rebase(&ConflictDetector).await,
        );
        Ok(())
    }

    #[tokio_test]
    async fn test_rebase_without_fast_forward() -> Result<(), Box<dyn Error>> {
        let repo = create_memory_store_repository().await;

        let mut ds = repo.writable_session("main").await?;

        ds.add_group("/".try_into().unwrap(), user_data()).await?;

        let new_array_path: Path = "/array".try_into().unwrap();
        ds.add_array(new_array_path.clone(), basic_shape(), None, user_data()).await?;
        ds.commit("create array", None).await?;

        // one writer sets chunks
        // other writer sets the same chunks, generating a conflict

        let mut ds1 = repo.writable_session("main").await?;
        let mut ds2 = repo.writable_session("main").await?;

        ds1.set_chunk_ref(
            new_array_path.clone(),
            ChunkIndices(vec![0]),
            Some(ChunkPayload::Inline("hello".into())),
        )
        .await?;
        ds1.set_chunk_ref(
            new_array_path.clone(),
            ChunkIndices(vec![1]),
            Some(ChunkPayload::Inline("hello".into())),
        )
        .await?;
        let conflicting_snap = ds1.commit("write two chunks with repo 1", None).await?;

        ds2.set_chunk_ref(
            new_array_path.clone(),
            ChunkIndices(vec![0]),
            Some(ChunkPayload::Inline("hello".into())),
        )
        .await?;

        // verify we cannot commit
        if let Err(SessionError { kind: SessionErrorKind::Conflict { .. }, .. }) =
            ds2.commit("write one chunk with repo2", None).await
        {
            // detect conflicts using rebase
            let result = ds2.rebase(&ConflictDetector).await;
            // assert the conflict is double chunk update
            assert!(matches!(
            result,
            Err(SessionError{kind: SessionErrorKind::RebaseFailed { snapshot, conflicts, },..})
                if snapshot == conflicting_snap &&
                conflicts.len() == 1 &&
                matches!(conflicts[0], Conflict::ChunkDoubleUpdate { ref path, ref chunk_coordinates, .. }
                            if path == &new_array_path && chunk_coordinates == &[ChunkIndices(vec![0])].into())
                ));
        } else {
            panic!("Bad test, it should conflict")
        }

        Ok(())
    }

    #[tokio_test]
    async fn test_rebase_fast_forwarding_over_chunk_writes() -> Result<(), Box<dyn Error>>
    {
        let repo = create_memory_store_repository().await;

        let mut ds = repo.writable_session("main").await?;

        ds.add_group("/".try_into().unwrap(), user_data()).await?;
        let new_array_path: Path = "/array".try_into().unwrap();
        ds.add_array(new_array_path.clone(), basic_shape(), None, user_data()).await?;
        let _array_created_snap = ds.commit("create array", None).await?;

        let mut ds1 = repo.writable_session("main").await?;
        let mut ds2 = repo.writable_session("main").await?;

        ds1.set_chunk_ref(
            new_array_path.clone(),
            ChunkIndices(vec![0]),
            Some(ChunkPayload::Inline("hello0".into())),
        )
        .await?;
        ds1.set_chunk_ref(
            new_array_path.clone(),
            ChunkIndices(vec![1]),
            Some(ChunkPayload::Inline("hello1".into())),
        )
        .await?;

        let new_array_2_path: Path = "/array_2".try_into().unwrap();
        ds1.add_array(new_array_2_path.clone(), basic_shape(), None, user_data()).await?;
        ds1.set_chunk_ref(
            new_array_2_path.clone(),
            ChunkIndices(vec![0]),
            Some(ChunkPayload::Inline("bye0".into())),
        )
        .await?;

        let conflicting_snap = ds1.commit("write two chunks with repo 1", None).await?;

        // let's try to create a new commit, that conflicts with the previous one but writes to
        // different chunks
        ds2.set_chunk_ref(
            new_array_path.clone(),
            ChunkIndices(vec![2]),
            Some(ChunkPayload::Inline("hello2".into())),
        )
        .await?;
        if let Err(SessionError { kind: SessionErrorKind::Conflict { .. }, .. }) =
            ds2.commit("write one chunk with repo2", None).await
        {
            let solver = BasicConflictSolver::default();
            // different chunks were written so this should fast forward
            ds2.rebase(&solver).await?;
            let snapshot = ds2.commit("after conflict", None).await?;
            let data = ds2.get_chunk_ref(&new_array_path, &ChunkIndices(vec![2])).await?;
            assert_eq!(data, Some(ChunkPayload::Inline("hello2".into())));

            // other chunks written by the conflicting commit are still there
            let data = ds2.get_chunk_ref(&new_array_path, &ChunkIndices(vec![0])).await?;
            assert_eq!(data, Some(ChunkPayload::Inline("hello0".into())));
            let data = ds2.get_chunk_ref(&new_array_path, &ChunkIndices(vec![1])).await?;
            assert_eq!(data, Some(ChunkPayload::Inline("hello1".into())));

            // new arrays written by the conflicting commit are still there
            let data =
                ds2.get_chunk_ref(&new_array_2_path, &ChunkIndices(vec![0])).await?;
            assert_eq!(data, Some(ChunkPayload::Inline("bye0".into())));

            let commits = repo
                .ancestry(&VersionInfo::SnapshotId(snapshot))
                .await?
                .try_collect::<Vec<_>>()
                .await?;
            assert_eq!(commits[0].message, "after conflict");
            assert_eq!(commits[1].message, "write two chunks with repo 1");
        } else {
            panic!("Bad test, it should conflict")
        }

        // reset the branch to what repo1 wrote
        let current_snap = fetch_branch_tip(
            repo.storage().as_ref(),
            &repo.storage().default_settings(),
            "main",
        )
        .await?
        .snapshot;
        update_branch(
            repo.storage().as_ref(),
            &repo.storage().default_settings(),
            "main",
            conflicting_snap.clone(),
            Some(&current_snap),
        )
        .await?;
        Ok(())
    }

    // TODO: We can't create writable sessions from arbitrary snapshots anymore so not sure what to do about this?
    // let's try to create a new commit, that conflicts with the previous one and writes
    // to the same chunk, recovering with "Fail" policy (so it shouldn't recover)
    // let mut repo2 =
    //     Repository::update(Arc::clone(&storage), array_created_snap.clone()).build();
    // repo2
    //     .set_chunk_ref(
    //         new_array_path.clone(),
    //         ChunkIndices(vec![1]),
    //         Some(ChunkPayload::Inline("overridden".into())),
    //     )
    //     .await?;

    // if let Err(SessionError::Conflict { .. }) =
    //     repo2.commit("main", "write one chunk with repo2", None).await
    // {
    //     let solver = BasicConflictSolver {
    //         on_chunk_conflict: VersionSelection::Fail,
    //         ..BasicConflictSolver::default()
    //     };

    //     let res = repo2.rebase(&solver, "main").await;
    //     assert!(matches!(
    //     res,
    //     Err(SessionError::RebaseFailed { snapshot, conflicts, })
    //         if snapshot == conflicting_snap &&
    //         conflicts.len() == 1 &&
    //         matches!(conflicts[0], Conflict::ChunkDoubleUpdate { ref path, ref chunk_coordinates, .. }
    //                     if path == &new_array_path && chunk_coordinates == &[ChunkIndices(vec![1])].into())
    //         ));
    // } else {
    //     panic!("Bad test, it should conflict")
    // }

    // // reset the branch to what repo1 wrote
    // let current_snap = fetch_branch_tip(storage.as_ref(), "main").await?.snapshot;
    // update_branch(
    //     storage.as_ref(),
    //     "main",
    //     conflicting_snap.clone(),
    //     Some(&current_snap),
    //     false,
    // )
    // .await?;

    // // let's try to create a new commit, that conflicts with the previous one and writes
    // // to the same chunk, recovering with "UseOurs" policy
    // let mut repo2 =
    //     Repository::update(Arc::clone(&storage), array_created_snap.clone()).build();
    // repo2
    //     .set_chunk_ref(
    //         new_array_path.clone(),
    //         ChunkIndices(vec![1]),
    //         Some(ChunkPayload::Inline("overridden".into())),
    //     )
    //     .await?;
    // if let Err(SessionError::Conflict { .. }) =
    //     repo2.commit("main", "write one chunk with repo2", None).await
    // {
    //     let solver = BasicConflictSolver {
    //         on_chunk_conflict: VersionSelection::UseOurs,
    //         ..Default::default()
    //     };

    //     repo2.rebase(&solver, "main").await?;
    //     repo2.commit("main", "after conflict", None).await?;
    //     let data =
    //         repo2.get_chunk_ref(&new_array_path, &ChunkIndices(vec![1])).await?;
    //     assert_eq!(data, Some(ChunkPayload::Inline("overridden".into())));
    //     let commits = repo2.ancestry().await?.try_collect::<Vec<_>>().await?;
    //     assert_eq!(commits[0].message, "after conflict");
    //     assert_eq!(commits[1].message, "write two chunks with repo 1");
    // } else {
    //     panic!("Bad test, it should conflict")
    // }

    // // reset the branch to what repo1 wrote
    // let current_snap = fetch_branch_tip(storage.as_ref(), "main").await?.snapshot;
    // update_branch(
    //     storage.as_ref(),
    //     "main",
    //     conflicting_snap.clone(),
    //     Some(&current_snap),
    //     false,
    // )
    // .await?;

    // // let's try to create a new commit, that conflicts with the previous one and writes
    // // to the same chunk, recovering with "UseTheirs" policy
    // let mut repo2 =
    //     Repository::update(Arc::clone(&storage), array_created_snap.clone()).build();
    // repo2
    //     .set_chunk_ref(
    //         new_array_path.clone(),
    //         ChunkIndices(vec![1]),
    //         Some(ChunkPayload::Inline("overridden".into())),
    //     )
    //     .await?;
    // if let Err(SessionError::Conflict { .. }) =
    //     repo2.commit("main", "write one chunk with repo2", None).await
    // {
    //     let solver = BasicConflictSolver {
    //         on_chunk_conflict: VersionSelection::UseTheirs,
    //         ..Default::default()
    //     };

    //     repo2.rebase(&solver, "main").await?;
    //     repo2.commit("main", "after conflict", None).await?;
    //     let data =
    //         repo2.get_chunk_ref(&new_array_path, &ChunkIndices(vec![1])).await?;
    //     assert_eq!(data, Some(ChunkPayload::Inline("hello1".into())));
    //     let commits = repo2.ancestry().await?.try_collect::<Vec<_>>().await?;
    //     assert_eq!(commits[0].message, "after conflict");
    //     assert_eq!(commits[1].message, "write two chunks with repo 1");
    // } else {
    //     panic!("Bad test, it should conflict")
    // }

    #[tokio_test]
    /// Test conflict resolution with rebase
    ///
    /// One session deletes an array, the other updates its metadata.
    /// We attempt to recover using the default [`BasicConflictSolver`]
    /// Array should still be deleted
    async fn test_conflict_resolution_delete_of_updated_array()
    -> Result<(), Box<dyn Error>> {
        let (mut ds1, mut ds2) = get_sessions_for_conflict().await?;

        let path: Path = "/foo/bar/some-array".try_into().unwrap();
        ds1.update_array(&path, basic_shape(), None, user_data()).await?;
        ds1.commit("update array", None).await?;

        ds2.delete_array(path.clone()).await?;
        ds2.commit("delete array", None).await.unwrap_err();

        ds2.rebase(&BasicConflictSolver::default()).await?;
        ds2.commit("after conflict", None).await?;

        assert!(matches!(
            ds2.get_node(&path).await,
            Err(SessionError { kind: SessionErrorKind::NodeNotFound { .. }, .. })
        ));

        Ok(())
    }

    #[tokio_test]
    /// Test conflict resolution with rebase
    ///
    /// Verify we can rebase over multiple commits if they are all fast-forwardable.
    /// We have multiple commits with chunk writes, and then a session has to rebase
    /// writing to the same chunks.
    async fn test_conflict_resolution_success_through_multiple_commits()
    -> Result<(), Box<dyn Error>> {
        let repo = get_repo_for_conflict().await?;
        let mut ds2 = repo.writable_session("main").await?;

        let path: Path = "/foo/bar/some-array".try_into().unwrap();
        // write chunks with repo 1
        for coord in [0u32, 1, 2] {
            let mut ds1 = repo.writable_session("main").await?;
            ds1.set_chunk_ref(
                path.clone(),
                ChunkIndices(vec![coord]),
                Some(ChunkPayload::Inline("repo 1".into())),
            )
            .await?;
            ds1.commit(format!("update chunk {coord}").as_str(), None).await?;
        }

        // write the same chunks with repo 2
        for coord in [0u32, 1, 2] {
            ds2.set_chunk_ref(
                path.clone(),
                ChunkIndices(vec![coord]),
                Some(ChunkPayload::Inline("repo 2".into())),
            )
            .await?;
        }

        ds2.commit("update chunk on repo 2", None).await.unwrap_err();

        let solver = BasicConflictSolver {
            on_chunk_conflict: VersionSelection::UseTheirs,
            ..Default::default()
        };

        ds2.rebase(&solver).await?;
        ds2.commit("after conflict", None).await?;
        for coord in [0, 1, 2] {
            let payload = ds2.get_chunk_ref(&path, &ChunkIndices(vec![coord])).await?;
            assert_eq!(payload, Some(ChunkPayload::Inline("repo 1".into())));
        }
        Ok(())
    }

    #[tokio_test]
    /// Rebase over multiple commits with partial failure
    ///
    /// We verify that we can partially fast forward, stopping at the first unrecoverable commit
    async fn test_conflict_resolution_failure_in_multiple_commits()
    -> Result<(), Box<dyn Error>> {
        let repo = get_repo_for_conflict().await?;

        let mut ds1 = repo.writable_session("main").await?;
        let mut ds2 = repo.writable_session("main").await?;

        let path: Path = "/foo/bar/some-array".try_into().unwrap();
        ds1.set_chunk_ref(
            path.clone(),
            ChunkIndices(vec![1]),
            Some(ChunkPayload::Inline("repo 1".into())),
        )
        .await?;
        let non_conflicting_snap = ds1.commit("updated non-conflict chunk", None).await?;

        let mut ds1 = repo.writable_session("main").await?;
        ds1.set_chunk_ref(
            path.clone(),
            ChunkIndices(vec![0]),
            Some(ChunkPayload::Inline("repo 1".into())),
        )
        .await?;

        let conflicting_snap = ds1.commit("update chunk ref", None).await?;

        ds2.set_chunk_ref(
            path.clone(),
            ChunkIndices(vec![0]),
            Some(ChunkPayload::Inline("repo 2".into())),
        )
        .await?;

        ds2.commit("update chunk ref", None).await.unwrap_err();
        // we setup a [`ConflictSolver`]` that can recover from the first but not the second
        // conflict
        let solver = BasicConflictSolver {
            on_chunk_conflict: VersionSelection::Fail,
            ..Default::default()
        };

        let err = ds2.rebase(&solver).await.unwrap_err();

        assert!(matches!(
        err,
        SessionError{kind: SessionErrorKind::RebaseFailed { snapshot, conflicts},..}
            if snapshot == conflicting_snap &&
            conflicts.len() == 1 &&
            matches!(conflicts[0], Conflict::ChunkDoubleUpdate { ref path, ref chunk_coordinates, .. }
                        if path == path && chunk_coordinates == &[ChunkIndices(vec![0])].into())
            ));

        // we were able to rebase one commit but not the second one,
        // so now the parent is the first commit
        assert_eq!(ds2.snapshot_id(), &non_conflicting_snap);

        Ok(())
    }

    #[tokio_test]
    /// Tests `commit_rebasing` retries the proper number of times when there are conflicts
    async fn test_commit_rebasing_attempts() -> Result<(), Box<dyn Error>> {
        let repo = Arc::new(create_memory_store_repository().await);
        let mut session = repo.writable_session("main").await?;
        session
            .add_array("/array".try_into().unwrap(), basic_shape(), None, Bytes::new())
            .await?;
        session.commit("create array", None).await?;

        // This is the main session we'll be trying to commit (and rebase)
        let mut session = repo.writable_session("main").await?;
        let path: Path = "/array".try_into().unwrap();
        session
            .set_chunk_ref(
                path.clone(),
                ChunkIndices(vec![1]),
                Some(ChunkPayload::Inline("repo 1".into())),
            )
            .await?;

        // we create an initial conflict for commit
        let mut session2 = repo.writable_session("main").await.unwrap();
        let path: Path = "/array".try_into().unwrap();
        session2
            .set_chunk_ref(
                path.clone(),
                ChunkIndices(vec![2]),
                Some(ChunkPayload::Inline("repo 1".into())),
            )
            .await
            .unwrap();
        session2.commit("conflicting", None).await.unwrap();

        let repo_ref = &repo;
        let attempts = AtomicU16::new(0);
        let attempts_ref = &attempts;

        // after each rebase attempt we'll run this closure that creates a new conflict
        // the result should be that it can never commit, failing after the indicated number of
        // attempts
        let conflicting = |attempt| async move {
            attempts_ref.fetch_add(1, Ordering::SeqCst); //*attempts_ref = *attempts_ref + 1;;
            assert_eq!(attempt, attempts_ref.load(Ordering::SeqCst));

            let repo_c = Arc::clone(repo_ref);
            let mut s = repo_c.writable_session("main").await.unwrap();
            s.set_chunk_ref(
                "/array".try_into().unwrap(),
                ChunkIndices(vec![2]),
                Some(ChunkPayload::Inline("repo 1".into())),
            )
            .await
            .unwrap();
            s.commit("conflicting", None).await.unwrap();
        };

        let res = session
            .commit_rebasing(
                &ConflictDetector,
                3,
                "updated non-conflict chunk",
                None,
                |_| async {},
                conflicting,
            )
            .await;

        // It has to give up eventually
        assert!(matches!(
            res,
            Err(SessionError { kind: SessionErrorKind::Conflict { .. }, .. })
        ));

        // It has to rebase 3 times
        assert_eq!(attempts.into_inner(), 3);

        let attempts = AtomicU16::new(0);
        let attempts_ref = &attempts;

        // now we'll create a new conflict twice, and finally do nothing so the commit can succeed
        let conflicting_twice = |attempt| async move {
            attempts_ref.fetch_add(1, Ordering::SeqCst); //*attempts_ref = *attempts_ref + 1;;
            assert_eq!(attempt, attempts_ref.load(Ordering::SeqCst));
            if attempt <= 2 {
                let repo_c = Arc::clone(repo_ref);

                let mut s = repo_c.writable_session("main").await.unwrap();
                s.set_chunk_ref(
                    "/array".try_into().unwrap(),
                    ChunkIndices(vec![2]),
                    Some(ChunkPayload::Inline("repo 1".into())),
                )
                .await
                .unwrap();
                s.commit("conflicting", None).await.unwrap();
            }
        };

        let res = session
            .commit_rebasing(
                &ConflictDetector,
                42,
                "updated non-conflict chunk",
                None,
                |_| async {},
                conflicting_twice,
            )
            .await;

        // The commit has to work after 3 rebase attempts
        assert!(res.is_ok());
        assert_eq!(attempts.into_inner(), 3);
        Ok(())
    }

    #[cfg(test)]
    mod state_machine_test {
        use crate::format::Path;
        use crate::format::snapshot::NodeData;
        use bytes::Bytes;
        use futures::Future;
        use proptest::prelude::*;
        use proptest::sample;
        use proptest::strategy::{BoxedStrategy, Just};
        use proptest_state_machine::{
            ReferenceStateMachine, StateMachineTest, prop_state_machine,
        };
        use std::collections::HashMap;
        use std::fmt::Debug;
        use tokio::runtime::Runtime;

        use proptest::test_runner::Config;

        use super::ArrayShape;
        use super::DimensionName;
        use super::Session;
        use super::create_memory_store_repository;
        use super::{node_paths, shapes_and_dims};

        #[derive(Clone, Debug)]
        enum RepositoryTransition {
            AddArray(Path, ArrayShape, Option<Vec<DimensionName>>, Bytes),
            UpdateArray(Path, ArrayShape, Option<Vec<DimensionName>>, Bytes),
            DeleteArray(Option<Path>),
            AddGroup(Path, Bytes),
            DeleteGroup(Option<Path>),
        }

        /// An empty type used for the `ReferenceStateMachine` implementation.
        struct RepositoryStateMachine;

        #[derive(Clone, Default, Debug)]
        struct RepositoryModel {
            arrays: HashMap<Path, (ArrayShape, Option<Vec<DimensionName>>, Bytes)>,
            groups: HashMap<Path, Bytes>,
        }

        impl ReferenceStateMachine for RepositoryStateMachine {
            type State = RepositoryModel;
            type Transition = RepositoryTransition;

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
                            .prop_map(|p| RepositoryTransition::DeleteArray(Some(p)))
                            .boxed()
                    } else {
                        Just(RepositoryTransition::DeleteArray(None)).boxed()
                    }
                };

                let delete_groups = {
                    if !state.groups.is_empty() {
                        sample::select(state.groups.keys().cloned().collect::<Vec<_>>())
                            .prop_map(|p| RepositoryTransition::DeleteGroup(Some(p)))
                            .boxed()
                    } else {
                        Just(RepositoryTransition::DeleteGroup(None)).boxed()
                    }
                };

                prop_oneof![
                    (
                        node_paths(),
                        shapes_and_dims(None),
                        proptest::collection::vec(any::<u8>(), 0..=100)
                    )
                        .prop_map(|(a, shape, user_data)| {
                            RepositoryTransition::AddArray(
                                a,
                                shape.shape,
                                shape.dimension_names,
                                Bytes::copy_from_slice(user_data.as_slice()),
                            )
                        }),
                    (
                        node_paths(),
                        shapes_and_dims(None),
                        proptest::collection::vec(any::<u8>(), 0..=100)
                    )
                        .prop_map(|(a, shape, user_data)| {
                            RepositoryTransition::UpdateArray(
                                a,
                                shape.shape,
                                shape.dimension_names,
                                Bytes::copy_from_slice(user_data.as_slice()),
                            )
                        }),
                    delete_arrays,
                    (node_paths(), proptest::collection::vec(any::<u8>(), 0..=100))
                        .prop_map(|(p, ud)| RepositoryTransition::AddGroup(
                            p,
                            Bytes::copy_from_slice(ud.as_slice())
                        )),
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
                    RepositoryTransition::AddArray(path, shape, dims, ud) => {
                        let res = state.arrays.insert(
                            path.clone(),
                            (shape.clone(), dims.clone(), ud.clone()),
                        );
                        assert!(res.is_none());
                    }
                    RepositoryTransition::UpdateArray(path, shape, dims, ud) => {
                        state
                            .arrays
                            .insert(
                                path.clone(),
                                (shape.clone(), dims.clone(), ud.clone()),
                            )
                            .expect("(postcondition) insertion failed");
                    }
                    RepositoryTransition::DeleteArray(path) => {
                        let path = path.clone().unwrap();
                        state
                            .arrays
                            .remove(&path)
                            .expect("(postcondition) deletion failed");
                    }

                    // Group ops
                    RepositoryTransition::AddGroup(path, ud) => {
                        state.groups.insert(path.clone(), ud.clone());
                        // TODO: postcondition
                    }
                    RepositoryTransition::DeleteGroup(Some(path)) => {
                        state.groups.remove(path);
                        state.groups.retain(|group, _| !group.starts_with(path));
                        state.arrays.retain(|array, _| !array.starts_with(path));
                    }
                    _ => panic!(),
                }
                state
            }

            fn preconditions(state: &Self::State, transition: &Self::Transition) -> bool {
                match transition {
                    RepositoryTransition::AddArray(path, _, _, _) => {
                        !state.arrays.contains_key(path)
                            && !state.groups.contains_key(path)
                    }
                    RepositoryTransition::UpdateArray(path, _, _, _) => {
                        state.arrays.contains_key(path)
                    }
                    RepositoryTransition::DeleteArray(path) => path.is_some(),
                    RepositoryTransition::AddGroup(path, _) => {
                        !state.arrays.contains_key(path)
                            && !state.groups.contains_key(path)
                    }
                    RepositoryTransition::DeleteGroup(p) => p.is_some(),
                }
            }
        }

        struct TestRepository {
            session: Session,
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

        impl StateMachineTest for TestRepository {
            type SystemUnderTest = Self;
            type Reference = RepositoryStateMachine;

            fn init_test(
                _ref_state: &<Self::Reference as ReferenceStateMachine>::State,
            ) -> Self::SystemUnderTest {
                let session = tokio::runtime::Runtime::new().unwrap().block_on(async {
                    let repo = create_memory_store_repository().await;
                    repo.writable_session("main").await.unwrap()
                });
                TestRepository { session, runtime: Runtime::new().unwrap() }
            }

            fn apply(
                mut state: Self::SystemUnderTest,
                _ref_state: &<Self::Reference as ReferenceStateMachine>::State,
                transition: RepositoryTransition,
            ) -> Self::SystemUnderTest {
                let runtime = &state.runtime;
                let repository = &mut state.session;
                match transition {
                    RepositoryTransition::AddArray(path, shape, dims, ud) => {
                        runtime.unwrap(repository.add_array(path, shape, dims, ud))
                    }
                    RepositoryTransition::UpdateArray(path, shape, dims, ud) => {
                        runtime.unwrap(repository.update_array(&path, shape, dims, ud))
                    }
                    RepositoryTransition::DeleteArray(Some(path)) => {
                        runtime.unwrap(repository.delete_array(path))
                    }
                    RepositoryTransition::AddGroup(path, ud) => {
                        runtime.unwrap(repository.add_group(path, ud))
                    }
                    RepositoryTransition::DeleteGroup(Some(path)) => {
                        runtime.unwrap(repository.delete_group(path))
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
                for (path, (shape, dims, ud)) in ref_state.arrays.iter() {
                    let node = runtime.unwrap(state.session.get_array(path));
                    let actual_metadata = match node.node_data {
                        NodeData::Array { shape, dimension_names, .. } => {
                            Ok((shape, dimension_names))
                        }
                        _ => Err("foo"),
                    }
                    .unwrap();
                    assert_eq!(shape, &actual_metadata.0);
                    assert_eq!(dims, &actual_metadata.1);
                    assert_eq!(ud, &node.user_data);
                }

                for (path, ud) in ref_state.groups.iter() {
                    let node = runtime.unwrap(state.session.get_group(path));
                    match node.node_data {
                        NodeData::Group => Ok(()),
                        _ => Err("foo"),
                    }
                    .unwrap();
                    assert_eq!(&node.user_data, ud)
                }
            }
        }

        prop_state_machine! {
            #![proptest_config(Config {
            verbose: 0,
            .. Config::default()
        })]

        #[icechunk_macros::test]
        fn run_repository_state_machine_test(
            // This is a macro's keyword - only `sequential` is currently supported.
            sequential
            // The number of transitions to be generated for each case. This can
            // be a single numerical value or a range as in here.
            1..20
            // Macro's boilerplate to separate the following identifier.
            =>
            // The name of the type that implements `StateMachineTest`.
            TestRepository
        );
        }
    }
}
