use std::{
    collections::HashSet,
    iter::{self},
    pin::Pin,
    sync::Arc,
};

pub use crate::{
    change_set::ChangeSet,
    format::{
        manifest::{ChunkPayload, VirtualChunkLocation},
        snapshot::{SnapshotMetadata, ZarrArrayMetadata},
        ChunkIndices, Path,
    },
    metadata::{
        ArrayShape, ChunkKeyEncoding, ChunkShape, Codec, DataType, DimensionName,
        DimensionNames, FillValue, StorageTransformer, UserAttributes,
    },
};
use crate::{
    format::{
        manifest::VirtualReferenceError, snapshot::ManifestFileInfo, ManifestId,
        SnapshotId,
    },
    storage::virtual_ref::{
        construct_valid_byte_range, ObjectStoreVirtualChunkResolverConfig,
        VirtualChunkResolver,
    },
};
use bytes::Bytes;
use chrono::Utc;
use futures::{future::ready, Future, FutureExt, Stream, StreamExt, TryStreamExt};
use itertools::Either;
use thiserror::Error;

use crate::{
    format::{
        manifest::{
            ChunkInfo, ChunkRef, Manifest, ManifestExtents, ManifestRef, VirtualChunkRef,
        },
        snapshot::{
            NodeData, NodeSnapshot, NodeType, Snapshot, SnapshotProperties,
            UserAttributesSnapshot,
        },
        ByteRange, IcechunkFormatError, NodeId, ObjectId,
    },
    refs::{
        create_tag, fetch_branch_tip, fetch_tag, update_branch, BranchVersion, Ref,
        RefError,
    },
    storage::virtual_ref::ObjectStoreVirtualChunkResolver,
    MemCachingStorage, Storage, StorageError,
};

#[derive(Clone, Debug)]
pub struct RepositoryConfig {
    // Chunks smaller than this will be stored inline in the manifst
    pub inline_chunk_threshold_bytes: u16,
    // Unsafely overwrite refs on write. This is not recommended, users should only use it at their
    // own risk in object stores for which we don't support write-object-if-not-exists. There is
    // the possibility of race conditions if this variable is set to true and there are concurrent
    // commit attempts.
    pub unsafe_overwrite_refs: bool,
}

impl Default for RepositoryConfig {
    fn default() -> Self {
        Self { inline_chunk_threshold_bytes: 512, unsafe_overwrite_refs: false }
    }
}

#[derive(Debug)]
pub struct Repository {
    config: RepositoryConfig,
    storage: Arc<dyn Storage + Send + Sync>,
    snapshot_id: SnapshotId,
    change_set: ChangeSet,
    virtual_resolver: Arc<dyn VirtualChunkResolver + Send + Sync>,
}

#[derive(Debug, Clone)]
pub struct RepositoryBuilder {
    config: RepositoryConfig,
    storage: Arc<dyn Storage + Send + Sync>,
    snapshot_id: SnapshotId,
    change_set: Option<ChangeSet>,
    virtual_ref_config: Option<ObjectStoreVirtualChunkResolverConfig>,
}

impl RepositoryBuilder {
    fn new(storage: Arc<dyn Storage + Send + Sync>, snapshot_id: SnapshotId) -> Self {
        Self {
            config: RepositoryConfig::default(),
            snapshot_id,
            storage,
            change_set: None,
            virtual_ref_config: None,
        }
    }

    pub fn with_inline_threshold_bytes(&mut self, threshold: u16) -> &mut Self {
        self.config.inline_chunk_threshold_bytes = threshold;
        self
    }

    pub fn with_unsafe_overwrite_refs(&mut self, value: bool) -> &mut Self {
        self.config.unsafe_overwrite_refs = value;
        self
    }

    pub fn with_config(&mut self, config: RepositoryConfig) -> &mut Self {
        self.config = config;
        self
    }

    pub fn with_virtual_ref_config(
        &mut self,
        config: ObjectStoreVirtualChunkResolverConfig,
    ) -> &mut Self {
        self.virtual_ref_config = Some(config);
        self
    }

    pub fn with_change_set(&mut self, change_set_bytes: ChangeSet) -> &mut Self {
        self.change_set = Some(change_set_bytes);
        self
    }

    pub fn build(&self) -> Repository {
        Repository::new(
            self.config.clone(),
            self.storage.clone(),
            self.snapshot_id.clone(),
            self.change_set.clone(),
            self.virtual_ref_config.clone(),
        )
    }
}

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum RepositoryError {
    #[error("error contacting storage {0}")]
    StorageError(#[from] StorageError),
    #[error("error in icechunk file")]
    FormatError(#[from] IcechunkFormatError),
    #[error("node not found at `{path}`: {message}")]
    NodeNotFound { path: Path, message: String },
    #[error("there is not an array at `{node:?}`: {message}")]
    NotAnArray { node: NodeSnapshot, message: String },
    #[error("there is not a group at `{node:?}`: {message}")]
    NotAGroup { node: NodeSnapshot, message: String },
    #[error("node already exists at `{node:?}`: {message}")]
    AlreadyExists { node: NodeSnapshot, message: String },
    #[error("cannot commit, no changes made to the repository")]
    NoChangesToCommit,
    #[error("unknown flush error")]
    OtherFlushError,
    #[error("ref error: `{0}`")]
    Ref(#[from] RefError),
    #[error("tag error: `{0}`")]
    Tag(String),
    #[error("branch update conflict: `({expected_parent:?}) != ({actual_parent:?})`")]
    Conflict { expected_parent: Option<SnapshotId>, actual_parent: Option<SnapshotId> },
    #[error("the repository has been initialized already (default branch exists)")]
    AlreadyInitialized,
    #[error("error when handling virtual reference {0}")]
    VirtualReferenceError(#[from] VirtualReferenceError),
    #[error("error in repository serialization `{0}`")]
    SerializationError(#[from] rmp_serde::encode::Error),
    #[error("error in repository deserialization `{0}`")]
    DeserializationError(#[from] rmp_serde::decode::Error),
}

pub type RepositoryResult<T> = Result<T, RepositoryError>;

/// FIXME: what do we want to do with implicit groups?
///
impl Repository {
    pub fn update(
        storage: Arc<dyn Storage + Send + Sync>,
        previous_version_snapshot_id: SnapshotId,
    ) -> RepositoryBuilder {
        RepositoryBuilder::new(storage, previous_version_snapshot_id)
    }

    pub async fn from_branch_tip(
        storage: Arc<dyn Storage + Send + Sync>,
        branch_name: &str,
    ) -> RepositoryResult<RepositoryBuilder> {
        let snapshot_id = fetch_branch_tip(storage.as_ref(), branch_name).await?.snapshot;
        Ok(Self::update(storage, snapshot_id))
    }

    pub async fn from_tag(
        storage: Arc<dyn Storage + Send + Sync>,
        tag_name: &str,
    ) -> RepositoryResult<RepositoryBuilder> {
        let ref_data = fetch_tag(storage.as_ref(), tag_name).await?;
        Ok(Self::update(storage, ref_data.snapshot))
    }

    /// Initialize a new repository with a single empty commit to the main branch.
    ///
    /// This is the default way to create a new repository to avoid race conditions
    /// when creating repositories.
    pub async fn init(
        storage: Arc<dyn Storage + Send + Sync>,
        unsafe_overwrite_refs: bool,
    ) -> RepositoryResult<RepositoryBuilder> {
        if Self::exists(storage.as_ref()).await? {
            return Err(RepositoryError::AlreadyInitialized);
        }
        let new_snapshot = Snapshot::empty();
        let new_snapshot_id = ObjectId::random();
        storage.write_snapshot(new_snapshot_id.clone(), Arc::new(new_snapshot)).await?;
        update_branch(
            storage.as_ref(),
            Ref::DEFAULT_BRANCH,
            new_snapshot_id.clone(),
            None,
            unsafe_overwrite_refs,
        )
        .await?;

        debug_assert!(Self::exists(storage.as_ref()).await.unwrap_or(false));

        Ok(RepositoryBuilder::new(storage, new_snapshot_id))
    }

    pub async fn exists(storage: &(dyn Storage + Send + Sync)) -> RepositoryResult<bool> {
        match fetch_branch_tip(storage, Ref::DEFAULT_BRANCH).await {
            Ok(_) => Ok(true),
            Err(RefError::RefNotFound(_)) => Ok(false),
            Err(err) => Err(err.into()),
        }
    }

    /// Provide a reasonable amount of caching for snapshots, manifests and other assets.
    /// We recommend always using some level of asset caching.
    pub fn add_in_mem_asset_caching(
        storage: Arc<dyn Storage + Send + Sync>,
    ) -> Arc<dyn Storage + Send + Sync> {
        // TODO: allow tuning once we experiment with different configurations
        Arc::new(MemCachingStorage::new(storage, 2, 2, 2, 0))
    }

    fn new(
        config: RepositoryConfig,
        storage: Arc<dyn Storage + Send + Sync>,
        snapshot_id: SnapshotId,
        change_set: Option<ChangeSet>,
        virtual_ref_config: Option<ObjectStoreVirtualChunkResolverConfig>,
    ) -> Self {
        Repository {
            snapshot_id,
            config,
            storage,
            change_set: change_set.unwrap_or_default(),
            virtual_resolver: Arc::new(ObjectStoreVirtualChunkResolver::new(
                virtual_ref_config,
            )),
        }
    }

    pub fn config(&self) -> &RepositoryConfig {
        &self.config
    }

    pub(crate) fn set_snapshot_id(&mut self, snapshot_id: SnapshotId) {
        self.snapshot_id = snapshot_id;
    }

    pub(crate) async fn set_snapshot_from_tag(
        &mut self,
        tag: &str,
    ) -> RepositoryResult<()> {
        let ref_data = fetch_tag(self.storage.as_ref(), tag).await?;
        self.snapshot_id = ref_data.snapshot;
        Ok(())
    }

    pub(crate) async fn set_snapshot_from_branch(
        &mut self,
        branch: &str,
    ) -> RepositoryResult<()> {
        let ref_data = fetch_branch_tip(self.storage.as_ref(), branch).await?;
        self.snapshot_id = ref_data.snapshot;
        Ok(())
    }

    /// Returns a pointer to the storage for the repository
    pub fn storage(&self) -> &Arc<dyn Storage + Send + Sync> {
        &self.storage
    }

    /// Returns the head snapshot id of the repository, not including
    /// anm uncommitted changes
    pub fn snapshot_id(&self) -> &SnapshotId {
        &self.snapshot_id
    }

    /// Indicates if the repository has pending changes
    pub fn has_uncommitted_changes(&self) -> bool {
        !self.change_set.is_empty()
    }

    /// Returns the sequence of parents of the current session, in order of latest first.
    pub async fn ancestry(
        &self,
    ) -> RepositoryResult<impl Stream<Item = RepositoryResult<SnapshotMetadata>>> {
        let parent = self.storage.fetch_snapshot(self.snapshot_id()).await?;
        let last = parent.metadata.clone();
        let it = if parent.short_term_history.len() < parent.total_parents as usize {
            // TODO: implement splitting of snapshot history
            Either::Left(parent.local_ancestry().chain(iter::once_with(|| todo!())))
        } else {
            Either::Right(parent.local_ancestry())
        };

        Ok(futures::stream::iter(iter::once(Ok(last)).chain(it.map(Ok))))
    }

    /// Add a group to the store.
    ///
    /// Calling this only records the operation in memory, doesn't have any consequence on the storage
    pub async fn add_group(&mut self, path: Path) -> RepositoryResult<()> {
        match self.get_node(&path).await {
            Err(RepositoryError::NodeNotFound { .. }) => {
                let id = NodeId::random();
                self.change_set.add_group(path.clone(), id);
                Ok(())
            }
            Ok(node) => Err(RepositoryError::AlreadyExists {
                node,
                message: "trying to add group".to_string(),
            }),
            Err(err) => Err(err),
        }
    }

    /// Delete a group in the hierarchy
    ///
    /// Deletes of non existing groups will succeed.
    pub async fn delete_group(&mut self, path: Path) -> RepositoryResult<()> {
        match self.get_group(&path).await {
            Ok(node) => {
                self.change_set.delete_group(node.path, &node.id);
            }
            Err(RepositoryError::NodeNotFound { .. }) => {}
            Err(err) => Err(err)?,
        }
        Ok(())
    }

    /// Add an array to the store.
    ///
    /// Calling this only records the operation in memory, doesn't have any consequence on the storage
    pub async fn add_array(
        &mut self,
        path: Path,
        metadata: ZarrArrayMetadata,
    ) -> RepositoryResult<()> {
        match self.get_node(&path).await {
            Err(RepositoryError::NodeNotFound { .. }) => {
                let id = NodeId::random();
                self.change_set.add_array(path, id, metadata);
                Ok(())
            }
            Ok(node) => Err(RepositoryError::AlreadyExists {
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
    ) -> RepositoryResult<()> {
        self.get_array(&path)
            .await
            .map(|node| self.change_set.update_array(node.id, metadata))
    }

    /// Delete an array in the hierarchy
    ///
    /// Deletes of non existing array will succeed.
    pub async fn delete_array(&mut self, path: Path) -> RepositoryResult<()> {
        match self.get_array(&path).await {
            Ok(node) => {
                self.change_set.delete_array(node.path, &node.id);
            }
            Err(RepositoryError::NodeNotFound { .. }) => {}
            Err(err) => Err(err)?,
        }
        Ok(())
    }

    /// Record the write or delete of user attributes to array or group
    pub async fn set_user_attributes(
        &mut self,
        path: Path,
        atts: Option<UserAttributes>,
    ) -> RepositoryResult<()> {
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
    ) -> RepositoryResult<()> {
        self.get_array(&path)
            .await
            .map(|node| self.change_set.set_chunk_ref(node.id, coord, data))
    }

    pub async fn get_node(&self, path: &Path) -> RepositoryResult<NodeSnapshot> {
        get_node(self.storage.as_ref(), &self.change_set, self.snapshot_id(), path).await
    }

    pub async fn get_array(&self, path: &Path) -> RepositoryResult<NodeSnapshot> {
        match self.get_node(path).await {
            res @ Ok(NodeSnapshot { node_data: NodeData::Array(..), .. }) => res,
            Ok(node @ NodeSnapshot { .. }) => Err(RepositoryError::NotAnArray {
                node,
                message: "getting an array".to_string(),
            }),
            other => other,
        }
    }

    pub async fn get_group(&self, path: &Path) -> RepositoryResult<NodeSnapshot> {
        match self.get_node(path).await {
            res @ Ok(NodeSnapshot { node_data: NodeData::Group, .. }) => res,
            Ok(node @ NodeSnapshot { .. }) => Err(RepositoryError::NotAGroup {
                node,
                message: "getting a group".to_string(),
            }),
            other => other,
        }
    }

    pub async fn get_chunk_ref(
        &self,
        path: &Path,
        coords: &ChunkIndices,
    ) -> RepositoryResult<Option<ChunkPayload>> {
        let node = self.get_node(path).await?;
        // TODO: it's ugly to have to do this destructuring even if we could be calling `get_array`
        // get_array should return the array data, not a node
        match node.node_data {
            NodeData::Group => Err(RepositoryError::NotAnArray {
                node,
                message: "getting chunk reference".to_string(),
            }),
            NodeData::Array(_, manifests) => {
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
    pub async fn get_chunk_reader(
        &self,
        path: &Path,
        coords: &ChunkIndices,
        byte_range: &ByteRange,
    ) -> RepositoryResult<
        Option<Pin<Box<dyn Future<Output = RepositoryResult<Bytes>> + Send>>>,
    > {
        match self.get_chunk_ref(path, coords).await? {
            Some(ChunkPayload::Ref(ChunkRef { id, .. })) => {
                let storage = Arc::clone(&self.storage);
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
                let resolver = Arc::clone(&self.virtual_resolver);
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
    pub fn get_chunk_writer(
        &self,
    ) -> impl FnOnce(
        Bytes,
    ) -> Pin<
        Box<dyn Future<Output = RepositoryResult<ChunkPayload>> + Send>,
    > {
        let threshold = self.config.inline_chunk_threshold_bytes as usize;
        let storage = Arc::clone(&self.storage);
        move |data: Bytes| {
            async move {
                let payload = if data.len() > threshold {
                    new_materialized_chunk(storage.as_ref(), data).await?
                } else {
                    new_inline_chunk(data)
                };
                Ok(payload)
            }
            .boxed()
        }
    }

    pub async fn clear(&mut self) -> RepositoryResult<()> {
        let to_delete: Vec<(NodeType, Path)> =
            self.list_nodes().await?.map(|node| (node.node_type(), node.path)).collect();

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
    ) -> RepositoryResult<Option<ChunkPayload>> {
        // FIXME: use manifest extents
        for manifest in manifests {
            let manifest_structure =
                self.storage.fetch_manifests(&manifest.object_id).await?;
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

    pub async fn list_nodes(
        &self,
    ) -> RepositoryResult<impl Iterator<Item = NodeSnapshot> + '_> {
        updated_nodes(self.storage.as_ref(), &self.change_set, &self.snapshot_id, None)
            .await
    }

    pub async fn all_chunks(
        &self,
    ) -> RepositoryResult<impl Stream<Item = RepositoryResult<(Path, ChunkInfo)>> + '_>
    {
        all_chunks(self.storage.as_ref(), &self.change_set, self.snapshot_id()).await
    }

    pub async fn distributed_flush<I: IntoIterator<Item = ChangeSet>>(
        &mut self,
        other_change_sets: I,
        message: &str,
        properties: SnapshotProperties,
    ) -> RepositoryResult<SnapshotId> {
        // FIXME: this clone can be avoided
        let change_sets = iter::once(self.change_set.clone()).chain(other_change_sets);
        let new_snapshot_id = distributed_flush(
            self.storage.as_ref(),
            change_sets,
            self.snapshot_id(),
            message,
            properties,
        )
        .await?;

        self.snapshot_id = new_snapshot_id.clone();
        self.change_set = ChangeSet::default();
        Ok(new_snapshot_id)
    }

    /// After changes to the repository have been made, this generates and writes to `Storage` the updated datastructures.
    ///
    /// After calling this, changes are reset and the [`Repository`] can continue to be used for further
    /// changes.
    ///
    /// Returns the `ObjectId` of the new Snapshot file. It's the callers responsibility to commit
    /// this id change.
    pub async fn flush(
        &mut self,
        message: &str,
        properties: SnapshotProperties,
    ) -> RepositoryResult<SnapshotId> {
        self.distributed_flush(iter::empty(), message, properties).await
    }

    pub async fn commit(
        &mut self,
        update_branch_name: &str,
        message: &str,
        properties: Option<SnapshotProperties>,
    ) -> RepositoryResult<SnapshotId> {
        self.distributed_commit(update_branch_name, iter::empty(), message, properties)
            .await
    }

    pub async fn distributed_commit<I: IntoIterator<Item = ChangeSet>>(
        &mut self,
        update_branch_name: &str,
        other_change_sets: I,
        message: &str,
        properties: Option<SnapshotProperties>,
    ) -> RepositoryResult<SnapshotId> {
        let current = fetch_branch_tip(self.storage.as_ref(), update_branch_name).await;
        match current {
            Err(RefError::RefNotFound(_)) => {
                self.do_distributed_commit(
                    update_branch_name,
                    other_change_sets,
                    message,
                    properties,
                )
                .await
            }
            Err(err) => Err(err.into()),
            Ok(ref_data) => {
                // we can detect there will be a conflict before generating the new snapshot
                if ref_data.snapshot != self.snapshot_id {
                    Err(RepositoryError::Conflict {
                        expected_parent: Some(self.snapshot_id.clone()),
                        actual_parent: Some(ref_data.snapshot.clone()),
                    })
                } else {
                    self.do_distributed_commit(
                        update_branch_name,
                        other_change_sets,
                        message,
                        properties,
                    )
                    .await
                }
            }
        }
    }

    async fn do_distributed_commit<I: IntoIterator<Item = ChangeSet>>(
        &mut self,
        update_branch_name: &str,
        other_change_sets: I,
        message: &str,
        properties: Option<SnapshotProperties>,
    ) -> RepositoryResult<SnapshotId> {
        let parent_snapshot = self.snapshot_id.clone();
        let properties = properties.unwrap_or_default();
        let new_snapshot =
            self.distributed_flush(other_change_sets, message, properties).await?;

        match update_branch(
            self.storage.as_ref(),
            update_branch_name,
            new_snapshot.clone(),
            Some(&parent_snapshot),
            self.config.unsafe_overwrite_refs,
        )
        .await
        {
            Ok(_) => Ok(new_snapshot),
            Err(RefError::Conflict { expected_parent, actual_parent }) => {
                Err(RepositoryError::Conflict { expected_parent, actual_parent })
            }
            Err(err) => Err(err.into()),
        }
    }

    pub fn change_set_bytes(&self) -> RepositoryResult<Vec<u8>> {
        self.change_set.export_to_bytes()
    }

    pub async fn new_branch(&self, branch_name: &str) -> RepositoryResult<BranchVersion> {
        // TODO: The parent snapshot should exist?
        let version = match update_branch(
            self.storage.as_ref(),
            branch_name,
            self.snapshot_id.clone(),
            None,
            self.config.unsafe_overwrite_refs,
        )
        .await
        {
            Ok(branch_version) => Ok(branch_version),
            Err(RefError::Conflict { expected_parent, actual_parent }) => {
                Err(RepositoryError::Conflict { expected_parent, actual_parent })
            }
            Err(err) => Err(err.into()),
        }?;

        Ok(version)
    }

    pub async fn tag(
        &self,
        tag_name: &str,
        snapshot_id: &SnapshotId,
    ) -> RepositoryResult<()> {
        create_tag(
            self.storage.as_ref(),
            tag_name,
            snapshot_id.clone(),
            self.config.unsafe_overwrite_refs,
        )
        .await?;
        Ok(())
    }
}

impl From<Repository> for ChangeSet {
    fn from(val: Repository) -> Self {
        val.change_set
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

pub async fn get_chunk(
    reader: Option<Pin<Box<dyn Future<Output = RepositoryResult<Bytes>> + Send>>>,
) -> RepositoryResult<Option<Bytes>> {
    match reader {
        Some(reader) => Ok(Some(reader.await?)),
        None => Ok(None),
    }
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

async fn updated_nodes<'a>(
    storage: &(dyn Storage + Send + Sync),
    change_set: &'a ChangeSet,
    parent_id: &SnapshotId,
    manifest_id: Option<&'a ManifestId>,
) -> RepositoryResult<impl Iterator<Item = NodeSnapshot> + 'a> {
    Ok(updated_existing_nodes(storage, change_set, parent_id, manifest_id)
        .await?
        .chain(change_set.new_nodes_iterator(manifest_id)))
}

async fn get_node<'a>(
    storage: &(dyn Storage + Send + Sync),
    change_set: &'a ChangeSet,
    snapshot_id: &SnapshotId,
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

async fn distributed_flush<I: IntoIterator<Item = ChangeSet>>(
    storage: &(dyn Storage + Send + Sync),
    change_sets: I,
    parent_id: &SnapshotId,
    message: &str,
    properties: SnapshotProperties,
) -> RepositoryResult<SnapshotId> {
    let mut change_set = ChangeSet::default();
    change_set.merge_many(change_sets);
    if change_set.is_empty() {
        return Err(RepositoryError::NoChangesToCommit);
    }

    let chunks = all_chunks(storage, &change_set, parent_id)
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
        updated_nodes(storage, &change_set, parent_id, new_manifest_id.as_ref()).await?;

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
    let new_snapshot_id = &new_snapshot.metadata.id;
    storage.write_snapshot(new_snapshot_id.clone(), Arc::clone(&new_snapshot)).await?;

    Ok(new_snapshot_id.clone())
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
    match get_node(storage, change_set, snapshot_id, path).await {
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

async fn all_chunks<'a>(
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

#[cfg(test)]
#[allow(clippy::panic, clippy::unwrap_used, clippy::expect_used)]
mod tests {

    use std::{error::Error, num::NonZeroU64};

    use crate::{
        format::manifest::ChunkInfo,
        metadata::{
            ChunkKeyEncoding, ChunkShape, Codec, DataType, FillValue, StorageTransformer,
        },
        refs::{fetch_ref, Ref},
        storage::{logging::LoggingStorage, ObjectStorage},
        strategies::*,
    };

    use super::*;
    use itertools::Itertools;
    use pretty_assertions::assert_eq;
    use proptest::prelude::{prop_assert, prop_assert_eq};
    use test_strategy::proptest;
    use tokio::sync::Barrier;

    #[proptest(async = "tokio")]
    async fn test_add_delete_group(
        #[strategy(node_paths())] path: Path,
        #[strategy(empty_repositories())] mut repository: Repository,
    ) {
        // getting any path from an empty repository must fail
        prop_assert!(repository.get_node(&path).await.is_err());

        // adding a new group must succeed
        prop_assert!(repository.add_group(path.clone()).await.is_ok());

        // Getting a group just added must succeed
        let node = repository.get_node(&path).await;
        prop_assert!(node.is_ok());

        // Getting the group twice must be equal
        prop_assert_eq!(node.unwrap(), repository.get_node(&path).await.unwrap());

        // adding an existing group fails
        let matches = matches!(
            repository.add_group(path.clone()).await.unwrap_err(),
            RepositoryError::AlreadyExists{node, ..} if node.path == path
        );
        prop_assert!(matches);

        // deleting the added group must succeed
        prop_assert!(repository.delete_group(path.clone()).await.is_ok());

        // deleting twice must succeed
        prop_assert!(repository.delete_group(path.clone()).await.is_ok());

        // getting a deleted group must fail
        prop_assert!(repository.get_node(&path).await.is_err());

        // adding again must succeed
        prop_assert!(repository.add_group(path.clone()).await.is_ok());

        // deleting again must succeed
        prop_assert!(repository.delete_group(path.clone()).await.is_ok());
    }

    #[proptest(async = "tokio")]
    async fn test_add_delete_array(
        #[strategy(node_paths())] path: Path,
        #[strategy(zarr_array_metadata())] metadata: ZarrArrayMetadata,
        #[strategy(empty_repositories())] mut repository: Repository,
    ) {
        // new array must always succeed
        prop_assert!(repository.add_array(path.clone(), metadata.clone()).await.is_ok());

        // adding to the same path must fail
        prop_assert!(repository.add_array(path.clone(), metadata.clone()).await.is_err());

        // first delete must succeed
        prop_assert!(repository.delete_array(path.clone()).await.is_ok());

        // deleting twice must succeed
        prop_assert!(repository.delete_array(path.clone()).await.is_ok());

        // adding again must succeed
        prop_assert!(repository.add_array(path.clone(), metadata.clone()).await.is_ok());

        // deleting again must succeed
        prop_assert!(repository.delete_array(path.clone()).await.is_ok());
    }

    #[proptest(async = "tokio")]
    async fn test_add_array_group_clash(
        #[strategy(node_paths())] path: Path,
        #[strategy(zarr_array_metadata())] metadata: ZarrArrayMetadata,
        #[strategy(empty_repositories())] mut repository: Repository,
    ) {
        // adding a group at an existing array node must fail
        prop_assert!(repository.add_array(path.clone(), metadata.clone()).await.is_ok());
        let matches = matches!(
            repository.add_group(path.clone()).await.unwrap_err(),
            RepositoryError::AlreadyExists{node, ..} if node.path == path
        );
        prop_assert!(matches);

        let matches = matches!(
            repository.delete_group(path.clone()).await.unwrap_err(),
            RepositoryError::NotAGroup{node, ..} if node.path == path
        );
        prop_assert!(matches);
        prop_assert!(repository.delete_array(path.clone()).await.is_ok());

        // adding an array at an existing group node must fail
        prop_assert!(repository.add_group(path.clone()).await.is_ok());
        let matches = matches!(
            repository.add_array(path.clone(), metadata.clone()).await.unwrap_err(),
            RepositoryError::AlreadyExists{node, ..} if node.path == path
        );
        prop_assert!(matches);
        let matches = matches!(
            repository.delete_array(path.clone()).await.unwrap_err(),
            RepositoryError::NotAnArray{node, ..} if node.path == path
        );
        prop_assert!(matches);
        prop_assert!(repository.delete_group(path.clone()).await.is_ok());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_repository_with_updates() -> Result<(), Box<dyn Error>> {
        let storage = ObjectStorage::new_in_memory_store(Some("prefix".into()));

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
            Arc::new(vec![chunk1.clone(), chunk2.clone()].into_iter().collect());
        let manifest_id = ObjectId::random();
        storage.write_manifests(manifest_id.clone(), Arc::clone(&manifest)).await?;

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
            object_id: manifest_id.clone(),
            extents: ManifestExtents(vec![]),
        };
        let array1_path: Path = "/array1".try_into().unwrap();
        let node_id = NodeId::random();
        let nodes = vec![
            NodeSnapshot {
                path: Path::root(),
                id: node_id,
                user_attributes: None,
                node_data: NodeData::Group,
            },
            NodeSnapshot {
                path: array1_path.clone(),
                id: array_id.clone(),
                user_attributes: Some(UserAttributesSnapshot::Inline(
                    UserAttributes::try_new(br#"{"foo":1}"#).unwrap(),
                )),
                node_data: NodeData::Array(zarr_meta1.clone(), vec![manifest_ref]),
            },
        ];

        let initial = Snapshot::empty();
        let manifests = vec![ManifestFileInfo {
            id: manifest_id.clone(),
            format_version: manifest.icechunk_manifest_format_version,
        }];
        let snapshot = Arc::new(Snapshot::from_iter(
            &initial,
            None,
            manifests,
            vec![],
            nodes.iter().cloned(),
        ));
        let snapshot_id = ObjectId::random();
        storage.write_snapshot(snapshot_id.clone(), snapshot).await?;
        let mut ds = Repository::update(Arc::new(storage), snapshot_id)
            .with_inline_threshold_bytes(512)
            .build();

        // retrieve the old array node
        let node = ds.get_node(&array1_path).await?;
        assert_eq!(nodes.get(1).unwrap(), &node);

        let group_name = "/tbd-group".to_string();
        ds.add_group(group_name.clone().try_into().unwrap()).await?;
        ds.delete_group(group_name.clone().try_into().unwrap()).await?;
        // deleting non-existing is no-op
        assert!(ds.delete_group(group_name.clone().try_into().unwrap()).await.is_ok());
        assert!(ds.get_node(&group_name.try_into().unwrap()).await.is_err());

        // add a new array and retrieve its node
        ds.add_group("/group".try_into().unwrap()).await?;

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

        let new_array_path: Path = "/group/array2".to_string().try_into().unwrap();
        ds.add_array(new_array_path.clone(), zarr_meta2.clone()).await?;

        ds.delete_array(new_array_path.clone()).await?;
        // Delete a non-existent array is no-op
        assert!(ds.delete_array(new_array_path.clone()).await.is_ok());
        assert!(ds.get_node(&new_array_path.clone()).await.is_err());

        ds.add_array(new_array_path.clone(), zarr_meta2.clone()).await?;

        let node = ds.get_node(&new_array_path).await;
        assert!(matches!(
            node.ok(),
            Some(NodeSnapshot {path, user_attributes, node_data,..})
                if path== new_array_path.clone() && user_attributes.is_none() && node_data == NodeData::Array(zarr_meta2.clone(), vec![])
        ));

        // set user attributes for the new array and retrieve them
        ds.set_user_attributes(
            new_array_path.clone(),
            Some(UserAttributes::try_new(br#"{"n":42}"#).unwrap()),
        )
        .await?;
        let node = ds.get_node(&new_array_path).await;
        assert!(matches!(
            node.ok(),
            Some(NodeSnapshot {path, user_attributes, node_data, ..})
                if path == "/group/array2".try_into().unwrap() &&
                    user_attributes ==  Some(UserAttributesSnapshot::Inline(
                        UserAttributes::try_new(br#"{"n":42}"#).unwrap()
                    )) &&
                    node_data == NodeData::Array(zarr_meta2.clone(), vec![])
        ));

        let payload = ds.get_chunk_writer()(Bytes::copy_from_slice(b"foo")).await?;
        ds.set_chunk_ref(new_array_path.clone(), ChunkIndices(vec![0]), Some(payload))
            .await?;

        let chunk = ds.get_chunk_ref(&new_array_path, &ChunkIndices(vec![0])).await?;
        assert_eq!(chunk, Some(ChunkPayload::Inline("foo".into())));

        // retrieve a non initialized chunk of the new array
        let non_chunk = ds.get_chunk_ref(&new_array_path, &ChunkIndices(vec![1])).await?;
        assert_eq!(non_chunk, None);

        // update old array use attributes and check them
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
        let payload = ds.get_chunk_writer()(data.clone()).await?;
        ds.set_chunk_ref(array1_path.clone(), ChunkIndices(vec![0, 0, 0]), Some(payload))
            .await?;

        let chunk = get_chunk(
            ds.get_chunk_reader(
                &array1_path,
                &ChunkIndices(vec![0, 0, 0]),
                &ByteRange::ALL,
            )
            .await
            .unwrap(),
        )
        .await?;
        assert_eq!(chunk, Some(data));

        let path: Path = "/group/array2".try_into().unwrap();
        let node = ds.get_node(&path).await;
        assert!(ds.change_set.has_updated_attributes(&node.as_ref().unwrap().id));
        assert!(ds.delete_array(path.clone()).await.is_ok());
        assert!(!ds.change_set.has_updated_attributes(&node?.id));

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

    #[tokio::test(flavor = "multi_thread")]
    async fn test_repository_with_updates_and_writes() -> Result<(), Box<dyn Error>> {
        let backend: Arc<dyn Storage + Send + Sync> =
            Arc::new(ObjectStorage::new_in_memory_store(Some("prefix".into())));

        let logging = Arc::new(LoggingStorage::new(Arc::clone(&backend)));
        let logging_c: Arc<dyn Storage + Send + Sync> = logging.clone();
        let storage = Repository::add_in_mem_asset_caching(Arc::clone(&logging_c));

        let mut ds = Repository::init(Arc::clone(&storage), false).await?.build();

        // add a new array and retrieve its node
        ds.add_group(Path::root()).await?;
        let snapshot_id = ds.flush("commit", SnapshotProperties::default()).await?;

        //let node_id3 = NodeId::random();
        assert_eq!(snapshot_id, ds.snapshot_id);
        assert!(matches!(
            ds.get_node(&Path::root()).await.ok(),
            Some(NodeSnapshot { path, user_attributes, node_data, .. })
              if path == Path::root() && user_attributes.is_none() && node_data == NodeData::Group
        ));

        ds.add_group("/group".try_into().unwrap()).await?;
        let _snapshot_id = ds.flush("commit", SnapshotProperties::default()).await?;
        assert!(matches!(
            ds.get_node(&Path::root()).await.ok(),
            Some(NodeSnapshot { path, user_attributes, node_data, .. })
              if path == Path::root() && user_attributes.is_none() && node_data == NodeData::Group
        ));

        assert!(matches!(
            ds.get_node(&"/group".try_into().unwrap()).await.ok(),
            Some(NodeSnapshot { path, user_attributes, node_data, .. })
              if path == "/group".try_into().unwrap() && user_attributes.is_none() && node_data == NodeData::Group
        ));

        let zarr_meta = ZarrArrayMetadata {
            shape: vec![1, 1, 2],
            data_type: DataType::Float16,
            chunk_shape: ChunkShape(vec![NonZeroU64::new(2).unwrap()]),
            chunk_key_encoding: ChunkKeyEncoding::Slash,
            fill_value: FillValue::Float16(f32::NEG_INFINITY),
            codecs: vec![Codec { name: "mycodec".to_string(), configuration: None }],
            storage_transformers: Some(vec![StorageTransformer {
                name: "mytransformer".to_string(),
                configuration: None,
            }]),
            dimension_names: Some(vec![Some("t".to_string())]),
        };

        let new_array_path: Path = "/group/array1".try_into().unwrap();
        ds.add_array(new_array_path.clone(), zarr_meta.clone()).await?;

        // wo commit to test the case of a chunkless array
        let _snapshot_id = ds.flush("commit", SnapshotProperties::default()).await?;

        // we set a chunk in a new array
        ds.set_chunk_ref(
            new_array_path.clone(),
            ChunkIndices(vec![0, 0, 0]),
            Some(ChunkPayload::Inline("hello".into())),
        )
        .await?;

        let _snapshot_id = ds.flush("commit", SnapshotProperties::default()).await?;
        assert!(matches!(
            ds.get_node(&Path::root()).await.ok(),
            Some(NodeSnapshot { path, user_attributes, node_data, .. })
              if path == Path::root() && user_attributes.is_none() && node_data == NodeData::Group
        ));
        assert!(matches!(
            ds.get_node(&"/group".try_into().unwrap()).await.ok(),
            Some(NodeSnapshot { path, user_attributes, node_data, .. })
              if path == "/group".try_into().unwrap() && user_attributes.is_none() && node_data == NodeData::Group
        ));
        assert!(matches!(
            ds.get_node(&new_array_path).await.ok(),
            Some(NodeSnapshot {
                path,
                user_attributes: None,
                node_data: NodeData::Array(meta, manifests),
                ..
            }) if path == new_array_path && meta == zarr_meta.clone() && manifests.len() == 1
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

        let previous_snapshot_id =
            ds.flush("commit", SnapshotProperties::default()).await?;
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

        let new_meta = ZarrArrayMetadata { shape: vec![1, 1, 1], ..zarr_meta };
        // we change zarr metadata
        ds.update_array(new_array_path.clone(), new_meta.clone()).await?;

        // we change user attributes metadata
        ds.set_user_attributes(
            new_array_path.clone(),
            Some(UserAttributes::try_new(br#"{"foo":42}"#).unwrap()),
        )
        .await?;

        let snapshot_id = ds.flush("commit", SnapshotProperties::default()).await?;
        let ds = Repository::update(Arc::clone(&storage), snapshot_id).build();

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
                user_attributes: Some(atts),
                node_data: NodeData::Array(meta, manifests),
                ..
            }) if path == new_array_path && meta == new_meta.clone() &&
                    manifests.len() == 1 &&
                    atts == UserAttributesSnapshot::Inline(UserAttributes::try_new(br#"{"foo":42}"#).unwrap())
        ));

        // since we wrote every asset and we are using a caching storage, we should never need to fetch them
        assert!(logging.fetch_operations().is_empty());

        //test the previous version is still alive
        let ds = Repository::update(Arc::clone(&storage), previous_snapshot_id).build();
        assert_eq!(
            ds.get_chunk_ref(&new_array_path, &ChunkIndices(vec![0, 0, 0])).await?,
            Some(ChunkPayload::Inline("bye".into()))
        );
        assert_eq!(
            ds.get_chunk_ref(&new_array_path, &ChunkIndices(vec![0, 0, 1])).await?,
            Some(ChunkPayload::Inline("new chunk".into()))
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_basic_delete_and_flush() -> Result<(), Box<dyn Error>> {
        let storage: Arc<dyn Storage + Send + Sync> =
            Arc::new(ObjectStorage::new_in_memory_store(Some("prefix".into())));
        let mut ds = Repository::init(Arc::clone(&storage), false).await?.build();
        ds.add_group(Path::root()).await?;
        ds.add_group("/1".try_into().unwrap()).await?;
        ds.delete_group("/1".try_into().unwrap()).await?;
        assert_eq!(ds.list_nodes().await?.count(), 1);
        ds.commit("main", "commit", None).await?;
        assert!(ds.get_group(&Path::root()).await.is_ok());
        assert!(ds.get_group(&"/1".try_into().unwrap()).await.is_err());
        assert_eq!(ds.list_nodes().await?.count(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn test_basic_delete_after_flush() -> Result<(), Box<dyn Error>> {
        let storage: Arc<dyn Storage + Send + Sync> =
            Arc::new(ObjectStorage::new_in_memory_store(Some("prefix".into())));
        let mut ds = Repository::init(Arc::clone(&storage), false).await?.build();
        ds.add_group(Path::root()).await?;
        ds.add_group("/1".try_into().unwrap()).await?;
        ds.commit("main", "commit", None).await?;

        ds.delete_group("/1".try_into().unwrap()).await?;
        assert!(ds.get_group(&Path::root()).await.is_ok());
        assert!(ds.get_group(&"/1".try_into().unwrap()).await.is_err());
        assert_eq!(ds.list_nodes().await?.count(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn test_commit_after_deleting_old_node() -> Result<(), Box<dyn Error>> {
        let storage: Arc<dyn Storage + Send + Sync> =
            Arc::new(ObjectStorage::new_in_memory_store(Some("prefix".into())));
        let mut ds = Repository::init(Arc::clone(&storage), false).await?.build();
        ds.add_group(Path::root()).await?;
        ds.commit("main", "commit", None).await?;
        ds.delete_group(Path::root()).await?;
        ds.commit("main", "commit", None).await?;
        assert_eq!(ds.list_nodes().await?.count(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_delete_children() -> Result<(), Box<dyn Error>> {
        let storage: Arc<dyn Storage + Send + Sync> =
            Arc::new(ObjectStorage::new_in_memory_store(Some("prefix".into())));
        let mut ds = Repository::init(Arc::clone(&storage), false).await?.build();
        ds.add_group(Path::root()).await?;
        ds.add_group("/a".try_into().unwrap()).await?;
        ds.add_group("/b".try_into().unwrap()).await?;
        ds.add_group("/b/bb".try_into().unwrap()).await?;
        ds.delete_group("/b".try_into().unwrap()).await?;
        assert!(ds.get_group(&"/b".try_into().unwrap()).await.is_err());
        assert!(ds.get_group(&"/b/bb".try_into().unwrap()).await.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn test_delete_children_of_old_nodes() -> Result<(), Box<dyn Error>> {
        let storage: Arc<dyn Storage + Send + Sync> =
            Arc::new(ObjectStorage::new_in_memory_store(Some("prefix".into())));
        let mut ds = Repository::init(Arc::clone(&storage), false).await?.build();
        ds.add_group(Path::root()).await?;
        ds.add_group("/a".try_into().unwrap()).await?;
        ds.add_group("/b".try_into().unwrap()).await?;
        ds.add_group("/b/bb".try_into().unwrap()).await?;
        ds.commit("main", "commit", None).await?;

        ds.delete_group("/b".try_into().unwrap()).await?;
        assert!(ds.get_group(&"/b".try_into().unwrap()).await.is_err());
        assert!(ds.get_group(&"/b/bb".try_into().unwrap()).await.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn test_manifests_shrink() -> Result<(), Box<dyn Error>> {
        let in_mem_storage =
            Arc::new(ObjectStorage::new_in_memory_store(Some("prefix".into())));
        let storage: Arc<dyn Storage + Send + Sync> = in_mem_storage.clone();
        let mut ds = Repository::init(Arc::clone(&storage), false).await?.build();

        // there should be no manifests yet
        assert!(!in_mem_storage
            .all_keys()
            .await?
            .iter()
            .any(|key| key.contains("manifest")));

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

        ds.add_group(Path::root()).await?;
        let zarr_meta = ZarrArrayMetadata {
            shape: vec![5, 5],
            data_type: DataType::Float16,
            chunk_shape: ChunkShape(vec![NonZeroU64::new(2).unwrap()]),
            chunk_key_encoding: ChunkKeyEncoding::Slash,
            fill_value: FillValue::Float16(f32::NEG_INFINITY),
            codecs: vec![Codec { name: "mycodec".to_string(), configuration: None }],
            storage_transformers: Some(vec![StorageTransformer {
                name: "mytransformer".to_string(),
                configuration: None,
            }]),
            dimension_names: Some(vec![Some("t".to_string())]),
        };

        let a1path: Path = "/array1".try_into()?;
        let a2path: Path = "/array2".try_into()?;

        ds.add_array(a1path.clone(), zarr_meta.clone()).await?;
        ds.add_array(a2path.clone(), zarr_meta.clone()).await?;

        let _ = ds.commit("main", "first commit", None).await?;

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

        // add 3 chunks
        ds.set_chunk_ref(
            a1path.clone(),
            ChunkIndices(vec![0, 0]),
            Some(ChunkPayload::Inline("hello".into())),
        )
        .await?;
        ds.set_chunk_ref(
            a1path.clone(),
            ChunkIndices(vec![0, 1]),
            Some(ChunkPayload::Inline("hello".into())),
        )
        .await?;
        ds.set_chunk_ref(
            a2path.clone(),
            ChunkIndices(vec![0, 1]),
            Some(ChunkPayload::Inline("hello".into())),
        )
        .await?;

        ds.commit("main", "commit", None).await?;

        // there should be one manifest now
        assert_eq!(
            1,
            in_mem_storage
                .all_keys()
                .await?
                .iter()
                .filter(|key| key.contains("manifest"))
                .count()
        );

        let manifest_id = match ds.get_array(&a1path).await?.node_data {
            NodeData::Array(_, manifests) => {
                manifests.first().as_ref().unwrap().object_id.clone()
            }
            NodeData::Group => panic!("must be an array"),
        };
        let manifest = storage.fetch_manifests(&manifest_id).await?;
        let initial_size = manifest.len();

        ds.delete_array(a2path).await?;
        ds.commit("main", "array2 deleted", None).await?;

        // there should be two manifests
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
            NodeData::Array(_, manifests) => {
                manifests.first().as_ref().unwrap().object_id.clone()
            }
            NodeData::Group => panic!("must be an array"),
        };
        let manifest = storage.fetch_manifests(&manifest_id).await?;
        let size_after_delete = manifest.len();

        assert!(size_after_delete < initial_size);

        // delete a chunk
        ds.set_chunk_ref(a1path.clone(), ChunkIndices(vec![0, 0]), None).await?;
        ds.commit("main", "chunk deleted", None).await?;

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
            NodeData::Array(_, manifests) => {
                manifests.first().as_ref().unwrap().object_id.clone()
            }
            NodeData::Group => panic!("must be an array"),
        };
        let manifest = storage.fetch_manifests(&manifest_id).await?;
        let size_after_chunk_delete = manifest.len();
        assert!(size_after_chunk_delete < size_after_delete);

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_all_chunks_iterator() -> Result<(), Box<dyn Error>> {
        let storage: Arc<dyn Storage + Send + Sync> =
            Arc::new(ObjectStorage::new_in_memory_store(Some("prefix".into())));
        let mut ds = Repository::init(Arc::clone(&storage), false).await?.build();

        // add a new array and retrieve its node
        ds.add_group(Path::root()).await?;
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

        let new_array_path: Path = "/array".try_into().unwrap();
        ds.add_array(new_array_path.clone(), zarr_meta.clone()).await?;
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
        let snapshot_id = ds.flush("commit", SnapshotProperties::default()).await?;
        let ds = Repository::update(Arc::clone(&storage), snapshot_id).build();
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
                ChunkIndices(vec![1, 0, 0])
            ]
            .into_iter()
            .collect()
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_commit_and_refs() -> Result<(), Box<dyn Error>> {
        let storage: Arc<dyn Storage + Send + Sync> =
            Arc::new(ObjectStorage::new_in_memory_store(Some("prefix".into())));
        let mut ds = Repository::init(Arc::clone(&storage), false).await?.build();

        // add a new array and retrieve its node
        ds.add_group(Path::root()).await?;
        let new_snapshot_id =
            ds.commit(Ref::DEFAULT_BRANCH, "first commit", None).await?;
        assert_eq!(
            new_snapshot_id,
            fetch_ref(storage.as_ref(), "main").await?.1.snapshot
        );
        assert_eq!(&new_snapshot_id, ds.snapshot_id());

        ds.tag("v1", &new_snapshot_id).await?;
        let (ref_name, ref_data) = fetch_ref(storage.as_ref(), "v1").await?;
        assert_eq!(ref_name, Ref::Tag("v1".to_string()));
        assert_eq!(new_snapshot_id, ref_data.snapshot);

        assert!(matches!(
                ds.get_node(&Path::root()).await.ok(),
                Some(NodeSnapshot { path, user_attributes, node_data, ..})
                    if path == Path::root() && user_attributes.is_none() && node_data == NodeData::Group
        ));

        let mut ds =
            Repository::from_branch_tip(Arc::clone(&storage), "main").await?.build();
        assert!(matches!(
                ds.get_node(&Path::root()).await.ok(),
                Some(NodeSnapshot { path, user_attributes, node_data, ..})
                        if path == Path::root() && user_attributes.is_none() && node_data == NodeData::Group
        ));
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

        let new_array_path: Path = "/array1".try_into().unwrap();
        ds.add_array(new_array_path.clone(), zarr_meta.clone()).await?;
        ds.set_chunk_ref(
            new_array_path.clone(),
            ChunkIndices(vec![0, 0, 0]),
            Some(ChunkPayload::Inline("hello".into())),
        )
        .await?;
        let new_snapshot_id =
            ds.commit(Ref::DEFAULT_BRANCH, "second commit", None).await?;
        let (ref_name, ref_data) =
            fetch_ref(storage.as_ref(), Ref::DEFAULT_BRANCH).await?;
        assert_eq!(ref_name, Ref::Branch("main".to_string()));
        assert_eq!(new_snapshot_id, ref_data.snapshot);

        let parents = ds.ancestry().await?.try_collect::<Vec<_>>().await?;
        assert_eq!(parents[0].message, "second commit");
        assert_eq!(parents[1].message, "first commit");
        assert_eq!(parents[2].message, Snapshot::INITIAL_COMMIT_MESSAGE);
        itertools::assert_equal(
            parents.iter().sorted_by_key(|m| m.written_at).rev(),
            parents.iter(),
        );

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_no_double_commit() -> Result<(), Box<dyn Error>> {
        let storage: Arc<dyn Storage + Send + Sync> =
            Arc::new(ObjectStorage::new_in_memory_store(Some("prefix".into())));
        let _ = Repository::init(Arc::clone(&storage), false).await?;
        let mut ds1 =
            Repository::from_branch_tip(Arc::clone(&storage), "main").await?.build();
        let mut ds2 =
            Repository::from_branch_tip(Arc::clone(&storage), "main").await?.build();

        ds1.add_group("/a".try_into().unwrap()).await?;
        ds2.add_group("/b".try_into().unwrap()).await?;

        let barrier = Arc::new(Barrier::new(2));
        let barrier_c = Arc::clone(&barrier);
        let barrier_cc = Arc::clone(&barrier);
        let handle1 = tokio::spawn(async move {
            let _ = barrier_c.wait().await;
            ds1.commit("main", "from 1", None).await
        });

        let handle2 = tokio::spawn(async move {
            let _ = barrier_cc.wait().await;
            ds2.commit("main", "from 2", None).await
        });

        let res1 = handle1.await.unwrap();
        let res2 = handle2.await.unwrap();

        // We check there is one error and one success, and that the error points to the right
        // conflicting commit
        let ok = match (&res1, &res2) {
            (
                Ok(new_snap),
                Err(RepositoryError::Conflict { expected_parent: _, actual_parent }),
            ) if Some(new_snap) == actual_parent.as_ref() => true,
            (
                Err(RepositoryError::Conflict { expected_parent: _, actual_parent }),
                Ok(new_snap),
            ) if Some(new_snap) == actual_parent.as_ref() => true,
            _ => false,
        };
        assert!(ok);

        let ds = Repository::from_branch_tip(Arc::clone(&storage), "main").await?.build();
        let parents = ds.ancestry().await?.try_collect::<Vec<_>>().await?;
        assert_eq!(parents.len(), 2);
        let msg = parents[0].message.as_str();
        assert!(msg == "from 1" || msg == "from 2");

        assert_eq!(parents[1].message.as_str(), Snapshot::INITIAL_COMMIT_MESSAGE);
        Ok(())
    }

    #[cfg(test)]
    mod state_machine_test {
        use crate::format::snapshot::NodeData;
        use crate::format::Path;
        use crate::ObjectStorage;
        use crate::Repository;
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
        enum RepositoryTransition {
            AddArray(Path, ZarrArrayMetadata),
            UpdateArray(Path, ZarrArrayMetadata),
            DeleteArray(Option<Path>),
            AddGroup(Path),
            DeleteGroup(Option<Path>),
        }

        /// An empty type used for the `ReferenceStateMachine` implementation.
        struct RepositoryStateMachine;

        #[derive(Clone, Default, Debug)]
        struct RepositoryModel {
            arrays: HashMap<Path, ZarrArrayMetadata>,
            groups: Vec<Path>,
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
                        sample::select(state.groups.clone())
                            .prop_map(|p| RepositoryTransition::DeleteGroup(Some(p)))
                            .boxed()
                    } else {
                        Just(RepositoryTransition::DeleteGroup(None)).boxed()
                    }
                };

                prop_oneof![
                    (node_paths(), zarr_array_metadata())
                        .prop_map(|(a, b)| RepositoryTransition::AddArray(a, b)),
                    (node_paths(), zarr_array_metadata())
                        .prop_map(|(a, b)| RepositoryTransition::UpdateArray(a, b)),
                    delete_arrays,
                    node_paths().prop_map(RepositoryTransition::AddGroup),
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
                    RepositoryTransition::AddArray(path, metadata) => {
                        let res = state.arrays.insert(path.clone(), metadata.clone());
                        assert!(res.is_none());
                    }
                    RepositoryTransition::UpdateArray(path, metadata) => {
                        state
                            .arrays
                            .insert(path.clone(), metadata.clone())
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
                    RepositoryTransition::AddGroup(path) => {
                        state.groups.push(path.clone());
                        // TODO: postcondition
                    }
                    RepositoryTransition::DeleteGroup(Some(path)) => {
                        let index =
                            state.groups.iter().position(|x| x == path).expect(
                                "Attempting to delete a non-existent path: {path}",
                            );
                        state.groups.swap_remove(index);
                        state.groups.retain(|group| !group.starts_with(path));
                        state.arrays.retain(|array, _| !array.starts_with(path));
                    }
                    _ => panic!(),
                }
                state
            }

            fn preconditions(state: &Self::State, transition: &Self::Transition) -> bool {
                match transition {
                    RepositoryTransition::AddArray(path, _) => {
                        !state.arrays.contains_key(path) && !state.groups.contains(path)
                    }
                    RepositoryTransition::UpdateArray(path, _) => {
                        state.arrays.contains_key(path)
                    }
                    RepositoryTransition::DeleteArray(path) => path.is_some(),
                    RepositoryTransition::AddGroup(path) => {
                        !state.arrays.contains_key(path) && !state.groups.contains(path)
                    }
                    RepositoryTransition::DeleteGroup(p) => p.is_some(),
                }
            }
        }

        struct TestRepository {
            repository: Repository,
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
                let storage = ObjectStorage::new_in_memory_store(Some("prefix".into()));
                let init_repository =
                    tokio::runtime::Runtime::new().unwrap().block_on(async {
                        let storage = Arc::new(storage);
                        Repository::init(storage, false).await.unwrap()
                    });
                TestRepository {
                    repository: init_repository.build(),
                    runtime: Runtime::new().unwrap(),
                }
            }

            fn apply(
                mut state: Self::SystemUnderTest,
                _ref_state: &<Self::Reference as ReferenceStateMachine>::State,
                transition: RepositoryTransition,
            ) -> Self::SystemUnderTest {
                let runtime = &state.runtime;
                let repository = &mut state.repository;
                match transition {
                    RepositoryTransition::AddArray(path, metadata) => {
                        runtime.unwrap(repository.add_array(path, metadata))
                    }
                    RepositoryTransition::UpdateArray(path, metadata) => {
                        runtime.unwrap(repository.update_array(path, metadata))
                    }
                    RepositoryTransition::DeleteArray(Some(path)) => {
                        runtime.unwrap(repository.delete_array(path))
                    }
                    RepositoryTransition::AddGroup(path) => {
                        runtime.unwrap(repository.add_group(path))
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
                for (path, metadata) in ref_state.arrays.iter() {
                    let node = runtime.unwrap(state.repository.get_array(path));
                    let actual_metadata = match node.node_data {
                        NodeData::Array(metadata, _) => Ok(metadata),
                        _ => Err("foo"),
                    }
                    .unwrap();
                    assert_eq!(metadata, &actual_metadata);
                }

                for path in ref_state.groups.iter() {
                    let node = runtime.unwrap(state.repository.get_group(path));
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
