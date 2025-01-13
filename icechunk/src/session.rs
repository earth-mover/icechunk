use std::{
    cmp::min,
    collections::HashSet,
    future::{ready, Future},
    iter,
    ops::Range,
    pin::Pin,
    sync::Arc,
};

use bytes::Bytes;
use chrono::Utc;
use futures::{FutureExt, Stream, StreamExt, TryStreamExt};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    asset_manager::AssetManager,
    change_set::ChangeSet,
    conflicts::{Conflict, ConflictResolution, ConflictSolver},
    format::{
        format_constants,
        manifest::{
            ChunkInfo, ChunkPayload, ChunkRef, Manifest, ManifestExtents, ManifestRef,
            VirtualChunkRef, VirtualReferenceError,
        },
        snapshot::{
            ManifestFileInfo, NodeData, NodeSnapshot, NodeType, Snapshot,
            SnapshotProperties, UserAttributesSnapshot, ZarrArrayMetadata,
        },
        transaction_log::TransactionLog,
        ByteRange, ChunkIndices, ChunkOffset, IcechunkFormatError, ManifestId, NodeId,
        ObjectId, Path, SnapshotId,
    },
    metadata::UserAttributes,
    refs::{fetch_branch_tip, update_branch, RefError},
    repository::RepositoryError,
    storage,
    virtual_chunks::VirtualChunkResolver,
    RepositoryConfig, Storage, StorageError,
};

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum SessionError {
    #[error("Read only sessions cannot modify the repository")]
    ReadOnlySession,
    #[error("Repository error: {0}")]
    RepositoryError(#[from] RepositoryError),
    #[error("error contacting storage {0}")]
    StorageError(#[from] StorageError),
    #[error("snapshot not found: `{id}`")]
    SnapshotNotFound { id: SnapshotId },
    #[error("error in icechunk file")]
    FormatError(#[from] IcechunkFormatError),
    #[error("no ancestor node was found for `{prefix}`")]
    AncestorNodeNotFound { prefix: Path },
    #[error("node not found at `{path}`: {message}")]
    NodeNotFound { path: Path, message: String },
    #[error("there is not an array at `{node:?}`: {message}")]
    NotAnArray { node: NodeSnapshot, message: String },
    #[error("there is not a group at `{node:?}`: {message}")]
    NotAGroup { node: NodeSnapshot, message: String },
    #[error("node already exists at `{node:?}`: {message}")]
    AlreadyExists { node: NodeSnapshot, message: String },
    #[error("cannot commit, no changes made to the session")]
    NoChangesToCommit,
    #[error("unknown flush error")]
    OtherFlushError,
    #[error("ref error: `{0}`")]
    Ref(#[from] RefError),
    #[error("branch update conflict: `({expected_parent:?}) != ({actual_parent:?})`")]
    Conflict { expected_parent: Option<SnapshotId>, actual_parent: Option<SnapshotId> },
    #[error("cannot rebase snapshot {snapshot} on top of the branch")]
    RebaseFailed { snapshot: SnapshotId, conflicts: Vec<Conflict> },
    #[error("error when handling virtual reference {0}")]
    VirtualReferenceError(#[from] VirtualReferenceError),
    #[error("error in session serialization `{0}`")]
    SerializationError(#[from] rmp_serde::encode::Error),
    #[error("error in session deserialization `{0}`")]
    DeserializationError(#[from] rmp_serde::decode::Error),
    #[error("error finding conflicting path for node `{0}`, this probably indicades a bug in `rebase`")]
    ConflictingPathNotFound(NodeId),
    #[error(
        "invalid chunk index: coordinates {coords:?} are not valid for array at {path}"
    )]
    InvalidIndex { coords: ChunkIndices, path: Path },
}

pub type SessionResult<T> = Result<T, SessionError>;

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
        }
    }

    pub fn create_writable_session(
        config: RepositoryConfig,
        storage_settings: storage::Settings,
        storage: Arc<dyn Storage + Send + Sync>,
        asset_manager: Arc<AssetManager>,
        virtual_resolver: Arc<VirtualChunkResolver>,
        branch_name: String,
        snapshot_id: SnapshotId,
    ) -> Self {
        Self {
            config,
            storage_settings: Arc::new(storage_settings),
            storage,
            asset_manager,
            virtual_resolver,
            branch_name: Some(branch_name),
            snapshot_id,
            change_set: ChangeSet::default(),
        }
    }

    pub fn from_bytes(bytes: Vec<u8>) -> SessionResult<Self> {
        rmp_serde::from_slice(&bytes).map_err(SessionError::DeserializationError)
    }

    pub fn as_bytes(&self) -> SessionResult<Vec<u8>> {
        rmp_serde::to_vec(self).map_err(SessionError::SerializationError)
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

    /// Add a group to the store.
    ///
    /// Calling this only records the operation in memory, doesn't have any consequence on the storage
    pub async fn add_group(&mut self, path: Path) -> SessionResult<()> {
        match self.get_node(&path).await {
            Err(SessionError::NodeNotFound { .. }) => {
                let id = NodeId::random();
                self.change_set.add_group(path.clone(), id);
                Ok(())
            }
            Ok(node) => Err(SessionError::AlreadyExists {
                node,
                message: "trying to add group".to_string(),
            }),
            Err(err) => Err(err),
        }
    }

    pub async fn delete_node(&mut self, node: NodeSnapshot) -> SessionResult<()> {
        match node {
            NodeSnapshot { node_data: NodeData::Group, path: node_path, .. } => {
                Ok(self.delete_group(node_path).await?)
            }
            NodeSnapshot { node_data: NodeData::Array(..), path: node_path, .. } => {
                Ok(self.delete_array(node_path).await?)
            }
        }
    }
    /// Delete a group in the hierarchy
    ///
    /// Deletes of non existing groups will succeed.
    pub async fn delete_group(&mut self, path: Path) -> SessionResult<()> {
        match self.get_group(&path).await {
            Ok(parent) => {
                let nodes_iter: Vec<NodeSnapshot> = self
                    .list_nodes()
                    .await?
                    .filter(|node| node.path.starts_with(&parent.path))
                    .collect();
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
            Err(SessionError::NodeNotFound { .. }) => {}
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
    ) -> SessionResult<()> {
        match self.get_node(&path).await {
            Err(SessionError::NodeNotFound { .. }) => {
                let id = NodeId::random();
                self.change_set.add_array(path, id, metadata);
                Ok(())
            }
            Ok(node) => Err(SessionError::AlreadyExists {
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
    ) -> SessionResult<()> {
        self.get_array(&path)
            .await
            .map(|node| self.change_set.update_array(node.id, metadata))
    }

    /// Delete an array in the hierarchy
    ///
    /// Deletes of non existing array will succeed.
    pub async fn delete_array(&mut self, path: Path) -> SessionResult<()> {
        match self.get_array(&path).await {
            Ok(node) => {
                self.change_set.delete_array(node.path, &node.id);
            }
            Err(SessionError::NodeNotFound { .. }) => {}
            Err(err) => Err(err)?,
        }
        Ok(())
    }

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

    /// Record the write or delete of user attributes to array or group
    pub async fn set_user_attributes(
        &mut self,
        path: Path,
        atts: Option<UserAttributes>,
    ) -> SessionResult<()> {
        let node = self.get_node(&path).await?;
        self.change_set.update_user_attributes(node.id, atts);
        Ok(())
    }

    // Record the write, referencing or delete of a chunk
    //
    // Caller has to write the chunk before calling this.
    pub async fn set_chunk_ref(
        &mut self,
        path: Path,
        coord: ChunkIndices,
        data: Option<ChunkPayload>,
    ) -> SessionResult<()> {
        let node_snapshot = self.get_array(&path).await?;
        self.set_node_chunk_ref(node_snapshot, coord, data).await
    }

    // Helper function that accepts a NodeSnapshot instead of a path,
    // this lets us do bulk sets (and deletes) without repeatedly grabbing the node.
    async fn set_node_chunk_ref(
        &mut self,
        node: NodeSnapshot,
        coord: ChunkIndices,
        data: Option<ChunkPayload>,
    ) -> SessionResult<()> {
        if let NodeData::Array(zarr_metadata, _) = node.node_data {
            if zarr_metadata.valid_chunk_coord(&coord) {
                self.change_set.set_chunk_ref(node.id, coord, data);
                Ok(())
            } else {
                Err(SessionError::InvalidIndex { coords: coord, path: node.path.clone() })
            }
        } else {
            Err(SessionError::NotAnArray {
                node: node.clone(),
                message: "getting an array".to_string(),
            })
        }
    }

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
        Err(SessionError::AncestorNodeNotFound { prefix: path.clone() })
    }

    pub async fn get_node(&self, path: &Path) -> SessionResult<NodeSnapshot> {
        get_node(&self.asset_manager, &self.change_set, self.snapshot_id(), path).await
    }

    pub async fn get_array(&self, path: &Path) -> SessionResult<NodeSnapshot> {
        match self.get_node(path).await {
            res @ Ok(NodeSnapshot { node_data: NodeData::Array(..), .. }) => res,
            Ok(node @ NodeSnapshot { .. }) => Err(SessionError::NotAnArray {
                node,
                message: "getting an array".to_string(),
            }),
            other => other,
        }
    }

    pub async fn get_group(&self, path: &Path) -> SessionResult<NodeSnapshot> {
        match self.get_node(path).await {
            res @ Ok(NodeSnapshot { node_data: NodeData::Group, .. }) => res,
            Ok(node @ NodeSnapshot { .. }) => Err(SessionError::NotAGroup {
                node,
                message: "getting a group".to_string(),
            }),
            other => other,
        }
    }

    pub async fn array_chunk_iterator<'a>(
        &'a self,
        path: &Path,
    ) -> impl Stream<Item = SessionResult<ChunkInfo>> + 'a {
        node_chunk_iterator(
            &self.asset_manager,
            &self.change_set,
            &self.snapshot_id,
            path,
        )
        .await
    }

    pub async fn get_chunk_ref(
        &self,
        path: &Path,
        coords: &ChunkIndices,
    ) -> SessionResult<Option<ChunkPayload>> {
        let node = self.get_node(path).await?;
        // TODO: it's ugly to have to do this destructuring even if we could be calling `get_array`
        // get_array should return the array data, not a node
        match node.node_data {
            NodeData::Group => Err(SessionError::NotAnArray {
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
    pub fn get_chunk_writer(
        &self,
    ) -> impl FnOnce(Bytes) -> Pin<Box<dyn Future<Output = SessionResult<ChunkPayload>> + Send>>
    {
        let threshold = self.config.inline_chunk_threshold_bytes as usize;
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

    pub async fn clear(&mut self) -> SessionResult<()> {
        // TODO: can this be a delete_group("/") instead?
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
    ) -> SessionResult<Option<ChunkPayload>> {
        // FIXME: use manifest extents
        for manifest in manifests {
            let manifest_structure = self.fetch_manifest(&manifest.object_id).await?;
            match manifest_structure.get_chunk_payload(&node, coords) {
                Ok(payload) => {
                    return Ok(Some(payload.clone()));
                }
                Err(IcechunkFormatError::ChunkCoordinatesNotFound { .. }) => {}
                Err(err) => return Err(err.into()),
            }
        }
        Ok(None)
    }

    async fn fetch_manifest(&self, id: &ManifestId) -> SessionResult<Arc<Manifest>> {
        fetch_manifest(id, self.snapshot_id(), self.asset_manager.as_ref()).await
    }

    pub async fn list_nodes(
        &self,
    ) -> SessionResult<impl Iterator<Item = NodeSnapshot> + '_> {
        updated_nodes(&self.asset_manager, &self.change_set, &self.snapshot_id, None)
            .await
    }

    pub async fn all_chunks(
        &self,
    ) -> SessionResult<impl Stream<Item = SessionResult<(Path, ChunkInfo)>> + '_> {
        all_chunks(&self.asset_manager, &self.change_set, self.snapshot_id()).await
    }

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
    pub fn discard_changes(&mut self) -> ChangeSet {
        std::mem::take(&mut self.change_set)
    }

    /// Merge a set of `ChangeSet`s into the repository without committing them
    pub async fn merge(&mut self, changes: ChangeSet) -> SessionResult<()> {
        if self.read_only() {
            return Err(SessionError::ReadOnlySession);
        }
        self.change_set.merge(changes);
        Ok(())
    }

    pub async fn commit(
        &mut self,
        message: &str,
        properties: Option<SnapshotProperties>,
    ) -> SessionResult<SnapshotId> {
        let Some(branch_name) = &self.branch_name else {
            return Err(SessionError::ReadOnlySession);
        };

        let current = fetch_branch_tip(
            self.storage.as_ref(),
            self.storage_settings.as_ref(),
            branch_name,
        )
        .await;

        let id = match current {
            Err(RefError::RefNotFound(_)) => {
                do_commit(
                    &self.config,
                    self.storage.as_ref(),
                    &self.asset_manager,
                    self.storage_settings.as_ref(),
                    branch_name,
                    &self.snapshot_id,
                    &self.change_set,
                    message,
                    properties,
                    self.config.compression.level,
                )
                .await
            }
            Err(err) => Err(err.into()),
            Ok(ref_data) => {
                // we can detect there will be a conflict before generating the new snapshot
                if ref_data.snapshot != self.snapshot_id {
                    Err(SessionError::Conflict {
                        expected_parent: Some(self.snapshot_id.clone()),
                        actual_parent: Some(ref_data.snapshot.clone()),
                    })
                } else {
                    do_commit(
                        &self.config,
                        self.storage.as_ref(),
                        &self.asset_manager,
                        self.storage_settings.as_ref(),
                        branch_name,
                        &self.snapshot_id,
                        &self.change_set,
                        message,
                        properties,
                        self.config.compression.level,
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
    pub async fn rebase(&mut self, solver: &dyn ConflictSolver) -> SessionResult<()> {
        let Some(branch_name) = &self.branch_name else {
            return Err(SessionError::ReadOnlySession);
        };

        let ref_data = fetch_branch_tip(
            self.storage.as_ref(),
            self.storage_settings.as_ref(),
            branch_name,
        )
        .await?;

        if ref_data.snapshot == self.snapshot_id {
            // nothing to do, commit should work without rebasing
            Ok(())
        } else {
            let current_snapshot =
                self.asset_manager.fetch_snapshot(&ref_data.snapshot).await?;
            // FIXME: this should be the whole ancestry not local
            let anc = current_snapshot.local_ancestry().map(|meta| meta.id);
            let new_commits = iter::once(ref_data.snapshot.clone())
                .chain(anc.take_while(|snap_id| snap_id != &self.snapshot_id))
                .collect::<Vec<_>>();

            // TODO: this clone is expensive
            // we currently need it to be able to process commits one by one without modifying the
            // changeset in case of failure
            // let mut changeset = self.change_set.clone();

            // we need to reverse the iterator to process them in order of oldest first
            for snap_id in new_commits.into_iter().rev() {
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
                        self.change_set = patched_changeset;
                        self.snapshot_id = snap_id;
                    }
                    ConflictResolution::Unsolvable { reason, unmodified } => {
                        self.change_set = unmodified;
                        return Err(SessionError::RebaseFailed {
                            snapshot: snap_id,
                            conflicts: reason,
                        });
                    }
                }
            }

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
    let res = nodes.filter_map(move |node| async move {
        // This iterator should yield chunks for existing arrays + any updates.
        // we check for deletion here in the case that `path` exists in the snapshot,
        // and was deleted and then recreated in this changeset.
        if change_set.is_deleted(&node.path, &node.id) {
            None
        } else {
            let path = node.path.clone();
            Some(
                node_chunk_iterator(asset_manager, change_set, snapshot_id, &node.path)
                    .await
                    .map_ok(move |ci| (path.clone(), ci)),
            )
        }
    });
    Ok(res.flatten())
}

/// Warning: The presence of a single error may mean multiple missing items
async fn node_chunk_iterator<'a>(
    asset_manager: &'a AssetManager,
    change_set: &'a ChangeSet,
    snapshot_id: &'a SnapshotId,
    path: &Path,
) -> impl Stream<Item = SessionResult<ChunkInfo>> + 'a {
    match get_node(asset_manager, change_set, snapshot_id, path).await {
        Ok(node) => futures::future::Either::Left(
            verified_node_chunk_iterator(asset_manager, snapshot_id, change_set, node)
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
) -> impl Stream<Item = SessionResult<ChunkInfo>> + 'a {
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
    manifest_id: Option<&'a ManifestId>,
) -> SessionResult<impl Iterator<Item = NodeSnapshot> + 'a> {
    let manifest_refs = manifest_id.map(|mid| {
        vec![ManifestRef { object_id: mid.clone(), extents: ManifestExtents(vec![]) }]
    });
    let updated_nodes =
        asset_manager.fetch_snapshot(parent_id).await?.iter_arc().filter_map(
            move |node| {
                let new_manifests = if node.node_type() == NodeType::Array {
                    //FIXME: it could be none for empty arrays
                    manifest_refs.clone()
                } else {
                    None
                };
                change_set.update_existing_node(node, new_manifests)
            },
        );

    Ok(updated_nodes)
}

/// Yields nodes with the snapshot, applying any relevant updates in the changeset,
/// *and* new nodes in the changeset
async fn updated_nodes<'a>(
    asset_manager: &AssetManager,
    change_set: &'a ChangeSet,
    parent_id: &SnapshotId,
    manifest_id: Option<&'a ManifestId>,
) -> SessionResult<impl Iterator<Item = NodeSnapshot> + 'a> {
    Ok(updated_existing_nodes(asset_manager, change_set, parent_id, manifest_id)
        .await?
        .chain(change_set.new_nodes_iterator(manifest_id)))
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
                Err(SessionError::NodeNotFound {
                    path: path.clone(),
                    message: "getting node".to_string(),
                })
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
        IcechunkFormatError::NodeNotFound { path } => SessionError::NodeNotFound {
            path,
            message: "existing node not found".to_string(),
        },
        err => SessionError::FormatError(err),
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
        .map_err(|_| SessionError::SnapshotNotFound { id: snapshot_id.clone() })?;
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

#[allow(clippy::too_many_arguments)]
async fn flush(
    asset_manager: &AssetManager,
    change_set: &ChangeSet,
    parent_id: &SnapshotId,
    message: &str,
    properties: SnapshotProperties,
    compression_level: u8,
) -> SessionResult<SnapshotId> {
    if change_set.is_empty() {
        return Err(SessionError::NoChangesToCommit);
    }

    let chunks = all_chunks(asset_manager, change_set, parent_id)
        .await?
        .map_ok(|(_path, chunk_info)| chunk_info);

    let new_manifest = Arc::new(Manifest::from_stream(chunks).await?);
    let new_manifest_info =
        asset_manager.write_manifest(new_manifest, compression_level).await?;

    let new_manifest_id = new_manifest_info.as_ref().map(|info| &info.0);
    let all_nodes =
        updated_nodes(asset_manager, change_set, parent_id, new_manifest_id).await?;

    let old_snapshot = asset_manager.fetch_snapshot(parent_id).await?;
    let mut new_snapshot = Snapshot::from_iter(
        old_snapshot.as_ref(),
        Some(properties),
        new_manifest_info
            .as_ref()
            .map(|(mid, msize)| {
                vec![ManifestFileInfo {
                    id: mid.clone(),
                    format_version: format_constants::LATEST_ICECHUNK_SPEC_VERSION_BINARY,
                    size: *msize,
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
    asset_manager.write_snapshot(Arc::clone(&new_snapshot), compression_level).await?;
    asset_manager
        .write_transaction_log(
            new_snapshot_id.clone(),
            Arc::new(tx_log),
            compression_level,
        )
        .await?;

    Ok(new_snapshot_id.clone())
}

#[allow(clippy::too_many_arguments)]
async fn do_commit(
    config: &RepositoryConfig,
    storage: &(dyn Storage + Send + Sync),
    asset_manager: &AssetManager,
    storage_settings: &storage::Settings,
    branch_name: &str,
    snapshot_id: &SnapshotId,
    change_set: &ChangeSet,
    message: &str,
    properties: Option<SnapshotProperties>,
    compression_level: u8,
) -> SessionResult<SnapshotId> {
    let parent_snapshot = snapshot_id.clone();
    let properties = properties.unwrap_or_default();
    let new_snapshot = flush(
        asset_manager,
        change_set,
        snapshot_id,
        message,
        properties,
        compression_level,
    )
    .await?;

    let id = match update_branch(
        storage,
        storage_settings,
        branch_name,
        new_snapshot.clone(),
        Some(&parent_snapshot),
        config.unsafe_overwrite_refs,
    )
    .await
    {
        Ok(_) => Ok(new_snapshot),
        Err(RefError::Conflict { expected_parent, actual_parent }) => {
            Err(SessionError::Conflict { expected_parent, actual_parent })
        }
        Err(err) => Err(err.into()),
    }?;

    Ok(id)
}
async fn fetch_manifest(
    manifest_id: &ManifestId,
    snapshot_id: &SnapshotId,
    asset_manager: &AssetManager,
) -> SessionResult<Arc<Manifest>> {
    let snapshot = asset_manager.fetch_snapshot(snapshot_id).await?;
    let manifest_info = snapshot.manifest_info(manifest_id).ok_or_else(|| {
        IcechunkFormatError::ManifestInfoNotFound { manifest_id: manifest_id.clone() }
    })?;
    Ok(asset_manager.fetch_manifest(manifest_id, manifest_info.size).await?)
}

#[cfg(test)]
#[allow(clippy::panic, clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::{collections::HashMap, error::Error, num::NonZeroU64};

    use crate::{
        conflicts::{
            basic_solver::{BasicConflictSolver, VersionSelection},
            detector::ConflictDetector,
        },
        metadata::{
            ChunkKeyEncoding, ChunkShape, Codec, DataType, FillValue, StorageTransformer,
        },
        refs::{fetch_ref, Ref},
        repository::VersionInfo,
        storage::{logging::LoggingStorage, new_in_memory_storage},
        strategies::{empty_writable_session, node_paths, zarr_array_metadata},
        ObjectStorage, Repository,
    };

    use super::*;
    use itertools::Itertools;
    use pretty_assertions::assert_eq;
    use proptest::prelude::{prop_assert, prop_assert_eq};
    use test_strategy::proptest;
    use tokio::sync::Barrier;

    async fn create_memory_store_repository() -> Repository {
        let storage = new_in_memory_storage().expect("failed to create in-memory store");
        Repository::create(None, storage, HashMap::new()).await.unwrap()
    }

    #[proptest(async = "tokio")]
    async fn test_add_delete_group(
        #[strategy(node_paths())] path: Path,
        #[strategy(empty_writable_session())] mut session: Session,
    ) {
        // getting any path from an empty repository must fail
        prop_assert!(session.get_node(&path).await.is_err());

        // adding a new group must succeed
        prop_assert!(session.add_group(path.clone()).await.is_ok());

        // Getting a group just added must succeed
        let node = session.get_node(&path).await;
        prop_assert!(node.is_ok());

        // Getting the group twice must be equal
        prop_assert_eq!(node.unwrap(), session.get_node(&path).await.unwrap());

        // adding an existing group fails
        let matches = matches!(
            session.add_group(path.clone()).await.unwrap_err(),
            SessionError::AlreadyExists{node, ..} if node.path == path
        );
        prop_assert!(matches);

        // deleting the added group must succeed
        prop_assert!(session.delete_group(path.clone()).await.is_ok());

        // deleting twice must succeed
        prop_assert!(session.delete_group(path.clone()).await.is_ok());

        // getting a deleted group must fail
        prop_assert!(session.get_node(&path).await.is_err());

        // adding again must succeed
        prop_assert!(session.add_group(path.clone()).await.is_ok());

        // deleting again must succeed
        prop_assert!(session.delete_group(path.clone()).await.is_ok());
    }

    #[proptest(async = "tokio")]
    async fn test_add_delete_array(
        #[strategy(node_paths())] path: Path,
        #[strategy(zarr_array_metadata())] metadata: ZarrArrayMetadata,
        #[strategy(empty_writable_session())] mut session: Session,
    ) {
        // new array must always succeed
        prop_assert!(session.add_array(path.clone(), metadata.clone()).await.is_ok());

        // adding to the same path must fail
        prop_assert!(session.add_array(path.clone(), metadata.clone()).await.is_err());

        // first delete must succeed
        prop_assert!(session.delete_array(path.clone()).await.is_ok());

        // deleting twice must succeed
        prop_assert!(session.delete_array(path.clone()).await.is_ok());

        // adding again must succeed
        prop_assert!(session.add_array(path.clone(), metadata.clone()).await.is_ok());

        // deleting again must succeed
        prop_assert!(session.delete_array(path.clone()).await.is_ok());
    }

    #[proptest(async = "tokio")]
    async fn test_add_array_group_clash(
        #[strategy(node_paths())] path: Path,
        #[strategy(zarr_array_metadata())] metadata: ZarrArrayMetadata,
        #[strategy(empty_writable_session())] mut session: Session,
    ) {
        // adding a group at an existing array node must fail
        prop_assert!(session.add_array(path.clone(), metadata.clone()).await.is_ok());
        let matches = matches!(
            session.add_group(path.clone()).await.unwrap_err(),
            SessionError::AlreadyExists{node, ..} if node.path == path
        );
        prop_assert!(matches);

        let matches = matches!(
            session.delete_group(path.clone()).await.unwrap_err(),
            SessionError::NotAGroup{node, ..} if node.path == path
        );
        prop_assert!(matches);
        prop_assert!(session.delete_array(path.clone()).await.is_ok());

        // adding an array at an existing group node must fail
        prop_assert!(session.add_group(path.clone()).await.is_ok());
        let matches = matches!(
            session.add_array(path.clone(), metadata.clone()).await.unwrap_err(),
            SessionError::AlreadyExists{node, ..} if node.path == path
        );
        prop_assert!(matches);
        let matches = matches!(
            session.delete_array(path.clone()).await.unwrap_err(),
            SessionError::NotAnArray{node, ..} if node.path == path
        );
        prop_assert!(matches);
        prop_assert!(session.delete_group(path.clone()).await.is_ok());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_repository_with_updates() -> Result<(), Box<dyn Error>> {
        let storage: Arc<dyn Storage + Send + Sync> = new_in_memory_storage()?;
        let storage_settings = storage.default_settings();
        let asset_manager =
            AssetManager::new_no_cache(Arc::clone(&storage), storage_settings.clone());

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
            Arc::new(Manifest::from_iter(vec![chunk1.clone(), chunk2.clone()]).await?);

        let (manifest_id, manifest_size) =
            asset_manager.write_manifest(Arc::clone(&manifest), 1).await?.unwrap();

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
            size: manifest_size,
        }];
        let snapshot = Arc::new(Snapshot::from_iter(
            &initial,
            None,
            manifests,
            vec![],
            nodes.iter().cloned(),
        ));
        let snapshot_id = asset_manager.write_snapshot(snapshot, 1).await?;
        update_branch(
            storage.as_ref(),
            &storage_settings,
            "main",
            snapshot_id.clone(),
            None,
            true,
        )
        .await?;
        Repository::store_config(storage.as_ref(), &RepositoryConfig::default(), None)
            .await?;

        let repo = Repository::open(None, storage, HashMap::new()).await?;
        let mut ds = repo.writable_session("main").await?;

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

    #[tokio::test(flavor = "multi_thread")]
    async fn test_repository_with_updates_and_writes() -> Result<(), Box<dyn Error>> {
        let backend: Arc<dyn Storage + Send + Sync> = new_in_memory_storage()?;

        let logging = Arc::new(LoggingStorage::new(Arc::clone(&backend)));
        let logging_c: Arc<dyn Storage + Send + Sync> = logging.clone();
        let storage = Arc::clone(&logging_c);

        let repository = Repository::create(None, storage, HashMap::new()).await?;

        let mut ds = repository.writable_session("main").await?;

        // add a new array and retrieve its node
        ds.add_group(Path::root()).await?;
        let snapshot_id =
            ds.commit("commit", Some(SnapshotProperties::default())).await?;

        // We need a new session after the commit
        let mut ds = repository.writable_session("main").await?;

        //let node_id3 = NodeId::random();
        assert_eq!(snapshot_id, ds.snapshot_id);
        assert!(matches!(
            ds.get_node(&Path::root()).await.ok(),
            Some(NodeSnapshot { path, user_attributes, node_data, .. })
              if path == Path::root() && user_attributes.is_none() && node_data == NodeData::Group
        ));

        ds.add_group("/group".try_into().unwrap()).await?;
        let _snapshot_id =
            ds.commit("commit", Some(SnapshotProperties::default())).await?;

        let mut ds = repository.writable_session("main").await?;
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
            chunk_shape: ChunkShape(vec![
                NonZeroU64::new(2).unwrap(),
                NonZeroU64::new(2).unwrap(),
                NonZeroU64::new(1).unwrap(),
            ]),
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
        let _snapshot_id =
            ds.commit("commit", Some(SnapshotProperties::default())).await?;

        let mut ds = repository.writable_session("main").await?;

        let new_new_array_path: Path = "/group/array2".try_into().unwrap();
        ds.add_array(new_new_array_path.clone(), zarr_meta.clone()).await?;

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

        let _snapshot_id =
            ds.commit("commit", Some(SnapshotProperties::default())).await?;

        let mut ds = repository.writable_session("main").await?;
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

        let previous_snapshot_id = ds.commit("commit", None).await?;

        let mut ds = repository.writable_session("main").await?;
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

        let _snapshot_id = ds.commit("commit", None).await?;

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
                user_attributes: Some(atts),
                node_data: NodeData::Array(meta, manifests),
                ..
            }) if path == new_array_path && meta == new_meta.clone() &&
                    manifests.len() == 1 &&
                    atts == UserAttributesSnapshot::Inline(UserAttributes::try_new(br#"{"foo":42}"#).unwrap())
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

        Ok(())
    }

    #[tokio::test]
    async fn test_basic_delete_and_flush() -> Result<(), Box<dyn Error>> {
        let repository = create_memory_store_repository().await;
        let mut ds = repository.writable_session("main").await?;
        ds.add_group(Path::root()).await?;
        ds.add_group("/1".try_into().unwrap()).await?;
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

    #[tokio::test]
    async fn test_basic_delete_after_flush() -> Result<(), Box<dyn Error>> {
        let repository = create_memory_store_repository().await;
        let mut ds = repository.writable_session("main").await?;
        ds.add_group(Path::root()).await?;
        ds.add_group("/1".try_into().unwrap()).await?;
        ds.commit("commit", None).await?;

        let mut ds = repository.writable_session("main").await?;
        ds.delete_group("/1".try_into().unwrap()).await?;
        assert!(ds.get_group(&Path::root()).await.is_ok());
        assert!(ds.get_group(&"/1".try_into().unwrap()).await.is_err());
        assert_eq!(ds.list_nodes().await?.count(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn test_commit_after_deleting_old_node() -> Result<(), Box<dyn Error>> {
        let repository = create_memory_store_repository().await;
        let mut ds = repository.writable_session("main").await?;
        ds.add_group(Path::root()).await?;
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

    #[tokio::test]
    async fn test_delete_children() -> Result<(), Box<dyn Error>> {
        let repository = create_memory_store_repository().await;
        let mut ds = repository.writable_session("main").await?;
        ds.add_group(Path::root()).await?;
        ds.commit("initialize", None).await?;

        let mut ds = repository.writable_session("main").await?;
        ds.add_group("/a".try_into().unwrap()).await?;
        ds.add_group("/b".try_into().unwrap()).await?;
        ds.add_group("/b/bb".try_into().unwrap()).await?;

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

    #[tokio::test]
    async fn test_delete_children_of_old_nodes() -> Result<(), Box<dyn Error>> {
        let repository = create_memory_store_repository().await;
        let mut ds = repository.writable_session("main").await?;
        ds.add_group(Path::root()).await?;
        ds.add_group("/a".try_into().unwrap()).await?;
        ds.add_group("/b".try_into().unwrap()).await?;
        ds.add_group("/b/bb".try_into().unwrap()).await?;
        ds.commit("commit", None).await?;

        let mut ds = repository.writable_session("main").await?;
        ds.delete_group("/b".try_into().unwrap()).await?;
        assert!(ds.get_group(&"/b".try_into().unwrap()).await.is_err());
        assert!(ds.get_group(&"/b/bb".try_into().unwrap()).await.is_err());
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_all_chunks_iterator() -> Result<(), Box<dyn Error>> {
        let storage: Arc<dyn Storage + Send + Sync> = new_in_memory_storage()?;
        let repo = Repository::create(None, storage, HashMap::new()).await?;
        let mut ds = repo.writable_session("main").await?;

        // add a new array and retrieve its node
        ds.add_group(Path::root()).await?;
        let zarr_meta = ZarrArrayMetadata {
            shape: vec![4, 2, 4],
            data_type: DataType::Int32,
            chunk_shape: ChunkShape(vec![
                NonZeroU64::new(2).unwrap(),
                NonZeroU64::new(1).unwrap(),
                NonZeroU64::new(2).unwrap(),
            ]),
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

    #[tokio::test]
    async fn test_manifests_shrink() -> Result<(), Box<dyn Error>> {
        let in_mem_storage = Arc::new(ObjectStorage::new_in_memory()?);
        let storage: Arc<dyn Storage + Send + Sync> = in_mem_storage.clone();
        let repo = Repository::create(None, Arc::clone(&storage), HashMap::new()).await?;

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

        let mut ds = repo.writable_session("main").await?;
        ds.add_group(Path::root()).await?;
        let zarr_meta = ZarrArrayMetadata {
            shape: vec![5, 5],
            data_type: DataType::Float16,
            chunk_shape: ChunkShape(vec![
                NonZeroU64::new(2).unwrap(),
                NonZeroU64::new(2).unwrap(),
            ]),
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
        let manifest =
            repo.asset_manager().fetch_manifest_unknown_size(&manifest_id).await?;
        let initial_size = manifest.len();

        assert_eq!(initial_size, 3);

        let mut ds = repo.writable_session("main").await?;
        ds.delete_array(a2path).await?;
        let _snap_id = ds.commit("array2 deleted", None).await?;

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
        let manifest =
            repo.asset_manager().fetch_manifest_unknown_size(&manifest_id).await?;
        let size_after_delete = manifest.len();

        assert!(size_after_delete < initial_size);

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
            NodeData::Array(_, manifests) => {
                manifests.first().as_ref().unwrap().object_id.clone()
            }
            NodeData::Group => panic!("must be an array"),
        };
        let manifest =
            repo.asset_manager().fetch_manifest_unknown_size(&manifest_id).await?;
        let size_after_chunk_delete = manifest.len();
        assert!(size_after_chunk_delete < size_after_delete);

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_commit_and_refs() -> Result<(), Box<dyn Error>> {
        let repo = create_memory_store_repository().await;
        let storage = Arc::clone(repo.storage());
        let storage_settings = storage.default_settings();
        let mut ds = repo.writable_session("main").await?;

        // add a new array and retrieve its node
        ds.add_group(Path::root()).await?;
        let new_snapshot_id = ds.commit("first commit", None).await?;
        assert_eq!(
            new_snapshot_id,
            fetch_ref(storage.as_ref(), &storage_settings, "main").await?.1.snapshot
        );
        assert_eq!(&new_snapshot_id, ds.snapshot_id());

        repo.create_tag("v1", &new_snapshot_id).await?;
        let (ref_name, ref_data) =
            fetch_ref(storage.as_ref(), &storage_settings, "v1").await?;
        assert_eq!(ref_name, Ref::Tag("v1".to_string()));
        assert_eq!(new_snapshot_id, ref_data.snapshot);

        assert!(matches!(
                ds.get_node(&Path::root()).await.ok(),
                Some(NodeSnapshot { path, user_attributes, node_data, ..})
                    if path == Path::root() && user_attributes.is_none() && node_data == NodeData::Group
        ));

        let mut ds = repo.writable_session("main").await?;

        assert!(matches!(
                ds.get_node(&Path::root()).await.ok(),
                Some(NodeSnapshot { path, user_attributes, node_data, ..})
                        if path == Path::root() && user_attributes.is_none() && node_data == NodeData::Group
        ));
        let zarr_meta = ZarrArrayMetadata {
            shape: vec![1, 1, 2],
            data_type: DataType::Int32,
            chunk_shape: ChunkShape(vec![
                NonZeroU64::new(2).unwrap(),
                NonZeroU64::new(2).unwrap(),
                NonZeroU64::new(2).unwrap(),
            ]),
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
        let new_snapshot_id = ds.commit("second commit", None).await?;
        let (ref_name, ref_data) =
            fetch_ref(storage.as_ref(), &storage_settings, Ref::DEFAULT_BRANCH).await?;
        assert_eq!(ref_name, Ref::Branch("main".to_string()));
        assert_eq!(new_snapshot_id, ref_data.snapshot);

        let parents =
            repo.ancestry(&new_snapshot_id).await?.try_collect::<Vec<_>>().await?;
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
        let repository = create_memory_store_repository().await;

        let mut ds1 = repository.writable_session("main").await?;
        let mut ds2 = repository.writable_session("main").await?;

        ds1.add_group("/a".try_into().unwrap()).await?;
        ds2.add_group("/b".try_into().unwrap()).await?;

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
                Err(SessionError::Conflict { expected_parent: _, actual_parent }),
            ) if Some(new_snap) == actual_parent.as_ref() => true,
            (
                Err(SessionError::Conflict { expected_parent: _, actual_parent }),
                Ok(new_snap),
            ) if Some(new_snap) == actual_parent.as_ref() => true,
            _ => false,
        };
        assert!(ok);

        let ds = repository
            .readonly_session(&VersionInfo::BranchTipRef("main".to_string()))
            .await?;
        let parents =
            repository.ancestry(&ds.snapshot_id).await?.try_collect::<Vec<_>>().await?;
        assert_eq!(parents.len(), 2);
        let msg = parents[0].message.as_str();
        assert!(msg == "from 1" || msg == "from 2");

        assert_eq!(parents[1].message.as_str(), Snapshot::INITIAL_COMMIT_MESSAGE);
        Ok(())
    }

    #[tokio::test]
    async fn test_setting_w_invalid_coords() -> Result<(), Box<dyn Error>> {
        let in_mem_storage = new_in_memory_storage()?;
        let storage: Arc<dyn Storage + Send + Sync> = in_mem_storage.clone();
        let repo = Repository::create(None, Arc::clone(&storage), HashMap::new()).await?;
        let mut ds = repo.writable_session("main").await?;

        ds.add_group(Path::root()).await?;
        let zarr_meta = ZarrArrayMetadata {
            shape: vec![5, 5],
            data_type: DataType::Float16,
            chunk_shape: ChunkShape(vec![
                NonZeroU64::new(2).unwrap(),
                NonZeroU64::new(2).unwrap(),
            ]),
            chunk_key_encoding: ChunkKeyEncoding::Slash,
            fill_value: FillValue::Float16(f32::NEG_INFINITY),
            codecs: vec![Codec { name: "mycodec".to_string(), configuration: None }],
            storage_transformers: None,
            dimension_names: None,
        };

        let apath: Path = "/array1".try_into()?;

        ds.add_array(apath.clone(), zarr_meta.clone()).await?;

        ds.commit("first commit", None).await?;

        // add 3 chunks
        // First 2 chunks are valid, third will be invalid chunk indices

        let mut ds = repo.writable_session("main").await?;

        assert!(ds
            .set_chunk_ref(
                apath.clone(),
                ChunkIndices(vec![0, 0]),
                Some(ChunkPayload::Inline("hello".into())),
            )
            .await
            .is_ok());
        assert!(ds
            .set_chunk_ref(
                apath.clone(),
                ChunkIndices(vec![2, 2]),
                Some(ChunkPayload::Inline("hello".into())),
            )
            .await
            .is_ok());

        let bad_result = ds
            .set_chunk_ref(
                apath.clone(),
                ChunkIndices(vec![3, 0]),
                Some(ChunkPayload::Inline("hello".into())),
            )
            .await;

        match bad_result {
            Err(SessionError::InvalidIndex { coords, path }) => {
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

        ds.add_group("/foo/bar".try_into().unwrap()).await?;
        ds.add_array("/foo/bar/some-array".try_into().unwrap(), basic_meta()).await?;
        ds.commit("create directory", None).await?;

        Ok(repository)
    }
    async fn get_sessions_for_conflict() -> Result<(Session, Session), Box<dyn Error>> {
        let repository = get_repo_for_conflict().await?;

        let ds = repository.writable_session("main").await?;
        let ds2 = repository.writable_session("main").await?;

        Ok((ds, ds2))
    }

    fn basic_meta() -> ZarrArrayMetadata {
        ZarrArrayMetadata {
            shape: vec![5],
            data_type: DataType::Int32,
            chunk_shape: ChunkShape(vec![NonZeroU64::new(1).unwrap()]),
            chunk_key_encoding: ChunkKeyEncoding::Slash,
            fill_value: FillValue::Int32(0),
            codecs: vec![],
            storage_transformers: None,
            dimension_names: None,
        }
    }

    fn assert_has_conflict(conflict: &Conflict, rebase_result: SessionResult<()>) {
        match rebase_result {
            Err(SessionError::RebaseFailed { conflicts, .. }) => {
                assert!(conflicts.contains(conflict));
            }
            other => panic!("test failed, expected conflict, got {:?}", other),
        }
    }

    #[tokio::test()]
    /// Test conflict detection
    ///
    /// This session: add array
    /// Previous commit: add group on same path
    async fn test_conflict_detection_node_conflict_with_existing_node(
    ) -> Result<(), Box<dyn Error>> {
        let (mut ds1, mut ds2) = get_sessions_for_conflict().await?;

        let conflict_path: Path = "/foo/bar/conflict".try_into().unwrap();
        ds1.add_group(conflict_path.clone()).await?;
        ds1.commit("create group", None).await?;

        ds2.add_array(conflict_path.clone(), basic_meta()).await?;
        ds2.commit("create array", None).await.unwrap_err();
        assert_has_conflict(
            &Conflict::NewNodeConflictsWithExistingNode(conflict_path),
            ds2.rebase(&ConflictDetector).await,
        );
        Ok(())
    }

    #[tokio::test()]
    /// Test conflict detection
    ///
    /// This session: add array
    /// Previous commit: add array in implicit path to the session array
    async fn test_conflict_detection_node_conflict_in_path() -> Result<(), Box<dyn Error>>
    {
        let (mut ds1, mut ds2) = get_sessions_for_conflict().await?;

        let conflict_path: Path = "/foo/bar/conflict".try_into().unwrap();
        ds1.add_array(conflict_path.clone(), basic_meta()).await?;
        ds1.commit("create array", None).await?;

        let inner_path: Path = "/foo/bar/conflict/inner".try_into().unwrap();
        ds2.add_array(inner_path.clone(), basic_meta()).await?;
        ds2.commit("create inner array", None).await.unwrap_err();
        assert_has_conflict(
            &Conflict::NewNodeInInvalidGroup(conflict_path),
            ds2.rebase(&ConflictDetector).await,
        );
        Ok(())
    }

    #[tokio::test()]
    /// Test conflict detection
    ///
    /// This session: update array metadata
    /// Previous commit: update array metadata
    async fn test_conflict_detection_double_zarr_metadata_edit(
    ) -> Result<(), Box<dyn Error>> {
        let (mut ds1, mut ds2) = get_sessions_for_conflict().await?;

        let path: Path = "/foo/bar/some-array".try_into().unwrap();
        ds1.update_array(path.clone(), basic_meta()).await?;
        ds1.commit("update array", None).await?;

        ds2.update_array(path.clone(), basic_meta()).await?;
        ds2.commit("update array again", None).await.unwrap_err();
        assert_has_conflict(
            &Conflict::ZarrMetadataDoubleUpdate(path),
            ds2.rebase(&ConflictDetector).await,
        );
        Ok(())
    }

    #[tokio::test()]
    /// Test conflict detection
    ///
    /// This session: delete array
    /// Previous commit: update same array metadata
    async fn test_conflict_detection_metadata_edit_of_deleted(
    ) -> Result<(), Box<dyn Error>> {
        let (mut ds1, mut ds2) = get_sessions_for_conflict().await?;

        let path: Path = "/foo/bar/some-array".try_into().unwrap();
        ds1.delete_array(path.clone()).await?;
        ds1.commit("delete array", None).await?;

        ds2.update_array(path.clone(), basic_meta()).await?;
        ds2.commit("update array again", None).await.unwrap_err();
        assert_has_conflict(
            &Conflict::ZarrMetadataUpdateOfDeletedArray(path),
            ds2.rebase(&ConflictDetector).await,
        );
        Ok(())
    }

    #[tokio::test()]
    /// Test conflict detection
    ///
    /// This session: uptade user attributes
    /// Previous commit: update user attributes
    async fn test_conflict_detection_double_user_atts_edit() -> Result<(), Box<dyn Error>>
    {
        let (mut ds1, mut ds2) = get_sessions_for_conflict().await?;

        let path: Path = "/foo/bar/some-array".try_into().unwrap();
        ds1.set_user_attributes(
            path.clone(),
            Some(UserAttributes::try_new(br#"{"foo":"bar"}"#).unwrap()),
        )
        .await?;
        ds1.commit("update array", None).await?;

        ds2.set_user_attributes(
            path.clone(),
            Some(UserAttributes::try_new(br#"{"foo":"bar"}"#).unwrap()),
        )
        .await?;
        ds2.commit("update array user atts", None).await.unwrap_err();
        let node_id = ds2.get_array(&path).await?.id;
        assert_has_conflict(
            &Conflict::UserAttributesDoubleUpdate { path, node_id },
            ds2.rebase(&ConflictDetector).await,
        );
        Ok(())
    }

    #[tokio::test()]
    /// Test conflict detection
    ///
    /// This session: uptade user attributes
    /// Previous commit: delete same array
    async fn test_conflict_detection_user_atts_edit_of_deleted(
    ) -> Result<(), Box<dyn Error>> {
        let (mut ds1, mut ds2) = get_sessions_for_conflict().await?;

        let path: Path = "/foo/bar/some-array".try_into().unwrap();
        ds1.delete_array(path.clone()).await?;
        ds1.commit("delete array", None).await?;

        ds2.set_user_attributes(
            path.clone(),
            Some(UserAttributes::try_new(br#"{"foo":"bar"}"#).unwrap()),
        )
        .await?;
        ds2.commit("update array user atts", None).await.unwrap_err();
        assert_has_conflict(
            &Conflict::UserAttributesUpdateOfDeletedNode(path),
            ds2.rebase(&ConflictDetector).await,
        );
        Ok(())
    }

    #[tokio::test()]
    /// Test conflict detection
    ///
    /// This session: delete array
    /// Previous commit: update same array metadata
    async fn test_conflict_detection_delete_when_array_metadata_updated(
    ) -> Result<(), Box<dyn Error>> {
        let (mut ds1, mut ds2) = get_sessions_for_conflict().await?;

        let path: Path = "/foo/bar/some-array".try_into().unwrap();
        ds1.update_array(path.clone(), basic_meta()).await?;
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

    #[tokio::test()]
    /// Test conflict detection
    ///
    /// This session: delete array
    /// Previous commit: update same array user attributes
    async fn test_conflict_detection_delete_when_array_user_atts_updated(
    ) -> Result<(), Box<dyn Error>> {
        let (mut ds1, mut ds2) = get_sessions_for_conflict().await?;

        let path: Path = "/foo/bar/some-array".try_into().unwrap();
        ds1.set_user_attributes(
            path.clone(),
            Some(UserAttributes::try_new(br#"{"foo":"bar"}"#).unwrap()),
        )
        .await?;
        ds1.commit("update user attributes", None).await?;

        let node = ds2.get_node(&path).await.unwrap();
        ds2.delete_array(path.clone()).await?;
        ds2.commit("delete array", None).await.unwrap_err();
        assert_has_conflict(
            &Conflict::DeleteOfUpdatedArray { path, node_id: node.id },
            ds2.rebase(&ConflictDetector).await,
        );
        Ok(())
    }

    #[tokio::test()]
    /// Test conflict detection
    ///
    /// This session: delete array
    /// Previous commit: update same array chunks
    async fn test_conflict_detection_delete_when_chunks_updated(
    ) -> Result<(), Box<dyn Error>> {
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

    #[tokio::test()]
    /// Test conflict detection
    ///
    /// This session: delete group
    /// Previous commit: update same group user attributes
    async fn test_conflict_detection_delete_when_group_user_atts_updated(
    ) -> Result<(), Box<dyn Error>> {
        let (mut ds1, mut ds2) = get_sessions_for_conflict().await?;

        let path: Path = "/foo/bar".try_into().unwrap();
        ds1.set_user_attributes(
            path.clone(),
            Some(UserAttributes::try_new(br#"{"foo":"bar"}"#).unwrap()),
        )
        .await?;
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

    #[tokio::test()]
    async fn test_rebase_without_fast_forward() -> Result<(), Box<dyn Error>> {
        let repo = create_memory_store_repository().await;

        let mut ds = repo.writable_session("main").await?;

        ds.add_group("/".try_into().unwrap()).await?;
        let zarr_meta = ZarrArrayMetadata {
            shape: vec![5],
            data_type: DataType::Int32,
            chunk_shape: ChunkShape(vec![NonZeroU64::new(1).unwrap()]),
            chunk_key_encoding: ChunkKeyEncoding::Slash,
            fill_value: FillValue::Int32(0),
            codecs: vec![],
            storage_transformers: None,
            dimension_names: None,
        };

        let new_array_path: Path = "/array".try_into().unwrap();
        ds.add_array(new_array_path.clone(), zarr_meta.clone()).await?;
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
        if let Err(SessionError::Conflict { .. }) =
            ds2.commit("write one chunk with repo2", None).await
        {
            // detect conflicts using rebase
            let result = ds2.rebase(&ConflictDetector).await;
            // assert the conflict is double chunk update
            assert!(matches!(
            result,
            Err(SessionError::RebaseFailed { snapshot, conflicts, })
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

    #[tokio::test()]
    async fn test_rebase_fast_forwarding_over_chunk_writes() -> Result<(), Box<dyn Error>>
    {
        let repo = create_memory_store_repository().await;

        let mut ds = repo.writable_session("main").await?;

        ds.add_group("/".try_into().unwrap()).await?;
        let zarr_meta = ZarrArrayMetadata {
            shape: vec![5],
            data_type: DataType::Int32,
            chunk_shape: ChunkShape(vec![NonZeroU64::new(1).unwrap()]),
            chunk_key_encoding: ChunkKeyEncoding::Slash,
            fill_value: FillValue::Int32(0),
            codecs: vec![],
            storage_transformers: None,
            dimension_names: None,
        };

        let new_array_path: Path = "/array".try_into().unwrap();
        ds.add_array(new_array_path.clone(), zarr_meta.clone()).await?;
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
        ds1.add_array(new_array_2_path.clone(), zarr_meta.clone()).await?;
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
        if let Err(SessionError::Conflict { .. }) =
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

            let commits = repo.ancestry(&snapshot).await?.try_collect::<Vec<_>>().await?;
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
            false,
        )
        .await?;

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

        Ok(())
    }

    #[tokio::test]
    /// Test conflict resolution with rebase
    ///
    /// Two sessions write user attributes to the same array
    /// We attempt to recover using [`VersionSelection::UseOurs`] policy
    async fn test_conflict_resolution_double_user_atts_edit_with_ours(
    ) -> Result<(), Box<dyn Error>> {
        let (mut ds1, mut ds2) = get_sessions_for_conflict().await?;

        let path: Path = "/foo/bar/some-array".try_into().unwrap();
        ds1.set_user_attributes(
            path.clone(),
            Some(UserAttributes::try_new(br#"{"repo":1}"#).unwrap()),
        )
        .await?;
        ds1.commit("update array", None).await?;

        ds2.set_user_attributes(
            path.clone(),
            Some(UserAttributes::try_new(br#"{"repo":2}"#).unwrap()),
        )
        .await?;
        ds2.commit("update array user atts", None).await.unwrap_err();

        let solver = BasicConflictSolver {
            on_user_attributes_conflict: VersionSelection::UseOurs,
            ..Default::default()
        };

        ds2.rebase(&solver).await?;
        ds2.commit("after conflict", None).await?;

        let atts = ds2.get_node(&path).await.unwrap().user_attributes.unwrap();
        assert_eq!(
            atts,
            UserAttributesSnapshot::Inline(
                UserAttributes::try_new(br#"{"repo":2}"#).unwrap()
            )
        );
        Ok(())
    }

    #[tokio::test]
    /// Test conflict resolution with rebase
    ///
    /// Two sessions write user attributes to the same array
    /// We attempt to recover using [`VersionSelection::UseTheirs`] policy
    async fn test_conflict_resolution_double_user_atts_edit_with_theirs(
    ) -> Result<(), Box<dyn Error>> {
        let (mut ds1, mut ds2) = get_sessions_for_conflict().await?;

        let path: Path = "/foo/bar/some-array".try_into().unwrap();
        ds1.set_user_attributes(
            path.clone(),
            Some(UserAttributes::try_new(br#"{"repo":1}"#).unwrap()),
        )
        .await?;
        ds1.commit("update array", None).await?;

        // we made one extra random change to the repo, because we'll undo the user attributes
        // update and we cannot commit an empty change
        ds2.add_group("/baz".try_into().unwrap()).await?;

        ds2.set_user_attributes(
            path.clone(),
            Some(UserAttributes::try_new(br#"{"repo":2}"#).unwrap()),
        )
        .await?;
        ds2.commit("update array user atts", None).await.unwrap_err();

        let solver = BasicConflictSolver {
            on_user_attributes_conflict: VersionSelection::UseTheirs,
            ..Default::default()
        };

        ds2.rebase(&solver).await?;
        ds2.commit("after conflict", None).await?;

        let atts = ds2.get_node(&path).await.unwrap().user_attributes.unwrap();
        assert_eq!(
            atts,
            UserAttributesSnapshot::Inline(
                UserAttributes::try_new(br#"{"repo":1}"#).unwrap()
            )
        );

        ds2.get_node(&"/baz".try_into().unwrap()).await?;
        Ok(())
    }

    #[tokio::test]
    /// Test conflict resolution with rebase
    ///
    /// One session deletes an array, the other updates its metadata.
    /// We attempt to recover using the default [`BasicConflictSolver`]
    /// Array should still be deleted
    async fn test_conflict_resolution_delete_of_updated_array(
    ) -> Result<(), Box<dyn Error>> {
        let (mut ds1, mut ds2) = get_sessions_for_conflict().await?;

        let path: Path = "/foo/bar/some-array".try_into().unwrap();
        ds1.update_array(path.clone(), basic_meta()).await?;
        ds1.commit("update array", None).await?;

        ds2.delete_array(path.clone()).await?;
        ds2.commit("delete array", None).await.unwrap_err();

        ds2.rebase(&BasicConflictSolver::default()).await?;
        ds2.commit("after conflict", None).await?;

        assert!(matches!(
            ds2.get_node(&path).await,
            Err(SessionError::NodeNotFound { .. })
        ));

        Ok(())
    }

    #[tokio::test]
    /// Test conflict resolution with rebase
    ///
    /// Verify we can rebase over multiple commits if they are all fast-forwardable.
    /// We have multiple commits with chunk writes, and then a session has to rebase
    /// writing to the same chunks.
    async fn test_conflict_resolution_success_through_multiple_commits(
    ) -> Result<(), Box<dyn Error>> {
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
            ds1.commit(format!("update chunk {}", coord).as_str(), None).await?;
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

    #[tokio::test]
    /// Rebase over multiple commits with partial failure
    ///
    /// We verify that we can partially fast forward, stopping at the first unrecoverable commit
    async fn test_conflict_resolution_failure_in_multiple_commits(
    ) -> Result<(), Box<dyn Error>> {
        let repo = get_repo_for_conflict().await?;

        let mut ds1 = repo.writable_session("main").await?;
        let mut ds2 = repo.writable_session("main").await?;

        let path: Path = "/foo/bar/some-array".try_into().unwrap();
        ds1.set_user_attributes(
            path.clone(),
            Some(UserAttributes::try_new(br#"{"repo":1}"#).unwrap()),
        )
        .await?;
        let non_conflicting_snap = ds1.commit("update user atts", None).await?;

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
        SessionError::RebaseFailed { snapshot, conflicts}
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

    #[cfg(test)]
    mod state_machine_test {
        use crate::format::snapshot::NodeData;
        use crate::format::Path;
        use futures::Future;
        use proptest::prelude::*;
        use proptest::sample;
        use proptest::strategy::{BoxedStrategy, Just};
        use proptest_state_machine::{
            prop_state_machine, ReferenceStateMachine, StateMachineTest,
        };
        use std::collections::HashMap;
        use std::fmt::Debug;
        use tokio::runtime::Runtime;

        use proptest::test_runner::Config;

        use super::create_memory_store_repository;
        use super::Session;
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
                    let node = runtime.unwrap(state.session.get_array(path));
                    let actual_metadata = match node.node_data {
                        NodeData::Array(metadata, _) => Ok(metadata),
                        _ => Err("foo"),
                    }
                    .unwrap();
                    assert_eq!(metadata, &actual_metadata);
                }

                for path in ref_state.groups.iter() {
                    let node = runtime.unwrap(state.session.get_group(path));
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
