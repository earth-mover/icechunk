use std::{
    collections::HashSet,
    future::{ready, Future},
    iter,
    pin::Pin,
    sync::Arc,
};

use bytes::Bytes;
use chrono::Utc;
use futures::{FutureExt, Stream, StreamExt, TryStreamExt};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    change_set::ChangeSet,
    conflicts::{Conflict, ConflictResolution, ConflictSolver},
    format::{
        manifest::{
            ChunkInfo, ChunkPayload, ChunkRef, Manifest, ManifestExtents, ManifestRef,
            VirtualChunkRef, VirtualReferenceError,
        },
        snapshot::{
            ManifestFileInfo, NodeData, NodeSnapshot, NodeType, Snapshot,
            SnapshotProperties, UserAttributesSnapshot, ZarrArrayMetadata,
        },
        transaction_log::TransactionLog,
        ByteRange, ChunkIndices, IcechunkFormatError, ManifestId, NodeId, Path,
        SnapshotId,
    },
    metadata::UserAttributes,
    refs::{fetch_branch_tip, update_branch, RefError},
    repo::{RepositoryConfig, RepositoryError},
    storage::virtual_ref::{construct_valid_byte_range, VirtualChunkResolver},
    zarr::ObjectId,
    Storage, StorageError,
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
}

pub type SessionResult<T> = Result<T, SessionError>;

#[derive(Serialize, Deserialize)]
pub struct Session {
    config: RepositoryConfig,
    storage: Arc<dyn Storage + Send + Sync>,
    virtual_resolver: Arc<dyn VirtualChunkResolver + Send + Sync>,
    branch_name: Option<String>,
    snapshot_id: SnapshotId,
    change_set: ChangeSet,
}

impl Session {
    pub fn create_readonly_session(
        config: RepositoryConfig,
        storage: Arc<dyn Storage + Send + Sync>,
        virtual_resolver: Arc<dyn VirtualChunkResolver + Send + Sync>,
        snapshot_id: SnapshotId,
    ) -> Self {
        Self {
            config,
            storage,
            virtual_resolver,
            branch_name: None,
            snapshot_id,
            change_set: ChangeSet::default(),
        }
    }

    pub fn create_writable_session(
        config: RepositoryConfig,
        storage: Arc<dyn Storage + Send + Sync>,
        virtual_resolver: Arc<dyn VirtualChunkResolver + Send + Sync>,
        branch_name: String,
        snapshot_id: SnapshotId,
    ) -> Self {
        Self {
            config,
            storage,
            virtual_resolver,
            branch_name: Some(branch_name),
            snapshot_id,
            change_set: ChangeSet::default(),
        }
    }

    pub fn read_only(&self) -> bool {
        self.branch_name.is_none()
    }

    pub async fn get_node(&self, path: &Path) -> SessionResult<NodeSnapshot> {
        get_node(self.storage.as_ref(), &self.snapshot_id, &self.change_set, path).await
    }

    pub async fn get_array(&self, path: &Path) -> SessionResult<NodeSnapshot> {
        get_array(self.storage.as_ref(), &self.snapshot_id, &self.change_set, path).await
    }

    pub async fn get_group(&self, path: &Path) -> SessionResult<NodeSnapshot> {
        get_group(self.storage.as_ref(), &self.snapshot_id, &self.change_set, path).await
    }

    pub async fn get_chunk_ref(
        &self,
        path: &Path,
        coords: &ChunkIndices,
    ) -> SessionResult<Option<ChunkPayload>> {
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
    pub async fn get_chunk_reader(
        &self,
        path: &Path,
        coords: &ChunkIndices,
        byte_range: &ByteRange,
    ) -> SessionResult<Option<Pin<Box<dyn Future<Output = SessionResult<Bytes>> + Send>>>>
    {
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

    pub async fn get_old_chunk(
        &self,
        node: NodeId,
        manifests: &[ManifestRef],
        coords: &ChunkIndices,
    ) -> SessionResult<Option<ChunkPayload>> {
        // FIXME: use manifest extents
        get_old_chunk(self.storage.as_ref(), node, manifests, coords).await
    }

    pub async fn list_nodes(
        &self,
    ) -> SessionResult<impl Iterator<Item = NodeSnapshot> + '_> {
        updated_nodes(self.storage.as_ref(), &self.change_set, &self.snapshot_id, None)
            .await
    }

    pub async fn all_chunks(
        &self,
    ) -> SessionResult<impl Stream<Item = SessionResult<(Path, ChunkInfo)>> + '_> {
        all_chunks(self.storage.as_ref(), &self.change_set, &self.snapshot_id).await
    }

    /// Add a group to the store.
    ///
    /// Calling this only records the operation in memory, doesn't have any consequence on the storage
    pub async fn add_group(&mut self, path: Path) -> SessionResult<()> {
        if self.read_only() {
            return Err(SessionError::ReadOnlySession);
        }

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
        }?;

        Ok(())
    }

    /// Delete a group in the hierarchy
    ///
    /// Deletes of non existing groups will succeed.
    pub async fn delete_group(&mut self, path: Path) -> SessionResult<()> {
        if self.read_only() {
            return Err(SessionError::ReadOnlySession);
        }

        match self.get_group(&path).await {
            Ok(node) => {
                self.change_set.delete_group(node.path, &node.id);
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
        if self.read_only() {
            return Err(SessionError::ReadOnlySession);
        }

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
        }?;

        Ok(())
    }

    // Updates an array Zarr metadata
    ///
    /// Calling this only records the operation in memory, doesn't have any consequence on the storage
    pub async fn update_array(
        &mut self,
        path: Path,
        metadata: ZarrArrayMetadata,
    ) -> SessionResult<()> {
        if self.read_only() {
            return Err(SessionError::ReadOnlySession);
        }

        let node = self.get_array(&path).await?;
        self.change_set.update_array(node.id, metadata);
        Ok(())
    }

    /// Delete an array in the hierarchy
    ///
    /// Deletes of non existing array will succeed.
    pub async fn delete_array(&mut self, path: Path) -> SessionResult<()> {
        if self.read_only() {
            return Err(SessionError::ReadOnlySession);
        }

        match self.get_array(&path).await {
            Ok(node) => {
                self.change_set.delete_array(node.path, &node.id);
            }
            Err(SessionError::NodeNotFound { .. }) => {}
            Err(err) => Err(err)?,
        }
        Ok(())
    }

    /// Record the write or delete of user attributes to array or group
    pub async fn set_user_attributes(
        &mut self,
        path: Path,
        atts: Option<UserAttributes>,
    ) -> SessionResult<()> {
        if self.read_only() {
            return Err(SessionError::ReadOnlySession);
        }

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
    ) -> SessionResult<()> {
        if self.read_only() {
            return Err(SessionError::ReadOnlySession);
        }

        let node = self.get_array(&path).await?;
        self.change_set.set_chunk_ref(node.id, coord, data);

        Ok(())
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
    ) -> SessionResult<
        impl FnOnce(
            Bytes,
        )
            -> Pin<Box<dyn Future<Output = SessionResult<ChunkPayload>> + Send>>,
    > {
        if self.read_only() {
            return Err(SessionError::ReadOnlySession);
        }

        let threshold = self.config.inline_chunk_threshold_bytes as usize;
        let storage = Arc::clone(&self.storage);
        let closure = move |data: Bytes| {
            async move {
                let payload = if data.len() > threshold {
                    new_materialized_chunk(storage.as_ref(), data).await?
                } else {
                    new_inline_chunk(data)
                };
                Ok(payload)
            }
            .boxed()
        };

        Ok(closure)
    }

    pub async fn clear(&mut self) -> SessionResult<()> {
        if self.read_only() {
            return Err(SessionError::ReadOnlySession);
        }

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

    /// Discard all uncommitted changes and return them as a `ChangeSet`
    pub fn discard_changes(&mut self) -> SessionResult<ChangeSet> {
        if self.read_only() {
            return Err(SessionError::ReadOnlySession);
        }

        Ok(std::mem::take(&mut self.change_set))
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
        self,
        message: &str,
        properties: Option<SnapshotProperties>,
    ) -> SessionResult<SnapshotId> {
        let Some(branch_name) = &self.branch_name else {
            return Err(SessionError::ReadOnlySession);
        };

        let current = fetch_branch_tip(self.storage.as_ref(), &branch_name).await;

        let id = match current {
            Err(RefError::RefNotFound(_)) => {
                do_commit(
                    &self.config,
                    self.storage.as_ref(),
                    &branch_name,
                    &self.snapshot_id,
                    &self.change_set,
                    message,
                    properties,
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
                        &branch_name,
                        &self.snapshot_id,
                        &self.change_set,
                        message,
                        properties,
                    )
                    .await
                }
            }
        }?;

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
    pub async fn rebase(
        &mut self,
        solver: &dyn ConflictSolver,
        update_branch_name: &str,
    ) -> SessionResult<()> {
        let ref_data =
            fetch_branch_tip(self.storage.as_ref(), update_branch_name).await?;

        if ref_data.snapshot == self.snapshot_id {
            // nothing to do, commit should work without rebasing
            Ok(())
        } else {
            let current_snapshot =
                self.storage.fetch_snapshot(&ref_data.snapshot).await?;
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
                let tx_log = self.storage.fetch_transaction_log(&snap_id).await?;

                let session = Self::create_readonly_session(
                    self.config.clone(),
                    Arc::clone(&self.storage),
                    Arc::clone(&self.virtual_resolver),
                    self.snapshot_id.clone(),
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

pub async fn updated_nodes<'a>(
    storage: &(dyn Storage + Send + Sync),
    change_set: &'a ChangeSet,
    parent_id: &SnapshotId,
    manifest_id: Option<&'a ManifestId>,
) -> SessionResult<impl Iterator<Item = NodeSnapshot> + 'a> {
    Ok(updated_existing_nodes(storage, change_set, parent_id, manifest_id)
        .await?
        .chain(change_set.new_nodes_iterator(manifest_id)))
}

async fn updated_existing_nodes<'a>(
    storage: &(dyn Storage + Send + Sync),
    change_set: &'a ChangeSet,
    parent_id: &SnapshotId,
    manifest_id: Option<&'a ManifestId>,
) -> SessionResult<impl Iterator<Item = NodeSnapshot> + 'a> {
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
) -> SessionResult<NodeSnapshot> {
    // We need to look for nodes in self.change_set and the snapshot file
    if change_set.is_deleted(path) {
        return Err(SessionError::NodeNotFound {
            path: path.clone(),
            message: "getting node".to_string(),
        });
    }
    match change_set.get_new_node(path) {
        Some(node) => Ok(node),
        None => {
            let node = get_existing_node(storage, change_set, snapshot_id, path).await?;
            if change_set.is_deleted(&node.path) {
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

async fn get_existing_node<'a>(
    storage: &(dyn Storage + Send + Sync),
    change_set: &'a ChangeSet,
    snapshot_id: &SnapshotId,
    path: &Path,
) -> SessionResult<NodeSnapshot> {
    // An existing node is one that is present in a Snapshot file on storage
    let snapshot = storage.fetch_snapshot(snapshot_id).await?;

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

pub async fn all_chunks<'a>(
    storage: &'a (dyn Storage + Send + Sync),
    change_set: &'a ChangeSet,
    snapshot_id: &'a SnapshotId,
) -> SessionResult<impl Stream<Item = SessionResult<(Path, ChunkInfo)>> + 'a> {
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
) -> SessionResult<impl Stream<Item = SessionResult<(Path, ChunkInfo)>> + 'a> {
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
) -> impl Stream<Item = SessionResult<ChunkInfo>> + 'a {
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
                                            SessionError::StorageError(err),
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
) -> SessionResult<ChunkPayload> {
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
) -> SessionResult<SnapshotId> {
    if change_set.is_empty() {
        return Err(SessionError::NoChangesToCommit);
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
) -> SessionResult<Option<ChunkPayload>> {
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
) -> SessionResult<Option<ChunkPayload>> {
    let node = get_node(storage, snapshot_id, change_set, path).await?;
    // TODO: it's ugly to have to do this destructuring even if we could be calling `get_array`
    // get_array should return the array data, not a node
    match node.node_data {
        NodeData::Group => Err(SessionError::NotAnArray {
            node,
            message: "getting chunk reference".to_string(),
        }),
        NodeData::Array(_, manifests) => {
            get_old_chunk(storage, node.id, manifests.as_slice(), coords).await
        }
    }
}

pub async fn get_chunk(
    reader: Option<Pin<Box<dyn Future<Output = SessionResult<Bytes>> + Send>>>,
) -> SessionResult<Option<Bytes>> {
    match reader {
        Some(reader) => Ok(Some(reader.await?)),
        None => Ok(None),
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
) -> SessionResult<Option<Pin<Box<dyn Future<Output = SessionResult<Bytes>> + Send>>>> {
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
) -> SessionResult<NodeSnapshot> {
    match get_node(storage, snapshot_id, change_set, path).await {
        res @ Ok(NodeSnapshot { node_data: NodeData::Array(..), .. }) => res,
        Ok(node @ NodeSnapshot { .. }) => Err(SessionError::NotAnArray {
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
) -> SessionResult<NodeSnapshot> {
    match get_node(storage, snapshot_id, change_set, path).await {
        res @ Ok(NodeSnapshot { node_data: NodeData::Group, .. }) => res,
        Ok(node @ NodeSnapshot { .. }) => {
            Err(SessionError::NotAGroup { node, message: "getting a group".to_string() })
        }
        other => other,
    }
}

async fn do_commit(
    config: &RepositoryConfig,
    storage: &(dyn Storage + Send + Sync),
    branch_name: &str,
    snapshot_id: &SnapshotId,
    change_set: &ChangeSet,
    message: &str,
    properties: Option<SnapshotProperties>,
) -> SessionResult<SnapshotId> {
    let parent_snapshot = snapshot_id.clone();
    let properties = properties.unwrap_or_default();
    let new_snapshot =
        flush(storage, change_set, snapshot_id, message, properties).await?;

    let id = match update_branch(
        storage,
        &branch_name,
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
