//! The transaction context for reading and writing data.
//!
//! A [`Session`] tracks a base snapshot, accumulates changes in a [`ChangeSet`],
//! and handles committing them.
//!
//! Sessions come in three modes:
//! - **Read-only**: Can read data, `ChangeSet` is unused
//! - **Writable**: Can read and write chunks/metadata
//! - **Rearrange**: Can move/rename nodes (no chunk writes)

use async_stream::try_stream;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::{Stream, StreamExt as _, TryStreamExt as _, future::Either, stream};
use itertools::{Itertools as _, enumerate, repeat_n};
use regex::bytes::Regex;
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    future::{Future, ready},
    ops::Range,
    pin::Pin,
    sync::Arc,
};
use thiserror::Error;
use tokio::task::JoinError;
use tracing::{Instrument as _, Span, debug, info, instrument, trace, warn};

use crate::{
    RepositoryConfig, Storage,
    asset_manager::AssetManager,
    change_set::{
        ArrayData, ChangeSet, ChunkTable, MovedFrom, transaction_log_from_change_set,
    },
    config::{
        ManifestConfig, ManifestSplitDim, ManifestSplitDimCondition,
        ManifestSplittingConfig,
    },
    conflicts::{Conflict, ConflictResolution, ConflictSolver},
    diff::{Diff, DiffBuilder},
    error::ICError,
    feature_flags::{MOVE_NODE_FLAG, raise_if_feature_flag_disabled},
    format::{
        ByteRange, ChunkIndices, ChunkOffset, IcechunkFormatError,
        IcechunkFormatErrorKind, ManifestId, NodeId, ObjectId, Path, SnapshotId,
        format_constants::SpecVersionBin,
        manifest::{
            ChunkInfo, ChunkPayload, ChunkRef, LocationCompressionConfig, Manifest,
            ManifestExtents, ManifestRef, ManifestSplits, Overlap, VirtualChunkLocation,
            VirtualChunkRef, VirtualReferenceErrorKind, uniform_manifest_split_edges,
        },
        repo_info::{RepoInfo, UpdateType},
        snapshot::{
            ArrayShape, DimensionName, ManifestFileInfo, NodeData, NodeSnapshot,
            NodeType, Snapshot, SnapshotInfo, SnapshotProperties,
            inject_icechunk_metadata,
        },
        transaction_log::TransactionLog,
    },
    refs::{RefError, RefErrorKind, fetch_branch_tip_v1, update_branch},
    repository::{RepositoryError, RepositoryErrorKind, RepositoryResult},
    storage::{self, StorageErrorKind},
    virtual_chunks::{VirtualChunkContainer, VirtualChunkResolver},
};
use icechunk_types::{ICResultExt as _, error::ICResultCtxExt as _};

/// The mode of a session, determining what operations are allowed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SessionMode {
    /// Cannot modify the repository.
    Readonly,
    /// Can modify arrays/groups but cannot move nodes.
    Writable,
    /// Can only move/rename nodes in the hierarchy.
    Rearrange,
}

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum SessionErrorKind {
    #[error(transparent)]
    RepositoryError(#[from] RepositoryErrorKind),
    #[error(transparent)]
    StorageError(#[from] StorageErrorKind),
    #[error(transparent)]
    FormatError(#[from] IcechunkFormatErrorKind),
    #[error(transparent)]
    VirtualReferenceError(#[from] VirtualReferenceErrorKind),
    #[error(transparent)]
    RefError(#[from] RefErrorKind),

    #[error("Read only sessions cannot modify the repository")]
    ReadOnlySession,
    #[error(
        "commits are not allowed on read-only or forked sessions, merge forked sessions back into the base session before committing. See https://icechunk.io/en/latest/parallel/ for more"
    )]
    CommitNotAllowed,
    #[error(
        "cannot merge a branch session into a fork session, only fork sessions can be merged into a fork. See https://icechunk.io/en/latest/parallel/ for more"
    )]
    MergeNotAllowed,
    #[error(
        "cannot fork a read-only session, read-only sessions can be serialized and transmitted directly. See https://icechunk.io/en/latest/parallel/ for more"
    )]
    CannotForkReadOnlySession,
    #[error(
        "This session was created to rearrange the hierarchy, other write operations cannot be executed. Commit or abandon the sessions and create a regular writable session"
    )]
    RearrangeSessionOnly,
    #[error(
        "To move nodes in the hierarchy you need to create a rearrange session. Commit or abandon this session and create a new rearrange session"
    )]
    NonRearrangeSession,
    #[error("move cannot overwrite existing node at `{0}`")]
    MoveWontOverwrite(String),
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
    #[error(
        "cannot commit, no changes made to the session (use `allow_empty=true` to commit anyway)"
    )]
    NoChangesToCommit,
    #[error("invalid snapshot timestamp ordering. parent: `{parent}`, child: `{child}` ")]
    InvalidSnapshotTimestampOrdering { parent: DateTime<Utc>, child: DateTime<Utc> },
    #[error(
        "snapshot timestamp is invalid, please verify if the machine clock has drifted: local clock: `{snapshot_time}`, object store clock: `{object_store_time}`"
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
    #[error("`to` snapshot ancestry doesn't include `from`")]
    BadSnapshotChainForDiff,
    #[error(
        "the first commit in the repository cannot be an amend, create a new commit instead"
    )]
    NoAmendForInitialCommit,
    #[error("failed to create manifest from chunk stream")]
    ManifestCreationError(#[from] Box<SessionError>),
    #[error(
        "inconsistent manifests detected: Snapshot will reference {snapshot} manifest, while array nodes will reference {nodes} manifests"
    )]
    ManifestsInconsistencyError { snapshot: usize, nodes: usize },
    #[error("failed to merge sessions: {0}")]
    SessionMerge(String),
    #[error("byte range {request:?} is out of bounds for chunk of length {chunk_length}")]
    InvalidByteRange { request: ByteRange, chunk_length: u64 },
    #[error("invalid commit configuration: {reason}")]
    InvalidCommitConfiguration { reason: &'static str },
    #[error("unknown error: {0}")]
    Other(#[from] Box<dyn std::error::Error + Send + Sync>),
}

pub type SessionError = ICError<SessionErrorKind>;
pub type SessionResult<T> = Result<T, SessionError>;

pub enum ReindexMapping<'a> {
    ForwardOnly(Box<dyn Fn(&ChunkIndices) -> ReindexOperationResult + 'a>),
    ForwardBackward {
        forward: Box<dyn Fn(&ChunkIndices) -> ReindexOperationResult + 'a>,
        backward: Box<dyn Fn(&ChunkIndices) -> ReindexOperationResult + 'a>,
    },
}

impl std::fmt::Debug for ReindexMapping<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ForwardOnly(_) => f.debug_tuple("ForwardOnly").finish(),
            Self::ForwardBackward { .. } => f.debug_struct("ForwardBackward").finish(),
        }
    }
}

// Returns the index of split_range that includes ChunkIndices using _linear search_.
// This is used at read time to choose which manifest to query for chunk payload
/// It is useful to have this act on an iterator (e.g. `get_chunk_ref`)
#[inline(always)]
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

pub type RebaseHook =
    Box<dyn Fn(u16) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;

#[derive(Debug, Clone, Copy)]
enum CommitKind {
    NewCommit,
    Flush,
    RewriteManifests,
}

pub struct CommitBuilder<'a> {
    session: &'a mut Session,
    message: String,
    properties: Option<SnapshotProperties>,
    max_concurrent_nodes: usize,
    allow_empty: bool,
    amend: bool,
    kind: CommitKind,
    rebase_solver: Option<&'a (dyn ConflictSolver + Send + Sync)>,
    rebase_attempts: u16,
    before_rebase: Option<RebaseHook>,
    after_rebase: Option<RebaseHook>,
}

impl std::fmt::Debug for CommitBuilder<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CommitBuilder")
            .field("message", &self.message)
            .field("kind", &self.kind)
            .finish_non_exhaustive()
    }
}

impl<'a> CommitBuilder<'a> {
    fn new(session: &'a mut Session, message: String) -> Self {
        Self {
            session,
            message,
            properties: None,
            max_concurrent_nodes: 1,
            allow_empty: false,
            amend: false,
            kind: CommitKind::NewCommit,
            rebase_solver: None,
            rebase_attempts: 0,
            before_rebase: None,
            after_rebase: None,
        }
    }

    pub fn properties(mut self, properties: SnapshotProperties) -> Self {
        self.properties = Some(properties);
        self
    }

    pub fn max_concurrent_nodes(mut self, n: usize) -> Self {
        self.max_concurrent_nodes = n;
        self
    }

    pub fn allow_empty(mut self, allow_empty: bool) -> Self {
        self.allow_empty = allow_empty;
        self
    }

    pub fn amend(mut self) -> Self {
        self.amend = true;
        self
    }

    pub fn anonymous(mut self) -> Self {
        self.kind = CommitKind::Flush;
        self
    }

    pub fn rewrite_manifests(mut self) -> Self {
        self.kind = CommitKind::RewriteManifests;
        self
    }

    pub fn rebase(
        mut self,
        solver: &'a (dyn ConflictSolver + Send + Sync),
        attempts: u16,
    ) -> Self {
        self.rebase_solver = Some(solver);
        self.rebase_attempts = attempts;
        self
    }

    pub fn before_rebase_hook(mut self, hook: RebaseHook) -> Self {
        self.before_rebase = Some(hook);
        self
    }

    pub fn after_rebase_hook(mut self, hook: RebaseHook) -> Self {
        self.after_rebase = Some(hook);
        self
    }

    pub async fn execute(self) -> SessionResult<SnapshotId> {
        let has_rebase = self.rebase_solver.is_some();
        let has_hooks = self.before_rebase.is_some() || self.after_rebase.is_some();

        if matches!(self.kind, CommitKind::Flush) && self.amend {
            return Err(SessionError::capture(
                SessionErrorKind::InvalidCommitConfiguration {
                    reason: "anonymous commits cannot be amended",
                },
            ));
        }
        if matches!(self.kind, CommitKind::Flush) && has_rebase {
            return Err(SessionError::capture(
                SessionErrorKind::InvalidCommitConfiguration {
                    reason: "anonymous commits cannot use rebase",
                },
            ));
        }
        if matches!(self.kind, CommitKind::RewriteManifests) && has_rebase {
            return Err(SessionError::capture(
                SessionErrorKind::InvalidCommitConfiguration {
                    reason: "rewrite_manifests cannot be combined with rebase",
                },
            ));
        }
        if has_hooks && !has_rebase {
            return Err(SessionError::capture(
                SessionErrorKind::InvalidCommitConfiguration {
                    reason: "rebase hooks require .rebase() to be set",
                },
            ));
        }

        if self.amend {
            self.session
                .asset_manager
                .fail_unless_spec_at_least(SpecVersionBin::V2)
                .inject()?;
        }

        let commit_method =
            if self.amend { CommitMethod::Amend } else { CommitMethod::NewCommit };

        match self.kind {
            CommitKind::Flush => {
                self.session
                    .do_flush(&self.message, self.max_concurrent_nodes, self.properties)
                    .await
            }
            CommitKind::RewriteManifests => {
                self.session
                    .do_rewrite_manifests(
                        &self.message,
                        self.max_concurrent_nodes,
                        self.properties,
                        commit_method,
                    )
                    .await
            }
            CommitKind::NewCommit => {
                if let Some(solver) = self.rebase_solver {
                    self.session
                        .do_commit_rebasing(
                            solver,
                            self.rebase_attempts,
                            &self.message,
                            self.max_concurrent_nodes,
                            self.properties,
                            self.allow_empty,
                            self.before_rebase,
                            self.after_rebase,
                        )
                        .await
                } else {
                    self.session
                        .commit_inner(
                            &self.message,
                            self.max_concurrent_nodes,
                            self.properties,
                            false,
                            commit_method,
                            self.allow_empty,
                        )
                        .await
                }
            }
        }
    }
}

pub type ReindexOperationResult = Result<Option<ChunkIndices>, SessionError>;

/// A transaction context for reading and writing Icechunk data.
///
/// Sessions track a base snapshot, accumulate changes in a [`ChangeSet`],
/// and handle committing them.
///
/// Three modes:
/// - **Read-only**: Can read data, `ChangeSet` is unused
/// - **Writable**: Can read and write chunks/metadata
/// - **Rearrange**: Can move/rename nodes (no chunk writes)
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Session {
    config: RepositoryConfig,
    storage_settings: Arc<storage::Settings>,
    storage: Arc<dyn Storage + Send + Sync>,
    asset_manager: Arc<AssetManager>,
    virtual_resolver: Arc<VirtualChunkResolver>,
    read_only: bool,
    branch_name: Option<String>,
    snapshot_id: SnapshotId,
    change_set: ChangeSet,
    default_commit_metadata: SnapshotProperties,
}

impl Session {
    /// Create a read-only session pinned to a specific snapshot.
    ///
    /// The returned session can read chunks and metadata but cannot write or commit.
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
            read_only: true,
            branch_name: None,
            snapshot_id,
            change_set: ChangeSet::for_edits(),
            default_commit_metadata: SnapshotProperties::default(),
        }
    }

    /// Create a writable session for editing chunks and metadata.
    ///
    /// Changes are accumulated in a [`ChangeSet`] and can be committed if the
    /// session is attached to a branch.
    /// Writable sessions can be created on top of anonymous snapshots (when `branch_name` is `None`).
    /// Such sessions can accept all modifications (other than move) but cannot be committed.
    /// They should be merged back with a base writable Session (show `branch_name` is `Some`)
    /// using [`Session::merge`], which can then be committed.
    #[expect(clippy::too_many_arguments)]
    pub fn create_writable_session(
        config: RepositoryConfig,
        storage_settings: storage::Settings,
        storage: Arc<dyn Storage + Send + Sync>,
        asset_manager: Arc<AssetManager>,
        virtual_resolver: Arc<VirtualChunkResolver>,
        branch_name: Option<String>,
        snapshot_id: SnapshotId,
        default_commit_metadata: SnapshotProperties,
    ) -> Self {
        Self {
            config,
            storage_settings: Arc::new(storage_settings),
            storage,
            asset_manager,
            virtual_resolver,
            read_only: false,
            branch_name,
            snapshot_id,
            change_set: ChangeSet::for_edits(),
            default_commit_metadata,
        }
    }

    /// Create a session for rearranging nodes (moves/renames).
    ///
    /// A branch name is required and modifications other than move are disallowed..
    #[expect(clippy::too_many_arguments)]
    pub fn create_rearrange_session(
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
            read_only: false,
            branch_name: Some(branch_name),
            snapshot_id,
            change_set: ChangeSet::for_rearranging(),
            default_commit_metadata,
        }
    }

    #[instrument(skip(bytes))]
    pub fn from_bytes(bytes: &[u8]) -> SessionResult<Self> {
        rmp_serde::from_slice(bytes).capture_box()
    }

    #[instrument(skip(self))]
    pub fn as_bytes(&self) -> SessionResult<Vec<u8>> {
        rmp_serde::to_vec(self).capture_box()
    }

    pub fn branch(&self) -> Option<&str> {
        self.branch_name.as_deref()
    }

    pub fn read_only(&self) -> bool {
        self.read_only
    }

    /// Returns the mode of this session.
    pub fn mode(&self) -> SessionMode {
        if self.branch_name.is_none() {
            SessionMode::Readonly
        } else {
            match &self.change_set {
                ChangeSet::Edit(_) => SessionMode::Writable,
                ChangeSet::Rearrange(_) => SessionMode::Rearrange,
            }
        }
    }

    pub fn snapshot_id(&self) -> &SnapshotId {
        &self.snapshot_id
    }

    pub fn has_uncommitted_changes(&self) -> bool {
        !self.change_set().is_empty()
    }

    pub fn config(&self) -> &RepositoryConfig {
        &self.config
    }

    pub fn matching_container(
        &self,
        chunk_location: &VirtualChunkLocation,
    ) -> Option<&VirtualChunkContainer> {
        self.virtual_resolver.matching_container(chunk_location)
    }

    /// Create a "forked" [`Session`] from a "base" [`Session`]
    /// Fork sessions:
    /// 1. contain an empty [`ChangeSet`]
    /// 2. Are built off an anonymous [`Snapshot`] that records the state of the base [`Session`].
    /// 3. Cannot be committed.
    ///
    /// Such Sessions are useful for distributed writes. You are expected to communicate the forked
    /// [`Session`] using [`Session.as_bytes()`] and [`Session.from_bytes`] methods to distributed workers,
    /// do the necessary writes, communicate back the Sessions from each work, and merge them in to the
    /// base Session, which can then be committed.
    #[instrument(skip(self))]
    pub async fn fork(&self) -> SessionResult<Self> {
        if self.read_only() {
            return Err(SessionError::capture(
                SessionErrorKind::CannotForkReadOnlySession,
            ));
        }
        // TODO: why do we allow Clone?
        let snap = self.clone().commit("fork").anonymous().execute().await?;

        Ok(Session::create_writable_session(
            self.config.clone(),
            (*self.storage_settings).clone(),
            Arc::clone(&self.storage),
            Arc::clone(&self.asset_manager),
            Arc::clone(&self.virtual_resolver),
            None,
            snap.clone(),
            self.default_commit_metadata.clone(),
        ))
    }

    /// Returns true if this is a fork session (writable, not attached to a branch).
    pub fn is_fork(&self) -> bool {
        self.branch_name.is_none() && !self.read_only()
    }

    /// Compute an overview of the current session changes
    pub async fn status(&self) -> SessionResult<Diff> {
        // it doesn't really matter what Id we give to the tx log, it's not going to be persisted
        let tx_log =
            transaction_log_from_change_set(&SnapshotId::random(), self.change_set());
        let from_session = Self::create_readonly_session(
            self.config().clone(),
            self.storage_settings.as_ref().clone(),
            Arc::clone(&self.storage),
            Arc::clone(&self.asset_manager),
            Arc::clone(&self.virtual_resolver),
            self.snapshot_id.clone(),
        );
        let mut builder = DiffBuilder::default();
        builder.add_changes(&tx_log).inject()?;
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
                self.change_set_mut()?.add_group(path.clone(), id, definition)?;
                Ok(())
            }
            Ok(node) => Err(SessionError::capture(SessionErrorKind::AlreadyExists {
                node: Box::new(node),
                message: "trying to add group".to_string(),
            })),
            Err(err) => Err(err),
        }
    }

    #[instrument(skip(self, node))]
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
                    .list_nodes(&path)
                    .await?
                    .filter_ok(|node| node.path.starts_with(&parent.path))
                    .try_collect()?;
                let change_set = self.change_set_mut()?;
                for node in nodes_iter {
                    match node.node_type() {
                        NodeType::Group => {
                            change_set.delete_group(node.path, &node.id)?;
                        }
                        NodeType::Array => {
                            change_set.delete_array(node.path, &node.id)?;
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
                self.change_set_mut()?.add_array(
                    path,
                    id,
                    ArrayData { shape, dimension_names, user_data },
                )?;
                Ok(())
            }
            Ok(node) => Err(SessionError::capture(SessionErrorKind::AlreadyExists {
                node: Box::new(node),
                message: "trying to add array".to_string(),
            })),
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
        match self.get_array(path).await {
            Ok(node) => {
                {
                    // we need to play this trick because we need to borrow from self twice
                    // once to get the mutable change set, and other to compute
                    // and pass splits
                    // This solution first call the function to trigger any
                    // errors, and then it takes the mutable ref again
                    // without referencing self, only the field
                    let _ = self.change_set_mut()?;
                }
                let change_set = &mut self.change_set;
                change_set.update_array(
                    &node.id,
                    path,
                    ArrayData { shape, dimension_names, user_data },
                )?;
                Ok(())
            }
            Err(e) => Err(e),
        }
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
        self.get_group(path).await.and_then(|node| {
            self.change_set_mut()?.update_group(&node.id, path, definition)
        })
    }

    /// Delete an array in the hierarchy
    ///
    /// Deletes of non existing array will succeed.
    #[instrument(skip(self))]
    pub async fn delete_array(&mut self, path: Path) -> SessionResult<()> {
        match self.get_array(&path).await {
            Ok(node) => {
                self.change_set_mut()?.delete_array(node.path, &node.id)?;
            }
            Err(SessionError { kind: SessionErrorKind::NodeNotFound { .. }, .. }) => {}
            Err(err) => Err(err)?,
        }
        Ok(())
    }

    #[instrument(skip(self))]
    pub async fn move_node(&mut self, from: Path, to: Path) -> SessionResult<()> {
        // Icechunk 1 has no way to represent move in its on-disk format
        self.asset_manager.fail_unless_spec_at_least(SpecVersionBin::V2).inject()?;
        // does the source node exist?
        let node = self.get_node(&from).await?;
        // are we overwriting the destination node?
        if (self.get_node(&to).await).is_ok() {
            return Err(SessionError::capture(SessionErrorKind::MoveWontOverwrite(
                to.to_string(),
            )));
        }

        // Get updated subtree
        let subtree_data: Vec<(Path, NodeId, NodeType)> = updated_nodes(
            &from,
            &self.asset_manager,
            &self.change_set,
            self.snapshot_id(),
        )
        .await?
        .filter_map(|r| r.ok().map(|n| (n.path.clone(), n.id.clone(), n.node_type())))
        .collect();

        self.change_set_mut()?.move_node(
            from,
            to,
            subtree_data,
            &node.id,
            node.node_type(),
        )?;
        Ok(())
    }

    #[instrument(skip(self, calculate_new_index))]
    /// Reindex chunks in an array by applying a transformation function to each chunk's coordinates.
    ///
    /// Only existing (non-empty) chunks are visited. The forward function receives each
    /// chunk's current coordinates and returns:
    /// - `Ok(Some(new_coords))` to move the chunk to new coordinates
    /// - `Ok(None)` to skip the chunk (leave it in place)
    /// - `Err(...)` to abort the operation
    ///
    /// With `ForwardOnly`, source positions that are not also destinations retain their
    /// existing chunk refs (stale data). With `ForwardBackward`, the backward function
    /// is used to detect and delete stale positions, ensuring empty chunks shift correctly.
    pub async fn reindex_array<'a>(
        &mut self,
        array_path: &Path,
        calculate_new_index: ReindexMapping<'a>,
    ) -> SessionResult<()> {
        let node = self.get_array(array_path).await?;
        #[expect(clippy::panic)]
        let NodeData::Array { shape, .. } = node.node_data else {
            // we know it's an array because get_array succeeded
            panic!("bug in reindex")
        };

        let mut original_chunks = self.chunk_coordinates(array_path).await?.boxed();
        let mut change_set = ChangeSet::for_edits();
        let mut destinations = HashSet::new();

        let (forward, backwards) = match calculate_new_index {
            ReindexMapping::ForwardOnly(forward) => (forward, None),
            ReindexMapping::ForwardBackward { forward, backward } => {
                (forward, Some(backward))
            }
        };
        // TODO: concurrency
        while let Some(old_chunk_index) = original_chunks.try_next().await? {
            // Skip out-of-bounds indices (e.g. ghost deletes from a prior resize)
            if !shape.valid_chunk_coord(&old_chunk_index) {
                continue;
            }
            if let Some(new_chunk_index) = forward(&old_chunk_index)? {
                let new_payload =
                    self.get_chunk_ref(array_path, &old_chunk_index).await?;
                if shape.valid_chunk_coord(&new_chunk_index) {
                    destinations.insert(new_chunk_index.clone());
                    change_set.set_chunk_ref(
                        node.id.clone(),
                        new_chunk_index,
                        new_payload,
                    )?;
                } else {
                    return Err(SessionError::capture(SessionErrorKind::InvalidIndex {
                        coords: new_chunk_index,
                        path: node.path.clone(),
                    }));
                }
            }
            if let Some(ref backwards) = backwards {
                if destinations.contains(&old_chunk_index) {
                    continue;
                }
                let should_delete = match backwards(&old_chunk_index)? {
                    None => true, // out of bounds
                    Some(source) => {
                        self.get_chunk_ref(array_path, &source).await?.is_none()
                    } // empty source chunk or not?
                };
                if should_delete {
                    change_set.set_chunk_ref(node.id.clone(), old_chunk_index, None)?;
                }
            }
        }
        drop(original_chunks);
        self.change_set_mut()?.merge(change_set)?;

        Ok(())
    }

    /// Shift all chunks in an array by the given chunk offset.
    ///
    /// Out-of-bounds chunks are discarded. To preserve them, resize the array first
    /// to make room. Vacated source positions are cleared (reset to fill value).
    #[instrument(skip(self))]
    pub async fn shift_array(
        &mut self,
        array_path: &Path,
        chunk_offset: &[i64],
    ) -> SessionResult<()> {
        let node = self.get_array(array_path).await?;
        let num_chunks: Vec<u32> = match &node.node_data {
            NodeData::Array { shape, .. } => shape.num_chunks().collect(),
            _ => unreachable!("get_array returned non-array"),
        };

        fn make_shift_closure(
            chunk_offset: &[i64],
            num_chunks: &[u32],
        ) -> Box<dyn Fn(&ChunkIndices) -> ReindexOperationResult> {
            let chunk_offset = chunk_offset.to_vec();

            let num_chunks = num_chunks.to_vec();
            Box::new(move |index: &ChunkIndices| {
                let new_indices: Option<Vec<u32>> = index
                    .0
                    .iter()
                    .enumerate()
                    .map(|(dim, &idx)| {
                        let n = num_chunks[dim] as i64;
                        // On overflow, checked_add returns None, which ? propagates
                        // to the map closure, causing .collect() to yield None and
                        // discarding the chunk
                        // So, we are setting the semantics of this to
                        // automatically discard out of bounds chunks
                        let new_idx = (idx as i64).checked_add(chunk_offset[dim])?;
                        if new_idx < 0 || new_idx >= n {
                            None
                        } else {
                            Some(new_idx as u32)
                        }
                    })
                    .collect();
                Ok(new_indices.map(ChunkIndices))
            })
        }
        let negated_offset: Vec<i64> = chunk_offset.iter().map(|&o| -o).collect();

        self.reindex_array(
            array_path,
            ReindexMapping::ForwardBackward {
                forward: make_shift_closure(chunk_offset, &num_chunks),
                backward: make_shift_closure(&negated_offset, &num_chunks),
            },
        )
        .await
    }

    #[instrument(skip(self, coords))]
    pub async fn delete_chunks(
        &mut self,
        node_path: &Path,
        coords: impl IntoIterator<Item = ChunkIndices>,
    ) -> SessionResult<()> {
        let node = self.get_array(node_path).await?;
        for coord in coords {
            self.set_node_chunk_ref(node.clone(), coord, None).await?;
        }
        Ok(())
    }

    // Record the write, referencing or delete of a chunk
    //
    // Caller has to write the chunk before calling this.
    #[instrument(skip(self, data))]
    pub async fn set_chunk_ref(
        &mut self,
        path: Path,
        coord: ChunkIndices,
        data: Option<ChunkPayload>,
    ) -> SessionResult<()> {
        let node_snapshot = self.get_array(&path).await?;
        self.set_node_chunk_ref(node_snapshot, coord, data).await
    }

    fn change_set(&self) -> &ChangeSet {
        &self.change_set
    }

    fn change_set_mut(&mut self) -> SessionResult<&mut ChangeSet> {
        if self.read_only() {
            Err(SessionError::capture(SessionErrorKind::ReadOnlySession))
        } else {
            Ok(&mut self.change_set)
        }
    }

    fn get_splits(
        &mut self,
        path: &Path,
        shape: &ArrayShape,
        dimension_names: &Option<Vec<DimensionName>>,
    ) -> ManifestSplits {
        self.config.manifest().splitting().get_split_sizes(path, shape, dimension_names)
    }

    // Helper function that accepts a NodeSnapshot instead of a path,
    // this lets us do bulk sets (and deletes) without repeatedly grabbing the node.
    async fn set_node_chunk_ref(
        &mut self,
        node: NodeSnapshot,
        coord: ChunkIndices,
        data: Option<ChunkPayload>,
    ) -> SessionResult<()> {
        if let NodeData::Array { shape, .. } = node.node_data {
            if shape.valid_chunk_coord(&coord) {
                self.change_set_mut()?.set_chunk_ref(node.id, coord, data)?;
                Ok(())
            } else {
                Err(SessionError::capture(SessionErrorKind::InvalidIndex {
                    coords: coord,
                    path: node.path.clone(),
                }))
            }
        } else {
            Err(SessionError::capture(SessionErrorKind::NotAnArray {
                node: Box::new(node.clone()),
                message: "getting an array".to_string(),
            }))
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
        Err(SessionError::capture(SessionErrorKind::AncestorNodeNotFound {
            prefix: path.clone(),
        }))
    }

    #[instrument(skip(self))]
    pub async fn get_node(&self, path: &Path) -> SessionResult<NodeSnapshot> {
        get_node(&self.asset_manager, self.change_set(), self.snapshot_id(), path).await
    }

    pub async fn get_array(&self, path: &Path) -> SessionResult<NodeSnapshot> {
        match self.get_node(path).await {
            res @ Ok(NodeSnapshot { node_data: NodeData::Array { .. }, .. }) => res,
            Ok(node @ NodeSnapshot { .. }) => {
                Err(SessionError::capture(SessionErrorKind::NotAnArray {
                    node: Box::new(node),
                    message: "getting an array".to_string(),
                }))
            }
            other => other,
        }
    }

    pub async fn get_group(&self, path: &Path) -> SessionResult<NodeSnapshot> {
        match self.get_node(path).await {
            res @ Ok(NodeSnapshot { node_data: NodeData::Group, .. }) => res,
            Ok(node @ NodeSnapshot { .. }) => {
                Err(SessionError::capture(SessionErrorKind::NotAGroup {
                    node: Box::new(node),
                    message: "getting a group".to_string(),
                }))
            }
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
            self.change_set(),
            &self.snapshot_id,
            path,
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
            NodeData::Group => Err(SessionError::capture(SessionErrorKind::NotAnArray {
                node: Box::new(node),
                message: "getting chunk reference".to_string(),
            })),
            NodeData::Array { shape, manifests, .. } => {
                if !shape.valid_chunk_coord(coords) {
                    // this chunk ref cannot exist
                    return Ok(None);
                }

                // check the chunks modified in this session first
                // TODO: I hate rust forces me to clone to search in a hashmap. How to do better?
                let session_chunk =
                    self.change_set().get_chunk_ref(&node.id, coords).cloned();

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
    /// [`crate::Repository`], that way, writes can happen concurrently.
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
    pub async fn get_chunk_reader(
        &self,
        path: &Path,
        coords: &ChunkIndices,
        byte_range: &ByteRange,
    ) -> SessionResult<Option<crate::compat::IcechunkBoxFuture<'_, SessionResult<Bytes>>>>
    {
        match self.get_chunk_ref(path, coords).await? {
            Some(ChunkPayload::Ref(ChunkRef { id, offset, length })) => {
                let byte_range = byte_range.clone();
                let asset_manager = Arc::clone(&self.asset_manager);
                let byte_range = construct_valid_byte_range(&byte_range, offset, length)?;
                Ok(Some(crate::compat::ic_boxed!(async move {
                    // TODO: we don't have a way to distinguish if we want to pass a range or not
                    asset_manager.fetch_chunk(&id, &byte_range).await.inject()
                })))
            }
            Some(ChunkPayload::Inline(bytes)) => {
                let byte_range =
                    construct_valid_byte_range(byte_range, 0, bytes.len() as u64)?;
                trace!("fetching inline chunk for range {:?}.", &byte_range);
                Ok(Some(crate::compat::ic_boxed!(ready(Ok(
                    bytes.slice(byte_range.start as usize..byte_range.end as usize)
                )))))
            }
            Some(ChunkPayload::Virtual(VirtualChunkRef {
                location,
                offset,
                length,
                checksum,
            })) => {
                let byte_range = construct_valid_byte_range(byte_range, offset, length)?;
                let resolver = Arc::clone(&self.virtual_resolver);
                Ok(Some(crate::compat::ic_boxed!(async move {
                    resolver
                        .fetch_chunk(location.url(), &byte_range, checksum.as_ref())
                        .await
                        .inject()
                })))
            }
            Some(_) => Ok(None),
            None => Ok(None),
        }
    }

    /// Returns a function that can be used to asynchronously write chunk bytes to object store
    ///
    /// The reason to use this design, instead of simple pass the [`Bytes`] is to avoid holding a
    /// reference to the repository while the payload is uploaded to object store. This way, the
    /// reference is hold very briefly, and then an owned object is obtained which can do the actual
    /// upload without holding any [`crate::Repository`] references.
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
    ) -> SessionResult<
        impl FnOnce(
            Bytes,
        )
            -> crate::compat::IcechunkBoxFuture<'static, SessionResult<ChunkPayload>>
        + use<>,
    > {
        let threshold = self.config().inline_chunk_threshold_bytes() as usize;
        let asset_manager = Arc::clone(&self.asset_manager);
        let fut = move |data: Bytes| {
            crate::compat::ic_boxed!(async move {
                let payload = if data.len() > threshold {
                    new_materialized_chunk(asset_manager.as_ref(), data).await?
                } else {
                    new_inline_chunk(data)
                };
                Ok(payload)
            })
        };
        Ok(fut)
    }

    #[instrument(skip(self))]
    pub async fn clear(&mut self) -> SessionResult<()> {
        // TODO: can this be a delete_group("/") instead?
        let to_delete: Vec<(NodeType, Path)> = self
            .list_nodes(&Path::root())
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

        // for an invalid coordinate, we bail.
        // This happens for two cases:
        // (1) the "coords" is out-of-range for the array shape
        // (2) the "coords" belongs to a shard that hasn't been written yet.
        let Some((index, _)) = find_coord(manifests.iter().map(|m| &m.extents), coords)
        else {
            return Ok(None);
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
            Err(err) => return Err(err.inject()),
        }
        Ok(None)
    }

    async fn fetch_manifest(&self, id: &ManifestId) -> SessionResult<Arc<Manifest>> {
        fetch_manifest(id, self.snapshot_id(), self.asset_manager.as_ref()).await
    }

    #[instrument(skip(self))]
    pub async fn list_nodes<'a>(
        &'a self,
        parent_group: &Path,
    ) -> SessionResult<impl Iterator<Item = SessionResult<NodeSnapshot>> + use<'a>> {
        updated_nodes(
            parent_group,
            &self.asset_manager,
            self.change_set(),
            &self.snapshot_id,
        )
        .await
    }

    #[instrument(skip(self))]
    pub async fn all_chunks(
        &self,
    ) -> SessionResult<impl Stream<Item = SessionResult<(Path, ChunkInfo)>> + '_> {
        all_chunks(&self.asset_manager, self.change_set(), self.snapshot_id()).await
    }

    #[instrument(skip(self, node))]
    pub async fn all_node_chunks<'a>(
        &'a self,
        node: &'a NodeSnapshot,
    ) -> impl Stream<Item = SessionResult<(Path, ChunkInfo)>> + 'a {
        updated_node_chunks_iterator(
            self.asset_manager.as_ref(),
            &self.change_set,
            &self.snapshot_id,
            node.clone(),
        )
        .await
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
            self.change_set(),
            &self.snapshot_id,
            node.clone(),
        )
        .await
        .map_ok(|(_path, chunk_info)| chunk_info.coord);

        let res = try_stream! {
            let new_chunks = stream::iter(
                self.change_set()
                    .array_chunks_iterator(&node.id, array_path)
                    .map(|(coord, _)| Ok::<ChunkIndices, SessionError>(coord.clone())),
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
        let resolver = &self.virtual_resolver;
        let stream = self.all_chunks().await?.try_filter_map(move |(_, info)| match info
            .payload
        {
            ChunkPayload::Virtual(reference) => {
                let expanded = match resolver.expand_location(reference.location.url()) {
                    Ok(abs) => abs,
                    Err(e) => return ready(Err(e.inject())),
                };
                ready(Ok(Some(expanded)))
            }
            _ => ready(Ok(None)),
        });
        Ok(stream)
    }

    /// Discard all uncommitted changes
    #[instrument(skip(self))]
    pub fn discard_changes(&mut self) -> SessionResult<()> {
        self.change_set_mut()?.discard_changes();
        Ok(())
    }

    /// Merge a set of `ChangeSet`s into the repository without committing them
    #[instrument(skip(self, other))]
    pub async fn merge(&mut self, other: Session) -> SessionResult<()> {
        if self.read_only() {
            return Err(SessionError::capture(SessionErrorKind::ReadOnlySession));
        }
        // A fork session cannot absorb a base session
        if self.branch_name.is_none() && other.branch_name.is_some() {
            return Err(SessionError::capture(SessionErrorKind::MergeNotAllowed));
        }
        let Session { change_set, .. } = other;

        self.change_set.merge(change_set)?;
        Ok(())
    }

    pub fn commit(&mut self, message: impl Into<String>) -> CommitBuilder<'_> {
        CommitBuilder::new(self, message.into())
    }

    async fn flush_v2(&mut self, new_snap: Arc<Snapshot>) -> SessionResult<()> {
        let update_type =
            UpdateType::NewDetachedSnapshotUpdate { new_snap_id: new_snap.id().clone() };
        let num_updates = self.config.num_updates_per_repo_info_file();
        let is_rearrange = self.mode() == SessionMode::Rearrange;
        let do_update = |repo_info: Arc<RepoInfo>, backup_path: &str, _| {
            if is_rearrange {
                raise_if_feature_flag_disabled(
                    repo_info.as_ref(),
                    MOVE_NODE_FLAG,
                    "flush rearrange session",
                )
                .inject()?;
            }
            let new_snapshot_info = SnapshotInfo {
                parent_id: Some(self.snapshot_id().clone()),
                ..new_snap.as_ref().try_into().inject()?
            };
            Ok(Arc::new(
                repo_info
                    .add_snapshot(
                        self.spec_version(),
                        new_snapshot_info,
                        None,
                        update_type.clone(),
                        None,
                        backup_path,
                        num_updates,
                    )
                    .inject()?,
            ))
        };

        let _ = self
            .asset_manager
            .update_repo_info(self.config.repo_update_retries().retries(), do_update)
            .await
            .inject()?;
        Ok(())
    }

    async fn flush_v1(&mut self, _: Arc<Snapshot>) -> SessionResult<()> {
        // In IC1 there is nothing to do here, the snapshot is already saved
        Ok(())
    }

    fn spec_version(&self) -> SpecVersionBin {
        self.asset_manager.spec_version()
    }

    fn resolve_properties(
        &self,
        overrides: Option<SnapshotProperties>,
    ) -> SnapshotProperties {
        let default = self.default_commit_metadata.clone();
        match overrides {
            Some(p) => {
                let mut merged = default;
                merged.extend(p);
                merged
            }
            None => default,
        }
    }

    #[instrument(skip(self, properties))]
    async fn do_flush(
        &mut self,
        message: &str,
        max_concurrent_nodes: usize,
        properties: Option<SnapshotProperties>,
    ) -> SessionResult<SnapshotId> {
        info!(old_snapshot_id=%self.snapshot_id(), "Flush started");

        let properties = self.resolve_properties(properties);

        let flush_data = FlushProcess::new(
            Arc::clone(&self.asset_manager),
            &self.change_set,
            self.snapshot_id(),
            self.config.manifest(),
        );
        let new_snap = do_flush(
            flush_data,
            message,
            max_concurrent_nodes,
            properties,
            false,
            CommitMethod::NewCommit,
            self.config.manifest().splitting(),
        )
        .await?;

        match self.spec_version() {
            SpecVersionBin::V1 => self.flush_v1(Arc::clone(&new_snap)).await,
            SpecVersionBin::V2 => self.flush_v2(Arc::clone(&new_snap)).await,
        }?;

        info!(
            parent_id=%self.snapshot_id(),
            new_snapshot_id=%new_snap.id(),
            "Flush done"
        );
        // if the commit was successful, we update the session to be
        // a read only session pointed at the new snapshot
        self.change_set = ChangeSet::for_edits();
        self.snapshot_id = new_snap.id().clone();
        // Once committed, the session is now read only, which we control
        // by setting the branch_name to None (you can only write to a branch session)
        self.read_only = true;
        self.branch_name = None;

        Ok(new_snap.id().clone())
    }

    #[instrument(skip(self, properties))]
    async fn do_rewrite_manifests(
        &mut self,
        message: &str,
        max_concurrent_nodes: usize,
        properties: Option<SnapshotProperties>,
        commit_method: CommitMethod,
    ) -> SessionResult<SnapshotId> {
        let nodes = self.list_nodes(&Path::root()).await?.collect::<Vec<_>>();
        // We need to populate the `splits` before calling `commit`.
        // In the normal chunk setting workflow, that would've been done by `set_chunk_ref`
        for node in nodes.into_iter().flatten() {
            if let NodeSnapshot {
                path,
                node_data: NodeData::Array { shape, dimension_names, .. },
                ..
            } = node
            {
                self.get_splits(&path, &shape, &dimension_names);
            }
        }

        let splitting_config_serialized =
            serde_json::to_value(self.config.manifest().splitting()).capture()?;
        let mut properties = properties.unwrap_or_default();
        inject_icechunk_metadata(
            &mut properties,
            "splitting_config",
            splitting_config_serialized,
        );

        self.commit_inner(
            message,
            max_concurrent_nodes,
            Some(properties),
            true,
            commit_method,
            true,
        )
        .await
    }

    #[instrument(skip(self, properties))]
    async fn commit_inner(
        &mut self,
        message: &str,
        max_concurrent_nodes: usize,
        properties: Option<SnapshotProperties>,
        rewrite_manifests: bool,
        commit_method: CommitMethod,
        allow_empty: bool,
    ) -> SessionResult<SnapshotId> {
        let Some(branch_name) = &self.branch_name else {
            return Err(SessionError::capture(SessionErrorKind::CommitNotAllowed));
        };

        // amend is only allowed in spec v2, this should be checked at this point so we only assert
        assert!(
            self.spec_version() >= SpecVersionBin::V2
                || commit_method == CommitMethod::NewCommit
        );

        let branch_name = branch_name.clone();

        let properties = self.resolve_properties(properties);
        let num_updates = self.config().num_updates_per_repo_info_file();
        {
            // we need to play this trick because we need to borrow from self twice
            // once to get the mutable change set, and other to compute
            // and pass splits
            // This solution first call the function to trigger any
            // errors, and then it takes the mutable ref again
            // without referencing self, only the field
            let _ = self.change_set_mut()?;
        }
        let change_set = &mut self.change_set;
        let is_rearrange = matches!(change_set, ChangeSet::Rearrange(_));

        let id = do_commit(
            Arc::clone(&self.asset_manager),
            branch_name.as_str(),
            &self.snapshot_id,
            change_set,
            message,
            max_concurrent_nodes,
            Some(properties),
            rewrite_manifests,
            commit_method,
            self.config.manifest(),
            allow_empty,
            is_rearrange,
            self.config.repo_update_retries().retries(),
            num_updates,
        )
        .await?;

        // if the commit was successful, we update the session to be
        // a read only session pointed at the new snapshot
        self.change_set = ChangeSet::for_edits();
        self.snapshot_id = id.clone();
        // Once committed, the session is now read only, which we control
        // by setting the branch_name to None (you can only write to a branch session)
        self.read_only = true;
        self.branch_name = None;

        Ok(id)
    }

    #[expect(clippy::too_many_arguments)]
    #[instrument(skip(self, solver, properties, before_rebase, after_rebase))]
    async fn do_commit_rebasing(
        &mut self,
        solver: &(dyn ConflictSolver + Send + Sync),
        rebase_attempts: u16,
        message: &str,
        max_concurrent_nodes: usize,
        properties: Option<SnapshotProperties>,
        allow_empty: bool,
        before_rebase: Option<RebaseHook>,
        after_rebase: Option<RebaseHook>,
    ) -> SessionResult<SnapshotId> {
        for attempt in 0..rebase_attempts {
            let mut props = properties.clone().unwrap_or_default();
            inject_icechunk_metadata(
                &mut props,
                "rebase_attempts",
                serde_json::Value::from(attempt),
            );
            match self
                .commit_inner(
                    message,
                    max_concurrent_nodes,
                    Some(props),
                    false,
                    CommitMethod::NewCommit,
                    allow_empty,
                )
                .await
            {
                Ok(snap) => return Ok(snap),
                Err(SessionError { kind: SessionErrorKind::Conflict { .. }, .. }) => {
                    if let Some(ref hook) = before_rebase {
                        hook(attempt + 1).await;
                    }
                    self.rebase(solver).await?;
                    if let Some(ref hook) = after_rebase {
                        hook(attempt + 1).await;
                    }
                }
                Err(other_err) => return Err(other_err),
            }
        }
        let mut props = properties.unwrap_or_default();
        inject_icechunk_metadata(
            &mut props,
            "rebase_attempts",
            serde_json::Value::from(rebase_attempts),
        );
        self.commit_inner(
            message,
            max_concurrent_nodes,
            Some(props),
            false,
            CommitMethod::NewCommit,
            allow_empty,
        )
        .await
    }

    /// Detect and optionally fix conflicts between the current [`ChangeSet`] (or session) and
    /// the tip of the branch.
    ///
    /// When [`Session::commit`] method is called, the system validates that the tip of the
    /// passed branch is exactly the same as the `snapshot_id` for the current session. If that
    /// is not the case, the commit operation fails with [`SessionErrorKind::Conflict`].
    ///
    /// In that situation, the user has two options:
    /// 1. Abort the session and start a new one with using the new branch tip as a parent.
    /// 2. Use [`Session::rebase`] to try to "fast-forward" the session through the new
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
    /// This is what [`Session::rebase`] helps with. It can detect conflicts to let
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
    /// `Session` in a consistent state, that would successfully commit on top
    /// of the latest successfully fast-forwarded commit.
    #[instrument(skip(self, solver))]
    pub async fn rebase(
        &mut self,
        solver: &(dyn ConflictSolver + Send + Sync),
    ) -> SessionResult<()> {
        let Some(branch_name) = &self.branch_name else {
            return Err(SessionError::capture(SessionErrorKind::CommitNotAllowed));
        };

        debug!("Rebase started");

        let new_commits = match self.spec_version() {
            SpecVersionBin::V1 => self.commits_to_rebase_v1(branch_name.as_str()).await?,
            SpecVersionBin::V2 => self.commits_to_rebase_v2(branch_name.as_str()).await?,
        };

        trace!("Found {} commits to rebase over", new_commits.len());

        let am = Arc::clone(&self.asset_manager);
        // we need to reverse the iterator to process them in order of oldest first
        let mut logs = stream::iter(new_commits.into_iter().rev())
            .map(move |snap_id| {
                let am = Arc::clone(&am);
                async move {
                    let tx_log = am.fetch_transaction_log(&snap_id).await.inject()?;
                    Ok::<_, SessionError>((snap_id, tx_log))
                }
            })
            .buffered(2);

        while let Some(res) = logs.next().await {
            let (snap_id, tx_log) = res?;
            debug!("Rebasing snapshot {}", &snap_id);
            let session = Self::create_readonly_session(
                self.config.clone(),
                self.storage_settings.as_ref().clone(),
                Arc::clone(&self.storage),
                Arc::clone(&self.asset_manager),
                Arc::clone(&self.virtual_resolver),
                snap_id.clone(),
            );

            let mut fresh = self.change_set().fresh();
            std::mem::swap(self.change_set_mut()?, &mut fresh);
            let change_set = fresh;
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
                    return Err(SessionError::capture(SessionErrorKind::RebaseFailed {
                        snapshot: snap_id,
                        conflicts: reason,
                    }));
                }
            }
        }
        debug!("Rebase done");
        Ok(())
    }

    async fn commits_to_rebase_v1(
        &self,
        branch_name: &str,
    ) -> SessionResult<Vec<SnapshotId>> {
        let ref_data = match fetch_branch_tip_v1(
            self.storage.as_ref(),
            self.storage_settings.as_ref(),
            branch_name,
        )
        .await
        {
            Ok(ref_data) => Ok(ref_data),
            Err(RefError { kind: RefErrorKind::RefNotFound { .. }, .. }) => {
                warn!(
                    branch = &self.branch_name,
                    "No rebase is needed, the branch was deleted. Aborting rebase."
                );
                return Ok(Vec::new());
            }
            Err(err) => Err(err.inject()),
        }?;

        if ref_data.snapshot == self.snapshot_id {
            // nothing to do, commit should work without rebasing
            warn!(
                branch = &self.branch_name,
                "No rebase is needed, parent snapshot is at the top of the branch. Aborting rebase."
            );
            Ok(Vec::new())
        } else {
            let current_snapshot =
                self.asset_manager.fetch_snapshot(&ref_data.snapshot).await.inject()?;
            #[expect(deprecated)]
            let ancestry = Arc::clone(&self.asset_manager)
                .snapshot_ancestry_v1(&current_snapshot.id())
                .await
                .inject()?
                .map_ok(|meta| meta.id());
            let new_commits =
                stream::once(ready(Ok(ref_data.snapshot.clone())))
                    .chain(ancestry.try_take_while(|snap_id| {
                        ready(Ok(snap_id != &self.snapshot_id))
                    }))
                    .try_collect()
                    .await
                    .inject()?;
            Ok(new_commits)
        }
    }

    async fn commits_to_rebase_v2(
        &self,
        branch_name: &str,
    ) -> SessionResult<Vec<SnapshotId>> {
        let (latest_repo_info, _) =
            self.asset_manager.fetch_repo_info().await.inject()?;

        match latest_repo_info.resolve_branch(branch_name) {
            Err(IcechunkFormatError {
                kind: IcechunkFormatErrorKind::BranchNotFound { .. },
                ..
            }) => {
                // FIXME: write test for this
                // nothing to do, branch deleted
                warn!(
                    branch = &self.branch_name,
                    "No rebase is needed, the branch was deleted. Aborting rebase."
                );
                Ok(Vec::new())
            }
            Err(err) => Err(err.inject()),
            Ok(current_snapshot_id) if current_snapshot_id == self.snapshot_id => {
                // nothing to do, commit should work without rebasing
                warn!(
                    branch = &self.branch_name,
                    "No rebase is needed, parent snapshot is at the top of the branch. Aborting rebase."
                );
                Ok(Vec::new())
            }
            Ok(current_snapshot_id) => {
                let ancestry = stream::iter(
                    latest_repo_info
                        .ancestry(&current_snapshot_id)
                        .inject()?
                        .map_ok(|snap| snap.id),
                );
                let res = ancestry
                    .try_take_while(|snap_id| ready(Ok(snap_id != &self.snapshot_id)))
                    .try_collect()
                    .await
                    .inject()?;
                Ok(res)
            }
        }
    }
}

/// Warning: The presence of a single error may mean multiple missing items
async fn updated_chunk_iterator<'a>(
    parent_group: &Path,
    asset_manager: &'a AssetManager,
    change_set: &'a ChangeSet,
    snapshot_id: &'a SnapshotId,
) -> SessionResult<impl Stream<Item = SessionResult<(Path, ChunkInfo)>> + use<'a>> {
    let snapshot = asset_manager.fetch_snapshot(snapshot_id).await.inject()?;
    let nodes = stream::iter(snapshot.iter_arc(parent_group)).map(|r| r.inject());
    let res = nodes.and_then(move |node| async move {
        // Note: Confusingly, these NodeSnapshot instances have the metadata stored in the snapshot.
        // We have not applied any changeset updates. At the moment, the downstream code only
        // use node.id so there is no need to update yet.

        Ok(updated_node_chunks_iterator(asset_manager, change_set, snapshot_id, node)
            .await)
    });
    Ok(res.try_flatten())
}

async fn updated_node_chunks_iterator<'a>(
    asset_manager: &'a AssetManager,
    change_set: &'a ChangeSet,
    snapshot_id: &'a SnapshotId,
    node: NodeSnapshot,
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
            verified_node_chunk_iterator(asset_manager, snapshot_id, change_set, node)
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
) -> impl Stream<Item = SessionResult<ChunkInfo>> + 'a + use<'a> {
    match get_node(asset_manager, change_set, snapshot_id, path).await {
        Ok(node) => Either::Left(
            verified_node_chunk_iterator(asset_manager, snapshot_id, change_set, node)
                .await,
        ),
        Err(_) => Either::Right(stream::empty()),
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
        NodeData::Group => Either::Left(stream::empty()),
        NodeData::Array { manifests, .. } => {
            let new_chunk_indices: Box<HashSet<&ChunkIndices>> = Box::new(
                change_set
                    .array_chunks_iterator(&node.id, &node.path)
                    .map(|(idx, _)| idx)
                    // by chaining here, we make sure we don't pull from the manifest
                    // any chunks that were deleted prior to resizing in this session
                    .chain(change_set.deleted_chunks_iterator(&node.id))
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

            Either::Right(
                stream::iter(new_chunks).chain(
                    stream::iter(manifests)
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
                                match manifest
                                    .and_then(|m| m.iter(node_id_c.clone()).inject())
                                {
                                    Ok(iter) => {
                                        let old_chunks = iter
                                            .filter_ok(move |(coord, _)| {
                                                !new_chunk_indices.contains(coord)
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
                                        Either::Left(
                                            stream::iter(old_chunks)
                                                .map_err(|e| e.inject()),
                                        )
                                    }
                                    // if we cannot even fetch the manifest, we generate a
                                    // single error value.
                                    Err(err) => {
                                        Either::Right(stream::once(ready(Err(err))))
                                    }
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
    let tomatch = if prefix != "/" { key.strip_prefix(prefix) } else { Some(key) };
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
    asset_manager.write_chunk(new_id.clone(), data.clone()).await.inject()?;
    Ok(ChunkPayload::Ref(ChunkRef { id: new_id, offset: 0, length: data.len() as u64 }))
}

fn new_inline_chunk(data: Bytes) -> ChunkPayload {
    trace!("Setting inline chunk of size {}", data.len());
    ChunkPayload::Inline(data)
}

pub async fn get_chunk(
    reader: Option<crate::compat::IcechunkBoxFuture<'_, SessionResult<Bytes>>>,
) -> SessionResult<Option<Bytes>> {
    match reader {
        Some(reader) => Ok(Some(reader.await?)),
        None => Ok(None),
    }
}

async fn updated_existing_nodes<'a>(
    parent_group: &Path,
    asset_manager: &AssetManager,
    change_set: &'a ChangeSet,
    parent_id: &SnapshotId,
) -> SessionResult<impl Iterator<Item = SessionResult<NodeSnapshot>> + use<'a>> {
    let parent_group = parent_group.clone();
    let snapshot = asset_manager.fetch_snapshot(parent_id).await.inject()?;

    let mut moved_nodes: Vec<SessionResult<NodeSnapshot>> = Vec::new();
    for (orig, final_path) in change_set.moved_into(&parent_group) {
        match snapshot.get_node(&orig) {
            Ok(node) => {
                moved_nodes
                    .push(Ok(NodeSnapshot { path: final_path, ..(*node).clone() }));
            }
            Err(err) => moved_nodes.push(Err(err.inject())),
        }
    }

    // Skip remapped nodes — they're already in moved_nodes above.
    let unmoved = snapshot
        .iter_arc(&parent_group)
        .filter_map_ok(move |node| {
            if change_set.is_remapped(&node.path) {
                None
            } else {
                change_set.update_existing_node(node)
            }
        })
        .map(|n| n.map_err(|err| err.inject()));

    Ok(moved_nodes.into_iter().chain(unmoved))
}

/// Yields nodes with the snapshot, applying any relevant updates in the changeset,
/// *and* new nodes in the changeset
async fn updated_nodes<'a>(
    parent_group: &Path,
    asset_manager: &AssetManager,
    change_set: &'a ChangeSet,
    parent_id: &SnapshotId,
) -> SessionResult<impl Iterator<Item = SessionResult<NodeSnapshot>> + use<'a>> {
    Ok(updated_existing_nodes(parent_group, asset_manager, change_set, parent_id)
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
            if change_set.is_deleted(path, &node.id) {
                Err(SessionError::capture(SessionErrorKind::NodeNotFound {
                    path: path.clone(),
                    message: "getting node".to_string(),
                }))
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
    let snapshot = asset_manager.fetch_snapshot(snapshot_id).await.inject()?;

    let moved_from = change_set.moved_from(path);
    if matches!(moved_from, MovedFrom::Deleted) {
        return Err(SessionError::capture(SessionErrorKind::NodeNotFound {
            path: path.clone(),
            message: "existing node not found".to_string(),
        }));
    }
    let was_moved = matches!(moved_from, MovedFrom::From(_));
    let renamed_path = match moved_from {
        MovedFrom::From(p) | MovedFrom::NotMoved(p) => p,
        MovedFrom::Deleted => unreachable!(),
    };

    match snapshot.get_node(renamed_path.as_ref()) {
        Ok(node) => {
            let node = match node.node_data {
                // this overly verbose match arm allows us to minimize clones
                NodeData::Array { .. } => match change_set.get_updated_array(&node.id) {
                    Some(new_data) => {
                        if let NodeData::Array { manifests, .. } = &node.node_data {
                            let node_data = NodeData::Array {
                                shape: new_data.shape.clone(),
                                dimension_names: new_data.dimension_names.clone(),
                                manifests: manifests.clone(),
                            };
                            NodeSnapshot {
                                user_data: new_data.user_data.clone(),
                                node_data,
                                id: node.id.clone(),
                                path: node.path.clone(),
                            }
                        } else {
                            unreachable!()
                        }
                    }
                    None => Arc::unwrap_or_clone(node),
                },
                NodeData::Group => {
                    let node = Arc::unwrap_or_clone(node);
                    if let Some(updated_definition) =
                        change_set.get_updated_group(&node.id)
                    {
                        NodeSnapshot { user_data: updated_definition.clone(), ..node }
                    } else {
                        node
                    }
                }
            };
            let node = if was_moved {
                // this is technically unnecessary for back-and-forth moves
                // but we will ignore that rare case
                NodeSnapshot { path: path.clone(), ..node }
            } else {
                node
            };
            Ok(node)
        }
        // A missing node here is not really a format error, so we need to
        // generate the correct error for repositories
        Err(IcechunkFormatError {
            kind: IcechunkFormatErrorKind::NodeNotFound { .. },
            ..
        }) => Err(SessionError::capture(SessionErrorKind::NodeNotFound {
            path: path.clone(),
            message: "existing node not found".to_string(),
        })),
        Err(err) => Err(err.inject()),
    }
}

async fn all_chunks<'a>(
    asset_manager: &'a AssetManager,
    change_set: &'a ChangeSet,
    snapshot_id: &'a SnapshotId,
) -> SessionResult<impl Stream<Item = SessionResult<(Path, ChunkInfo)>> + 'a> {
    let existing_array_chunks =
        updated_chunk_iterator(&Path::root(), asset_manager, change_set, snapshot_id)
            .await?;
    let new_array_chunks = stream::iter(change_set.new_arrays_chunk_iterator().map(Ok));
    Ok(existing_array_chunks.chain(new_array_chunks))
}

// Converts the requested ByteRange to a valid ByteRange appropriate
// to the chunk reference of known `offset` and `length`.
pub fn construct_valid_byte_range(
    request: &ByteRange,
    chunk_offset: u64,
    chunk_length: u64,
) -> SessionResult<Range<ChunkOffset>> {
    let err = || -> SessionError {
        SessionError::capture(SessionErrorKind::InvalidByteRange {
            request: request.clone(),
            chunk_length,
        })
    };
    match request {
        ByteRange::Bounded(Range { start: req_start, end: req_end }) => {
            let new_start = chunk_offset + req_start;
            let new_end = chunk_offset + req_end;
            if new_start >= chunk_offset + chunk_length
                || new_end > chunk_offset + chunk_length
            {
                return Err(err());
            }
            Ok(new_start..new_end)
        }
        ByteRange::From(n) => {
            let new_start = chunk_offset + n;
            if new_start >= chunk_offset + chunk_length {
                return Err(err());
            }
            Ok(new_start..chunk_offset + chunk_length)
        }
        ByteRange::Until(n) => {
            if *n > chunk_length {
                return Err(err());
            }
            let new_end = chunk_offset + chunk_length;
            let new_start = new_end - n;
            Ok(new_start..new_end)
        }
        ByteRange::Last(n) => {
            if *n > chunk_length {
                return Err(err());
            }
            let new_end = chunk_offset + chunk_length;
            let new_start = new_end - n;
            Ok(new_start..new_end)
        }
    }
}

struct FlushProcess<'a> {
    asset_manager: Arc<AssetManager>,
    change_set: &'a ChangeSet,
    parent_id: &'a SnapshotId,
    manifest_config: &'a ManifestConfig,
    manifest_refs: HashMap<NodeId, Vec<ManifestRef>>,
    manifest_files: HashSet<ManifestFileInfo>,
}

impl<'a> FlushProcess<'a> {
    fn new(
        asset_manager: Arc<AssetManager>,
        change_set: &'a ChangeSet,
        parent_id: &'a SnapshotId,
        manifest_config: &'a ManifestConfig,
    ) -> Self {
        Self {
            asset_manager,
            change_set,
            parent_id,
            manifest_config,
            manifest_refs: Default::default(),
            manifest_files: Default::default(),
        }
    }
}

struct NodeFlushResult {
    node_id: NodeId,
    manifest_refs: Vec<ManifestRef>,
    manifest_files: Vec<ManifestFileInfo>,
}

async fn write_manifest_from_stream(
    asset_manager: &AssetManager,
    manifest_config: &ManifestConfig,
    chunks: impl Stream<Item = SessionResult<ChunkInfo>>,
) -> SessionResult<Option<(ManifestRef, ManifestFileInfo)>> {
    let mut from = vec![];
    let mut to = vec![];
    let chunks = aggregate_extents(&mut from, &mut to, chunks, |ci| &ci.coord);

    let compression_config: Option<LocationCompressionConfig> =
        if asset_manager.spec_version() >= SpecVersionBin::V2 {
            Some(manifest_config.virtual_chunk_location_compression().into())
        } else {
            None
        };
    let mut all: Vec<ChunkInfo> = chunks.try_collect().await?;
    all.sort_by(|a, b| (&a.node, &a.coord).cmp(&(&b.node, &b.coord)));
    if let Some(new_manifest) =
        Manifest::from_sorted_vec(&ManifestId::random(), all, compression_config.as_ref())
            .inject()?
    {
        let new_manifest = Arc::new(new_manifest);
        let new_manifest_size =
            asset_manager.write_manifest(Arc::clone(&new_manifest)).await.inject()?;

        let file_info = ManifestFileInfo::new(new_manifest.as_ref(), new_manifest_size);
        let new_ref = ManifestRef {
            object_id: new_manifest.id().clone(),
            extents: ManifestExtents::new(&from, &to),
        };
        Ok(Some((new_ref, file_info)))
    } else {
        Ok(None)
    }
}

/// Creates a new manifest for the node, by obtaining all previous chunks coming from
/// `previous_manifests`, filtering those that are in the `extent`, and overriding them
/// with any changes in `modified_chunks`
async fn write_manifest_with_changes(
    asset_manager: &AssetManager,
    manifest_config: &ManifestConfig,
    previous_manifests: impl Iterator<Item = &ManifestRef>,
    modified_chunks: ChunkTable,
    extent: &ManifestExtents,
    node_id: &NodeId,
    old_snapshot_id: &SnapshotId,
) -> SessionResult<Option<(ManifestRef, ManifestFileInfo)>> {
    // First add chunks from previous manifests that are not modified
    let futs = previous_manifests
        .map(|mref| fetch_manifest(&mref.object_id, old_snapshot_id, asset_manager))
        .collect::<Vec<_>>();

    // Hardcoded to 1: this fetches manifests for a single extent within a single node.
    // Node-level parallelism is already controlled by max_concurrent_nodes in the
    // caller (do_flush), so adding concurrency here would compound it.
    let mut all_chunks_vec = stream::iter(futs)
        .buffer_unordered(1)
        .try_fold(Vec::with_capacity(modified_chunks.len()), |mut acc, manifest| async {
            acc.extend(manifest.iter(node_id.clone()).inject()?.filter_map_ok(
                |(idx, payload)| {
                    if !modified_chunks.contains_key(&idx) && extent.contains(&idx.0) {
                        Some(ChunkInfo { node: node_id.clone(), coord: idx, payload })
                    } else {
                        None
                    }
                },
            ));
            Ok(acc)
        })
        .await?;

    // Then add modified chunks from ChangeSet
    all_chunks_vec.extend(modified_chunks.into_iter().filter_map(
        |(idx, maybe_payload)| {
            maybe_payload.map(|payload| {
                Ok(ChunkInfo { node: node_id.clone(), coord: idx, payload })
            })
        },
    ));

    write_manifest_from_stream(
        asset_manager,
        manifest_config,
        stream::iter(all_chunks_vec).map_err(|e| e.inject()),
    )
    .await
}

/// Process a single existing array node during flush.
///
/// Returns `None` if the node was deleted or is not an array.
/// Otherwise returns the manifest refs and files for this node.
#[expect(clippy::too_many_arguments)]
async fn flush_existing_node(
    asset_manager: &AssetManager,
    manifest_config: &ManifestConfig,
    change_set: &ChangeSet,
    parent_id: &SnapshotId,
    old_snapshot: &Snapshot,
    split_config: &ManifestSplittingConfig,
    rewrite_manifests: bool,
    node: NodeSnapshot,
) -> SessionResult<Option<NodeFlushResult>> {
    let node_id = &node.id;

    if change_set.array_is_deleted(&(node.path.clone(), node_id.clone())) {
        trace!(path=%node.path, "Node deleted, not writing a manifest");
        return Ok(None);
    }

    if rewrite_manifests
        || change_set.is_updated_array(node_id)
        || change_set.has_chunk_changes(node_id)
    {
        trace!(path=%node.path, "Node has changes, writing a new manifest");
        let new_node =
            get_existing_node(asset_manager, change_set, parent_id, &node.path).await?;

        if let NodeData::Array { manifests, shape, dimension_names } = new_node.node_data
        {
            let splits =
                split_config.get_split_sizes(&new_node.path, &shape, &dimension_names);

            let mut result = NodeFlushResult {
                node_id: node_id.clone(),
                manifest_refs: Vec::new(),
                manifest_files: Vec::new(),
            };

            // Some points to take into account to understand this algorithm:
            // * The `splits` could have changed, so the `manifests` not necessarily were
            // created with the same splits, they could be widely different
            // * In general we don't want to rewrite past manifests if we don't have to, we just
            // try to reuse them, but if user says `rewrite_manifests=true` we'll rewrite everything
            // * This function needs to work in the scenario where there are multiple past manifests
            // for the node, and there are also session changes to chunks. These changes can be
            // modifying, adding or deleting existing chunks.
            // * We want this function to take time and space proportional to the size of the split,
            // and not to the total size of the array.
            let mut updated_chunks_by_extent: HashMap<ManifestExtents, ChunkTable> =
                change_set.array_chunks_iterator(&node.id, &node.path).fold(
                    HashMap::new(),
                    |mut res, (idx, payload)| {
                        if let Some(extents) = splits.find(idx) {
                            let entry = res.entry(extents).or_default();
                            entry.insert(idx.clone(), payload.clone());
                        }
                        res
                    },
                );
            let snapshot_id = old_snapshot.id();

            for extent in splits.iter() {
                let intersecting_manifests: Vec<(&ManifestRef, Overlap)> = manifests
                    .iter()
                    .filter_map(|mr| match mr.extents.overlap_with(&extent) {
                        Overlap::None => None,
                        ov => Some((mr, ov)),
                    })
                    .collect();

                let modified_chunks =
                    updated_chunks_by_extent.remove(&extent).unwrap_or_default();

                if !modified_chunks.is_empty() || rewrite_manifests {
                    if let Some((new_ref, file_info)) = write_manifest_with_changes(
                        asset_manager,
                        manifest_config,
                        intersecting_manifests.iter().map(|(mr, _)| *mr),
                        modified_chunks,
                        &extent,
                        &node.id,
                        &snapshot_id,
                    )
                    .await?
                    {
                        result.manifest_refs.push(new_ref);
                        result.manifest_files.push(file_info);
                    }
                } else {
                    for (mref, overlap) in intersecting_manifests {
                        if overlap == Overlap::Complete {
                            result.manifest_refs.push(mref.clone());
                            #[expect(clippy::expect_used)]
                            result.manifest_files.push(
                                old_snapshot.manifest_info(&mref.object_id).inject()?.expect("logic bug. creating manifest file info for an existing manifest failed."),
                            );
                        } else if let Some((new_ref, file_info)) =
                            write_manifest_with_changes(
                                asset_manager,
                                manifest_config,
                                std::iter::once(mref),
                                Default::default(),
                                &extent,
                                &node.id,
                                &snapshot_id,
                            )
                            .await?
                        {
                            result.manifest_refs.push(new_ref);
                            result.manifest_files.push(file_info);
                        }
                    }
                }
            }

            Ok(Some(result))
        } else {
            Ok(None)
        }
    } else {
        trace!(path=%node.path, "Node has no changes, keeping the previous manifest");
        match node.node_data {
            NodeData::Array { manifests: array_refs, .. } => {
                let mut result = NodeFlushResult {
                    node_id: node_id.clone(),
                    manifest_refs: Vec::new(),
                    manifest_files: Vec::new(),
                };
                for mr in &array_refs {
                    #[expect(clippy::expect_used)]
                    let mf = old_snapshot.manifest_info(&mr.object_id).inject()?.expect(
                        "Bug in flush function, no manifest file found in snapshot",
                    );
                    result.manifest_files.push(mf);
                }
                result.manifest_refs.extend(array_refs.into_iter());
                Ok(Some(result))
            }
            NodeData::Group => Ok(None),
        }
    }
}

/// Process a single new array node during flush.
async fn flush_new_node(
    asset_manager: &AssetManager,
    manifest_config: &ManifestConfig,
    change_set: &ChangeSet,
    node_id: &NodeId,
    node_path: &Path,
    splits: &ManifestSplits,
) -> SessionResult<NodeFlushResult> {
    let mut result = NodeFlushResult {
        node_id: node_id.clone(),
        manifest_refs: Vec::new(),
        manifest_files: Vec::new(),
    };

    for extent in splits.iter() {
        if change_set.array_manifest(node_id).is_some() {
            let chunks = stream::iter(
                change_set
                    .array_chunks_iterator(node_id, node_path)
                    // FIXME: do we need to optimize this so we don't need multiple passes over all chunks calling
                    // contains?
                    .filter_map(|(coord, payload)| {
                        if let Some(payload) = payload
                            && extent.contains(&coord.0)
                        {
                            Some(ChunkInfo {
                                node: node_id.clone(),
                                coord: coord.clone(),
                                payload: payload.clone(),
                            })
                        } else {
                            None
                        }
                    })
                    .map(Ok),
            );
            if let Some((new_ref, file_info)) =
                write_manifest_from_stream(asset_manager, manifest_config, chunks).await?
            {
                result.manifest_refs.push(new_ref);
                result.manifest_files.push(file_info);
            }
        }
    }

    Ok(result)
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

        #[expect(clippy::expect_used)]
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
                for (axis, dimname) in enumerate(dimension_names) {
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

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum CommitMethod {
    NewCommit,
    Amend,
}

async fn do_flush(
    mut flush_data: FlushProcess<'_>,
    message: &str,
    max_concurrent_nodes: usize,
    properties: SnapshotProperties,
    rewrite_manifests: bool,
    commit_method: CommitMethod,
    split_config: &ManifestSplittingConfig,
) -> SessionResult<Arc<Snapshot>> {
    let old_snapshot =
        flush_data.asset_manager.fetch_snapshot(flush_data.parent_id).await.inject()?;

    let previous_tx_log = if commit_method == CommitMethod::NewCommit {
        // We won't be merging with a previous tx log, so no need to retrieve it
        None
    } else {
        let previous_log = flush_data
            .asset_manager
            .fetch_transaction_log(&old_snapshot.id())
            .await
            .inject()?;

        // need to check if previous tx log has moves / this is a rearrange session
        if previous_log.has_moves() && commit_method == CommitMethod::Amend {
            match flush_data.change_set {
                ChangeSet::Edit(_) => {
                    return Err(SessionError::capture(
                        SessionErrorKind::RearrangeSessionOnly,
                    ));
                }
                ChangeSet::Rearrange(_) => {
                    // Fine for now
                }
            }
        }

        Some(previous_log)
    };

    // We first go through all existing nodes to see if we need to rewrite any manifests

    let change_set = flush_data.change_set;
    let manifest_config = flush_data.manifest_config;
    let parent_id = flush_data.parent_id;

    let array_nodes: Vec<NodeSnapshot> = old_snapshot
        .iter()
        .filter_ok(|node| node.node_type() == NodeType::Array)
        .try_collect()
        .inject()?;

    let existing_results: Vec<Option<NodeFlushResult>> =
        stream::iter(array_nodes.into_iter().map(|node| {
            let asset_manager = Arc::clone(&flush_data.asset_manager);
            let old_snapshot = Arc::clone(&old_snapshot);
            async move {
                flush_existing_node(
                    asset_manager.as_ref(),
                    manifest_config,
                    change_set,
                    parent_id,
                    old_snapshot.as_ref(),
                    split_config,
                    rewrite_manifests,
                    node,
                )
                .await
            }
        }))
        .buffer_unordered(max_concurrent_nodes)
        .try_collect()
        .await?;

    for result in existing_results.into_iter().flatten() {
        flush_data
            .manifest_refs
            .entry(result.node_id)
            .or_default()
            .extend(result.manifest_refs);
        flush_data.manifest_files.extend(result.manifest_files);
    }

    // Now we need to go through all the new arrays, and generate manifests for them

    let new_arrays: Vec<(Path, NodeId, ManifestSplits)> = change_set
        .new_arrays()
        .map(|(node_path, node_id, array_data)| {
            let splits = split_config.get_split_sizes(
                node_path,
                &array_data.shape,
                &array_data.dimension_names,
            );
            trace!(path=%node_path, "New node, writing a manifest");
            (node_path.clone(), node_id.clone(), splits)
        })
        .collect();

    let new_node_results: Vec<NodeFlushResult> =
        stream::iter(new_arrays.into_iter().map(|(node_path, node_id, splits)| {
            let asset_manager = Arc::clone(&flush_data.asset_manager);
            async move {
                flush_new_node(
                    asset_manager.as_ref(),
                    manifest_config,
                    change_set,
                    &node_id,
                    &node_path,
                    &splits,
                )
                .await
            }
        }))
        .buffer_unordered(max_concurrent_nodes)
        .try_collect()
        .await?;

    for result in new_node_results {
        flush_data
            .manifest_refs
            .entry(result.node_id)
            .or_default()
            .extend(result.manifest_refs);
        flush_data.manifest_files.extend(result.manifest_files);
    }

    // manifest_files & manifest_refs _must_ be consistent
    let mfiles =
        flush_data.manifest_files.iter().map(|x| x.id.clone()).collect::<HashSet<_>>();
    let mrefs = flush_data
        .manifest_refs
        .values()
        .flatten()
        .map(|x| x.object_id.clone())
        .collect::<HashSet<_>>();
    if mfiles != mrefs {
        return Err(SessionError::capture(
            SessionErrorKind::ManifestsInconsistencyError {
                snapshot: mfiles.len(),
                nodes: mrefs.len(),
            },
        ));
    }

    trace!("Building new snapshot");
    // gather and sort nodes:
    // this is a requirement for Snapshot::from_iter
    let mut all_nodes: Vec<_> = updated_nodes(
        &Path::root(),
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

    all_nodes
        .sort_by(|a, b| a.path.to_string().as_str().cmp(b.path.to_string().as_str()));

    // Icechunk 2 no longer stores the parent snapshot id in the snapshot
    let parent_id = if flush_data.asset_manager.spec_version() == SpecVersionBin::V1 {
        Some(flush_data.parent_id.clone())
    } else {
        None
    };

    let new_snapshot = Snapshot::from_iter(
        None,
        parent_id,
        flush_data.asset_manager.spec_version(),
        message,
        Some(properties),
        flush_data.manifest_files.into_iter().collect(),
        None,
        all_nodes.into_iter().map(Ok::<_, IcechunkFormatError>),
    )
    .inject()?;

    let new_ts = new_snapshot.flushed_at().inject()?;
    let old_ts = old_snapshot.flushed_at().inject()?;
    if new_ts <= old_ts {
        tracing::error!(
            new_timestamp = %new_ts,
            old_timestamp = %old_ts,
            "Snapshot timestamp older than parent, aborting commit"
        );
        return Err(SessionError::capture(
            SessionErrorKind::InvalidSnapshotTimestampOrdering {
                parent: old_ts,
                child: new_ts,
            },
        ));
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

    let this_tx_log =
        transaction_log_from_change_set(&new_snapshot_id, flush_data.change_set);
    let new_tx_log = if commit_method == CommitMethod::NewCommit {
        this_tx_log
    } else {
        match previous_tx_log {
            Some(previous_log) => {
                let snapshot_id = new_snapshot_id.clone();
                let span = Span::current();
                tokio::task::spawn_blocking(move || {
                    let _entered = span.entered();
                    TransactionLog::merge(
                        &snapshot_id,
                        [previous_log.as_ref(), &this_tx_log],
                    )
                })
                .await
                .capture()?
                .inject()?
            }
            None => this_tx_log,
        }
    };

    flush_data
        .asset_manager
        .write_transaction_log(new_snapshot_id.clone(), Arc::new(new_tx_log))
        .await
        .inject()?;

    let snapshot_timestamp = snapshot_timestamp.await.capture()?.inject()?;

    // Fail if there is too much clock difference with the object store
    // This is to prevent issues with snapshot ordering and expiration
    if (snapshot_timestamp - new_ts).num_seconds().abs() > 600 {
        tracing::error!(
            snapshot_timestamp = %new_ts,
            object_store_timestamp = %snapshot_timestamp,
            "Snapshot timestamp drifted from object store clock, aborting commit"
        );
        return Err(SessionError::capture(SessionErrorKind::InvalidSnapshotTimestamp {
            object_store_time: snapshot_timestamp,
            snapshot_time: new_ts,
        }));
    }

    Ok(new_snapshot)
}

#[expect(clippy::too_many_arguments)]
async fn do_commit(
    asset_manager: Arc<AssetManager>,
    branch_name: &str,
    snapshot_id: &SnapshotId,
    change_set: &ChangeSet,
    message: &str,
    max_concurrent_nodes: usize,
    properties: Option<SnapshotProperties>,
    rewrite_manifests: bool,
    commit_method: CommitMethod,
    manifest_config: &ManifestConfig,
    allow_empty: bool,
    is_rearrange: bool,
    retry_settings: &storage::RetriesSettings,
    num_updates_per_repo_info_file: u16,
) -> SessionResult<SnapshotId> {
    info!(branch_name, old_snapshot_id=%snapshot_id, "Commit started");

    if !allow_empty && change_set.is_empty() {
        return Err(SessionError::capture(SessionErrorKind::NoChangesToCommit));
    }

    // Cannot amend the initial commit
    if commit_method == CommitMethod::Amend && snapshot_id.is_initial() {
        return Err(SessionError::capture(SessionErrorKind::NoAmendForInitialCommit));
    }

    let properties = properties.unwrap_or_default();
    let flush_data = FlushProcess::new(
        Arc::clone(&asset_manager),
        change_set,
        snapshot_id,
        manifest_config,
    );
    let new_snapshot = do_flush(
        flush_data,
        message,
        max_concurrent_nodes,
        properties,
        rewrite_manifests,
        commit_method,
        manifest_config.splitting(),
    )
    .await?;
    let new_snapshot_id = new_snapshot.id();

    let res = match asset_manager.spec_version() {
        SpecVersionBin::V1 => {
            do_commit_v1(
                asset_manager.storage().as_ref(),
                asset_manager.storage_settings(),
                branch_name,
                snapshot_id,
                new_snapshot_id.clone(),
            )
            .await
        }
        SpecVersionBin::V2 => {
            do_commit_v2(
                asset_manager,
                branch_name,
                snapshot_id,
                new_snapshot,
                commit_method,
                is_rearrange,
                retry_settings,
                num_updates_per_repo_info_file,
            )
            .await
        }
    };

    match res {
        Ok(new_version) => {
            info!(
                branch_name,
                new_commit_object_version=?new_version,
                %new_snapshot_id,
                "Commit done"
            );
            Ok(new_snapshot_id)
        }
        Err(RepositoryError {
            kind: RepositoryErrorKind::Conflict { expected_parent, actual_parent },
            context,
        }) => Err(ICError {
            kind: SessionErrorKind::Conflict { expected_parent, actual_parent },
            context,
        }),
        Err(RepositoryError {
            kind: RepositoryErrorKind::NoAmendForInitialCommit,
            context,
        }) => Err(ICError { kind: SessionErrorKind::NoAmendForInitialCommit, context }),
        Err(err) => Err(err.inject()),
    }
}

async fn do_commit_v1(
    storage: &dyn Storage,
    storage_settings: &storage::Settings,
    branch_name: &str,
    parent_snapshot_id: &SnapshotId,
    new_snapshot_id: SnapshotId,
) -> RepositoryResult<storage::VersionInfo> {
    debug!(branch_name, new_snapshot_id=%new_snapshot_id, "Updating branch");
    match update_branch(
        storage,
        storage_settings,
        branch_name,
        new_snapshot_id,
        Some(parent_snapshot_id),
    )
    .await
    {
        Ok(version) => Ok(version),
        Err(RefError {
            kind: RefErrorKind::Conflict { expected_parent, actual_parent },
            context,
        }) => Err(ICError {
            kind: RepositoryErrorKind::Conflict { expected_parent, actual_parent },
            context,
        }),
        Err(err) => Err(err.inject()),
    }
}

#[expect(clippy::too_many_arguments)]
async fn do_commit_v2(
    asset_manager: Arc<AssetManager>,
    branch_name: &str,
    parent_snapshot_id: &SnapshotId,
    new_snapshot: Arc<Snapshot>,
    commit_method: CommitMethod,
    is_rearrange: bool,
    retry_settings: &storage::RetriesSettings,
    num_updates_per_repo_info_file: u16,
) -> RepositoryResult<storage::VersionInfo> {
    let mut attempt = 0;
    let new_snapshot_id = new_snapshot.id();
    let do_update = |repo_info: Arc<RepoInfo>, backup_path: &str, _| {
        if is_rearrange {
            raise_if_feature_flag_disabled(
                repo_info.as_ref(),
                MOVE_NODE_FLAG,
                "commit rearrange session",
            )
            .inject()?;
        }
        attempt += 1;
        let actual_parent = repo_info.resolve_branch(branch_name).inject()?;
        if &actual_parent != parent_snapshot_id {
            info!(branch_name, %new_snapshot_id, attempt, "Branch tip has changed, rebase needed");
            return Err(RepositoryError::capture(RepositoryErrorKind::Conflict {
                expected_parent: Some(parent_snapshot_id.clone()),
                actual_parent: Some(actual_parent),
            }));
        }

        let parent_snapshot = repo_info.find_snapshot(parent_snapshot_id).inject()?;
        let parent_id = match (commit_method, parent_snapshot.parent_id) {
            (CommitMethod::NewCommit, _) => parent_snapshot_id.clone(),
            (CommitMethod::Amend, Some(parent_id)) => parent_id,
            (CommitMethod::Amend, None) => {
                unreachable!("do_commit() disallows amend on initial commit")
            }
        };

        debug!(branch_name, %new_snapshot_id, %parent_id, attempt, "Generating new repo info object");
        let new_snapshot_info = SnapshotInfo {
            parent_id: Some(parent_id.clone()),
            ..new_snapshot.as_ref().try_into().inject()?
        };

        let update_type = match commit_method {
            CommitMethod::NewCommit => UpdateType::NewCommitUpdate {
                branch: branch_name.to_string(),
                new_snap_id: new_snapshot_id.clone(),
            },
            CommitMethod::Amend => UpdateType::CommitAmendedUpdate {
                branch: branch_name.to_string(),
                previous_snap_id: parent_snapshot.id.clone(),
                new_snap_id: new_snapshot_id.clone(),
            },
        };
        Ok(Arc::new(
            repo_info
                .add_snapshot(
                    asset_manager.spec_version(),
                    new_snapshot_info,
                    Some(branch_name),
                    update_type,
                    None,
                    backup_path,
                    num_updates_per_repo_info_file,
                )
                .inject()?,
        ))
    };

    let res = asset_manager.update_repo_info(retry_settings, do_update).await?;
    Ok(res)
}

async fn fetch_manifest(
    manifest_id: &ManifestId,
    snapshot_id: &SnapshotId,
    asset_manager: &AssetManager,
) -> SessionResult<Arc<Manifest>> {
    let snapshot = asset_manager.fetch_snapshot(snapshot_id).await.inject()?;
    let manifest_info = snapshot
        .manifest_info(manifest_id)
        .inject()?
        .ok_or_else(|| IcechunkFormatErrorKind::ManifestInfoNotFound {
            manifest_id: manifest_id.clone(),
        })
        .capture::<IcechunkFormatErrorKind>()
        .inject()?;
    asset_manager.fetch_manifest(manifest_id, manifest_info.size_bytes).await.inject()
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
                if let Some(from_current) = from.get_mut(coord_idx)
                    && value < from_current
                {
                    *from_current = *value;
                }
                if let Some(to_current) = to.get_mut(coord_idx) {
                    let range_value = value + 1;
                    if range_value > *to_current {
                        *to_current = range_value;
                    }
                }
            }
        }
        t
    })
}

#[cfg(test)]
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
        format::{
            format_constants::SpecVersionBin,
            manifest::{ManifestExtents, ManifestSplits},
            repo_info::RepoInfo,
        },
        repository::VersionInfo,
        storage::new_in_memory_storage,
        strategies::{
            ShapeDim, chunk_indices, empty_writable_session, node_paths, shapes_and_dims,
        },
    };

    use super::*;
    use async_trait::async_trait;
    use icechunk_macros::tokio_test;
    use itertools::assert_equal;
    use rstest::rstest;
    use rstest_reuse::{self, *};

    use pretty_assertions::assert_eq;
    use proptest::prelude::{prop_assert, prop_assert_eq};
    use storage::logging::LoggingStorage;
    use test_strategy::proptest;
    // #[cfg(not(feature = "shuttle"))]
    use tokio::sync::Barrier;

    use crate::test_utils::spec_version_cases;

    async fn assert_manifest_count(
        asset_manager: &Arc<AssetManager>,
        total_manifests: usize,
    ) {
        let expected = asset_manager.list_manifests().await.unwrap().count().await;
        assert_eq!(
            total_manifests, expected,
            "Mismatch in manifest count: expected {expected}, but got {total_manifests}",
        );
    }

    async fn create_memory_store_repository(spec_version: SpecVersionBin) -> Repository {
        let storage =
            new_in_memory_storage().await.expect("failed to create in-memory store");
        Repository::create(None, storage, HashMap::new(), Some(spec_version), true)
            .await
            .unwrap()
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
        #[strategy(shapes_and_dims(None, None))] metadata: ShapeDim,
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
        #[strategy(shapes_and_dims(None, None))] metadata: ShapeDim,
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
            stream::iter(
                indices.into_iter().map(Ok::<ChunkIndices, std::convert::Infallible>),
            ),
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

        assert_eq!(
            splits.find(&ChunkIndices(vec![1])),
            Some(ManifestExtents::new(&[0], &[10]))
        );
        assert_eq!(
            splits.find(&ChunkIndices(vec![11])),
            Some(ManifestExtents::new(&[10], &[20]))
        );

        let edges = vec![vec![0, 10, 20], vec![0, 10, 20]];

        let splits = ManifestSplits::from_edges(edges);
        assert_eq!(
            splits.find(&ChunkIndices(vec![1, 1])),
            Some(ManifestExtents::new(&[0, 0], &[10, 10]))
        );
        assert_eq!(
            splits.find(&ChunkIndices(vec![1, 10])),
            Some(ManifestExtents::new(&[0, 10], &[10, 20]))
        );
        assert_eq!(
            splits.find(&ChunkIndices(vec![1, 11])),
            Some(ManifestExtents::new(&[0, 10], &[10, 20]))
        );
        assert!(splits.find(&ChunkIndices(vec![21, 21])).is_none());
        assert!(splits.find(&ChunkIndices(vec![0, 21])).is_none());
        assert!(splits.find(&ChunkIndices(vec![21, 0])).is_none());

        Ok(())
    }

    #[tokio_test]
    #[apply(spec_version_cases)]
    async fn test_repository_with_default_commit_metadata(
        #[case] spec_version: SpecVersionBin,
    ) -> Result<(), Box<dyn Error>> {
        let mut repo = create_memory_store_repository(spec_version).await;
        let mut ds = repo.writable_session("main").await?;
        ds.add_group(Path::root(), Bytes::new()).await?;
        let snapshot = ds.commit("commit 1").max_concurrent_nodes(8).execute().await?;

        // Verify that the first commit has no metadata
        let v = VersionInfo::SnapshotId(snapshot.clone());
        let ancestry = repo.ancestry(&v).await?;
        let snapshot_infos = ancestry.try_collect::<Vec<_>>().await?;
        assert!(snapshot_infos[0].metadata.is_empty());

        // Set some default metadata
        let mut default_metadata = SnapshotProperties::default();
        default_metadata.insert("author".to_string(), "John Doe".to_string().into());
        default_metadata.insert("project".to_string(), "My Project".to_string().into());
        repo.set_default_commit_metadata(default_metadata.clone());

        let mut ds = repo.writable_session("main").await?;
        ds.add_group("/group".try_into().unwrap(), Bytes::new()).await?;
        let snapshot = ds.commit("commit 2").max_concurrent_nodes(8).execute().await?;

        let v = VersionInfo::SnapshotId(snapshot.clone());
        let snapshot_info = repo.ancestry(&v).await?;
        let snapshot_infos = snapshot_info.try_collect::<Vec<_>>().await?;
        assert_eq!(snapshot_infos[0].metadata, default_metadata);

        // Check that metadata is merged with users provided metadata taking precedence
        let mut metadata = SnapshotProperties::default();
        metadata.insert("author".to_string(), "Jane Doe".to_string().into());
        metadata.insert("id".to_string(), "ideded".to_string().into());
        let mut ds = repo.writable_session("main").await?;
        ds.add_group("/group2".try_into().unwrap(), Bytes::new()).await?;
        let snapshot = ds
            .commit("commit")
            .max_concurrent_nodes(8)
            .properties(metadata.clone())
            .execute()
            .await?;

        let v = VersionInfo::SnapshotId(snapshot.clone());
        let snapshot_info = repo.ancestry(&v).await?;
        let snapshot_infos = snapshot_info.try_collect::<Vec<_>>().await?;
        let mut expected_result = SnapshotProperties::default();
        expected_result.insert("author".to_string(), "Jane Doe".to_string().into());
        expected_result.insert("project".to_string(), "My Project".to_string().into());
        expected_result.insert("id".to_string(), "ideded".to_string().into());
        assert_eq!(snapshot_infos[0].metadata, expected_result);

        Ok(())
    }

    #[tokio_test]
    #[apply(spec_version_cases)]
    async fn test_repository_with_splits_and_resizes(
        #[case] spec_version: SpecVersionBin,
    ) -> Result<(), Box<dyn Error>> {
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
            Some(spec_version),
            true,
        )
        .await?;
        let mut session = repo.writable_session("main").await?;
        session.add_group(Path::root(), Bytes::copy_from_slice(b"")).await?;

        let array_path: Path = "/array".to_string().try_into().unwrap();
        let shape = ArrayShape::new(vec![(4, 4)]).unwrap();
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
            let payload = session.get_chunk_writer()?(bytes.clone()).await?;
            session
                .set_chunk_ref(array_path.clone(), ChunkIndices(vec![idx]), Some(payload))
                .await?;
        }
        let first_snapshot =
            session.commit("None").max_concurrent_nodes(8).execute().await?;
        let _session = repo
            .readonly_session(&VersionInfo::SnapshotId(first_snapshot.clone()))
            .await?;
        // 2 manifests from first commit + 1 new manifest after second commit modifies a split
        let initial_manifest_count = 3;

        let mut session = repo.writable_session("main").await?;
        // This is how Zarr resizes
        // first, delete any out of bounds chunks
        session.set_chunk_ref(array_path.clone(), ChunkIndices(vec![2]), None).await?;
        // second, update metadata
        let shape2 = ArrayShape::new(vec![(2, 2)]).unwrap();
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
        let payload = session.get_chunk_writer()?(bytes.clone()).await?;
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
        let _updated_snapshot =
            session.commit("updated").max_concurrent_nodes(8).execute().await?;

        // should still be deleted
        assert!(
            session.get_chunk_ref(&array_path, &ChunkIndices(vec![2])).await?.is_none()
        );
        // new ref should be present
        assert!(
            session.get_chunk_ref(&array_path, &ChunkIndices(vec![3])).await?.is_some()
        );

        assert_manifest_count(repo.asset_manager(), initial_manifest_count).await;

        // empty commit should not alter manifests
        let mut session = repo.writable_session("main").await?;
        let _empty_snapshot = session
            .commit("empty commit")
            .max_concurrent_nodes(8)
            .allow_empty(true)
            .execute()
            .await?;
        assert_manifest_count(repo.asset_manager(), initial_manifest_count).await;

        Ok(())
    }

    #[tokio_test]
    async fn test_repository_with_updates() -> Result<(), Box<dyn Error>> {
        let storage: Arc<dyn Storage + Send + Sync> = new_in_memory_storage().await?;
        let storage_settings = storage.default_settings().await?;
        let asset_manager = AssetManager::new_no_cache(
            Arc::clone(&storage),
            storage_settings.clone(),
            SpecVersionBin::current(),
            1,
            100,
        );

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

        let manifest = Manifest::from_iter(
            &ManifestId::random(),
            vec![chunk1.clone(), chunk2.clone()],
            None,
        )
        .await?
        .unwrap();
        let manifest = Arc::new(manifest);
        let manifest_id = manifest.id();
        let manifest_size = asset_manager.write_manifest(Arc::clone(&manifest)).await?;

        let shape = ArrayShape::new(vec![(2, 2), (2, 2), (2, 2)]).unwrap();
        let dimension_names = Some(vec!["x".into(), "y".into(), "t".into()]);

        let manifest_ref = ManifestRef {
            object_id: manifest_id.clone(),
            extents: ManifestExtents::new(&[0, 0, 0], &[1, 1, 2]),
        };

        let group_def = Bytes::from_static(br#"{"some":"group"}"#);
        let array_def = Bytes::from_static(br#"{"this":"array"}"#);

        let array1_path: Path = "/array1".try_into().unwrap();
        let node_id = NodeId::random();
        let nodes = [
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

        let initial = Snapshot::initial(SpecVersionBin::current()).unwrap();
        let manifests = vec![ManifestFileInfo::new(manifest.as_ref(), manifest_size)];
        let snapshot = Arc::new(Snapshot::from_iter(
            None,
            None,
            SpecVersionBin::current(),
            "message",
            None,
            manifests,
            None,
            nodes.iter().cloned().map(Ok::<NodeSnapshot, IcechunkFormatError>),
        )?);
        asset_manager.write_snapshot(Arc::clone(&snapshot)).await?;
        // FIXME:
        // update_branch(
        //     storage.as_ref(),
        //     &storage_settings,
        //     "main",
        //     snapshot.id().clone(),
        //     None,
        // )
        // .await?;
        Repository::store_config(
            Arc::clone(&storage),
            &RepositoryConfig::default(),
            &storage::VersionInfo::for_creation(),
        )
        .await?;
        let repo_info = RepoInfo::initial(
            SpecVersionBin::current(),
            (&initial).try_into()?,
            100,
            None::<&()>,
            None,
        )
        .add_snapshot(
            SpecVersionBin::current(),
            snapshot.as_ref().try_into()?,
            Some("main"),
            UpdateType::NewCommitUpdate {
                branch: "main".to_string(),
                new_snap_id: snapshot.id().clone(),
            },
            None,
            "backup_path",
            100,
        )?;
        asset_manager.create_repo_info(Arc::new(repo_info)).await?;

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

        let shape2 = ArrayShape::new(vec![(2, 1)]).unwrap();
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
        let shape3 = ArrayShape::new(vec![(4, 2)]).unwrap();
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

        let payload = ds.get_chunk_writer()?(Bytes::copy_from_slice(b"foo")).await?;
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
        let payload = ds.get_chunk_writer()?(data.clone()).await?;
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
        let shape4 = ArrayShape::new(vec![(6, 2)]).unwrap();
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
        let payload = ds.get_chunk_writer()?(data.clone()).await?;
        ds.set_chunk_ref(new_array_path.clone(), ChunkIndices(vec![0]), Some(payload))
            .await?;
        let data = Bytes::copy_from_slice(b"new".repeat(512).as_slice());
        let payload = ds.get_chunk_writer()?(data.clone()).await?;
        ds.set_chunk_ref(new_array_path.clone(), ChunkIndices(vec![1]), Some(payload))
            .await?;

        let chunk = get_chunk(
            ds.get_chunk_reader(&new_array_path, &ChunkIndices(vec![1]), &ByteRange::ALL)
                .await
                .unwrap(),
        )
        .await?;
        assert_eq!(chunk, Some(data.clone()));

        ds.commit("commit")
            .max_concurrent_nodes(8)
            .properties(SnapshotProperties::default())
            .execute()
            .await?;

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
    #[apply(spec_version_cases)]
    async fn test_repository_with_updates_and_writes(
        #[case] spec_version: SpecVersionBin,
    ) -> Result<(), Box<dyn Error>> {
        let backend: Arc<dyn Storage + Send + Sync> = new_in_memory_storage().await?;

        let logging = Arc::new(LoggingStorage::new(Arc::clone(&backend)));
        let logging_c = Arc::clone(&logging);
        let logging_c: Arc<dyn Storage + Send + Sync> = logging_c;
        let storage = Arc::clone(&logging_c);

        let config = RepositoryConfig {
            inline_chunk_threshold_bytes: Some(0),
            ..Default::default()
        };
        let repository = Repository::create(
            Some(config),
            storage,
            HashMap::new(),
            Some(spec_version),
            true,
        )
        .await?;

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

        let first_commit = ds
            .commit("commit")
            .max_concurrent_nodes(8)
            .properties(SnapshotProperties::default())
            .execute()
            .await?;

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
        let _snapshot_id = ds
            .commit("commit")
            .max_concurrent_nodes(8)
            .properties(SnapshotProperties::default())
            .execute()
            .await?;

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

        let shape = ArrayShape::new([(1, 1), (1, 1), (2, 2)]).unwrap();
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
        let _snapshot_id = ds
            .commit("commit")
            .max_concurrent_nodes(8)
            .properties(SnapshotProperties::default())
            .execute()
            .await?;

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
        ds.discard_changes()?;
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

        let _snapshot_id = ds
            .commit("commit")
            .max_concurrent_nodes(8)
            .properties(SnapshotProperties::default())
            .execute()
            .await?;

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

        let previous_snapshot_id =
            ds.commit("commit").max_concurrent_nodes(8).execute().await?;

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

        let snapshot_id = ds.commit("commit").max_concurrent_nodes(8).execute().await?;

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

        let ops =
            Vec::from_iter(logging.fetch_operations().into_iter().filter(|(op, key)| {
                op == "get_object_range"
                    && (key.starts_with("snapshots") || key.starts_with("manifests"))
            }));
        assert_eq!(ops.len(), 0);

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

        repository.save_config().await?;

        if spec_version == SpecVersionBin::V2 {
            let overwritten = repository
                .asset_manager()
                .list_overwritten_objects()
                .await?
                .try_collect::<Vec<_>>()
                .await?;

            // 6 commits + 1 config change recorded in ops log
            assert_eq!(overwritten.iter().filter(|s| s.starts_with("repo")).count(), 7);

            // V2 repos don't write config.yaml — config is in repo info
            assert_eq!(
                overwritten.iter().filter(|s| s.starts_with("config.yaml")).count(),
                0
            );
        }
        Ok(())
    }

    #[tokio_test]
    #[apply(spec_version_cases)]
    async fn test_basic_delete_and_flush(
        #[case] spec_version: SpecVersionBin,
    ) -> Result<(), Box<dyn Error>> {
        let repository = create_memory_store_repository(spec_version).await;
        let mut ds = repository.writable_session("main").await?;
        ds.add_group(Path::root(), Bytes::copy_from_slice(b"")).await?;
        ds.add_group("/1".try_into().unwrap(), Bytes::copy_from_slice(b"")).await?;
        ds.delete_group("/1".try_into().unwrap()).await?;
        assert_eq!(ds.list_nodes(&Path::root()).await?.count(), 1);
        ds.commit("commit").max_concurrent_nodes(8).execute().await?;

        let ds = repository
            .readonly_session(&VersionInfo::BranchTipRef("main".to_string()))
            .await?;
        assert!(ds.get_group(&Path::root()).await.is_ok());
        assert!(ds.get_group(&"/1".try_into().unwrap()).await.is_err());
        assert_eq!(ds.list_nodes(&Path::root()).await?.count(), 1);
        Ok(())
    }

    #[tokio_test]
    #[apply(spec_version_cases)]
    async fn test_basic_delete_after_flush(
        #[case] spec_version: SpecVersionBin,
    ) -> Result<(), Box<dyn Error>> {
        let repository = create_memory_store_repository(spec_version).await;
        let mut ds = repository.writable_session("main").await?;
        ds.add_group(Path::root(), Bytes::copy_from_slice(b"")).await?;
        ds.add_group("/1".try_into().unwrap(), Bytes::copy_from_slice(b"")).await?;
        ds.commit("commit").max_concurrent_nodes(8).execute().await?;

        let mut ds = repository.writable_session("main").await?;
        ds.delete_group("/1".try_into().unwrap()).await?;
        assert!(ds.get_group(&Path::root()).await.is_ok());
        assert!(ds.get_group(&"/1".try_into().unwrap()).await.is_err());
        assert_eq!(ds.list_nodes(&Path::root()).await?.count(), 1);
        Ok(())
    }

    #[tokio_test]
    #[apply(spec_version_cases)]
    async fn test_commit_after_deleting_old_node(
        #[case] spec_version: SpecVersionBin,
    ) -> Result<(), Box<dyn Error>> {
        let repository = create_memory_store_repository(spec_version).await;
        let mut ds = repository.writable_session("main").await?;
        ds.add_group(Path::root(), Bytes::copy_from_slice(b"")).await?;
        ds.commit("commit").max_concurrent_nodes(8).execute().await?;

        let mut ds = repository.writable_session("main").await?;
        ds.delete_group(Path::root()).await?;
        ds.commit("commit").max_concurrent_nodes(8).execute().await?;

        let ds = repository
            .readonly_session(&VersionInfo::BranchTipRef("main".to_string()))
            .await?;
        assert_eq!(ds.list_nodes(&Path::root()).await?.count(), 0);
        Ok(())
    }

    #[tokio_test]
    #[apply(spec_version_cases)]
    async fn test_delete_children(
        #[case] spec_version: SpecVersionBin,
    ) -> Result<(), Box<dyn Error>> {
        let def = Bytes::copy_from_slice(b"");
        let repository = create_memory_store_repository(spec_version).await;
        let mut ds = repository.writable_session("main").await?;
        ds.add_group(Path::root(), def.clone()).await?;
        ds.commit("initialize").max_concurrent_nodes(8).execute().await?;

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
    #[apply(spec_version_cases)]
    async fn test_delete_children_of_old_nodes(
        #[case] spec_version: SpecVersionBin,
    ) -> Result<(), Box<dyn Error>> {
        let repository = create_memory_store_repository(spec_version).await;
        let mut ds = repository.writable_session("main").await?;
        let def = Bytes::copy_from_slice(b"");
        ds.add_group(Path::root(), def.clone()).await?;
        ds.add_group("/a".try_into().unwrap(), def.clone()).await?;
        ds.add_group("/b".try_into().unwrap(), def.clone()).await?;
        ds.add_group("/b/bb".try_into().unwrap(), def.clone()).await?;
        ds.commit("commit").max_concurrent_nodes(8).execute().await?;

        let mut ds = repository.writable_session("main").await?;
        ds.delete_group("/b".try_into().unwrap()).await?;
        assert!(ds.get_group(&"/b".try_into().unwrap()).await.is_err());
        assert!(ds.get_group(&"/b/bb".try_into().unwrap()).await.is_err());
        Ok(())
    }

    #[tokio_test(flavor = "multi_thread")]
    #[apply(spec_version_cases)]
    async fn test_all_chunks_iterator(
        #[case] spec_version: SpecVersionBin,
    ) -> Result<(), Box<dyn Error>> {
        let storage: Arc<dyn Storage + Send + Sync> = new_in_memory_storage().await?;
        let repo =
            Repository::create(None, storage, HashMap::new(), Some(spec_version), true)
                .await?;
        let mut ds = repo.writable_session("main").await?;
        let def = Bytes::copy_from_slice(b"");

        // add a new array and retrieve its node
        ds.add_group(Path::root(), def.clone()).await?;

        let shape = ArrayShape::new(vec![(4, 2), (2, 2), (4, 2)]).unwrap();
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
        let snapshot_id = ds.commit("commit").max_concurrent_nodes(8).execute().await?;
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
    #[apply(spec_version_cases)]
    async fn test_manifests_shrink(
        #[case] spec_version: SpecVersionBin,
    ) -> Result<(), Box<dyn Error>> {
        let in_mem_storage = Arc::new(ObjectStorage::new_in_memory().await?);
        let storage = Arc::clone(&in_mem_storage);
        let storage: Arc<dyn Storage + Send + Sync> = storage;
        let repo = Repository::create(
            None,
            Arc::clone(&storage),
            HashMap::new(),
            Some(spec_version),
            true,
        )
        .await?;

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

        let shape = ArrayShape::new(vec![(5, 3), (5, 5)]).unwrap();
        let dimension_names = Some(vec!["t".into()]);

        ds.add_group(Path::root(), def.clone()).await?;
        let a1path: Path = "/array1".try_into()?;
        let a2path: Path = "/array2".try_into()?;

        ds.add_array(a1path.clone(), shape.clone(), dimension_names.clone(), def.clone())
            .await?;
        ds.add_array(a2path.clone(), shape.clone(), dimension_names.clone(), def.clone())
            .await?;

        let _ = ds.commit("first commit").max_concurrent_nodes(8).execute().await?;

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

        let _snap_id = ds.commit("commit").max_concurrent_nodes(8).execute().await?;

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
        let _snap_id =
            ds.commit("array2 deleted").max_concurrent_nodes(8).execute().await?;

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
        let _snap_id =
            ds.commit("chunk deleted").max_concurrent_nodes(8).execute().await?;

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
        let _snap_id =
            ds.commit("chunk deleted").max_concurrent_nodes(8).execute().await?;

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
    #[apply(spec_version_cases)]
    async fn test_commit_and_refs(
        #[case] spec_version: SpecVersionBin,
    ) -> Result<(), Box<dyn Error>> {
        let repo = create_memory_store_repository(spec_version).await;
        let mut ds = repo.writable_session("main").await?;

        let def = Bytes::copy_from_slice(b"");

        // add a new array and retrieve its node
        ds.add_group(Path::root(), def.clone()).await?;
        let new_snapshot_id =
            ds.commit("first commit").max_concurrent_nodes(8).execute().await?;
        assert_eq!(new_snapshot_id, repo.lookup_branch("main").await?);
        assert_eq!(&new_snapshot_id, ds.snapshot_id());

        repo.create_tag("v1", &new_snapshot_id).await?;
        let s = repo.lookup_tag("v1").await?;
        assert_eq!(new_snapshot_id, s);

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

        let shape = ArrayShape::new(vec![(1, 1), (2, 2), (4, 2)]).unwrap();
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
        let new_snapshot_id =
            ds.commit("second commit").max_concurrent_nodes(8).execute().await?;
        assert_eq!(new_snapshot_id, repo.lookup_branch("main").await?);

        let parents = repo
            .ancestry(&VersionInfo::SnapshotId(new_snapshot_id))
            .await?
            .try_collect::<Vec<_>>()
            .await?;
        assert_eq!(parents[0].message, "second commit");
        assert_eq!(parents[1].message, "first commit");
        assert_eq!(parents[2].message, Snapshot::INITIAL_COMMIT_MESSAGE);
        assert_eq!(parents[2].id, Snapshot::INITIAL_SNAPSHOT_ID);
        assert_equal(
            parents.iter().sorted_by_key(|m| m.flushed_at).rev(),
            parents.iter(),
        );

        Ok(())
    }

    #[tokio_test]
    async fn test_amend() -> Result<(), Box<dyn Error>> {
        let repo = create_memory_store_repository(SpecVersionBin::current()).await;

        let mut session = repo.writable_session("main").await?;
        session.add_group(Path::root(), Bytes::copy_from_slice(b"")).await?;
        let amend_result = session
            .commit("cannot amend initial commit")
            .max_concurrent_nodes(8)
            .amend()
            .execute()
            .await;
        assert!(amend_result.is_err());
        assert!(amend_result.unwrap_err().to_string().contains("first commit"));

        let mut session = repo.writable_session("main").await?;
        let amend_result = session
            .commit("cannot amend initial commit")
            .max_concurrent_nodes(8)
            .amend()
            .allow_empty(true)
            .execute()
            .await;
        assert!(amend_result.is_err());
        assert!(amend_result.unwrap_err().to_string().contains("first commit"));

        // Now make a proper first commit
        let mut session = repo.writable_session("main").await?;
        session.add_group(Path::root(), Bytes::copy_from_slice(b"")).await?;
        let snap1 = session.commit("make root").max_concurrent_nodes(8).execute().await?;

        let mut session = repo.writable_session("main").await?;
        session.add_group("/a".try_into().unwrap(), Bytes::copy_from_slice(b"")).await?;
        let before_amend1 =
            session.commit("will be amended").max_concurrent_nodes(8).execute().await?;
        let mut session = repo.writable_session("main").await?;
        session.add_group("/b".try_into().unwrap(), Bytes::copy_from_slice(b"")).await?;
        let before_amend2 = session
            .commit("first amend")
            .max_concurrent_nodes(8)
            .amend()
            .execute()
            .await?;

        let main_version = VersionInfo::BranchTipRef("main".to_string());
        let anc: Vec<_> = repo
            .ancestry(&main_version)
            .await?
            .map_ok(|si| si.message)
            .try_collect()
            .await?;

        // the amended commit is not in the history
        assert_eq!(anc, vec!["first amend", "make root", "Repository initialized"]);

        let session = repo.readonly_session(&main_version).await?;
        assert!(session.get_group(&Path::root()).await.is_ok());
        assert!(session.get_group(&"/a".try_into().unwrap()).await.is_ok());
        assert!(session.get_group(&"/b".try_into().unwrap()).await.is_ok());
        assert_eq!(session.list_nodes(&Path::root()).await?.count(), 3);

        let last =
            repo.resolve_version(&VersionInfo::BranchTipRef("main".to_string())).await?;

        repo.create_tag("tag", &last).await?;

        let mut session = repo.writable_session("main").await?;
        session
            .add_group("/error".try_into().unwrap(), Bytes::copy_from_slice(b""))
            .await?;
        let after_amend2 = session
            .commit("second amend")
            .max_concurrent_nodes(8)
            .amend()
            .execute()
            .await?;

        let anc_from_tag: Vec<_> = repo
            .ancestry(&VersionInfo::TagRef("tag".to_string()))
            .await?
            .map_ok(|si| si.message)
            .try_collect()
            .await?;
        assert_eq!(
            anc_from_tag,
            vec!["first amend", "make root", "Repository initialized"]
        );

        let anc_from_main: Vec<_> = repo
            .ancestry(&main_version)
            .await?
            .map_ok(|si| si.message)
            .try_collect()
            .await?;
        assert_eq!(
            anc_from_main,
            vec!["second amend", "make root", "Repository initialized"]
        );
        let updates = repo
            .ops_log()
            .await?
            .0
            .map_ok(|(_, up, _)| up)
            .try_collect::<Vec<_>>()
            .await?;

        use UpdateType::*;
        assert_eq!(
            updates,
            vec![
                CommitAmendedUpdate {
                    branch: "main".to_string(),
                    previous_snap_id: before_amend2.clone(),
                    new_snap_id: after_amend2.clone(),
                },
                TagCreatedUpdate { name: "tag".to_string() },
                CommitAmendedUpdate {
                    branch: "main".to_string(),
                    previous_snap_id: before_amend1.clone(),
                    new_snap_id: before_amend2.clone(),
                },
                NewCommitUpdate {
                    branch: "main".to_string(),
                    new_snap_id: before_amend1
                },
                NewCommitUpdate { branch: "main".to_string(), new_snap_id: snap1 },
                RepoInitializedUpdate,
            ]
        );

        Ok(())
    }

    #[tokio_test]
    async fn test_session_amending_with_move() -> Result<(), Box<dyn Error>> {
        let repo = create_memory_store_repository(SpecVersionBin::current()).await;

        let mut session = repo.writable_session("main").await?;
        session.add_group(Path::root(), Bytes::copy_from_slice(b"")).await?;
        session
            .add_group("/source".try_into().unwrap(), Bytes::copy_from_slice(b""))
            .await?;
        session.commit("setup").max_concurrent_nodes(8).execute().await?;

        // Rearrange session, only has a move: should be fine
        let mut session = repo.rearrange_session("main").await?;
        session
            .move_node("/source".try_into().unwrap(), "/dest".try_into().unwrap())
            .await?;
        session.commit("move commit").max_concurrent_nodes(8).execute().await?;

        // Amend on top of a move commit: should fail
        let mut session = repo.writable_session("main").await?;
        session
            .add_group("/fail".try_into().unwrap(), Bytes::copy_from_slice(b""))
            .await?;
        let result = session
            .commit("amend after move")
            .max_concurrent_nodes(8)
            .amend()
            .execute()
            .await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err.kind, SessionErrorKind::RearrangeSessionOnly));

        // Rearrange session, amend on top of a move commit: should be fine
        let mut session = repo.rearrange_session("main").await?;
        session
            .move_node("/dest".try_into().unwrap(), "/another_dest".try_into().unwrap())
            .await?;
        let result = session
            .commit("amend after move, only new moves")
            .max_concurrent_nodes(8)
            .amend()
            .execute()
            .await;
        assert!(result.is_ok());
        let snapshot_id = result.unwrap();

        // Check if moved group still shows up properly after amending two rearrange sessions
        let session =
            repo.readonly_session(&VersionInfo::SnapshotId(snapshot_id)).await?;
        assert!(session.get_group(&"/another_dest".try_into().unwrap()).await.is_ok());

        // Add nested groups, try to move them
        let mut session = repo.writable_session("main").await?;
        session
            .add_group("/another_dest/1".try_into().unwrap(), Bytes::copy_from_slice(b""))
            .await?;
        session
            .add_group(
                "/another_dest/1/2".try_into().unwrap(),
                Bytes::copy_from_slice(b""),
            )
            .await?;
        session
            .add_group(
                "/another_dest/1/2/3".try_into().unwrap(),
                Bytes::copy_from_slice(b""),
            )
            .await?;
        session.commit("add nested groups").max_concurrent_nodes(8).execute().await?;

        let mut session = repo.rearrange_session("main").await?;
        session
            .move_node(
                "/another_dest/1/2".try_into().unwrap(),
                "/nested_move".try_into().unwrap(),
            )
            .await?;
        session
            .move_node(
                "/another_dest".try_into().unwrap(),
                "/new_dest".try_into().unwrap(),
            )
            .await?;
        let snapshot_id = session
            .commit("move nested groups")
            .max_concurrent_nodes(8)
            .execute()
            .await?;

        // Check if moved nested groups still shows up properly after commit
        let session =
            repo.readonly_session(&VersionInfo::SnapshotId(snapshot_id)).await?;
        assert!(session.get_group(&"/new_dest".try_into().unwrap()).await.is_ok());
        assert!(session.get_group(&"/new_dest/1".try_into().unwrap()).await.is_ok());
        assert!(session.get_group(&"/nested_move".try_into().unwrap()).await.is_ok());
        assert!(session.get_group(&"/nested_move/3".try_into().unwrap()).await.is_ok());

        let mut session = repo.rearrange_session("main").await?;
        session
            .move_node(
                "/nested_move".try_into().unwrap(),
                "/moved_again".try_into().unwrap(),
            )
            .await?;
        let snapshot_id = session
            .commit("amend nested groups move ")
            .max_concurrent_nodes(8)
            .amend()
            .execute()
            .await?;

        // Check if moved nested groups still shows up properly after amend
        let session =
            repo.readonly_session(&VersionInfo::SnapshotId(snapshot_id)).await?;
        assert!(session.get_group(&"/new_dest".try_into().unwrap()).await.is_ok());
        assert!(session.get_group(&"/new_dest/1".try_into().unwrap()).await.is_ok());
        assert!(session.get_group(&"/moved_again".try_into().unwrap()).await.is_ok());
        assert!(session.get_group(&"/moved_again/3".try_into().unwrap()).await.is_ok());

        Ok(())
    }

    /// Integration test that `fetch_snapshot_info` correctly identifies initial vs non-initial.
    #[tokio_test]
    #[apply(spec_version_cases)]
    async fn test_fetch_snapshot_info_is_initial(
        #[case] spec_version: SpecVersionBin,
    ) -> Result<(), Box<dyn Error>> {
        let repo = create_memory_store_repository(spec_version).await;
        let asset_manager = repo.asset_manager();

        // Look up the initial snapshot via the branch
        let initial_snap_id = repo.lookup_branch("main").await?;
        let initial_info = asset_manager.fetch_snapshot_info(&initial_snap_id).await?;
        assert!(initial_info.is_initial());

        // Non-initial snapshot should NOT be marked as initial
        let mut session = repo.writable_session("main").await?;
        session.add_group(Path::root(), Bytes::copy_from_slice(b"")).await?;
        let snap1 =
            session.commit("first commit").max_concurrent_nodes(8).execute().await?;

        let snap1_info = asset_manager.fetch_snapshot_info(&snap1).await?;
        assert!(!snap1_info.is_initial());

        Ok(())
    }

    /// Test that the initial snapshot has an empty transaction log that can be fetched.
    #[tokio_test]
    #[apply(spec_version_cases)]
    async fn test_initial_snapshot_has_empty_transaction_log(
        #[case] spec_version: SpecVersionBin,
    ) -> Result<(), Box<dyn Error>> {
        let repo = create_memory_store_repository(spec_version).await;

        if spec_version == SpecVersionBin::V1 {
            // Transaction logs for initial commit are a V2-only feature
            let initial_snap_id = repo.lookup_branch("main").await?;
            let result =
                repo.asset_manager().fetch_transaction_log(&initial_snap_id).await;
            assert!(result.is_err());
            return Ok(());
        }

        let asset_manager = repo.asset_manager();

        let initial_snap_id = repo.lookup_branch("main").await?;
        let tx_log = asset_manager.fetch_transaction_log(&initial_snap_id).await?;

        // The transaction log should exist and be empty (no changes)
        assert_eq!(tx_log.new_groups().count(), 0);
        assert_eq!(tx_log.new_arrays().count(), 0);
        assert_eq!(tx_log.deleted_groups().count(), 0);
        assert_eq!(tx_log.deleted_arrays().count(), 0);
        assert_eq!(tx_log.updated_groups().count(), 0);
        assert_eq!(tx_log.updated_arrays().count(), 0);
        assert_eq!(tx_log.updated_chunks().count(), 0);
        assert_eq!(tx_log.moves().count(), 0);

        // Diff from initial snapshot to first real commit should show the new group
        let mut session = repo.writable_session("main").await?;
        session.add_group(Path::root(), Bytes::copy_from_slice(b"")).await?;
        let snap1 =
            session.commit("first commit").max_concurrent_nodes(8).execute().await?;

        let diff = repo
            .diff(
                &VersionInfo::SnapshotId(initial_snap_id),
                &VersionInfo::SnapshotId(snap1),
            )
            .await?;
        assert!(!diff.is_empty());
        assert_eq!(&diff.new_groups, &[Path::root()].into());
        assert!(diff.new_arrays.is_empty());
        assert!(diff.deleted_groups.is_empty());
        assert!(diff.deleted_arrays.is_empty());
        assert!(diff.updated_groups.is_empty());
        assert!(diff.updated_arrays.is_empty());
        assert!(diff.updated_chunks.is_empty());

        Ok(())
    }

    #[tokio_test]
    #[apply(spec_version_cases)]
    async fn test_empty_commit(
        #[case] spec_version: SpecVersionBin,
    ) -> Result<(), Box<dyn Error>> {
        let repo = create_memory_store_repository(spec_version).await;
        let mut session = repo.writable_session("main").await?;
        session.add_group(Path::root(), Bytes::copy_from_slice(b"")).await?;
        let snap1 = session.commit("make root").max_concurrent_nodes(8).execute().await?;

        let mut session = repo.writable_session("main").await?;
        let result =
            session.commit("an empty commit").max_concurrent_nodes(8).execute().await;
        assert!(matches!(
            result,
            Err(SessionError { kind: SessionErrorKind::NoChangesToCommit, .. })
        ));

        let mut session = repo.writable_session("main").await?;
        let snap2 = session
            .commit("an empty commit")
            .max_concurrent_nodes(8)
            .allow_empty(true)
            .execute()
            .await?;
        let snap2_info = repo.lookup_snapshot(&snap2).await?;
        assert_eq!(snap2_info.parent_id, Some(snap1.clone()));

        let diff = repo
            .diff(
                &VersionInfo::SnapshotId(snap1.clone()),
                &VersionInfo::SnapshotId(snap2.clone()),
            )
            .await?;
        assert!(diff.is_empty());

        // Verify ancestry includes both commits (plus initial snapshot)
        let ancestry: Vec<_> = repo
            .ancestry(&VersionInfo::SnapshotId(snap2.clone()))
            .await?
            .map_ok(|si| si.id)
            .try_collect()
            .await?;
        assert_eq!(ancestry, vec![snap2, snap1, Snapshot::INITIAL_SNAPSHOT_ID]);

        Ok(())
    }

    #[tokio_test]
    #[apply(spec_version_cases)]
    async fn test_no_double_commit(
        #[case] spec_version: SpecVersionBin,
    ) -> Result<(), Box<dyn Error>> {
        let repository = create_memory_store_repository(spec_version).await;

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
            ds1.commit("from 1").max_concurrent_nodes(8).execute().await
        });

        let handle2 = tokio::spawn(async move {
            let _ = barrier_cc.wait().await;
            ds2.commit("from 2").max_concurrent_nodes(8).execute().await
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
    async fn test_basic_move() -> Result<(), Box<dyn Error>> {
        let in_mem_storage = new_in_memory_storage().await?;
        let storage = Arc::clone(&in_mem_storage);
        let storage: Arc<dyn Storage + Send + Sync> = storage;
        let repo = Repository::create(
            None,
            Arc::clone(&storage),
            HashMap::new(),
            Some(SpecVersionBin::current()),
            true,
        )
        .await?;
        let mut session = repo.writable_session("main").await?;

        let shape = ArrayShape::new(vec![(5, 3), (5, 3)]).unwrap();
        session.add_group(Path::root(), Bytes::new()).await?;
        session.add_group(Path::new("/foo/old").unwrap(), Bytes::new()).await?;
        let apath: Path = "/foo/old/array".try_into()?;
        session.add_array(apath.clone(), shape, None, Bytes::new()).await?;
        session.commit("first commit").max_concurrent_nodes(8).execute().await?;

        let mut session = repo.rearrange_session("main").await?;
        session
            .move_node(Path::new("/foo/old").unwrap(), Path::new("/foo/new").unwrap())
            .await?;

        assert_eq!(
            session.get_node(&Path::new("/").unwrap()).await?.path.to_string(),
            "/"
        );

        assert_eq!(
            session
                .get_node(&Path::new("/foo/new/array").unwrap())
                .await?
                .path
                .to_string(),
            "/foo/new/array"
        );
        assert!(session.get_node(&Path::new("/foo/old/array").unwrap()).await.is_err());

        let mut nodes: Vec<_> =
            session.list_nodes(&Path::root()).await?.map(|n| n.unwrap().path).collect();
        nodes.sort();
        assert_equal(
            nodes.into_iter(),
            [
                Path::new("/").unwrap(),
                Path::new("/foo/new").unwrap(),
                Path::new("/foo/new/array").unwrap(),
            ],
        );

        session.commit("moved").max_concurrent_nodes(8).execute().await?;

        let session =
            repo.readonly_session(&VersionInfo::BranchTipRef("main".to_string())).await?;

        assert_eq!(
            session
                .get_node(&Path::new("/foo/new/array").unwrap())
                .await?
                .path
                .to_string(),
            "/foo/new/array"
        );
        Ok(())
    }

    #[tokio_test]
    async fn test_move_into_then_move_parent() -> Result<(), Box<dyn Error>> {
        // When a node is moved INTO a subtree and then that subtree is moved,
        // the moved-in node must follow along.
        //
        // Start:
        // /
        // ├── a        (G)
        // │   └── 0    [A]
        // └── c        (G)
        //     └── 0    [A]
        //
        // Move 1: /a -> /b
        // Move 2: /c -> /a
        // Move 3: /b/0 -> /a/1
        // Move 4: /a -> /d
        //
        // After:
        // /
        // ├── b        (G)   # was /a
        // └── d        (G)   # was /c
        //     ├── 0    [A]   # was /c/0
        //     └── 1    [A]   # was /a/0
        fn p(s: &str) -> Path {
            Path::new(s).unwrap()
        }

        let repo = create_memory_store_repository(SpecVersionBin::current()).await;
        let mut session = repo.writable_session("main").await?;
        let shape = ArrayShape::new(vec![(2, 2)]).unwrap();
        session.add_group(Path::root(), Bytes::new()).await?;
        session.add_group(p("/a"), Bytes::new()).await?;
        session.add_array(p("/a/0"), shape.clone(), None, Bytes::new()).await?;
        session.add_group(p("/c"), Bytes::new()).await?;
        session.add_array(p("/c/0"), shape, None, Bytes::new()).await?;
        session.commit("init").max_concurrent_nodes(8).execute().await?;

        let mut session = repo.rearrange_session("main").await?;
        session.move_node(p("/a"), p("/b")).await?;
        session.move_node(p("/c"), p("/a")).await?;
        session.move_node(p("/b/0"), p("/a/1")).await?;
        session.move_node(p("/a"), p("/d")).await?;

        // Verify the tree matches the ASCII art above
        let mut nodes: Vec<_> =
            session.list_nodes(&Path::root()).await?.map(|n| n.unwrap().path).collect();
        nodes.sort();
        assert_equal(nodes.into_iter(), [p("/"), p("/b"), p("/d"), p("/d/0"), p("/d/1")]);

        Ok(())
    }

    #[tokio_test]
    async fn test_move_errors() -> Result<(), Box<dyn Error>> {
        let in_mem_storage = new_in_memory_storage().await?;
        let storage = Arc::clone(&in_mem_storage);
        let storage: Arc<dyn Storage + Send + Sync> = storage;
        let repo = Repository::create(
            None,
            Arc::clone(&storage),
            HashMap::new(),
            Some(SpecVersionBin::current()),
            true,
        )
        .await?;
        let mut session = repo.writable_session("main").await?;

        let shape = ArrayShape::new(vec![(5, 3), (5, 3)]).unwrap();
        session.add_group(Path::root(), Bytes::new()).await?;
        let apath: Path = "/foo/old/array".try_into()?;
        session.add_array(apath.clone(), shape, None, Bytes::new()).await?;
        session.commit("first commit").max_concurrent_nodes(8).execute().await?;

        let mut session = repo.rearrange_session("main").await?;
        assert!(matches!(
                session
                    .move_node(Path::new("/foo/old/array").unwrap(), Path::new("/foo/old/array").unwrap())
                    .await,
                Err(SessionError{kind: SessionErrorKind::MoveWontOverwrite(s), ..}) if s == "/foo/old/array"
        ));

        assert!(matches!(
                session
                    .move_node(Path::new("/foo/old/unknown").unwrap(), Path::new("/foo/bar").unwrap())
                    .await,
                Err(SessionError{kind: SessionErrorKind::NodeNotFound{path, ..}, ..}) if path == Path::new("/foo/old/unknown").unwrap()
        ));
        Ok(())
    }

    #[tokio_test]
    #[apply(spec_version_cases)]
    async fn test_setting_w_invalid_coords(
        #[case] spec_version: SpecVersionBin,
    ) -> Result<(), Box<dyn Error>> {
        let in_mem_storage = new_in_memory_storage().await?;
        let storage = Arc::clone(&in_mem_storage);
        let storage: Arc<dyn Storage + Send + Sync> = storage;
        let repo = Repository::create(
            None,
            Arc::clone(&storage),
            HashMap::new(),
            Some(spec_version),
            true,
        )
        .await?;
        let mut ds = repo.writable_session("main").await?;

        let shape = ArrayShape::new(vec![(5, 3), (5, 3)]).unwrap();
        ds.add_group(Path::root(), Bytes::new()).await?;

        let apath: Path = "/array1".try_into()?;

        ds.add_array(apath.clone(), shape, None, Bytes::new()).await?;

        ds.commit("first commit").max_concurrent_nodes(8).execute().await?;

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

    #[tokio_test]
    #[apply(spec_version_cases)]
    async fn test_array_shift(
        #[case] spec_version: SpecVersionBin,
    ) -> Result<(), Box<dyn Error>> {
        let in_mem_storage = new_in_memory_storage().await?;
        let storage = Arc::clone(&in_mem_storage);
        let storage: Arc<dyn Storage + Send + Sync> = storage;
        let repo = Repository::create(
            None,
            Arc::clone(&storage),
            HashMap::new(),
            Some(spec_version),
            true,
        )
        .await?;
        let mut session = repo.writable_session("main").await?;
        let shape = ArrayShape::new(vec![(20, 10)]).unwrap();
        session.add_group(Path::root(), Bytes::new()).await?;
        let apath: Path = "/array".try_into()?;
        session.add_array(apath.clone(), shape, None, Bytes::new()).await?;

        for chunk_index in 0..10 {
            session
                .set_chunk_ref(
                    apath.clone(),
                    ChunkIndices(vec![chunk_index]),
                    Some(ChunkPayload::Inline(chunk_index.to_string().into())),
                )
                .await?;
        }

        session.commit("first commit").max_concurrent_nodes(8).execute().await?;

        let mut session = repo.writable_session("main").await?;
        session.shift_array(&apath, &[-1]).await?;
        assert_eq!(
            session.get_chunk_ref(&apath, &ChunkIndices(vec![0])).await?,
            Some(ChunkPayload::Inline("1".into()))
        );
        for chunk_index in 0..=8 {
            let new_payload =
                session.get_chunk_ref(&apath, &ChunkIndices(vec![chunk_index])).await?;
            assert_eq!(
                new_payload,
                Some(ChunkPayload::Inline((chunk_index + 1).to_string().into()))
            );
        }

        assert_eq!(session.get_chunk_ref(&apath, &ChunkIndices(vec![9])).await?, None);
        Ok(())
    }

    #[tokio_test]
    #[apply(spec_version_cases)]
    async fn test_flush(
        #[case] spec_version: SpecVersionBin,
    ) -> Result<(), Box<dyn Error>> {
        let repository = create_memory_store_repository(spec_version).await;
        let mut session = repository.writable_session("main").await?;
        session.add_group(Path::root(), Bytes::copy_from_slice(b"")).await?;

        let path: Path = "/array".try_into().unwrap();
        session.add_array(path.clone(), basic_shape(), None, Bytes::new()).await?;
        session
            .set_chunk_ref(
                path.clone(),
                ChunkIndices(vec![0]),
                Some(ChunkPayload::Inline("hello".into())),
            )
            .await?;
        let meta: SnapshotProperties = [("test".to_string(), 42.into())].into();
        let snap_id = session
            .commit("flush")
            .max_concurrent_nodes(8)
            .anonymous()
            .properties(meta.clone())
            .execute()
            .await?;

        let chunk = get_chunk(
            session
                .get_chunk_reader(&path, &ChunkIndices(vec![0]), &ByteRange::ALL)
                .await?,
        )
        .await?;
        assert_eq!(chunk, Some(Bytes::from_static(br#"hello"#)));
        assert!(session.branch().is_none());
        assert!(session.change_set().is_empty());

        repository
            .reset_branch("main", &snap_id, Some(&Snapshot::INITIAL_SNAPSHOT_ID))
            .await?;
        let parents: Vec<_> = repository
            .ancestry(&VersionInfo::BranchTipRef("main".to_string()))
            .await?
            .try_collect()
            .await?;
        assert_eq!(parents.len(), 2);
        assert_eq!(&parents[0].id, &snap_id);
        assert_eq!(&parents[0].message, "flush");
        assert_eq!(&parents[0].metadata, &meta);
        assert_eq!(&parents[1].id, &Snapshot::INITIAL_SNAPSHOT_ID);

        let session = repository
            .readonly_session(&VersionInfo::BranchTipRef("main".to_string()))
            .await?;
        let chunk = get_chunk(
            session
                .get_chunk_reader(&path, &ChunkIndices(vec![0]), &ByteRange::ALL)
                .await?,
        )
        .await?;
        assert_eq!(chunk, Some(Bytes::from_static(br#"hello"#)));
        Ok(())
    }

    /// Construct two repos on the same storage, with a commit, a group and an array
    ///
    /// Group: /foo/bar
    /// Array: /foo/bar/some-array
    async fn get_repo_for_conflict() -> Result<Repository, Box<dyn Error>> {
        let repository = create_memory_store_repository(SpecVersionBin::current()).await;
        let mut ds = repository.writable_session("main").await?;

        ds.add_group("/foo/bar".try_into().unwrap(), Bytes::new()).await?;
        ds.add_array(
            "/foo/bar/some-array".try_into().unwrap(),
            basic_shape(),
            None,
            Bytes::new(),
        )
        .await?;
        ds.commit("create directory").max_concurrent_nodes(8).execute().await?;

        Ok(repository)
    }
    async fn get_sessions_for_conflict() -> Result<(Session, Session), Box<dyn Error>> {
        let repository = get_repo_for_conflict().await?;

        let ds = repository.writable_session("main").await?;
        let ds2 = repository.writable_session("main").await?;

        Ok((ds, ds2))
    }

    fn basic_shape() -> ArrayShape {
        ArrayShape::new(vec![(5, 5)]).unwrap()
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
        ds1.commit("create group").max_concurrent_nodes(8).execute().await?;

        ds2.add_array(conflict_path.clone(), basic_shape(), None, user_data()).await?;
        ds2.commit("create array").max_concurrent_nodes(8).execute().await.unwrap_err();
        assert_has_conflict(
            &Conflict::NewNodeConflictsWithExistingNode(conflict_path),
            ds2.rebase(&ConflictDetector).await,
        );
        Ok(())
    }

    #[tokio_test]
    /// Test that delete-then-recreate exactly the same node
    /// does NOT conflict
    ///
    /// This session: delete group + recreate group at same path
    /// Previous commit: add a different sibling group
    async fn test_no_conflict_on_delete_then_recreate() -> Result<(), Box<dyn Error>> {
        let (mut ds1, mut ds2) = get_sessions_for_conflict().await?;

        let path: Path = "/foo/bar".try_into().unwrap();
        ds1.add_group("/foo/quux".try_into().unwrap(), user_data()).await?;
        ds1.commit("add sibling group").max_concurrent_nodes(8).execute().await?;

        ds2.delete_group(path.clone()).await?;
        ds2.add_group(path.clone(), Bytes::new()).await?;
        assert!(matches!(
            ds2.commit("delete+re-add group").max_concurrent_nodes(8).execute().await,
            Err(SessionError {
                kind: SessionErrorKind::Conflict {
                    expected_parent, actual_parent
                }, ..
            }) if expected_parent != actual_parent
        ));

        ds2.rebase(&ConflictDetector).await?;
        ds2.commit("delete+re-add group").max_concurrent_nodes(8).execute().await?;

        Ok(())
    }

    #[tokio_test]
    /// Test that delete-then-recreate DOES conflict when the previous commit
    /// updated the group's metadata.
    ///
    /// This session: delete group + recreate group at same path
    /// Previous commit: updated the group's metadata
    async fn test_conflict_on_delete_then_recreate_when_group_updated()
    -> Result<(), Box<dyn Error>> {
        let (mut ds1, mut ds2) = get_sessions_for_conflict().await?;

        let path: Path = "/foo/bar".try_into().unwrap();
        ds1.update_group(&path, Bytes::from("updated")).await?;
        ds1.commit("update group metadata").max_concurrent_nodes(8).execute().await?;

        let node = ds2.get_node(&path).await.unwrap();
        ds2.delete_group(path.clone()).await?;
        ds2.add_group(path.clone(), user_data()).await?;
        assert!(matches!(
            ds2.commit("delete+re-add group").max_concurrent_nodes(8).execute().await,
            Err(SessionError {
                kind: SessionErrorKind::Conflict {
                    expected_parent, actual_parent
                }, ..
            }) if expected_parent != actual_parent
        ));

        assert_has_conflict(
            &Conflict::DeleteOfUpdatedGroup { path, node_id: node.id },
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
        ds1.commit("create array").max_concurrent_nodes(8).execute().await?;

        let inner_path: Path = "/foo/bar/conflict/inner".try_into().unwrap();
        ds2.add_array(inner_path.clone(), basic_shape(), None, user_data()).await?;
        ds2.commit("create inner array")
            .max_concurrent_nodes(8)
            .execute()
            .await
            .unwrap_err();
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
        ds1.commit("update array").max_concurrent_nodes(8).execute().await?;

        ds2.update_array(&path.clone(), basic_shape(), None, user_data()).await?;
        ds2.commit("update array again")
            .max_concurrent_nodes(8)
            .execute()
            .await
            .unwrap_err();
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
        ds1.commit("delete array").max_concurrent_nodes(8).execute().await?;

        ds2.update_array(&path.clone(), basic_shape(), None, user_data()).await?;
        ds2.commit("update array again")
            .max_concurrent_nodes(8)
            .execute()
            .await
            .unwrap_err();
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
        ds1.commit("update array").max_concurrent_nodes(8).execute().await?;

        let node = ds2.get_node(&path).await.unwrap();
        ds2.delete_array(path.clone()).await?;
        ds2.commit("delete array").max_concurrent_nodes(8).execute().await.unwrap_err();
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
        ds1.commit("update chunks").max_concurrent_nodes(8).execute().await?;

        let node = ds2.get_node(&path).await.unwrap();
        ds2.delete_array(path.clone()).await?;
        ds2.commit("delete array").max_concurrent_nodes(8).execute().await.unwrap_err();
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
        ds1.commit("update user attributes").max_concurrent_nodes(8).execute().await?;

        let node = ds2.get_node(&path).await.unwrap();
        ds2.delete_group(path.clone()).await?;
        ds2.commit("delete group").max_concurrent_nodes(8).execute().await.unwrap_err();
        assert_has_conflict(
            &Conflict::DeleteOfUpdatedGroup { path, node_id: node.id },
            ds2.rebase(&ConflictDetector).await,
        );
        Ok(())
    }

    #[tokio_test]
    #[apply(spec_version_cases)]
    async fn test_rebase_without_fast_forward(
        #[case] spec_version: SpecVersionBin,
    ) -> Result<(), Box<dyn Error>> {
        let repo = create_memory_store_repository(spec_version).await;

        let mut ds = repo.writable_session("main").await?;

        ds.add_group("/".try_into().unwrap(), user_data()).await?;

        let new_array_path: Path = "/array".try_into().unwrap();
        ds.add_array(new_array_path.clone(), basic_shape(), None, user_data()).await?;
        ds.commit("create array").max_concurrent_nodes(8).execute().await?;

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
        let conflicting_snap = ds1
            .commit("write two chunks with repo 1")
            .max_concurrent_nodes(8)
            .execute()
            .await?;

        ds2.set_chunk_ref(
            new_array_path.clone(),
            ChunkIndices(vec![0]),
            Some(ChunkPayload::Inline("hello".into())),
        )
        .await?;

        // verify we cannot commit
        if let Err(SessionError { kind: SessionErrorKind::Conflict { .. }, .. }) = ds2
            .commit("write one chunk with repo2")
            .max_concurrent_nodes(8)
            .execute()
            .await
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
    #[apply(spec_version_cases)]
    async fn test_rebase_fast_forwarding_over_chunk_writes(
        #[case] spec_version: SpecVersionBin,
    ) -> Result<(), Box<dyn Error>> {
        let repo = create_memory_store_repository(spec_version).await;

        let mut ds = repo.writable_session("main").await?;

        ds.add_group("/".try_into().unwrap(), user_data()).await?;
        let new_array_path: Path = "/array".try_into().unwrap();
        ds.add_array(new_array_path.clone(), basic_shape(), None, user_data()).await?;
        let _array_created_snap =
            ds.commit("create array").max_concurrent_nodes(8).execute().await?;

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

        let _conflicting_snap = ds1
            .commit("write two chunks with repo 1")
            .max_concurrent_nodes(8)
            .execute()
            .await?;

        // let's try to create a new commit, that conflicts with the previous one but writes to
        // different chunks
        ds2.set_chunk_ref(
            new_array_path.clone(),
            ChunkIndices(vec![2]),
            Some(ChunkPayload::Inline("hello2".into())),
        )
        .await?;
        if let Err(SessionError { kind: SessionErrorKind::Conflict { .. }, .. }) = ds2
            .commit("write one chunk with repo2")
            .max_concurrent_nodes(8)
            .execute()
            .await
        {
            let solver = BasicConflictSolver::default();
            // different chunks were written so this should fast forward
            ds2.rebase(&solver).await?;
            let snapshot =
                ds2.commit("after conflict").max_concurrent_nodes(8).execute().await?;
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

        let _ = repo.lookup_branch("main").await?;
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
        ds1.commit("update array").max_concurrent_nodes(8).execute().await?;

        ds2.delete_array(path.clone()).await?;
        ds2.commit("delete array").max_concurrent_nodes(8).execute().await.unwrap_err();

        ds2.rebase(&BasicConflictSolver::default()).await?;
        ds2.commit("after conflict").max_concurrent_nodes(8).execute().await?;

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
            ds1.commit(format!("update chunk {coord}").as_str())
                .max_concurrent_nodes(8)
                .execute()
                .await?;
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

        ds2.commit("update chunk on repo 2")
            .max_concurrent_nodes(8)
            .execute()
            .await
            .unwrap_err();

        let solver = BasicConflictSolver {
            on_chunk_conflict: VersionSelection::UseTheirs,
            ..Default::default()
        };

        ds2.rebase(&solver).await?;
        ds2.commit("after conflict").max_concurrent_nodes(8).execute().await?;
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
        let non_conflicting_snap = ds1
            .commit("updated non-conflict chunk")
            .max_concurrent_nodes(8)
            .execute()
            .await?;

        let mut ds1 = repo.writable_session("main").await?;
        ds1.set_chunk_ref(
            path.clone(),
            ChunkIndices(vec![0]),
            Some(ChunkPayload::Inline("repo 1".into())),
        )
        .await?;

        let conflicting_snap =
            ds1.commit("update chunk ref").max_concurrent_nodes(8).execute().await?;

        ds2.set_chunk_ref(
            path.clone(),
            ChunkIndices(vec![0]),
            Some(ChunkPayload::Inline("repo 2".into())),
        )
        .await?;

        ds2.commit("update chunk ref")
            .max_concurrent_nodes(8)
            .execute()
            .await
            .unwrap_err();
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

    #[icechunk_macros::tokio_test]
    // Test rebase over a commit with a resize.
    //
    // Error-triggering flow:
    // 1. Session A starts from snapshot S1 (array shape [5, 1])
    // 2. Splits are cached for the array with extent [0..5)
    // 3. Session A writes chunk [3]
    // 4. Meanwhile, another session resizes array to [20, 1] and writes chunk [10]
    // 5. Session A rebases to the new snapshot
    // 6. BUG: Session A's splits are NOT updated (still [0..5))
    // 7. Session A commits - during flush, verified_node_chunk_iterator filters
    //    parent chunks by extent, dropping chunk [10] because it's outside [0..5)
    // 8. Result: chunk [10] is lost
    //
    // With IC1, doing this correctly requires updating Session.splits during rebase to match the new
    // parent snapshot's array shapes.
    async fn test_rebase_over_resize() -> Result<(), Box<dyn Error>> {
        struct YoloSolver;
        #[async_trait]
        impl ConflictSolver for YoloSolver {
            async fn solve(
                &self,
                _previous_change: &TransactionLog,
                _previous_repo: &Session,
                current_changes: ChangeSet,
                _sccurrent_repo: &Session,
            ) -> SessionResult<ConflictResolution> {
                Ok(ConflictResolution::Patched(current_changes))
            }
        }

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
        ds1.commit("writer 1 updated non-conflict chunk")
            .max_concurrent_nodes(8)
            .execute()
            .await?;

        let mut ds1 = repo.writable_session("main").await?;
        ds1.update_array(
            &path,
            ArrayShape::new(vec![(20, 20)]).unwrap(),
            None,
            Bytes::new(),
        )
        .await?;
        // Write a chunk beyond the original extent [0..5) to trigger the bug
        ds1.set_chunk_ref(
            path.clone(),
            ChunkIndices(vec![10]),
            Some(ChunkPayload::Inline("repo 1 chunk 10".into())),
        )
        .await?;
        ds1.commit("writer 1 updates array size and adds chunk 10")
            .max_concurrent_nodes(8)
            .execute()
            .await?;

        // now set a chunk ref that is valid with both old and new shape.
        ds2.set_chunk_ref(
            path.clone(),
            ChunkIndices(vec![3]),
            Some(ChunkPayload::Inline("repo 2".into())),
        )
        .await?;
        ds2.commit("writer 2 writes chunk 0")
            .max_concurrent_nodes(8)
            .rebase(&YoloSolver, 1u16)
            .execute()
            .await?;

        let ds3 = repo.writable_session("main").await?;
        // All three chunks should be present: [1] and [10] from ds1, [3] from ds2
        for i in [1u32, 3, 10] {
            assert!(
                get_chunk(
                    ds3.get_chunk_reader(&path, &ChunkIndices(vec![i]), &ByteRange::ALL,)
                        .await?
                )
                .await?
                .is_some(),
                "chunk [{i}] should be present"
            );
        }
        Ok(())
    }

    #[tokio_test]
    #[apply(spec_version_cases)]
    /// Tests `commit_rebasing` retries the proper number of times when there are conflicts
    async fn test_commit_rebasing_attempts(
        #[case] spec_version: SpecVersionBin,
    ) -> Result<(), Box<dyn Error>> {
        let repo = Arc::new(create_memory_store_repository(spec_version).await);
        let mut session = repo.writable_session("main").await?;
        session
            .add_array("/array".try_into().unwrap(), basic_shape(), None, Bytes::new())
            .await?;
        session.commit("create array").max_concurrent_nodes(8).execute().await?;

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
        session2.commit("conflicting").max_concurrent_nodes(8).execute().await.unwrap();

        let attempts = Arc::new(AtomicU16::new(0));

        // after each rebase attempt we'll run this closure that creates a new conflict
        // the result should be that it can never commit, failing after the indicated number of
        // attempts
        let conflicting: RebaseHook = {
            let attempts = Arc::clone(&attempts);
            let repo = Arc::clone(&repo);
            Box::new(move |attempt| {
                let attempts = Arc::clone(&attempts);
                let repo = Arc::clone(&repo);
                Box::pin(async move {
                    attempts.fetch_add(1, Ordering::SeqCst);
                    assert_eq!(attempt, attempts.load(Ordering::SeqCst));

                    let mut s = repo.writable_session("main").await.unwrap();
                    s.set_chunk_ref(
                        "/array".try_into().unwrap(),
                        ChunkIndices(vec![2]),
                        Some(ChunkPayload::Inline("repo 1".into())),
                    )
                    .await
                    .unwrap();
                    s.commit("conflicting")
                        .max_concurrent_nodes(8)
                        .execute()
                        .await
                        .unwrap();
                })
            })
        };

        let res = session
            .commit("updated non-conflict chunk")
            .max_concurrent_nodes(8)
            .rebase(&ConflictDetector, 3)
            .after_rebase_hook(conflicting)
            .execute()
            .await;

        // It has to give up eventually
        assert!(matches!(
            res,
            Err(SessionError { kind: SessionErrorKind::Conflict { .. }, .. })
        ));

        // It has to rebase 3 times
        assert_eq!(Arc::try_unwrap(attempts).unwrap().into_inner(), 3);

        let attempts = Arc::new(AtomicU16::new(0));

        // now we'll create a new conflict twice, and finally do nothing so the commit can succeed
        let conflicting_twice: RebaseHook = {
            let attempts = Arc::clone(&attempts);
            let repo = Arc::clone(&repo);
            Box::new(move |attempt| {
                let attempts = Arc::clone(&attempts);
                let repo = Arc::clone(&repo);
                Box::pin(async move {
                    attempts.fetch_add(1, Ordering::SeqCst);
                    assert_eq!(attempt, attempts.load(Ordering::SeqCst));
                    if attempt <= 2 {
                        let mut s = repo.writable_session("main").await.unwrap();
                        s.set_chunk_ref(
                            "/array".try_into().unwrap(),
                            ChunkIndices(vec![2]),
                            Some(ChunkPayload::Inline("repo 1".into())),
                        )
                        .await
                        .unwrap();
                        s.commit("conflicting")
                            .max_concurrent_nodes(8)
                            .execute()
                            .await
                            .unwrap();
                    }
                })
            })
        };

        let res = session
            .commit("updated non-conflict chunk")
            .max_concurrent_nodes(8)
            .rebase(&ConflictDetector, 42)
            .after_rebase_hook(conflicting_twice)
            .execute()
            .await;

        // The commit has to work after 3 rebase attempts
        let snap_id = res.unwrap();
        let v = VersionInfo::SnapshotId(snap_id);
        let infos = repo.ancestry(&v).await?.try_collect::<Vec<_>>().await?;
        assert_eq!(
            infos[0].metadata.get("__icechunk"),
            Some(&serde_json::json!({ "rebase_attempts": 3 }))
        );
        assert_eq!(Arc::try_unwrap(attempts).unwrap().into_inner(), 3);
        Ok(())
    }

    #[tokio_test]
    async fn manifest_files_consistency() -> Result<(), Box<dyn Error>> {
        let repo = Arc::new(create_memory_store_repository(SpecVersionBin::V2).await);
        let mut session = repo.writable_session("main").await?;
        session
            .add_array("/array".try_into().unwrap(), basic_shape(), None, Bytes::new())
            .await?;

        let properties = Default::default();
        let manifest_config = ManifestConfig::default();
        let mut flush_data = FlushProcess::new(
            Arc::clone(&session.asset_manager),
            &session.change_set,
            &Snapshot::INITIAL_SNAPSHOT_ID,
            &manifest_config,
        );

        // poke at flush_data to make inconsistent
        flush_data.manifest_files.insert(ManifestFileInfo {
            id: ManifestId::random(),
            num_chunk_refs: 0,
            size_bytes: 9000,
        });

        let res = do_flush(
            flush_data,
            "fail",
            1,
            properties,
            false,
            CommitMethod::NewCommit,
            manifest_config.splitting(),
        )
        .await;

        // verify it returns the right error
        assert!(matches!(
            res,
            Err(SessionError {
                kind: SessionErrorKind::ManifestsInconsistencyError { snapshot, nodes },
                ..
            }) if snapshot == 1 && nodes == 0
        ));

        Ok(())
    }

    #[test]
    fn test_construct_valid_byte_range() {
        // chunk at offset 100, length 50 → valid absolute range is [100, 150]
        let offset = 100u64;
        let length = 50u64;

        // Bounded: valid requests
        assert_eq!(
            construct_valid_byte_range(&ByteRange::Bounded(0..50), offset, length)
                .unwrap(),
            100..150,
        );
        assert_eq!(
            construct_valid_byte_range(&ByteRange::Bounded(10..30), offset, length)
                .unwrap(),
            110..130,
        );
        // Bounded: empty range at start is ok
        assert_eq!(
            construct_valid_byte_range(&ByteRange::Bounded(0..0), offset, length)
                .unwrap(),
            100..100,
        );

        // Bounded: end beyond chunk
        assert!(
            construct_valid_byte_range(&ByteRange::Bounded(0..51), offset, length)
                .is_err()
        );
        // Bounded: start at chunk length
        assert!(
            construct_valid_byte_range(&ByteRange::Bounded(50..50), offset, length)
                .is_err()
        );
        // Bounded: start beyond chunk length
        assert!(
            construct_valid_byte_range(&ByteRange::Bounded(60..70), offset, length)
                .is_err()
        );

        // From: valid
        assert_eq!(
            construct_valid_byte_range(&ByteRange::From(0), offset, length).unwrap(),
            100..150,
        );
        assert_eq!(
            construct_valid_byte_range(&ByteRange::From(25), offset, length).unwrap(),
            125..150,
        );
        // From: at chunk length
        assert!(
            construct_valid_byte_range(&ByteRange::From(50), offset, length).is_err()
        );
        // From: beyond chunk length
        assert!(
            construct_valid_byte_range(&ByteRange::From(60), offset, length).is_err()
        );

        // Until: valid (last n bytes)
        assert_eq!(
            construct_valid_byte_range(&ByteRange::Until(50), offset, length).unwrap(),
            100..150,
        );
        assert_eq!(
            construct_valid_byte_range(&ByteRange::Until(10), offset, length).unwrap(),
            140..150,
        );
        assert_eq!(
            construct_valid_byte_range(&ByteRange::Until(0), offset, length).unwrap(),
            150..150,
        );
        // Until: beyond chunk length
        assert!(
            construct_valid_byte_range(&ByteRange::Until(51), offset, length).is_err()
        );

        // Last: valid
        assert_eq!(
            construct_valid_byte_range(&ByteRange::Last(50), offset, length).unwrap(),
            100..150,
        );
        assert_eq!(
            construct_valid_byte_range(&ByteRange::Last(1), offset, length).unwrap(),
            149..150,
        );
        // Last: beyond chunk length
        assert!(
            construct_valid_byte_range(&ByteRange::Last(51), offset, length).is_err()
        );

        // Edge case: chunk at offset 0
        assert_eq!(
            construct_valid_byte_range(&ByteRange::Bounded(0..10), 0, 10).unwrap(),
            0..10,
        );
        assert!(construct_valid_byte_range(&ByteRange::Bounded(0..11), 0, 10).is_err());
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
        use crate::format::format_constants::SpecVersionBin;

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
                        shapes_and_dims(None, None),
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
                        shapes_and_dims(None, None),
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
                let session = Runtime::new().unwrap().block_on(async {
                    let repo =
                        create_memory_store_repository(SpecVersionBin::current()).await;
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
                        runtime.unwrap(repository.add_array(path, shape, dims, ud));
                    }
                    RepositoryTransition::UpdateArray(path, shape, dims, ud) => {
                        runtime.unwrap(repository.update_array(&path, shape, dims, ud));
                    }
                    RepositoryTransition::DeleteArray(Some(path)) => {
                        runtime.unwrap(repository.delete_array(path));
                    }
                    RepositoryTransition::AddGroup(path, ud) => {
                        runtime.unwrap(repository.add_group(path, ud));
                    }
                    RepositoryTransition::DeleteGroup(Some(path)) => {
                        runtime.unwrap(repository.delete_group(path));
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
                    assert_eq!(&node.user_data, ud);
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
