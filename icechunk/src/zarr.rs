use std::{
    collections::HashSet,
    fmt::Display,
    iter,
    num::NonZeroU64,
    ops::{Deref, DerefMut},
    path::PathBuf,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
};

use async_stream::try_stream;
use bytes::Bytes;
use futures::{Stream, StreamExt, TryStreamExt};
use itertools::Itertools;
use serde::{de, Deserialize, Serialize};
use serde_with::{serde_as, skip_serializing_none, TryFromInto};
use thiserror::Error;
use tokio::sync::RwLock;

use crate::{
    change_set::ChangeSet,
    format::{
        manifest::VirtualChunkRef,
        snapshot::{NodeData, UserAttributesSnapshot},
        ByteRange, ChunkOffset, IcechunkFormatError, SnapshotId,
    },
    refs::{BranchVersion, Ref},
    repository::{
        get_chunk, ArrayShape, ChunkIndices, ChunkKeyEncoding, ChunkPayload, ChunkShape,
        Codec, DataType, DimensionNames, FillValue, Path, RepositoryError,
        RepositoryResult, StorageTransformer, UserAttributes, ZarrArrayMetadata,
    },
    storage::{
        s3::{S3Config, S3Storage},
        virtual_ref::ObjectStoreVirtualChunkResolverConfig,
    },
    ObjectStorage, Repository, RepositoryBuilder, SnapshotMetadata, Storage,
};

pub use crate::format::ObjectId;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type")]
#[non_exhaustive]
pub enum StorageConfig {
    #[serde(rename = "in_memory")]
    InMemory { prefix: Option<String> },

    #[serde(rename = "local_filesystem")]
    LocalFileSystem { root: PathBuf },

    #[serde(rename = "s3")]
    S3ObjectStore {
        bucket: String,
        prefix: String,
        #[serde(flatten)]
        config: Option<S3Config>,
    },
}

impl StorageConfig {
    pub async fn make_storage(&self) -> Result<Arc<dyn Storage + Send + Sync>, String> {
        match self {
            StorageConfig::InMemory { prefix } => {
                Ok(Arc::new(ObjectStorage::new_in_memory_store(prefix.clone())))
            }
            StorageConfig::LocalFileSystem { root } => {
                let storage = ObjectStorage::new_local_store(root)
                    .map_err(|e| format!("Error creating storage: {e}"))?;
                Ok(Arc::new(storage))
            }
            StorageConfig::S3ObjectStore { bucket, prefix, config } => {
                let storage = S3Storage::new_s3_store(bucket, prefix, config.as_ref())
                    .await
                    .map_err(|e| format!("Error creating storage: {e}"))?;
                Ok(Arc::new(storage))
            }
        }
    }

    pub async fn make_cached_storage(
        &self,
    ) -> Result<Arc<dyn Storage + Send + Sync>, String> {
        let storage = self.make_storage().await?;
        let cached_storage = Repository::add_in_mem_asset_caching(storage);
        Ok(cached_storage)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[non_exhaustive]
pub enum VersionInfo {
    #[serde(rename = "snapshot_id")]
    SnapshotId(SnapshotId),
    #[serde(rename = "tag")]
    TagRef(String),
    #[serde(rename = "branch")]
    BranchTipRef(String),
}

#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct RepositoryConfig {
    pub version: Option<VersionInfo>,
    pub inline_chunk_threshold_bytes: Option<u16>,
    pub unsafe_overwrite_refs: Option<bool>,
    pub change_set_bytes: Option<Vec<u8>>,
    pub virtual_ref_config: Option<ObjectStoreVirtualChunkResolverConfig>,
}

impl RepositoryConfig {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn existing(version: VersionInfo) -> Self {
        Self { version: Some(version), ..Self::default() }
    }

    pub fn with_version(mut self, version: VersionInfo) -> Self {
        self.version = Some(version);
        self
    }

    pub fn with_inline_chunk_threshold_bytes(mut self, threshold: u16) -> Self {
        self.inline_chunk_threshold_bytes = Some(threshold);
        self
    }

    pub fn with_unsafe_overwrite_refs(mut self, unsafe_overwrite_refs: bool) -> Self {
        self.unsafe_overwrite_refs = Some(unsafe_overwrite_refs);
        self
    }

    pub fn with_virtual_ref_credentials(
        mut self,
        config: ObjectStoreVirtualChunkResolverConfig,
    ) -> Self {
        self.virtual_ref_config = Some(config);
        self
    }

    pub fn with_change_set_bytes(mut self, change_set_bytes: Vec<u8>) -> Self {
        self.change_set_bytes = Some(change_set_bytes);
        self
    }

    pub async fn make_repository(
        &self,
        storage: Arc<dyn Storage + Send + Sync>,
    ) -> Result<(Repository, Option<String>), String> {
        let (mut builder, branch): (RepositoryBuilder, Option<String>) =
            match &self.version {
                None => {
                    let builder = Repository::init(
                        storage,
                        self.unsafe_overwrite_refs.unwrap_or(false),
                    )
                    .await
                    .map_err(|err| format!("Error initializing repository: {err}"))?;
                    (builder, Some(String::from(Ref::DEFAULT_BRANCH)))
                }
                Some(VersionInfo::SnapshotId(sid)) => {
                    let builder = Repository::update(storage, sid.clone());
                    (builder, None)
                }
                Some(VersionInfo::TagRef(tag)) => {
                    let builder = Repository::from_tag(storage, tag)
                        .await
                        .map_err(|err| format!("Error fetching tag: {err}"))?;
                    (builder, None)
                }
                Some(VersionInfo::BranchTipRef(branch)) => {
                    let builder = Repository::from_branch_tip(storage, branch)
                        .await
                        .map_err(|err| format!("Error fetching branch: {err}"))?;
                    (builder, Some(branch.clone()))
                }
            };

        if let Some(inline_theshold) = self.inline_chunk_threshold_bytes {
            builder.with_inline_threshold_bytes(inline_theshold);
        }
        if let Some(value) = self.unsafe_overwrite_refs {
            builder.with_unsafe_overwrite_refs(value);
        }
        if let Some(config) = &self.virtual_ref_config {
            builder.with_virtual_ref_config(config.clone());
        }
        if let Some(change_set_bytes) = &self.change_set_bytes {
            let change_set = ChangeSet::import_from_bytes(change_set_bytes)
                .map_err(|err| format!("Error parsing change set: {err}"))?;
            builder.with_change_set(change_set);
        }

        // TODO: add error checking, does the previous version exist?
        Ok((builder.build(), branch))
    }
}

#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StoreOptions {
    pub get_partial_values_concurrency: u16,
}

impl Default for StoreOptions {
    fn default() -> Self {
        Self { get_partial_values_concurrency: 10 }
    }
}

#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ConsolidatedStore {
    pub storage: StorageConfig,
    pub repository: RepositoryConfig,
    pub config: Option<StoreOptions>,
}

impl ConsolidatedStore {
    pub fn with_version(mut self, version: VersionInfo) -> Self {
        self.repository.version = Some(version);
        self
    }

    pub fn with_change_set_bytes(
        mut self,
        change_set: Vec<u8>,
    ) -> RepositoryResult<Self> {
        self.repository.change_set_bytes = Some(change_set);
        Ok(self)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[non_exhaustive]
pub enum AccessMode {
    #[serde(rename = "r")]
    ReadOnly,
    #[serde(rename = "rw")]
    ReadWrite,
}

pub type StoreResult<A> = Result<A, StoreError>;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum KeyNotFoundError {
    #[error("chunk cannot be find for key `{key}`")]
    ChunkNotFound { key: String, path: Path, coords: ChunkIndices },
    #[error("node not found at `{path}`")]
    NodeNotFound { path: Path },
    #[error("v2 key not found at `{key}`")]
    ZarrV2KeyNotFound { key: String },
}

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum StoreError {
    #[error("invalid zarr key format `{key}`")]
    InvalidKey { key: String },
    #[error("this operation is not allowed: {0}")]
    NotAllowed(String),
    #[error("object not found: `{0}`")]
    NotFound(#[from] KeyNotFoundError),
    #[error("unsuccessful repository operation: `{0}`")]
    RepositoryError(#[from] RepositoryError),
    #[error("cannot commit when no snapshot is present")]
    NoSnapshot,
    #[error("all commits must be made on a branch")]
    NotOnBranch,
    #[error("bad metadata: `{0}`")]
    BadMetadata(#[from] serde_json::Error),
    #[error("store method `{0}` is not implemented by Icechunk")]
    Unimplemented(&'static str),
    #[error("bad key prefix: `{0}`")]
    BadKeyPrefix(String),
    #[error("error during parallel execution of get_partial_values")]
    PartialValuesPanic,
    #[error("cannot write to read-only store")]
    ReadOnly,
    #[error(
        "uncommitted changes in repository, commit changes or reset repository and try again."
    )]
    UncommittedChanges,
    #[error("unknown store error: `{0}`")]
    Unknown(Box<dyn std::error::Error + Send + Sync>),
}

#[derive(Debug)]
pub struct Store {
    repository: Arc<RwLock<Repository>>,
    mode: AccessMode,
    current_branch: Option<String>,
    config: StoreOptions,
}

impl Store {
    pub async fn from_consolidated(
        consolidated: &ConsolidatedStore,
        mode: AccessMode,
    ) -> Result<Self, String> {
        let storage = consolidated.storage.make_cached_storage().await?;
        let (repository, branch) =
            consolidated.repository.make_repository(storage).await?;
        Ok(Self::from_repository(repository, mode, branch, consolidated.config.clone()))
    }

    pub async fn from_json(json: &[u8], mode: AccessMode) -> Result<Self, String> {
        let config: ConsolidatedStore =
            serde_json::from_slice(json).map_err(|e| e.to_string())?;
        Self::from_consolidated(&config, mode).await
    }

    pub async fn new_from_storage(
        storage: Arc<dyn Storage + Send + Sync>,
    ) -> Result<Self, String> {
        let (repository, branch) =
            RepositoryConfig::default().make_repository(storage).await?;
        Ok(Self::from_repository(repository, AccessMode::ReadWrite, branch, None))
    }

    pub fn from_repository(
        repository: Repository,
        mode: AccessMode,
        current_branch: Option<String>,
        config: Option<StoreOptions>,
    ) -> Self {
        Store {
            repository: Arc::new(RwLock::new(repository)),
            mode,
            current_branch,
            config: config.unwrap_or_default(),
        }
    }

    /// Creates a new clone of the store with the given access mode.
    pub fn with_access_mode(&self, mode: AccessMode) -> Self {
        Store {
            repository: self.repository.clone(),
            mode,
            current_branch: self.current_branch.clone(),
            config: self.config.clone(),
        }
    }

    pub fn access_mode(&self) -> &AccessMode {
        &self.mode
    }

    pub fn current_branch(&self) -> &Option<String> {
        &self.current_branch
    }

    pub async fn snapshot_id(&self) -> SnapshotId {
        self.repository.read().await.snapshot_id().clone()
    }

    pub async fn current_version(&self) -> VersionInfo {
        if let Some(branch) = &self.current_branch {
            VersionInfo::BranchTipRef(branch.clone())
        } else {
            VersionInfo::SnapshotId(self.snapshot_id().await)
        }
    }

    pub async fn has_uncommitted_changes(&self) -> bool {
        self.repository.read().await.has_uncommitted_changes()
    }

    /// Resets the store to the head commit state. If there are any uncommitted changes, they will
    /// be lost.
    pub async fn reset(&mut self) -> StoreResult<()> {
        let guard = self.repository.read().await;
        // carefully avoid the deadlock if we were to call self.snapshot_id()
        let head_snapshot = guard.snapshot_id().clone();
        let storage = Arc::clone(guard.storage());
        let new_repository = Repository::update(storage, head_snapshot).build();
        drop(guard);
        self.repository = Arc::new(RwLock::new(new_repository));

        Ok(())
    }

    /// Checkout a specific version of the repository. This can be a snapshot id, a tag, or a branch tip.
    ///
    /// If the version is a branch tip, the branch will be set as the current branch. If the version
    /// is a tag or snapshot id, the current branch will be unset and the store will be in detached state.
    ///
    /// If there are uncommitted changes, this method will return an error.
    pub async fn checkout(&mut self, version: VersionInfo) -> StoreResult<()> {
        let mut repo = self.repository.write().await;

        // Checking out is not allowed if there are uncommitted changes
        if repo.has_uncommitted_changes() {
            return Err(StoreError::UncommittedChanges);
        }

        match version {
            VersionInfo::SnapshotId(sid) => {
                self.current_branch = None;
                repo.set_snapshot_id(sid);
            }
            VersionInfo::TagRef(tag) => {
                self.current_branch = None;
                repo.set_snapshot_from_tag(tag.as_str()).await?
            }
            VersionInfo::BranchTipRef(branch) => {
                self.current_branch = Some(branch.clone());
                repo.set_snapshot_from_branch(&branch).await?
            }
        }

        Ok(())
    }

    /// Switch to a new branch and commit the current snapshot to it. This fails if there is uncommitted changes,
    /// or if the branch already exists (because this would cause a conflict).
    pub async fn new_branch(
        &mut self,
        branch: &str,
    ) -> StoreResult<(SnapshotId, BranchVersion)> {
        // this needs to be done carefully to avoid deadlocks and race conditions
        let guard = self.repository.write().await;
        if guard.has_uncommitted_changes() {
            return Err(StoreError::UncommittedChanges);
        }
        let version = guard.new_branch(branch).await?;
        let snapshot_id = guard.snapshot_id().clone();

        self.current_branch = Some(branch.to_string());

        Ok((snapshot_id, version))
    }

    /// Commit the current changes to the current branch. If the store is not currently
    /// on a branch, this will return an error.
    pub async fn commit(&mut self, message: &str) -> StoreResult<SnapshotId> {
        self.distributed_commit(message, vec![]).await
    }

    pub async fn distributed_commit<'a, I: IntoIterator<Item = Vec<u8>>>(
        &mut self,
        message: &str,
        other_changesets_bytes: I,
    ) -> StoreResult<SnapshotId> {
        if let Some(branch) = &self.current_branch {
            let other_change_sets: Vec<ChangeSet> = other_changesets_bytes
                .into_iter()
                .map(|v| ChangeSet::import_from_bytes(v.as_slice()))
                .try_collect()?;
            let result = self
                .repository
                .write()
                .await
                .deref_mut()
                .distributed_commit(branch, other_change_sets, message, None)
                .await?;
            Ok(result)
        } else {
            Err(StoreError::NotOnBranch)
        }
    }

    /// Tag the given snapshot with a specified tag
    pub async fn tag(&mut self, tag: &str, snapshot_id: &SnapshotId) -> StoreResult<()> {
        self.repository.write().await.deref_mut().tag(tag, snapshot_id).await?;
        Ok(())
    }

    /// Returns the sequence of parents of the current session, in order of latest first.
    pub async fn ancestry(
        &self,
    ) -> StoreResult<impl Stream<Item = StoreResult<SnapshotMetadata>> + Send> {
        let repository = Arc::clone(&self.repository).read_owned().await;
        // FIXME: in memory realization to avoid maintaining a lock on the repository
        let all = repository.ancestry().await?.err_into().collect::<Vec<_>>().await;
        Ok(futures::stream::iter(all))
    }

    pub async fn change_set_bytes(&self) -> StoreResult<Vec<u8>> {
        Ok(self.repository.read().await.change_set_bytes()?)
    }

    pub async fn empty(&self) -> StoreResult<bool> {
        let res = self.repository.read().await.list_nodes().await?.next().is_none();
        Ok(res)
    }

    pub async fn clear(&mut self) -> StoreResult<()> {
        let mut repo = self.repository.write().await;
        Ok(repo.clear().await?)
    }

    pub async fn get(&self, key: &str, byte_range: &ByteRange) -> StoreResult<Bytes> {
        let repo = self.repository.read().await;
        get_key(key, byte_range, repo.deref()).await
    }

    /// Get all the requested keys concurrently.
    ///
    /// Returns a vector of the results, in the same order as the keys passed. Errors retrieving
    /// individual keys will be flagged in the inner [`StoreResult`].
    ///
    /// The outer [`StoreResult`] is used to flag a global failure and it could be [`StoreError::PartialValuesPanic`].
    ///
    /// Currently this function is using concurrency but not parallelism. To limit the number of
    /// concurrent tasks use the Store config value `get_partial_values_concurrency`.
    pub async fn get_partial_values(
        &self,
        key_ranges: impl IntoIterator<Item = (String, ByteRange)>,
    ) -> StoreResult<Vec<StoreResult<Bytes>>> {
        // TODO: prototype argument
        //
        // There is a challenges implementing this function: async rust is not well prepared to
        // do scoped tasks. We want to spawn parallel tasks for each key_range, but spawn requires
        // a `'static` `Future`. Since the `Future` needs `&self`, it cannot be `'static`. One
        // solution would be to wrap `self` in an Arc, but that makes client code much more
        // complicated.
        //
        // This [excellent post](https://without.boats/blog/the-scoped-task-trilemma/) explains why something like this is not currently achievable:
        // [Here](https://github.com/tokio-rs/tokio/issues/3162) is a a tokio thread explaining this cannot be done with current Rust.
        //
        // The compromise we found is using [`Stream::for_each_concurrent`]. This achieves the
        // borrowing and the concurrency but not the parallelism. So all the concurrent tasks will
        // execute on the same thread. This is not as bad as it sounds, since most of this will be
        // IO bound.

        let stream = futures::stream::iter(key_ranges);
        let results = Arc::new(Mutex::new(Vec::new()));
        let num_keys = AtomicUsize::new(0);
        stream
            .for_each_concurrent(
                self.config.get_partial_values_concurrency as usize,
                |(key, range)| {
                    let index = num_keys.fetch_add(1, Ordering::Release);
                    let results = Arc::clone(&results);
                    async move {
                        let value = self.get(&key, &range).await;
                        if let Ok(mut results) = results.lock() {
                            if index >= results.len() {
                                results.resize_with(index + 1, || None);
                            }
                            results[index] = Some(value);
                        }
                    }
                },
            )
            .await;

        let results = Arc::into_inner(results)
            .ok_or(StoreError::PartialValuesPanic)?
            .into_inner()
            .map_err(|_| StoreError::PartialValuesPanic)?;

        debug_assert!(results.len() == num_keys.into_inner());
        let res: Option<Vec<_>> = results.into_iter().collect();
        res.ok_or(StoreError::PartialValuesPanic)
    }

    pub async fn exists(&self, key: &str) -> StoreResult<bool> {
        let guard = self.repository.read().await;
        exists(key, guard.deref()).await
    }

    pub fn supports_writes(&self) -> StoreResult<bool> {
        Ok(true)
    }

    pub fn supports_deletes(&self) -> StoreResult<bool> {
        Ok(true)
    }

    pub async fn set(&self, key: &str, value: Bytes) -> StoreResult<()> {
        self.set_with_optional_locking(key, value, None).await
    }

    async fn set_with_optional_locking(
        &self,
        key: &str,
        value: Bytes,
        locked_repo: Option<&mut Repository>,
    ) -> StoreResult<()> {
        if self.mode == AccessMode::ReadOnly {
            return Err(StoreError::ReadOnly);
        }

        match Key::parse(key)? {
            Key::Metadata { node_path } => {
                if let Ok(array_meta) = serde_json::from_slice(value.as_ref()) {
                    self.set_array_meta(node_path, array_meta, locked_repo).await
                } else {
                    match serde_json::from_slice(value.as_ref()) {
                        Ok(group_meta) => {
                            self.set_group_meta(node_path, group_meta, locked_repo).await
                        }
                        Err(err) => Err(StoreError::BadMetadata(err)),
                    }
                }
            }
            Key::Chunk { node_path, coords } => {
                match locked_repo {
                    Some(repo) => {
                        let writer = repo.get_chunk_writer();
                        let payload = writer(value).await?;
                        repo.set_chunk_ref(node_path, coords, Some(payload)).await?
                    }
                    None => {
                        // we only lock the repository to get the writer
                        let writer = self.repository.read().await.get_chunk_writer();
                        // then we can write the bytes without holding the lock
                        let payload = writer(value).await?;
                        // and finally we lock for write and update the reference
                        self.repository
                            .write()
                            .await
                            .set_chunk_ref(node_path, coords, Some(payload))
                            .await?
                    }
                }
                Ok(())
            }
            Key::ZarrV2(_) => Err(StoreError::Unimplemented(
                "Icechunk cannot set Zarr V2 metadata keys",
            )),
        }
    }

    pub async fn set_if_not_exists(&self, key: &str, value: Bytes) -> StoreResult<()> {
        let mut guard = self.repository.write().await;
        if exists(key, guard.deref()).await? {
            Ok(())
        } else {
            self.set_with_optional_locking(key, value, Some(guard.deref_mut())).await
        }
    }

    // alternate API would take array path, and a mapping from string coord to ChunkPayload
    pub async fn set_virtual_ref(
        &mut self,
        key: &str,
        reference: VirtualChunkRef,
    ) -> StoreResult<()> {
        if self.mode == AccessMode::ReadOnly {
            return Err(StoreError::ReadOnly);
        }

        match Key::parse(key)? {
            Key::Chunk { node_path, coords } => {
                self.repository
                    .write()
                    .await
                    .set_chunk_ref(
                        node_path,
                        coords,
                        Some(ChunkPayload::Virtual(reference)),
                    )
                    .await?;
                Ok(())
            }
            Key::Metadata { .. } | Key::ZarrV2(_) => Err(StoreError::NotAllowed(
                format!("use .set to modify metadata for key {}", key),
            )),
        }
    }

    pub async fn delete(&self, key: &str) -> StoreResult<()> {
        if self.mode == AccessMode::ReadOnly {
            return Err(StoreError::ReadOnly);
        }

        match Key::parse(key)? {
            Key::Metadata { node_path } => {
                // we need to hold the lock while we do the node search and the write
                // to avoid race conditions with other writers
                // (remember this method takes &self and not &mut self)
                let mut guard = self.repository.write().await;
                let node = guard.get_node(&node_path).await.map_err(|_| {
                    KeyNotFoundError::NodeNotFound { path: node_path.clone() }
                })?;
                match node.node_data {
                    NodeData::Array(_, _) => {
                        Ok(guard.deref_mut().delete_array(node_path).await?)
                    }
                    NodeData::Group => {
                        Ok(guard.deref_mut().delete_group(node_path).await?)
                    }
                }
            }
            Key::Chunk { node_path, coords } => {
                let mut guard = self.repository.write().await;
                let repository = guard.deref_mut();
                Ok(repository.set_chunk_ref(node_path, coords, None).await?)
            }
            Key::ZarrV2(_) => Ok(()),
        }
    }

    pub fn supports_partial_writes(&self) -> StoreResult<bool> {
        Ok(false)
    }

    pub async fn set_partial_values(
        &self,
        _key_start_values: impl IntoIterator<Item = (&str, ChunkOffset, Bytes)>,
    ) -> StoreResult<()> {
        if self.mode == AccessMode::ReadOnly {
            return Err(StoreError::ReadOnly);
        }

        Err(StoreError::Unimplemented("set_partial_values"))
    }

    pub fn supports_listing(&self) -> StoreResult<bool> {
        Ok(true)
    }

    pub async fn list(
        &self,
    ) -> StoreResult<impl Stream<Item = StoreResult<String>> + Send> {
        self.list_prefix("/").await
    }

    pub async fn list_prefix(
        &self,
        prefix: &str,
    ) -> StoreResult<impl Stream<Item = StoreResult<String>> + Send> {
        // TODO: this is inefficient because it filters based on the prefix, instead of only
        // generating items that could potentially match
        let meta = self.list_metadata_prefix(prefix).await?;
        let chunks = self.list_chunks_prefix(prefix).await?;
        // FIXME: this is wrong, we are realizing all keys in memory
        // it should be lazy instead
        Ok(futures::stream::iter(meta.chain(chunks).collect::<Vec<_>>().await))
    }

    pub async fn list_dir(
        &self,
        prefix: &str,
    ) -> StoreResult<impl Stream<Item = StoreResult<String>> + Send> {
        // TODO: this is inefficient because it filters based on the prefix, instead of only
        // generating items that could potentially match
        // FIXME: this is not lazy, it goes through every chunk. This should be implemented using
        // metadata only, and ignore the chunks, but we should decide on that based on Zarr3 spec
        // evolution

        let idx: usize = if prefix == "/" { 0 } else { prefix.len() };

        let parents: HashSet<_> = self
            .list_prefix(prefix)
            .await?
            .map_ok(move |s| {
                // If the prefix is "/", get rid of it. This can happend when prefix is missing
                // the trailing slash (as it does in zarr-python impl)
                let rem = &s[idx..].trim_start_matches('/');
                let parent = rem.split_once('/').map_or(*rem, |(parent, _)| parent);
                parent.to_string()
            })
            .try_collect()
            .await?;
        // We tould return a Stream<Item = String> with this implementation, but the present
        // signature is better if we change the impl
        Ok(futures::stream::iter(parents.into_iter().map(Ok)))
    }

    async fn set_array_meta(
        &self,
        path: Path,
        array_meta: ArrayMetadata,
        locked_repo: Option<&mut Repository>,
    ) -> Result<(), StoreError> {
        match locked_repo {
            Some(repo) => set_array_meta(path, array_meta, repo).await,
            None => self.set_array_meta_locking(path, array_meta).await,
        }
    }

    async fn set_array_meta_locking(
        &self,
        path: Path,
        array_meta: ArrayMetadata,
    ) -> Result<(), StoreError> {
        // we need to hold the lock while we search the array and do the update to avoid race
        // conditions with other writers (notice we don't take &mut self)
        let mut guard = self.repository.write().await;
        set_array_meta(path, array_meta, guard.deref_mut()).await
    }

    async fn set_group_meta(
        &self,
        path: Path,
        group_meta: GroupMetadata,
        locked_repo: Option<&mut Repository>,
    ) -> Result<(), StoreError> {
        match locked_repo {
            Some(repo) => set_group_meta(path, group_meta, repo).await,
            None => self.set_group_meta_locking(path, group_meta).await,
        }
    }

    async fn set_group_meta_locking(
        &self,
        path: Path,
        group_meta: GroupMetadata,
    ) -> Result<(), StoreError> {
        // we need to hold the lock while we search the array and do the update to avoid race
        // conditions with other writers (notice we don't take &mut self)
        let mut guard = self.repository.write().await;
        set_group_meta(path, group_meta, guard.deref_mut()).await
    }

    async fn list_metadata_prefix<'a, 'b: 'a>(
        &'a self,
        prefix: &'b str,
    ) -> StoreResult<impl Stream<Item = StoreResult<String>> + 'a> {
        let prefix = prefix.trim_end_matches('/');
        let res = try_stream! {
            let repository = Arc::clone(&self.repository).read_owned().await;
            for node in repository.list_nodes().await? {
                // TODO: handle non-utf8?
                let meta_key = Key::Metadata { node_path: node.path }.to_string();
                    match meta_key.strip_prefix(prefix) {
                        None => {}
                        Some(rest) => {
                            // we have a few cases
                            if prefix.is_empty()   // if prefix was empty anything matches
                               || rest.is_empty()  // if stripping prefix left empty we have a match
                               || rest.starts_with('/') // next component so we match
                               // what we don't include is other matches,
                               // we want to catch prefix/foo but not prefix-foo
                            {
                                yield meta_key;
                            }

                    }
                }
            }
        };
        Ok(res)
    }

    async fn list_chunks_prefix<'a, 'b: 'a>(
        &'a self,
        prefix: &'b str,
    ) -> StoreResult<impl Stream<Item = StoreResult<String>> + 'a> {
        let prefix = prefix.trim_end_matches('/');
        let res = try_stream! {
            let repository = Arc::clone(&self.repository).read_owned().await;
            // TODO: this is inefficient because it filters based on the prefix, instead of only
            // generating items that could potentially match
            for await maybe_path_chunk in  repository.all_chunks().await.map_err(StoreError::RepositoryError)? {
                // FIXME: utf8 handling
                match maybe_path_chunk {
                    Ok((path,chunk)) => {
                        let chunk_key = Key::Chunk { node_path: path, coords: chunk.coord }.to_string();
                        if chunk_key.starts_with(prefix) {
                            yield chunk_key;
                        }
                    }
                    Err(err) => Err(err)?
                }
            }
        };
        Ok(res)
    }
}

async fn set_array_meta(
    path: Path,
    array_meta: ArrayMetadata,
    repo: &mut Repository,
) -> Result<(), StoreError> {
    if repo.get_array(&path).await.is_ok() {
        // TODO: we don't necessarily need to update both
        repo.set_user_attributes(path.clone(), array_meta.attributes).await?;
        repo.update_array(path, array_meta.zarr_metadata).await?;
        Ok(())
    } else {
        repo.add_array(path.clone(), array_meta.zarr_metadata).await?;
        repo.set_user_attributes(path, array_meta.attributes).await?;
        Ok(())
    }
}

async fn set_group_meta(
    path: Path,
    group_meta: GroupMetadata,
    repo: &mut Repository,
) -> Result<(), StoreError> {
    // we need to hold the lock while we search the group and do the update to avoid race
    // conditions with other writers (notice we don't take &mut self)
    //
    if repo.get_group(&path).await.is_ok() {
        repo.set_user_attributes(path, group_meta.attributes).await?;
        Ok(())
    } else {
        repo.add_group(path.clone()).await?;
        repo.set_user_attributes(path, group_meta.attributes).await?;
        Ok(())
    }
}

async fn get_metadata(
    _key: &str,
    path: &Path,
    range: &ByteRange,
    repo: &Repository,
) -> StoreResult<Bytes> {
    let node = repo.get_node(path).await.map_err(|_| {
        StoreError::NotFound(KeyNotFoundError::NodeNotFound { path: path.clone() })
    })?;
    let user_attributes = match node.user_attributes {
        None => None,
        Some(UserAttributesSnapshot::Inline(atts)) => Some(atts),
        // FIXME: implement
        Some(UserAttributesSnapshot::Ref(_)) => todo!(),
    };
    let full_metadata = match node.node_data {
        NodeData::Group => {
            Ok::<Bytes, StoreError>(GroupMetadata::new(user_attributes).to_bytes())
        }
        NodeData::Array(zarr_metadata, _) => {
            Ok(ArrayMetadata::new(user_attributes, zarr_metadata).to_bytes())
        }
    }?;

    Ok(range.slice(full_metadata))
}

async fn get_chunk_bytes(
    key: &str,
    path: Path,
    coords: ChunkIndices,
    byte_range: &ByteRange,
    repo: &Repository,
) -> StoreResult<Bytes> {
    let reader = repo.get_chunk_reader(&path, &coords, byte_range).await?;

    // then we can fetch the bytes without holding the lock
    let chunk = get_chunk(reader).await?;
    chunk.ok_or(StoreError::NotFound(KeyNotFoundError::ChunkNotFound {
        key: key.to_string(),
        path,
        coords,
    }))
}

async fn get_key(
    key: &str,
    byte_range: &ByteRange,
    repo: &Repository,
) -> StoreResult<Bytes> {
    let bytes = match Key::parse(key)? {
        Key::Metadata { node_path } => {
            get_metadata(key, &node_path, byte_range, repo).await
        }
        Key::Chunk { node_path, coords } => {
            get_chunk_bytes(key, node_path, coords, byte_range, repo).await
        }
        Key::ZarrV2(key) => {
            Err(StoreError::NotFound(KeyNotFoundError::ZarrV2KeyNotFound { key }))
        }
    }?;

    Ok(bytes)
}

async fn exists(key: &str, repo: &Repository) -> StoreResult<bool> {
    match get_key(key, &ByteRange::ALL, repo).await {
        Ok(_) => Ok(true),
        Err(StoreError::NotFound(_)) => Ok(false),
        Err(other_error) => Err(other_error),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Key {
    Metadata { node_path: Path },
    Chunk { node_path: Path, coords: ChunkIndices },
    ZarrV2(String),
}

impl Key {
    const ROOT_KEY: &'static str = "zarr.json";
    const METADATA_SUFFIX: &'static str = "/zarr.json";
    const CHUNK_COORD_PREFIX: &'static str = "c";

    fn parse(key: &str) -> Result<Self, StoreError> {
        fn parse_chunk(key: &str) -> Result<Key, StoreError> {
            if key == ".zgroup"
                || key == ".zarray"
                || key == ".zattrs"
                || key == ".zmetadata"
                || key.ends_with("/.zgroup")
                || key.ends_with("/.zarray")
                || key.ends_with("/.zattrs")
                || key.ends_with("/.zmetadata")
            {
                return Ok(Key::ZarrV2(key.to_string()));
            }

            if key == "c" {
                return Ok(Key::Chunk {
                    node_path: Path::root(),
                    coords: ChunkIndices(vec![]),
                });
            }
            if let Some((path, coords)) = key.rsplit_once(Key::CHUNK_COORD_PREFIX) {
                let path = path.strip_suffix('/').unwrap_or(path);
                if coords.is_empty() {
                    Ok(Key::Chunk {
                        node_path: format!("/{path}").try_into().map_err(|_| {
                            StoreError::InvalidKey { key: key.to_string() }
                        })?,
                        coords: ChunkIndices(vec![]),
                    })
                } else {
                    let absolute = format!("/{path}")
                        .try_into()
                        .map_err(|_| StoreError::InvalidKey { key: key.to_string() })?;
                    coords
                        .strip_prefix('/')
                        .ok_or(StoreError::InvalidKey { key: key.to_string() })?
                        .split('/')
                        .map(|s| s.parse::<u32>())
                        .collect::<Result<Vec<_>, _>>()
                        .map(|coords| Key::Chunk {
                            node_path: absolute,
                            coords: ChunkIndices(coords),
                        })
                        .map_err(|_| StoreError::InvalidKey { key: key.to_string() })
                }
            } else {
                Err(StoreError::InvalidKey { key: key.to_string() })
            }
        }

        if key == Key::ROOT_KEY {
            Ok(Key::Metadata { node_path: Path::root() })
        } else if let Some(path) = key.strip_suffix(Key::METADATA_SUFFIX) {
            // we need to be careful indexing into utf8 strings
            Ok(Key::Metadata {
                node_path: format!("/{path}")
                    .try_into()
                    .map_err(|_| StoreError::InvalidKey { key: key.to_string() })?,
            })
        } else {
            parse_chunk(key)
        }
    }
}

impl Display for Key {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Key::Metadata { node_path } => {
                let s =
                    format!("{}{}", &node_path.to_string()[1..], Key::METADATA_SUFFIX)
                        .trim_start_matches('/')
                        .to_string();
                f.write_str(s.as_str())
            }
            Key::Chunk { node_path, coords } => {
                let coords = coords.0.iter().map(|c| c.to_string()).join("/");
                let s = [node_path.to_string()[1..].to_string(), "c".to_string(), coords]
                    .iter()
                    .filter(|s| !s.is_empty())
                    .join("/");
                f.write_str(s.as_str())
            }
            Key::ZarrV2(key) => f.write_str(key.as_str()),
        }
    }
}

#[serde_as]
#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct ArrayMetadata {
    zarr_format: u8,
    #[serde(deserialize_with = "validate_array_node_type")]
    node_type: String,
    attributes: Option<UserAttributes>,
    #[serde(flatten)]
    #[serde_as(as = "TryFromInto<ZarrArrayMetadataSerialzer>")]
    zarr_metadata: ZarrArrayMetadata,
}

#[serde_as]
#[derive(Serialize, Deserialize)]
pub struct ZarrArrayMetadataSerialzer {
    pub shape: ArrayShape,
    pub data_type: DataType,

    #[serde_as(as = "TryFromInto<NameConfigSerializer>")]
    #[serde(rename = "chunk_grid")]
    pub chunk_shape: ChunkShape,

    #[serde_as(as = "TryFromInto<NameConfigSerializer>")]
    pub chunk_key_encoding: ChunkKeyEncoding,
    pub fill_value: serde_json::Value,
    pub codecs: Vec<Codec>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub storage_transformers: Option<Vec<StorageTransformer>>,
    // each dimension name can be null in Zarr
    pub dimension_names: Option<DimensionNames>,
}

impl TryFrom<ZarrArrayMetadataSerialzer> for ZarrArrayMetadata {
    type Error = IcechunkFormatError;

    fn try_from(value: ZarrArrayMetadataSerialzer) -> Result<Self, Self::Error> {
        let ZarrArrayMetadataSerialzer {
            shape,
            data_type,
            chunk_shape,
            chunk_key_encoding,
            fill_value,
            codecs,
            storage_transformers,
            dimension_names,
        } = value;
        {
            let fill_value = FillValue::from_data_type_and_json(&data_type, &fill_value)?;
            Ok(ZarrArrayMetadata {
                fill_value,
                shape,
                data_type,
                chunk_shape,
                chunk_key_encoding,
                codecs,
                storage_transformers,
                dimension_names,
            })
        }
    }
}

impl From<ZarrArrayMetadata> for ZarrArrayMetadataSerialzer {
    fn from(value: ZarrArrayMetadata) -> Self {
        let ZarrArrayMetadata {
            shape,
            data_type,
            chunk_shape,
            chunk_key_encoding,
            fill_value,
            codecs,
            storage_transformers,
            dimension_names,
        } = value;
        {
            fn fill_value_to_json(f: FillValue) -> serde_json::Value {
                match f {
                    FillValue::Bool(b) => b.into(),
                    FillValue::Int8(n) => n.into(),
                    FillValue::Int16(n) => n.into(),
                    FillValue::Int32(n) => n.into(),
                    FillValue::Int64(n) => n.into(),
                    FillValue::UInt8(n) => n.into(),
                    FillValue::UInt16(n) => n.into(),
                    FillValue::UInt32(n) => n.into(),
                    FillValue::UInt64(n) => n.into(),
                    FillValue::Float16(f) => {
                        if f.is_nan() {
                            FillValue::NAN_STR.into()
                        } else if f == f32::INFINITY {
                            FillValue::INF_STR.into()
                        } else if f == f32::NEG_INFINITY {
                            FillValue::NEG_INF_STR.into()
                        } else {
                            f.into()
                        }
                    }
                    FillValue::Float32(f) => {
                        if f.is_nan() {
                            FillValue::NAN_STR.into()
                        } else if f == f32::INFINITY {
                            FillValue::INF_STR.into()
                        } else if f == f32::NEG_INFINITY {
                            FillValue::NEG_INF_STR.into()
                        } else {
                            f.into()
                        }
                    }
                    FillValue::Float64(f) => {
                        if f.is_nan() {
                            FillValue::NAN_STR.into()
                        } else if f == f64::INFINITY {
                            FillValue::INF_STR.into()
                        } else if f == f64::NEG_INFINITY {
                            FillValue::NEG_INF_STR.into()
                        } else {
                            f.into()
                        }
                    }
                    FillValue::Complex64(r, i) => ([r, i].as_ref()).into(),
                    FillValue::Complex128(r, i) => ([r, i].as_ref()).into(),
                    FillValue::String(s) => s.into(),
                    FillValue::Bytes(b) => b.into(),
                }
            }

            let fill_value = fill_value_to_json(fill_value);
            ZarrArrayMetadataSerialzer {
                shape,
                data_type,
                chunk_shape,
                chunk_key_encoding,
                codecs,
                storage_transformers,
                dimension_names,
                fill_value,
            }
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct GroupMetadata {
    zarr_format: u8,
    #[serde(deserialize_with = "validate_group_node_type")]
    node_type: String,
    attributes: Option<UserAttributes>,
}

fn validate_group_node_type<'de, D>(d: D) -> Result<String, D::Error>
where
    D: de::Deserializer<'de>,
{
    let value = String::deserialize(d)?;

    if value != "group" {
        return Err(de::Error::invalid_value(
            de::Unexpected::Str(value.as_str()),
            &"the word 'group'",
        ));
    }

    Ok(value)
}

fn validate_array_node_type<'de, D>(d: D) -> Result<String, D::Error>
where
    D: de::Deserializer<'de>,
{
    let value = String::deserialize(d)?;

    if value != "array" {
        return Err(de::Error::invalid_value(
            de::Unexpected::Str(value.as_str()),
            &"the word 'array'",
        ));
    }

    Ok(value)
}

impl ArrayMetadata {
    fn new(attributes: Option<UserAttributes>, zarr_metadata: ZarrArrayMetadata) -> Self {
        Self { zarr_format: 3, node_type: "array".to_string(), attributes, zarr_metadata }
    }

    fn to_bytes(&self) -> Bytes {
        Bytes::from_iter(
            // We can unpack because it comes from controlled datastructures that can be serialized
            #[allow(clippy::expect_used)]
            serde_json::to_vec(self).expect("bug in ArrayMetadata serialization"),
        )
    }
}

impl GroupMetadata {
    fn new(attributes: Option<UserAttributes>) -> Self {
        Self { zarr_format: 3, node_type: "group".to_string(), attributes }
    }

    fn to_bytes(&self) -> Bytes {
        Bytes::from_iter(
            // We can unpack because it comes from controlled datastructures that can be serialized
            #[allow(clippy::expect_used)]
            serde_json::to_vec(self).expect("bug in GroupMetadata serialization"),
        )
    }
}

#[derive(Serialize, Deserialize)]
struct NameConfigSerializer {
    name: String,
    configuration: serde_json::Value,
}

impl From<ChunkShape> for NameConfigSerializer {
    fn from(value: ChunkShape) -> Self {
        let arr = serde_json::Value::Array(
            value
                .0
                .iter()
                .map(|v| {
                    serde_json::Value::Number(serde_json::value::Number::from(v.get()))
                })
                .collect(),
        );
        let kvs = serde_json::value::Map::from_iter(iter::once((
            "chunk_shape".to_string(),
            arr,
        )));
        Self {
            name: "regular".to_string(),
            configuration: serde_json::Value::Object(kvs),
        }
    }
}

impl TryFrom<NameConfigSerializer> for ChunkShape {
    type Error = &'static str;

    fn try_from(value: NameConfigSerializer) -> Result<Self, Self::Error> {
        match value {
            NameConfigSerializer {
                name,
                configuration: serde_json::Value::Object(kvs),
            } if name == "regular" => {
                let values = kvs
                    .get("chunk_shape")
                    .and_then(|v| v.as_array())
                    .ok_or("cannot parse ChunkShape")?;
                let shape = values
                    .iter()
                    .map(|v| v.as_u64().and_then(|u64| NonZeroU64::try_from(u64).ok()))
                    .collect::<Option<Vec<_>>>()
                    .ok_or("cannot parse ChunkShape")?;
                Ok(ChunkShape(shape))
            }
            _ => Err("cannot parse ChunkShape"),
        }
    }
}

impl From<ChunkKeyEncoding> for NameConfigSerializer {
    fn from(_value: ChunkKeyEncoding) -> Self {
        let kvs = serde_json::value::Map::from_iter(iter::once((
            "separator".to_string(),
            serde_json::Value::String("/".to_string()),
        )));
        Self {
            name: "default".to_string(),
            configuration: serde_json::Value::Object(kvs),
        }
    }
}

impl TryFrom<NameConfigSerializer> for ChunkKeyEncoding {
    type Error = &'static str;

    fn try_from(value: NameConfigSerializer) -> Result<Self, Self::Error> {
        //FIXME: we are hardcoding / as the separator
        match value {
            NameConfigSerializer {
                name,
                configuration: serde_json::Value::Object(kvs),
            } if name == "default" => {
                if let Some("/") =
                    kvs.get("separator").ok_or("cannot parse ChunkKeyEncoding")?.as_str()
                {
                    Ok(ChunkKeyEncoding::Slash)
                } else {
                    Err("cannot parse ChunkKeyEncoding")
                }
            }
            _ => Err("cannot parse ChunkKeyEncoding"),
        }
    }
}

#[cfg(test)]
#[allow(clippy::panic, clippy::unwrap_used, clippy::expect_used)]
mod tests {

    use std::borrow::BorrowMut;

    use crate::storage::s3::{S3Credentials, StaticS3Credentials};

    use super::*;
    use pretty_assertions::assert_eq;

    async fn all_keys(store: &Store) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        let version1 = keys(store, "/").await?;
        let mut version2 = store.list().await?.try_collect::<Vec<_>>().await?;
        version2.sort();
        assert_eq!(version1, version2);
        Ok(version1)
    }

    async fn keys(
        store: &Store,
        prefix: &str,
    ) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        let mut res = store.list_prefix(prefix).await?.try_collect::<Vec<_>>().await?;
        res.sort();
        Ok(res)
    }

    #[test]
    fn test_parse_key() {
        assert!(matches!(
            Key::parse("zarr.json"),
            Ok(Key::Metadata { node_path}) if node_path.to_string() == "/"
        ));
        assert!(matches!(
            Key::parse("a/zarr.json"),
            Ok(Key::Metadata { node_path }) if node_path.to_string() == "/a"
        ));
        assert!(matches!(
            Key::parse("a/b/c/zarr.json"),
            Ok(Key::Metadata { node_path }) if node_path.to_string() == "/a/b/c"
        ));
        assert!(matches!(
            Key::parse("foo/c"),
            Ok(Key::Chunk { node_path, coords }) if node_path.to_string() == "/foo" && coords == ChunkIndices(vec![])
        ));
        assert!(matches!(
            Key::parse("foo/bar/c"),
            Ok(Key::Chunk { node_path, coords}) if node_path.to_string() == "/foo/bar" && coords == ChunkIndices(vec![])
        ));
        assert!(matches!(
            Key::parse("foo/c/1/2/3"),
            Ok(Key::Chunk {
                node_path,
                coords,
            }) if node_path.to_string() == "/foo" && coords == ChunkIndices(vec![1,2,3])
        ));
        assert!(matches!(
            Key::parse("foo/bar/baz/c/1/2/3"),
            Ok(Key::Chunk {
                node_path,
                coords,
            }) if node_path.to_string() == "/foo/bar/baz" && coords == ChunkIndices(vec![1,2,3])
        ));
        assert!(matches!(
            Key::parse("c"),
            Ok(Key::Chunk { node_path, coords}) if node_path.to_string() == "/" && coords == ChunkIndices(vec![])
        ));
        assert!(matches!(
            Key::parse("c/0/0"),
            Ok(Key::Chunk { node_path, coords}) if node_path.to_string() == "/" && coords == ChunkIndices(vec![0,0])
        ));
        assert!(matches!(
            Key::parse(".zarray"),
            Ok(Key::ZarrV2(s) ) if s == ".zarray"
        ));
        assert!(matches!(
            Key::parse(".zgroup"),
            Ok(Key::ZarrV2(s) ) if s == ".zgroup"
        ));
        assert!(matches!(
            Key::parse(".zattrs"),
            Ok(Key::ZarrV2(s) ) if s == ".zattrs"
        ));
        assert!(matches!(
            Key::parse(".zmetadata"),
            Ok(Key::ZarrV2(s) ) if s == ".zmetadata"
        ));
        assert!(matches!(
            Key::parse("foo/.zgroup"),
            Ok(Key::ZarrV2(s) ) if s == "foo/.zgroup"
        ));
        assert!(matches!(
            Key::parse("foo/bar/.zarray"),
            Ok(Key::ZarrV2(s) ) if s == "foo/bar/.zarray"
        ));
        assert!(matches!(
            Key::parse("foo/.zmetadata"),
            Ok(Key::ZarrV2(s) ) if s == "foo/.zmetadata"
        ));
        assert!(matches!(
            Key::parse("foo/.zattrs"),
            Ok(Key::ZarrV2(s) ) if s == "foo/.zattrs"
        ));
    }

    #[test]
    fn test_format_key() {
        assert_eq!(
            Key::Metadata { node_path: Path::root() }.to_string(),
            "zarr.json".to_string()
        );
        assert_eq!(
            Key::Metadata { node_path: "/a".try_into().unwrap() }.to_string(),
            "a/zarr.json".to_string()
        );
        assert_eq!(
            Key::Metadata { node_path: "/a/b/c".try_into().unwrap() }.to_string(),
            "a/b/c/zarr.json".to_string()
        );
        assert_eq!(
            Key::Chunk { node_path: Path::root(), coords: ChunkIndices(vec![]) }
                .to_string(),
            "c".to_string()
        );
        assert_eq!(
            Key::Chunk { node_path: Path::root(), coords: ChunkIndices(vec![0]) }
                .to_string(),
            "c/0".to_string()
        );
        assert_eq!(
            Key::Chunk { node_path: Path::root(), coords: ChunkIndices(vec![1, 2]) }
                .to_string(),
            "c/1/2".to_string()
        );
        assert_eq!(
            Key::Chunk {
                node_path: "/a".try_into().unwrap(),
                coords: ChunkIndices(vec![])
            }
            .to_string(),
            "a/c".to_string()
        );
        assert_eq!(
            Key::Chunk {
                node_path: "/a".try_into().unwrap(),
                coords: ChunkIndices(vec![1])
            }
            .to_string(),
            "a/c/1".to_string()
        );
        assert_eq!(
            Key::Chunk {
                node_path: "/a".try_into().unwrap(),
                coords: ChunkIndices(vec![1, 2])
            }
            .to_string(),
            "a/c/1/2".to_string()
        );
    }

    #[test]
    fn test_metadata_serialization() {
        assert!(serde_json::from_str::<GroupMetadata>(
            r#"{"zarr_format":3, "node_type":"group"}"#
        )
        .is_ok());
        assert!(serde_json::from_str::<GroupMetadata>(
            r#"{"zarr_format":3, "node_type":"array"}"#
        )
        .is_err());

        assert!(serde_json::from_str::<ArrayMetadata>(
            r#"{"zarr_format":3,"node_type":"array","shape":[2,2,2],"data_type":"int32","chunk_grid":{"name":"regular","configuration":{"chunk_shape":[1,1,1]}},"chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},"fill_value":0,"codecs":[{"name":"mycodec","configuration":{"foo":42}}],"storage_transformers":[{"name":"mytransformer","configuration":{"bar":43}}],"dimension_names":["x","y","t"]}"#
        )
        .is_ok());
        assert!(serde_json::from_str::<ArrayMetadata>(
            r#"{"zarr_format":3,"node_type":"group","shape":[2,2,2],"data_type":"int32","chunk_grid":{"name":"regular","configuration":{"chunk_shape":[1,1,1]}},"chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},"fill_value":0,"codecs":[{"name":"mycodec","configuration":{"foo":42}}],"storage_transformers":[{"name":"mytransformer","configuration":{"bar":43}}],"dimension_names":["x","y","t"]}"#
        )
        .is_err());

        // deserialize with nan
        assert!(matches!(
            serde_json::from_str::<ArrayMetadata>(
                r#"{"zarr_format":3,"node_type":"array","shape":[2,2,2],"data_type":"float16","chunk_grid":{"name":"regular","configuration":{"chunk_shape":[1,1,1]}},"chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},"fill_value":"NaN","codecs":[{"name":"mycodec","configuration":{"foo":42}}],"storage_transformers":[{"name":"mytransformer","configuration":{"bar":43}}],"dimension_names":["x","y","t"]}"#
            ).unwrap().zarr_metadata.fill_value,
            FillValue::Float16(n) if n.is_nan()
        ));
        assert!(matches!(
            serde_json::from_str::<ArrayMetadata>(
                r#"{"zarr_format":3,"node_type":"array","shape":[2,2,2],"data_type":"float32","chunk_grid":{"name":"regular","configuration":{"chunk_shape":[1,1,1]}},"chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},"fill_value":"NaN","codecs":[{"name":"mycodec","configuration":{"foo":42}}],"storage_transformers":[{"name":"mytransformer","configuration":{"bar":43}}],"dimension_names":["x","y","t"]}"#
            ).unwrap().zarr_metadata.fill_value,
            FillValue::Float32(n) if n.is_nan()
        ));
        assert!(matches!(
            serde_json::from_str::<ArrayMetadata>(
                r#"{"zarr_format":3,"node_type":"array","shape":[2,2,2],"data_type":"float64","chunk_grid":{"name":"regular","configuration":{"chunk_shape":[1,1,1]}},"chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},"fill_value":"NaN","codecs":[{"name":"mycodec","configuration":{"foo":42}}],"storage_transformers":[{"name":"mytransformer","configuration":{"bar":43}}],"dimension_names":["x","y","t"]}"#
            ).unwrap().zarr_metadata.fill_value,
            FillValue::Float64(n) if n.is_nan()
        ));

        // deserialize with infinity
        assert_eq!(
            serde_json::from_str::<ArrayMetadata>(
                r#"{"zarr_format":3,"node_type":"array","shape":[2,2,2],"data_type":"float16","chunk_grid":{"name":"regular","configuration":{"chunk_shape":[1,1,1]}},"chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},"fill_value":"Infinity","codecs":[{"name":"mycodec","configuration":{"foo":42}}],"storage_transformers":[{"name":"mytransformer","configuration":{"bar":43}}],"dimension_names":["x","y","t"]}"#
            ).unwrap().zarr_metadata.fill_value,
            FillValue::Float16(f32::INFINITY)
        );
        assert_eq!(
            serde_json::from_str::<ArrayMetadata>(
                r#"{"zarr_format":3,"node_type":"array","shape":[2,2,2],"data_type":"float32","chunk_grid":{"name":"regular","configuration":{"chunk_shape":[1,1,1]}},"chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},"fill_value":"Infinity","codecs":[{"name":"mycodec","configuration":{"foo":42}}],"storage_transformers":[{"name":"mytransformer","configuration":{"bar":43}}],"dimension_names":["x","y","t"]}"#
            ).unwrap().zarr_metadata.fill_value,
            FillValue::Float32(f32::INFINITY)
        );
        assert_eq!(
            serde_json::from_str::<ArrayMetadata>(
                r#"{"zarr_format":3,"node_type":"array","shape":[2,2,2],"data_type":"float64","chunk_grid":{"name":"regular","configuration":{"chunk_shape":[1,1,1]}},"chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},"fill_value":"Infinity","codecs":[{"name":"mycodec","configuration":{"foo":42}}],"storage_transformers":[{"name":"mytransformer","configuration":{"bar":43}}],"dimension_names":["x","y","t"]}"#
            ).unwrap().zarr_metadata.fill_value,
            FillValue::Float64(f64::INFINITY)
        );

        // deserialize with -infinity
        assert_eq!(
            serde_json::from_str::<ArrayMetadata>(
                r#"{"zarr_format":3,"node_type":"array","shape":[2,2,2],"data_type":"float16","chunk_grid":{"name":"regular","configuration":{"chunk_shape":[1,1,1]}},"chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},"fill_value":"-Infinity","codecs":[{"name":"mycodec","configuration":{"foo":42}}],"storage_transformers":[{"name":"mytransformer","configuration":{"bar":43}}],"dimension_names":["x","y","t"]}"#
            ).unwrap().zarr_metadata.fill_value,
            FillValue::Float16(f32::NEG_INFINITY)
        );
        assert_eq!(
            serde_json::from_str::<ArrayMetadata>(
                r#"{"zarr_format":3,"node_type":"array","shape":[2,2,2],"data_type":"float32","chunk_grid":{"name":"regular","configuration":{"chunk_shape":[1,1,1]}},"chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},"fill_value":"-Infinity","codecs":[{"name":"mycodec","configuration":{"foo":42}}],"storage_transformers":[{"name":"mytransformer","configuration":{"bar":43}}],"dimension_names":["x","y","t"]}"#
            ).unwrap().zarr_metadata.fill_value,
            FillValue::Float32(f32::NEG_INFINITY)
        );
        assert_eq!(
            serde_json::from_str::<ArrayMetadata>(
                r#"{"zarr_format":3,"node_type":"array","shape":[2,2,2],"data_type":"float64","chunk_grid":{"name":"regular","configuration":{"chunk_shape":[1,1,1]}},"chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},"fill_value":"-Infinity","codecs":[{"name":"mycodec","configuration":{"foo":42}}],"storage_transformers":[{"name":"mytransformer","configuration":{"bar":43}}],"dimension_names":["x","y","t"]}"#
            ).unwrap().zarr_metadata.fill_value,
            FillValue::Float64(f64::NEG_INFINITY)
        );

        // infinity roundtrip
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
        let zarr_meta = ArrayMetadata::new(None, zarr_meta);

        assert_eq!(
            serde_json::from_str::<ArrayMetadata>(
                serde_json::to_string(&zarr_meta).unwrap().as_str()
            )
            .unwrap(),
            zarr_meta,
        )
    }

    #[tokio::test]
    async fn test_metadata_set_and_get() -> Result<(), Box<dyn std::error::Error>> {
        let storage: Arc<dyn Storage + Send + Sync> =
            Arc::new(ObjectStorage::new_in_memory_store(Some("prefix".into())));
        let ds = Repository::init(Arc::clone(&storage), false).await?.build();
        let store = Store::from_repository(
            ds,
            AccessMode::ReadWrite,
            Some("main".to_string()),
            None,
        );

        assert!(matches!(
            store.get("zarr.json", &ByteRange::ALL).await,
            Err(StoreError::NotFound(KeyNotFoundError::NodeNotFound {path})) if path.to_string() == "/"
        ));

        store
            .set(
                "zarr.json",
                Bytes::copy_from_slice(br#"{"zarr_format":3, "node_type":"group"}"#),
            )
            .await?;
        assert_eq!(
            store.get("zarr.json", &ByteRange::ALL).await.unwrap(),
            Bytes::copy_from_slice(
                br#"{"zarr_format":3,"node_type":"group","attributes":null}"#
            )
        );

        store.set("a/b/zarr.json", Bytes::copy_from_slice(br#"{"zarr_format":3, "node_type":"group", "attributes": {"spam":"ham", "eggs":42}}"#)).await?;
        assert_eq!(
            store.get("a/b/zarr.json", &ByteRange::ALL).await.unwrap(),
            Bytes::copy_from_slice(
                br#"{"zarr_format":3,"node_type":"group","attributes":{"eggs":42,"spam":"ham"}}"#
            )
        );

        let zarr_meta = Bytes::copy_from_slice(br#"{"zarr_format":3,"node_type":"array","attributes":{"foo":42},"shape":[2,2,2],"data_type":"int32","chunk_grid":{"name":"regular","configuration":{"chunk_shape":[1,1,1]}},"chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},"fill_value":0,"codecs":[{"name":"mycodec","configuration":{"foo":42}}],"storage_transformers":[{"name":"mytransformer","configuration":{"bar":43}}],"dimension_names":["x","y","t"]}"#);
        store.set("a/b/array/zarr.json", zarr_meta.clone()).await?;
        assert_eq!(
            store.get("a/b/array/zarr.json", &ByteRange::ALL).await.unwrap(),
            zarr_meta.clone()
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_metadata_delete() -> Result<(), Box<dyn std::error::Error>> {
        let in_mem_storage: Arc<dyn Storage + Send + Sync> =
            Arc::new(ObjectStorage::new_in_memory_store(Some("prefix".into())));
        let storage =
            Arc::clone(&(in_mem_storage.clone() as Arc<dyn Storage + Send + Sync>));
        let ds = Repository::init(Arc::clone(&storage), false).await?.build();
        let store = Store::from_repository(
            ds,
            AccessMode::ReadWrite,
            Some("main".to_string()),
            None,
        );
        let group_data = br#"{"zarr_format":3, "node_type":"group", "attributes": {"spam":"ham", "eggs":42}}"#;

        store
            .set(
                "zarr.json",
                Bytes::copy_from_slice(br#"{"zarr_format":3, "node_type":"group"}"#),
            )
            .await
            .unwrap();
        let zarr_meta = Bytes::copy_from_slice(br#"{"zarr_format":3,"node_type":"array","attributes":{"foo":42},"shape":[2,2,2],"data_type":"int32","chunk_grid":{"name":"regular","configuration":{"chunk_shape":[1,1,1]}},"chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},"fill_value":0,"codecs":[{"name":"mycodec","configuration":{"foo":42}}],"storage_transformers":[{"name":"mytransformer","configuration":{"bar":43}}],"dimension_names":["x","y","t"]}"#);
        store.set("array/zarr.json", zarr_meta.clone()).await.unwrap();

        // delete metadata tests
        store.delete("array/zarr.json").await.unwrap();
        assert!(matches!(
            store.get("array/zarr.json", &ByteRange::ALL).await,
            Err(StoreError::NotFound(KeyNotFoundError::NodeNotFound { path }))
                if path.to_string() == "/array",
        ));
        store.set("array/zarr.json", zarr_meta.clone()).await.unwrap();
        store.delete("array/zarr.json").await.unwrap();
        assert!(matches!(
            store.get("array/zarr.json", &ByteRange::ALL).await,
            Err(StoreError::NotFound(KeyNotFoundError::NodeNotFound { path } ))
                if path.to_string() == "/array",
        ));
        store.set("array/zarr.json", Bytes::copy_from_slice(group_data)).await.unwrap();

        Ok(())
    }

    #[tokio::test]
    async fn test_chunk_set_and_get() -> Result<(), Box<dyn std::error::Error>> {
        // TODO: turn this test into pure Store operations once we support writes through Zarr
        let in_mem_storage: Arc<dyn Storage + Send + Sync> =
            Arc::new(ObjectStorage::new_in_memory_store(Some("prefix".into())));
        let storage =
            Arc::clone(&(in_mem_storage.clone() as Arc<dyn Storage + Send + Sync>));
        let ds = Repository::init(Arc::clone(&storage), false).await?.build();
        let mut store = Store::from_repository(
            ds,
            AccessMode::ReadWrite,
            Some("main".to_string()),
            None,
        );

        store
            .set(
                "zarr.json",
                Bytes::copy_from_slice(br#"{"zarr_format":3, "node_type":"group"}"#),
            )
            .await?;
        let zarr_meta = Bytes::copy_from_slice(br#"{"zarr_format":3,"node_type":"array","attributes":{"foo":42},"shape":[2,2,2],"data_type":"int32","chunk_grid":{"name":"regular","configuration":{"chunk_shape":[1,1,1]}},"chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},"fill_value":0,"codecs":[{"name":"mycodec","configuration":{"foo":42}}],"storage_transformers":[{"name":"mytransformer","configuration":{"bar":43}}],"dimension_names":["x","y","t"]}"#);
        store.set("array/zarr.json", zarr_meta.clone()).await?;
        assert_eq!(
            store.get("array/zarr.json", &ByteRange::ALL).await.unwrap(),
            zarr_meta
        );
        assert_eq!(
            store.get("array/zarr.json", &ByteRange::to_offset(5)).await.unwrap(),
            zarr_meta[..5]
        );
        assert_eq!(
            store.get("array/zarr.json", &ByteRange::from_offset(5)).await.unwrap(),
            zarr_meta[5..]
        );
        assert_eq!(
            store.get("array/zarr.json", &ByteRange::bounded(1, 24)).await.unwrap(),
            zarr_meta[1..24]
        );

        // a small inline chunk
        let small_data = Bytes::copy_from_slice(b"hello");
        store.set("array/c/0/1/0", small_data.clone()).await?;
        assert_eq!(
            store.get("array/c/0/1/0", &ByteRange::ALL).await.unwrap(),
            small_data
        );
        assert_eq!(
            store.get("array/c/0/1/0", &ByteRange::to_offset(2)).await.unwrap(),
            small_data[0..2]
        );
        assert_eq!(
            store.get("array/c/0/1/0", &ByteRange::from_offset(3)).await.unwrap(),
            small_data[3..]
        );
        assert_eq!(
            store.get("array/c/0/1/0", &ByteRange::bounded(1, 4)).await.unwrap(),
            small_data[1..4]
        );
        // no new chunks written because it was inline
        // FiXME: add this test
        //assert!(in_mem_storage.chunk_ids().is_empty());

        // a big chunk
        let big_data = Bytes::copy_from_slice(b"hello".repeat(512).as_slice());
        store.set("array/c/0/1/1", big_data.clone()).await?;
        assert_eq!(store.get("array/c/0/1/1", &ByteRange::ALL).await.unwrap(), big_data);
        assert_eq!(
            store.get("array/c/0/1/1", &ByteRange::from_offset(512 - 3)).await.unwrap(),
            big_data[(512 - 3)..]
        );
        assert_eq!(
            store.get("array/c/0/1/1", &ByteRange::to_offset(5)).await.unwrap(),
            big_data[..5]
        );
        assert_eq!(
            store.get("array/c/0/1/1", &ByteRange::bounded(20, 90)).await.unwrap(),
            big_data[20..90]
        );
        // FiXME: add this test
        //let chunk_id = in_mem_storage.chunk_ids().iter().next().cloned().unwrap();
        //assert_eq!(in_mem_storage.fetch_chunk(&chunk_id, &None).await?, big_data);

        let oid = store.commit("commit").await?;

        let ds = Repository::update(storage, oid).build();
        let store = Store::from_repository(ds, AccessMode::ReadWrite, None, None);
        assert_eq!(
            store.get("array/c/0/1/0", &ByteRange::ALL).await.unwrap(),
            small_data
        );
        assert_eq!(store.get("array/c/0/1/1", &ByteRange::ALL).await.unwrap(), big_data);

        Ok(())
    }

    #[tokio::test]
    async fn test_chunk_delete() -> Result<(), Box<dyn std::error::Error>> {
        let in_mem_storage: Arc<dyn Storage + Send + Sync> =
            Arc::new(ObjectStorage::new_in_memory_store(Some("prefix".into())));
        let storage =
            Arc::clone(&(in_mem_storage.clone() as Arc<dyn Storage + Send + Sync>));
        let ds = Repository::init(Arc::clone(&storage), false).await?.build();
        let store = Store::from_repository(
            ds,
            AccessMode::ReadWrite,
            Some("main".to_string()),
            None,
        );

        store
            .set(
                "zarr.json",
                Bytes::copy_from_slice(br#"{"zarr_format":3, "node_type":"group"}"#),
            )
            .await
            .unwrap();
        let zarr_meta = Bytes::copy_from_slice(br#"{"zarr_format":3,"node_type":"array","attributes":{"foo":42},"shape":[2,2,2],"data_type":"int32","chunk_grid":{"name":"regular","configuration":{"chunk_shape":[1,1,1]}},"chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},"fill_value":0,"codecs":[{"name":"mycodec","configuration":{"foo":42}}],"storage_transformers":[{"name":"mytransformer","configuration":{"bar":43}}],"dimension_names":["x","y","t"]}"#);
        store.set("array/zarr.json", zarr_meta.clone()).await.unwrap();

        let data = Bytes::copy_from_slice(b"hello");
        store.set("array/c/0/1/0", data.clone()).await.unwrap();

        // delete chunk
        store.delete("array/c/0/1/0").await.unwrap();
        // deleting a deleted chunk is allowed
        store.delete("array/c/0/1/0").await.unwrap();
        // deleting non-existent chunk is allowed
        store.delete("array/c/1/1/1").await.unwrap();
        assert!(matches!(
            store.get("array/c/0/1/0", &ByteRange::ALL).await,
            Err(StoreError::NotFound(KeyNotFoundError::ChunkNotFound { key, path, coords }))
                if key == "array/c/0/1/0" && path.to_string() == "/array" && coords == ChunkIndices([0, 1, 0].to_vec())
        ));
        assert!(matches!(
            store.delete("array/foo").await,
            Err(StoreError::InvalidKey { key }) if key == "array/foo",
        ));
        // FIXME: deleting an invalid chunk should not be allowed.
        store.delete("array/c/10/1/1").await.unwrap();

        Ok(())
    }

    #[tokio::test]
    async fn test_metadata_list() -> Result<(), Box<dyn std::error::Error>> {
        let storage: Arc<dyn Storage + Send + Sync> =
            Arc::new(ObjectStorage::new_in_memory_store(Some("prefix".into())));
        let ds = Repository::init(Arc::clone(&storage), false).await?.build();
        let mut store = Store::from_repository(
            ds,
            AccessMode::ReadWrite,
            Some("main".to_string()),
            None,
        );

        assert!(store.empty().await.unwrap());
        assert!(!store.exists("zarr.json").await.unwrap());

        assert_eq!(all_keys(&store).await.unwrap(), Vec::<String>::new());
        store
            .borrow_mut()
            .set(
                "zarr.json",
                Bytes::copy_from_slice(br#"{"zarr_format":3, "node_type":"group"}"#),
            )
            .await?;

        assert!(!store.empty().await.unwrap());
        assert!(store.exists("zarr.json").await.unwrap());
        assert_eq!(all_keys(&store).await.unwrap(), vec!["zarr.json".to_string()]);
        store
            .borrow_mut()
            .set(
                "group/zarr.json",
                Bytes::copy_from_slice(br#"{"zarr_format":3, "node_type":"group"}"#),
            )
            .await?;
        assert_eq!(
            all_keys(&store).await.unwrap(),
            vec!["group/zarr.json".to_string(), "zarr.json".to_string()]
        );
        assert_eq!(
            keys(&store, "group/").await.unwrap(),
            vec!["group/zarr.json".to_string()]
        );

        let zarr_meta = Bytes::copy_from_slice(br#"{"zarr_format":3,"node_type":"array","attributes":{"foo":42},"shape":[2,2,2],"data_type":"int32","chunk_grid":{"name":"regular","configuration":{"chunk_shape":[1,1,1]}},"chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},"fill_value":0,"codecs":[{"name":"mycodec","configuration":{"foo":42}}],"storage_transformers":[{"name":"mytransformer","configuration":{"bar":43}}],"dimension_names":["x","y","t"]}"#);
        store.set("group/array/zarr.json", zarr_meta).await?;
        assert!(!store.empty().await.unwrap());
        assert!(store.exists("zarr.json").await.unwrap());
        assert!(store.exists("group/array/zarr.json").await.unwrap());
        assert!(store.exists("group/zarr.json").await.unwrap());
        assert_eq!(
            all_keys(&store).await.unwrap(),
            vec![
                "group/array/zarr.json".to_string(),
                "group/zarr.json".to_string(),
                "zarr.json".to_string()
            ]
        );
        assert_eq!(
            keys(&store, "group/").await.unwrap(),
            vec!["group/array/zarr.json".to_string(), "group/zarr.json".to_string()]
        );
        assert_eq!(
            keys(&store, "group/array/").await.unwrap(),
            vec!["group/array/zarr.json".to_string()]
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_set_array_metadata() -> Result<(), Box<dyn std::error::Error>> {
        let storage: Arc<dyn Storage + Send + Sync> =
            Arc::new(ObjectStorage::new_in_memory_store(Some("prefix".into())));
        let ds = Repository::init(Arc::clone(&storage), false).await?.build();
        let mut store = Store::from_repository(
            ds,
            AccessMode::ReadWrite,
            Some("main".to_string()),
            None,
        );

        store
            .borrow_mut()
            .set(
                "zarr.json",
                Bytes::copy_from_slice(br#"{"zarr_format":3, "node_type":"group"}"#),
            )
            .await?;

        let zarr_meta = Bytes::copy_from_slice(br#"{"zarr_format":3,"node_type":"array","attributes":{"foo":42},"shape":[2,2,2],"data_type":"int32","chunk_grid":{"name":"regular","configuration":{"chunk_shape":[1,1,1]}},"chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},"fill_value":0,"codecs":[{"name":"mycodec","configuration":{"foo":42}}],"storage_transformers":[{"name":"mytransformer","configuration":{"bar":43}}],"dimension_names":["x","y","t"]}"#);
        store.set("/array/zarr.json", zarr_meta.clone()).await?;
        assert_eq!(
            store.get("/array/zarr.json", &ByteRange::ALL).await?,
            zarr_meta.clone()
        );

        store.set("0/zarr.json", zarr_meta.clone()).await?;
        assert_eq!(store.get("0/zarr.json", &ByteRange::ALL).await?, zarr_meta.clone());

        store.set("/0/zarr.json", zarr_meta.clone()).await?;
        assert_eq!(store.get("/0/zarr.json", &ByteRange::ALL).await?, zarr_meta);

        // store.set("c/0", zarr_meta.clone()).await?;
        // assert_eq!(store.get("c/0", &ByteRange::ALL).await?, zarr_meta);

        Ok(())
    }

    #[tokio::test]
    async fn test_chunk_list() -> Result<(), Box<dyn std::error::Error>> {
        let storage: Arc<dyn Storage + Send + Sync> =
            Arc::new(ObjectStorage::new_in_memory_store(Some("prefix".into())));
        let ds = Repository::init(Arc::clone(&storage), false).await?.build();
        let mut store = Store::from_repository(
            ds,
            AccessMode::ReadWrite,
            Some("main".to_string()),
            None,
        );

        store
            .borrow_mut()
            .set(
                "zarr.json",
                Bytes::copy_from_slice(br#"{"zarr_format":3, "node_type":"group"}"#),
            )
            .await?;

        let zarr_meta = Bytes::copy_from_slice(br#"{"zarr_format":3,"node_type":"array","attributes":{"foo":42},"shape":[2,2,2],"data_type":"int32","chunk_grid":{"name":"regular","configuration":{"chunk_shape":[1,1,1]}},"chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},"fill_value":0,"codecs":[{"name":"mycodec","configuration":{"foo":42}}],"storage_transformers":[{"name":"mytransformer","configuration":{"bar":43}}],"dimension_names":["x","y","t"]}"#);
        store.set("array/zarr.json", zarr_meta).await?;

        let data = Bytes::copy_from_slice(b"hello");
        store.set("array/c/0/1/0", data.clone()).await?;
        store.set("array/c/1/1/1", data.clone()).await?;

        assert_eq!(
            all_keys(&store).await.unwrap(),
            vec![
                "array/c/0/1/0".to_string(),
                "array/c/1/1/1".to_string(),
                "array/zarr.json".to_string(),
                "zarr.json".to_string()
            ]
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_list_dir() -> Result<(), Box<dyn std::error::Error>> {
        let storage: Arc<dyn Storage + Send + Sync> =
            Arc::new(ObjectStorage::new_in_memory_store(Some("prefix".into())));
        let ds = Repository::init(Arc::clone(&storage), false).await?.build();
        let mut store = Store::from_repository(
            ds,
            AccessMode::ReadWrite,
            Some("main".to_string()),
            None,
        );

        store
            .borrow_mut()
            .set(
                "zarr.json",
                Bytes::copy_from_slice(br#"{"zarr_format":3, "node_type":"group"}"#),
            )
            .await?;

        let zarr_meta = Bytes::copy_from_slice(br#"{"zarr_format":3,"node_type":"array","attributes":{"foo":42},"shape":[2,2,2],"data_type":"int32","chunk_grid":{"name":"regular","configuration":{"chunk_shape":[1,1,1]}},"chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},"fill_value":0,"codecs":[{"name":"mycodec","configuration":{"foo":42}}],"storage_transformers":[{"name":"mytransformer","configuration":{"bar":43}}],"dimension_names":["x","y","t"]}"#);
        store.set("array/zarr.json", zarr_meta).await?;

        let data = Bytes::copy_from_slice(b"hello");
        store.set("array/c/0/1/0", data.clone()).await?;
        store.set("array/c/1/1/1", data.clone()).await?;

        assert_eq!(
            all_keys(&store).await.unwrap(),
            vec![
                "array/c/0/1/0".to_string(),
                "array/c/1/1/1".to_string(),
                "array/zarr.json".to_string(),
                "zarr.json".to_string()
            ]
        );

        let mut dir = store.list_dir("/").await?.try_collect::<Vec<_>>().await?;
        dir.sort();
        assert_eq!(dir, vec!["array".to_string(), "zarr.json".to_string()]);

        let mut dir = store.list_dir("array").await?.try_collect::<Vec<_>>().await?;
        dir.sort();
        assert_eq!(dir, vec!["c".to_string(), "zarr.json".to_string()]);

        let mut dir = store.list_dir("array/").await?.try_collect::<Vec<_>>().await?;
        dir.sort();
        assert_eq!(dir, vec!["c".to_string(), "zarr.json".to_string()]);

        let mut dir = store.list_dir("array/c/").await?.try_collect::<Vec<_>>().await?;
        dir.sort();
        assert_eq!(dir, vec!["0".to_string(), "1".to_string()]);

        let mut dir = store.list_dir("array/c/1/").await?.try_collect::<Vec<_>>().await?;
        dir.sort();
        assert_eq!(dir, vec!["1".to_string()]);
        Ok(())
    }

    #[tokio::test]
    async fn test_list_dir_with_prefix() -> Result<(), Box<dyn std::error::Error>> {
        let storage: Arc<dyn Storage + Send + Sync> =
            Arc::new(ObjectStorage::new_in_memory_store(Some("prefix".into())));
        let ds = Repository::init(Arc::clone(&storage), false).await?.build();
        let mut store = Store::from_repository(
            ds,
            AccessMode::ReadWrite,
            Some("main".to_string()),
            None,
        );

        store
            .borrow_mut()
            .set(
                "zarr.json",
                Bytes::copy_from_slice(br#"{"zarr_format":3, "node_type":"group"}"#),
            )
            .await?;

        store
            .borrow_mut()
            .set(
                "group/zarr.json",
                Bytes::copy_from_slice(br#"{"zarr_format":3, "node_type":"group"}"#),
            )
            .await?;

        store
            .borrow_mut()
            .set(
                "group-suffix/zarr.json",
                Bytes::copy_from_slice(br#"{"zarr_format":3, "node_type":"group"}"#),
            )
            .await?;

        assert_eq!(
            store.list_dir("group/").await?.try_collect::<Vec<_>>().await?,
            vec!["zarr.json"]
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_get_partial_values() -> Result<(), Box<dyn std::error::Error>> {
        let storage: Arc<dyn Storage + Send + Sync> =
            Arc::new(ObjectStorage::new_in_memory_store(Some("prefix".into())));
        let ds = Repository::init(Arc::clone(&storage), false).await?.build();
        let mut store = Store::from_repository(
            ds,
            AccessMode::ReadWrite,
            Some("main".to_string()),
            None,
        );

        store
            .borrow_mut()
            .set(
                "zarr.json",
                Bytes::copy_from_slice(br#"{"zarr_format":3, "node_type":"group"}"#),
            )
            .await?;

        let zarr_meta = Bytes::copy_from_slice(br#"{"zarr_format":3,"node_type":"array","attributes":{"foo":42},"shape":[20],"data_type":"int32","chunk_grid":{"name":"regular","configuration":{"chunk_shape":[1]}},"chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},"fill_value":0,"codecs":[{"name":"mycodec","configuration":{"foo":42}}],"storage_transformers":[{"name":"mytransformer","configuration":{"bar":43}}],"dimension_names":["x"]}"#);
        store.set("array/zarr.json", zarr_meta).await?;

        let key_vals: Vec<_> = (0i32..20)
            .map(|idx| {
                (
                    format!("array/c/{idx}"),
                    Bytes::copy_from_slice(idx.to_be_bytes().to_owned().as_slice()),
                )
            })
            .collect();

        for (key, value) in key_vals.iter() {
            store.set(key.as_str(), value.clone()).await?;
        }

        let key_ranges = key_vals.iter().map(|(k, _)| (k.clone(), ByteRange::ALL));

        assert_eq!(
            key_vals.iter().map(|(_, v)| v.clone()).collect::<Vec<_>>(),
            store
                .get_partial_values(key_ranges)
                .await?
                .into_iter()
                .map(|v| v.unwrap())
                .collect::<Vec<_>>()
        );

        // let's try in reverse order
        let key_ranges = key_vals.iter().rev().map(|(k, _)| (k.clone(), ByteRange::ALL));

        assert_eq!(
            key_vals.iter().rev().map(|(_, v)| v.clone()).collect::<Vec<_>>(),
            store
                .get_partial_values(key_ranges)
                .await?
                .into_iter()
                .map(|v| v.unwrap())
                .collect::<Vec<_>>()
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_commit_and_checkout() -> Result<(), Box<dyn std::error::Error>> {
        let storage: Arc<dyn Storage + Send + Sync> =
            Arc::new(ObjectStorage::new_in_memory_store(Some("prefix".into())));

        let mut store = Store::new_from_storage(Arc::clone(&storage)).await?;

        store
            .set(
                "zarr.json",
                Bytes::copy_from_slice(br#"{"zarr_format":3, "node_type":"group"}"#),
            )
            .await
            .unwrap();
        let zarr_meta = Bytes::copy_from_slice(br#"{"zarr_format":3,"node_type":"array","attributes":{"foo":42},"shape":[2,2,2],"data_type":"int32","chunk_grid":{"name":"regular","configuration":{"chunk_shape":[1,1,1]}},"chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},"fill_value":0,"codecs":[{"name":"mycodec","configuration":{"foo":42}}],"storage_transformers":[{"name":"mytransformer","configuration":{"bar":43}}],"dimension_names":["x","y","t"]}"#);
        store.set("array/zarr.json", zarr_meta.clone()).await.unwrap();

        let data = Bytes::copy_from_slice(b"hello");
        store.set_if_not_exists("array/c/0/1/0", data.clone()).await.unwrap();
        assert_eq!(store.get("array/c/0/1/0", &ByteRange::ALL).await.unwrap(), data);

        let snapshot_id = store.commit("initial commit").await.unwrap();

        let new_data = Bytes::copy_from_slice(b"world");
        store.set_if_not_exists("array/c/0/1/0", new_data.clone()).await.unwrap();
        assert_eq!(store.get("array/c/0/1/0", &ByteRange::ALL).await.unwrap(), data);

        store.set("array/c/0/1/0", new_data.clone()).await.unwrap();
        assert_eq!(store.get("array/c/0/1/0", &ByteRange::ALL).await.unwrap(), new_data);

        let new_snapshot_id = store.commit("update").await.unwrap();

        store.checkout(VersionInfo::SnapshotId(snapshot_id.clone())).await.unwrap();
        assert_eq!(store.get("array/c/0/1/0", &ByteRange::ALL).await.unwrap(), data);

        store.checkout(VersionInfo::SnapshotId(new_snapshot_id)).await.unwrap();
        assert_eq!(store.get("array/c/0/1/0", &ByteRange::ALL).await.unwrap(), new_data);

        let _newest_data = Bytes::copy_from_slice(b"earth");
        store.set("array/c/0/1/0", data.clone()).await.unwrap();
        assert_eq!(store.has_uncommitted_changes().await, true);

        let result = store.checkout(VersionInfo::SnapshotId(snapshot_id.clone())).await;
        assert!(result.is_err());

        store.reset().await?;
        assert_eq!(store.get("array/c/0/1/0", &ByteRange::ALL).await.unwrap(), new_data);

        // Create a new branch and do stuff with it
        store.new_branch("dev").await?;
        store.set("array/c/0/1/0", new_data.clone()).await?;
        let dev_snapshot_id = store.commit("update dev branch").await?;
        store.checkout(VersionInfo::SnapshotId(dev_snapshot_id)).await?;
        assert_eq!(store.get("array/c/0/1/0", &ByteRange::ALL).await.unwrap(), new_data);

        let new_store_from_snapshot = Store::from_repository(
            Repository::update(Arc::clone(&storage), snapshot_id).build(),
            AccessMode::ReadWrite,
            None,
            None,
        );
        assert_eq!(
            new_store_from_snapshot.get("array/c/0/1/0", &ByteRange::ALL).await.unwrap(),
            data
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_clear() -> Result<(), Box<dyn std::error::Error>> {
        let storage: Arc<dyn Storage + Send + Sync> =
            Arc::new(ObjectStorage::new_in_memory_store(Some("prefix".into())));

        let mut store = Store::new_from_storage(Arc::clone(&storage)).await?;

        store
            .set(
                "zarr.json",
                Bytes::copy_from_slice(br#"{"zarr_format":3, "node_type":"group"}"#),
            )
            .await
            .unwrap();

        let empty: Vec<String> = Vec::new();
        store.clear().await?;
        assert_eq!(
            store.list_prefix("").await?.try_collect::<Vec<String>>().await?,
            empty
        );

        store
            .set(
                "zarr.json",
                Bytes::copy_from_slice(br#"{"zarr_format":3, "node_type":"group"}"#),
            )
            .await
            .unwrap();
        store
            .set(
                "group/zarr.json",
                Bytes::copy_from_slice(br#"{"zarr_format":3, "node_type":"group"}"#),
            )
            .await
            .unwrap();
        let zarr_meta = Bytes::copy_from_slice(br#"{"zarr_format":3,"node_type":"array","attributes":{"foo":42},"shape":[2,2,2],"data_type":"int32","chunk_grid":{"name":"regular","configuration":{"chunk_shape":[1,1,1]}},"chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},"fill_value":0,"codecs":[{"name":"mycodec","configuration":{"foo":42}}],"storage_transformers":[{"name":"mytransformer","configuration":{"bar":43}}],"dimension_names":["x","y","t"]}"#);
        let new_data = Bytes::copy_from_slice(b"world");
        store.set("array/zarr.json", zarr_meta.clone()).await.unwrap();
        store.set("group/array/zarr.json", zarr_meta.clone()).await.unwrap();
        store.set("array/c/1/0/0", new_data.clone()).await.unwrap();
        store.set("group/array/c/1/0/0", new_data.clone()).await.unwrap();

        let _ = store.commit("initial commit").await.unwrap();

        store
            .set(
                "group/group2/zarr.json",
                Bytes::copy_from_slice(br#"{"zarr_format":3, "node_type":"group"}"#),
            )
            .await
            .unwrap();
        store.set("group/group2/array/zarr.json", zarr_meta.clone()).await.unwrap();
        store.set("group/group2/array/c/1/0/0", new_data.clone()).await.unwrap();

        store.clear().await?;

        assert_eq!(
            store.list_prefix("").await?.try_collect::<Vec<String>>().await?,
            empty
        );

        let empty_snap = store.commit("no content commit").await.unwrap();

        assert_eq!(
            store.list_prefix("").await?.try_collect::<Vec<String>>().await?,
            empty
        );

        let store = Store::from_repository(
            Repository::update(Arc::clone(&storage), empty_snap).build(),
            AccessMode::ReadWrite,
            None,
            None,
        );
        assert_eq!(
            store.list_prefix("").await?.try_collect::<Vec<String>>().await?,
            empty
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_access_mode() {
        let storage: Arc<dyn Storage + Send + Sync> =
            Arc::new(ObjectStorage::new_in_memory_store(Some("prefix".into())));

        let writeable_store =
            Store::new_from_storage(Arc::clone(&storage)).await.unwrap();
        assert_eq!(writeable_store.access_mode(), &AccessMode::ReadWrite);

        writeable_store
            .set(
                "zarr.json",
                Bytes::copy_from_slice(br#"{"zarr_format":3, "node_type":"group"}"#),
            )
            .await
            .unwrap();

        let readable_store = writeable_store.with_access_mode(AccessMode::ReadOnly);
        assert_eq!(readable_store.access_mode(), &AccessMode::ReadOnly);

        let result = readable_store
            .set(
                "zarr.json",
                Bytes::copy_from_slice(br#"{"zarr_format":3, "node_type":"group"}"#),
            )
            .await;
        let correct_error = matches!(result, Err(StoreError::ReadOnly { .. }));
        assert!(correct_error);

        readable_store.get("zarr.json", &ByteRange::ALL).await.unwrap();
    }

    #[test]
    fn test_store_config_deserialization() -> Result<(), Box<dyn std::error::Error>> {
        let expected = ConsolidatedStore {
            storage: StorageConfig::LocalFileSystem { root: "/tmp/test".into() },
            repository: RepositoryConfig {
                inline_chunk_threshold_bytes: Some(128),
                version: Some(VersionInfo::SnapshotId(SnapshotId::new([
                    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11,
                ]))),
                unsafe_overwrite_refs: Some(true),
                change_set_bytes: None,
                virtual_ref_config: None,
            },
            config: Some(StoreOptions { get_partial_values_concurrency: 100 }),
        };

        let json = r#"
            {"storage": {"type": "local_filesystem", "root":"/tmp/test"},
             "repository": {
                "version": {"snapshot_id":"000G40R40M30E209185G"},
                "inline_chunk_threshold_bytes":128,
                "unsafe_overwrite_refs":true
             },
             "config": {
                "get_partial_values_concurrency": 100
             }
            }
        "#;
        assert_eq!(expected, serde_json::from_str(json)?);

        let json = r#"
            {"storage":
                {"type": "local_filesystem", "root":"/tmp/test"},
             "repository": {
                "version": null,
                "inline_chunk_threshold_bytes": null,
                "unsafe_overwrite_refs":null
             }}
        "#;
        assert_eq!(
            ConsolidatedStore {
                repository: RepositoryConfig {
                    version: None,
                    inline_chunk_threshold_bytes: None,
                    unsafe_overwrite_refs: None,
                    change_set_bytes: None,
                    virtual_ref_config: None,
                },
                config: None,
                ..expected.clone()
            },
            serde_json::from_str(json)?
        );

        let json = r#"
            {"storage":
                {"type": "local_filesystem", "root":"/tmp/test"},
             "repository": {}
            }
        "#;
        assert_eq!(
            ConsolidatedStore {
                repository: RepositoryConfig {
                    version: None,
                    inline_chunk_threshold_bytes: None,
                    unsafe_overwrite_refs: None,
                    change_set_bytes: None,
                    virtual_ref_config: None,
                },
                config: None,
                ..expected.clone()
            },
            serde_json::from_str(json)?
        );

        let json = r#"
            {"storage":{"type": "in_memory", "prefix": "prefix"},
             "repository": {}
            }
        "#;
        assert_eq!(
            ConsolidatedStore {
                repository: RepositoryConfig {
                    version: None,
                    inline_chunk_threshold_bytes: None,
                    unsafe_overwrite_refs: None,
                    change_set_bytes: None,
                    virtual_ref_config: None,
                },
                storage: StorageConfig::InMemory { prefix: Some("prefix".to_string()) },
                config: None,
            },
            serde_json::from_str(json)?
        );

        let json = r#"
            {"storage":{"type": "in_memory"},
             "repository": {}
            }
        "#;
        assert_eq!(
            ConsolidatedStore {
                repository: RepositoryConfig {
                    version: None,
                    inline_chunk_threshold_bytes: None,
                    unsafe_overwrite_refs: None,
                    change_set_bytes: None,
                    virtual_ref_config: None,
                },
                storage: StorageConfig::InMemory { prefix: None },
                config: None,
            },
            serde_json::from_str(json)?
        );

        let json = r#"
            {"storage":{"type": "s3", "bucket":"test", "prefix":"root"},
             "repository": {}
            }
        "#;
        assert_eq!(
            ConsolidatedStore {
                repository: RepositoryConfig {
                    version: None,
                    inline_chunk_threshold_bytes: None,
                    unsafe_overwrite_refs: None,
                    change_set_bytes: None,
                    virtual_ref_config: None,
                },
                storage: StorageConfig::S3ObjectStore {
                    bucket: String::from("test"),
                    prefix: String::from("root"),
                    config: None,
                },
                config: None,
            },
            serde_json::from_str(json)?
        );

        let json = r#"
        {"storage":{
             "type": "s3",
             "bucket":"test",
             "prefix":"root",
             "credentials":{
                 "type":"static",
                 "access_key_id":"my-key",
                 "secret_access_key":"my-secret-key"
             },
             "endpoint":"http://localhost:9000",
             "allow_http": true
         },
         "repository": {}
        }
    "#;
        assert_eq!(
            ConsolidatedStore {
                repository: RepositoryConfig {
                    version: None,
                    inline_chunk_threshold_bytes: None,
                    unsafe_overwrite_refs: None,
                    change_set_bytes: None,
                    virtual_ref_config: None,
                },
                storage: StorageConfig::S3ObjectStore {
                    bucket: String::from("test"),
                    prefix: String::from("root"),
                    config: Some(S3Config {
                        region: None,
                        endpoint: Some(String::from("http://localhost:9000")),
                        credentials: S3Credentials::Static(StaticS3Credentials {
                            access_key_id: String::from("my-key"),
                            secret_access_key: String::from("my-secret-key"),
                            session_token: None,
                        }),
                        allow_http: true,
                    })
                },
                config: None,
            },
            serde_json::from_str(json)?
        );

        Ok(())
    }
}
