use itertools::Itertools as _;
use std::{
    collections::{BTreeSet, HashMap, HashSet},
    future::ready,
    ops::RangeBounds,
    sync::Arc,
};

use async_recursion::async_recursion;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use err_into::ErrorInto as _;
use futures::{
    Stream, StreamExt, TryStreamExt,
    stream::{self, FuturesOrdered, FuturesUnordered},
};
use regex::bytes::Regex;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::{task::JoinError, try_join};
use tracing::{Instrument, debug, error, instrument, trace};

use crate::{
    Storage, StorageError,
    asset_manager::AssetManager,
    config::{Credentials, ManifestPreloadCondition, RepositoryConfig},
    error::ICError,
    format::{
        IcechunkFormatError, IcechunkFormatErrorKind, ManifestId, NodeId, Path,
        SnapshotId,
        format_constants::SpecVersionBin,
        repo_info::RepoInfo,
        snapshot::{
            ManifestFileInfo, NodeData, Snapshot, SnapshotInfo, SnapshotProperties,
        },
        transaction_log::{Diff, DiffBuilder},
    },
    refs::{
        Ref, RefError, RefErrorKind, fetch_branch_tip, fetch_tag, list_branches,
        list_tags,
    },
    session::{Session, SessionErrorKind, SessionResult},
    storage::{self, StorageErrorKind, VersionedFetchResult, VersionedUpdateResult},
    virtual_chunks::VirtualChunkResolver,
};

#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum VersionInfo {
    SnapshotId(SnapshotId),
    TagRef(String),
    BranchTipRef(String),
    AsOf { branch: String, at: DateTime<Utc> },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RefVersionInfo {
    SnapshotId(SnapshotId),
    TagRef(String),
    BranchTipRef(String),
}

impl TryFrom<&VersionInfo> for RefVersionInfo {
    type Error = (String, DateTime<Utc>);

    fn try_from(value: &VersionInfo) -> Result<Self, Self::Error> {
        match value {
            VersionInfo::SnapshotId(id) => Ok(RefVersionInfo::SnapshotId(id.clone())),
            VersionInfo::TagRef(name) => Ok(RefVersionInfo::TagRef(name.clone())),
            VersionInfo::BranchTipRef(name) => {
                Ok(RefVersionInfo::BranchTipRef(name.clone()))
            }
            VersionInfo::AsOf { branch, at } => Err((branch.clone(), *at)),
        }
    }
}

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum RepositoryErrorKind {
    #[error(transparent)]
    StorageError(StorageErrorKind),
    #[error(transparent)]
    FormatError(IcechunkFormatErrorKind),
    #[error(transparent)]
    Ref(RefErrorKind),
    #[error("snapshot not found: `{id}`")]
    SnapshotNotFound { id: SnapshotId },
    #[error("branch {branch} does not have a snapshots before or at {at}")]
    InvalidAsOfSpec { branch: String, at: DateTime<Utc> },
    #[error("invalid snapshot id: `{0}`")]
    InvalidSnapshotId(String),
    #[error("tag error: `{0}`")]
    Tag(String),
    #[error("repositories can only be created in clean prefixes")]
    ParentDirectoryNotClean,
    #[error("the repository doesn't exist")]
    RepositoryDoesntExist,
    #[error("error in repository serialization")]
    SerializationError(#[from] Box<rmp_serde::encode::Error>),
    #[error("error in repository deserialization")]
    DeserializationError(#[from] Box<rmp_serde::decode::Error>),
    #[error(
        "error finding conflicting path for node `{0}`, this probably indicades a bug in `rebase`"
    )]
    ConflictingPathNotFound(NodeId),
    #[error("error in config deserialization")]
    ConfigDeserializationError(#[from] serde_yaml_ng::Error),
    #[error("config was updated by other session")]
    ConfigWasUpdated,
    #[error("branch update conflict: `({expected_parent:?}) != ({actual_parent:?})`")]
    Conflict { expected_parent: Option<SnapshotId>, actual_parent: Option<SnapshotId> },
    #[error("repo info object was updated after this session started")]
    RepoInfoUpdated,
    #[error("I/O error")]
    IOError(#[from] std::io::Error),
    #[error("a concurrent task failed")]
    ConcurrencyError(#[from] JoinError),
    #[error("main branch cannot be deleted")]
    CannotDeleteMain,
    #[error("the storage used by this Icechunk repository is read-only: {0}")]
    ReadonlyStorage(String),
    #[error("unexpected error: {0}")]
    Other(String),
}

pub type RepositoryError = ICError<RepositoryErrorKind>;

// it would be great to define this impl in error.rs, but it conflicts with the blanket
// `impl From<T> for T`
impl<E> From<E> for RepositoryError
where
    E: Into<RepositoryErrorKind>,
{
    fn from(value: E) -> Self {
        Self::new(value.into())
    }
}

impl From<StorageError> for RepositoryError {
    fn from(value: StorageError) -> Self {
        Self::with_context(RepositoryErrorKind::StorageError(value.kind), value.context)
    }
}

impl From<RefError> for RepositoryError {
    fn from(value: RefError) -> Self {
        Self::with_context(RepositoryErrorKind::Ref(value.kind), value.context)
    }
}

impl From<IcechunkFormatError> for RepositoryError {
    fn from(value: IcechunkFormatError) -> Self {
        Self::with_context(RepositoryErrorKind::FormatError(value.kind), value.context)
    }
}

pub type RepositoryResult<T> = Result<T, RepositoryError>;

#[derive(Debug, Serialize, Deserialize)]
pub struct Repository {
    spec_version: SpecVersionBin,
    config: RepositoryConfig,
    storage_settings: storage::Settings,
    config_version: storage::VersionInfo,
    storage: Arc<dyn Storage + Send + Sync>,
    asset_manager: Arc<AssetManager>,
    virtual_resolver: Arc<VirtualChunkResolver>,
    authorized_virtual_containers: HashMap<String, Option<Credentials>>,
    default_commit_metadata: SnapshotProperties,
}

impl Repository {
    #[instrument(skip_all)]
    pub async fn create(
        config: Option<RepositoryConfig>,
        storage: Arc<dyn Storage + Send + Sync>,
        authorize_virtual_chunk_access: HashMap<String, Option<Credentials>>,
    ) -> RepositoryResult<Self> {
        debug!("Creating Repository");
        if !storage.can_write() {
            return Err(RepositoryErrorKind::ReadonlyStorage(
                "Cannot create repository".to_string(),
            )
            .into());
        }

        let has_overriden_config = match config {
            Some(ref config) => config != &RepositoryConfig::default(),
            None => false,
        };
        // Merge the given config with the defaults
        let config =
            config.map(|c| RepositoryConfig::default().merge(c)).unwrap_or_default();
        let storage_settings =
            config.storage().cloned().unwrap_or_else(|| storage.default_settings());

        let asset_manager = Arc::new(AssetManager::new_with_config(
            Arc::clone(&storage),
            storage_settings.clone(),
            config.caching(),
            config.compression().level(),
        ));

        if !storage.root_is_clean().await? {
            return Err(RepositoryErrorKind::ParentDirectoryNotClean.into());
        }

        let asset_manager_c = Arc::clone(&asset_manager);
        let create_repo_info = async move {
            // On create we need to create the default branch
            let new_snapshot = Arc::new(Snapshot::initial()?);
            asset_manager_c.write_snapshot(Arc::clone(&new_snapshot)).await?;

            let snap_info = new_snapshot.as_ref().try_into()?;
            let repo_info = Arc::new(RepoInfo::initial(snap_info));
            let _ = asset_manager_c
                .update_repo_info(
                    Arc::clone(&repo_info),
                    &storage::VersionInfo::for_creation(),
                )
                .await?;
            Ok::<_, RepositoryError>(())
        }
        .in_current_span();

        let storage_c = Arc::clone(&storage);
        let config_c = config.clone();
        let update_config = async move {
            if has_overriden_config {
                let version = Repository::store_config(
                    storage_c.as_ref(),
                    &config_c,
                    &storage::VersionInfo::for_creation(),
                )
                .await?;
                Ok::<_, RepositoryError>(version)
            } else {
                Ok(storage::VersionInfo::for_creation())
            }
        }
        .in_current_span();

        let (_, config_version) = try_join!(create_repo_info, update_config)?;

        debug_assert!(Self::exists(Arc::clone(&storage)).await.unwrap_or(false));
        Self::new(
            SpecVersionBin::current(),
            config,
            config_version,
            storage,
            asset_manager,
            authorize_virtual_chunk_access,
        )
    }

    #[instrument(skip_all)]
    pub async fn open(
        config: Option<RepositoryConfig>,
        storage: Arc<dyn Storage + Send + Sync>,
        authorize_virtual_chunk_access: HashMap<String, Option<Credentials>>,
    ) -> RepositoryResult<Self> {
        debug!("Opening Repository");
        let storage_c = Arc::clone(&storage);
        let fetch_config = tokio::spawn(
            async move { Self::fetch_config(storage_c.as_ref()).await }.in_current_span(),
        );

        let storage_c = Arc::clone(&storage);
        let find_spec_version = tokio::spawn(Self::repo_spec_version(storage_c));

        let (config_res, spec_version_res) = try_join!(fetch_config, find_spec_version)?;

        // fail if repo doesn't exist
        let version = match spec_version_res {
            Ok(Some(v)) => Ok(v),
            Ok(None) => {
                Err(RepositoryError::from(RepositoryErrorKind::RepositoryDoesntExist))
            }
            Err(err) => Err(err),
        }?;

        if let Some((default_config, config_version)) = config_res? {
            // Merge the given config with the defaults
            let config =
                config.map(|c| default_config.merge(c)).unwrap_or(default_config);

            let storage_settings =
                config.storage().cloned().unwrap_or_else(|| storage.default_settings());

            let asset_manager = Arc::new(AssetManager::new_with_config(
                Arc::clone(&storage),
                storage_settings.clone(),
                config.caching(),
                config.compression().level(),
            ));

            Self::new(
                version,
                config,
                config_version,
                storage,
                asset_manager,
                authorize_virtual_chunk_access,
            )
        } else {
            let config = config.unwrap_or_default();
            let storage_settings =
                config.storage().cloned().unwrap_or_else(|| storage.default_settings());

            let asset_manager = Arc::new(AssetManager::new_with_config(
                Arc::clone(&storage),
                storage_settings.clone(),
                config.caching(),
                config.compression().level(),
            ));
            Self::new(
                version,
                config,
                storage::VersionInfo::for_creation(),
                storage,
                asset_manager,
                authorize_virtual_chunk_access,
            )
        }
    }

    pub async fn open_or_create(
        config: Option<RepositoryConfig>,
        storage: Arc<dyn Storage + Send + Sync>,
        authorize_virtual_chunk_access: HashMap<String, Option<Credentials>>,
    ) -> RepositoryResult<Self> {
        if Self::exists(Arc::clone(&storage)).await? {
            Self::open(config, storage, authorize_virtual_chunk_access).await
        } else {
            Self::create(config, storage, authorize_virtual_chunk_access).await
        }
    }

    fn new(
        spec_version: SpecVersionBin,
        config: RepositoryConfig,
        config_version: storage::VersionInfo,
        storage: Arc<dyn Storage + Send + Sync>,
        asset_manager: Arc<AssetManager>,
        authorized_virtual_containers: HashMap<String, Option<Credentials>>,
    ) -> RepositoryResult<Self> {
        let containers = config.virtual_chunk_containers().cloned();
        validate_credentials(&config, &authorized_virtual_containers)?;
        let storage_settings =
            config.storage().cloned().unwrap_or_else(|| storage.default_settings());
        let virtual_resolver = Arc::new(VirtualChunkResolver::new(
            containers,
            authorized_virtual_containers.clone(),
            storage_settings.clone(),
        ));
        Ok(Self {
            spec_version,
            config,
            config_version,
            storage,
            storage_settings,
            virtual_resolver,
            asset_manager,
            authorized_virtual_containers,
            default_commit_metadata: SnapshotProperties::default(),
        })
    }

    #[instrument(skip_all)]
    pub async fn exists(
        storage: Arc<dyn Storage + Send + Sync>,
    ) -> RepositoryResult<bool> {
        Ok(Self::repo_spec_version(storage).await?.is_some())
    }

    #[instrument(skip_all)]
    async fn repo_spec_version(
        storage: Arc<dyn Storage + Send + Sync>,
    ) -> RepositoryResult<Option<SpecVersionBin>> {
        let storage_c = Arc::clone(&storage);
        let is_v1 = async move {
            match fetch_branch_tip(
                storage_c.as_ref(),
                &storage_c.default_settings(),
                Ref::DEFAULT_BRANCH,
            )
            .await
            {
                Ok(_) => Ok(true),
                Err(RefError { kind: RefErrorKind::RefNotFound(_), .. }) => Ok(false),
                Err(err) => Err(err),
            }
        }
        .in_current_span();

        let after_v1 = async move {
            let temp_asset_manager = Arc::new(AssetManager::new_no_cache(
                Arc::clone(&storage),
                storage.default_settings(),
                1, // we are only reading, compression doesn't matter
            ));

            let res = temp_asset_manager.fetch_repo_info().await;
            Ok(res.and_then(|(ri, _)| ri.spec_version().err_into()))
        }
        .in_current_span();

        let (is_v1, after_v1) = try_join!(is_v1, after_v1)?;
        match after_v1 {
            // FIXME: what to do if we have both
            Ok(v) => Ok(Some(v)),
            Err(_) => {
                if is_v1 {
                    Ok(Some(SpecVersionBin::V1dot0))
                } else {
                    Ok(None)
                }
            }
        }
    }

    /// Reopen the repository changing its config and or virtual chunk credentials
    #[instrument(skip_all)]
    pub fn reopen(
        &self,
        config: Option<RepositoryConfig>,
        authorize_virtual_chunk_access: Option<HashMap<String, Option<Credentials>>>,
    ) -> RepositoryResult<Self> {
        // Merge the given config with the current config
        let config = config
            .map(|c| self.config().merge(c))
            .unwrap_or_else(|| self.config().clone());

        Self::new(
            self.spec_version,
            config,
            self.config_version.clone(),
            Arc::clone(&self.storage),
            Arc::clone(&self.asset_manager),
            authorize_virtual_chunk_access
                .unwrap_or_else(|| self.authorized_virtual_containers.clone()),
        )
    }

    #[instrument(skip(bytes))]
    pub fn from_bytes(bytes: Vec<u8>) -> RepositoryResult<Self> {
        rmp_serde::from_slice(&bytes).map_err(Box::new).err_into()
    }

    #[instrument(skip(self))]
    pub fn as_bytes(&self) -> RepositoryResult<Vec<u8>> {
        rmp_serde::to_vec(self).map_err(Box::new).err_into()
    }

    #[instrument(skip_all)]
    pub async fn fetch_config(
        storage: &(dyn Storage + Send + Sync),
    ) -> RepositoryResult<Option<(RepositoryConfig, storage::VersionInfo)>> {
        match storage.fetch_config(&storage.default_settings()).await? {
            VersionedFetchResult::Found { result, version } => {
                let config = serde_yaml_ng::from_slice(&result)?;
                Ok(Some((config, version)))
            }
            VersionedFetchResult::NotFound => Ok(None),
        }
    }

    #[instrument(skip_all)]
    pub async fn save_config(&self) -> RepositoryResult<storage::VersionInfo> {
        Repository::store_config(
            self.storage().as_ref(),
            self.config(),
            &self.config_version,
        )
        .await
    }

    #[instrument(skip_all)]
    pub fn set_default_commit_metadata(&mut self, metadata: SnapshotProperties) {
        self.default_commit_metadata = metadata;
    }

    #[instrument(skip_all)]
    pub fn default_commit_metadata(&self) -> &SnapshotProperties {
        &self.default_commit_metadata
    }

    #[instrument(skip(storage, config))]
    pub(crate) async fn store_config(
        storage: &(dyn Storage + Send + Sync),
        config: &RepositoryConfig,
        previous_version: &storage::VersionInfo,
    ) -> RepositoryResult<storage::VersionInfo> {
        if !storage.can_write() {
            return Err(RepositoryErrorKind::ReadonlyStorage(
                "Cannot save configuration".to_string(),
            )
            .into());
        }

        let bytes = Bytes::from(serde_yaml_ng::to_string(config)?);
        let storage_settings =
            config.storage().cloned().unwrap_or_else(|| storage.default_settings());
        match storage.update_config(&storage_settings, bytes, previous_version).await? {
            VersionedUpdateResult::Updated { new_version } => Ok(new_version),
            VersionedUpdateResult::NotOnLatestVersion => {
                Err(RepositoryErrorKind::ConfigWasUpdated.into())
            }
        }
    }

    pub fn config(&self) -> &RepositoryConfig {
        &self.config
    }

    pub fn storage_settings(&self) -> &storage::Settings {
        &self.storage_settings
    }

    pub fn storage(&self) -> &Arc<dyn Storage + Send + Sync> {
        &self.storage
    }

    pub fn asset_manager(&self) -> &Arc<AssetManager> {
        &self.asset_manager
    }

    /// Returns the sequence of parents of the current session, in order of latest first.
    #[instrument(skip(self))]
    async fn snapshot_ancestry_v1(
        &self,
        snapshot_id: &SnapshotId,
    ) -> RepositoryResult<impl Stream<Item = RepositoryResult<SnapshotInfo>> + '_ + use<'_>>
    {
        Arc::clone(&self.asset_manager).snapshot_info_ancestry(snapshot_id).await
    }

    #[instrument(skip(self))]
    pub async fn ancestry<'a>(
        &'a self,
        version: &'a VersionInfo,
    ) -> RepositoryResult<
        impl Stream<Item = RepositoryResult<SnapshotInfo>> + Send + 'a + use<'a>,
    > {
        match self.spec_version {
            SpecVersionBin::V1dot0 => Ok(self.ancestry_v1(version).await?.left_stream()),
            SpecVersionBin::V2dot0 => {
                let iter = self.ancestry_v2(version).await?;
                Ok(stream::iter(iter).right_stream())
            }
        }
    }

    /// Returns the sequence of parents of the snapshot pointed by the given version
    #[async_recursion(?Send)]
    #[instrument(skip(self))]
    async fn ancestry_v1<'a>(
        &'a self,
        version: &VersionInfo,
    ) -> RepositoryResult<
        impl Stream<Item = RepositoryResult<SnapshotInfo>> + Send + 'a + use<'a>,
    > {
        let snapshot_id = self.resolve_version(version).await?;
        self.snapshot_ancestry_v1(&snapshot_id).await
    }

    async fn ancestry_v2(
        &self,
        version: &VersionInfo,
    ) -> RepositoryResult<impl Iterator<Item = RepositoryResult<SnapshotInfo>> + Send>
    {
        let (repo_info, _) = self.get_repo_info().await?;
        let snapshot_id = self.resolve_version_v2(&repo_info, version).await?;
        let it = unsafe {
            let repo_info_ref = &*Arc::as_ptr(&repo_info);
            Box::new(repo_info_ref.ancestry(&snapshot_id)?.map(|e| e.err_into()))
        };
        Ok(AcestryIterator { _repo_info: repo_info.clone(), it })
    }

    #[instrument(skip(self))]
    async fn ancestry_ref<'a>(
        &'a self,
        version: &RefVersionInfo,
    ) -> RepositoryResult<impl Stream<Item = RepositoryResult<SnapshotInfo>> + 'a + use<'a>>
    {
        let snapshot_id = self.resolve_ref_version_v1(version).await?;
        self.snapshot_ancestry_v1(&snapshot_id).await
    }

    /// Create a new branch in the repository at the given snapshot id
    #[instrument(skip(self))]
    pub async fn create_branch(
        &self,
        branch_name: &str,
        snapshot_id: &SnapshotId,
    ) -> RepositoryResult<()> {
        if !self.storage.can_write() {
            return Err(RepositoryErrorKind::ReadonlyStorage(
                "Cannot create branch".to_string(),
            )
            .into());
        }
        if self.spec_version != SpecVersionBin::current() {
            return Err(RepositoryErrorKind::ReadonlyStorage(
                // FIXME:
                "This repository is incompatible with Icechunk version you are using"
                    .to_string(),
            )
            .into());
        }

        let (repo_info, version) = self.get_repo_info().await?;
        raise_if_invalid_snapshot_id_v2(repo_info.as_ref(), snapshot_id)?;
        let new_repo_info = match repo_info.add_branch(branch_name, snapshot_id) {
            Ok(new) => Ok(new),
            Err(IcechunkFormatError {
                kind:
                    IcechunkFormatErrorKind::BranchAlreadyExists {
                        snapshot_id: actual_parent,
                        ..
                    },
                ..
            }) => Err(RepositoryError::from(RepositoryErrorKind::Conflict {
                expected_parent: None,
                actual_parent: Some(actual_parent),
            })),
            Err(err) => Err(err.into()),
        }?;

        let _ = self
            .asset_manager
            .update_repo_info(Arc::new(new_repo_info), &version)
            .await?;
        Ok(())
    }

    /// List all branches in the repository.
    #[instrument(skip(self))]
    async fn list_branches_v1(&self) -> RepositoryResult<BTreeSet<String>> {
        let branches =
            list_branches(self.storage.as_ref(), &self.storage_settings).await?;
        Ok(branches)
    }

    #[instrument(skip(self))]
    async fn list_branches_v2(&self) -> RepositoryResult<BTreeSet<String>> {
        let (ri, _) = self.get_repo_info().await?;
        Ok(ri.branch_names()?.map(|s| s.to_string()).collect())
    }

    #[instrument(skip(self))]
    pub async fn list_branches(&self) -> RepositoryResult<BTreeSet<String>> {
        match self.spec_version {
            SpecVersionBin::V1dot0 => self.list_branches_v1().await,
            SpecVersionBin::V2dot0 => self.list_branches_v2().await,
        }
    }

    /// Get the snapshot id of the tip of a branch
    #[instrument(skip(self))]
    async fn lookup_branch_v1(&self, branch: &str) -> RepositoryResult<SnapshotId> {
        let branch_version =
            fetch_branch_tip(self.storage.as_ref(), &self.storage_settings, branch)
                .await?;
        Ok(branch_version.snapshot)
    }

    #[instrument(skip(self))]
    async fn lookup_branch_v2(&self, branch: &str) -> RepositoryResult<SnapshotId> {
        let (ri, _) = self.get_repo_info().await?;
        match ri.resolve_branch(branch) {
            Ok(snap) => Ok(snap),
            Err(IcechunkFormatError {
                kind: IcechunkFormatErrorKind::BranchNotFound { .. },
                ..
            }) => Err(RepositoryError::from(RefError::from(RefErrorKind::RefNotFound(
                branch.to_string(),
            )))),
            Err(err) => Err(err.into()),
        }
    }

    #[instrument(skip(self))]
    pub async fn lookup_branch(&self, branch: &str) -> RepositoryResult<SnapshotId> {
        match self.spec_version {
            SpecVersionBin::V1dot0 => self.lookup_branch_v1(branch).await,
            SpecVersionBin::V2dot0 => self.lookup_branch_v2(branch).await,
        }
    }

    #[instrument(skip(self))]
    async fn lookup_snapshot_v1(
        &self,
        snapshot_id: &SnapshotId,
    ) -> RepositoryResult<SnapshotInfo> {
        self.asset_manager.fetch_snapshot_info(snapshot_id).await
    }

    #[instrument(skip(self))]
    async fn lookup_snapshot_v2(
        &self,
        snapshot_id: &SnapshotId,
    ) -> RepositoryResult<SnapshotInfo> {
        let (ri, _) = self.get_repo_info().await?;
        Ok(ri.find_snapshot(snapshot_id)?)
    }

    #[instrument(skip(self))]
    pub async fn lookup_snapshot(
        &self,
        snapshot_id: &SnapshotId,
    ) -> RepositoryResult<SnapshotInfo> {
        match self.spec_version {
            SpecVersionBin::V1dot0 => self.lookup_snapshot_v1(snapshot_id).await,
            SpecVersionBin::V2dot0 => self.lookup_snapshot_v2(snapshot_id).await,
        }
    }

    #[instrument(skip(self))]
    pub async fn lookup_manifest_files(
        &self,
        snapshot_id: &SnapshotId,
    ) -> RepositoryResult<impl Iterator<Item = ManifestFileInfo>> {
        let snap = self.asset_manager.fetch_snapshot(snapshot_id).await?;
        Ok(snap.manifest_files().collect::<Vec<_>>().into_iter())
    }

    #[instrument(skip(self))]
    pub async fn reset_branch(
        &self,
        branch: &str,
        snapshot_id: &SnapshotId,
    ) -> RepositoryResult<()> {
        if !self.storage.can_write() {
            return Err(RepositoryErrorKind::ReadonlyStorage(
                "Cannot create branch".to_string(),
            )
            .into());
        }
        if self.spec_version != SpecVersionBin::current() {
            return Err(RepositoryErrorKind::ReadonlyStorage(
                // FIXME:
                "This repository is incompatible with Icechunk version you are using"
                    .to_string(),
            )
            .into());
        }
        let (ri, version) = self.get_repo_info().await?;
        match ri.update_branch(branch, snapshot_id) {
            Ok(new_ri) => {
                let _ = self
                    .asset_manager
                    .update_repo_info(Arc::new(new_ri), &version)
                    .await?;
                Ok(())
            }
            Err(IcechunkFormatError {
                kind: IcechunkFormatErrorKind::BranchNotFound { .. },
                ..
            }) => {
                Err(RefError::from(RefErrorKind::RefNotFound(branch.to_string())).into())
            }
            Err(err) => Err(err.into()),
        }
    }

    /// Delete a branch from the repository.
    /// This will remove the branch reference and the branch history. It will not remove the
    /// chunks or snapshots associated with the branch.
    #[instrument(skip(self))]
    pub async fn delete_branch(&self, branch: &str) -> RepositoryResult<()> {
        if !self.storage.can_write() {
            return Err(RepositoryErrorKind::ReadonlyStorage(
                "Cannot delete branch".to_string(),
            )
            .into());
        }
        if self.spec_version != SpecVersionBin::current() {
            return Err(RepositoryErrorKind::ReadonlyStorage(
                // FIXME:
                "This repository is incompatible with Icechunk version you are using"
                    .to_string(),
            )
            .into());
        }
        if branch != Ref::DEFAULT_BRANCH {
            let (ri, version) = self.get_repo_info().await?;
            match ri.delete_branch(branch) {
                Ok(new_ri) => {
                    let _ = self
                        .asset_manager
                        .update_repo_info(Arc::new(new_ri), &version)
                        .await?;
                    Ok(())
                }
                Err(IcechunkFormatError {
                    kind: IcechunkFormatErrorKind::BranchNotFound { .. },
                    ..
                }) => {
                    Err(RefError::from(RefErrorKind::RefNotFound(branch.to_string()))
                        .into())
                }
                Err(err) => Err(err.into()),
            }
        } else {
            Err(RepositoryErrorKind::CannotDeleteMain.into())
        }
    }

    /// Delete a tag from the repository.
    /// This will remove the tag reference. It will not remove the
    /// chunks or snapshots associated with the tag.
    #[instrument(skip(self))]
    pub async fn delete_tag(&self, tag: &str) -> RepositoryResult<()> {
        if !self.storage.can_write() {
            return Err(RepositoryErrorKind::ReadonlyStorage(
                "Cannot delete tag".to_string(),
            )
            .into());
        }
        if self.spec_version != SpecVersionBin::current() {
            return Err(RepositoryErrorKind::ReadonlyStorage(
                // FIXME:
                "This repository is incompatible with Icechunk version you are using"
                    .to_string(),
            )
            .into());
        }
        let (ri, version) = self.get_repo_info().await?;
        match ri.delete_tag(tag) {
            Ok(new_ri) => {
                let _ = self
                    .asset_manager
                    .update_repo_info(Arc::new(new_ri), &version)
                    .await?;
                Ok(())
            }
            Err(IcechunkFormatError {
                kind: IcechunkFormatErrorKind::TagNotFound { .. },
                ..
            }) => Err(RefError::from(RefErrorKind::RefNotFound(tag.to_string())).into()),
            Err(err) => Err(err.into()),
        }
    }

    /// Create a new tag in the repository at the given snapshot id
    #[instrument(skip(self))]
    pub async fn create_tag(
        &self,
        tag_name: &str,
        snapshot_id: &SnapshotId,
    ) -> RepositoryResult<()> {
        if !self.storage.can_write() {
            return Err(RepositoryErrorKind::ReadonlyStorage(
                "Cannot create tag".to_string(),
            )
            .into());
        }
        if self.spec_version != SpecVersionBin::current() {
            return Err(RepositoryErrorKind::ReadonlyStorage(
                // FIXME:
                "This repository is incompatible with Icechunk version you are using"
                    .to_string(),
            )
            .into());
        }
        let (ri, version) = self.get_repo_info().await?;
        raise_if_invalid_snapshot_id_v2(ri.as_ref(), snapshot_id)?;

        match ri.add_tag(tag_name, snapshot_id) {
            Ok(new_ri) => {
                let _ = self
                    .asset_manager
                    .update_repo_info(Arc::new(new_ri), &version)
                    .await?;
                Ok(())
            }
            Err(IcechunkFormatError {
                kind: IcechunkFormatErrorKind::TagAlreadyExists { .. },
                ..
            }) => Err(RepositoryError::from(RefError::from(
                RefErrorKind::TagAlreadyExists(tag_name.to_string()),
            ))),
            Err(err) => Err(err.into()),
        }
    }

    /// List all tags in the repository.
    #[instrument(skip(self))]
    async fn list_tags_v1(&self) -> RepositoryResult<BTreeSet<String>> {
        let tags = list_tags(self.storage.as_ref(), &self.storage_settings).await?;
        Ok(tags)
    }

    async fn list_tags_v2(&self) -> RepositoryResult<BTreeSet<String>> {
        let (ri, _) = self.get_repo_info().await?;
        Ok(ri.tag_names()?.map(|s| s.to_string()).collect())
    }

    pub async fn list_tags(&self) -> RepositoryResult<BTreeSet<String>> {
        match self.spec_version {
            SpecVersionBin::V1dot0 => self.list_tags_v1().await,
            SpecVersionBin::V2dot0 => self.list_tags_v2().await,
        }
    }

    #[instrument(skip(self))]
    async fn lookup_tag_v1(&self, tag: &str) -> RepositoryResult<SnapshotId> {
        let ref_data =
            fetch_tag(self.storage.as_ref(), &self.storage_settings, tag).await?;
        Ok(ref_data.snapshot)
    }

    #[instrument(skip(self))]
    async fn lookup_tag_v2(&self, tag: &str) -> RepositoryResult<SnapshotId> {
        let (ri, _) = self.get_repo_info().await?;
        match ri.resolve_tag(tag) {
            Ok(snap) => Ok(snap),
            Err(IcechunkFormatError {
                kind: IcechunkFormatErrorKind::TagNotFound { .. },
                ..
            }) => Err(RepositoryError::from(RefError::from(RefErrorKind::RefNotFound(
                tag.to_string(),
            )))),
            Err(err) => Err(err.into()),
        }
    }

    #[instrument(skip(self))]
    pub async fn lookup_tag(&self, tag: &str) -> RepositoryResult<SnapshotId> {
        match self.spec_version {
            SpecVersionBin::V1dot0 => self.lookup_tag_v1(tag).await,
            SpecVersionBin::V2dot0 => self.lookup_tag_v2(tag).await,
        }
    }

    #[instrument(skip(self))]
    async fn resolve_ref_version_v1(
        &self,
        version: &RefVersionInfo,
    ) -> RepositoryResult<SnapshotId> {
        match version {
            RefVersionInfo::SnapshotId(sid) => {
                raise_if_invalid_snapshot_id_v1(
                    self.storage.as_ref(),
                    &self.storage_settings,
                    sid,
                )
                .await?;
                Ok(sid.clone())
            }
            RefVersionInfo::TagRef(tag) => {
                let ref_data =
                    fetch_tag(self.storage.as_ref(), &self.storage_settings, tag).await?;
                Ok(ref_data.snapshot)
            }
            RefVersionInfo::BranchTipRef(branch) => {
                let ref_data = fetch_branch_tip(
                    self.storage.as_ref(),
                    &self.storage_settings,
                    branch,
                )
                .await?;
                Ok(ref_data.snapshot)
            }
        }
    }

    #[instrument(skip(self))]
    async fn resolve_ref_version_v2(
        &self,
        repo_info: &RepoInfo,
        version: &RefVersionInfo,
    ) -> RepositoryResult<SnapshotId> {
        match version {
            RefVersionInfo::SnapshotId(sid) => {
                raise_if_invalid_snapshot_id_v2(repo_info, sid)?;
                Ok(sid.clone())
            }
            RefVersionInfo::TagRef(tag) => self.lookup_tag(tag).await,
            RefVersionInfo::BranchTipRef(branch) => self.lookup_branch(branch).await,
        }
    }

    async fn get_repo_info(
        &self,
    ) -> RepositoryResult<(Arc<RepoInfo>, storage::VersionInfo)> {
        self.asset_manager.fetch_repo_info().await
    }

    #[instrument(skip(self))]
    pub async fn resolve_version(
        &self,
        version: &VersionInfo,
    ) -> RepositoryResult<SnapshotId> {
        match self.spec_version {
            SpecVersionBin::V1dot0 => self.resolve_version_v1(version).await,
            SpecVersionBin::V2dot0 => {
                self.resolve_version_v2(self.get_repo_info().await?.0.as_ref(), version)
                    .await
            }
        }
    }

    #[instrument(skip(self))]
    async fn resolve_version_v1(
        &self,
        version: &VersionInfo,
    ) -> RepositoryResult<SnapshotId> {
        match version.try_into() {
            Ok(ref_version_info) => self.resolve_ref_version_v1(&ref_version_info).await,
            Err((branch, at)) => {
                let tip = RefVersionInfo::BranchTipRef(branch.clone());
                let snap = self
                    .ancestry_ref(&tip)
                    .await?
                    .try_skip_while(|parent| ready(Ok(parent.flushed_at > at)))
                    .take(1)
                    .try_collect::<Vec<_>>()
                    .await?;
                match snap.into_iter().next() {
                    Some(snap) => Ok(snap.id),
                    None => Err(RepositoryErrorKind::InvalidAsOfSpec {
                        branch: branch.clone(),
                        at,
                    }
                    .into()),
                }
            }
        }
    }

    #[instrument(skip(self))]
    async fn resolve_version_v2(
        &self,
        repo_info: &RepoInfo,
        version: &VersionInfo,
    ) -> RepositoryResult<SnapshotId> {
        match version.try_into() {
            Ok(ref_version_info) => {
                self.resolve_ref_version_v2(repo_info, &ref_version_info).await
            }
            Err((branch, at)) => {
                let snap_id = repo_info.resolve_branch(branch.as_str())?;
                let ancestry = repo_info.ancestry(&snap_id)?;
                let snap: Vec<_> = ancestry
                    .skip_while(|parent| {
                        if let Ok(parent) = parent {
                            parent.flushed_at > at
                        } else {
                            false
                        }
                    })
                    .take(1)
                    .try_collect()?;

                match snap.into_iter().next() {
                    Some(snap) => Ok(snap.id),
                    None => Err(RepositoryErrorKind::InvalidAsOfSpec {
                        branch: branch.clone(),
                        at,
                    }
                    .into()),
                }
            }
        }
    }

    #[instrument(skip(self))]
    /// Compute the diff between `from` and `to` snapshots.
    ///
    /// If `from` is not in the ancestry of `to`, `RepositoryErrorKind::BadSnapshotChainForDiff`
    /// will be returned.
    ///
    /// Result includes the diffs in `to` snapshot but not in `from`.
    pub async fn diff(
        &self,
        from: &VersionInfo,
        to: &VersionInfo,
    ) -> SessionResult<Diff> {
        let from = self.resolve_version(from).await?;
        let to = self.resolve_version(to).await?;
        let all_snaps = self
            .ancestry_ref(&RefVersionInfo::SnapshotId(to))
            .await?
            .try_take_while(|snap_info| ready(Ok(snap_info.id != from)))
            .try_collect::<Vec<_>>()
            .await?;

        if all_snaps.last().and_then(|info| info.parent_id.as_ref()) != Some(&from) {
            return Err(SessionErrorKind::BadSnapshotChainForDiff.into());
        }

        // we don't include the changes in from
        let fut: FuturesOrdered<_> = all_snaps
            .iter()
            .filter_map(|snap_info| {
                if snap_info.is_initial() {
                    None
                } else {
                    Some(
                        self.asset_manager
                            .fetch_transaction_log(&snap_info.id)
                            .in_current_span(),
                    )
                }
            })
            .collect();

        let builder = fut
            .try_fold(DiffBuilder::default(), |mut res, log| {
                res.add_changes(log.as_ref());
                ready(Ok(res))
            })
            .await?;

        if let Some(to_snap) = all_snaps.first().as_ref().map(|snap| snap.id.clone()) {
            let from_session =
                self.readonly_session(&VersionInfo::SnapshotId(from)).await?;
            let to_session =
                self.readonly_session(&VersionInfo::SnapshotId(to_snap)).await?;
            builder.to_diff(&from_session, &to_session).await
        } else {
            Err(SessionErrorKind::BadSnapshotChainForDiff.into())
        }
    }

    #[instrument(skip(self))]
    pub async fn readonly_session(
        &self,
        version: &VersionInfo,
    ) -> RepositoryResult<Session> {
        let snapshot_id = self.resolve_version(version).await?;
        let session = Session::create_readonly_session(
            self.config.clone(),
            self.storage_settings.clone(),
            self.storage.clone(),
            Arc::clone(&self.asset_manager),
            self.virtual_resolver.clone(),
            snapshot_id.clone(),
        );
        self.preload_manifests(snapshot_id);

        Ok(session)
    }

    #[instrument(skip(self))]
    pub async fn writable_session(&self, branch: &str) -> RepositoryResult<Session> {
        if !self.storage.can_write() {
            return Err(RepositoryErrorKind::ReadonlyStorage(
                "Cannot create writable_session".to_string(),
            )
            .into());
        }
        let snapshot_id = self.lookup_branch(branch).await?;

        let session = Session::create_writable_session(
            self.config.clone(),
            self.storage_settings.clone(),
            self.storage.clone(),
            Arc::clone(&self.asset_manager),
            self.virtual_resolver.clone(),
            branch.to_string(),
            snapshot_id.clone(),
            self.default_commit_metadata.clone(),
        );

        self.preload_manifests(snapshot_id);

        Ok(session)
    }

    #[instrument(skip(self))]
    fn preload_manifests(&self, snapshot_id: SnapshotId) {
        debug!("Preloading manifests");
        let asset_manager = Arc::clone(self.asset_manager());
        let preload_config = self.config().manifest().preload().clone();
        if preload_config.max_total_refs() == 0
            || matches!(preload_config.preload_if(), ManifestPreloadCondition::False)
        {
            return;
        }
        tokio::spawn(async move {
            let mut loaded_manifests: HashSet<ManifestId> = HashSet::new();
            let mut loaded_refs: u32 = 0;
            let futures = FuturesUnordered::new();
            // TODO: unnest this code
            if let Ok(snap) = asset_manager.fetch_snapshot(&snapshot_id).await {
                let snap_c = Arc::clone(&snap);
                for node in snap.iter_arc() {
                    match node {
                        Err(err) => {
                            error!(error=%err, "Error retrieving snapshot nodes");
                        }
                        Ok(node) => match node.node_data {
                            NodeData::Group => {}
                            NodeData::Array { manifests, .. } => {
                                for manifest in manifests {
                                    if !loaded_manifests.contains(&manifest.object_id) {
                                        let manifest_id = manifest.object_id;
                                        if let Some(manifest_info) =
                                            snap_c.manifest_info(&manifest_id)
                                        {
                                            if loaded_refs + manifest_info.num_chunk_refs
                                                <= preload_config.max_total_refs()
                                                && preload_config
                                                    .preload_if()
                                                    .matches(&node.path, &manifest_info)
                                            {
                                                let size_bytes = manifest_info.size_bytes;
                                                let asset_manager =
                                                    Arc::clone(&asset_manager);
                                                let manifest_id_c = manifest_id.clone();
                                                let path = node.path.clone();
                                                futures.push(async move {
                                                    trace!("Preloading manifest {} for array {}", &manifest_id_c, path);
                                                    if let Err(err) = asset_manager
                                                        .fetch_manifest(
                                                            &manifest_id_c,
                                                            size_bytes,
                                                        )
                                                        .await
                                                    {
                                                        error!(
                                                            "Failure pre-loading manifest {}: {}",
                                                            &manifest_id_c, err
                                                        );
                                                    }
                                                });
                                                loaded_manifests.insert(manifest_id);
                                                loaded_refs +=
                                                    manifest_info.num_chunk_refs;
                                            }
                                        }
                                    }
                                }
                            }
                        },
                    }
                }
                futures.collect::<()>().await;
            };
            ().in_current_span()
        });
    }
}

struct AcestryIterator {
    // we need to keep the Arc alive
    _repo_info: Arc<RepoInfo>,
    it: Box<dyn Iterator<Item = RepositoryResult<SnapshotInfo>> + Send>,
}

impl Iterator for AcestryIterator {
    type Item = RepositoryResult<SnapshotInfo>;

    fn next(&mut self) -> Option<Self::Item> {
        self.it.next()
    }
}

impl ManifestPreloadCondition {
    pub fn matches(&self, path: &Path, info: &ManifestFileInfo) -> bool {
        match self {
            ManifestPreloadCondition::Or(vec) => {
                vec.iter().any(|c| c.matches(path, info))
            }
            ManifestPreloadCondition::And(vec) => {
                vec.iter().all(|c| c.matches(path, info))
            }
            // TODO: precompile the regex
            ManifestPreloadCondition::PathMatches { regex } => Regex::new(regex)
                .map(|regex| regex.is_match(path.to_string().as_bytes()))
                .unwrap_or(false),
            // TODO: precompile the regex
            ManifestPreloadCondition::NameMatches { regex } => Regex::new(regex)
                .map(|regex| {
                    path.name()
                        .map(|name| regex.is_match(name.as_bytes()))
                        .unwrap_or(false)
                })
                .unwrap_or(false),
            ManifestPreloadCondition::NumRefs { from, to } => {
                (*from, *to).contains(&info.num_chunk_refs)
            }
            ManifestPreloadCondition::True => true,
            ManifestPreloadCondition::False => false,
        }
    }
}

fn validate_credentials(
    config: &RepositoryConfig,
    creds: &HashMap<String, Option<Credentials>>,
) -> RepositoryResult<()> {
    for (url_prefix, cred) in creds {
        if let Some(cont) = config.get_virtual_chunk_container(url_prefix) {
            if let Err(error) = cont.validate_credentials(cred.as_ref()) {
                return Err(RepositoryErrorKind::StorageError(StorageErrorKind::Other(
                    error,
                ))
                .into());
            }
        }
    }
    Ok(())
}

async fn raise_if_invalid_snapshot_id_v1(
    storage: &(dyn Storage + Send + Sync),
    storage_settings: &storage::Settings,
    snapshot_id: &SnapshotId,
) -> RepositoryResult<()> {
    storage
        .fetch_snapshot(storage_settings, snapshot_id)
        .await
        .map_err(|_| RepositoryErrorKind::SnapshotNotFound { id: snapshot_id.clone() })?;
    Ok(())
}

fn raise_if_invalid_snapshot_id_v2(
    repo_info: &RepoInfo,
    snapshot_id: &SnapshotId,
) -> RepositoryResult<()> {
    repo_info.find_snapshot(snapshot_id)?;
    Ok(())
}

#[cfg(test)]
#[allow(clippy::panic, clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::{collections::HashMap, error::Error, iter::zip, path::PathBuf, sync::Arc};

    use icechunk_macros::tokio_test;
    use itertools::enumerate;
    use storage::logging::LoggingStorage;
    use tempfile::TempDir;

    use crate::{
        Repository, Storage,
        config::{
            CachingConfig, ManifestConfig, ManifestPreloadConfig, ManifestSplitCondition,
            ManifestSplitDim, ManifestSplitDimCondition, ManifestSplittingConfig,
            RepositoryConfig,
        },
        conflicts::basic_solver::BasicConflictSolver,
        format::{
            ByteRange, ChunkIndices,
            manifest::{ChunkPayload, ManifestSplits},
            snapshot::{ArrayShape, DimensionName},
        },
        new_local_filesystem_storage,
        ops::manifests::rewrite_manifests,
        session::{SessionError, get_chunk},
        storage::new_in_memory_storage,
    };

    use super::*;

    fn ravel_multi_index(index: &[u32], shape: &[u32]) -> u32 {
        index
            .iter()
            .zip(shape.iter())
            .rev()
            .fold((0, 1), |(acc, stride), (index, size)| {
                (acc + *index * stride, stride * *size)
            })
            .0
    }

    async fn assert_manifest_count(
        storage: &Arc<dyn Storage + Send + Sync>,
        total_manifests: usize,
    ) {
        let expected = storage
            .list_manifests(&storage.default_settings())
            .await
            .unwrap()
            .count()
            .await;
        assert_eq!(
            total_manifests, expected,
            "Mismatch in manifest count: expected {expected}, but got {total_manifests}",
        );
    }

    #[tokio::test]
    async fn test_repository_persistent_config() -> Result<(), Box<dyn Error>> {
        let storage: Arc<dyn Storage + Send + Sync> = new_in_memory_storage().await?;

        let repo = Repository::create(None, Arc::clone(&storage), HashMap::new()).await?;

        // initializing a repo does not create the config file
        assert!(Repository::fetch_config(storage.as_ref()).await?.is_none());
        // it inits with the default config
        assert_eq!(repo.config(), &RepositoryConfig::default());
        // updating the persistent config create a new file with default values
        let version = repo.save_config().await?;
        assert_ne!(version, storage::VersionInfo::for_creation());
        assert_eq!(
            Repository::fetch_config(storage.as_ref()).await?.unwrap().0,
            RepositoryConfig::default()
        );

        // reload the repo changing config
        let repo = Repository::open(
            Some(RepositoryConfig {
                inline_chunk_threshold_bytes: Some(42),
                ..Default::default()
            }),
            Arc::clone(&storage),
            HashMap::new(),
        )
        .await?;

        assert_eq!(repo.config().inline_chunk_threshold_bytes(), 42);

        // update the persistent config
        let version = repo.save_config().await?;
        assert_ne!(version, storage::VersionInfo::for_creation());
        assert_eq!(
            Repository::fetch_config(storage.as_ref())
                .await?
                .unwrap()
                .0
                .inline_chunk_threshold_bytes(),
            42
        );

        // verify loading again gets the value from persistent config
        let repo = Repository::open(None, storage, HashMap::new()).await?;
        assert_eq!(repo.config().inline_chunk_threshold_bytes(), 42);

        // creating a repo we can override certain config atts:
        let storage: Arc<dyn Storage + Send + Sync> = new_in_memory_storage().await?;
        let config = RepositoryConfig {
            inline_chunk_threshold_bytes: Some(20),
            caching: Some(CachingConfig {
                num_chunk_refs: Some(21),
                ..CachingConfig::default()
            }),
            ..RepositoryConfig::default()
        };
        let repo = Repository::create(Some(config), Arc::clone(&storage), HashMap::new())
            .await?;
        assert_eq!(repo.config().inline_chunk_threshold_bytes(), 20);
        assert_eq!(repo.config().caching().num_chunk_refs(), 21);

        // reopen merges configs too
        let config = RepositoryConfig {
            caching: Some(CachingConfig {
                num_chunk_refs: Some(100),
                ..CachingConfig::default()
            }),
            ..RepositoryConfig::default()
        };
        let repo = repo.reopen(Some(config.clone()), None)?;
        assert_eq!(repo.config().inline_chunk_threshold_bytes(), 20);
        assert_eq!(repo.config().caching().num_chunk_refs(), 100);

        // and we can save the merge
        let version = repo.save_config().await?;
        assert_ne!(version, storage::VersionInfo::for_creation());
        assert_eq!(
            &Repository::fetch_config(storage.as_ref()).await?.unwrap().0,
            repo.config()
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_manage_refs() -> Result<(), Box<dyn Error>> {
        let storage: Arc<dyn Storage + Send + Sync> = new_in_memory_storage().await?;

        let repo = Repository::create(None, Arc::clone(&storage), HashMap::new()).await?;

        let initial_branches = repo.list_branches().await?;
        assert_eq!(initial_branches, BTreeSet::from(["main".into()]));

        let initial_tags = repo.list_tags().await?;
        assert_eq!(initial_tags, BTreeSet::new());

        // cannot create invalid tag
        assert!(repo.create_tag("foo", &SnapshotId::random()).await.is_err());
        assert_eq!(repo.list_tags().await?, BTreeSet::new());

        // cannot create invalid branch
        assert!(repo.create_branch("bar", &SnapshotId::random()).await.is_err());
        assert_eq!(repo.list_branches().await?, BTreeSet::from(["main".into()]));

        // Create some branches
        let initial_snapshot = repo.lookup_branch("main").await?;
        repo.create_branch("branch1", &initial_snapshot).await?;
        repo.create_branch("branch2", &initial_snapshot).await?;

        let branches = repo.list_branches().await?;
        assert_eq!(
            branches,
            BTreeSet::from([
                "main".to_string(),
                "branch1".to_string(),
                "branch2".to_string()
            ])
        );

        // Main branch cannot be deleted
        assert!(matches!(
            repo.delete_branch("main").await,
            Err(RepositoryError { kind: RepositoryErrorKind::CannotDeleteMain, .. })
        ));

        // Delete a branch
        repo.delete_branch("branch1").await?;

        let branches = repo.list_branches().await?;
        assert_eq!(branches, BTreeSet::from(["main".to_string(), "branch2".to_string()]));

        // Create some tags
        repo.create_tag("tag1", &initial_snapshot).await?;

        let tags = repo.list_tags().await?;
        assert_eq!(tags, BTreeSet::from(["tag1".to_string()]));

        // get the snapshot id of the tag
        let tag_snapshot = repo.lookup_tag("tag1").await?;
        assert_eq!(tag_snapshot, initial_snapshot);

        Ok(())
    }

    #[test]
    fn test_manifest_preload_default_condition() {
        let condition =
            RepositoryConfig::default().manifest().preload().preload_if().clone();
        // no name match
        assert!(!condition.matches(
            &"/array".try_into().unwrap(),
            &ManifestFileInfo {
                id: ManifestId::random(),
                size_bytes: 1,
                num_chunk_refs: 1
            }
        ));
        // partial match only
        assert!(!condition.matches(
            &"/nottime".try_into().unwrap(),
            &ManifestFileInfo {
                id: ManifestId::random(),
                size_bytes: 1,
                num_chunk_refs: 1
            }
        ));
        // too large to match
        assert!(!condition.matches(
            &"/time".try_into().unwrap(),
            &ManifestFileInfo {
                id: ManifestId::random(),
                size_bytes: 1,
                num_chunk_refs: 1_000_000
            }
        ));
    }

    fn reopen_repo_with_new_splitting_config(
        repo: &Repository,
        split_sizes: Option<Vec<(ManifestSplitCondition, Vec<ManifestSplitDim>)>>,
    ) -> Repository {
        let split_config = ManifestSplittingConfig { split_sizes };
        let man_config = ManifestConfig {
            preload: Some(ManifestPreloadConfig {
                max_total_refs: None,
                preload_if: None,
            }),
            splitting: Some(split_config.clone()),
        };
        let config = RepositoryConfig {
            manifest: Some(man_config),
            ..RepositoryConfig::default()
        };
        repo.reopen(Some(config), None).unwrap()
    }

    async fn create_repo_with_split_manifest_config(
        path: &Path,
        shape: &ArrayShape,
        dimension_names: &Option<Vec<DimensionName>>,
        split_config: &ManifestSplittingConfig,
        storage: Option<Arc<dyn Storage + Send + Sync>>,
    ) -> Result<Repository, Box<dyn Error>> {
        let backend: Arc<dyn Storage + Send + Sync> =
            storage.unwrap_or(new_in_memory_storage().await?);
        let storage = Arc::clone(&backend);

        let man_config = ManifestConfig {
            preload: Some(ManifestPreloadConfig {
                max_total_refs: None,
                preload_if: None,
            }),
            splitting: Some(split_config.clone()),
        };
        let config = RepositoryConfig {
            manifest: Some(man_config),
            ..RepositoryConfig::default()
        };
        let repository =
            Repository::create(Some(config), storage, HashMap::new()).await?;

        let mut session = repository.writable_session("main").await?;

        let def = Bytes::from_static(br#"{"this":"array"}"#);
        session.add_group(Path::root(), def.clone()).await?;
        session
            .add_array(path.clone(), shape.clone(), dimension_names.clone(), def.clone())
            .await?;
        session.commit("initialized", None).await?;

        Ok(repository)
    }

    #[tokio_test]
    async fn test_resize_rewrites_manifests() -> Result<(), Box<dyn Error>> {
        let storage: Arc<dyn Storage + Send + Sync> = new_in_memory_storage().await?;
        let repo = Repository::create(
            Some(RepositoryConfig {
                inline_chunk_threshold_bytes: Some(0),
                ..Default::default()
            }),
            Arc::clone(&storage),
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
        for idx in 0..4 {
            let payload = session.get_chunk_writer()(bytes.clone()).await?;
            session
                .set_chunk_ref(array_path.clone(), ChunkIndices(vec![idx]), Some(payload))
                .await?;
        }
        session.commit("first commit", None).await?;
        assert_manifest_count(&storage, 1).await;

        // Important we are not issuing any chunk deletes here (which is what Zarr does)
        // Note we are still rewriting the manifest even without chunk changes
        // GH604
        let mut session = repo.writable_session("main").await?;
        let shape2 = ArrayShape::new(vec![(2, 1)]).unwrap();
        session
            .update_array(
                &array_path,
                shape2.clone(),
                dimension_names.clone(),
                array_def.clone(),
            )
            .await?;
        session.commit("second commit", None).await?;
        assert_manifest_count(&storage, 2).await;

        // Now we expand the size, but don't write chunks.
        // No new manifests need to be written
        let mut session = repo.writable_session("main").await?;
        let shape3 = ArrayShape::new(vec![(6, 1)]).unwrap();
        session
            .update_array(
                &array_path,
                shape3.clone(),
                dimension_names.clone(),
                array_def.clone(),
            )
            .await?;
        session.commit("second commit", None).await?;
        assert_manifest_count(&storage, 2).await;

        Ok(())
    }

    #[tokio_test]
    async fn test_splits_change_in_session() -> Result<(), Box<dyn Error>> {
        let shape = ArrayShape::new(vec![(13, 1), (2, 1), (1, 1)]).unwrap();
        let dimension_names = Some(vec!["t".into(), "y".into(), "x".into()]);
        let new_dimension_names = Some(vec!["time".into(), "y".into(), "x".into()]);
        let array_path: Path = "/temperature".try_into().unwrap();
        let array_def = Bytes::from_static(br#"{"this":"other array"}"#);

        // two possible split sizes t: 3, time: 4;
        // then we rename `t` to `time` 😈
        let split_sizes = vec![
            (
                ManifestSplitCondition::PathMatches { regex: r".*".to_string() },
                vec![ManifestSplitDim {
                    condition: ManifestSplitDimCondition::DimensionName(
                        "^t$".to_string(),
                    ),
                    num_chunks: 3,
                }],
            ),
            (
                ManifestSplitCondition::PathMatches { regex: r".*".to_string() },
                vec![ManifestSplitDim {
                    condition: ManifestSplitDimCondition::DimensionName(
                        "time".to_string(),
                    ),
                    num_chunks: 4,
                }],
            ),
        ];
        let split_config = ManifestSplittingConfig { split_sizes: Some(split_sizes) };

        let backend: Arc<dyn Storage + Send + Sync> = new_in_memory_storage().await?;
        let logging = Arc::new(LoggingStorage::new(Arc::clone(&backend)));
        let storage: Arc<dyn Storage + Send + Sync> = logging.clone();
        let repository = create_repo_with_split_manifest_config(
            &array_path,
            &shape,
            &dimension_names,
            &split_config,
            Some(Arc::clone(&storage)),
        )
        .await?;

        let verify_data = async |session: &Session, offset: u32| {
            for idx in 0..12 {
                let actual = get_chunk(
                    session
                        .get_chunk_reader(
                            &array_path,
                            &ChunkIndices(vec![idx, 0, 0]),
                            &ByteRange::ALL,
                        )
                        .await
                        .unwrap(),
                )
                .await
                .unwrap()
                .unwrap();
                let expected =
                    Bytes::copy_from_slice(format!("{0}", idx + offset).as_bytes());
                assert_eq!(actual, expected);
            }
        };

        let mut session = repository.writable_session("main").await?;
        for i in 0..12 {
            session
                .set_chunk_ref(
                    array_path.clone(),
                    ChunkIndices(vec![i, 0, 0]),
                    Some(ChunkPayload::Inline(format!("{i}").into())),
                )
                .await?
        }
        verify_data(&session, 0).await;

        let node = session.get_node(&array_path).await?;
        let orig_splits = session.lookup_splits(&node.id).cloned();
        assert_eq!(
            orig_splits,
            Some(ManifestSplits::from_edges(vec![
                vec![0, 3, 6, 9, 12, 13],
                vec![0, 2],
                vec![0, 1]
            ]))
        );

        // this should update the splits
        session
            .update_array(
                &array_path,
                shape.clone(),
                new_dimension_names.clone(),
                array_def.clone(),
            )
            .await?;
        verify_data(&session, 0).await;
        let new_splits = session.lookup_splits(&node.id).cloned();
        assert_eq!(
            new_splits,
            Some(ManifestSplits::from_edges(vec![
                vec![0, 4, 8, 12, 13],
                vec![0, 2],
                vec![0, 1]
            ]))
        );

        // update data
        for i in 0..12 {
            session
                .set_chunk_ref(
                    array_path.clone(),
                    ChunkIndices(vec![i, 0, 0]),
                    Some(ChunkPayload::Inline(format!("{0}", i + 10).into())),
                )
                .await?
        }
        verify_data(&session, 10).await;

        Ok(())
    }

    #[tokio_test]
    async fn tests_manifest_rewriting_simple() -> Result<(), Box<dyn Error>> {
        let split_size = 3u32;
        let dim_size = 10u32;

        let shape = ArrayShape::new(vec![(dim_size as u64, 1)]).unwrap();
        let dimension_names = Some(vec!["t".into()]);
        let temp_path: Path = "/temperature".try_into().unwrap();
        let split_config = ManifestSplittingConfig::with_size(split_size);

        let storage: Arc<dyn Storage + Send + Sync> = new_in_memory_storage().await?;
        let repository = create_repo_with_split_manifest_config(
            &temp_path,
            &shape,
            &dimension_names,
            &split_config,
            Some(Arc::clone(&storage)),
        )
        .await?;

        let mut total_manifests = 0;
        assert_manifest_count(&storage, total_manifests).await;

        let mut session = repository.writable_session("main").await?;
        for i in 0..dim_size {
            session
                .set_chunk_ref(
                    temp_path.clone(),
                    ChunkIndices(vec![i]),
                    Some(ChunkPayload::Inline(format!("{i}").into())),
                )
                .await?
        }
        session.commit("first split", None).await?;
        total_manifests += 4;
        assert_manifest_count(&storage, total_manifests).await;

        // make sure data is correct
        let validate_data = async || {
            let new_repo = reopen_repo_with_new_splitting_config(&repository, None);
            let session = new_repo
                .readonly_session(&VersionInfo::BranchTipRef("main".to_string()))
                .await
                .unwrap();
            for i in 0..dim_size {
                let val = get_chunk(
                    session
                        .get_chunk_reader(
                            &temp_path,
                            &ChunkIndices(vec![i]),
                            &ByteRange::ALL,
                        )
                        .await
                        .unwrap(),
                )
                .await
                .unwrap()
                .unwrap();
                assert_eq!(val, Bytes::copy_from_slice(format!("{i}").as_bytes()));
            }
        };

        validate_data().await;

        // consolidate manifests together
        let split_sizes = vec![(
            ManifestSplitCondition::PathMatches { regex: r".*".to_string() },
            vec![ManifestSplitDim {
                condition: ManifestSplitDimCondition::Any,
                num_chunks: 12,
            }],
        )];

        let new_repo =
            reopen_repo_with_new_splitting_config(&repository, Some(split_sizes));

        let snap = rewrite_manifests(
            &new_repo,
            "main",
            "rewrite_manifests with split-size=12",
            None,
        )
        .await?;
        total_manifests += 1;
        assert_manifest_count(&storage, total_manifests).await;
        validate_data().await;
        assert!(
            repository
                .lookup_snapshot(&snap)
                .await?
                .metadata
                .contains_key("splitting_config")
        );

        // split manifests to smaller sizes
        let split_sizes = vec![(
            ManifestSplitCondition::PathMatches { regex: r".*".to_string() },
            vec![ManifestSplitDim {
                condition: ManifestSplitDimCondition::Any,
                num_chunks: 4,
            }],
        )];

        let new_repo =
            reopen_repo_with_new_splitting_config(&repository, Some(split_sizes));

        let snap = rewrite_manifests(
            &new_repo,
            "main",
            "rewrite_manifests with split-size=4",
            None,
        )
        .await?;
        total_manifests += 3;
        assert_manifest_count(&storage, total_manifests).await;
        validate_data().await;
        assert!(
            repository
                .lookup_snapshot(&snap)
                .await?
                .metadata
                .contains_key("splitting_config")
        );

        Ok(())
    }

    #[tokio_test]
    async fn tests_manifest_splitting_simple() -> Result<(), Box<dyn Error>> {
        let dim_size = 25u32;
        let chunk_size = 1u32;
        let split_size = 3u32;

        let shape =
            ArrayShape::new(vec![(dim_size.into(), chunk_size.into()), (2, 1), (1, 1)])
                .unwrap();
        let dimension_names = Some(vec!["t".into()]);
        let temp_path: Path = "/temperature".try_into().unwrap();
        let split_config = ManifestSplittingConfig::with_size(split_size);

        let backend: Arc<dyn Storage + Send + Sync> = new_in_memory_storage().await?;
        let logging = Arc::new(LoggingStorage::new(Arc::clone(&backend)));
        let storage: Arc<dyn Storage + Send + Sync> = logging.clone();
        let repository = create_repo_with_split_manifest_config(
            &temp_path,
            &shape,
            &dimension_names,
            &split_config,
            Some(Arc::clone(&storage)),
        )
        .await?;

        let mut total_manifests = 0;
        assert_manifest_count(&backend, total_manifests).await;

        logging.clear();
        let ops = logging.fetch_operations();
        assert!(ops.is_empty());
        let mut session = repository.writable_session("main").await?;

        // only add refs that will be packed in the first split.
        for i in 0..2 {
            session
                .set_chunk_ref(
                    temp_path.clone(),
                    ChunkIndices(vec![i, 0, 0]),
                    Some(ChunkPayload::Inline(format!("{i}").into())),
                )
                .await?
        }
        session.commit("first split", None).await?;
        total_manifests += 1;
        assert_manifest_count(&storage, total_manifests).await;

        // now only last split
        let last_chunk = dim_size - 1;
        let mut session = repository.writable_session("main").await?;
        session
            .set_chunk_ref(
                temp_path.clone(),
                ChunkIndices(vec![last_chunk, 0, 0]),
                Some(ChunkPayload::Inline(format!("{last_chunk}").into())),
            )
            .await?;
        session.commit("last split", None).await?;
        total_manifests += 1;
        assert_manifest_count(&storage, total_manifests).await;

        // check that reads are optimized; we should only fetch the last split for this query
        let logging2 = Arc::new(LoggingStorage::new(Arc::clone(&backend)));
        let storage2: Arc<dyn Storage + Send + Sync> = logging2.clone();
        let config = RepositoryConfig {
            manifest: Some(ManifestConfig::empty()),
            ..RepositoryConfig::default()
        };
        let read_repo = Repository::open(Some(config), storage2, HashMap::new()).await?;
        let session = read_repo
            .readonly_session(&VersionInfo::BranchTipRef("main".to_string()))
            .await?;
        get_chunk(
            session
                .get_chunk_reader(
                    &temp_path,
                    &ChunkIndices(vec![last_chunk, 0, 0]),
                    &ByteRange::ALL,
                )
                .await
                .unwrap(),
        )
        .await
        .unwrap()
        .unwrap();
        let ops = logging2.fetch_operations();
        assert_eq!(
            ops.iter().filter(|(op, _)| op == "fetch_manifest_splitting").count(),
            1
        );

        // fetching a chunk that wasn't written shouldn't fetch any more manifests
        logging2.clear();
        get_chunk(
            session
                .get_chunk_reader(
                    &temp_path,
                    &ChunkIndices(vec![split_size + 1, 0, 0]),
                    &ByteRange::ALL,
                )
                .await
                .unwrap(),
        )
        .await
        .unwrap();
        let ops = logging2.fetch_operations();
        assert_eq!(
            ops.iter().filter(|(op, _)| op == "fetch_manifest_splitting").count(),
            0
        );

        // write one ref per split
        let mut session = repository.writable_session("main").await?;
        for i in (0..dim_size).step_by(split_size as usize) {
            total_manifests += 1;
            session
                .set_chunk_ref(
                    temp_path.clone(),
                    ChunkIndices(vec![i, 0, 0]),
                    Some(ChunkPayload::Inline(format!("{i}").into())),
                )
                .await?
        }
        session.commit("wrote all splits", None).await?;
        assert_manifest_count(&storage, total_manifests).await;

        let mut session = repository.writable_session("main").await?;
        for i in 0..dim_size {
            session
                .set_chunk_ref(
                    temp_path.clone(),
                    ChunkIndices(vec![i, 0, 0]),
                    Some(ChunkPayload::Inline(format!("{i}").into())),
                )
                .await?
        }
        // We are counting total manifests in the `assert_manifest_count` helper function
        // So we keep a running count of the total and update that at each step.
        total_manifests += dim_size.div_ceil(split_size) as usize;
        session.commit("full overwrite", None).await?;
        assert_manifest_count(&storage, total_manifests).await;

        // test reads
        for i in 0..dim_size {
            let val = get_chunk(
                session
                    .get_chunk_reader(
                        &temp_path,
                        &ChunkIndices(vec![i, 0, 0]),
                        &ByteRange::ALL,
                    )
                    .await
                    .unwrap(),
            )
            .await
            .unwrap()
            .unwrap();
            assert_eq!(val, Bytes::copy_from_slice(format!("{i}").as_bytes()));
        }

        // delete all chunks
        let mut session = repository.writable_session("main").await?;
        for i in 0..dim_size {
            session
                .set_chunk_ref(temp_path.clone(), ChunkIndices(vec![i, 0, 0]), None)
                .await?;
        }
        total_manifests += 0;
        session.commit("clear existing array", None).await?;
        assert_manifest_count(&storage, total_manifests).await;

        // add a new array
        let def = Bytes::from_static(br#"{"this":"array"}"#);
        let array_path: Path = "/array2".to_string().try_into().unwrap();
        let mut session = repository.writable_session("main").await?;
        session
            .add_array(
                array_path.clone(),
                shape.clone(),
                dimension_names.clone(),
                def.clone(),
            )
            .await?;
        // set a chunk
        session
            .set_chunk_ref(
                array_path.clone(),
                ChunkIndices(vec![1, 0, 0]),
                Some(ChunkPayload::Inline(format!("{0}", 10).into())),
            )
            .await?;
        // delete that chunk, so the chunks iterator is empty
        // regression test for bug found by hypothesis
        session
            .set_chunk_ref(array_path.clone(), ChunkIndices(vec![1, 0, 0]), None)
            .await?;
        total_manifests += 0;
        session.commit("clear new array", None).await?;
        assert_manifest_count(&storage, total_manifests).await;

        Ok(())
    }

    #[tokio_test]
    async fn test_manifest_splitting_complex_config() -> Result<(), Box<dyn Error>> {
        let shape = ArrayShape::new(vec![(25, 1), (10, 1), (3, 1), (4, 1)]).unwrap();
        let dimension_names = Some(vec!["t".into(), "z".into(), "y".into(), "x".into()]);
        let temp_path: Path = "/temperature".try_into().unwrap();

        let split_sizes = vec![
            (
                ManifestSplitCondition::PathMatches { regex: r".*".to_string() },
                vec![ManifestSplitDim {
                    condition: ManifestSplitDimCondition::DimensionName("t".to_string()),
                    num_chunks: 12,
                }],
            ),
            (
                ManifestSplitCondition::PathMatches { regex: r".*".to_string() },
                vec![ManifestSplitDim {
                    condition: ManifestSplitDimCondition::Axis(2),
                    num_chunks: 2,
                }],
            ),
            (
                ManifestSplitCondition::PathMatches { regex: r".*".to_string() },
                vec![ManifestSplitDim {
                    condition: ManifestSplitDimCondition::Any,
                    num_chunks: 9,
                }],
            ),
        ];
        let split_config = ManifestSplittingConfig { split_sizes: Some(split_sizes) };

        let expected = ManifestSplits::from_edges(vec![
            vec![0, 12, 24, 25],
            vec![0, 9, 10],
            vec![0, 2, 3],
            vec![0, 4],
        ]);

        let actual = split_config.get_split_sizes(&temp_path, &shape, &dimension_names);
        assert_eq!(actual, expected);

        let split_sizes = vec![(
            ManifestSplitCondition::PathMatches { regex: r".*".to_string() },
            vec![
                ManifestSplitDim {
                    condition: ManifestSplitDimCondition::DimensionName("t".to_string()),
                    num_chunks: 12,
                },
                ManifestSplitDim {
                    condition: ManifestSplitDimCondition::Axis(2),
                    num_chunks: 2,
                },
                ManifestSplitDim {
                    condition: ManifestSplitDimCondition::Any,
                    num_chunks: 9,
                },
            ],
        )];
        let split_config = ManifestSplittingConfig { split_sizes: Some(split_sizes) };
        let actual = split_config.get_split_sizes(&temp_path, &shape, &dimension_names);
        assert_eq!(actual, expected);

        Ok(())
    }

    #[tokio_test]
    async fn test_manifest_splitting_complex_writes() -> Result<(), Box<dyn Error>> {
        let t_split_size = 12u32;
        let other_split_size = 9u32;
        let y_split_size = 2u32;

        let shape = ArrayShape::new(vec![(25, 1), (10, 1), (3, 1), (4, 1)]).unwrap();
        let dimension_names = Some(vec!["t".into(), "z".into(), "y".into(), "x".into()]);
        let temp_path: Path = "/temperature".try_into().unwrap();

        let split_sizes = vec![
            (
                ManifestSplitCondition::AnyArray,
                vec![ManifestSplitDim {
                    condition: ManifestSplitDimCondition::DimensionName("t".to_string()),
                    num_chunks: t_split_size,
                }],
            ),
            (
                ManifestSplitCondition::PathMatches { regex: r".*".to_string() },
                vec![ManifestSplitDim {
                    condition: ManifestSplitDimCondition::Axis(2),
                    num_chunks: y_split_size,
                }],
            ),
            (
                ManifestSplitCondition::NameMatches { regex: r".*".to_string() },
                vec![ManifestSplitDim {
                    condition: ManifestSplitDimCondition::Any,
                    num_chunks: other_split_size,
                }],
            ),
        ];

        let expected_split_sizes = [t_split_size, 9, y_split_size, 9];

        let split_config = ManifestSplittingConfig { split_sizes: Some(split_sizes) };
        let backend: Arc<dyn Storage + Send + Sync> = new_in_memory_storage().await?;
        let logging = Arc::new(LoggingStorage::new(Arc::clone(&backend)));
        let logging_c: Arc<dyn Storage + Send + Sync> = logging.clone();
        let repository = create_repo_with_split_manifest_config(
            &temp_path,
            &shape,
            &dimension_names,
            &split_config,
            Some(logging_c),
        )
        .await?;
        let repo_clone = repository.reopen(None, None)?;

        let mut total_manifests = 0;
        assert_manifest_count(&backend, total_manifests).await;

        logging.clear();
        let ops = logging.fetch_operations();
        assert!(ops.is_empty());

        let array_shape =
            shape.iter().map(|x| x.array_length() as u32).collect::<Vec<_>>();

        let verify_data = async |ax, session: &Session| {
            for i in 0..shape.get(ax).unwrap().array_length() {
                let mut index = vec![0u32, 0, 0, 0];
                index[ax] = i as u32;
                let ic = index.clone();
                let val = get_chunk(
                    session
                        .get_chunk_reader(
                            &temp_path,
                            &ChunkIndices(index),
                            &ByteRange::ALL,
                        )
                        .await
                        .unwrap(),
                )
                .await
                .unwrap()
                .unwrap_or_else(|| panic!("getting chunk ref failed for {:?}", &ic));
                let expected_value =
                    ravel_multi_index(ic.as_slice(), array_shape.as_slice());
                let expected =
                    Bytes::copy_from_slice(format!("{expected_value}").as_bytes());
                assert_eq!(
                    val, expected,
                    "For chunk {ic:?}, received {val:?}, expected {expected:?}"
                );
            }
        };
        let verify_all_data = async |repo: &Repository| {
            let session = repo
                .readonly_session(&VersionInfo::BranchTipRef("main".to_string()))
                .await
                .unwrap();
            for ax in 0..shape.len() {
                verify_data(ax, &session).await;
            }
        };

        //=========================================================
        // This loop iterates over axis and rewrites the boundary chunks.
        // Each loop iteration must rewrite chunk_shape/split_size manifests
        for ax in 0..shape.len() {
            let mut session = repository.writable_session("main").await?;
            let axis_size = shape.get(ax).unwrap().array_length();
            for i in 0..axis_size {
                let mut index = vec![0u32, 0, 0, 0];
                index[ax] = i as u32;
                let value = ravel_multi_index(index.as_slice(), array_shape.as_slice());
                session
                    .set_chunk_ref(
                        temp_path.clone(),
                        ChunkIndices(index),
                        Some(ChunkPayload::Inline(format!("{value}").into())),
                    )
                    .await?
            }

            total_manifests +=
                (axis_size as u32).div_ceil(expected_split_sizes[ax]) as usize;
            session.commit(format!("finished axis {ax}").as_ref(), None).await?;
            assert_manifest_count(&backend, total_manifests).await;

            verify_data(ax, &session).await;
        }
        verify_all_data(&repository).await;

        //=========================================================
        // Now change splitting config
        let split_sizes = vec![(
            ManifestSplitCondition::AnyArray,
            vec![ManifestSplitDim {
                condition: ManifestSplitDimCondition::DimensionName("t".to_string()),
                num_chunks: t_split_size,
            }],
        )];

        let repository =
            reopen_repo_with_new_splitting_config(&repository, Some(split_sizes));
        verify_all_data(&repository).await;
        let mut session = repository.writable_session("main").await?;
        let index = vec![13, 0, 0, 0];
        let value = ravel_multi_index(index.as_slice(), array_shape.as_slice());
        session
            .set_chunk_ref(
                temp_path.clone(),
                ChunkIndices(index),
                Some(ChunkPayload::Inline(format!("{value}").into())),
            )
            .await?;
        // Important: we only create one new manifest in this case for
        // the first split in the `t`-axis. Since the other splits
        // are not modified we preserve all the old manifests
        total_manifests += 1;
        session.commit("finished time again".to_string().as_ref(), None).await?;
        assert_manifest_count(&backend, total_manifests).await;
        verify_all_data(&repository).await;

        // now modify all splits to trigger a full rewrite
        let mut session = repository.writable_session("main").await?;
        for idx in [0, 12, 24] {
            let index = vec![idx, 0, 0, 0];
            let value = ravel_multi_index(index.as_slice(), array_shape.as_slice());
            session
                .set_chunk_ref(
                    temp_path.clone(),
                    ChunkIndices(index),
                    Some(ChunkPayload::Inline(format!("{value}").into())),
                )
                .await?;
        }
        total_manifests +=
            (shape.get(0).unwrap().array_length() as u32).div_ceil(t_split_size) as usize;
        session.commit("finished time again".to_string().as_ref(), None).await?;
        assert_manifest_count(&backend, total_manifests).await;
        verify_all_data(&repository).await;

        //=========================================================
        // Now get back to original repository with original config
        // Modify one `t` split.
        let mut session = repo_clone.writable_session("main").await?;
        session
            .set_chunk_ref(
                temp_path.clone(),
                ChunkIndices(vec![0, 0, 0, 0]),
                Some(ChunkPayload::Inline(format!("{0}", 0).into())),
            )
            .await?;
        // Important: now we rewrite one split per dimension
        total_manifests += 3;
        session.commit("finished time again".to_string().as_ref(), None).await?;
        assert_manifest_count(&backend, total_manifests).await;
        verify_all_data(&repo_clone).await;
        verify_all_data(&repository).await;

        let mut session = repo_clone.writable_session("main").await?;
        for idx in [0, 12, 24] {
            let index = vec![idx, 0, 0, 0];
            let value = ravel_multi_index(index.as_slice(), array_shape.as_slice());
            session
                .set_chunk_ref(
                    temp_path.clone(),
                    ChunkIndices(index),
                    Some(ChunkPayload::Inline(format!("{value}").into())),
                )
                .await?;
        }
        total_manifests +=
            (shape.get(0).unwrap().array_length() as u32).div_ceil(t_split_size) as usize;
        session.commit("finished time again".to_string().as_ref(), None).await?;
        assert_manifest_count(&backend, total_manifests).await;
        verify_all_data(&repo_clone).await;

        // do that again, but with different values and test those specifically
        let mut session = repo_clone.writable_session("main").await?;
        for idx in [0, 12, 24] {
            let index = vec![idx, 0, 0, 0];
            session
                .set_chunk_ref(
                    temp_path.clone(),
                    ChunkIndices(index),
                    Some(ChunkPayload::Inline(format!("{0}", idx + 2).into())),
                )
                .await?;
        }
        total_manifests +=
            (shape.get(0).unwrap().array_length() as u32).div_ceil(t_split_size) as usize;
        session.commit("finished time again".to_string().as_ref(), None).await?;
        assert_manifest_count(&backend, total_manifests).await;
        for idx in [0, 12, 24] {
            let actual = get_chunk(
                session
                    .get_chunk_reader(
                        &temp_path,
                        &ChunkIndices(vec![idx, 0, 0, 0]),
                        &ByteRange::ALL,
                    )
                    .await
                    .unwrap(),
            )
            .await
            .unwrap()
            .unwrap();
            let expected = Bytes::copy_from_slice(format!("{0}", idx + 2).as_bytes());
            assert_eq!(actual, expected);
        }
        Ok(())
    }

    #[tokio_test]
    async fn test_manifest_splits_merge_sessions() -> Result<(), Box<dyn Error>> {
        let shape = ArrayShape::new(vec![(25, 1), (10, 1), (3, 1), (4, 1)]).unwrap();
        let dimension_names = Some(vec!["t".into(), "z".into(), "y".into(), "x".into()]);
        let temp_path: Path = "/temperature".try_into().unwrap();

        let orig_split_sizes = vec![(
            ManifestSplitCondition::AnyArray,
            vec![ManifestSplitDim {
                condition: ManifestSplitDimCondition::DimensionName("t".to_string()),
                num_chunks: 12u32,
            }],
        )];
        let split_config =
            ManifestSplittingConfig { split_sizes: Some(orig_split_sizes.clone()) };
        let backend: Arc<dyn Storage + Send + Sync> = new_in_memory_storage().await?;
        let repository = create_repo_with_split_manifest_config(
            &temp_path,
            &shape,
            &dimension_names,
            &split_config,
            Some(backend),
        )
        .await?;

        let indices =
            [vec![0, 0, 1, 0], vec![0, 0, 0, 0], vec![0, 2, 0, 0], vec![0, 2, 0, 1]];

        let mut session1 = repository.writable_session("main").await?;
        let node_id = session1.get_node(&temp_path).await?.id;
        session1
            .set_chunk_ref(
                temp_path.clone(),
                ChunkIndices(indices[0].clone()),
                Some(ChunkPayload::Inline(format!("{0}", 0).into())),
            )
            .await?;
        session1
            .set_chunk_ref(
                temp_path.clone(),
                ChunkIndices(indices[1].clone()),
                Some(ChunkPayload::Inline(format!("{0}", 1).into())),
            )
            .await?;

        for incompatible_size in [1, 11u32, 24u32, u32::MAX] {
            let incompatible_split_sizes = vec![(
                ManifestSplitCondition::AnyArray,
                vec![ManifestSplitDim {
                    condition: ManifestSplitDimCondition::DimensionName("t".to_string()),
                    num_chunks: incompatible_size,
                }],
            )];
            let other_repo = reopen_repo_with_new_splitting_config(
                &repository,
                Some(incompatible_split_sizes),
            );

            assert_ne!(other_repo.config(), repository.config());

            let mut session2 = other_repo.writable_session("main").await?;
            session2
                .set_chunk_ref(
                    temp_path.clone(),
                    ChunkIndices(indices[2].clone()),
                    Some(ChunkPayload::Inline(format!("{0}", 2).into())),
                )
                .await?;
            session2
                .set_chunk_ref(
                    temp_path.clone(),
                    ChunkIndices(indices[3].clone()),
                    Some(ChunkPayload::Inline(format!("{0}", 3).into())),
                )
                .await?;

            assert!(session1.merge(session2).await.is_err());
        }

        // now with the same split sizes
        let other_repo =
            reopen_repo_with_new_splitting_config(&repository, Some(orig_split_sizes));
        let mut session2 = other_repo.writable_session("main").await?;
        session2
            .set_chunk_ref(
                temp_path.clone(),
                ChunkIndices(indices[2].clone()),
                Some(ChunkPayload::Inline(format!("{0}", 2).into())),
            )
            .await?;
        session2
            .set_chunk_ref(
                temp_path.clone(),
                ChunkIndices(indices[3].clone()),
                Some(ChunkPayload::Inline(format!("{0}", 3).into())),
            )
            .await?;

        // Session.splits should be _complete_ so it should be identical for the same node
        // on any two sessions with compatible splits
        let splits = session1.lookup_splits(&node_id).unwrap().clone();
        assert_eq!(session1.lookup_splits(&node_id), session2.lookup_splits(&node_id));
        session1.merge(session2).await?;
        assert_eq!(session1.lookup_splits(&node_id), Some(&splits));
        for (val, idx) in enumerate(indices.iter()) {
            let actual = get_chunk(
                session1
                    .get_chunk_reader(
                        &temp_path,
                        &ChunkIndices(idx.clone()),
                        &ByteRange::ALL,
                    )
                    .await
                    .unwrap(),
            )
            .await
            .unwrap()
            .unwrap_or_else(|| panic!("getting chunk ref failed for {:?}", &idx));
            let expected = Bytes::copy_from_slice(format!("{val}").as_bytes());
            assert_eq!(actual, expected);
        }

        // now merge two sessions: one with only writes, one with only deletes
        let mut session1 = repository.writable_session("main").await?;
        session1
            .set_chunk_ref(
                temp_path.clone(),
                ChunkIndices(indices[0].clone()),
                Some(ChunkPayload::Inline(format!("{0}", 3).into())),
            )
            .await?;
        session1
            .set_chunk_ref(
                temp_path.clone(),
                ChunkIndices(indices[1].clone()),
                Some(ChunkPayload::Inline(format!("{0}", 4).into())),
            )
            .await?;
        let mut session2 = repository.writable_session("main").await?;
        session2
            .set_chunk_ref(temp_path.clone(), ChunkIndices(indices[2].clone()), None)
            .await?;
        session2
            .set_chunk_ref(temp_path.clone(), ChunkIndices(indices[3].clone()), None)
            .await?;

        session1.merge(session2).await?;
        let expected = [Some(3), Some(4), None, None];
        for (expect, idx) in zip(expected.iter(), indices.iter()) {
            let actual = get_chunk(
                session1
                    .get_chunk_reader(
                        &temp_path,
                        &ChunkIndices(idx.clone()),
                        &ByteRange::ALL,
                    )
                    .await
                    .unwrap(),
            )
            .await
            .unwrap();
            let expected_value =
                expect.map(|val| Bytes::copy_from_slice(format!("{val}").as_bytes()));
            assert_eq!(actual, expected_value);
        }

        Ok(())
    }

    #[tokio_test]
    async fn test_commits_with_conflicting_manifest_splits() -> Result<(), Box<dyn Error>>
    {
        let shape = ArrayShape::new(vec![(25, 1), (10, 1), (3, 1), (4, 1)]).unwrap();
        let dimension_names = Some(vec!["t".into(), "z".into(), "y".into(), "x".into()]);
        let temp_path: Path = "/temperature".try_into().unwrap();

        let orig_split_sizes = vec![(
            ManifestSplitCondition::AnyArray,
            vec![ManifestSplitDim {
                condition: ManifestSplitDimCondition::DimensionName("t".to_string()),
                num_chunks: 12u32,
            }],
        )];
        let split_config =
            ManifestSplittingConfig { split_sizes: Some(orig_split_sizes.clone()) };
        let backend: Arc<dyn Storage + Send + Sync> = new_in_memory_storage().await?;
        let repository = create_repo_with_split_manifest_config(
            &temp_path,
            &shape,
            &dimension_names,
            &split_config,
            Some(backend),
        )
        .await?;

        let indices =
            [vec![0, 0, 1, 0], vec![0, 0, 0, 0], vec![0, 2, 0, 0], vec![0, 2, 0, 1]];

        let mut session1 = repository.writable_session("main").await?;
        session1
            .set_chunk_ref(
                temp_path.clone(),
                ChunkIndices(indices[0].clone()),
                Some(ChunkPayload::Inline(format!("{0}", 0).into())),
            )
            .await?;
        session1
            .set_chunk_ref(
                temp_path.clone(),
                ChunkIndices(indices[1].clone()),
                Some(ChunkPayload::Inline(format!("{0}", 1).into())),
            )
            .await?;

        let incompatible_size = 11u32;
        let incompatible_split_sizes = vec![(
            ManifestSplitCondition::AnyArray,
            vec![ManifestSplitDim {
                condition: ManifestSplitDimCondition::DimensionName("t".to_string()),
                num_chunks: incompatible_size,
            }],
        )];
        let other_repo = reopen_repo_with_new_splitting_config(
            &repository,
            Some(incompatible_split_sizes),
        );

        assert_ne!(other_repo.config(), repository.config());

        let mut session2 = other_repo.writable_session("main").await?;
        session2
            .set_chunk_ref(
                temp_path.clone(),
                ChunkIndices(indices[2].clone()),
                Some(ChunkPayload::Inline(format!("{0}", 2).into())),
            )
            .await?;
        session2
            .set_chunk_ref(
                temp_path.clone(),
                ChunkIndices(indices[3].clone()),
                Some(ChunkPayload::Inline(format!("{0}", 3).into())),
            )
            .await?;

        session1.commit("first commit", None).await?;
        if let Err(SessionError { kind: SessionErrorKind::Conflict { .. }, .. }) =
            session2.commit("second commit", None).await
        {
            let solver = BasicConflictSolver::default();
            // different chunks were written so this should fast forward
            assert!(session2.rebase(&solver).await.is_ok());
            session2.commit("second commit after rebase", None).await?;
        } else {
            panic!("this should have conflicted!");
        }

        let new_session = repository
            .readonly_session(&VersionInfo::BranchTipRef("main".into()))
            .await?;
        for (val, idx) in enumerate(indices.iter()) {
            let actual = get_chunk(
                new_session
                    .get_chunk_reader(
                        &temp_path,
                        &ChunkIndices(idx.clone()),
                        &ByteRange::ALL,
                    )
                    .await
                    .unwrap(),
            )
            .await
            .unwrap()
            .unwrap_or_else(|| panic!("getting chunk ref failed for {:?}", &idx));
            let expected = Bytes::copy_from_slice(format!("{val}").as_bytes());
            assert_eq!(actual, expected);
        }

        Ok(())
    }

    #[tokio::test]
    /// Writes four arrays to a repo arrays, checks preloading of the manifests
    ///
    /// Three of the arrays have a preload name. But on of them (time) is larger
    /// than what we allow for preload (via config).
    ///
    /// We verify only the correct two arrays are preloaded
    async fn test_manifest_preload_known_manifests() -> Result<(), Box<dyn Error>> {
        //let backend: Arc<dyn Storage + Send + Sync> =
        //    new_local_filesystem_storage(&(std::path::Path::new("/tmp/testrepo2")))
        //        .await?;
        let backend: Arc<dyn Storage + Send + Sync> = new_in_memory_storage().await?;
        let storage = Arc::clone(&backend);

        let repository = Repository::create(None, storage, HashMap::new()).await?;

        let mut session = repository.writable_session("main").await?;

        let def = Bytes::from_static(br#"{"this":"array"}"#);
        session.add_group(Path::root(), def.clone()).await?;

        let shape = ArrayShape::new(vec![(1_000, 1), (1, 1), (1, 1)]).unwrap();
        let dimension_names = Some(vec!["t".into()]);

        let time_path: Path = "/time".try_into().unwrap();
        let temp_path: Path = "/temperature".try_into().unwrap();
        let lat_path: Path = "/latitude".try_into().unwrap();
        let lon_path: Path = "/longitude".try_into().unwrap();
        session
            .add_array(
                time_path.clone(),
                shape.clone(),
                dimension_names.clone(),
                def.clone(),
            )
            .await?;
        session
            .add_array(
                temp_path.clone(),
                shape.clone(),
                dimension_names.clone(),
                def.clone(),
            )
            .await?;
        session
            .add_array(
                lat_path.clone(),
                shape.clone(),
                dimension_names.clone(),
                def.clone(),
            )
            .await?;
        session
            .add_array(
                lon_path.clone(),
                shape.clone(),
                dimension_names.clone(),
                def.clone(),
            )
            .await?;

        session
            .set_chunk_ref(
                time_path.clone(),
                ChunkIndices(vec![0, 0, 0]),
                Some(ChunkPayload::Inline("hello".into())),
            )
            .await?;

        session
            .set_chunk_ref(
                time_path.clone(),
                ChunkIndices(vec![1, 0, 0]),
                Some(ChunkPayload::Inline("hello".into())),
            )
            .await?;

        session
            .set_chunk_ref(
                time_path.clone(),
                ChunkIndices(vec![2, 0, 0]),
                Some(ChunkPayload::Inline("hello".into())),
            )
            .await?;

        session
            .set_chunk_ref(
                lat_path.clone(),
                ChunkIndices(vec![0, 0, 0]),
                Some(ChunkPayload::Inline("hello".into())),
            )
            .await?;
        session
            .set_chunk_ref(
                lon_path.clone(),
                ChunkIndices(vec![0, 0, 0]),
                Some(ChunkPayload::Inline("hello".into())),
            )
            .await?;
        session
            .set_chunk_ref(
                temp_path.clone(),
                ChunkIndices(vec![0, 0, 0]),
                Some(ChunkPayload::Inline("hello".into())),
            )
            .await?;

        session.commit("create arrays", None).await?;

        let logging = Arc::new(LoggingStorage::new(Arc::clone(&backend)));
        let logging_c: Arc<dyn Storage + Send + Sync> = logging.clone();
        let storage = Arc::clone(&logging_c);

        let man_config = ManifestConfig {
            preload: Some(ManifestPreloadConfig {
                max_total_refs: Some(2),
                preload_if: None,
            }),
            ..ManifestConfig::default()
        };
        let config = RepositoryConfig {
            manifest: Some(man_config),
            ..RepositoryConfig::default()
        };
        let repository = Repository::open(Some(config), storage, HashMap::new()).await?;

        let ops = logging.fetch_operations();
        assert!(ops.is_empty());

        let session = repository
            .readonly_session(&VersionInfo::BranchTipRef("main".to_string()))
            .await?;

        // give some time for manifests to load
        let mut retries = 0;
        while retries < 50 && logging.fetch_operations().is_empty() {
            tokio::time::sleep(std::time::Duration::from_secs_f32(0.1)).await;
            retries += 1
        }
        let ops = logging.fetch_operations();

        let lat_manifest_id = match session.get_node(&lat_path).await?.node_data {
            NodeData::Array { manifests, .. } => manifests[0].object_id.to_string(),
            NodeData::Group => panic!(),
        };
        let lon_manifest_id = match session.get_node(&lon_path).await?.node_data {
            NodeData::Array { manifests, .. } => manifests[0].object_id.to_string(),
            NodeData::Group => panic!(),
        };
        assert_eq!(ops[0].0, "fetch_snapshot");
        // nodes sorted lexicographically
        assert_eq!(ops[1], ("fetch_manifest_splitting".to_string(), lat_manifest_id));
        assert_eq!(ops[2], ("fetch_manifest_splitting".to_string(), lon_manifest_id));

        Ok(())
    }

    #[tokio::test]
    async fn creation_in_non_empty_directory_fails() -> Result<(), Box<dyn Error>> {
        let repo_dir = TempDir::new()?;

        let storage: Arc<dyn Storage + Send + Sync> =
            new_local_filesystem_storage(repo_dir.path())
                .await
                .expect("Creating local storage failed");

        Repository::create(None, Arc::clone(&storage), HashMap::new()).await?;
        assert!(
            Repository::create(None, Arc::clone(&storage), HashMap::new()).await.is_err()
        );

        let inner_path: PathBuf =
            [repo_dir.path().to_string_lossy().into_owned().as_str(), "snapshots"]
                .iter()
                .collect();
        let storage: Arc<dyn Storage + Send + Sync> =
            new_local_filesystem_storage(&inner_path)
                .await
                .expect("Creating local storage failed");

        assert!(
            Repository::create(None, Arc::clone(&storage), HashMap::new()).await.is_err()
        );

        Ok(())
    }
}
