use std::{
    collections::{BTreeSet, HashMap},
    sync::Arc,
};

use bytes::Bytes;
use futures::Stream;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::task::JoinError;

use crate::{
    asset_manager::AssetManager,
    config::{Credentials, RepositoryConfig},
    format::{
        snapshot::{Snapshot, SnapshotInfo},
        IcechunkFormatError, NodeId, SnapshotId,
    },
    refs::{
        create_tag, delete_branch, delete_tag, fetch_branch_tip, fetch_tag,
        list_branches, list_tags, update_branch, BranchVersion, Ref, RefError,
    },
    session::Session,
    storage::{self, ETag},
    virtual_chunks::{ContainerName, VirtualChunkResolver},
    Storage, StorageError,
};

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

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum RepositoryError {
    #[error("error contacting storage {0}")]
    StorageError(#[from] StorageError),
    #[error("snapshot not found: `{id}`")]
    SnapshotNotFound { id: SnapshotId },
    #[error("invalid snapshot id: `{0}`")]
    InvalidSnapshotId(String),
    #[error("error in icechunk file")]
    FormatError(#[from] IcechunkFormatError),
    #[error("ref error: `{0}`")]
    Ref(#[from] RefError),
    #[error("tag error: `{0}`")]
    Tag(String),
    #[error("the repository has been initialized already (default branch exists)")]
    AlreadyInitialized,
    #[error("the repository doesn't exist")]
    RepositoryDoesntExist,
    #[error("error in repository serialization `{0}`")]
    SerializationError(#[from] rmp_serde::encode::Error),
    #[error("error in repository deserialization `{0}`")]
    DeserializationError(#[from] rmp_serde::decode::Error),
    #[error("error finding conflicting path for node `{0}`, this probably indicades a bug in `rebase`")]
    ConflictingPathNotFound(NodeId),
    #[error("error in config deserialization `{0}`")]
    ConfigDeserializationError(#[from] serde_yml::Error),
    #[error("branch update conflict: `({expected_parent:?}) != ({actual_parent:?})`")]
    Conflict { expected_parent: Option<SnapshotId>, actual_parent: Option<SnapshotId> },
    #[error("I/O error `{0}`")]
    IOError(#[from] std::io::Error),
    #[error("a concurrent task failed {0}")]
    ConcurrencyError(#[from] JoinError),
    #[error("main branch cannot be deleted")]
    CannotDeleteMain,
}

pub type RepositoryResult<T> = Result<T, RepositoryError>;

#[derive(Debug, Serialize, Deserialize)]
pub struct Repository {
    config: RepositoryConfig,
    storage_settings: storage::Settings,
    config_etag: Option<ETag>,
    storage: Arc<dyn Storage + Send + Sync>,
    asset_manager: Arc<AssetManager>,
    virtual_resolver: Arc<VirtualChunkResolver>,
    virtual_chunk_credentials: HashMap<ContainerName, Credentials>,
}

impl Repository {
    pub async fn create(
        config: Option<RepositoryConfig>,
        storage: Arc<dyn Storage + Send + Sync>,
        virtual_chunk_credentials: HashMap<ContainerName, Credentials>,
    ) -> RepositoryResult<Self> {
        if Self::exists(storage.as_ref()).await? {
            return Err(RepositoryError::AlreadyInitialized);
        }
        let has_overriden_config = match config {
            Some(ref config) => config != &RepositoryConfig::default(),
            None => false,
        };
        // Merge the given config with the defaults
        let config =
            config.map(|c| RepositoryConfig::default().merge(c)).unwrap_or_default();
        let compression = config.compression().level();
        let overwrite_refs = config.unsafe_overwrite_refs();
        let storage_c = Arc::clone(&storage);
        let storage_settings =
            config.storage().cloned().unwrap_or_else(|| storage.default_settings());

        let handle1 = tokio::spawn(async move {
            // TODO: we could cache this first snapshot
            let asset_manager = AssetManager::new_no_cache(
                Arc::clone(&storage_c),
                storage_settings.clone(),
                compression,
            );
            // On create we need to create the default branch
            let new_snapshot = Arc::new(Snapshot::initial());
            asset_manager.write_snapshot(Arc::clone(&new_snapshot)).await?;

            update_branch(
                storage_c.as_ref(),
                &storage_settings,
                Ref::DEFAULT_BRANCH,
                new_snapshot.id().clone(),
                None,
                overwrite_refs,
            )
            .await?;
            Ok::<(), RepositoryError>(())
        });

        let storage_c = Arc::clone(&storage);
        let config_c = config.clone();
        let handle2 = tokio::spawn(async move {
            if has_overriden_config {
                let etag =
                    Repository::store_config(storage_c.as_ref(), &config_c, None).await?;
                Ok::<_, RepositoryError>(Some(etag))
            } else {
                Ok(None)
            }
        });

        handle1.await??;
        let config_etag = handle2.await??;

        debug_assert!(Self::exists(storage.as_ref()).await.unwrap_or(false));

        Self::new(config, config_etag, storage, virtual_chunk_credentials)
    }

    pub async fn open(
        config: Option<RepositoryConfig>,
        storage: Arc<dyn Storage + Send + Sync>,
        virtual_chunk_credentials: HashMap<ContainerName, Credentials>,
    ) -> RepositoryResult<Self> {
        let storage_c = Arc::clone(&storage);
        let handle1 =
            tokio::spawn(async move { Self::fetch_config(storage_c.as_ref()).await });

        let storage_c = Arc::clone(&storage);
        let handle2 = tokio::spawn(async move {
            if !Self::exists(storage_c.as_ref()).await? {
                return Err(RepositoryError::RepositoryDoesntExist);
            }
            Ok(())
        });

        #[allow(clippy::expect_used)]
        handle2.await.expect("Error checking if repo exists")?;
        #[allow(clippy::expect_used)]
        if let Some((default_config, config_etag)) =
            handle1.await.expect("Error fetching repo config")?
        {
            // Merge the given config with the defaults
            let config =
                config.map(|c| default_config.merge(c)).unwrap_or(default_config);

            Self::new(config, Some(config_etag), storage, virtual_chunk_credentials)
        } else {
            let config = config.unwrap_or_default();
            Self::new(config, None, storage, virtual_chunk_credentials)
        }
    }

    pub async fn open_or_create(
        config: Option<RepositoryConfig>,
        storage: Arc<dyn Storage + Send + Sync>,
        virtual_chunk_credentials: HashMap<ContainerName, Credentials>,
    ) -> RepositoryResult<Self> {
        if Self::exists(storage.as_ref()).await? {
            Self::open(config, storage, virtual_chunk_credentials).await
        } else {
            Self::create(config, storage, virtual_chunk_credentials).await
        }
    }

    fn new(
        config: RepositoryConfig,
        config_etag: Option<ETag>,
        storage: Arc<dyn Storage + Send + Sync>,
        virtual_chunk_credentials: HashMap<ContainerName, Credentials>,
    ) -> RepositoryResult<Self> {
        let containers = config.virtual_chunk_containers().cloned();
        validate_credentials(&config, &virtual_chunk_credentials)?;
        let storage_settings =
            config.storage().cloned().unwrap_or_else(|| storage.default_settings());
        let virtual_resolver = Arc::new(VirtualChunkResolver::new(
            containers,
            virtual_chunk_credentials.clone(),
            storage_settings.clone(),
        ));
        let asset_manager = Arc::new(AssetManager::new_with_config(
            Arc::clone(&storage),
            storage_settings.clone(),
            config.caching(),
            config.compression().level(),
        ));
        Ok(Self {
            config,
            config_etag,
            storage,
            storage_settings,
            virtual_resolver,
            asset_manager,
            virtual_chunk_credentials,
        })
    }

    pub async fn exists(storage: &(dyn Storage + Send + Sync)) -> RepositoryResult<bool> {
        match fetch_branch_tip(storage, &storage.default_settings(), Ref::DEFAULT_BRANCH)
            .await
        {
            Ok(_) => Ok(true),
            Err(RefError::RefNotFound(_)) => Ok(false),
            Err(err) => Err(err.into()),
        }
    }

    /// Reopen the repository changing its config and or virtual chunk credentials
    pub fn reopen(
        &self,
        config: Option<RepositoryConfig>,
        virtual_chunk_credentials: Option<HashMap<ContainerName, Credentials>>,
    ) -> RepositoryResult<Self> {
        // Merge the given config with the current config
        let config = config
            .map(|c| self.config().merge(c))
            .unwrap_or_else(|| self.config().clone());

        Self::new(
            config,
            self.config_etag.clone(),
            Arc::clone(&self.storage),
            virtual_chunk_credentials
                .unwrap_or_else(|| self.virtual_chunk_credentials.clone()),
        )
    }

    pub async fn fetch_config(
        storage: &(dyn Storage + Send + Sync),
    ) -> RepositoryResult<Option<(RepositoryConfig, ETag)>> {
        match storage.fetch_config(&storage.default_settings()).await? {
            Some((bytes, etag)) => {
                let config = serde_yml::from_slice(&bytes)?;
                Ok(Some((config, etag)))
            }
            None => Ok(None),
        }
    }

    pub async fn save_config(&self) -> RepositoryResult<ETag> {
        Repository::store_config(
            self.storage().as_ref(),
            self.config(),
            self.config_etag.as_ref(),
        )
        .await
    }

    pub(crate) async fn store_config(
        storage: &(dyn Storage + Send + Sync),
        config: &RepositoryConfig,
        config_etag: Option<&ETag>,
    ) -> RepositoryResult<ETag> {
        let bytes = Bytes::from(serde_yml::to_string(config)?);
        let res = storage
            .update_config(
                &storage.default_settings(),
                bytes,
                config_etag.map(|e| e.as_str()),
            )
            .await?;
        Ok(res)
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
    pub async fn snapshot_ancestry(
        &self,
        snapshot_id: &SnapshotId,
    ) -> RepositoryResult<impl Stream<Item = RepositoryResult<SnapshotInfo>> + '_> {
        self.asset_manager.snapshot_ancestry(snapshot_id).await
    }

    /// Returns the sequence of parents of the snapshot pointed by the given version
    pub async fn ancestry(
        &self,
        version: &VersionInfo,
    ) -> RepositoryResult<impl Stream<Item = RepositoryResult<SnapshotInfo>> + '_> {
        let snapshot_id = self.resolve_version(version).await?;
        self.snapshot_ancestry(&snapshot_id).await
    }

    /// Create a new branch in the repository at the given snapshot id
    pub async fn create_branch(
        &self,
        branch_name: &str,
        snapshot_id: &SnapshotId,
    ) -> RepositoryResult<BranchVersion> {
        // TODO: The parent snapshot should exist?
        let version = match update_branch(
            self.storage.as_ref(),
            &self.storage_settings,
            branch_name,
            snapshot_id.clone(),
            None,
            self.config().unsafe_overwrite_refs(),
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

    /// List all branches in the repository.
    pub async fn list_branches(&self) -> RepositoryResult<BTreeSet<String>> {
        let branches =
            list_branches(self.storage.as_ref(), &self.storage_settings).await?;
        Ok(branches)
    }

    /// Get the snapshot id of the tip of a branch
    pub async fn lookup_branch(&self, branch: &str) -> RepositoryResult<SnapshotId> {
        let branch_version =
            fetch_branch_tip(self.storage.as_ref(), &self.storage_settings, branch)
                .await?;
        Ok(branch_version.snapshot)
    }

    /// Make a branch point to the specified snapshot.
    /// After execution, history of the branch will be altered, and the current
    /// store will point to a different base snapshot_id
    pub async fn reset_branch(
        &self,
        branch: &str,
        snapshot_id: &SnapshotId,
    ) -> RepositoryResult<BranchVersion> {
        raise_if_invalid_snapshot_id(
            self.storage.as_ref(),
            &self.storage_settings,
            snapshot_id,
        )
        .await?;
        let branch_tip = self.lookup_branch(branch).await?;
        let version = update_branch(
            self.storage.as_ref(),
            &self.storage_settings,
            branch,
            snapshot_id.clone(),
            Some(&branch_tip),
            self.config().unsafe_overwrite_refs(),
        )
        .await?;

        Ok(version)
    }

    /// Delete a branch from the repository.
    /// This will remove the branch reference and the branch history. It will not remove the
    /// chunks or snapshots associated with the branch.
    pub async fn delete_branch(&self, branch: &str) -> RepositoryResult<()> {
        if branch != Ref::DEFAULT_BRANCH {
            delete_branch(self.storage.as_ref(), &self.storage_settings, branch).await?;
            Ok(())
        } else {
            Err(RepositoryError::CannotDeleteMain)
        }
    }

    /// Delete a tag from the repository.
    /// This will remove the tag reference. It will not remove the
    /// chunks or snapshots associated with the tag.
    pub async fn delete_tag(&self, tag: &str) -> RepositoryResult<()> {
        Ok(delete_tag(
            self.storage.as_ref(),
            &self.storage_settings,
            tag,
            self.config().unsafe_overwrite_refs(),
        )
        .await?)
    }

    /// Create a new tag in the repository at the given snapshot id
    pub async fn create_tag(
        &self,
        tag_name: &str,
        snapshot_id: &SnapshotId,
    ) -> RepositoryResult<()> {
        create_tag(
            self.storage.as_ref(),
            &self.storage_settings,
            tag_name,
            snapshot_id.clone(),
            self.config().unsafe_overwrite_refs(),
        )
        .await?;
        Ok(())
    }

    /// List all tags in the repository.
    pub async fn list_tags(&self) -> RepositoryResult<BTreeSet<String>> {
        let tags = list_tags(self.storage.as_ref(), &self.storage_settings).await?;
        Ok(tags)
    }

    pub async fn lookup_tag(&self, tag: &str) -> RepositoryResult<SnapshotId> {
        let ref_data =
            fetch_tag(self.storage.as_ref(), &self.storage_settings, tag).await?;
        Ok(ref_data.snapshot)
    }

    async fn resolve_version(
        &self,
        version: &VersionInfo,
    ) -> RepositoryResult<SnapshotId> {
        match version {
            VersionInfo::SnapshotId(sid) => {
                raise_if_invalid_snapshot_id(
                    self.storage.as_ref(),
                    &self.storage_settings,
                    sid,
                )
                .await?;
                Ok(sid.clone())
            }
            VersionInfo::TagRef(tag) => {
                let ref_data =
                    fetch_tag(self.storage.as_ref(), &self.storage_settings, tag).await?;
                Ok(ref_data.snapshot)
            }
            VersionInfo::BranchTipRef(branch) => {
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
            snapshot_id,
        );

        Ok(session)
    }

    pub async fn writable_session(&self, branch: &str) -> RepositoryResult<Session> {
        let ref_data =
            fetch_branch_tip(self.storage.as_ref(), &self.storage_settings, branch)
                .await?;
        let session = Session::create_writable_session(
            self.config.clone(),
            self.storage_settings.clone(),
            self.storage.clone(),
            Arc::clone(&self.asset_manager),
            self.virtual_resolver.clone(),
            branch.to_string(),
            ref_data.snapshot,
        );

        Ok(session)
    }
}

fn validate_credentials(
    config: &RepositoryConfig,
    creds: &HashMap<String, Credentials>,
) -> RepositoryResult<()> {
    for (cont, cred) in creds {
        if let Some(cont) = config.get_virtual_chunk_container(cont) {
            if let Err(error) = cont.validate_credentials(cred) {
                return Err(RepositoryError::StorageError(StorageError::Other(error)));
            }
        }
    }
    Ok(())
}

pub async fn raise_if_invalid_snapshot_id(
    storage: &(dyn Storage + Send + Sync),
    storage_settings: &storage::Settings,
    snapshot_id: &SnapshotId,
) -> RepositoryResult<()> {
    storage
        .fetch_snapshot(storage_settings, snapshot_id)
        .await
        .map_err(|_| RepositoryError::SnapshotNotFound { id: snapshot_id.clone() })?;
    Ok(())
}

#[cfg(test)]
#[allow(clippy::panic, clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::{collections::HashMap, error::Error, sync::Arc};

    use crate::{
        config::{CachingConfig, RepositoryConfig},
        storage::new_in_memory_storage,
        Repository, Storage,
    };

    use super::*;

    // TODO: Add Tests
    #[tokio::test]
    async fn test_repository_persistent_config() -> Result<(), Box<dyn Error>> {
        let storage: Arc<dyn Storage + Send + Sync> = new_in_memory_storage()?;

        let repo = Repository::create(None, Arc::clone(&storage), HashMap::new()).await?;

        // initializing a repo does not create the config file
        assert!(Repository::fetch_config(storage.as_ref()).await?.is_none());
        // it inits with the default config
        assert_eq!(repo.config(), &RepositoryConfig::default());
        // updating the persistent config create a new file with default values
        let etag = repo.save_config().await?;
        assert_ne!(etag, "");
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
        let etag = repo.save_config().await?;
        assert_ne!(etag, "");
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
        let storage: Arc<dyn Storage + Send + Sync> = new_in_memory_storage()?;
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
        let etag = repo.save_config().await?;
        assert_ne!(etag, "");
        assert_eq!(
            &Repository::fetch_config(storage.as_ref()).await?.unwrap().0,
            repo.config()
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_manage_refs() -> Result<(), Box<dyn Error>> {
        let storage: Arc<dyn Storage + Send + Sync> = new_in_memory_storage()?;

        let repo = Repository::create(None, Arc::clone(&storage), HashMap::new()).await?;

        let initial_branches = repo.list_branches().await?;
        assert_eq!(initial_branches, BTreeSet::from(["main".into()]));

        let initial_tags = repo.list_tags().await?;
        assert_eq!(initial_tags, BTreeSet::new());

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
            Err(RepositoryError::CannotDeleteMain)
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
}
