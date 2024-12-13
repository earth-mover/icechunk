use std::{collections::HashMap, iter, sync::Arc};

use bytes::Bytes;
use futures::Stream;
use itertools::Either;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    format::{
        snapshot::{Snapshot, SnapshotMetadata},
        IcechunkFormatError, NodeId, SnapshotId,
    },
    refs::{
        create_tag, fetch_branch_tip, fetch_tag, list_branches, list_tags, update_branch,
        BranchVersion, Ref, RefError,
    },
    session::Session,
    storage::ETag,
    virtual_chunks::{
        mk_default_containers, sort_containers, ContainerName, ObjectStoreCredentials,
        VirtualChunkContainer, VirtualChunkResolver,
    },
    MemCachingStorage, Storage, StorageError,
};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RepositoryConfig {
    // Chunks smaller than this will be stored inline in the manifst
    pub inline_chunk_threshold_bytes: u16,
    // Unsafely overwrite refs on write. This is not recommended, users should only use it at their
    // own risk in object stores for which we don't support write-object-if-not-exists. There is
    // the possibility of race conditions if this variable is set to true and there are concurrent
    // commit attempts.
    pub unsafe_overwrite_refs: bool,

    pub virtual_chunk_containers: Vec<VirtualChunkContainer>,
}

impl Default for RepositoryConfig {
    fn default() -> Self {
        let mut containers = mk_default_containers();
        sort_containers(&mut containers);
        Self {
            inline_chunk_threshold_bytes: 512,
            unsafe_overwrite_refs: false,
            virtual_chunk_containers: containers,
        }
    }
}

impl RepositoryConfig {
    fn add_virtual_chunk_container(&mut self, cont: VirtualChunkContainer) {
        self.virtual_chunk_containers.push(cont);
        sort_containers(&mut self.virtual_chunk_containers);
    }

    pub fn virtual_chunk_containers(&self) -> &Vec<VirtualChunkContainer> {
        &self.virtual_chunk_containers
    }

    pub fn clear_virtual_chunk_containers(&mut self) {
        self.virtual_chunk_containers.clear();
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
}

pub type RepositoryResult<T> = Result<T, RepositoryError>;

// pub struct RepositoryBuilder {
//     config: RepositoryConfig,
//     config_etag: Option<ETag>,
//     storage: Arc<dyn Storage + Send + Sync>,
//     snapshot_id: SnapshotId,
//     change_set: Option<ChangeSet>,
//     virtual_chunk_credentials: HashMap<ContainerName, ObjectStoreCredentials>,
// }

// impl RepositoryBuilder {
//     fn new(
//         storage: Arc<dyn Storage + Send + Sync>,
//         snapshot_id: SnapshotId,
//         config: RepositoryConfig,
//         config_etag: Option<ETag>,
//     ) -> Self {
//         Self {
//             config,
//             config_etag,
//             snapshot_id,
//             storage,
//             change_set: None,
//             virtual_chunk_credentials: HashMap::new(),
//         }
//     }

//     pub fn with_inline_threshold_bytes(&mut self, threshold: u16) -> &mut Self {
//         self.config.inline_chunk_threshold_bytes = threshold;
//         self
//     }

//     pub fn with_unsafe_overwrite_refs(&mut self, value: bool) -> &mut Self {
//         self.config.unsafe_overwrite_refs = value;
//         self
//     }

//     pub fn with_virtual_chunk_container(
//         &mut self,
//         cont: VirtualChunkContainer,
//     ) -> &mut Self {
//         // TODO: validate the container: valid container, no duplicate names, no duplicate prefixes
//         self.config.add_virtual_chunk_container(cont);
//         self
//     }

//     pub fn with_virtual_chunk_containers(
//         &mut self,
//         containers: Vec<VirtualChunkContainer>,
//     ) -> &mut Self {
//         self.config.clear_virtual_chunk_containers();
//         for cont in containers {
//             self.config.add_virtual_chunk_container(cont)
//         }
//         self
//     }

//     pub fn with_virtual_chunk_credentials(
//         &mut self,
//         container: &str,
//         credentials: ObjectStoreCredentials,
//     ) -> &mut Self {
//         self.virtual_chunk_credentials.insert(container.to_string(), credentials);
//         self
//     }

//     pub fn with_change_set(&mut self, change_set_bytes: ChangeSet) -> &mut Self {
//         self.change_set = Some(change_set_bytes);
//         self
//     }

//     pub fn build(&self) -> Repository {
//         Repository::new(
//             self.config.clone(),
//             self.config_etag.clone(),
//             self.storage.clone(),
//             self.snapshot_id.clone(),
//             self.change_set.clone(),
//             self.virtual_chunk_credentials.clone(),
//         )
//     }
// }

#[derive(Debug, Serialize, Deserialize)]
pub struct Repository {
    config: RepositoryConfig,
    config_etag: Option<ETag>,
    storage: Arc<dyn Storage + Send + Sync>,
    virtual_resolver: Arc<VirtualChunkResolver>,
}

impl Repository {
    pub async fn create(
        config: Option<RepositoryConfig>,
        config_etag: Option<ETag>,
        storage: Arc<dyn Storage + Send + Sync>,
        virtual_chunk_credentials: HashMap<ContainerName, ObjectStoreCredentials>,
    ) -> RepositoryResult<Self> {
        if Self::exists(storage.as_ref()).await? {
            return Err(RepositoryError::AlreadyInitialized);
        }

        let config = config.unwrap_or_default();

        // On create we need to create the default branch
        let new_snapshot = Snapshot::empty();
        let new_snapshot_id = new_snapshot.metadata.id.clone();
        storage.write_snapshot(new_snapshot_id.clone(), Arc::new(new_snapshot)).await?;
        update_branch(
            storage.as_ref(),
            Ref::DEFAULT_BRANCH,
            new_snapshot_id.clone(),
            None,
            config.unsafe_overwrite_refs,
        )
        .await?;

        debug_assert!(Self::exists(storage.as_ref()).await.unwrap_or(false));

        let containers = config.virtual_chunk_containers.clone();
        let virtual_resolver =
            Arc::new(VirtualChunkResolver::new(containers, virtual_chunk_credentials));

        Ok(Self { config, config_etag, storage, virtual_resolver })
    }

    pub async fn open(
        config: Option<RepositoryConfig>,
        config_etag: Option<ETag>,
        storage: Arc<dyn Storage + Send + Sync>,
        virtual_chunk_credentials: HashMap<ContainerName, ObjectStoreCredentials>,
    ) -> RepositoryResult<Self> {
        if !Self::exists(storage.as_ref()).await? {
            return Err(RepositoryError::RepositoryDoesntExist);
        }

        let (config, config_etag) = if let Some(config) = config {
            (config, config_etag)
        } else {
            Self::fetch_config(storage.as_ref())
                .await?
                .map(|(config, etag)| (config, Some(etag)))
                .unwrap_or((RepositoryConfig::default(), None))
        };

        let containers = config.virtual_chunk_containers.clone();
        let virtual_resolver =
            Arc::new(VirtualChunkResolver::new(containers, virtual_chunk_credentials));

        Ok(Self { config, config_etag, storage, virtual_resolver })
    }

    pub async fn open_or_create(
        config: Option<RepositoryConfig>,
        config_etag: Option<ETag>,
        storage: Arc<dyn Storage + Send + Sync>,
        virtual_chunk_credentials: HashMap<ContainerName, ObjectStoreCredentials>,
    ) -> RepositoryResult<Self> {
        if Self::exists(storage.as_ref()).await? {
            Self::open(config, config_etag, storage, virtual_chunk_credentials).await
        } else {
            Self::create(config, config_etag, storage, virtual_chunk_credentials).await
        }
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
        Arc::new(MemCachingStorage::new(storage, 2, 2, 0, 2, 0))
    }

    pub async fn fetch_config(
        storage: &(dyn Storage + Send + Sync),
    ) -> RepositoryResult<Option<(RepositoryConfig, ETag)>> {
        match storage.fetch_config().await? {
            Some((bytes, etag)) => {
                let config = serde_yml::from_slice(&bytes)?;
                Ok(Some((config, etag)))
            }
            None => Ok(None),
        }
    }

    pub async fn update_config(&self) -> RepositoryResult<ETag> {
        let bytes = Bytes::from(serde_yml::to_string(self.config())?);
        let res = self.storage.update_config(bytes, self.config_etag.as_deref()).await?;
        Ok(res)
    }

    pub fn config(&self) -> &RepositoryConfig {
        &self.config
    }

    pub fn storage(&self) -> &Arc<dyn Storage + Send + Sync> {
        &self.storage
    }

    /// Returns the sequence of parents of the current session, in order of latest first.
    pub async fn ancestry(
        &self,
        snapshot_id: &SnapshotId,
    ) -> RepositoryResult<impl Stream<Item = RepositoryResult<SnapshotMetadata>>> {
        let parent = self.storage.fetch_snapshot(snapshot_id).await?;
        let last = parent.metadata.clone();
        let it = if parent.short_term_history.len() < parent.total_parents as usize {
            // FIXME: implement splitting of snapshot history
            #[allow(clippy::unimplemented)]
            Either::Left(
                parent.local_ancestry().chain(iter::once_with(|| unimplemented!())),
            )
        } else {
            Either::Right(parent.local_ancestry())
        };

        Ok(futures::stream::iter(iter::once(Ok(last)).chain(it.map(Ok))))
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
            branch_name,
            snapshot_id.clone(),
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

    /// List all branches in the repository.
    pub async fn list_branches(&self) -> RepositoryResult<Vec<String>> {
        let branches = list_branches(self.storage.as_ref()).await?;
        Ok(branches)
    }

    /// Get the snapshot id of the tip of a branch
    pub async fn branch_tip(&self, branch: &str) -> RepositoryResult<SnapshotId> {
        let branch_version = fetch_branch_tip(self.storage.as_ref(), branch).await?;
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
        raise_if_invalid_snapshot_id(self.storage.as_ref(), &snapshot_id).await?;
        let branch_tip = self.branch_tip(branch).await?;
        let version = update_branch(
            self.storage.as_ref(),
            branch,
            snapshot_id.clone(),
            Some(&branch_tip),
            self.config.unsafe_overwrite_refs,
        )
        .await?;

        Ok(version)
    }

    /// Create a new tag in the repository at the given snapshot id
    pub async fn create_tag(
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

    /// List all tags in the repository.
    pub async fn list_tags(&self) -> RepositoryResult<Vec<String>> {
        let tags = list_tags(self.storage.as_ref()).await?;
        Ok(tags)
    }

    pub async fn readonly_session(
        &self,
        version: &VersionInfo,
    ) -> RepositoryResult<Session> {
        let snapshot_id: SnapshotId = match version {
            VersionInfo::SnapshotId(sid) => {
                raise_if_invalid_snapshot_id(self.storage.as_ref(), &sid).await?;
                Ok::<_, RepositoryError>(SnapshotId::from(sid.clone()))
            }
            VersionInfo::TagRef(tag) => {
                let ref_data = fetch_tag(self.storage.as_ref(), tag).await?;
                Ok::<_, RepositoryError>(ref_data.snapshot)
            }
            VersionInfo::BranchTipRef(branch) => {
                let ref_data = fetch_branch_tip(self.storage.as_ref(), branch).await?;
                Ok::<_, RepositoryError>(ref_data.snapshot)
            }
        }?;

        let session = Session::create_readonly_session(
            self.config.clone(),
            self.storage.clone(),
            self.virtual_resolver.clone(),
            snapshot_id,
        );

        Ok(session)
    }

    pub async fn writeable_session(&self, branch: &str) -> RepositoryResult<Session> {
        let ref_data = fetch_branch_tip(self.storage.as_ref(), branch).await?;
        let session = Session::create_writable_session(
            self.config.clone(),
            self.storage.clone(),
            self.virtual_resolver.clone(),
            branch.to_string(),
            ref_data.snapshot,
        );

        Ok(session)
    }
}

pub async fn raise_if_invalid_snapshot_id(
    storage: &(dyn Storage + Send + Sync),
    snapshot_id: &SnapshotId,
) -> RepositoryResult<()> {
    storage
        .fetch_snapshot(snapshot_id)
        .await
        .map_err(|_| RepositoryError::SnapshotNotFound { id: snapshot_id.clone() })?;
    Ok(())
}

#[cfg(test)]
#[allow(clippy::panic, clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::{collections::HashMap, error::Error, sync::Arc};

    use crate::{ObjectStorage, Repository, RepositoryConfig, Storage};

    // use super::*;
    // TODO: Add Tests
    #[tokio::test]
    async fn test_repository_persistent_config() -> Result<(), Box<dyn Error>> {
        let storage: Arc<dyn Storage + Send + Sync> =
            Arc::new(ObjectStorage::new_in_memory_store(Some("prefix".into()))?);

        let repo =
            Repository::create(None, None, Arc::clone(&storage), HashMap::new()).await?;

        // initializing a repo doesn't create the config file
        assert!(Repository::fetch_config(storage.as_ref()).await?.is_none());
        // it inits with the default config
        assert_eq!(repo.config(), &RepositoryConfig::default());
        // updating the persistent config create a new file with default values
        let etag = repo.update_config().await?;
        assert_ne!(etag, "");
        // assert_eq!(
        //     Repository::fetch_config(storage.as_ref()).await?.unwrap().0,
        //     RepositoryConfig::default()
        // );

        // // reload the repo changing config
        // let repo = Repository::open(
        //     Some(RepositoryConfig {
        //         inline_chunk_threshold_bytes: 42,
        //         ..Default::default()
        //     }),
        //     None,
        //     Arc::clone(&storage),
        //     HashMap::new(),
        // )
        // .await?;

        // assert_eq!(repo.config().inline_chunk_threshold_bytes, 42);

        // // update the persistent config
        // let etag = repo.update_config().await?;
        // assert_ne!(etag, "");
        // assert_eq!(
        //     Repository::fetch_config(storage.as_ref())
        //         .await?
        //         .unwrap()
        //         .0
        //         .inline_chunk_threshold_bytes,
        //     42
        // );

        // // verify loading again gets the value from persistent config
        // let repo = Repository::open(None, None, storage, HashMap::new()).await?;
        // assert_eq!(repo.config().inline_chunk_threshold_bytes, 42);
        Ok(())
    }
}
