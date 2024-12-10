use std::{iter, sync::Arc};

use futures::Stream;
use itertools::Either;
use serde::{Deserialize, Serialize};

use crate::{
    format::{snapshot::{Snapshot, SnapshotMetadata}, SnapshotId},
    refs::{
        create_tag, fetch_branch_tip, fetch_tag, list_branches, list_tags,
        update_branch, BranchVersion, Ref, RefError,
    },
    repository::{raise_if_invalid_snapshot_id, RepositoryError, RepositoryResult},
    session::Session,
    storage::virtual_ref::VirtualChunkResolver,
    MemCachingStorage, Storage,
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
}

impl Default for RepositoryConfig {
    fn default() -> Self {
        Self { inline_chunk_threshold_bytes: 512, unsafe_overwrite_refs: false }
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


#[derive(Debug, Serialize, Deserialize)]
pub struct Repository {
    config: RepositoryConfig,
    storage: Arc<dyn Storage + Send + Sync>,
    virtual_resolver: Arc<dyn VirtualChunkResolver + Send + Sync>,
}

impl Repository {
    pub async fn create(
        config: RepositoryConfig,
        storage: Arc<dyn Storage + Send + Sync>,
        virtual_resolver: Arc<dyn VirtualChunkResolver + Send + Sync>,
    ) -> RepositoryResult<Self> {
        if Self::exists(storage.as_ref()).await? {
            return Err(RepositoryError::AlreadyInitialized);
        }

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

        Ok(Self { config, storage, virtual_resolver })
    }

    pub async fn open(
        config: RepositoryConfig,
        storage: Arc<dyn Storage + Send + Sync>,
        virtual_resolver: Arc<dyn VirtualChunkResolver + Send + Sync>,
    ) -> RepositoryResult<Self> {
        if !Self::exists(storage.as_ref()).await? {
            return Err(RepositoryError::AlreadyInitialized);
        }

        Ok(Self { config, storage, virtual_resolver })
    }

    pub async fn open_or_create(
        config: RepositoryConfig,
        storage: Arc<dyn Storage + Send + Sync>,
        virtual_resolver: Arc<dyn VirtualChunkResolver + Send + Sync>,
    ) -> RepositoryResult<Self> {
        if Self::exists(storage.as_ref()).await? {
            Self::open(config, storage, virtual_resolver).await
        } else {
            Self::create(config, storage, virtual_resolver).await
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
