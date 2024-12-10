use std::{iter, sync::Arc};

use futures::Stream;
use itertools::Either;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    format::{snapshot::{Snapshot, SnapshotMetadata}, IcechunkFormatError, SnapshotId},
    refs::{
        create_tag, fetch_branch_tip, fetch_tag, list_branches, list_tags,
        update_branch, BranchVersion, Ref, RefError,
    },
    session::Session,
    storage::virtual_ref::{ObjectStoreVirtualChunkResolver, VirtualChunkResolver},
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

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum RepositoryError {
    #[error("error contacting storage {0}")]
    StorageError(#[from] StorageError),
    #[error("snapshot not found: `{id}`")]
    SnapshotNotFound { id: SnapshotId },
    #[error("error in icechunk file")]
    FormatError(#[from] IcechunkFormatError),
    #[error("ref error: `{0}`")]
    Ref(#[from] RefError),
    #[error("tag error: `{0}`")]
    Tag(String),
    #[error("the repository has been initialized already (default branch exists)")]
    AlreadyInitialized,
    #[error("error in repository serialization `{0}`")]
    SerializationError(#[from] rmp_serde::encode::Error),
    #[error("error in repository deserialization `{0}`")]
    DeserializationError(#[from] rmp_serde::decode::Error),
    #[error("branch update conflict: `({expected_parent:?}) != ({actual_parent:?})`")]
    Conflict { expected_parent: Option<SnapshotId>, actual_parent: Option<SnapshotId> },
}

pub type RepositoryResult<T> = Result<T, RepositoryError>;

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
        virtual_resolver: Option<Arc<dyn VirtualChunkResolver + Send + Sync>>,
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

        let virtual_resolver = virtual_resolver.unwrap_or_else(|| {
            Arc::new(ObjectStoreVirtualChunkResolver::new(None))
        });

        Ok(Self { config, storage, virtual_resolver })
    }

    pub async fn open(
        config: RepositoryConfig,
        storage: Arc<dyn Storage + Send + Sync>,
        virtual_resolver: Option<Arc<dyn VirtualChunkResolver + Send + Sync>>,
    ) -> RepositoryResult<Self> {
        if !Self::exists(storage.as_ref()).await? {
            return Err(RepositoryError::AlreadyInitialized);
        }

        let virtual_resolver = virtual_resolver.unwrap_or_else(|| {
            Arc::new(ObjectStoreVirtualChunkResolver::new(None))
        });

        Ok(Self { config, storage, virtual_resolver })
    }

    pub async fn open_or_create(
        config: RepositoryConfig,
        storage: Arc<dyn Storage + Send + Sync>,
        virtual_resolver: Option<Arc<dyn VirtualChunkResolver + Send + Sync>>,
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

    use std::{error::Error, num::NonZeroU64};

    use crate::{
        conflicts::{
            basic_solver::{BasicConflictSolver, VersionSelection},
            detector::ConflictDetector,
        },
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

    /// Construct two repos on the same storage, with a commit, a group and an array
    ///
    /// Group: /foo/bar
    /// Array: /foo/bar/some-array
    async fn get_repos_for_conflict() -> Result<(Repository, Repository), Box<dyn Error>>
    {
        let storage: Arc<dyn Storage + Send + Sync> =
            Arc::new(ObjectStorage::new_in_memory_store(Some("prefix".into())));
        let mut repo1 = Repository::init(Arc::clone(&storage), false).await?.build();

        repo1.add_group("/foo/bar".try_into().unwrap()).await?;
        repo1.add_array("/foo/bar/some-array".try_into().unwrap(), basic_meta()).await?;
        repo1.commit(Ref::DEFAULT_BRANCH, "create directory", None).await?;

        let repo2 =
            Repository::from_branch_tip(Arc::clone(&storage), "main").await?.build();

        Ok((repo1, repo2))
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

    fn assert_has_conflict(conflict: &Conflict, rebase_result: RepositoryResult<()>) {
        match rebase_result {
            Err(RepositoryError::RebaseFailed { conflicts, .. }) => {
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
        let (mut repo1, mut repo2) = get_repos_for_conflict().await?;

        let conflict_path: Path = "/foo/bar/conflict".try_into().unwrap();
        repo1.add_group(conflict_path.clone()).await?;
        repo1.commit(Ref::DEFAULT_BRANCH, "create group", None).await?;

        repo2.add_array(conflict_path.clone(), basic_meta()).await?;
        repo2.commit("main", "create array", None).await.unwrap_err();
        assert_has_conflict(
            &Conflict::NewNodeConflictsWithExistingNode(conflict_path),
            repo2.rebase(&ConflictDetector, "main").await,
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
        let (mut repo1, mut repo2) = get_repos_for_conflict().await?;

        let conflict_path: Path = "/foo/bar/conflict".try_into().unwrap();
        repo1.add_array(conflict_path.clone(), basic_meta()).await?;
        repo1.commit(Ref::DEFAULT_BRANCH, "create array", None).await?;

        let inner_path: Path = "/foo/bar/conflict/inner".try_into().unwrap();
        repo2.add_array(inner_path.clone(), basic_meta()).await?;
        repo2.commit("main", "create inner array", None).await.unwrap_err();
        assert_has_conflict(
            &Conflict::NewNodeInInvalidGroup(conflict_path),
            repo2.rebase(&ConflictDetector, "main").await,
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
        let (mut repo1, mut repo2) = get_repos_for_conflict().await?;

        let path: Path = "/foo/bar/some-array".try_into().unwrap();
        repo1.update_array(path.clone(), basic_meta()).await?;
        repo1.commit(Ref::DEFAULT_BRANCH, "update array", None).await?;

        repo2.update_array(path.clone(), basic_meta()).await?;
        repo2.commit("main", "update array again", None).await.unwrap_err();
        assert_has_conflict(
            &Conflict::ZarrMetadataDoubleUpdate(path),
            repo2.rebase(&ConflictDetector, "main").await,
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
        let (mut repo1, mut repo2) = get_repos_for_conflict().await?;

        let path: Path = "/foo/bar/some-array".try_into().unwrap();
        repo1.delete_array(path.clone()).await?;
        repo1.commit(Ref::DEFAULT_BRANCH, "delete array", None).await?;

        repo2.update_array(path.clone(), basic_meta()).await?;
        repo2.commit("main", "update array again", None).await.unwrap_err();
        assert_has_conflict(
            &Conflict::ZarrMetadataUpdateOfDeletedArray(path),
            repo2.rebase(&ConflictDetector, "main").await,
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
        let (mut repo1, mut repo2) = get_repos_for_conflict().await?;

        let path: Path = "/foo/bar/some-array".try_into().unwrap();
        repo1
            .set_user_attributes(
                path.clone(),
                Some(UserAttributes::try_new(br#"{"foo":"bar"}"#).unwrap()),
            )
            .await?;
        repo1.commit(Ref::DEFAULT_BRANCH, "update array", None).await?;

        repo2
            .set_user_attributes(
                path.clone(),
                Some(UserAttributes::try_new(br#"{"foo":"bar"}"#).unwrap()),
            )
            .await?;
        repo2.commit("main", "update array user atts", None).await.unwrap_err();
        let node_id = repo2.get_array(&path).await?.id;
        assert_has_conflict(
            &Conflict::UserAttributesDoubleUpdate { path, node_id },
            repo2.rebase(&ConflictDetector, "main").await,
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
        let (mut repo1, mut repo2) = get_repos_for_conflict().await?;

        let path: Path = "/foo/bar/some-array".try_into().unwrap();
        repo1.delete_array(path.clone()).await?;
        repo1.commit(Ref::DEFAULT_BRANCH, "delete array", None).await?;

        repo2
            .set_user_attributes(
                path.clone(),
                Some(UserAttributes::try_new(br#"{"foo":"bar"}"#).unwrap()),
            )
            .await?;
        repo2.commit("main", "update array user atts", None).await.unwrap_err();
        assert_has_conflict(
            &Conflict::UserAttributesUpdateOfDeletedNode(path),
            repo2.rebase(&ConflictDetector, "main").await,
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
        let (mut repo1, mut repo2) = get_repos_for_conflict().await?;

        let path: Path = "/foo/bar/some-array".try_into().unwrap();
        repo1.update_array(path.clone(), basic_meta()).await?;
        repo1.commit(Ref::DEFAULT_BRANCH, "update array", None).await?;

        repo2.delete_array(path.clone()).await?;
        repo2.commit("main", "delete array", None).await.unwrap_err();
        assert_has_conflict(
            &Conflict::DeleteOfUpdatedArray(path),
            repo2.rebase(&ConflictDetector, "main").await,
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
        let (mut repo1, mut repo2) = get_repos_for_conflict().await?;

        let path: Path = "/foo/bar/some-array".try_into().unwrap();
        repo1
            .set_user_attributes(
                path.clone(),
                Some(UserAttributes::try_new(br#"{"foo":"bar"}"#).unwrap()),
            )
            .await?;
        repo1.commit(Ref::DEFAULT_BRANCH, "update user attributes", None).await?;

        repo2.delete_array(path.clone()).await?;
        repo2.commit("main", "delete array", None).await.unwrap_err();
        assert_has_conflict(
            &Conflict::DeleteOfUpdatedArray(path),
            repo2.rebase(&ConflictDetector, "main").await,
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
        let (mut repo1, mut repo2) = get_repos_for_conflict().await?;

        let path: Path = "/foo/bar/some-array".try_into().unwrap();
        repo1
            .set_chunk_ref(
                path.clone(),
                ChunkIndices(vec![0]),
                Some(ChunkPayload::Inline("hello".into())),
            )
            .await?;
        repo1.commit(Ref::DEFAULT_BRANCH, "update chunks", None).await?;

        repo2.delete_array(path.clone()).await?;
        repo2.commit("main", "delete array", None).await.unwrap_err();
        assert_has_conflict(
            &Conflict::DeleteOfUpdatedArray(path),
            repo2.rebase(&ConflictDetector, "main").await,
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
        let (mut repo1, mut repo2) = get_repos_for_conflict().await?;

        let path: Path = "/foo/bar".try_into().unwrap();
        repo1
            .set_user_attributes(
                path.clone(),
                Some(UserAttributes::try_new(br#"{"foo":"bar"}"#).unwrap()),
            )
            .await?;
        repo1.commit(Ref::DEFAULT_BRANCH, "update user attributes", None).await?;

        repo2.delete_group(path.clone()).await?;
        repo2.commit("main", "delete group", None).await.unwrap_err();
        assert_has_conflict(
            &Conflict::DeleteOfUpdatedGroup(path),
            repo2.rebase(&ConflictDetector, "main").await,
        );
        Ok(())
    }

    #[tokio::test()]
    async fn test_rebase_without_fast_forward() -> Result<(), Box<dyn Error>> {
        let storage: Arc<dyn Storage + Send + Sync> =
            Arc::new(ObjectStorage::new_in_memory_store(Some("prefix".into())));
        let mut repo = Repository::init(Arc::clone(&storage), false).await?.build();

        repo.add_group("/".try_into().unwrap()).await?;
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
        repo.add_array(new_array_path.clone(), zarr_meta.clone()).await?;
        repo.commit(Ref::DEFAULT_BRANCH, "create array", None).await?;

        // one writer sets chunks
        // other writer sets the same chunks, generating a conflict

        let mut repo1 =
            Repository::from_branch_tip(Arc::clone(&storage), "main").await?.build();
        let mut repo2 =
            Repository::from_branch_tip(Arc::clone(&storage), "main").await?.build();

        repo1
            .set_chunk_ref(
                new_array_path.clone(),
                ChunkIndices(vec![0]),
                Some(ChunkPayload::Inline("hello".into())),
            )
            .await?;
        repo1
            .set_chunk_ref(
                new_array_path.clone(),
                ChunkIndices(vec![1]),
                Some(ChunkPayload::Inline("hello".into())),
            )
            .await?;
        let conflicting_snap =
            repo1.commit("main", "write two chunks with repo 1", None).await?;

        repo2
            .set_chunk_ref(
                new_array_path.clone(),
                ChunkIndices(vec![0]),
                Some(ChunkPayload::Inline("hello".into())),
            )
            .await?;

        // verify we cannot commit
        if let Err(RepositoryError::Conflict { .. }) =
            repo2.commit("main", "write one chunk with repo2", None).await
        {
            // detect conflicts using rebase
            let result = repo2.rebase(&ConflictDetector, "main").await;
            // assert the conflict is double chunk update
            assert!(matches!(
            result,
            Err(RepositoryError::RebaseFailed { snapshot, conflicts, })
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
        let storage: Arc<dyn Storage + Send + Sync> =
            Arc::new(ObjectStorage::new_in_memory_store(Some("prefix".into())));
        let mut repo = Repository::init(Arc::clone(&storage), false).await?.build();

        repo.add_group("/".try_into().unwrap()).await?;
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
        repo.add_array(new_array_path.clone(), zarr_meta.clone()).await?;
        let array_created_snap =
            repo.commit(Ref::DEFAULT_BRANCH, "create array", None).await?;

        let mut repo1 =
            Repository::from_branch_tip(Arc::clone(&storage), "main").await?.build();

        repo1
            .set_chunk_ref(
                new_array_path.clone(),
                ChunkIndices(vec![0]),
                Some(ChunkPayload::Inline("hello0".into())),
            )
            .await?;
        repo1
            .set_chunk_ref(
                new_array_path.clone(),
                ChunkIndices(vec![1]),
                Some(ChunkPayload::Inline("hello1".into())),
            )
            .await?;

        let new_array_2_path: Path = "/array_2".try_into().unwrap();
        repo1.add_array(new_array_2_path.clone(), zarr_meta.clone()).await?;
        repo1
            .set_chunk_ref(
                new_array_2_path.clone(),
                ChunkIndices(vec![0]),
                Some(ChunkPayload::Inline("bye0".into())),
            )
            .await?;

        let conflicting_snap =
            repo1.commit("main", "write two chunks with repo 1", None).await?;

        // let's try to create a new commit, that conflicts with the previous one but writes to
        // different chunks
        let mut repo2 =
            Repository::update(Arc::clone(&storage), array_created_snap.clone()).build();
        repo2
            .set_chunk_ref(
                new_array_path.clone(),
                ChunkIndices(vec![2]),
                Some(ChunkPayload::Inline("hello2".into())),
            )
            .await?;
        if let Err(RepositoryError::Conflict { .. }) =
            repo2.commit("main", "write one chunk with repo2", None).await
        {
            let solver = BasicConflictSolver::default();
            // different chunks were written so this should fast forward
            repo2.rebase(&solver, "main").await?;
            repo2.commit("main", "after conflict", None).await?;
            let data =
                repo2.get_chunk_ref(&new_array_path, &ChunkIndices(vec![2])).await?;
            assert_eq!(data, Some(ChunkPayload::Inline("hello2".into())));

            // other chunks written by the conflicting commit are still there
            let data =
                repo2.get_chunk_ref(&new_array_path, &ChunkIndices(vec![0])).await?;
            assert_eq!(data, Some(ChunkPayload::Inline("hello0".into())));
            let data =
                repo2.get_chunk_ref(&new_array_path, &ChunkIndices(vec![1])).await?;
            assert_eq!(data, Some(ChunkPayload::Inline("hello1".into())));

            // new arrays written by the conflicting commit are still there
            let data =
                repo2.get_chunk_ref(&new_array_2_path, &ChunkIndices(vec![0])).await?;
            assert_eq!(data, Some(ChunkPayload::Inline("bye0".into())));

            let commits = repo2.ancestry().await?.try_collect::<Vec<_>>().await?;
            assert_eq!(commits[0].message, "after conflict");
            assert_eq!(commits[1].message, "write two chunks with repo 1");
        } else {
            panic!("Bad test, it should conflict")
        }

        // reset the branch to what repo1 wrote
        let current_snap = fetch_branch_tip(storage.as_ref(), "main").await?.snapshot;
        update_branch(
            storage.as_ref(),
            "main",
            conflicting_snap.clone(),
            Some(&current_snap),
            false,
        )
        .await?;

        // let's try to create a new commit, that conflicts with the previous one and writes
        // to the same chunk, recovering with "Fail" policy (so it shouldn't recover)
        let mut repo2 =
            Repository::update(Arc::clone(&storage), array_created_snap.clone()).build();
        repo2
            .set_chunk_ref(
                new_array_path.clone(),
                ChunkIndices(vec![1]),
                Some(ChunkPayload::Inline("overridden".into())),
            )
            .await?;

        if let Err(RepositoryError::Conflict { .. }) =
            repo2.commit("main", "write one chunk with repo2", None).await
        {
            let solver = BasicConflictSolver {
                on_chunk_conflict: VersionSelection::Fail,
                ..BasicConflictSolver::default()
            };

            let res = repo2.rebase(&solver, "main").await;
            assert!(matches!(
            res,
            Err(RepositoryError::RebaseFailed { snapshot, conflicts, })
                if snapshot == conflicting_snap &&
                conflicts.len() == 1 &&
                matches!(conflicts[0], Conflict::ChunkDoubleUpdate { ref path, ref chunk_coordinates, .. }
                            if path == &new_array_path && chunk_coordinates == &[ChunkIndices(vec![1])].into())
                ));
        } else {
            panic!("Bad test, it should conflict")
        }

        // reset the branch to what repo1 wrote
        let current_snap = fetch_branch_tip(storage.as_ref(), "main").await?.snapshot;
        update_branch(
            storage.as_ref(),
            "main",
            conflicting_snap.clone(),
            Some(&current_snap),
            false,
        )
        .await?;

        // let's try to create a new commit, that conflicts with the previous one and writes
        // to the same chunk, recovering with "UseOurs" policy
        let mut repo2 =
            Repository::update(Arc::clone(&storage), array_created_snap.clone()).build();
        repo2
            .set_chunk_ref(
                new_array_path.clone(),
                ChunkIndices(vec![1]),
                Some(ChunkPayload::Inline("overridden".into())),
            )
            .await?;
        if let Err(RepositoryError::Conflict { .. }) =
            repo2.commit("main", "write one chunk with repo2", None).await
        {
            let solver = BasicConflictSolver {
                on_chunk_conflict: VersionSelection::UseOurs,
                ..Default::default()
            };

            repo2.rebase(&solver, "main").await?;
            repo2.commit("main", "after conflict", None).await?;
            let data =
                repo2.get_chunk_ref(&new_array_path, &ChunkIndices(vec![1])).await?;
            assert_eq!(data, Some(ChunkPayload::Inline("overridden".into())));
            let commits = repo2.ancestry().await?.try_collect::<Vec<_>>().await?;
            assert_eq!(commits[0].message, "after conflict");
            assert_eq!(commits[1].message, "write two chunks with repo 1");
        } else {
            panic!("Bad test, it should conflict")
        }

        // reset the branch to what repo1 wrote
        let current_snap = fetch_branch_tip(storage.as_ref(), "main").await?.snapshot;
        update_branch(
            storage.as_ref(),
            "main",
            conflicting_snap.clone(),
            Some(&current_snap),
            false,
        )
        .await?;

        // let's try to create a new commit, that conflicts with the previous one and writes
        // to the same chunk, recovering with "UseTheirs" policy
        let mut repo2 =
            Repository::update(Arc::clone(&storage), array_created_snap.clone()).build();
        repo2
            .set_chunk_ref(
                new_array_path.clone(),
                ChunkIndices(vec![1]),
                Some(ChunkPayload::Inline("overridden".into())),
            )
            .await?;
        if let Err(RepositoryError::Conflict { .. }) =
            repo2.commit("main", "write one chunk with repo2", None).await
        {
            let solver = BasicConflictSolver {
                on_chunk_conflict: VersionSelection::UseTheirs,
                ..Default::default()
            };

            repo2.rebase(&solver, "main").await?;
            repo2.commit("main", "after conflict", None).await?;
            let data =
                repo2.get_chunk_ref(&new_array_path, &ChunkIndices(vec![1])).await?;
            assert_eq!(data, Some(ChunkPayload::Inline("hello1".into())));
            let commits = repo2.ancestry().await?.try_collect::<Vec<_>>().await?;
            assert_eq!(commits[0].message, "after conflict");
            assert_eq!(commits[1].message, "write two chunks with repo 1");
        } else {
            panic!("Bad test, it should conflict")
        }

        Ok(())
    }

    #[tokio::test]
    /// Test conflict resolution with rebase
    ///
    /// Two sessions write user attributes to the same array
    /// We attempt to recover using [`VersionSelection::UseOurs`] policy
    async fn test_conflict_resolution_double_user_atts_edit_with_ours(
    ) -> Result<(), Box<dyn Error>> {
        let (mut repo1, mut repo2) = get_repos_for_conflict().await?;

        let path: Path = "/foo/bar/some-array".try_into().unwrap();
        repo1
            .set_user_attributes(
                path.clone(),
                Some(UserAttributes::try_new(br#"{"repo":1}"#).unwrap()),
            )
            .await?;
        repo1.commit(Ref::DEFAULT_BRANCH, "update array", None).await?;

        repo2
            .set_user_attributes(
                path.clone(),
                Some(UserAttributes::try_new(br#"{"repo":2}"#).unwrap()),
            )
            .await?;
        repo2.commit("main", "update array user atts", None).await.unwrap_err();

        let solver = BasicConflictSolver {
            on_user_attributes_conflict: VersionSelection::UseOurs,
            ..Default::default()
        };

        repo2.rebase(&solver, "main").await?;
        repo2.commit("main", "after conflict", None).await?;

        let atts = repo2.get_node(&path).await.unwrap().user_attributes.unwrap();
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
        let (mut repo1, mut repo2) = get_repos_for_conflict().await?;

        let path: Path = "/foo/bar/some-array".try_into().unwrap();
        repo1
            .set_user_attributes(
                path.clone(),
                Some(UserAttributes::try_new(br#"{"repo":1}"#).unwrap()),
            )
            .await?;
        repo1.commit(Ref::DEFAULT_BRANCH, "update array", None).await?;

        // we made one extra random change to the repo, because we'll undo the user attributes
        // update and we cannot commit an empty change
        repo2.add_group("/baz".try_into().unwrap()).await?;

        repo2
            .set_user_attributes(
                path.clone(),
                Some(UserAttributes::try_new(br#"{"repo":2}"#).unwrap()),
            )
            .await?;
        repo2.commit("main", "update array user atts", None).await.unwrap_err();

        let solver = BasicConflictSolver {
            on_user_attributes_conflict: VersionSelection::UseTheirs,
            ..Default::default()
        };

        repo2.rebase(&solver, "main").await?;
        repo2.commit("main", "after conflict", None).await?;

        let atts = repo2.get_node(&path).await.unwrap().user_attributes.unwrap();
        assert_eq!(
            atts,
            UserAttributesSnapshot::Inline(
                UserAttributes::try_new(br#"{"repo":1}"#).unwrap()
            )
        );

        repo2.get_node(&"/baz".try_into().unwrap()).await?;
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
        let (mut repo1, mut repo2) = get_repos_for_conflict().await?;

        let path: Path = "/foo/bar/some-array".try_into().unwrap();
        repo1.update_array(path.clone(), basic_meta()).await?;
        repo1.commit(Ref::DEFAULT_BRANCH, "update array", None).await?;

        repo2.delete_array(path.clone()).await?;
        repo2.commit("main", "delete array", None).await.unwrap_err();

        repo2.rebase(&BasicConflictSolver::default(), "main").await?;
        repo2.commit("main", "after conflict", None).await?;

        assert!(matches!(
            repo2.get_node(&path).await,
            Err(RepositoryError::NodeNotFound { .. })
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
        let (mut repo1, mut repo2) = get_repos_for_conflict().await?;

        let path: Path = "/foo/bar/some-array".try_into().unwrap();
        // write chunks with repo 1
        for coord in [0u32, 1, 2] {
            repo1
                .set_chunk_ref(
                    path.clone(),
                    ChunkIndices(vec![coord]),
                    Some(ChunkPayload::Inline("repo 1".into())),
                )
                .await?;
            repo1
                .commit(
                    Ref::DEFAULT_BRANCH,
                    format!("update chunk {}", coord).as_str(),
                    None,
                )
                .await?;
        }

        // write the same chunks with repo 2
        for coord in [0u32, 1, 2] {
            repo2
                .set_chunk_ref(
                    path.clone(),
                    ChunkIndices(vec![coord]),
                    Some(ChunkPayload::Inline("repo 2".into())),
                )
                .await?;
        }

        repo2
            .commit(Ref::DEFAULT_BRANCH, "update chunk on repo 2", None)
            .await
            .unwrap_err();

        let solver = BasicConflictSolver {
            on_chunk_conflict: VersionSelection::UseTheirs,
            ..Default::default()
        };

        repo2.rebase(&solver, "main").await?;
        repo2.commit("main", "after conflict", None).await?;
        for coord in [0, 1, 2] {
            let payload = repo2.get_chunk_ref(&path, &ChunkIndices(vec![coord])).await?;
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
        let (mut repo1, mut repo2) = get_repos_for_conflict().await?;

        let path: Path = "/foo/bar/some-array".try_into().unwrap();
        repo1
            .set_user_attributes(
                path.clone(),
                Some(UserAttributes::try_new(br#"{"repo":1}"#).unwrap()),
            )
            .await?;
        let non_conflicting_snap =
            repo1.commit(Ref::DEFAULT_BRANCH, "update user atts", None).await?;

        repo1
            .set_chunk_ref(
                path.clone(),
                ChunkIndices(vec![0]),
                Some(ChunkPayload::Inline("repo 1".into())),
            )
            .await?;

        let conflicting_snap =
            repo1.commit(Ref::DEFAULT_BRANCH, "update chunk ref", None).await?;

        repo2
            .set_chunk_ref(
                path.clone(),
                ChunkIndices(vec![0]),
                Some(ChunkPayload::Inline("repo 2".into())),
            )
            .await?;

        repo2.commit(Ref::DEFAULT_BRANCH, "update chunk ref", None).await.unwrap_err();
        // we setup a [`ConflictSolver`]` that can recover from the first but not the second
        // conflict
        let solver = BasicConflictSolver {
            on_chunk_conflict: VersionSelection::Fail,
            ..Default::default()
        };

        let err = repo2.rebase(&solver, "main").await.unwrap_err();

        assert!(matches!(
        err,
        RepositoryError::RebaseFailed { snapshot, conflicts}
            if snapshot == conflicting_snap &&
            conflicts.len() == 1 &&
            matches!(conflicts[0], Conflict::ChunkDoubleUpdate { ref path, ref chunk_coordinates, .. }
                        if path == path && chunk_coordinates == &[ChunkIndices(vec![0])].into())
            ));

        // we were able to rebase one commit but not the second one,
        // so now the parent is the first commit
        assert_eq!(repo2.snapshot_id(), &non_conflicting_snap);

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
