#![allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]

use std::{
    collections::HashMap,
    num::{NonZeroU16, NonZeroUsize},
    sync::Arc,
};

use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::{StreamExt, TryStreamExt};
use icechunk::{
    Repository, RepositoryConfig, Storage,
    asset_manager::AssetManager,
    config::{
        DEFAULT_MAX_CONCURRENT_REQUESTS, ManifestConfig, ManifestSplitCondition,
        ManifestSplitDim, ManifestSplitDimCondition, ManifestSplittingConfig,
    },
    format::{ByteRange, ChunkIndices, Path, snapshot::ArrayShape},
    new_in_memory_storage,
    ops::gc::{ExpiredRefAction, GCConfig, GCSummary, expire, garbage_collect},
    repository::VersionInfo,
    session::get_chunk,
};
use icechunk_macros::tokio_test;
use pretty_assertions::assert_eq;

mod common;

#[tokio_test]
pub async fn test_gc_in_minio() -> Result<(), Box<dyn std::error::Error>> {
    let prefix = format!("test_gc_{}", Utc::now().timestamp_millis());
    let storage = common::make_minio_integration_storage(prefix)?;
    do_test_gc(storage).await
}

#[tokio_test]
#[ignore = "needs credentials from env"]
pub async fn test_gc_in_aws() -> Result<(), Box<dyn std::error::Error>> {
    let prefix = format!("test_gc_{}", Utc::now().timestamp_millis());
    let storage = common::make_aws_integration_storage(prefix)?;
    do_test_gc(storage).await
}

#[tokio_test]
#[ignore = "needs credentials from env"]
pub async fn test_gc_in_r2() -> Result<(), Box<dyn std::error::Error>> {
    let prefix = format!("test_gc_{}", Utc::now().timestamp_millis());
    let storage = common::make_r2_integration_storage(prefix)?;
    do_test_gc(storage).await
}

#[tokio_test]
#[ignore = "needs credentials from env"]
pub async fn test_gc_in_tigris() -> Result<(), Box<dyn std::error::Error>> {
    let prefix = format!("test_gc_{}", Utc::now().timestamp_millis());
    let storage = common::make_tigris_integration_storage(prefix)?;
    do_test_gc(storage).await
}

/// Create a repo with two commits, reset the branch to "forget" the last commit, run gc
///
/// It runs [`garbage_collect`] to verify it's doing its job.
pub async fn do_test_gc(
    storage: Arc<dyn Storage + Send + Sync>,
) -> Result<(), Box<dyn std::error::Error>> {
    let shape = ArrayShape::new(vec![(1100, 1)]).unwrap();
    // intentionally small to create garbage
    let manifest_split_size = 10;
    let split_sizes = Some(vec![(
        ManifestSplitCondition::PathMatches { regex: r".*".to_string() },
        vec![ManifestSplitDim {
            condition: ManifestSplitDimCondition::Any,
            num_chunks: manifest_split_size,
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
        Arc::clone(&storage),
        HashMap::new(),
    )
    .await?;

    let mut ds = repo.writable_session("main").await?;

    let user_data = Bytes::new();
    ds.add_group(Path::root(), user_data.clone()).await?;

    let array_path: Path = "/array".try_into().unwrap();
    ds.add_array(array_path.clone(), shape, None, user_data.clone()).await?;
    // we write more than 1k chunks to go beyond the chunk size for object listing and delete
    for idx in 0..1100 {
        let bytes = Bytes::copy_from_slice(&42i8.to_be_bytes());
        let payload = ds.get_chunk_writer()?(bytes.clone()).await?;
        ds.set_chunk_ref(array_path.clone(), ChunkIndices(vec![idx]), Some(payload))
            .await?;
    }

    let first_snap_id = ds.commit("first", None).await?;
    assert_eq!(repo.asset_manager().list_chunks().await?.count().await, 1100);
    assert_eq!(repo.asset_manager().list_manifests().await?.count().await, 110);

    let mut ds = repo.writable_session("main").await?;

    // overwrite 10 chunks
    // This will only overwrite one split manifest.
    for idx in 0..10 {
        let bytes = Bytes::copy_from_slice(&0i8.to_be_bytes());
        let payload = ds.get_chunk_writer()?(bytes.clone()).await?;
        ds.set_chunk_ref(array_path.clone(), ChunkIndices(vec![idx]), Some(payload))
            .await?;
    }
    let _second_snap_id = ds.commit("second", None).await?;
    assert_eq!(repo.asset_manager().list_chunks().await?.count().await, 1110);
    assert_eq!(repo.asset_manager().list_manifests().await?.count().await, 111);

    // verify doing gc without dangling objects doesn't change the repo
    let now = Utc::now();
    let gc_config = GCConfig::clean_all(
        now,
        now,
        None,
        NonZeroU16::new(50).unwrap(),
        NonZeroUsize::new(512 * 1024 * 1024).unwrap(),
        NonZeroU16::new(500).unwrap(),
        false,
    );
    let summary = garbage_collect(repo.asset_manager().clone(), &gc_config).await?;
    assert_eq!(summary, GCSummary::default());
    assert_eq!(repo.asset_manager().list_chunks().await?.count().await, 1110);
    for idx in 0..10 {
        let bytes = get_chunk(
            ds.get_chunk_reader(&array_path, &ChunkIndices(vec![idx]), &ByteRange::ALL)
                .await?,
        )
        .await?
        .unwrap();
        assert_eq!(&0i8.to_be_bytes(), bytes.as_ref());
    }

    // Reset the branch to leave the latest commit dangling
    repo.reset_branch("main", &first_snap_id).await?;

    // we still have all the chunks
    assert_eq!(repo.asset_manager().list_chunks().await?.count().await, 1110);
    assert_eq!(repo.asset_manager().list_manifests().await?.count().await, 111);

    let summary = garbage_collect(repo.asset_manager().clone(), &gc_config).await?;
    assert_eq!(summary.chunks_deleted, 10);
    // only one manifest was re-created, so there is only one garbage manifest
    assert_eq!(summary.manifests_deleted, 1);
    assert_eq!(summary.snapshots_deleted, 1);
    assert!(summary.bytes_deleted > summary.chunks_deleted);

    // 10 chunks should be drop
    assert_eq!(repo.asset_manager().list_chunks().await?.count().await, 1100);
    assert_eq!(repo.asset_manager().list_manifests().await?.count().await, 110);
    assert_eq!(repo.asset_manager().list_snapshots().await?.count().await, 2);

    // Opening the repo on main should give the right data
    let ds =
        repo.readonly_session(&VersionInfo::BranchTipRef("main".to_string())).await?;
    for idx in 0..10 {
        let bytes = get_chunk(
            ds.get_chunk_reader(&array_path, &ChunkIndices(vec![idx]), &ByteRange::ALL)
                .await?,
        )
        .await?
        .unwrap();
        assert_eq!(&42i8.to_be_bytes(), bytes.as_ref());
    }

    Ok(())
}

async fn branch_commit_messages(repo: &Repository, branch: &str) -> Vec<String> {
    repo.ancestry(&VersionInfo::BranchTipRef(branch.to_string()))
        .await
        .unwrap()
        .map_ok(|meta| meta.message)
        .try_collect::<Vec<_>>()
        .await
        .unwrap()
}

async fn tag_commit_messages(repo: &Repository, tag: &str) -> Vec<String> {
    repo.ancestry(&VersionInfo::TagRef(tag.to_string()))
        .await
        .unwrap()
        .map_ok(|meta| meta.message)
        .try_collect::<Vec<_>>()
        .await
        .unwrap()
}

/// Setup a repo in the configuration of design document number 7
/// Returns the timestamp threshold for "old" commits
async fn make_design_doc_repo(
    repo: &mut Repository,
) -> Result<DateTime<Utc>, Box<dyn std::error::Error>> {
    let mut session = repo.writable_session("main").await?;
    let user_data = Bytes::new();
    session.add_group(Path::root(), user_data.clone()).await?;
    session.commit("1", None).await?;
    let mut session = repo.writable_session("main").await?;
    session.add_group(Path::try_from("/2").unwrap(), user_data.clone()).await?;
    let commit_2 = session.commit("2", None).await?;
    let mut session = repo.writable_session("main").await?;
    session.add_group(Path::try_from("/4").unwrap(), user_data.clone()).await?;
    session.commit("4", None).await?;
    let mut session = repo.writable_session("main").await?;
    session.add_group(Path::try_from("/5").unwrap(), user_data.clone()).await?;
    let snap_id = session.commit("5", None).await?;
    repo.create_tag("tag2", &snap_id).await?;

    repo.create_branch("develop", &commit_2).await?;
    let mut session = repo.writable_session("develop").await?;
    session.add_group(Path::try_from("/3").unwrap(), user_data.clone()).await?;
    let snap_id = session.commit("3", None).await?;
    repo.create_tag("tag1", &snap_id).await?;
    let mut session = repo.writable_session("develop").await?;
    session.add_group(Path::try_from("/6").unwrap(), user_data.clone()).await?;
    let commit_6 = session.commit("6", None).await?;

    repo.create_branch("test", &commit_6).await?;
    let mut session = repo.writable_session("test").await?;
    session.add_group(Path::try_from("/7").unwrap(), user_data.clone()).await?;
    let commit_7 = session.commit("7", None).await?;

    let expire_older_than = Utc::now();

    repo.create_branch("qa", &commit_7).await?;
    let mut session = repo.writable_session("qa").await?;
    session.add_group(Path::try_from("/8").unwrap(), user_data.clone()).await?;
    session.commit("8", None).await?;
    let mut session = repo.writable_session("main").await?;
    session.add_group(Path::try_from("/12").unwrap(), user_data.clone()).await?;
    session.commit("12", None).await?;
    let mut session = repo.writable_session("main").await?;
    session.add_group(Path::try_from("/13").unwrap(), user_data.clone()).await?;
    session.commit("13", None).await?;
    let mut session = repo.writable_session("main").await?;
    session.add_group(Path::try_from("/14").unwrap(), user_data.clone()).await?;
    session.commit("14", None).await?;
    let mut session = repo.writable_session("develop").await?;
    session.add_group(Path::try_from("/10").unwrap(), user_data.clone()).await?;
    session.commit("10", None).await?;
    let mut session = repo.writable_session("develop").await?;
    session.add_group(Path::try_from("/11").unwrap(), user_data.clone()).await?;
    session.commit("11", None).await?;
    let mut session = repo.writable_session("test").await?;
    session.add_group(Path::try_from("/9").unwrap(), user_data.clone()).await?;
    session.commit("9", None).await?;

    // let's double check the structuer of the commit tree
    assert_eq!(
        branch_commit_messages(repo, "main").await,
        Vec::from(["14", "13", "12", "5", "4", "2", "1", "Repository initialized"])
    );
    assert_eq!(
        branch_commit_messages(repo, "develop").await,
        Vec::from(["11", "10", "6", "3", "2", "1", "Repository initialized"])
    );
    assert_eq!(
        branch_commit_messages(repo, "test").await,
        Vec::from(["9", "7", "6", "3", "2", "1", "Repository initialized"])
    );
    assert_eq!(
        branch_commit_messages(repo, "qa").await,
        Vec::from(["8", "7", "6", "3", "2", "1", "Repository initialized"])
    );

    Ok(expire_older_than)
}

#[tokio_test]
pub async fn test_expire_and_garbage_collect_in_memory()
-> Result<(), Box<dyn std::error::Error>> {
    let storage: Arc<dyn Storage + Send + Sync> = new_in_memory_storage().await?;
    do_test_expire_and_garbage_collect(storage).await
}

#[tokio_test]
pub async fn test_expire_and_garbage_collect_in_minio()
-> Result<(), Box<dyn std::error::Error>> {
    let prefix =
        format!("test_expire_and_garbage_collect_{}", Utc::now().timestamp_millis());
    let storage: Arc<dyn Storage + Send + Sync> =
        common::make_minio_integration_storage(prefix)?;
    do_test_expire_and_garbage_collect(storage).await
}

#[tokio_test]
#[ignore = "needs credentials from env"]
pub async fn test_expire_and_garbage_collect_in_aws()
-> Result<(), Box<dyn std::error::Error>> {
    let prefix =
        format!("test_expire_and_garbage_collect_{}", Utc::now().timestamp_millis());
    let storage: Arc<dyn Storage + Send + Sync> =
        common::make_aws_integration_storage(prefix)?;
    do_test_expire_and_garbage_collect(storage).await
}

#[tokio_test]
#[ignore = "needs credentials from env"]
pub async fn test_expire_and_garbage_collect_in_r2()
-> Result<(), Box<dyn std::error::Error>> {
    let prefix =
        format!("test_expire_and_garbage_collect_{}", Utc::now().timestamp_millis());
    let storage: Arc<dyn Storage + Send + Sync> =
        common::make_r2_integration_storage(prefix)?;
    do_test_expire_and_garbage_collect(storage).await
}

#[tokio_test]
#[ignore = "needs credentials from env"]
pub async fn test_expire_and_garbage_collect_in_tigris()
-> Result<(), Box<dyn std::error::Error>> {
    let prefix =
        format!("test_expire_and_garbage_collect_{}", Utc::now().timestamp_millis());
    let storage: Arc<dyn Storage + Send + Sync> =
        common::make_tigris_integration_storage(prefix)?;
    do_test_expire_and_garbage_collect(storage).await
}

/// In this test, we set up a repo as in the design document for expiration.
///
/// We then, expire old snapshots and garbage collect. We verify we end up
/// with what is expected according to the design document.
pub async fn do_test_expire_and_garbage_collect(
    storage: Arc<dyn Storage + Send + Sync>,
) -> Result<(), Box<dyn std::error::Error>> {
    let storage_settings = storage.default_settings();
    let mut repo = Repository::create(None, Arc::clone(&storage), HashMap::new()).await?;

    let expire_older_than = make_design_doc_repo(&mut repo).await?;

    let asset_manager = Arc::new(AssetManager::new_no_cache(
        storage.clone(),
        storage_settings.clone(),
        1,
        DEFAULT_MAX_CONCURRENT_REQUESTS,
    ));

    let result = expire(
        asset_manager.clone(),
        expire_older_than,
        ExpiredRefAction::Ignore,
        ExpiredRefAction::Ignore,
    )
    .await?;

    assert_eq!(result.released_snapshots.len(), 5);
    assert_eq!(result.deleted_refs.len(), 0);

    let repo = Repository::open(None, Arc::clone(&storage), HashMap::new()).await?;

    // this behavior is slightly different than the one documented
    // in the initial design doc. IC 2.0 doesn't remove snapshot "5"
    // from the path to root because it's pointed by a tag
    assert_eq!(
        branch_commit_messages(&repo, "main").await,
        Vec::from(["14", "13", "12", "5", "Repository initialized"])
    );
    assert_eq!(
        branch_commit_messages(&repo, "develop").await,
        Vec::from(["11", "10", "Repository initialized"])
    );
    assert_eq!(
        branch_commit_messages(&repo, "test").await,
        Vec::from(["9", "Repository initialized"])
    );
    assert_eq!(
        branch_commit_messages(&repo, "qa").await,
        Vec::from(["8", "Repository initialized"])
    );
    assert_eq!(
        tag_commit_messages(&repo, "tag1").await,
        Vec::from(["3", "Repository initialized"])
    );
    assert_eq!(
        tag_commit_messages(&repo, "tag2").await,
        Vec::from(["5", "Repository initialized"])
    );

    let now = Utc::now();
    let gc_config = GCConfig::clean_all(
        now,
        now,
        None,
        NonZeroU16::new(50).unwrap(),
        NonZeroUsize::new(512 * 1024 * 1024).unwrap(),
        NonZeroU16::new(500).unwrap(),
        false,
    );
    let asset_manager = Arc::new(AssetManager::new_no_cache(
        storage.clone(),
        storage_settings.clone(),
        1,
        DEFAULT_MAX_CONCURRENT_REQUESTS,
    ));

    let summary = garbage_collect(asset_manager.clone(), &gc_config).await?;
    // other expired snapshots are pointed by tags
    assert_eq!(summary.snapshots_deleted, 5);

    // the non expired snapshots + the 2 expired but pointed by tags snapshots
    assert_eq!(repo.asset_manager().list_snapshots().await?.count().await, 10);

    repo.delete_tag("tag1").await?;

    let summary = garbage_collect(asset_manager.clone(), &gc_config).await?;
    // other expired snapshots are pointed by tag2
    assert_eq!(summary.snapshots_deleted, 1);

    // the non expired snapshots + the 1 pointed by tag2 snapshots
    assert_eq!(repo.asset_manager().list_snapshots().await?.count().await, 9);

    repo.delete_tag("tag2").await?;

    let summary = garbage_collect(asset_manager.clone(), &gc_config).await?;
    // tag2 snapshosts are not released yet because it's in the path to root from main
    // this behavior changed in IC 2.0
    assert_eq!(summary.snapshots_deleted, 0);

    // only the non expired snapshots left
    assert_eq!(repo.asset_manager().list_snapshots().await?.count().await, 9);

    Ok(())
}

#[tokio_test]
/// In this test, we set up a repo as in the design document for expiration.
///
/// We then, expire old snapshots and garbage collect. We verify we end up
/// with what is expected according to the design document.
pub async fn test_expire_and_garbage_collect_deleting_expired_refs()
-> Result<(), Box<dyn std::error::Error>> {
    let storage: Arc<dyn Storage + Send + Sync> = new_in_memory_storage().await?;
    let storage_settings = storage.default_settings();
    let mut repo = Repository::create(None, Arc::clone(&storage), HashMap::new()).await?;

    let expire_older_than = make_design_doc_repo(&mut repo).await?;

    let asset_manager = Arc::new(AssetManager::new_no_cache(
        storage.clone(),
        storage_settings.clone(),
        1,
        DEFAULT_MAX_CONCURRENT_REQUESTS,
    ));

    let result = expire(
        asset_manager.clone(),
        expire_older_than,
        // This is different compared to the previous test
        ExpiredRefAction::Delete,
        ExpiredRefAction::Delete,
    )
    .await?;

    assert_eq!(result.released_snapshots.len(), 7);
    assert_eq!(result.deleted_refs.len(), 2);

    let now = Utc::now();
    let gc_config = GCConfig::clean_all(
        now,
        now,
        None,
        NonZeroU16::new(50).unwrap(),
        NonZeroUsize::new(512 * 1024 * 1024).unwrap(),
        NonZeroU16::new(500).unwrap(),
        false,
    );
    let summary = garbage_collect(asset_manager.clone(), &gc_config).await?;

    assert_eq!(summary.snapshots_deleted, 7);
    assert_eq!(summary.transaction_logs_deleted, 7);

    // only the non expired snapshots left
    assert_eq!(repo.asset_manager().list_snapshots().await?.count().await, 8);
    Ok(())
}
