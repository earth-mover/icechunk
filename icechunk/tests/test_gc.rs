#![allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]

use std::{collections::HashMap, sync::Arc};

use bytes::Bytes;
use chrono::{DateTime, TimeDelta, Utc};
use futures::{StreamExt, TryStreamExt};
use icechunk::{
    Repository, RepositoryConfig, Storage,
    asset_manager::AssetManager,
    config::{
        ManifestConfig, ManifestSplitCondition, ManifestSplitDim,
        ManifestSplitDimCondition, ManifestSplittingConfig,
    },
    format::{ByteRange, ChunkIndices, Path, snapshot::ArrayShape},
    new_in_memory_storage,
    ops::gc::{
        ExpireRefResult, ExpiredRefAction, GCConfig, GCSummary, expire, expire_ref,
        garbage_collect,
    },
    refs::{Ref, update_branch},
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
    let storage_settings = storage.default_settings();

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
        let payload = ds.get_chunk_writer()(bytes.clone()).await?;
        ds.set_chunk_ref(array_path.clone(), ChunkIndices(vec![idx]), Some(payload))
            .await?;
    }

    let first_snap_id = ds.commit("first", None).await?;
    assert_eq!(storage.list_chunks(&storage_settings).await?.count().await, 1100);
    assert_eq!(storage.list_manifests(&storage_settings).await?.count().await, 110);

    let mut ds = repo.writable_session("main").await?;

    // overwrite 10 chunks
    // This will only overwrite one split manifest.
    for idx in 0..10 {
        let bytes = Bytes::copy_from_slice(&0i8.to_be_bytes());
        let payload = ds.get_chunk_writer()(bytes.clone()).await?;
        ds.set_chunk_ref(array_path.clone(), ChunkIndices(vec![idx]), Some(payload))
            .await?;
    }
    let second_snap_id = ds.commit("second", None).await?;
    assert_eq!(storage.list_chunks(&storage_settings).await?.count().await, 1110);
    assert_eq!(storage.list_manifests(&storage_settings).await?.count().await, 111);

    // verify doing gc without dangling objects doesn't change the repo
    let now = Utc::now();
    let gc_config = GCConfig::clean_all(now, now, None);
    let summary = garbage_collect(
        storage.as_ref(),
        &storage_settings,
        repo.asset_manager().clone(),
        &gc_config,
    )
    .await?;
    assert_eq!(summary, GCSummary::default());
    assert_eq!(storage.list_chunks(&storage_settings).await?.count().await, 1110);
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
    update_branch(
        storage.as_ref(),
        &storage_settings,
        "main",
        first_snap_id,
        Some(&second_snap_id),
    )
    .await?;

    // we still have all the chunks
    assert_eq!(storage.list_chunks(&storage_settings).await?.count().await, 1110);
    assert_eq!(storage.list_manifests(&storage_settings).await?.count().await, 111);

    let summary = garbage_collect(
        storage.as_ref(),
        &storage_settings,
        repo.asset_manager().clone(),
        &gc_config,
    )
    .await?;
    assert_eq!(summary.chunks_deleted, 10);
    // only one manifest was re-created, so there is only one garbage manifest
    assert_eq!(summary.manifests_deleted, 1);
    assert_eq!(summary.snapshots_deleted, 1);
    assert!(summary.bytes_deleted > summary.chunks_deleted);

    // 10 chunks should be drop
    assert_eq!(storage.list_chunks(&storage_settings).await?.count().await, 1100);
    assert_eq!(storage.list_manifests(&storage_settings).await?.count().await, 110);
    assert_eq!(storage.list_snapshots(&storage_settings).await?.count().await, 2);

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
pub async fn test_expire_ref_in_memory() -> Result<(), Box<dyn std::error::Error>> {
    let storage: Arc<dyn Storage + Send + Sync> = new_in_memory_storage().await?;
    do_test_expire_ref(storage).await
}

#[tokio_test]
#[ignore = "needs credentials from env"]
pub async fn test_expire_ref_in_aws() -> Result<(), Box<dyn std::error::Error>> {
    let prefix = format!("test_expire_ref_{}", Utc::now().timestamp_millis());
    let storage: Arc<dyn Storage + Send + Sync> =
        common::make_aws_integration_storage(prefix)?;
    do_test_expire_ref(storage).await
}

#[tokio_test]
#[ignore = "needs credentials from env"]
pub async fn test_expire_ref_in_r2() -> Result<(), Box<dyn std::error::Error>> {
    let prefix = format!("test_expire_ref_{}", Utc::now().timestamp_millis());
    let storage: Arc<dyn Storage + Send + Sync> =
        common::make_r2_integration_storage(prefix)?;
    do_test_expire_ref(storage).await
}

#[tokio_test]
#[ignore = "needs credentials from env"]
pub async fn test_expire_ref_in_tigris() -> Result<(), Box<dyn std::error::Error>> {
    let prefix = format!("test_expire_ref_{}", Utc::now().timestamp_millis());
    let storage: Arc<dyn Storage + Send + Sync> =
        common::make_tigris_integration_storage(prefix)?;
    do_test_expire_ref(storage).await
}

/// In this test, we set up a repo as in the design document for expiration.
///
/// We then, expire the branches and tags in the same order as the document
/// and we verify we get the same results.
pub async fn do_test_expire_ref(
    storage: Arc<dyn Storage + Send + Sync>,
) -> Result<(), Box<dyn std::error::Error>> {
    let storage_settings = storage.default_settings();
    let mut repo = Repository::create(None, Arc::clone(&storage), HashMap::new()).await?;

    let expire_older_than = make_design_doc_repo(&mut repo).await?;

    match expire_ref(
        storage.as_ref(),
        &storage_settings,
        repo.asset_manager().clone(),
        &Ref::Branch("main".to_string()),
        expire_older_than,
    )
    .await?
    {
        ExpireRefResult::NothingToDo { .. } => {
            panic!()
        }
        ExpireRefResult::Done { released_snapshots, ref_is_expired, .. } => {
            assert_eq!(released_snapshots.len(), 4);
            assert!(!ref_is_expired);
        }
    }

    assert_eq!(
        branch_commit_messages(&repo, "main").await,
        Vec::from(["14", "13", "12", "Repository initialized"])
    );
    assert_eq!(
        branch_commit_messages(&repo, "develop").await,
        Vec::from(["11", "10", "6", "3", "2", "1", "Repository initialized"])
    );
    assert_eq!(
        branch_commit_messages(&repo, "test").await,
        Vec::from(["9", "7", "6", "3", "2", "1", "Repository initialized"])
    );
    assert_eq!(
        branch_commit_messages(&repo, "qa").await,
        Vec::from(["8", "7", "6", "3", "2", "1", "Repository initialized"])
    );

    match expire_ref(
        storage.as_ref(),
        &storage_settings,
        repo.asset_manager().clone(),
        &Ref::Branch("develop".to_string()),
        expire_older_than,
    )
    .await?
    {
        ExpireRefResult::NothingToDo { .. } => panic!(),
        ExpireRefResult::Done { released_snapshots, ref_is_expired, .. } => {
            assert!(!ref_is_expired);
            assert_eq!(released_snapshots.len(), 4);
        }
    }

    assert_eq!(
        branch_commit_messages(&repo, "main").await,
        Vec::from(["14", "13", "12", "Repository initialized"])
    );
    assert_eq!(
        branch_commit_messages(&repo, "develop").await,
        Vec::from(["11", "10", "Repository initialized"])
    );
    assert_eq!(
        branch_commit_messages(&repo, "test").await,
        Vec::from(["9", "7", "6", "3", "2", "1", "Repository initialized"])
    );
    assert_eq!(
        branch_commit_messages(&repo, "qa").await,
        Vec::from(["8", "7", "6", "3", "2", "1", "Repository initialized"])
    );

    match expire_ref(
        storage.as_ref(),
        &storage_settings,
        repo.asset_manager().clone(),
        &Ref::Branch("test".to_string()),
        expire_older_than,
    )
    .await?
    {
        ExpireRefResult::NothingToDo { .. } => panic!(),
        ExpireRefResult::Done { released_snapshots, ref_is_expired, .. } => {
            assert!(!ref_is_expired);
            assert_eq!(released_snapshots.len(), 5);
        }
    }

    assert_eq!(
        branch_commit_messages(&repo, "main").await,
        Vec::from(["14", "13", "12", "Repository initialized"])
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
        Vec::from(["8", "7", "6", "3", "2", "1", "Repository initialized"])
    );

    match expire_ref(
        storage.as_ref(),
        &storage_settings,
        repo.asset_manager().clone(),
        &Ref::Branch("qa".to_string()),
        expire_older_than,
    )
    .await?
    {
        ExpireRefResult::NothingToDo { .. } => panic!(),
        ExpireRefResult::Done { released_snapshots, ref_is_expired, .. } => {
            assert!(!ref_is_expired);
            assert_eq!(released_snapshots.len(), 5);
        }
    }

    assert_eq!(
        branch_commit_messages(&repo, "main").await,
        Vec::from(["14", "13", "12", "Repository initialized"])
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

    Ok(())
}

#[tokio_test]
pub async fn test_expire_ref_with_odd_timestamps()
-> Result<(), Box<dyn std::error::Error>> {
    let storage: Arc<dyn Storage + Send + Sync> = new_in_memory_storage().await?;
    let storage_settings = storage.default_settings();
    let repo = Repository::create(None, Arc::clone(&storage), HashMap::new()).await?;

    let mut session = repo.writable_session("main").await?;

    let user_data = Bytes::new();
    session.add_group(Path::root(), user_data.clone()).await?;
    session.commit("first", None).await?;
    let mut session = repo.writable_session("main").await?;
    session.add_group("/a".try_into().unwrap(), user_data.clone()).await?;
    session.commit("second", None).await?;
    let mut session = repo.writable_session("main").await?;
    session.add_group("/b".try_into().unwrap(), user_data.clone()).await?;
    session.commit("third", None).await?;

    match expire_ref(
        storage.as_ref(),
        &storage_settings,
        repo.asset_manager().clone(),
        &Ref::Branch("main".to_string()),
        Utc::now() - TimeDelta::days(10),
    )
    .await?
    {
        ExpireRefResult::NothingToDo { ref_is_expired } => {
            assert!(!ref_is_expired);
        }
        _ => panic!(),
    }

    assert_eq!(
        branch_commit_messages(&repo, "main").await,
        Vec::from(["third", "second", "first", "Repository initialized"])
    );

    match expire_ref(
        storage.as_ref(),
        &storage_settings,
        repo.asset_manager().clone(),
        &Ref::Branch("main".to_string()),
        Utc::now() + TimeDelta::days(10),
    )
    .await?
    {
        ExpireRefResult::Done { ref_is_expired, .. } => {
            assert!(ref_is_expired);
            assert_eq!(
                branch_commit_messages(&repo, "main").await,
                Vec::from(["third", "Repository initialized"])
            );
        }
        _ => panic!(),
    }

    assert_eq!(
        branch_commit_messages(&repo, "main").await,
        Vec::from(["third", "Repository initialized"])
    );
    Ok(())
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
    ));

    let result = expire(
        storage.as_ref(),
        &storage_settings,
        asset_manager.clone(),
        expire_older_than,
        ExpiredRefAction::Ignore,
        ExpiredRefAction::Ignore,
    )
    .await?;

    assert_eq!(result.released_snapshots.len(), 7);

    let repo = Repository::open(None, Arc::clone(&storage), HashMap::new()).await?;

    assert_eq!(
        branch_commit_messages(&repo, "main").await,
        Vec::from(["14", "13", "12", "Repository initialized"])
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
    let gc_config = GCConfig::clean_all(now, now, None);
    let asset_manager = Arc::new(AssetManager::new_no_cache(
        storage.clone(),
        storage_settings.clone(),
        1,
    ));

    let summary = garbage_collect(
        storage.as_ref(),
        &storage_settings,
        asset_manager.clone(),
        &gc_config,
    )
    .await?;
    // other expired snapshots are pointed by tags
    assert_eq!(summary.snapshots_deleted, 5);

    // the non expired snapshots + the 2 expired but pointed by tags snapshots
    assert_eq!(storage.list_snapshots(&storage_settings).await?.count().await, 10);

    repo.delete_tag("tag1").await?;

    let summary = garbage_collect(
        storage.as_ref(),
        &storage_settings,
        asset_manager.clone(),
        &gc_config,
    )
    .await?;
    // other expired snapshots are pointed by tag2
    assert_eq!(summary.snapshots_deleted, 1);

    // the non expired snapshots + the 1 pointed by tag2 snapshots
    assert_eq!(storage.list_snapshots(&storage_settings).await?.count().await, 9);

    repo.delete_tag("tag2").await?;

    let summary = garbage_collect(
        storage.as_ref(),
        &storage_settings,
        asset_manager.clone(),
        &gc_config,
    )
    .await?;
    // tag2 snapshosts are released now
    assert_eq!(summary.snapshots_deleted, 1);

    // only the non expired snapshots left
    assert_eq!(storage.list_snapshots(&storage_settings).await?.count().await, 8);

    Ok(())
}

#[tokio_test]
/// In this test, we set up a repo as in the design document for expiration.
///
/// We then, expire old snapshots and garbage collect. We verify we end up
/// with what is expected according to the design document.
pub async fn test_expire_and_garbage_collect_deliting_expired_refs()
-> Result<(), Box<dyn std::error::Error>> {
    let storage: Arc<dyn Storage + Send + Sync> = new_in_memory_storage().await?;
    let storage_settings = storage.default_settings();
    let mut repo = Repository::create(None, Arc::clone(&storage), HashMap::new()).await?;

    let expire_older_than = make_design_doc_repo(&mut repo).await?;

    let asset_manager = Arc::new(AssetManager::new_no_cache(
        storage.clone(),
        storage_settings.clone(),
        1,
    ));

    let result = expire(
        storage.as_ref(),
        &storage_settings,
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
    let gc_config = GCConfig::clean_all(now, now, None);
    let summary = garbage_collect(
        storage.as_ref(),
        &storage_settings,
        asset_manager.clone(),
        &gc_config,
    )
    .await?;

    assert_eq!(summary.snapshots_deleted, 7);
    assert_eq!(summary.transaction_logs_deleted, 7);

    // only the non expired snapshots left
    assert_eq!(storage.list_snapshots(&storage_settings).await?.count().await, 8);
    Ok(())
}
