#![allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]

use std::{collections::HashMap, sync::Arc};

use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::{StreamExt, TryStreamExt};
use icechunk::{
    asset_manager::AssetManager,
    config::{S3Credentials, S3Options, S3StaticCredentials},
    format::{snapshot::ArrayShape, ByteRange, ChunkId, ChunkIndices, Path},
    new_in_memory_storage,
    ops::gc::{
        expire, expire_ref, garbage_collect, ExpireRefResult, ExpiredRefAction, GCConfig,
        GCSummary,
    },
    refs::{update_branch, Ref},
    repository::VersionInfo,
    session::get_chunk,
    storage::new_s3_storage,
    Repository, RepositoryConfig, Storage,
};
use pretty_assertions::assert_eq;

fn minio_s3_config() -> (S3Options, S3Credentials) {
    let config = S3Options {
        region: Some("us-east-1".to_string()),
        endpoint_url: Some("http://localhost:9000".to_string()),
        allow_http: true,
        anonymous: false,
        force_path_style: true,
    };
    let credentials = S3Credentials::Static(S3StaticCredentials {
        access_key_id: "minio123".into(),
        secret_access_key: "minio123".into(),
        session_token: None,
        expires_after: None,
    });
    (config, credentials)
}

#[tokio::test]
/// Create a repo with two commits, reset the branch to "forget" the last commit, run gc
///
/// It runs [`garbage_collect`] to verify it's doing its job.
pub async fn test_gc() -> Result<(), Box<dyn std::error::Error>> {
    let prefix = format!("{:?}", ChunkId::random());
    let (config, credentials) = minio_s3_config();
    let storage: Arc<dyn Storage + Send + Sync> =
        new_s3_storage(config, "testbucket".to_string(), Some(prefix), Some(credentials))
            .expect("Creating minio storage failed");
    let storage_settings = storage.default_settings();
    let asset_manager = Arc::new(AssetManager::new_no_cache(
        storage.clone(),
        storage_settings.clone(),
        1,
    ));

    let repo = Repository::create(
        Some(RepositoryConfig {
            inline_chunk_threshold_bytes: Some(0),
            ..Default::default()
        }),
        Arc::clone(&storage),
        HashMap::new(),
    )
    .await?;

    let mut ds = repo.writable_session("main").await?;

    let user_data = Bytes::new();
    ds.add_group(Path::root(), user_data.clone()).await?;

    let shape = ArrayShape::new(vec![(1100, 1)]).unwrap();

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

    let mut ds = repo.writable_session("main").await?;

    // overwrite 10 chunks
    for idx in 0..10 {
        let bytes = Bytes::copy_from_slice(&0i8.to_be_bytes());
        let payload = ds.get_chunk_writer()(bytes.clone()).await?;
        ds.set_chunk_ref(array_path.clone(), ChunkIndices(vec![idx]), Some(payload))
            .await?;
    }
    let second_snap_id = ds.commit("second", None).await?;
    assert_eq!(storage.list_chunks(&storage_settings).await?.count().await, 1110);

    // verify doing gc without dangling objects doesn't change the repo
    let now = Utc::now();
    let gc_config = GCConfig::clean_all(now, now, None);
    let summary = garbage_collect(
        storage.as_ref(),
        &storage_settings,
        asset_manager.clone(),
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

    let summary = garbage_collect(
        storage.as_ref(),
        &storage_settings,
        asset_manager.clone(),
        &gc_config,
    )
    .await?;
    assert_eq!(summary.chunks_deleted, 10);
    assert_eq!(summary.manifests_deleted, 1);
    assert_eq!(summary.snapshots_deleted, 1);
    assert!(summary.bytes_deleted > summary.chunks_deleted);

    // 10 chunks should be drop
    assert_eq!(storage.list_chunks(&storage_settings).await?.count().await, 1100);
    assert_eq!(storage.list_manifests(&storage_settings).await?.count().await, 1);
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

#[tokio::test]
/// In this test, we set up a repo as in the design document for expiration.
///
/// We then, expire the branches and tags in the same order as the document
/// and we verify we get the same results.
pub async fn test_expire_ref() -> Result<(), Box<dyn std::error::Error>> {
    let storage: Arc<dyn Storage + Send + Sync> = new_in_memory_storage().await?;
    let storage_settings = storage.default_settings();
    let mut repo = Repository::create(None, Arc::clone(&storage), HashMap::new()).await?;

    let expire_older_than = make_design_doc_repo(&mut repo).await?;

    let asset_manager = Arc::new(AssetManager::new_no_cache(
        storage.clone(),
        storage_settings.clone(),
        1,
    ));

    match expire_ref(
        storage.as_ref(),
        &storage_settings,
        asset_manager.clone(),
        &Ref::Branch("main".to_string()),
        expire_older_than,
    )
    .await?
    {
        ExpireRefResult::RefIsExpired => {
            panic!()
        }
        ExpireRefResult::NothingToDo => {
            panic!()
        }
        ExpireRefResult::Done { released_snapshots, .. } => {
            assert_eq!(released_snapshots.len(), 4);
        }
    }

    let repo = Repository::open(None, Arc::clone(&storage), HashMap::new()).await?;

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
        asset_manager.clone(),
        &Ref::Branch("develop".to_string()),
        expire_older_than,
    )
    .await?
    {
        ExpireRefResult::RefIsExpired => panic!(),
        ExpireRefResult::NothingToDo => panic!(),
        ExpireRefResult::Done { released_snapshots, .. } => {
            assert_eq!(released_snapshots.len(), 4);
        }
    }

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
        Vec::from(["9", "7", "6", "3", "2", "1", "Repository initialized"])
    );
    assert_eq!(
        branch_commit_messages(&repo, "qa").await,
        Vec::from(["8", "7", "6", "3", "2", "1", "Repository initialized"])
    );

    match expire_ref(
        storage.as_ref(),
        &storage_settings,
        asset_manager.clone(),
        &Ref::Branch("test".to_string()),
        expire_older_than,
    )
    .await?
    {
        ExpireRefResult::RefIsExpired => panic!(),
        ExpireRefResult::NothingToDo => panic!(),
        ExpireRefResult::Done { released_snapshots, .. } => {
            assert_eq!(released_snapshots.len(), 5);
        }
    }

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
        Vec::from(["8", "7", "6", "3", "2", "1", "Repository initialized"])
    );

    match expire_ref(
        storage.as_ref(),
        &storage_settings,
        asset_manager.clone(),
        &Ref::Branch("qa".to_string()),
        expire_older_than,
    )
    .await?
    {
        ExpireRefResult::RefIsExpired => panic!(),
        ExpireRefResult::NothingToDo => panic!(),
        ExpireRefResult::Done { released_snapshots, .. } => {
            assert_eq!(released_snapshots.len(), 5);
        }
    }

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

    Ok(())
}

#[tokio::test]
/// In this test, we set up a repo as in the design document for expiration.
///
/// We then, expire old snapshots and garbage collect. We verify we end up
/// with what is expected according to the design document.
pub async fn test_expire_and_garbage_collect() -> Result<(), Box<dyn std::error::Error>> {
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
        Vec::from(["3", "2", "1", "Repository initialized"])
    );
    assert_eq!(
        tag_commit_messages(&repo, "tag2").await,
        Vec::from(["5", "4", "2", "1", "Repository initialized"])
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
    assert_eq!(summary.snapshots_deleted, 2);

    // the non expired snapshots + the expired but pointed by tags snapshots
    assert_eq!(storage.list_snapshots(&storage_settings).await?.count().await, 13);

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

    // the non expired snapshots + the expired but pointed by tags snapshots
    assert_eq!(storage.list_snapshots(&storage_settings).await?.count().await, 12);

    repo.delete_tag("tag2").await?;

    let summary = garbage_collect(
        storage.as_ref(),
        &storage_settings,
        asset_manager.clone(),
        &gc_config,
    )
    .await?;
    // tag2 snapshosts are released now
    assert_eq!(summary.snapshots_deleted, 4);

    // only the non expired snapshots left
    assert_eq!(storage.list_snapshots(&storage_settings).await?.count().await, 8);

    Ok(())
}
