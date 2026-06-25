use std::{
    collections::HashMap,
    num::{NonZeroU16, NonZeroUsize},
    sync::Arc,
};

use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::{StreamExt as _, TryStreamExt as _};
use icechunk::{
    Repository, RepositoryConfig, Storage,
    asset_manager::AssetManager,
    config::{
        DEFAULT_MAX_CONCURRENT_REQUESTS, ManifestConfig, ManifestSplitCondition,
        ManifestSplitDim, ManifestSplitDimCondition, ManifestSplittingConfig,
    },
    format::{
        ByteRange, ChunkIndices, Path, format_constants::SpecVersionBin,
        manifest::ChunkPayload, snapshot::ArrayShape,
    },
    new_in_memory_storage,
    ops::gc::{ExpiredRefAction, GCConfig, GCSummary, expire, garbage_collect},
    refs::Ref,
    repository::VersionInfo,
    session::get_chunk,
};
use icechunk_macros::tokio_test;
use pretty_assertions::assert_eq;

use crate::common;
use crate::common::Permission;

#[tokio_test]
async fn test_gc_in_minio_spec_v1() -> Result<(), Box<dyn std::error::Error>> {
    let prefix = format!("test_gc_v1_{}", Utc::now().timestamp_millis());
    let storage = common::make_minio_integration_storage(prefix, &Permission::Modify)?;
    do_test_gc(storage, Some(SpecVersionBin::V1)).await
}

#[tokio_test]
async fn test_gc_in_minio_spec_v2() -> Result<(), Box<dyn std::error::Error>> {
    let prefix = format!("test_gc_v2_{}", Utc::now().timestamp_millis());
    let storage = common::make_minio_integration_storage(prefix, &Permission::Modify)?;
    do_test_gc(storage, Some(SpecVersionBin::V2)).await
}

#[tokio_test]
#[ignore = "needs credentials from env"]
async fn test_gc_in_aws() -> Result<(), Box<dyn std::error::Error>> {
    let prefix = format!("test_gc_{}", Utc::now().timestamp_millis());
    let storage = common::make_aws_integration_storage(prefix)?;
    do_test_gc(storage, None).await
}

#[tokio_test]
#[ignore = "needs credentials from env"]
async fn test_gc_in_r2() -> Result<(), Box<dyn std::error::Error>> {
    let prefix = format!("test_gc_{}", Utc::now().timestamp_millis());
    let storage = common::make_r2_integration_storage(prefix)?;
    do_test_gc(storage, None).await
}

#[tokio_test]
#[ignore = "needs credentials from env"]
async fn test_gc_in_tigris() -> Result<(), Box<dyn std::error::Error>> {
    let prefix = format!("test_gc_{}", Utc::now().timestamp_millis());
    let storage = common::make_tigris_integration_storage(prefix)?;
    do_test_gc(storage, None).await
}

/// Create a repo with two commits, reset the branch to "forget" the last commit, run gc
///
/// It runs [`garbage_collect`] to verify it's doing its job.
async fn do_test_gc(
    storage: Arc<dyn Storage + Send + Sync>,
    spec_version: Option<SpecVersionBin>,
) -> Result<(), Box<dyn std::error::Error>> {
    let shape = ArrayShape::new(vec![(1100, 1100)]).unwrap();
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
        spec_version,
        true,
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

    let first_snap_id = ds.commit("first").max_concurrent_nodes(8).execute().await?;
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
    let _second_snap_id = ds.commit("second").max_concurrent_nodes(8).execute().await?;
    assert_eq!(repo.asset_manager().list_chunks().await?.count().await, 1110);
    assert_eq!(repo.asset_manager().list_manifests().await?.count().await, 111);

    // verify doing gc without dangling objects doesn't change the repo

    // GC compares the cutoff against each object's `created_at`, which on a real
    // store is its second-precision, server-clock `LastModified`. Sleep so the
    // cutoff clears second-truncation and clock skew (in-memory needs only 1ms;
    // see `threshold_between_commits`).
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
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
    let summary =
        garbage_collect(Arc::clone(repo.asset_manager()), &gc_config, None, 100).await?;
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
    repo.reset_branch("main", &first_snap_id, None).await?;

    // we still have all the chunks
    assert_eq!(repo.asset_manager().list_chunks().await?.count().await, 1110);
    assert_eq!(repo.asset_manager().list_manifests().await?.count().await, 111);

    let summary =
        garbage_collect(Arc::clone(repo.asset_manager()), &gc_config, None, 100).await?;
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

    // Create 5 anonymous snapshots (detached, not on any branch). The first two
    // are expired, the last three kept. Take the cutoff between the groups from
    // `Utc::now()` after a sleep so it clears the second-truncation + clock skew
    // of the server-side `created_at` (`LastModified`) that GC deletes by — same
    // reasoning as the `clean_all` sleep above. A sub-second gap is not enough on
    // a real store (e.g. S3 truncates `LastModified` to whole seconds), so anon[1]
    // could otherwise survive with `created_at` truncated past the cutoff.
    let mut anon_snaps = vec![];
    let mut cutoff = None;
    for i in 0..5 {
        if i == 2 {
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            cutoff = Some(Utc::now());
        }
        let mut session = repo.writable_session("main").await?;
        let bytes = Bytes::copy_from_slice(&(100i8 + i as i8).to_be_bytes());
        let payload = session.get_chunk_writer()?(bytes.clone()).await?;
        session
            .set_chunk_ref(array_path.clone(), ChunkIndices(vec![0]), Some(payload))
            .await?;
        let snap_id = session.commit(format!("anon {i}")).anonymous().execute().await?;
        anon_snaps.push(snap_id);
    }

    // anon[0..2] were created before the cutoff, anon[2..5] after it.
    let cutoff = cutoff.expect("cutoff set at i == 2");
    let gc_config = GCConfig::clean_all(
        cutoff,
        cutoff,
        None,
        NonZeroU16::new(50).unwrap(),
        NonZeroUsize::new(512 * 1024 * 1024).unwrap(),
        NonZeroU16::new(500).unwrap(),
        false,
    );
    let summary =
        garbage_collect(Arc::clone(repo.asset_manager()), &gc_config, None, 100).await?;
    assert_eq!(summary.snapshots_deleted, 2);

    // The last 3 should still be accessible
    for snap_id in &anon_snaps[2..] {
        repo.readonly_session(&VersionInfo::SnapshotId(snap_id.clone())).await?;
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
    session.commit("1").max_concurrent_nodes(8).execute().await?;
    let mut session = repo.writable_session("main").await?;
    session.add_group(Path::try_from("/2").unwrap(), user_data.clone()).await?;
    let commit_2 = session.commit("2").max_concurrent_nodes(8).execute().await?;
    let mut session = repo.writable_session("main").await?;
    session.add_group(Path::try_from("/4").unwrap(), user_data.clone()).await?;
    session.commit("4").max_concurrent_nodes(8).execute().await?;
    let mut session = repo.writable_session("main").await?;
    session.add_group(Path::try_from("/5").unwrap(), user_data.clone()).await?;
    let snap_id = session.commit("5").max_concurrent_nodes(8).execute().await?;
    repo.create_tag("tag2", &snap_id).await?;

    repo.create_branch("develop", &commit_2).await?;
    let mut session = repo.writable_session("develop").await?;
    session.add_group(Path::try_from("/3").unwrap(), user_data.clone()).await?;
    let snap_id = session.commit("3").max_concurrent_nodes(8).execute().await?;
    repo.create_tag("tag1", &snap_id).await?;
    let mut session = repo.writable_session("develop").await?;
    session.add_group(Path::try_from("/6").unwrap(), user_data.clone()).await?;
    let commit_6 = session.commit("6").max_concurrent_nodes(8).execute().await?;

    repo.create_branch("test", &commit_6).await?;
    let mut session = repo.writable_session("test").await?;
    session.add_group(Path::try_from("/7").unwrap(), user_data.clone()).await?;
    let commit_7 = session.commit("7").max_concurrent_nodes(8).execute().await?;

    let expire_older_than = Utc::now();

    repo.create_branch("qa", &commit_7).await?;
    let mut session = repo.writable_session("qa").await?;
    session.add_group(Path::try_from("/8").unwrap(), user_data.clone()).await?;
    session.commit("8").max_concurrent_nodes(8).execute().await?;
    let mut session = repo.writable_session("main").await?;
    session.add_group(Path::try_from("/12").unwrap(), user_data.clone()).await?;
    session.commit("12").max_concurrent_nodes(8).execute().await?;
    let mut session = repo.writable_session("main").await?;
    session.add_group(Path::try_from("/13").unwrap(), user_data.clone()).await?;
    session.commit("13").max_concurrent_nodes(8).execute().await?;
    let mut session = repo.writable_session("main").await?;
    session.add_group(Path::try_from("/14").unwrap(), user_data.clone()).await?;
    session.commit("14").max_concurrent_nodes(8).execute().await?;
    let mut session = repo.writable_session("develop").await?;
    session.add_group(Path::try_from("/10").unwrap(), user_data.clone()).await?;
    session.commit("10").max_concurrent_nodes(8).execute().await?;
    let mut session = repo.writable_session("develop").await?;
    session.add_group(Path::try_from("/11").unwrap(), user_data.clone()).await?;
    session.commit("11").max_concurrent_nodes(8).execute().await?;
    let mut session = repo.writable_session("test").await?;
    session.add_group(Path::try_from("/9").unwrap(), user_data.clone()).await?;
    session.commit("9").max_concurrent_nodes(8).execute().await?;

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
async fn test_expire_and_garbage_collect_in_memory()
-> Result<(), Box<dyn std::error::Error>> {
    let storage: Arc<dyn Storage + Send + Sync> = new_in_memory_storage().await?;
    do_test_expire_and_garbage_collect(storage).await
}

#[tokio_test]
async fn test_expire_and_garbage_collect_in_minio()
-> Result<(), Box<dyn std::error::Error>> {
    let prefix =
        format!("test_expire_and_garbage_collect_{}", Utc::now().timestamp_millis());
    let storage: Arc<dyn Storage + Send + Sync> =
        common::make_minio_integration_storage(prefix, &Permission::Modify)?;
    do_test_expire_and_garbage_collect(storage).await
}

#[tokio_test]
#[ignore = "needs credentials from env"]
async fn test_expire_and_garbage_collect_in_aws() -> Result<(), Box<dyn std::error::Error>>
{
    let prefix =
        format!("test_expire_and_garbage_collect_{}", Utc::now().timestamp_millis());
    let storage: Arc<dyn Storage + Send + Sync> =
        common::make_aws_integration_storage(prefix)?;
    do_test_expire_and_garbage_collect(storage).await
}

#[tokio_test]
#[ignore = "needs credentials from env"]
async fn test_expire_and_garbage_collect_in_r2() -> Result<(), Box<dyn std::error::Error>>
{
    let prefix =
        format!("test_expire_and_garbage_collect_{}", Utc::now().timestamp_millis());
    let storage: Arc<dyn Storage + Send + Sync> =
        common::make_r2_integration_storage(prefix)?;
    do_test_expire_and_garbage_collect(storage).await
}

#[tokio_test]
#[ignore = "needs credentials from env"]
async fn test_expire_and_garbage_collect_in_tigris()
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
async fn do_test_expire_and_garbage_collect(
    storage: Arc<dyn Storage + Send + Sync>,
) -> Result<(), Box<dyn std::error::Error>> {
    let storage_settings = storage.default_settings().await?;
    let mut repo =
        Repository::create(None, Arc::clone(&storage), HashMap::new(), None, true)
            .await?;

    let expire_older_than = make_design_doc_repo(&mut repo).await?;

    let asset_manager = Arc::new(AssetManager::new_no_cache(
        Arc::clone(&storage),
        storage_settings.clone(),
        SpecVersionBin::current(),
        1,
        DEFAULT_MAX_CONCURRENT_REQUESTS,
    ));

    let result = expire(
        Arc::clone(&asset_manager),
        expire_older_than,
        ExpiredRefAction::Ignore,
        ExpiredRefAction::Ignore,
        None,
        100,
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
        Arc::clone(&storage),
        storage_settings.clone(),
        SpecVersionBin::current(),
        1,
        DEFAULT_MAX_CONCURRENT_REQUESTS,
    ));

    let summary =
        garbage_collect(Arc::clone(&asset_manager), &gc_config, None, 100).await?;
    // other expired snapshots are pointed by tags
    assert_eq!(summary.snapshots_deleted, 5);

    // the non expired snapshots + the 2 expired but pointed by tags snapshots
    assert_eq!(repo.asset_manager().list_snapshots().await?.count().await, 10);

    repo.delete_tag("tag1").await?;

    let summary =
        garbage_collect(Arc::clone(&asset_manager), &gc_config, None, 100).await?;
    // other expired snapshots are pointed by tag2
    assert_eq!(summary.snapshots_deleted, 1);

    // the non expired snapshots + the 1 pointed by tag2 snapshots
    assert_eq!(repo.asset_manager().list_snapshots().await?.count().await, 9);

    repo.delete_tag("tag2").await?;

    let summary =
        garbage_collect(Arc::clone(&asset_manager), &gc_config, None, 100).await?;
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
async fn test_expire_and_garbage_collect_deleting_expired_refs()
-> Result<(), Box<dyn std::error::Error>> {
    let storage: Arc<dyn Storage + Send + Sync> = new_in_memory_storage().await?;
    let storage_settings = storage.default_settings().await?;
    let mut repo =
        Repository::create(None, Arc::clone(&storage), HashMap::new(), None, true)
            .await?;

    let expire_older_than = make_design_doc_repo(&mut repo).await?;

    let asset_manager = Arc::new(AssetManager::new_no_cache(
        Arc::clone(&storage),
        storage_settings.clone(),
        SpecVersionBin::current(),
        1,
        DEFAULT_MAX_CONCURRENT_REQUESTS,
    ));

    let result = expire(
        Arc::clone(&asset_manager),
        expire_older_than,
        // This is different compared to the previous test
        ExpiredRefAction::Delete,
        ExpiredRefAction::Delete,
        None,
        100,
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
    let summary =
        garbage_collect(Arc::clone(&asset_manager), &gc_config, None, 100).await?;

    assert_eq!(summary.snapshots_deleted, 7);
    // The 7 released snapshots' files are deleted, but their transaction logs
    // are all retained: each is referenced by some edited snapshot's
    // pruned_ancestor_tx_logs
    assert_eq!(summary.transaction_logs_deleted, 0);

    // only the non expired snapshots left
    assert_eq!(repo.asset_manager().list_snapshots().await?.count().await, 8);
    Ok(())
}

/// After expiration collapses a branch's history and GC deletes the expired
/// snapshot files, `diff(root, tip)` must still report every change, because
/// the expired commits' transaction logs are retained via the edited tip's
/// `pruned_ancestor_tx_logs`.
#[tokio_test]
async fn test_diff_complete_after_expire_and_gc() -> Result<(), Box<dyn std::error::Error>>
{
    let storage: Arc<dyn Storage + Send + Sync> = new_in_memory_storage().await?;
    let storage_settings = storage.default_settings().await?;
    let repo = Repository::create(None, Arc::clone(&storage), HashMap::new(), None, true)
        .await?;

    // The initial (root) snapshot, which survives expiration and becomes the
    // new parent of the boundary commit.
    let initial_id = repo
        .readonly_session(&VersionInfo::BranchTipRef("main".to_string()))
        .await?
        .snapshot_id()
        .clone();

    let user_data = Bytes::new();
    // Three commits that will be expired (root group, /a, /b)...
    let mut session = repo.writable_session("main").await?;
    session.add_group(Path::root(), user_data.clone()).await?;
    session.commit("root group").execute().await?;
    for name in ["/a", "/b"] {
        let mut session = repo.writable_session("main").await?;
        session.add_group(Path::try_from(name).unwrap(), user_data.clone()).await?;
        session.commit(name).execute().await?;
    }

    let expire_older_than = Utc::now();

    // ...and one that survives, becoming the boundary re-parented to root.
    let mut session = repo.writable_session("main").await?;
    session.add_group(Path::try_from("/c").unwrap(), user_data.clone()).await?;
    session.commit("/c").execute().await?;

    let asset_manager = Arc::new(AssetManager::new_no_cache(
        Arc::clone(&storage),
        storage_settings.clone(),
        SpecVersionBin::current(),
        1,
        DEFAULT_MAX_CONCURRENT_REQUESTS,
    ));
    let result = expire(
        Arc::clone(&asset_manager),
        expire_older_than,
        ExpiredRefAction::Ignore,
        ExpiredRefAction::Ignore,
        None,
        100,
    )
    .await?;
    assert_eq!(result.released_snapshots.len(), 3); // root group, /a, /b
    assert_eq!(result.edited_snapshots.len(), 1); // /c re-parented to root

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
    let summary =
        garbage_collect(Arc::clone(&asset_manager), &gc_config, None, 100).await?;
    // the 3 expired snapshot files are deleted, but their transaction logs are
    // all retained: /c's pruned_ancestor_tx_logs references them.
    assert_eq!(summary.snapshots_deleted, 3);
    assert_eq!(summary.transaction_logs_deleted, 0);

    // Reopen for a fresh view of storage, then diff root -> tip.
    let repo = Repository::open(None, Arc::clone(&storage), HashMap::new()).await?;
    let diff = repo
        .diff(
            &VersionInfo::SnapshotId(initial_id),
            &VersionInfo::BranchTipRef("main".to_string()),
        )
        .await?;

    // Without tracking pruned tx logs this would only report "/c"; with it, the whole history.
    let expected: std::collections::BTreeSet<Path> = [
        Path::root(),
        Path::try_from("/a").unwrap(),
        Path::try_from("/b").unwrap(),
        Path::try_from("/c").unwrap(),
    ]
    .into_iter()
    .collect();
    assert_eq!(diff.new_groups, expected);
    Ok(())
}

/// GC must *discriminate* among expired tx logs: a released snapshot whose log is
/// referenced by a survivor's `pruned_ancestor_tx_logs` is retained, while a
/// released snapshot with no surviving descendant has its log deleted.
///
/// Topology: `main` is `root group -> /a -> /b -> /c`, with the threshold just
/// before `/c`. `/a` and `/b` are released but referenced by the re-parented
/// `/c`, so their logs survive. A `doomed` branch off `/a` (`/d -> /e`, both
/// pre-threshold) is deleted at expiration; `/d` and `/e` have no surviving
/// descendant, so nobody references their logs and GC deletes them.
#[tokio_test]
async fn test_gc_deletes_only_unreferenced_expired_tx_logs()
-> Result<(), Box<dyn std::error::Error>> {
    let storage: Arc<dyn Storage + Send + Sync> = new_in_memory_storage().await?;
    let storage_settings = storage.default_settings().await?;
    let repo = Repository::create(None, Arc::clone(&storage), HashMap::new(), None, true)
        .await?;

    commit_group(&repo, "main", "/").await?;
    let a = commit_group(&repo, "main", "/a").await?;
    let b = commit_group(&repo, "main", "/b").await?;

    repo.create_branch("doomed", &a).await?;
    let d = commit_group(&repo, "doomed", "/d").await?;
    let e = commit_group(&repo, "doomed", "/e").await?;

    let expire_older_than = threshold_between_commits().await;

    // main tip survives, becoming the boundary re-parented to root.
    commit_group(&repo, "main", "/c").await?;

    let asset_manager = Arc::new(AssetManager::new_no_cache(
        Arc::clone(&storage),
        storage_settings.clone(),
        SpecVersionBin::current(),
        1,
        DEFAULT_MAX_CONCURRENT_REQUESTS,
    ));
    let result = expire(
        Arc::clone(&asset_manager),
        expire_older_than,
        ExpiredRefAction::Delete,
        ExpiredRefAction::Ignore,
        None,
        100,
    )
    .await?;
    // root group, /a, /b on main + /d, /e on doomed
    assert_eq!(result.released_snapshots.len(), 5);
    assert_eq!(result.edited_snapshots.len(), 1); // /c re-parented to root
    assert!(result.deleted_refs.iter().any(|r| r.name() == "doomed"));

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
    let summary =
        garbage_collect(Arc::clone(&asset_manager), &gc_config, None, 100).await?;

    // All 5 released snapshot files are deleted...
    assert_eq!(summary.snapshots_deleted, 5);
    // ...but only the two unreferenced logs (doomed's /d, /e): /a and /b are
    // retained via /c's pruned_ancestor_tx_logs.
    assert_eq!(summary.transaction_logs_deleted, 2);

    // The referenced logs survive and stay fetchable...
    asset_manager.fetch_transaction_log(&a).await?;
    asset_manager.fetch_transaction_log(&b).await?;
    // ...while the unreferenced ones are gone.
    assert!(asset_manager.fetch_transaction_log(&d).await.is_err());
    assert!(asset_manager.fetch_transaction_log(&e).await.is_err());
    Ok(())
}

/// Commit a single new group on `branch` and return the new snapshot id.
async fn commit_group(
    repo: &Repository,
    branch: &str,
    path: &str,
) -> Result<icechunk::format::SnapshotId, Box<dyn std::error::Error>> {
    let mut session = repo.writable_session(branch).await?;
    let path = if path == "/" { Path::root() } else { Path::try_from(path).unwrap() };
    session.add_group(path, Bytes::new()).await?;
    Ok(session.commit(branch).execute().await?)
}

/// Capture an expiration threshold cleanly separated from the surrounding
/// commits. The `flushed_at < older_than` gate in expiration is strict, and on a
/// fast machine the previous commit's `flushed_at` and a bare `Utc::now()` read
/// can share a microsecond. Bracketing the read with gaps guarantees prior
/// commits land strictly before the threshold and later commits strictly after.
/// `flushed_at` is microsecond-precision here (no `created_at` truncation, since
/// this is in-memory), so a 1ms gap — which `sleep` always overshoots — is ample.
async fn threshold_between_commits() -> DateTime<Utc> {
    tokio::time::sleep(std::time::Duration::from_millis(1)).await;
    let t = Utc::now();
    tokio::time::sleep(std::time::Duration::from_millis(1)).await;
    t
}

fn clean_all_now() -> GCConfig {
    let now = Utc::now();
    GCConfig::clean_all(
        now,
        now,
        None,
        NonZeroU16::new(50).unwrap(),
        NonZeroUsize::new(512 * 1024 * 1024).unwrap(),
        NonZeroU16::new(500).unwrap(),
        false,
    )
}

/// Expiring the same branch repeatedly must grow `pruned_ancestor_tx_logs`
/// monotonically: a later run edits a *new* boundary, and the ids accumulated
/// by the previous run live on the now-pruned old boundary, which the walk
/// splices in. Diff must stay complete across rounds.
#[tokio_test]
async fn test_repeated_expiration_accumulates_pruned_logs()
-> Result<(), Box<dyn std::error::Error>> {
    let storage: Arc<dyn Storage + Send + Sync> = new_in_memory_storage().await?;
    let repo = Repository::create(None, Arc::clone(&storage), HashMap::new(), None, true)
        .await?;
    // Run expiration/GC on the repo's own asset manager so its caches stay
    // coherent and we can keep committing without reopening.
    let am = Arc::clone(repo.asset_manager());

    let initial_id = repo
        .readonly_session(&VersionInfo::BranchTipRef("main".to_string()))
        .await?
        .snapshot_id()
        .clone();

    // Round 1 victims, then the round-1 boundary /c.
    let g0 = commit_group(&repo, "main", "/").await?;
    let a = commit_group(&repo, "main", "/a").await?;
    let b = commit_group(&repo, "main", "/b").await?;
    let threshold1 = threshold_between_commits().await;
    let c = commit_group(&repo, "main", "/c").await?;

    let r1 = expire(
        Arc::clone(&am),
        threshold1,
        ExpiredRefAction::Ignore,
        ExpiredRefAction::Ignore,
        None,
        100,
    )
    .await?;
    assert_eq!(r1.released_snapshots.len(), 3); // g0, a, b
    assert!(r1.edited_snapshots.contains(&c));

    // Round 2: /c becomes a victim, /d the new boundary.
    let threshold2 = threshold_between_commits().await;
    let d = commit_group(&repo, "main", "/d").await?;
    let _e = commit_group(&repo, "main", "/e").await?;

    let r2 = expire(
        Arc::clone(&am),
        threshold2,
        ExpiredRefAction::Ignore,
        ExpiredRefAction::Ignore,
        None,
        100,
    )
    .await?;
    assert_eq!(r2.released_snapshots.len(), 1); // c
    assert!(r2.edited_snapshots.contains(&d));

    // /d accumulated the whole collapsed run, oldest first: [g0, a, b, c].
    let d_ancestry = repo
        .ancestry(&VersionInfo::SnapshotId(d.clone()))
        .await?
        .try_collect::<Vec<_>>()
        .await?;
    assert_eq!(d_ancestry[0].id, d);
    assert_eq!(
        d_ancestry[0].pruned_ancestor_tx_logs,
        vec![g0.clone(), a.clone(), b.clone(), c.clone()]
    );

    garbage_collect(Arc::clone(&am), &clean_all_now(), None, 100).await?;

    // Diff still reports every group despite two rounds of collapse + GC.
    let diff = repo
        .diff(
            &VersionInfo::SnapshotId(initial_id),
            &VersionInfo::BranchTipRef("main".to_string()),
        )
        .await?;
    let expected: std::collections::BTreeSet<Path> = ["/", "/a", "/b", "/c", "/d", "/e"]
        .into_iter()
        .map(|p| if p == "/" { Path::root() } else { Path::try_from(p).unwrap() })
        .collect();
    assert_eq!(diff.new_groups, expected);
    Ok(())
}

/// Re-parenting a snapshot that already carries `pruned_ancestor_tx_logs` must
/// accumulate, not overwrite. Regression test for
/// <https://github.com/earth-mover/icechunk/pull/2184#pullrequestreview-4461588063>
#[tokio_test]
async fn test_reparent_accumulates_existing_pruned_logs()
-> Result<(), Box<dyn std::error::Error>> {
    let storage: Arc<dyn Storage + Send + Sync> = new_in_memory_storage().await?;
    let repo = Repository::create(None, Arc::clone(&storage), HashMap::new(), None, true)
        .await?;
    let am = Arc::clone(repo.asset_manager());

    let initial_id = repo
        .readonly_session(&VersionInfo::BranchTipRef("main".to_string()))
        .await?
        .snapshot_id()
        .clone();

    // init -> a -> b -> c
    let a = commit_group(&repo, "main", "/a").await?;
    let b = commit_group(&repo, "main", "/b").await?;
    let gc_threshold = threshold_between_commits().await;
    let c = commit_group(&repo, "main", "/c").await?;

    // Reset to /a, then GC between /b and /c: /b is dropped, /c is kept (too new)
    // and re-parented onto /a, seeding pruned_ancestor_tx_logs = [b].
    repo.reset_branch("main", &a, None).await?;
    let gc_config = GCConfig::clean_all(
        gc_threshold,
        gc_threshold,
        None,
        NonZeroU16::new(50).unwrap(),
        NonZeroUsize::new(512 * 1024 * 1024).unwrap(),
        NonZeroU16::new(500).unwrap(),
        false,
    );
    garbage_collect(Arc::clone(&am), &gc_config, None, 100).await?;

    let (written, _) = am.fetch_repo_info().await?;
    let edited = written.find_snapshot(&c)?;
    assert_eq!(edited.parent_id.as_ref(), Some(&a));
    assert_eq!(edited.pruned_ancestor_tx_logs, vec![b.clone()]);

    // Point main back at /c and expire /a. /c must accumulate /a before its
    // existing [b], not overwrite it.
    repo.reset_branch("main", &c, None).await?;
    let expire_threshold = threshold_between_commits().await;
    expire(
        Arc::clone(&am),
        expire_threshold,
        ExpiredRefAction::Ignore,
        ExpiredRefAction::Ignore,
        None,
        100,
    )
    .await?;

    let c_ancestry = repo
        .ancestry(&VersionInfo::SnapshotId(c.clone()))
        .await?
        .try_collect::<Vec<_>>()
        .await?;
    assert_eq!(c_ancestry[0].id, c);
    assert_eq!(c_ancestry[0].pruned_ancestor_tx_logs, vec![a.clone(), b.clone()]);

    // Diff from the initial commit still sees every group after GC.
    garbage_collect(Arc::clone(&am), &clean_all_now(), None, 100).await?;
    let diff = repo
        .diff(
            &VersionInfo::SnapshotId(initial_id),
            &VersionInfo::BranchTipRef("main".to_string()),
        )
        .await?;
    let expected: std::collections::BTreeSet<Path> =
        ["/a", "/b", "/c"].into_iter().map(|p| Path::try_from(p).unwrap()).collect();
    assert_eq!(diff.new_groups, expected);
    Ok(())
}

/// Amending the boundary commit must copy its `pruned_ancestor_tx_logs` onto
/// the amended snapshot, so diff stays complete afterward.
#[tokio_test]
async fn test_amend_preserves_pruned_logs() -> Result<(), Box<dyn std::error::Error>> {
    let storage: Arc<dyn Storage + Send + Sync> = new_in_memory_storage().await?;
    let repo = Repository::create(None, Arc::clone(&storage), HashMap::new(), None, true)
        .await?;
    let am = Arc::clone(repo.asset_manager());

    let initial_id = repo
        .readonly_session(&VersionInfo::BranchTipRef("main".to_string()))
        .await?
        .snapshot_id()
        .clone();

    let g0 = commit_group(&repo, "main", "/").await?;
    let a = commit_group(&repo, "main", "/a").await?;
    let b = commit_group(&repo, "main", "/b").await?;
    let threshold = threshold_between_commits().await;
    let c = commit_group(&repo, "main", "/c").await?;

    let r = expire(
        Arc::clone(&am),
        threshold,
        ExpiredRefAction::Ignore,
        ExpiredRefAction::Ignore,
        None,
        100,
    )
    .await?;
    assert!(r.edited_snapshots.contains(&c));

    // Amend the boundary /c, adding /c_extra.
    let mut session = repo.writable_session("main").await?;
    session.add_group(Path::try_from("/c_extra").unwrap(), Bytes::new()).await?;
    let amended = session.commit("c amended").amend().execute().await?;
    assert_ne!(amended, c);

    // The amended snapshot inherits /c's pruned-ancestor chain.
    let amended_ancestry = repo
        .ancestry(&VersionInfo::SnapshotId(amended.clone()))
        .await?
        .try_collect::<Vec<_>>()
        .await?;
    assert_eq!(
        amended_ancestry[0].pruned_ancestor_tx_logs,
        vec![g0.clone(), a.clone(), b.clone()]
    );

    garbage_collect(Arc::clone(&am), &clean_all_now(), None, 100).await?;

    let diff = repo
        .diff(
            &VersionInfo::SnapshotId(initial_id),
            &VersionInfo::BranchTipRef("main".to_string()),
        )
        .await?;
    let expected: std::collections::BTreeSet<Path> = ["/", "/a", "/b", "/c", "/c_extra"]
        .into_iter()
        .map(|p| if p == "/" { Path::root() } else { Path::try_from(p).unwrap() })
        .collect();
    assert_eq!(diff.new_groups, expected);
    Ok(())
}

/// A conflict that lives only in an expiration-pruned ancestor's transaction
/// log must still be detected during rebase; otherwise expiration would
/// silently hide conflicts (a correctness hazard). This is design doc 016's
/// rebase case.
#[tokio_test]
async fn test_rebase_detects_conflict_in_pruned_ancestor()
-> Result<(), Box<dyn std::error::Error>> {
    use icechunk::conflicts::{Conflict, detector::ConflictDetector};
    use icechunk::session::{SessionError, SessionErrorKind};

    let storage: Arc<dyn Storage + Send + Sync> = new_in_memory_storage().await?;
    let repo = Repository::create(None, Arc::clone(&storage), HashMap::new(), None, true)
        .await?;
    let am = Arc::clone(repo.asset_manager());

    let conflict_path: Path = "/conflict".try_into().unwrap();

    // Session forked at the initial (root) snapshot, adding an *array* at
    // /conflict. Kept pending across the expiration below.
    let mut conflicting = repo.writable_session("main").await?;
    conflicting
        .add_array(
            conflict_path.clone(),
            ArrayShape::new(vec![(5, 5)]).unwrap(),
            None,
            Bytes::new(),
        )
        .await?;

    // On main: a commit adding a *group* at /conflict (will be expired), then a
    // later commit so the group commit becomes a pruned ancestor of the tip.
    let mut session = repo.writable_session("main").await?;
    session.add_group(conflict_path.clone(), Bytes::new()).await?;
    let x = session.commit("add /conflict group").execute().await?;

    let threshold = threshold_between_commits().await;

    let mut session = repo.writable_session("main").await?;
    session.add_group(Path::try_from("/other").unwrap(), Bytes::new()).await?;
    session.commit("add /other").execute().await?;

    let r = expire(
        Arc::clone(&am),
        threshold,
        ExpiredRefAction::Ignore,
        ExpiredRefAction::Ignore,
        None,
        100,
    )
    .await?;
    assert!(r.released_snapshots.contains(&x));

    garbage_collect(Arc::clone(&am), &clean_all_now(), None, 100).await?;

    // The conflict exists only in pruned ancestor X's log; the tip's own log
    // (/other) does not conflict. Rebase must still surface it.
    match conflicting.rebase(&ConflictDetector).await {
        Err(SessionError {
            kind: SessionErrorKind::RebaseFailed { conflicts, .. },
            ..
        }) => {
            assert!(
                conflicts.contains(&Conflict::NewNodeConflictsWithExistingNode(
                    conflict_path.clone()
                )),
                "expected a conflict at {conflict_path:?}, got {conflicts:?}"
            );
        }
        other => panic!("expected a rebase conflict, got {other:?}"),
    }
    Ok(())
}

/// If a pruned-ancestor log is missing (e.g. an older GC deleted it), rebase
/// must error rather than silently skip it and hide conflicts.
#[tokio_test]
async fn test_rebase_errors_on_missing_pruned_ancestor_log()
-> Result<(), Box<dyn std::error::Error>> {
    use futures::stream;
    use icechunk::conflicts::detector::ConflictDetector;
    use icechunk::session::{SessionError, SessionErrorKind};

    let storage: Arc<dyn Storage + Send + Sync> = new_in_memory_storage().await?;
    let repo = Repository::create(None, Arc::clone(&storage), HashMap::new(), None, true)
        .await?;
    let am = Arc::clone(repo.asset_manager());

    let conflict_path: Path = "/conflict".try_into().unwrap();
    let mut conflicting = repo.writable_session("main").await?;
    conflicting
        .add_array(
            conflict_path.clone(),
            ArrayShape::new(vec![(5, 5)]).unwrap(),
            None,
            Bytes::new(),
        )
        .await?;

    let mut session = repo.writable_session("main").await?;
    session.add_group(conflict_path.clone(), Bytes::new()).await?;
    let x = session.commit("add /conflict group").execute().await?;
    let threshold = threshold_between_commits().await;
    let mut session = repo.writable_session("main").await?;
    session.add_group(Path::try_from("/other").unwrap(), Bytes::new()).await?;
    session.commit("add /other").execute().await?;

    expire(
        Arc::clone(&am),
        threshold,
        ExpiredRefAction::Ignore,
        ExpiredRefAction::Ignore,
        None,
        100,
    )
    .await?;
    garbage_collect(Arc::clone(&am), &clean_all_now(), None, 100).await?;

    // Simulate an older GC having deleted the pruned ancestor's tx log.
    am.delete_transaction_logs(stream::once(async { (x.clone(), 0u64) }).boxed()).await?;
    am.remove_cached_tx_log(&x);

    match conflicting.rebase(&ConflictDetector).await {
        Err(SessionError {
            kind: SessionErrorKind::MissingPrunedAncestorTxLog { tx_log, .. },
            ..
        }) => {
            assert_eq!(tx_log, x);
        }
        other => panic!("expected MissingPrunedAncestorTxLog, got {other:?}"),
    }
    Ok(())
}

/// If a pruned-ancestor log is missing, diff degrades gracefully: it warns and
/// skips that log, producing an incomplete diff rather than failing.
#[tokio_test]
async fn test_diff_skips_missing_pruned_log() -> Result<(), Box<dyn std::error::Error>> {
    use futures::stream;

    let storage: Arc<dyn Storage + Send + Sync> = new_in_memory_storage().await?;
    let repo = Repository::create(None, Arc::clone(&storage), HashMap::new(), None, true)
        .await?;
    let am = Arc::clone(repo.asset_manager());

    let initial_id = repo
        .readonly_session(&VersionInfo::BranchTipRef("main".to_string()))
        .await?
        .snapshot_id()
        .clone();

    commit_group(&repo, "main", "/").await?;
    let a = commit_group(&repo, "main", "/a").await?;
    commit_group(&repo, "main", "/b").await?;
    let threshold = threshold_between_commits().await;
    commit_group(&repo, "main", "/c").await?;

    expire(
        Arc::clone(&am),
        threshold,
        ExpiredRefAction::Ignore,
        ExpiredRefAction::Ignore,
        None,
        100,
    )
    .await?;
    garbage_collect(Arc::clone(&am), &clean_all_now(), None, 100).await?;

    // Delete /a's pruned-ancestor tx log, then diff: it should still succeed.
    am.delete_transaction_logs(stream::once(async { (a.clone(), 0u64) }).boxed()).await?;
    am.remove_cached_tx_log(&a);

    let diff = repo
        .diff(
            &VersionInfo::SnapshotId(initial_id),
            &VersionInfo::BranchTipRef("main".to_string()),
        )
        .await?;
    // /a is missing from the diff because its log was skipped.
    let expected: std::collections::BTreeSet<Path> = ["/", "/b", "/c"]
        .into_iter()
        .map(|p| if p == "/" { Path::root() } else { Path::try_from(p).unwrap() })
        .collect();
    assert_eq!(diff.new_groups, expected);
    Ok(())
}

/// Inspecting the transaction log of an expiration-edited snapshot returns the
/// synthetic composite (its pruned-ancestor logs merged with its own), flagged
/// as such — and when an older GC has deleted one of those pruned logs, the
/// missing log is flagged and its content dropped rather than silently omitted.
#[tokio_test]
async fn test_inspect_shows_synthetic_composite() -> Result<(), Box<dyn std::error::Error>>
{
    let storage: Arc<dyn Storage + Send + Sync> = new_in_memory_storage().await?;
    let repo = Repository::create(None, Arc::clone(&storage), HashMap::new(), None, true)
        .await?;
    let am = Arc::clone(repo.asset_manager());

    let g0 = commit_group(&repo, "main", "/").await?;
    let a = commit_group(&repo, "main", "/a").await?;
    let b = commit_group(&repo, "main", "/b").await?;
    let threshold = threshold_between_commits().await;
    let c = commit_group(&repo, "main", "/c").await?;

    // Node ids of the groups each commit created. The composite renders new
    // groups by node id, so these let us assert it actually *merges* the pruned
    // logs' content, not merely lists their ids. /a and /b live only in the
    // pruned-ancestor logs (a, b); /c in c's own log.
    let tip =
        repo.readonly_session(&VersionInfo::BranchTipRef("main".to_string())).await?;
    let a_node = tip.get_node(&Path::try_from("/a").unwrap()).await?.id.to_string();
    let b_node = tip.get_node(&Path::try_from("/b").unwrap()).await?.id.to_string();
    let c_node = tip.get_node(&Path::try_from("/c").unwrap()).await?.id.to_string();

    let r = expire(
        Arc::clone(&am),
        threshold,
        ExpiredRefAction::Ignore,
        ExpiredRefAction::Ignore,
        None,
        100,
    )
    .await?;
    assert!(r.edited_snapshots.contains(&c));

    // Inspecting the edited boundary returns the synthetic composite, naming
    // the pruned-ancestor logs it merged in...
    let json = icechunk::inspect::transaction_log_json(am.as_ref(), &c, true).await?;
    assert!(json.contains("synthetic_composite"));
    assert!(json.contains(&g0.to_string()));
    assert!(json.contains(&a.to_string()));
    assert!(json.contains(&b.to_string()));
    // ...and the merged content: /a and /b (from the pruned logs) plus /c (its
    // own) all appear as new groups.
    assert!(json.contains(&a_node));
    assert!(json.contains(&b_node));
    assert!(json.contains(&c_node));

    // An unedited snapshot's log is rendered as-is, with no composite flag.
    let json_g0 = icechunk::inspect::transaction_log_json(am.as_ref(), &g0, true).await?;
    assert!(!json_g0.contains("synthetic_composite"));

    // GC retains the pruned-ancestor logs (c references them), so the composite
    // stays complete. Now simulate an older GC having deleted one of them.
    use futures::stream;
    garbage_collect(Arc::clone(&am), &clean_all_now(), None, 100).await?;
    am.delete_transaction_logs(stream::once(async { (a.clone(), 0u64) }).boxed()).await?;
    am.remove_cached_tx_log(&a);

    // The composite now flags `a` as missing and drops its content (/a), while
    // still merging the surviving logs (/b from b, /c from c's own).
    let json = icechunk::inspect::transaction_log_json(am.as_ref(), &c, true).await?;
    assert!(json.contains("missing_tx_logs"));
    assert!(json.contains(&a.to_string()));
    assert!(!json.contains(&a_node));
    assert!(json.contains(&b_node));
    assert!(json.contains(&c_node));
    Ok(())
}

#[tokio::test]
async fn test_gc_reset_branch() -> Result<(), Box<dyn std::error::Error>> {
    // Replicates a bug detected by stateful testing.
    // 1. Imagine a sequence of commits: 1, 2, 3, 4, 5
    // 2. Reset branch from (5) to (2)
    // 3. GC commits older than (4)
    // 4. At this point (3) is deleted but (4) and (5) are preserved.
    // 5. The parent of (4) has not been rewritten to None, but is still (3).
    //    Any attempt to trace ancestry from (5) or (4) back will fail.
    // 6. Next GC attempt will en up tracing that ancestry (in pointed_snapshots)

    let storage: Arc<dyn Storage + Send + Sync> = new_in_memory_storage().await?;
    let storage_settings = storage.default_settings().await?;
    let asset_manager = Arc::new(AssetManager::new_no_cache(
        Arc::clone(&storage),
        storage_settings,
        SpecVersionBin::current(),
        1,
        DEFAULT_MAX_CONCURRENT_REQUESTS,
    ));
    let repo = Repository::create(None, Arc::clone(&storage), HashMap::new(), None, true)
        .await?;

    let mut session = repo.writable_session("main").await?;
    let array_path: Path = "/array".to_string().try_into().unwrap();
    let shape = ArrayShape::new(vec![(4, 4)]).unwrap();
    let dimension_names = Some(vec!["t".into()]);
    let def = Bytes::from_static(br#"{"this":"other array"}"#);
    session
        .add_array(
            array_path.clone(),
            shape.clone(),
            dimension_names.clone(),
            def.clone(),
        )
        .await?;
    session.commit("initialized").max_concurrent_nodes(8).execute().await?;

    let mut snaps = vec![];
    for i in 0..6 {
        let mut session = repo.writable_session("main").await?;
        session
            .set_chunk_ref(
                array_path.clone(),
                ChunkIndices(vec![0]),
                Some(ChunkPayload::Inline(Bytes::from(format!("{i}")))),
            )
            .await?;
        let snap = session
            .commit(format!("commit {i}"))
            .max_concurrent_nodes(8)
            .execute()
            .await?;
        snaps.push(snap);
    }

    repo.reset_branch("main", &snaps[1], None).await?;

    let before = repo.lookup_snapshot(&snaps[3]).await?.flushed_at;
    let gc_config = GCConfig::clean_all(
        before,
        before,
        None,
        NonZeroU16::new(50).unwrap(),
        NonZeroUsize::new(512 * 1024 * 1024).unwrap(),
        NonZeroU16::new(500).unwrap(),
        false,
    );
    let summary =
        garbage_collect(Arc::clone(&asset_manager), &gc_config, None, 100).await?;
    assert_eq!(summary.snapshots_deleted, 1);

    // snaps[2] is the dropped ancestor sitting between the retained
    // main tip (snaps[1]) and the retained-because-new snaps[3]. The re-parented
    // snaps[3] must (a) re-parent to its nearest *retained* ancestor snaps[1] —
    // not collapse to the initial id — so the ancestry chain stays contiguous
    // back to the initial commit, and (b) carry snaps[2] in its
    // pruned_ancestor_tx_logs so the dropped ancestor's tx log is preserved and
    // its delta stays reconstructable.
    let (written, _) = asset_manager.fetch_repo_info().await?;
    let edited = written.find_snapshot(&snaps[3])?;
    assert_eq!(edited.parent_id.as_ref(), Some(&snaps[1]));
    assert_eq!(edited.pruned_ancestor_tx_logs, vec![snaps[2].clone()]);

    // snaps[2]'s snapshot was deleted, but its transaction log must survive
    // because snaps[3] now references it; otherwise diff/rebase over snaps[3]
    // would be incomplete.
    asset_manager.fetch_transaction_log(&snaps[2]).await?;

    // make sure ancestry works
    repo.create_tag("foo", &snaps[3]).await?;
    let _anc =
        repo.ancestry(&VersionInfo::TagRef("foo".into())).await?.try_collect::<Vec<_>>();

    repo.create_branch("zoo", &snaps[5]).await?;
    let _anc = repo
        .ancestry(&VersionInfo::BranchTipRef("zoo".into()))
        .await?
        .try_collect::<Vec<_>>();

    // garbage_collect also traces ancestry
    let summary = garbage_collect(asset_manager, &gc_config, None, 100).await?;
    assert_eq!(summary.snapshots_deleted, 0);

    repo.readonly_session(&VersionInfo::SnapshotId(snaps[3].clone())).await?;
    repo.readonly_session(&VersionInfo::SnapshotId(snaps[4].clone())).await?;
    repo.readonly_session(&VersionInfo::SnapshotId(snaps[5].clone())).await?;

    Ok(())
}

/// Regression test for <https://github.com/earth-mover/icechunk/issues/1520>
///
/// When two branches point to the same old commit, expire with
/// `delete_expired_branches` should still delete the non-main branch.
#[tokio_test]
async fn test_expire_deletes_branch_sharing_tip_with_main()
-> Result<(), Box<dyn std::error::Error>> {
    let storage: Arc<dyn Storage + Send + Sync> = new_in_memory_storage().await?;
    let storage_settings = storage.default_settings().await?;
    let repo = Repository::create(None, Arc::clone(&storage), HashMap::new(), None, true)
        .await?;

    let mut session = repo.writable_session("main").await?;
    let user_data = Bytes::new();
    session.add_group(Path::root(), user_data.clone()).await?;
    let commit_1 = session.commit("first").execute().await?;

    repo.create_branch("feature", &commit_1).await?;

    let branches = repo.list_branches().await?;
    assert!(branches.contains("main"));
    assert!(branches.contains("feature"));

    let expire_older_than = Utc::now();

    let asset_manager = Arc::new(AssetManager::new_no_cache(
        Arc::clone(&storage),
        storage_settings.clone(),
        SpecVersionBin::current(),
        1,
        DEFAULT_MAX_CONCURRENT_REQUESTS,
    ));

    let result = expire(
        Arc::clone(&asset_manager),
        expire_older_than,
        ExpiredRefAction::Delete,
        ExpiredRefAction::Ignore,
        None,
        100,
    )
    .await?;

    assert!(result.deleted_refs.contains(&Ref::Branch("feature".to_string())));

    let repo = Repository::open(None, Arc::clone(&storage), HashMap::new()).await?;
    let branches = repo.list_branches().await?;
    assert!(branches.contains("main"));
    assert!(!branches.contains("feature"));

    Ok(())
}
