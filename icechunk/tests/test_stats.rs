use std::{
    num::{NonZeroU16, NonZeroUsize},
    sync::Arc,
};

use bytes::Bytes;
use chrono::Utc;
use icechunk::{
    Repository, RepositoryConfig, Storage,
    asset_manager::AssetManager,
    config::DEFAULT_MAX_CONCURRENT_REQUESTS,
    format::{
        ChunkIndices, Path,
        format_constants::SpecVersionBin,
        manifest::{ChunkPayload, VirtualChunkLocation, VirtualChunkRef},
        snapshot::ArrayShape,
    },
    new_in_memory_storage,
    ops::stats::repo_chunks_storage,
};
use icechunk_macros::tokio_test;
use rstest::rstest;
use rstest_reuse::{self, *};

use crate::common;
use crate::common::Permission;

#[template]
#[rstest]
#[case::v1(SpecVersionBin::V1)]
#[case::v2(SpecVersionBin::V2)]
fn spec_version_cases(#[case] spec_version: SpecVersionBin) {}

#[tokio_test]
#[apply(spec_version_cases)]
pub async fn test_repo_chunks_storage_in_memory(
    #[case] spec_version: SpecVersionBin,
) -> Result<(), Box<dyn std::error::Error>> {
    let storage = new_in_memory_storage().await?;
    do_test_repo_chunks_storage(storage, spec_version).await
}

#[tokio_test]
#[apply(spec_version_cases)]
pub async fn test_repo_chunks_storage_in_minio(
    #[case] spec_version: SpecVersionBin,
) -> Result<(), Box<dyn std::error::Error>> {
    let prefix =
        format!("test_stats_{:?}_{}", spec_version, Utc::now().timestamp_millis());
    let storage = common::make_minio_integration_storage(prefix, &Permission::Modify)?;
    do_test_repo_chunks_storage(storage, spec_version).await
}

#[tokio_test]
#[apply(spec_version_cases)]
#[ignore = "needs credentials from env"]
pub async fn test_repo_chunks_storage_in_aws(
    #[case] spec_version: SpecVersionBin,
) -> Result<(), Box<dyn std::error::Error>> {
    let prefix =
        format!("test_stats_{:?}_{}", spec_version, Utc::now().timestamp_millis());
    let storage = common::make_aws_integration_storage(prefix)?;
    do_test_repo_chunks_storage(storage, spec_version).await
}

#[tokio_test]
#[apply(spec_version_cases)]
#[ignore = "needs credentials from env"]
pub async fn test_repo_chunks_storage_in_tigris(
    #[case] spec_version: SpecVersionBin,
) -> Result<(), Box<dyn std::error::Error>> {
    let prefix =
        format!("test_stats_{:?}_{}", spec_version, Utc::now().timestamp_millis());
    let storage = common::make_tigris_integration_storage(prefix)?;
    do_test_repo_chunks_storage(storage, spec_version).await
}

#[tokio_test]
#[apply(spec_version_cases)]
#[ignore = "needs credentials from env"]
pub async fn test_repo_chunks_storage_in_r2(
    #[case] spec_version: SpecVersionBin,
) -> Result<(), Box<dyn std::error::Error>> {
    let prefix =
        format!("test_stats_{:?}_{}", spec_version, Utc::now().timestamp_millis());
    let storage = common::make_r2_integration_storage(prefix)?;
    do_test_repo_chunks_storage(storage, spec_version).await
}

async fn do_test_repo_chunks_storage(
    storage: Arc<dyn Storage + Send + Sync>,
    spec_version: SpecVersionBin,
) -> Result<(), Box<dyn std::error::Error>> {
    let storage_settings = storage.default_settings().await?;
    let asset_manager = Arc::new(AssetManager::new_no_cache(
        storage.clone(),
        storage_settings.clone(),
        spec_version,
        1,
        DEFAULT_MAX_CONCURRENT_REQUESTS,
    ));

    let repo = Repository::create(
        Some(RepositoryConfig {
            inline_chunk_threshold_bytes: Some(5),
            ..Default::default()
        }),
        Arc::clone(&storage),
        Default::default(),
        Some(spec_version),
        true,
    )
    .await?;

    let mut session = repo.writable_session("main").await?;

    let user_data = Bytes::new();
    session.add_group(Path::root(), user_data.clone()).await?;

    let shape = ArrayShape::new(vec![(100, 100)]).unwrap();

    let array_path: Path = "/array".try_into().unwrap();
    session.add_array(array_path.clone(), shape, None, user_data.clone()).await?;

    // we write 50 native chunks, 6 bytes each
    for idx in 0..50 {
        let bytes = Bytes::copy_from_slice(&[0, 1, 2, 3, 4, 5]);
        let payload = session.get_chunk_writer()?(bytes.clone()).await?;
        session
            .set_chunk_ref(array_path.clone(), ChunkIndices(vec![idx]), Some(payload))
            .await?;
    }

    // we write 10 inline chunks, 1 byte each
    for idx in 50..60 {
        let bytes = Bytes::copy_from_slice(&[0]);
        let payload = session.get_chunk_writer()?(bytes.clone()).await?;
        session
            .set_chunk_ref(array_path.clone(), ChunkIndices(vec![idx]), Some(payload))
            .await?;
    }

    // we write 10 virtual chunks, 100 bytes each
    for idx in 60..70 {
        let payload = ChunkPayload::Virtual(VirtualChunkRef {
            location: VirtualChunkLocation::from_url("s3://foo/bar").unwrap(),
            offset: idx as u64,
            length: 100,
            checksum: None,
        });
        session
            .set_chunk_ref(array_path.clone(), ChunkIndices(vec![idx]), Some(payload))
            .await?;
    }

    let stats = repo_chunks_storage(
        Arc::clone(&asset_manager),
        NonZeroU16::new(5).unwrap(),
        NonZeroUsize::MIN,
        NonZeroU16::try_from(10).unwrap(),
    )
    .await
    .unwrap();
    // no commits
    assert_eq!(stats.native_bytes, 0);
    assert_eq!(stats.virtual_bytes, 0);
    assert_eq!(stats.inlined_bytes, 0);

    let _ = session.commit("first").max_concurrent_nodes(8).execute().await?;
    let stats = repo_chunks_storage(
        Arc::clone(&asset_manager),
        NonZeroU16::new(5).unwrap(),
        NonZeroUsize::MAX,
        NonZeroU16::try_from(10).unwrap(),
    )
    .await
    .unwrap();

    // 50 native chunks 6 bytes each, 10 inline chunks 1 byte each, 10 virtual chunks 100 bytes each
    assert_eq!(stats.native_bytes, 50 * 6);
    assert_eq!(stats.inlined_bytes, 10);
    assert_eq!(stats.virtual_bytes, 10 * 100);

    // we do a new commit
    let mut session = repo.writable_session("main").await?;
    // we write 10 more native chunks, 6 bytes each
    for idx in 0..10 {
        let bytes = Bytes::copy_from_slice(&[0, 1, 2, 3, 4, 5]);
        let payload = session.get_chunk_writer()?(bytes.clone()).await?;
        session
            .set_chunk_ref(array_path.clone(), ChunkIndices(vec![idx]), Some(payload))
            .await?;
    }

    let second_commit =
        session.commit("second").max_concurrent_nodes(8).execute().await?;
    let stats = repo_chunks_storage(
        Arc::clone(&asset_manager),
        NonZeroU16::new(5).unwrap(),
        NonZeroUsize::MIN,
        NonZeroU16::try_from(10).unwrap(),
    )
    .await
    .unwrap();
    // 50 native chunks from first commit, 10 from second
    assert_eq!(stats.native_bytes, (50 + 10) * 6);
    // Inline chunks are NOT deduplicated - they're stored in each manifest
    // First manifest has 10, second manifest inherits 10 from parent = 20 total
    assert_eq!(stats.inlined_bytes, 10 + 10);
    assert_eq!(stats.virtual_bytes, 10 * 100);

    // add more chunks in a different branch
    repo.create_branch("dev", &second_commit).await.unwrap();
    let mut session = repo.writable_session("dev").await?;
    // we write 5 native chunks 6 bytes each
    for idx in 0..5 {
        let bytes = Bytes::copy_from_slice(&[0, 1, 2, 3, 4, 5]);
        let payload = session.get_chunk_writer()?(bytes.clone()).await?;
        session
            .set_chunk_ref(array_path.clone(), ChunkIndices(vec![idx]), Some(payload))
            .await?;
    }
    // we write a few inline chunks
    for idx in 50..60 {
        let bytes = Bytes::copy_from_slice(&[0]);
        let payload = session.get_chunk_writer()?(bytes.clone()).await?;
        session
            .set_chunk_ref(array_path.clone(), ChunkIndices(vec![idx]), Some(payload))
            .await?;
    }
    let _ = session.commit("third").max_concurrent_nodes(8).execute().await?;
    let stats = repo_chunks_storage(
        Arc::clone(&asset_manager),
        NonZeroU16::new(5).unwrap(),
        NonZeroUsize::MAX,
        NonZeroU16::try_from(10).unwrap(),
    )
    .await
    .unwrap();
    // 50 native chunks from first commit, 10 from second, 5 from third
    assert_eq!(stats.native_bytes, 50 * 6 + 10 * 6 + 5 * 6);
    // Inline chunks are stored in each manifest, so NOT deduplicated:
    // Manifest 1: 10 bytes, Manifest 2: 10 bytes (inherited), Manifest 3: 10 bytes (rewritten) = 30 total
    assert_eq!(stats.inlined_bytes, 10 + 10 + 10);
    assert_eq!(stats.virtual_bytes, 10 * 100);
    Ok(())
}

#[tokio_test]
#[apply(spec_version_cases)]
pub async fn test_virtual_chunk_deduplication(
    #[case] spec_version: SpecVersionBin,
) -> Result<(), Box<dyn std::error::Error>> {
    let storage = new_in_memory_storage().await?;
    let storage_settings = storage.default_settings().await?;
    let asset_manager = Arc::new(AssetManager::new_no_cache(
        storage.clone(),
        storage_settings.clone(),
        spec_version,
        1,
        DEFAULT_MAX_CONCURRENT_REQUESTS,
    ));

    let repo = Repository::create(
        Some(RepositoryConfig {
            inline_chunk_threshold_bytes: Some(5),
            ..Default::default()
        }),
        storage,
        Default::default(),
        Some(spec_version),
        true,
    )
    .await?;

    let mut session = repo.writable_session("main").await?;
    let user_data = Bytes::new();
    session.add_group(Path::root(), user_data.clone()).await?;
    let shape = ArrayShape::new(vec![(100, 100)]).unwrap();
    let array_path: Path = "/array".try_into().unwrap();
    session.add_array(array_path.clone(), shape, None, user_data.clone()).await?;

    // Write virtual chunks with the same location but different offsets
    for idx in 0u32..10 {
        let payload = ChunkPayload::Virtual(VirtualChunkRef {
            location: VirtualChunkLocation::from_url("s3://bucket/file.dat").unwrap(),
            offset: (idx * 100) as u64,
            length: 100,
            checksum: None,
        });
        session
            .set_chunk_ref(array_path.clone(), ChunkIndices(vec![idx]), Some(payload))
            .await?;
    }

    // Write duplicate virtual chunks (same location AND offset AND length)
    for idx in 10u32..15 {
        let payload = ChunkPayload::Virtual(VirtualChunkRef {
            location: VirtualChunkLocation::from_url("s3://bucket/file.dat").unwrap(),
            offset: 0, // Same offset as idx=0
            length: 100,
            checksum: None,
        });
        session
            .set_chunk_ref(array_path.clone(), ChunkIndices(vec![idx]), Some(payload))
            .await?;
    }

    session.commit("first").max_concurrent_nodes(8).execute().await?;

    let stats = repo_chunks_storage(
        Arc::clone(&asset_manager),
        NonZeroU16::new(5).unwrap(),
        NonZeroUsize::MAX,
        NonZeroU16::try_from(10).unwrap(),
    )
    .await
    .unwrap();

    // Should have 10 unique virtual chunks (indices 0-9 with different offsets)
    // Indices 10-14 are duplicates of index 0, so they shouldn't add to the count
    assert_eq!(stats.virtual_bytes, 10 * 100);
    assert_eq!(stats.native_bytes, 0);
    assert_eq!(stats.inlined_bytes, 0);

    Ok(())
}
