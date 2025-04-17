#![allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]

use std::sync::Arc;

use bytes::Bytes;
use chrono::Utc;
use icechunk::{
    Repository, RepositoryConfig, Storage,
    asset_manager::AssetManager,
    format::{
        ChunkIndices, Path,
        manifest::{ChunkPayload, VirtualChunkLocation, VirtualChunkRef},
        snapshot::ArrayShape,
    },
    new_in_memory_storage,
    ops::stats::repo_chunks_storage,
};
use icechunk_macros::tokio_test;

mod common;

#[tokio_test]
pub async fn test_repo_chunks_storage_in_memory() -> Result<(), Box<dyn std::error::Error>>
{
    let storage = new_in_memory_storage().await?;
    do_test_repo_chunks_storage(storage).await
}

#[tokio_test]
pub async fn test_repo_chunks_storage_in_minio() -> Result<(), Box<dyn std::error::Error>>
{
    let prefix = format!("test_distributed_writes_{}", Utc::now().timestamp_millis());
    let storage = common::make_minio_integration_storage(prefix)?;
    do_test_repo_chunks_storage(storage).await
}

#[tokio_test]
#[ignore = "needs credentials from env"]
pub async fn test_repo_chunks_storage_in_aws() -> Result<(), Box<dyn std::error::Error>> {
    let prefix = format!("test_distributed_writes_{}", Utc::now().timestamp_millis());
    let storage = common::make_aws_integration_storage(prefix)?;
    do_test_repo_chunks_storage(storage).await
}

#[tokio_test]
#[ignore = "needs credentials from env"]
pub async fn test_repo_chunks_storage_in_tigris() -> Result<(), Box<dyn std::error::Error>>
{
    let prefix = format!("test_distributed_writes_{}", Utc::now().timestamp_millis());
    let storage = common::make_tigris_integration_storage(prefix)?;
    do_test_repo_chunks_storage(storage).await
}

#[tokio_test]
#[ignore = "needs credentials from env"]
pub async fn test_repo_chunks_storage_in_r2() -> Result<(), Box<dyn std::error::Error>> {
    let prefix = format!("test_distributed_writes_{}", Utc::now().timestamp_millis());
    let storage = common::make_r2_integration_storage(prefix)?;
    do_test_repo_chunks_storage(storage).await
}

pub async fn do_test_repo_chunks_storage(
    storage: Arc<dyn Storage + Send + Sync>,
) -> Result<(), Box<dyn std::error::Error>> {
    let storage_settings = storage.default_settings();
    let asset_manager = Arc::new(AssetManager::new_no_cache(
        storage.clone(),
        storage_settings.clone(),
        1,
    ));

    let repo = Repository::create(
        Some(RepositoryConfig {
            inline_chunk_threshold_bytes: Some(5),
            ..Default::default()
        }),
        Arc::clone(&storage),
        Default::default(),
    )
    .await?;

    let mut session = repo.writable_session("main").await?;

    let user_data = Bytes::new();
    session.add_group(Path::root(), user_data.clone()).await?;

    let shape = ArrayShape::new(vec![(100, 1)]).unwrap();

    let array_path: Path = "/array".try_into().unwrap();
    session.add_array(array_path.clone(), shape, None, user_data.clone()).await?;

    // we write 50 native chunks 6 bytes each
    for idx in 0..50 {
        let bytes = Bytes::copy_from_slice(&[0, 1, 2, 3, 4, 5]);
        let payload = session.get_chunk_writer()(bytes.clone()).await?;
        session
            .set_chunk_ref(array_path.clone(), ChunkIndices(vec![idx]), Some(payload))
            .await?;
    }

    // we write a few inline chunks
    for idx in 50..60 {
        let bytes = Bytes::copy_from_slice(&[0]);
        let payload = session.get_chunk_writer()(bytes.clone()).await?;
        session
            .set_chunk_ref(array_path.clone(), ChunkIndices(vec![idx]), Some(payload))
            .await?;
    }

    // we write a few virtual chunks
    for idx in 60..70 {
        let payload = ChunkPayload::Virtual(VirtualChunkRef {
            location: VirtualChunkLocation::from_absolute_path("s3://foo/bar").unwrap(),
            offset: idx as u64,
            length: 100,
            checksum: None,
        });
        session
            .set_chunk_ref(array_path.clone(), ChunkIndices(vec![idx]), Some(payload))
            .await?;
    }

    let size = repo_chunks_storage(
        storage.as_ref(),
        &storage_settings,
        Arc::clone(&asset_manager),
    )
    .await
    .unwrap();
    // no commits
    assert_eq!(size, 0);

    let _ = session.commit("first", None).await?;
    let size = repo_chunks_storage(
        storage.as_ref(),
        &storage_settings,
        Arc::clone(&asset_manager),
    )
    .await
    .unwrap();

    // 50 native chunks 60 bytes each
    assert_eq!(size, 50 * 6);

    // we do a new commit
    let mut session = repo.writable_session("main").await?;
    // we write 10 native chunks 6 bytes each
    for idx in 0..10 {
        let bytes = Bytes::copy_from_slice(&[0, 1, 2, 3, 4, 5]);
        let payload = session.get_chunk_writer()(bytes.clone()).await?;
        session
            .set_chunk_ref(array_path.clone(), ChunkIndices(vec![idx]), Some(payload))
            .await?;
    }

    let second_commit = session.commit("second", None).await?;
    let size = repo_chunks_storage(
        storage.as_ref(),
        &storage_settings,
        Arc::clone(&asset_manager),
    )
    .await
    .unwrap();
    // 50 native chunks from first commit, 10 from second
    assert_eq!(size, 50 * 6 + 10 * 6);

    // add more chunks in a different branch
    repo.create_branch("dev", &second_commit).await.unwrap();
    let mut session = repo.writable_session("dev").await?;
    // we write 5 native chunks 6 bytes each
    for idx in 0..5 {
        let bytes = Bytes::copy_from_slice(&[0, 1, 2, 3, 4, 5]);
        let payload = session.get_chunk_writer()(bytes.clone()).await?;
        session
            .set_chunk_ref(array_path.clone(), ChunkIndices(vec![idx]), Some(payload))
            .await?;
    }
    // we write a few inline chunks
    for idx in 50..60 {
        let bytes = Bytes::copy_from_slice(&[0]);
        let payload = session.get_chunk_writer()(bytes.clone()).await?;
        session
            .set_chunk_ref(array_path.clone(), ChunkIndices(vec![idx]), Some(payload))
            .await?;
    }
    let _ = session.commit("third", None).await?;
    let size = repo_chunks_storage(
        storage.as_ref(),
        &storage_settings,
        Arc::clone(&asset_manager),
    )
    .await
    .unwrap();
    // 50 native chunks from first commit, 10 from second, 5 from third
    assert_eq!(size, 50 * 6 + 10 * 6 + 5 * 6);
    Ok(())
}
