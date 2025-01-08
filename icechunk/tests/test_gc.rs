#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::{collections::HashMap, num::NonZeroU64, sync::Arc};

use bytes::Bytes;
use chrono::Utc;
use futures::StreamExt;
use icechunk::{
    asset_manager::AssetManager,
    config::{S3Credentials, S3Options, S3StaticCredentials},
    format::{snapshot::ZarrArrayMetadata, ByteRange, ChunkId, ChunkIndices, Path},
    metadata::{ChunkKeyEncoding, ChunkShape, DataType, FillValue},
    ops::gc::{garbage_collect, GCConfig, GCSummary},
    refs::update_branch,
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
    let asset_manager =
        AssetManager::new_no_cache(storage.clone(), storage_settings.clone());

    let repo = Repository::create(
        Some(RepositoryConfig {
            inline_chunk_threshold_bytes: 0,
            unsafe_overwrite_refs: true,
            ..Default::default()
        }),
        Arc::clone(&storage),
        HashMap::new(),
    )
    .await?;

    let mut ds = repo.writable_session("main").await?;

    ds.add_group(Path::root()).await?;
    let zarr_meta = ZarrArrayMetadata {
        shape: vec![1100],
        data_type: DataType::Int8,
        chunk_shape: ChunkShape(vec![NonZeroU64::new(1).expect("Cannot create NonZero")]),
        chunk_key_encoding: ChunkKeyEncoding::Slash,
        fill_value: FillValue::Int8(0),
        codecs: vec![],
        storage_transformers: None,
        dimension_names: None,
    };

    let array_path: Path = "/array".try_into().unwrap();
    ds.add_array(array_path.clone(), zarr_meta.clone()).await?;
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
    let summary =
        garbage_collect(storage.as_ref(), &storage_settings, &asset_manager, &gc_config)
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
        false,
    )
    .await?;

    // we still have all the chunks
    assert_eq!(storage.list_chunks(&storage_settings).await?.count().await, 1110);

    let summary =
        garbage_collect(storage.as_ref(), &storage_settings, &asset_manager, &gc_config)
            .await?;
    assert_eq!(summary.chunks_deleted, 10);
    assert_eq!(summary.manifests_deleted, 1);
    assert_eq!(summary.snapshots_deleted, 1);

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
