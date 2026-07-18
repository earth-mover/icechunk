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
async fn test_repo_chunks_storage_in_memory(
    #[case] spec_version: SpecVersionBin,
) -> Result<(), Box<dyn std::error::Error>> {
    let storage = new_in_memory_storage().await?;
    do_test_repo_chunks_storage(storage, spec_version).await
}

#[tokio_test]
#[apply(spec_version_cases)]
async fn test_repo_chunks_storage_in_minio(
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
async fn test_repo_chunks_storage_in_aws(
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
async fn test_repo_chunks_storage_in_tigris(
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
async fn test_repo_chunks_storage_in_r2(
    #[case] spec_version: SpecVersionBin,
) -> Result<(), Box<dyn std::error::Error>> {
    let prefix =
        format!("test_stats_{:?}_{}", spec_version, Utc::now().timestamp_millis());
    let storage = common::make_r2_integration_storage(prefix)?;
    do_test_repo_chunks_storage(storage, spec_version).await
}

#[tokio_test]
async fn test_total_nonvirtual_size_in_minio() -> Result<(), Box<dyn std::error::Error>> {
    use futures::{StreamExt as _, TryStreamExt as _};
    use icechunk::format::{
        ChunkId, ManifestId, NodeId,
        manifest::{ChunkInfo, Manifest},
    };

    let prefix = format!("test_nvsize_{}", Utc::now().timestamp_millis());
    let storage = common::make_minio_integration_storage(prefix, &Permission::Modify)?;
    let settings = storage.default_settings().await?;
    let manager = AssetManager::new_no_cache(
        Arc::clone(&storage),
        settings.clone(),
        SpecVersionBin::default(),
        1,
        100,
    );

    // More than one page (1000 keys) of chunks, so the single-page probe sees a
    // truncated listing and the two-character shard fan-out actually runs; the
    // random ids span many shards.
    futures::stream::iter(0..1100u32)
        .map(|i| {
            let manager = &manager;
            async move {
                manager
                    .write_chunk(
                        ChunkId::random(),
                        Bytes::from(vec![
                            u8::try_from(i % 251).unwrap_or(0);
                            (i % 7 + 1) as usize
                        ]),
                    )
                    .await
            }
        })
        .buffer_unordered(64)
        .try_collect::<Vec<_>>()
        .await?;
    let ci = ChunkInfo {
        node: NodeId::random(),
        coord: ChunkIndices(vec![0]),
        payload: ChunkPayload::Inline(Bytes::copy_from_slice(b"inline")),
    };
    let manifest = Arc::new(
        Manifest::from_iter(&ManifestId::random(), vec![ci].into_iter(), None)
            .await?
            .unwrap(),
    );
    manager.write_manifest(manifest).await?;

    // The prefix-sharded S3 fast path must equal a naive single-stream listing of
    // the whole repo root: no gaps between shards, no double counting. This holds
    // only against real raw-prefix S3 semantics, which rustfs/MinIO provide.
    let mut naive = 0u64;
    let mut all = storage.list_objects(&settings, "").await?;
    while let Some(info) = all.next().await {
        naive = naive.saturating_add(info?.size_bytes);
    }
    assert!(naive > 0);
    assert_eq!(manager._total_nonvirtual_size().await?, naive);
    Ok(())
}

#[tokio_test]
async fn test_total_nonvirtual_size_in_azurite() -> Result<(), Box<dyn std::error::Error>>
{
    use std::collections::HashMap;

    use futures::{StreamExt as _, TryStreamExt as _};
    use icechunk::{
        ObjectStorage,
        config::{AzureCredentials, AzureStaticCredentials},
        format::{
            ChunkId, ManifestId, NodeId,
            manifest::{ChunkInfo, Manifest},
        },
        storage::Settings,
    };
    use icechunk_arrow_object_store::object_store::azure::AzureConfigKey;

    // Azurite's well-known development-storage account key.
    const EMULATOR_KEY: &str = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";
    // The long-lived account SAS `azurite-init` in compose.yaml uses to create
    // `testcontainer` (valid through 2035, permissions rwdftlacup).
    const COMPOSE_SAS: &str = "sv=2023-01-03&ss=btqf&srt=sco&spr=https%2Chttp&st=2025-01-06T14%3A53%3A30Z&se=2035-01-07T14%3A53%3A00Z&sp=rwdftlacup&sig=jclETGilOzONYp4Y0iK9SpVRLGyehaS5lg5booJ9VYA%3D";

    async fn azurite_storage(
        prefix: &str,
        credentials: AzureStaticCredentials,
    ) -> Result<Arc<dyn Storage + Send + Sync>, Box<dyn std::error::Error>> {
        Ok(Arc::new(
            ObjectStorage::new_azure(
                "devstoreaccount1".to_string(),
                "testcontainer".to_string(),
                Some(prefix.to_string()),
                Some(AzureCredentials::Static(credentials)),
                Some(HashMap::from([(AzureConfigKey::UseEmulator, "true".to_string())])),
            )
            .await?,
        ))
    }

    async fn naive_size(
        storage: &Arc<dyn Storage + Send + Sync>,
        settings: &Settings,
        prefix: &str,
    ) -> Result<u64, Box<dyn std::error::Error>> {
        let mut objects = storage.list_objects(settings, prefix).await?;
        let mut sum = 0u64;
        while let Some(info) = objects.next().await {
            sum = sum.saturating_add(info?.size_bytes);
        }
        Ok(sum)
    }

    let prefix = format!("test_nvsize_az_{}", Utc::now().timestamp_millis());
    let seeder = azurite_storage(
        &prefix,
        AzureStaticCredentials::AccessKey(EMULATOR_KEY.to_string()),
    )
    .await?;
    let settings = seeder.default_settings().await?;
    let manager = AssetManager::new_no_cache(
        Arc::clone(&seeder),
        settings.clone(),
        SpecVersionBin::default(),
        1,
        100,
    );

    // More than one page (5000 blobs) of chunks, so the single-page probe sees
    // a truncated listing and the two-character shard fan-out actually runs;
    // the random ids span many shards.
    futures::stream::iter(0..5100u32)
        .map(|i| {
            let manager = &manager;
            async move {
                manager
                    .write_chunk(
                        ChunkId::random(),
                        Bytes::from(vec![
                            u8::try_from(i % 251).unwrap_or(0);
                            (i % 7 + 1) as usize
                        ]),
                    )
                    .await
            }
        })
        .buffer_unordered(64)
        .try_collect::<Vec<_>>()
        .await?;
    let ci = ChunkInfo {
        node: NodeId::random(),
        coord: ChunkIndices(vec![0]),
        payload: ChunkPayload::Inline(Bytes::copy_from_slice(b"inline")),
    };
    let manifest = Arc::new(
        Manifest::from_iter(&ManifestId::random(), vec![ci].into_iter(), None)
            .await?
            .unwrap(),
    );
    manager.write_manifest(manifest).await?;

    let naive_all = naive_size(&seeder, &settings, "").await?;
    let naive_chunks = naive_size(&seeder, &settings, "chunks").await?;
    let naive_manifests = naive_size(&seeder, &settings, "manifests").await?;
    assert!(naive_chunks > 0);
    assert!(naive_manifests > 0);
    assert_eq!(naive_all, naive_chunks + naive_manifests);

    // The fast path must be byte-exact against a naive full listing under both
    // fast-path credential shapes: SharedKey (account key) and SAS — the
    // latter is what arraylake vends.
    let sas = azurite_storage(
        &prefix,
        AzureStaticCredentials::SASToken(COMPOSE_SAS.to_string()),
    )
    .await?;
    for (label, storage) in [("access_key", seeder), ("sas", sas)] {
        // Truncated probe + 1024-shard fan-out.
        assert_eq!(
            storage.sum_object_sizes(&settings, "chunks", true).await?,
            naive_chunks,
            "sharded chunks sum ({label})"
        );
        // Sequential drain of the same >5000-blob prefix: marker pagination.
        assert_eq!(
            storage.sum_object_sizes(&settings, "chunks", false).await?,
            naive_chunks,
            "sequential chunks sum ({label})"
        );
        // Single-page probe short-circuit.
        assert_eq!(
            storage.sum_object_sizes(&settings, "manifests", true).await?,
            naive_manifests,
            "manifests sum ({label})"
        );
        let manager = AssetManager::new_no_cache(
            Arc::clone(&storage),
            settings.clone(),
            SpecVersionBin::default(),
            1,
            100,
        );
        assert_eq!(
            manager._total_nonvirtual_size().await?,
            naive_all,
            "total nonvirtual size ({label})"
        );
    }
    Ok(())
}

async fn do_test_repo_chunks_storage(
    storage: Arc<dyn Storage + Send + Sync>,
    spec_version: SpecVersionBin,
) -> Result<(), Box<dyn std::error::Error>> {
    let storage_settings = storage.default_settings().await?;
    let asset_manager = Arc::new(AssetManager::new_no_cache(
        Arc::clone(&storage),
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
async fn test_virtual_chunk_deduplication(
    #[case] spec_version: SpecVersionBin,
) -> Result<(), Box<dyn std::error::Error>> {
    let storage = new_in_memory_storage().await?;
    let storage_settings = storage.default_settings().await?;
    let asset_manager = Arc::new(AssetManager::new_no_cache(
        Arc::clone(&storage),
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
