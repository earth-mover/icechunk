#![allow(clippy::expect_used, clippy::unwrap_used, dead_code)]
use bytes::Bytes;
use icechunk::{
    Repository, RepositoryConfig, Store,
    config::{ManifestConfig, ManifestSplittingConfig},
    format::{
        ChunkIndices,
        manifest::{ChunkPayload, VirtualChunkLocation, VirtualChunkRef},
    },
    session::Session,
    storage::new_in_memory_storage,
};
use std::{collections::HashMap, sync::Arc};
use tokio::{
    sync::{RwLock, Semaphore},
    task::JoinSet,
};

const TOTAL_NUM_REFS: usize = 100_000_000;
const MANIFEST_SPLIT_SIZE: u32 = 1_000_000;
const CHUNK_SIZE: u32 = 1;
const TASK_CHUNK_SIZE: usize = 1_000; // each task writes this many refs in serial
const NUM_TASKS: usize = TOTAL_NUM_REFS / TASK_CHUNK_SIZE;
const MAX_CONCURRENT_TASKS: usize = 100;

// #[tokio::test]
async fn test_write_large_number_of_refs() -> Result<(), Box<dyn std::error::Error>> {
    let storage = new_in_memory_storage().await?;
    let config = RepositoryConfig {
        inline_chunk_threshold_bytes: Some(128),
        manifest: Some(ManifestConfig {
            splitting: Some(ManifestSplittingConfig::with_size(MANIFEST_SPLIT_SIZE)),
            ..Default::default()
        }),
        ..Default::default()
    };
    let repo = Repository::create(Some(config), storage, HashMap::new()).await?;
    let session = Arc::new(RwLock::new(repo.writable_session("main").await?));
    let store = Store::from_session(Arc::clone(&session)).await;

    store
        .set(
            "zarr.json",
            Bytes::copy_from_slice(br#"{"zarr_format":3, "node_type":"group"}"#),
        )
        .await?;

    let array_json = format!(
        r#"{{
    "zarr_format": 3,
    "node_type": "array",
    "attributes": {{"foo": 42}},
    "shape": [{TOTAL_NUM_REFS}],
    "data_type": "float32",
    "chunk_grid": {{"name": "regular", "configuration": {{"chunk_shape": [{CHUNK_SIZE}]}}}},
    "chunk_key_encoding": {{"name": "default", "configuration": {{"separator": "/"}}}},
    "fill_value": 0.0,
    "codecs": [{{"name": "mycodec", "configuration": {{"foo": 42}}}}],
    "storage_transformers": [{{"name": "mytransformer", "configuration": {{"bar": 43}}}}],
    "dimension_names": ["x"]
}}"#
    );

    let zarr_meta = Bytes::copy_from_slice(array_json.as_ref());
    store.set("array/zarr.json", zarr_meta).await?;

    let mut set = JoinSet::new();

    let semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_TASKS));

    ("num_tasks", &NUM_TASKS);
    for i in 0..NUM_TASKS {
        let semaphore = semaphore.clone();
        let cloned_session = Arc::clone(&session);
        set.spawn(async move {
            let _permit = semaphore.acquire().await.unwrap();
            write_chunk_refs_batch(cloned_session, i).await;
        });
    }

    set.join_all().await;

    session.write().await.commit("first", None).await?;

    Ok(())
}

async fn write_chunk_refs_batch(session: Arc<RwLock<Session>>, batch: usize) {
    // let mut guard = session.write().await;
    ("starting batch", &batch);
    for i in batch * TASK_CHUNK_SIZE..(batch + 1) * TASK_CHUNK_SIZE {
        let payload = ChunkPayload::Virtual(VirtualChunkRef {
            location: VirtualChunkLocation(format!("s3://foo/bar/{i}")),
            offset: 0,
            length: 1,
            checksum: None,
        });
        session
            .write()
            .await
            .set_chunk_ref(
                "/array".try_into().unwrap(),
                ChunkIndices(vec![i as u32]),
                Some(payload),
            )
            .await
            .expect("Failed to write chunk ref");
    }
    ("finished batch", &batch);
}
