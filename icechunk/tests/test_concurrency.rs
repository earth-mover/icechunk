#![allow(clippy::expect_used, clippy::unwrap_used)]
use bytes::Bytes;
use chrono::Utc;
use icechunk::{
    Repository, RepositoryConfig, Storage,
    config::{ManifestConfig, ManifestSplittingConfig},
    format::{ByteRange, ChunkIndices, Path, snapshot::ArrayShape},
    session::{Session, get_chunk},
    storage::new_in_memory_storage,
};
use icechunk_macros::tokio_test;
use pretty_assertions::assert_eq;
use rand::{Rng, rng};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};
use tokio::{
    sync::{Barrier, RwLock},
    task::{self, JoinSet},
    time::sleep,
};

use futures::TryStreamExt;

mod common;

const N: usize = 20;

#[tokio_test]
async fn test_concurrency_in_memory() -> Result<(), Box<dyn std::error::Error>> {
    let storage: Arc<dyn Storage + Send + Sync> = new_in_memory_storage().await?;
    do_test_concurrency(storage).await
}

#[tokio_test]
#[ignore = "needs credentials from env"]
async fn test_concurrency_in_r2() -> Result<(), Box<dyn std::error::Error>> {
    let prefix = format!("test_concurrency_{}", Utc::now().timestamp_millis());
    let storage: Arc<dyn Storage + Send + Sync> =
        common::make_r2_integration_storage(prefix)?;
    do_test_concurrency(storage).await
}

#[tokio_test]
#[ignore = "needs credentials from env"]
async fn test_concurrency_in_aws() -> Result<(), Box<dyn std::error::Error>> {
    let prefix = format!("test_concurrency_{}", Utc::now().timestamp_millis());
    let storage: Arc<dyn Storage + Send + Sync> =
        common::make_aws_integration_storage(prefix)?;
    do_test_concurrency(storage).await
}

#[tokio_test]
#[ignore = "needs credentials from env"]
async fn test_concurrency_in_tigris() -> Result<(), Box<dyn std::error::Error>> {
    let prefix = format!("test_concurrency_{}", Utc::now().timestamp_millis());
    let storage: Arc<dyn Storage + Send + Sync> =
        common::make_tigris_integration_storage(prefix)?;
    do_test_concurrency(storage).await
}

/// This test starts concurrent tasks to read, write and list a repository.
///
/// It writes an `NxN` array,  with individual tasks for each 1x1 chunk. Concurrently with that it
/// starts NxN tasks to read each chunk, these tasks only finish when the chunk was successfully
/// read. While that happens, another Task lists the chunk contents and only finishes when it finds
/// all chunks written.
async fn do_test_concurrency(
    storage: Arc<dyn Storage + Send + Sync>,
) -> Result<(), Box<dyn std::error::Error>> {
    let shape = ArrayShape::new(vec![(N as u64, 1), (N as u64, 1)]).unwrap();

    let config = RepositoryConfig {
        manifest: Some(ManifestConfig {
            splitting: Some(ManifestSplittingConfig::with_size(2)),
            ..Default::default()
        }),
        ..Default::default()
    };
    let repo = Repository::create(Some(config), storage, HashMap::new()).await?;

    let mut ds = repo.writable_session("main").await?;

    let dimension_names = Some(vec!["x".into(), "y".into()]);
    let user_data = Bytes::new();
    let new_array_path: Path = "/array".try_into().unwrap();
    ds.add_array(new_array_path.clone(), shape, dimension_names, user_data).await?;

    let ds = Arc::new(RwLock::new(ds));

    let barrier = Arc::new(Barrier::new(N * N + 1));
    let mut set = JoinSet::new();

    let ds_c = Arc::clone(&ds);
    let barrier_c = Arc::clone(&barrier);
    set.spawn(async move { list_task(ds_c, barrier_c).await });

    for x in 0..N {
        for y in 0..N {
            let ds = Arc::clone(&ds);
            let barrier = Arc::clone(&barrier);
            set.spawn(async move { read_task(ds, x as u32, y as u32, barrier).await });
        }
    }

    for x in 0..N {
        for y in 0..N {
            let ds = Arc::clone(&ds);
            set.spawn(async move { write_task(ds, x as u32, y as u32).await });
        }
    }

    set.join_all().await;

    Ok(())
}

async fn write_task(ds: Arc<RwLock<Session>>, x: u32, y: u32) {
    let value = x as f64 * y as f64;
    let bytes = Bytes::copy_from_slice(&value.to_be_bytes());

    let random_sleep = {
        let mut rng = rng();
        rng.random_range(0..200)
    };
    sleep(Duration::from_millis(random_sleep)).await;

    let payload = {
        let guard = ds.read().await;
        let writer = guard.get_chunk_writer();
        writer(bytes).await.expect("Failed to write chunk")
    };

    ds.write()
        .await
        .set_chunk_ref(
            "/array".try_into().unwrap(),
            ChunkIndices(vec![x, y]),
            Some(payload),
        )
        .await
        .expect("Failed to write chunk ref");
}

async fn read_task(ds: Arc<RwLock<Session>>, x: u32, y: u32, barrier: Arc<Barrier>) {
    let value = x as f64 * y as f64;
    let expected_bytes = Bytes::copy_from_slice(&value.to_be_bytes());
    barrier.wait().await;
    loop {
        let readable_ds = ds.read().await;
        let chunk_reader = readable_ds
            .get_chunk_reader(
                &"/array".try_into().unwrap(),
                &ChunkIndices(vec![x, y]),
                &ByteRange::ALL,
            )
            .await
            .expect("Failed to get chunk reader");

        let actual_bytes =
            get_chunk(chunk_reader).await.expect("Failed to getch chunk payload");

        if let Some(bytes) = &actual_bytes {
            if bytes == &expected_bytes {
                break;
            }
        }
        let random_sleep = {
            let mut rng = rng();
            rng.random_range(0..20)
        };
        sleep(Duration::from_millis(random_sleep)).await;
    }
}

async fn list_task(ds: Arc<RwLock<Session>>, barrier: Arc<Barrier>) {
    let mut expected_indices = HashSet::new();
    for x in 0..(N as u32) {
        for y in 0..(N as u32) {
            expected_indices.insert(ChunkIndices(vec![x, y]));
        }
    }
    let expected_nodes: HashSet<String> = HashSet::from_iter(vec!["/array".to_string()]);

    barrier.wait().await;
    loop {
        let nodes = ds
            .read()
            .await
            .list_nodes()
            .await
            .expect("list_nodes failed")
            .map(|n| n.unwrap().path.to_string())
            .collect::<HashSet<_>>();
        assert_eq!(expected_nodes, nodes);

        let available_indices = ds
            .read()
            .await
            .all_chunks()
            .await
            .expect("all_chunks failed")
            .map_ok(|(_, chunk)| chunk.coord)
            .try_collect::<HashSet<_>>()
            .await
            .expect("try_collect failed");

        if available_indices == expected_indices {
            break;
        }

        task::yield_now().await;
    }
}
