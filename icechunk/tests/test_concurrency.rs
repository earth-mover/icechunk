#![allow(clippy::expect_used, clippy::unwrap_used)]
use bytes::Bytes;
use icechunk::{
    format::{snapshot::ZarrArrayMetadata, ByteRange, ChunkIndices, Path},
    metadata::{
        ChunkKeyEncoding, ChunkShape, Codec, DataType, FillValue, StorageTransformer,
    },
    session::{get_chunk, Session},
    storage::new_in_memory_storage,
    Repository, Storage,
};
use pretty_assertions::assert_eq;
use rand::{thread_rng, Rng};
use std::{
    collections::{HashMap, HashSet},
    num::NonZeroU64,
    sync::Arc,
    time::Duration,
};
use tokio::{
    sync::{Barrier, RwLock},
    task::{self, JoinSet},
    time::sleep,
};

use futures::TryStreamExt;

const N: usize = 20;

#[tokio::test]
/// This test starts concurrent tasks to read, write and list a repository.
///
/// It writes an `NxN` array,  with individual tasks for each 1x1 chunk. Concurrently with that it
/// starts NxN tasks to read each chunk, these tasks only finish when the chunk was successfully
/// read. While that happens, another Task lists the chunk contents and only finishes when it finds
/// all chunks written.
async fn test_concurrency() -> Result<(), Box<dyn std::error::Error>> {
    let storage: Arc<dyn Storage + Send + Sync> = new_in_memory_storage()?;
    let repo = Repository::create(None, storage, HashMap::new()).await?;
    let mut ds = repo.writable_session("main").await?;

    ds.add_group(Path::root()).await?;
    let zarr_meta = ZarrArrayMetadata {
        shape: vec![N as u64, N as u64],
        data_type: DataType::Float64,
        chunk_shape: ChunkShape(vec![
            NonZeroU64::new(1).expect("Cannot create NonZero"),
            NonZeroU64::new(1).expect("Cannot create NonZero"),
        ]),
        chunk_key_encoding: ChunkKeyEncoding::Slash,
        fill_value: FillValue::Float64(f64::NAN),
        codecs: vec![Codec { name: "mycodec".to_string(), configuration: None }],
        storage_transformers: Some(vec![StorageTransformer {
            name: "mytransformer".to_string(),
            configuration: None,
        }]),
        dimension_names: Some(vec![Some("x".to_string()), Some("y".to_string())]),
    };

    let new_array_path: Path = "/array".try_into().unwrap();
    ds.add_array(new_array_path.clone(), zarr_meta.clone()).await?;

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
        let mut rng = thread_rng();
        rng.gen_range(0..200)
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
            let mut rng = thread_rng();
            rng.gen_range(0..20)
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
    let expected_nodes: HashSet<String> =
        HashSet::from_iter(vec!["/".to_string(), "/array".to_string()]);

    barrier.wait().await;
    loop {
        let nodes = ds
            .read()
            .await
            .list_nodes()
            .await
            .expect("list_nodes failed")
            .map(|n| n.path.to_string())
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
