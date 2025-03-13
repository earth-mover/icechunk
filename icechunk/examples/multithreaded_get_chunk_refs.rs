//! This example is used to benchmark multithreaded reads and writes of manifest files
//!
//! It launches hundreds of thousands of tasks to writes and then read refs.
//! It generates a manifest with 1M refs and executes 1M random reads.
//! Local filesystem storage is used to try to measure times without depending
//! on bandwidith.
//!
//! Run the example passing --write /path/to/repo
//! and then passing --read /path/to/repo

#![allow(clippy::unwrap_used)]

use std::{
    collections::HashMap,
    env::{self},
    sync::Arc,
    time::Instant,
};

use bytes::Bytes;
use futures::{StreamExt, stream::FuturesUnordered};
use icechunk::{
    Repository, RepositoryConfig,
    config::CompressionConfig,
    format::{
        ChunkId, ChunkIndices, Path,
        manifest::{ChunkPayload, ChunkRef},
        snapshot::ArrayShape,
    },
    new_local_filesystem_storage,
    repository::VersionInfo,
    session::Session,
};
use itertools::iproduct;
use rand::random_range;
use tokio::sync::RwLock;

const MAX_I: u64 = 10;
const MAX_J: u64 = 10;
const MAX_L: u64 = 100;
const MAX_K: u64 = 100;
const READS: u64 = 1_000_000;

async fn mk_repo(
    path: &std::path::Path,
) -> Result<Repository, Box<dyn std::error::Error>> {
    let storage = new_local_filesystem_storage(path).await?;
    let config = RepositoryConfig {
        compression: Some(CompressionConfig {
            level: Some(3),
            ..CompressionConfig::default()
        }),
        ..RepositoryConfig::default()
    };
    let repo = Repository::open_or_create(Some(config), storage, HashMap::new()).await?;
    Ok(repo)
}

async fn do_writes(path: &std::path::Path) -> Result<(), Box<dyn std::error::Error>> {
    let repo = mk_repo(path).await?;
    let mut session = repo.writable_session("main").await?;
    let shape =
        ArrayShape::new(vec![(MAX_I, 1), (MAX_J, 1), (MAX_K, 1), (MAX_L, 1)]).unwrap();
    let user_data = Bytes::new();
    let dimension_names = Some(vec!["i".into(), "j".into(), "k".into(), "l".into()]);
    let path: Path = "/array".try_into().unwrap();
    session.add_array(path.clone(), shape, dimension_names, user_data).await?;
    session.commit("array created", None).await?;

    let session = Arc::new(RwLock::new(repo.writable_session("main").await?));
    println!("Doing {} writes, wait...", MAX_I * MAX_J * MAX_K * MAX_L);
    let before = Instant::now();
    let futures: FuturesUnordered<_> = iproduct!(0..MAX_I, 0..MAX_J, 0..MAX_L, 0..MAX_K)
        .map(|(i, j, k, l)| {
            let path = path.clone();
            let session = session.clone();
            async move {
                let mut session = session.write().await;
                let payload = ChunkPayload::Ref(ChunkRef {
                    id: ChunkId::random(),
                    offset: i * j * k * l,
                    length: random_range(1_000_000..2_000_000),
                });
                session
                    .set_chunk_ref(
                        path.clone(),
                        ChunkIndices(vec![i as u32, j as u32, k as u32, l as u32]),
                        Some(payload),
                    )
                    .await
                    .unwrap();
            }
        })
        .collect();

    futures.collect::<()>().await;
    println!("Time to execute writes: {:?}", before.elapsed());
    let before = Instant::now();
    println!("Committing");
    session.write().await.commit("array created", None).await?;
    println!("Time to execute commit: {:?}", before.elapsed());
    Ok(())
}

async fn do_reads(path: &std::path::Path) -> Result<(), Box<dyn std::error::Error>> {
    let repo = mk_repo(path).await?;
    let session = Arc::new(RwLock::new(
        repo.readonly_session(&VersionInfo::BranchTipRef("main".to_string())).await?,
    ));

    let path: Path = "/array".try_into().unwrap();
    println!("Doing {} reads, wait...", 4 * (READS / 4));
    let before = Instant::now();
    let join1 = tokio::spawn(thread_reads(session.clone(), path.clone(), READS / 4));
    let join2 = tokio::spawn(thread_reads(session.clone(), path.clone(), READS / 4));
    let join3 = tokio::spawn(thread_reads(session.clone(), path.clone(), READS / 4));
    let join4 = tokio::spawn(thread_reads(session.clone(), path.clone(), READS / 4));

    let total = join1.await? + join2.await? + join3.await? + join4.await?;
    assert_eq!(total, 4 * (READS / 4));
    println!("Time to execute reads: {:?}", before.elapsed());

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = env::args().collect();
    if args.len() != 3 {
        println!(
            "Error: Pass either\n --write path/to/repo\n or\n --read path/to/repo\n as command line argument."
        );
        return Err("Invalid arguments".into());
    }

    let path = std::path::PathBuf::from(args[2].as_str());

    match &args[1] {
        s if s == "--write" => do_writes(path.as_path()).await?,
        s if s == "--read" => do_reads(path.as_path()).await?,
        _ => {
            println!("Error: Pass either --write or --read as command line argument.");
            let err: Box<dyn std::error::Error> = "Invalid arguments".into();
            return Err(err);
        }
    }

    Ok(())
}

async fn thread_reads(session: Arc<RwLock<Session>>, path: Path, reads: u64) -> u64 {
    let futures: FuturesUnordered<_> = (0..reads)
        .map(|_| {
            let i = random_range(0..MAX_I);
            let j = random_range(0..MAX_J);
            let k = random_range(0..MAX_K);
            let l = random_range(0..MAX_L);
            let path = path.clone();
            let session = session.clone();
            async move {
                let session = session.read().await;
                let the_ref = session
                    .get_chunk_ref(
                        &path,
                        &ChunkIndices(vec![i as u32, j as u32, k as u32, l as u32]),
                    )
                    .await
                    .unwrap();
                assert!(matches!(the_ref, Some(ChunkPayload::Ref(ChunkRef{ offset, .. })) if offset == i*j*k*l));
                1
            }
        })
        .collect();

    futures.collect::<Vec<_>>().await.iter().sum()
}
