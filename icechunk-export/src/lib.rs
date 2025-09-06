use std::{
    collections::HashSet,
    fs::File,
    hash::Hash,
    io::{BufRead as _, BufReader},
    path::Path,
    sync::{Arc, atomic::AtomicU64},
    time::{Duration, Instant},
};

use bytes::Bytes;
use chrono::Utc;
use err_into::ErrorInto;
use futures::{
    SinkExt,
    stream::{self, FuturesUnordered},
};
use icechunk::{
    Repository, Storage, StorageError,
    asset_manager::AssetManager,
    config::S3Options,
    format::{
        ChunkId, ManifestId, SnapshotId, manifest::ChunkPayload, repo_info::RepoInfo,
    },
    new_s3_storage,
    ops::gc::{GCConfig, find_retained},
    repository::{RepositoryError, RepositoryResult},
    storage::Settings,
};
use indicatif::ProgressBar;
use itertools::Itertools as _;
use tokio::{
    io::AsyncReadExt as _,
    sync::{
        OwnedSemaphorePermit, Semaphore,
        mpsc::{self, Receiver, Sender},
    },
};

// fn parse_file_lines<P: AsRef<Path>>(
//     path: P,
// ) -> impl Iterator<Item = Result<(u64, String), Box<dyn std::error::Error>>> {
//     let file = File::open(path).expect("Failed to open file");
//     let reader = BufReader::new(file);
//
//     reader.lines().map(|line_result| {
//         let line = line_result?;
//         let parts: Vec<&str> = line.splitn(2, ' ').collect();
//
//         if parts.len() != 2 {
//             return Err(format!("Invalid line format: {}", line).into());
//         }
//
//         let number = parts[0].parse::<u64>()?;
//         let text = parts[1].to_string();
//
//         Ok((number, text))
//     })
// }

// pub async fn test_saturation(
//     max_concurrent: usize,
//     copy_chunks: usize,
//     chunks_file_path: &Path,
// ) -> Result<(), Box<dyn std::error::Error>> {
//     let (tx, rx) = mpsc::channel(max_concurrent);
//     let chunks = parse_file_lines(chunks_file_path).take(copy_chunks);
//     let handle = tokio::spawn(copier(rx, max_concurrent));
//     for task in chunks {
//         match task {
//             Ok(task) => tx.send(task).await?,
//             Err(_) => panic!("Error reading file"),
//         }
//     }
//     drop(tx);
//     //tx.send((0, "".to_string())).await?;
//
//     handle.await??;
//
//     Ok(())
// }

// async fn copier(
//     mut rx: Receiver<(u64, String)>,
//     max_concurrent: usize,
// ) -> Result<(), RepositoryError> {
//     let config = S3Options {
//         region: Some("us-east-1".to_string()),
//         endpoint_url: None,
//         anonymous: false,
//         allow_http: false,
//         force_path_style: false,
//         network_stream_timeout_seconds: None,
//     };
//     let source_bucket = "icechunk-public-data".to_string();
//     let source = new_s3_storage(
//         config.clone(),
//         source_bucket,
//         Some("v1/era5_weatherbench2".to_string()),
//         None,
//     )?;
//
//     let destination_bucket = "icechunk-test".to_string();
//     let destination = new_s3_storage(
//         config.clone(),
//         destination_bucket,
//         Some("test-net-saturation".to_string()),
//         None,
//     )?;
//
//     let semaphore = Arc::new(Semaphore::new(max_concurrent));
//     let settings = Arc::new(source.default_settings());
//
//     let (done_sender, mut done_receiver) = mpsc::channel(10_000);
//
//     let done_task = tokio::spawn(async move {
//         let mut bytes_copied = 0u64;
//         let mut chunks_copied = 0u64;
//         let start_time = Instant::now();
//
//         while let Some(size) = done_receiver.recv().await {
//             bytes_copied += size;
//             chunks_copied += 1;
//             if start_time.elapsed().as_secs() > 0 {
//                 let speed = bytes_copied as f64
//                     / 1_000_000.0
//                     / start_time.elapsed().as_secs() as f64;
//                 println!(
//                     "Copied {chunks_copied} chunks. Average speed: {speed:.2} MB/sec"
//                 );
//             }
//         }
//         bytes_copied
//     });
//
//     while let Some((size, id)) = rx.recv().await {
//         let guard = semaphore.clone().acquire_owned().await?;
//         tokio::spawn(copy_chunk(
//             size,
//             id,
//             source.clone(),
//             destination.clone(),
//             settings.clone(),
//             done_sender.clone(),
//             guard,
//         ));
//     }
//
//     drop(done_sender);
//     done_task.await?;
//
//     Ok(())
// }

// async fn copy_chunk(
//     size: u64,
//     id: String,
//     source: Arc<dyn Storage>,
//     destination: Arc<dyn Storage>,
//     settings: Arc<Settings>,
//     done: Sender<u64>,
//     _semaphore: OwnedSemaphorePermit,
// ) -> Result<(), StorageError> {
//     let key = format!("chunks/{id}");
//     //println!("Copying {key}");
//     let (mut reader, _) =
//         source.get_object(settings.as_ref(), key.as_str(), None).await?;
//
//     let mut buffer = Vec::with_capacity(1024 * 1024);
//     reader.read_to_end(&mut buffer).await?;
//     let bytes = Bytes::from_owner(buffer);
//
//     destination
//         .put_object(
//             settings.as_ref(),
//             key.as_str(),
//             bytes,
//             None,
//             Default::default(),
//             None,
//         )
//         .await?;
//     //println!("Done {key}");
//     done.send(size).await.unwrap();
//     Ok(())
// }

pub enum VersionSelection {
    SingleSnapshot(SnapshotId),
    AllHistory,
    // TODO: can we do refs here instead of String?
    RefsHistory { branches: Vec<String>, tags: Vec<String> },
}

pub struct ExportConfig<'a> {
    pub source: &'a Repository,
    pub destination: Arc<dyn Storage>,
    pub versions: VersionSelection,
    pub update_config: bool,
}

fn select_snapshots(
    repo: &RepoInfo,
    versions: &VersionSelection,
) -> RepositoryResult<HashSet<SnapshotId>> {
    match versions {
        VersionSelection::SingleSnapshot(snapshot_id) => {
            Ok(HashSet::from([snapshot_id.clone()]))
        }
        VersionSelection::AllHistory => {
            let branches = repo.branch_names()?.map(|s| s.to_string());
            let tags = repo.tag_names()?.map(|s| s.to_string());
            let selection = VersionSelection::RefsHistory {
                branches: branches.collect(),
                tags: tags.collect(),
            };
            select_snapshots(repo, &selection)
        }
        VersionSelection::RefsHistory { branches, tags } => {
            let branch_roots = branches
                .iter()
                .map(|name| repo.resolve_branch(name))
                .map_ok(|snap_id| repo.ancestry(&snap_id));
            let tag_roots = tags
                .iter()
                .map(|name| repo.resolve_tag(name))
                .map_ok(|snap_id| repo.ancestry(&snap_id));
            let mut res = HashSet::new();
            for it in branch_roots.chain(tag_roots) {
                for snap_id in it?? {
                    if !res.insert(snap_id?.id) {
                        // we don'n need to continue because
                        // we have already seen this snapshot and all its ancestry
                        break;
                    }
                }
            }
            Ok(res)
        }
    }
}

enum ObjectType {
    Chunk,
    Manifest,
    TransactionLog,
    Snapshot,
}
//
// struct CopyMetadata {
//     object_type: ObjectType,
//     object_size: u64,
// }

enum Operation {
    Copy { from: String, to: String, object_type: ObjectType, object_size: Option<u64> },
}

// async fn process_snap_ids(
//     mut rec: Receiver<SnapshotId>,
//     mut sender: Sender<ManifestId>,
//     asset_manager: Arc<AssetManager>,
//     known_manifests: &HashSet<ManifestId>,
//     max_snapshots_in_memory: usize,
// ) {
//     let semaphore = Arc::new(Semaphore::new(max_snapshots_in_memory));
//     while let Some(sid) = rec.recv().await {
//         let guard = semaphore.acquire().await.unwrap();
//         let snap = asset_manager.fetch_snapshot(&sid).await.unwrap();
//         for mfile in snap.manifest_files() {
//             if !known_manifests.contains(&mfile.id) {
//                 sender.send(mfile.id).await;
//             }
//         }
//     }
// }

// fn export_snaps(
//     source: Arc<AssetManager>,
//     destination: Arc<AssetManager>,
//     missing_snaps: HashSet<SnapshotId>,
//     known_manifests: HashSet<ManifestId>,
//     known_chunks: HashSet<ChunkId>,
// ) {
//     let max_concurrent_snaps = 100; // FIXME: tune, configurable
//
//     let (snap_id_tx, snap_id_rx) = mpsc::channel(max_concurrent_snaps);
//     let snap_id_task_handle = tokio::spawn(process_snap_ids(rx, max_concurrent));
//     let handle = tokio::spawn(copier(rx, max_concurrent));
//     for task in chunks {
//         match task {
//             Ok(task) => tx.send(task).await?,
//             Err(_) => panic!("Error reading file"),
//         }
//     }
//     drop(tx);
//     //tx.send((0, "".to_string())).await?;
//
//     handle.await??;
//
//     Ok(())
// }

async fn calculate_diff(
    repo: &RepoInfo,
    versions: &VersionSelection,
    destination: Arc<dyn Storage>,
) -> RepositoryResult<(HashSet<SnapshotId>, HashSet<ManifestId>, HashSet<ChunkId>)> {
    // FIXME: tune caches
    let destination =
        Repository::open_or_create(None, destination, Default::default()).await?;
    //TODO: bring outside of GC
    let gc_config = GCConfig::clean_all(
        Utc::now(),
        Utc::now(),
        None,
        50.try_into().unwrap(),
        (50 * 1_024 * 1_024).try_into().unwrap(),
        500.try_into().unwrap(),
        true,
    );
    let (dest_chunks, dest_manifests, dest_snapshots) =
        find_retained(destination.asset_manager().clone(), &gc_config).await?;

    let source_snaps = select_snapshots(repo, versions)?;
    let missing_snaps = &source_snaps - &dest_snapshots;

    Ok((missing_snaps, dest_manifests, dest_chunks))
}

async fn copy_object(
    from_path: String,
    to_path: String,
    object_type: ObjectType,
    object_size: Option<u64>,
    source: Arc<dyn Storage>,
    destination: Arc<dyn Storage>,
    source_settings: Arc<Settings>,
    destination_settings: Arc<Settings>,
    result: Sender<Result<(ObjectType, u64), Box<dyn std::error::Error + Send>>>,
    _semaphore: OwnedSemaphorePermit,
) -> Result<(), Box<dyn std::error::Error + Send>> {
    let object_size = do_copy_object(
        from_path,
        to_path,
        object_size,
        source,
        destination,
        source_settings,
        destination_settings,
    )
    .await
    .map_err(|e| {
        let e: Box<dyn std::error::Error + Send> = Box::new(e);
        e
    })?;
    result.send(Ok((object_type, object_size))).await.unwrap();
    Ok(())
}

async fn do_copy_object(
    from_path: String,
    to_path: String,
    object_size: Option<u64>,
    source: Arc<dyn Storage>,
    destination: Arc<dyn Storage>,
    source_settings: Arc<Settings>,
    destination_settings: Arc<Settings>,
) -> Result<u64, StorageError> {
    let range = object_size.map(|n| 0..n);
    let (mut reader, _) = source
        .get_object(source_settings.as_ref(), from_path.as_str(), range.as_ref())
        .await?;

    // TODO: better capacity
    let mut buffer = Vec::with_capacity(object_size.unwrap_or(1024 * 1024) as usize);
    reader.read_to_end(&mut buffer).await?;
    let bytes = Bytes::from_owner(buffer);
    let len = bytes.len() as u64;

    destination
        .put_object(
            destination_settings.as_ref(),
            to_path.as_str(),
            bytes,
            None,
            Default::default(),
            None,
        )
        .await?;
    Ok(len)
}

async fn execute_operations(
    mut rec: Receiver<Operation>,
    result: Sender<Result<(ObjectType, u64), Box<dyn std::error::Error + Send>>>,
    source: Arc<dyn Storage>,
    destination: Arc<dyn Storage>,
    concurrent_operations: usize,
) -> Result<(), Box<dyn std::error::Error + Send>> {
    let source_settings = Arc::new(source.default_settings());
    let destination_settings = Arc::new(destination.default_settings());

    let semaphore = Arc::new(Semaphore::new(concurrent_operations));
    while let Some(op) = rec.recv().await {
        match op {
            Operation::Copy { from, to, object_type, object_size } => {
                let guard = semaphore.clone().acquire_owned().await.unwrap();
                // FIXME:
                //tokio::time::sleep(Duration::from_millis(400)).await;
                tokio::spawn(copy_object(
                    from,
                    to,
                    object_type,
                    object_size,
                    source.clone(),
                    destination.clone(),
                    source_settings.clone(),
                    destination_settings.clone(),
                    result.clone(),
                    guard,
                ));
            }
        }
    }
    Ok(())
}

async fn receive_operation_result(
    mut rec: Receiver<Result<(ObjectType, u64), Box<dyn std::error::Error + Send>>>,
    chunk_progress: ProgressBar,
    manifest_progress: ProgressBar,
    snapshot_progress: ProgressBar,
    bytes_progress: ProgressBar,
) {
    while let Some(res) = rec.recv().await {
        match res {
            Ok((object_type, object_size)) => match object_type {
                ObjectType::Chunk => {
                    chunk_progress.inc(1);
                    bytes_progress.inc(object_size);
                }
                ObjectType::Manifest => {
                    manifest_progress.inc(1);
                    bytes_progress.inc(object_size);
                }
                ObjectType::TransactionLog => todo!(),
                ObjectType::Snapshot => {
                    snapshot_progress.inc(1);
                    bytes_progress.inc(object_size);
                }
            },
            Err(err) => panic!("{}", err.to_string()),
        }
    }
}

pub async fn export(
    source: &Repository,
    destination: Arc<dyn Storage>,
    versions: &VersionSelection,
) -> Result<(), Box<dyn std::error::Error>> {
    // FIXME: fail for v1 repos
    let multi = indicatif::MultiProgress::new();
    let bytes_sty = indicatif::ProgressStyle::with_template(
        "{prefix:15.green} {binary_bytes} [{binary_bytes_per_sec}]",
    )
    .unwrap();
    let chunks_sty = indicatif::ProgressStyle::with_template(
        "{prefix:15.green} {bar:60} {human_pos:>}/{human_len} [{eta}]",
    )
    .unwrap();
    let others_sty = indicatif::ProgressStyle::with_template(
        "{prefix:15.green} {bar:60} {human_pos:>}/{human_len}",
    )
    .unwrap();
    let bytes_progress = indicatif::ProgressBar::new(0);
    let snapshots_progress = indicatif::ProgressBar::new(0);
    let manifests_progress = indicatif::ProgressBar::new(0);
    let chunks_progress = indicatif::ProgressBar::new(0);
    multi
        .add(chunks_progress.clone())
        .with_style(chunks_sty.clone())
        .with_prefix("🧊 Chunks:");
    multi
        .add(manifests_progress.clone())
        .with_style(others_sty.clone())
        .with_prefix("📜 Manifests:");
    multi
        .add(snapshots_progress.clone())
        .with_style(others_sty.clone())
        .with_prefix("📸 Snapshots:");
    multi.add(bytes_progress.clone()).with_style(bytes_sty).with_prefix("💾 Copied:");

    let (source_repo_info, _) = source.asset_manager().fetch_repo_info().await?;
    let (missing_snapshots, mut dest_manifests, mut dest_chunks) =
        calculate_diff(&source_repo_info, versions, destination.clone()).await?;
    snapshots_progress.set_length(missing_snapshots.len() as u64);

    let (op_result_sender, op_result_receiver) = mpsc::channel(100_000_000); // FIXME:
    let (op_execute_sender, op_execute_receiver) = mpsc::channel(100_000_000); // FIXME:

    let op_exec_handle = tokio::spawn(execute_operations(
        op_execute_receiver,
        op_result_sender,
        source.storage().clone(),
        destination.clone(),
        512, // FIXME:
    ));

    let op_result_handle = tokio::spawn(receive_operation_result(
        op_result_receiver,
        chunks_progress.clone(),
        manifests_progress.clone(),
        snapshots_progress.clone(),
        bytes_progress.clone(),
    ));

    for snap_id in missing_snapshots {
        let snap = source.asset_manager().fetch_snapshot(&snap_id).await?;
        for mfile in snap.manifest_files() {
            if !dest_manifests.contains(&mfile.id) {
                manifests_progress.inc_length(1);
                dest_manifests.insert(mfile.id.clone());
                let manifest = source
                    .asset_manager()
                    .fetch_manifest(&mfile.id, mfile.size_bytes)
                    .await?;

                //copy all chunks
                for chunk_payload in manifest.chunk_payloads() {
                    match chunk_payload? {
                        ChunkPayload::Inline(_) => { //TODO: materialize
                        }
                        ChunkPayload::Virtual(_) => { //TODO: materialize
                        }
                        ChunkPayload::Ref(chunk_ref) => {
                            dest_chunks.insert(chunk_ref.id.clone());
                            chunks_progress.inc_length(1);
                            let from_path = AssetManager::chunk_path(&chunk_ref.id);
                            let to_path = from_path.clone();
                            let op = Operation::Copy {
                                from: from_path,
                                to: to_path,
                                object_type: ObjectType::Chunk,
                                object_size: Some(chunk_ref.length),
                            };
                            op_execute_sender.send(op).await?;
                        }
                        _ => {
                            panic!("Unknown payload type");
                        }
                    }
                }

                //copy manifest
                let from_path = AssetManager::manifest_path(&manifest.id());
                let to_path = from_path.clone();
                let op = Operation::Copy {
                    from: from_path,
                    to: to_path,
                    object_type: ObjectType::Manifest,
                    object_size: Some(mfile.size_bytes),
                };
                op_execute_sender.send(op).await?;
            }
        }
        // copy snapshot
        let from_path = AssetManager::snapshot_path(&snap.id());
        let to_path = from_path.clone();
        let op = Operation::Copy {
            from: from_path,
            to: to_path,
            object_size: None,
            object_type: ObjectType::Snapshot,
        };
        op_execute_sender.send(op).await?;
    }

    drop(op_execute_sender);
    op_exec_handle.await?.unwrap();
    op_result_handle.await?;

    Ok(())
}

#[cfg(test)]
mod tests {}
