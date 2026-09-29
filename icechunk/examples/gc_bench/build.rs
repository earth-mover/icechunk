//! The `build` subcommand: writes a synthetic repository directly through the `AssetManager`.

use std::{collections::HashMap, num::NonZeroU16, sync::Arc, time::Instant};

use bytes::Bytes;
use chrono::Utc;
use futures::{
    FutureExt as _, StreamExt as _, TryStreamExt as _,
    future::{BoxFuture, Shared, ready},
};
use icechunk::{
    Repository, Storage,
    asset_manager::AssetManager,
    format::{
        ChunkId, ChunkIndices, ManifestId, Move, NodeId, Path, SnapshotId,
        format_constants::SpecVersionBin,
        manifest::{
            ChunkInfo, ChunkPayload, ChunkRef, Manifest, ManifestExtents, ManifestRef,
        },
        repo_info::{RepoInfo, UpdateInfo, UpdateType},
        snapshot::{
            ArrayShape, ManifestFileInfo, NodeData, NodeSnapshot, Snapshot, SnapshotInfo,
        },
        transaction_log::TransactionLog,
    },
    storage::{RetriesSettings, StorageContext},
};
use icechunk_types::error::ICResultCtxExt as _;
use tokio::{sync::Semaphore, task::JoinSet};

use crate::{
    BoxError,
    cli::{BuildArgs, DatasetParams},
    storage::{RUSTFS_PORT, rustfs_storage},
};

/// Length recorded in every chunk ref. Constant so stats has something to sum;
/// it does not need to match the zero-byte chunk objects.
const REF_LENGTH: u64 = 1024;
const ARRAY_USER_DATA: &[u8] = br#"{"zarr_format":3,"node_type":"array"}"#;

/// Result of a spawned write that several later tasks may await.
/// Errors are strings because `Shared` needs a `Clone` output.
type SharedResult<T> = Shared<BoxFuture<'static, Result<T, String>>>;

/// One manifest lineage.
struct Split {
    node: NodeId,
    path: Path,
    ids: Vec<ChunkId>,
    current: SharedResult<ManifestFileInfo>,
}

/// Bounded-concurrency writer over a `JoinSet`.
struct Writer {
    am: Arc<AssetManager>,
    permits: Arc<Semaphore>,
    tasks: JoinSet<Result<(), BoxError>>,
}

impl Writer {
    fn new(am: Arc<AssetManager>, concurrency: usize) -> Self {
        Self { am, permits: Arc::new(Semaphore::new(concurrency)), tasks: JoinSet::new() }
    }

    /// Spawn `f(asset_manager)` once a permit is available; reap finished tasks.
    async fn spawn<F, Fut>(&mut self, f: F) -> Result<(), BoxError>
    where
        F: FnOnce(Arc<AssetManager>) -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), BoxError>> + Send + 'static,
    {
        let permit = Arc::clone(&self.permits).acquire_owned().await?;
        let am = Arc::clone(&self.am);
        self.tasks.spawn(async move {
            let _permit = permit;
            f(am).await
        });
        while let Some(res) = self.tasks.try_join_next() {
            res??;
        }
        Ok(())
    }

    /// Like `spawn`, but hands back the task's value as a shared future.
    async fn spawn_shared<T, F, Fut>(&mut self, f: F) -> Result<SharedResult<T>, BoxError>
    where
        T: Clone + Send + Sync + 'static,
        F: FnOnce(Arc<AssetManager>) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T, BoxError>> + Send + 'static,
    {
        let permit = Arc::clone(&self.permits).acquire_owned().await?;
        let am = Arc::clone(&self.am);
        let handle = tokio::spawn(async move {
            let _permit = permit;
            f(am).await.map_err(|e| e.to_string())
        });
        Ok(handle
            .map(|joined| joined.map_err(|e| e.to_string()).and_then(|r| r))
            .boxed()
            .shared())
    }

    /// Wait for every spawned task.
    async fn drain(&mut self) -> Result<(), BoxError> {
        while let Some(res) = self.tasks.join_next().await {
            res??;
        }
        Ok(())
    }
}

fn random_ids(n: usize) -> Vec<ChunkId> {
    (0..n).map(|_| ChunkId::random()).collect()
}

/// Serialize a manifest on the blocking pool. `Manifest::from_iter` is async
/// in signature only; it never waits on IO.
async fn make_manifest(
    node: NodeId,
    ids: Vec<ChunkId>,
) -> Result<Arc<Manifest>, BoxError> {
    let manifest = tokio::task::spawn_blocking(move || {
        let id = ManifestId::random();
        let infos = ids.into_iter().enumerate().map(|(i, chunk_id)| ChunkInfo {
            node: node.clone(),
            coord: ChunkIndices(vec![i as u32]),
            payload: ChunkPayload::Ref(ChunkRef {
                id: chunk_id,
                offset: 0,
                length: REF_LENGTH,
            }),
        });
        futures::executor::block_on(Manifest::from_iter(&id, infos, None))
    })
    .await??;
    manifest.map(Arc::new).ok_or_else(|| "manifest with no refs".into())
}

/// Write a manifest and return the info a snapshot needs to reference it.
async fn write_manifest(
    am: &AssetManager,
    node: NodeId,
    ids: Vec<ChunkId>,
) -> Result<ManifestFileInfo, BoxError> {
    let manifest = make_manifest(node, ids).await?;
    let size = am.write_manifest("a", Arc::clone(&manifest)).await?;
    Ok(ManifestFileInfo::new(&manifest, size))
}

/// One array node per `(node, path, manifest)` triple; nodes sorted by path.
fn make_snapshot(
    id: SnapshotId,
    parent: SnapshotId,
    message: &str,
    arrays: &[(NodeId, Path, ManifestId)],
    manifest_files: Vec<ManifestFileInfo>,
    refs_per_manifest: usize,
) -> Result<Snapshot, BoxError> {
    let shape = ArrayShape::new([(refs_per_manifest as u64, refs_per_manifest as u32)])
        .ok_or("invalid array shape")?;
    let mut nodes: Vec<NodeSnapshot> = arrays
        .iter()
        .map(|(node, path, manifest_id)| NodeSnapshot {
            id: node.clone(),
            path: path.clone(),
            user_data: Bytes::from_static(ARRAY_USER_DATA),
            node_data: NodeData::Array {
                shape: shape.clone(),
                dimension_names: None,
                manifests: vec![ManifestRef {
                    object_id: manifest_id.clone(),
                    extents: ManifestExtents::ALL,
                }],
            },
        })
        .collect();
    nodes.sort_by(|a, b| a.path.cmp(&b.path));
    Ok(Snapshot::from_iter(
        Some(id),
        Some(parent),
        SpecVersionBin::V2,
        message,
        None,
        manifest_files,
        Some(Utc::now()),
        nodes.into_iter().map(Ok),
    )?)
}

/// An empty transaction log for `snapshot_id`.
fn make_tx_log(snapshot_id: &SnapshotId) -> TransactionLog {
    TransactionLog::new_from_parts(
        snapshot_id,
        std::iter::empty::<NodeId>(),
        std::iter::empty::<NodeId>(),
        std::iter::empty::<NodeId>(),
        std::iter::empty::<NodeId>(),
        std::iter::empty::<NodeId>(),
        std::iter::empty::<NodeId>(),
        std::iter::empty::<(NodeId, std::iter::Empty<ChunkIndices>)>(),
        std::iter::empty::<Move>(),
    )
}

/// Write snapshot and its tx log; serialization on the blocking pool.
async fn write_snapshot_and_log(
    am: &AssetManager,
    id: SnapshotId,
    parent: SnapshotId,
    message: String,
    arrays: Vec<(NodeId, Path, ManifestId)>,
    manifest_files: Vec<ManifestFileInfo>,
    refs_per_manifest: usize,
) -> Result<(), BoxError> {
    let snap_id = id.clone();
    let (snapshot, log) = tokio::task::spawn_blocking(move || {
        let snapshot = make_snapshot(
            id,
            parent,
            &message,
            &arrays,
            manifest_files,
            refs_per_manifest,
        )?;
        let log = make_tx_log(&snapshot.id());
        Ok::<_, BoxError>((Arc::new(snapshot), Arc::new(log)))
    })
    .await??;
    am.write_snapshot(snapshot).await?;
    am.write_transaction_log(snap_id, log).await?;
    Ok(())
}

async fn delete_everything(
    storage: &Arc<dyn Storage + Send + Sync>,
) -> Result<(), BoxError> {
    let settings = storage.default_settings().await?;
    let ctx = StorageContext::unattributed(&settings);
    let keys: Vec<(String, u64)> = storage
        .list_objects(&ctx, "")
        .await?
        .map_ok(|info| (info.id, info.size_bytes))
        .try_collect()
        .await?;
    println!("Deleting {} existing objects", keys.len());
    for batch in keys.chunks(1_000) {
        storage.delete_batch(&ctx, "", batch.to_vec()).await?;
    }
    Ok(())
}

async fn prefix_is_empty(
    storage: &Arc<dyn Storage + Send + Sync>,
) -> Result<bool, BoxError> {
    let settings = storage.default_settings().await?;
    let ctx = StorageContext::unattributed(&settings);
    let mut stream = storage.list_objects(&ctx, "").await?;
    Ok(stream.next().await.is_none())
}

async fn count_and_bytes<Id>(
    stream: futures::stream::BoxStream<
        '_,
        icechunk::repository::RepositoryResult<icechunk::storage::ListInfo<Id>>,
    >,
) -> Result<(u64, u64), BoxError> {
    Ok(stream
        .try_fold((0u64, 0u64), |(n, b), info| ready(Ok((n + 1, b + info.size_bytes))))
        .await?)
}

async fn print_summary(
    am: &AssetManager,
    unique_referenced: usize,
) -> Result<(), BoxError> {
    println!();
    println!("{:<18} {:>12} {:>16}", "prefix", "objects", "bytes");
    let (n, b) = count_and_bytes(am.list_snapshots().await?).await?;
    println!("{:<18} {n:>12} {b:>16}", "snapshots");
    let (n, b) = count_and_bytes(am.list_transaction_logs().await?).await?;
    println!("{:<18} {n:>12} {b:>16}", "transaction_logs");
    let (n, b) = count_and_bytes(am.list_manifests().await?).await?;
    println!("{:<18} {n:>12} {b:>16}", "manifests");
    let (n, b) = count_and_bytes(am.list_chunks().await?).await?;
    println!("{:<18} {n:>12} {b:>16}", "chunks");
    println!("unique chunk ids referenced by the reachable chain: {unique_referenced}");
    Ok(())
}

pub(crate) async fn build(args: BuildArgs) -> Result<(), BoxError> {
    let params = DatasetParams::resolve(&args);
    println!("Building dataset {} with {params:#?}", args.name);
    let started = Instant::now();

    let storage = rustfs_storage(&args.name, RUSTFS_PORT)?;
    if !prefix_is_empty(&storage).await? {
        if args.force {
            delete_everything(&storage).await?;
        } else {
            return Err(format!(
                "dataset {} already exists; pass --force to replace it",
                args.name
            )
            .into());
        }
    }

    let repo = Repository::create(
        None,
        Arc::clone(&storage),
        HashMap::new(),
        Some(SpecVersionBin::V2),
        true,
        None,
    )
    .await?;
    let num_updates_per_file = repo.config().num_updates_per_repo_info_file();
    let am = Arc::clone(repo.asset_manager());
    let mut writer = Writer::new(Arc::clone(&am), params.write_concurrency);
    let mut rng = rand::rng();

    // ---- garbage: snapshots (with their manifests and tx logs) not in repo info
    println!("Writing {} garbage snapshots", params.garbage_snapshots);
    for i in 0..params.garbage_snapshots {
        let refs = params.refs_per_manifest;
        let per_snapshot = params.garbage_manifests_per_snapshot;
        writer
            .spawn(move |am| async move {
                let mut arrays = Vec::with_capacity(per_snapshot);
                let mut files = Vec::with_capacity(per_snapshot);
                for j in 0..per_snapshot {
                    let node = NodeId::random();
                    let path = Path::try_from(format!("/garbage-{i}-{j}").as_str())?;
                    let info =
                        write_manifest(&am, node.clone(), random_ids(refs)).await?;
                    arrays.push((node, path, info.id.clone()));
                    files.push(info);
                }
                write_snapshot_and_log(
                    &am,
                    SnapshotId::random(),
                    SnapshotId::random(),
                    format!("garbage {i}"),
                    arrays,
                    files,
                    refs,
                )
                .await
            })
            .await?;
    }
    writer.drain().await?;

    let garbage_chunks =
        (params.chunk_objects as f64 * params.garbage_chunk_fraction).round() as usize;
    println!("Writing {garbage_chunks} garbage chunk objects");
    for _ in 0..garbage_chunks {
        writer
            .spawn(|am| async move {
                Ok(am.write_chunk("a", &[0], ChunkId::random(), Bytes::new()).await?)
            })
            .await?;
    }
    writer.drain().await?;
    println!("garbage/reachable boundary: {}", Utc::now().to_rfc3339());

    // ---- reachable chain
    println!("Initializing {} splits", params.splits);
    let mut splits: Vec<Split> = Vec::with_capacity(params.splits);
    let mut all_referenced: Vec<ChunkId> = Vec::new();
    for i in 0..params.splits {
        let node = NodeId::random();
        let path = Path::try_from(format!("/array-{i:04}").as_str())?;
        let ids = random_ids(params.refs_per_manifest);
        all_referenced.extend(ids.iter().cloned());
        let current = {
            let node = node.clone();
            let ids = ids.clone();
            writer
                .spawn_shared(
                    move |am| async move { write_manifest(&am, node, ids).await },
                )
                .await?
        };
        splits.push(Split { node, path, ids, current });
    }

    println!("Writing {} reachable snapshots", params.reachable_snapshots);
    // Pre-generated so a snapshot task never has to wait for its parent's task.
    let snap_ids: Vec<SnapshotId> =
        (0..params.reachable_snapshots).map(|_| SnapshotId::random()).collect();
    let mut infos: Vec<SnapshotInfo> = Vec::with_capacity(params.reachable_snapshots);
    let mut next_split = 0usize;
    let churned = (params.refs_per_manifest as f64 * params.churn).round() as usize;
    for (i, id) in snap_ids.iter().enumerate() {
        // Snapshot 0 references the initial manifests, so every manifest ever
        // written ends up referenced by at least one reachable snapshot.
        if i > 0 {
            for _ in 0..params.rewrites_per_snapshot {
                let split = &mut splits[next_split % params.splits];
                next_split += 1;
                // Distinct slots: replacing the same slot twice would strand the
                // first fresh id in `all_referenced` without it ever reaching a
                // manifest, and the count has to stay exact.
                let slots = rand::seq::index::sample(
                    &mut rng,
                    split.ids.len(),
                    churned.min(split.ids.len()),
                );
                for idx in slots.iter() {
                    let fresh = ChunkId::random();
                    all_referenced.push(fresh.clone());
                    split.ids[idx] = fresh;
                }
                let node = split.node.clone();
                let ids = split.ids.clone();
                split.current = writer
                    .spawn_shared(move |am| async move {
                        write_manifest(&am, node, ids).await
                    })
                    .await?;
            }
        }
        let parent =
            if i == 0 { Snapshot::INITIAL_SNAPSHOT_ID } else { snap_ids[i - 1].clone() };
        let manifests: Vec<SharedResult<ManifestFileInfo>> =
            splits.iter().map(|s| s.current.clone()).collect();
        let nodes: Vec<(NodeId, Path)> =
            splits.iter().map(|s| (s.node.clone(), s.path.clone())).collect();
        let refs = params.refs_per_manifest;
        let message = format!("commit {i}");
        let snap_id = id.clone();
        let snap_parent = parent.clone();
        let snap_message = message.clone();
        writer
            .spawn(move |am| async move {
                let files: Vec<ManifestFileInfo> =
                    futures::future::try_join_all(manifests).await?;
                let arrays: Vec<(NodeId, Path, ManifestId)> = nodes
                    .into_iter()
                    .zip(files.iter())
                    .map(|((node, path), info)| (node, path, info.id.clone()))
                    .collect();
                write_snapshot_and_log(
                    &am,
                    snap_id,
                    snap_parent,
                    snap_message,
                    arrays,
                    files,
                    refs,
                )
                .await
            })
            .await?;
        infos.push(SnapshotInfo {
            id: id.clone(),
            parent_id: Some(parent),
            flushed_at: Utc::now(),
            message,
            metadata: Default::default(),
            pruned_ancestor_tx_logs: vec![],
        });
        if (i + 1) % 100 == 0 {
            println!("  {} snapshots spawned, {:?} elapsed", i + 1, started.elapsed());
        }
    }
    writer.drain().await?;

    let referenced_chunks = params.chunk_objects - garbage_chunks;
    let amount = referenced_chunks.min(all_referenced.len());
    println!("Writing {amount} referenced chunk objects");
    let picks = rand::seq::index::sample(&mut rng, all_referenced.len(), amount);
    for idx in picks.iter() {
        let id = all_referenced[idx].clone();
        writer
            .spawn(move |am| async move {
                Ok(am.write_chunk("a", &[0], id, Bytes::new()).await?)
            })
            .await?;
    }
    writer.drain().await?;

    // ---- register the chain in repo info
    println!("Updating repo info");
    let tip = snap_ids.last().cloned().ok_or("no reachable snapshots")?;
    let retries =
        RetriesSettings { max_tries: Some(NonZeroU16::MIN), ..Default::default() };
    am.update_repo_info(
        &retries,
        |repo_info: Arc<RepoInfo>, backup_path: &str, _version| {
            let branches: Vec<(&str, SnapshotId)> = repo_info
                .branches()
                .inject()?
                .map(
                    |(name, snap)| {
                        if name == "main" { (name, tip.clone()) } else { (name, snap) }
                    },
                )
                .collect();
            let mut all: Vec<SnapshotInfo> =
                repo_info.all_snapshots().inject()?.collect::<Result<_, _>>().inject()?;
            all.extend(infos.iter().cloned());
            let config_bytes = repo_info.config_bytes_raw().inject()?;
            let new_info = RepoInfo::new(
                SpecVersionBin::V2,
                repo_info.tags().inject()?,
                branches,
                repo_info.deleted_tags().inject()?,
                all,
                &repo_info.metadata().inject()?,
                UpdateInfo {
                    update_type: UpdateType::NewCommitUpdate {
                        branch: "main".to_string(),
                        new_snap_id: tip.clone(),
                    },
                    update_time: Utc::now(),
                    previous_updates: repo_info.latest_updates().inject()?,
                },
                Some(backup_path),
                num_updates_per_file,
                repo_info.repo_before_updates().inject()?,
                config_bytes.as_deref(),
                repo_info.enabled_feature_flags().inject()?,
                repo_info.disabled_feature_flags().inject()?,
                &repo_info.status().inject()?,
            )
            .inject()?;
            Ok(Arc::new(new_info))
        },
    )
    .await?;

    println!("Build finished in {:?}", started.elapsed());
    print_summary(&am, all_referenced.len()).await?;
    Ok(())
}
