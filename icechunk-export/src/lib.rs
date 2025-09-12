use std::{
    collections::HashSet,
    sync::{Arc, atomic::AtomicUsize},
};

use async_trait::async_trait;
use bytes::Bytes;
use chrono::Utc;
use icechunk::{
    Repository, Storage,
    asset_manager::AssetManager,
    error::ICError,
    format::{
        ChunkId, IcechunkFormatError, IcechunkFormatErrorKind, IcechunkResult,
        ManifestId, SnapshotId,
        format_constants::SpecVersionBin,
        manifest::ChunkPayload,
        repo_info::{RepoInfo, UpdateType},
        snapshot::Snapshot,
    },
    ops::gc::{GCConfig, find_retained},
    refs::Ref,
    repository::{RepositoryError, RepositoryErrorKind, RepositoryResult},
    storage::{Settings, StorageErrorKind},
};
use indicatif::{MultiProgress, ProgressBar};
use itertools::Itertools as _;
use tokio::{
    io::AsyncReadExt as _,
    sync::{
        OwnedSemaphorePermit, Semaphore,
        mpsc::{self, UnboundedReceiver, UnboundedSender},
    },
};
use tokio_util::task::TaskTracker;

#[derive(Debug, PartialEq, Eq)]
pub enum VersionSelection {
    SingleSnapshot(SnapshotId),
    AllHistory,
    // TODO: can we do refs here instead of String?
    RefsHistory {
        branches: Vec<String>,
        tags: Vec<String>,
        main_points_to: Option<SnapshotId>,
    },
}

fn all_history_as_refs_history(repo: &RepoInfo) -> IcechunkResult<VersionSelection> {
    let branches = repo.branch_names()?.map(|s| s.to_string());
    let tags = repo.tag_names()?.map(|s| s.to_string());
    Ok(VersionSelection::RefsHistory {
        branches: branches.collect(),
        tags: tags.collect(),
        main_points_to: None,
    })
}

fn collect_requested_snapshots(
    repo: &RepoInfo,
    branches: &Vec<String>,
    tags: &Vec<String>,
) -> IcechunkResult<HashSet<SnapshotId>> {
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

fn select_snapshots(
    repo: &RepoInfo,
    versions: &VersionSelection,
) -> ExportResult<HashSet<SnapshotId>> {
    match versions {
        VersionSelection::SingleSnapshot(snapshot_id) => {
            Ok(HashSet::from([snapshot_id.clone()]))
        }
        VersionSelection::AllHistory => {
            let selection = all_history_as_refs_history(repo)
                .map_err(|e| ExportError::from(ExportErrorKind::FormatError(e.kind)))?;
            select_snapshots(repo, &selection)
        }
        VersionSelection::RefsHistory { branches, tags, main_points_to } => {
            if main_points_to.is_none()
                && !branches.contains(&Ref::DEFAULT_BRANCH.to_string())
            {
                return Err(ExportErrorKind::InvalidVersionSelection(
                    "target repository needs a `main` branch, if `main_points_to` is not specified, `main` must be included in the exported branches".to_string(),
                ).into());
            }
            let res = collect_requested_snapshots(repo, branches, tags)
                .map_err(|e| ExportError::from(ExportErrorKind::FormatError(e.kind)))?;
            if let Some(sid) = main_points_to
                && !res.contains(sid)
            {
                return Err(ExportErrorKind::InvalidVersionSelection(
                    "target repository needs a valid `main` branch, the snapshot in `main_points_to` is not exported by this version selection".to_string(),
                ).into());
            }
            Ok(res)
        }
    }
}

pub enum ObjectType {
    Chunk,
    Manifest,
    TransactionLog,
    Snapshot,
}

enum Operation {
    Copy { path: String, object_type: ObjectType, object_size: Option<u64> },
}

async fn calculate_diff(
    requested_snaps: &HashSet<SnapshotId>,
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

    let missing_snaps = requested_snaps - &dest_snapshots;

    Ok((missing_snaps, dest_manifests, dest_chunks))
}

pub type ObjectSize = u64;
pub type OperationResult = (ObjectType, ObjectSize);

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ExportErrorKind {
    #[error(transparent)]
    StorageError(StorageErrorKind),
    #[error(transparent)]
    RepositoryError(RepositoryErrorKind),
    #[error(transparent)]
    FormatError(IcechunkFormatErrorKind),
    #[error("I/O error")]
    IOError(#[from] std::io::Error),

    #[error("invalid selection of versions to export: {0}")]
    InvalidVersionSelection(String),
}

pub type ExportError = ICError<ExportErrorKind>;

pub type ExportResult<T> = Result<T, ExportError>;

impl From<ExportErrorKind> for ExportError {
    fn from(value: ExportErrorKind) -> Self {
        Self::new(value)
    }
}

#[derive(Debug, Clone)]
struct Endpoint {
    pub storage: Arc<dyn Storage>,
    pub settings: Arc<Settings>,
}

async fn copy_object(
    path: String,
    object_type: ObjectType,
    object_size: Option<u64>,
    source: Endpoint,
    destination: Endpoint,
    result: UnboundedSender<ExportResult<OperationResult>>,
    semaphore: OwnedSemaphorePermit,
) {
    let res = do_copy_object(path.as_str(), object_size, source, destination)
        .await
        .map(|size| (object_type, size));

    drop(semaphore);

    // we can expect here because the result Receiver is never closed
    #[allow(clippy::expect_used)]
    result
        .send(res)
        .expect("Unexpected error: Failed to send copy_object operation response");
}

async fn do_copy_object(
    path: &str,
    object_size: Option<u64>,
    source: Endpoint,
    destination: Endpoint,
) -> ExportResult<u64> {
    let range = object_size.map(|n| 0..n);
    let (mut reader, _) = source
        .storage
        .get_object(source.settings.as_ref(), path, range.as_ref())
        .await
        .map_err(|e| ExportError::from(ExportErrorKind::StorageError(e.kind)))?;

    // TODO: better capacity
    let mut buffer = Vec::with_capacity(object_size.unwrap_or(1024 * 1024) as usize);
    reader
        .read_to_end(&mut buffer)
        .await
        .map_err(|e| ExportError::from(ExportErrorKind::IOError(e)))?;
    let bytes = Bytes::from_owner(buffer);
    let len = bytes.len() as u64;

    destination
        .storage
        .put_object(
            destination.settings.as_ref(),
            path,
            bytes,
            None,
            Default::default(),
            None,
        )
        .await
        .map_err(|e| ExportError::from(ExportErrorKind::StorageError(e.kind)))?;
    Ok(len)
}

async fn execute_operations(
    mut rec: UnboundedReceiver<Operation>,
    result: UnboundedSender<ExportResult<OperationResult>>,
    source: Arc<dyn Storage>,
    destination: Arc<dyn Storage>,
    max_concurrent_operations: usize,
    task_tracker: TaskTracker,
) -> usize {
    let settings = Arc::new(source.default_settings());
    let source = Endpoint { storage: source, settings };
    let settings = Arc::new(destination.default_settings());
    let destination = Endpoint { storage: destination, settings };

    let mut spawned = 0;
    let semaphore = Arc::new(Semaphore::new(max_concurrent_operations));
    while let Some(op) = rec.recv().await {
        spawned += 1;
        match op {
            Operation::Copy { path, object_type, object_size } => {
                // we can expect here because the semaphore is never closed
                #[allow(clippy::expect_used)]
                let guard =
                    semaphore.clone().acquire_owned().await.expect(
                        "Unexpected error executing operation: semaphore is closed",
                    );
                task_tracker.spawn(copy_object(
                    path,
                    object_type,
                    object_size,
                    source.clone(),
                    destination.clone(),
                    result.clone(),
                    guard,
                ));
            }
        }
    }
    spawned
}

#[async_trait]
pub trait ProgresListener {
    async fn completed(&self, object_type: ObjectType, object_size: u64);
    async fn discovered(&self, object_type: ObjectType, count: u64);
    async fn done(&self);
}

#[derive(Debug)]
pub struct ProgressBars {
    chunk_progress: ProgressBar,
    manifest_progress: ProgressBar,
    snapshot_progress: ProgressBar,
    transaction_progress: ProgressBar,
    bytes_progress: ProgressBar,
    _multi: MultiProgress,
}

impl Default for ProgressBars {
    fn default() -> Self {
        Self::new()
    }
}

impl ProgressBars {
    pub fn new() -> Self {
        let multi = indicatif::MultiProgress::new();
        // unwrapping is allowed when creating templates because the
        // template string is verified to work
        #[allow(clippy::unwrap_used)]
        let bytes_sty = indicatif::ProgressStyle::with_template(
            "{prefix:16.green} {binary_bytes} [{binary_bytes_per_sec}]",
        )
        .unwrap();
        #[allow(clippy::unwrap_used)]
        let chunks_sty = indicatif::ProgressStyle::with_template(
            "{prefix:16.green} {bar:60} {human_pos:>}/{human_len} [{eta}]",
        )
        .unwrap();
        #[allow(clippy::unwrap_used)]
        let others_sty = indicatif::ProgressStyle::with_template(
            "{prefix:16.green} {bar:60} {human_pos:>}/{human_len}",
        )
        .unwrap();
        let bytes_progress = indicatif::ProgressBar::new(0);
        let snapshot_progress = indicatif::ProgressBar::new(0);
        let transaction_progress = indicatif::ProgressBar::new(0);
        let manifest_progress = indicatif::ProgressBar::new(0);
        let chunk_progress = indicatif::ProgressBar::new(0);
        multi
            .add(chunk_progress.clone())
            .with_style(chunks_sty.clone())
            .with_prefix("🧊 Chunks:");
        multi
            .add(manifest_progress.clone())
            .with_style(others_sty.clone())
            .with_prefix("📜 Manifests:");
        multi
            .add(snapshot_progress.clone())
            .with_style(others_sty.clone())
            .with_prefix("📸 Snapshots:");
        multi
            .add(transaction_progress.clone())
            .with_style(others_sty.clone())
            .with_prefix("🤝 Transactions:");
        multi.add(bytes_progress.clone()).with_style(bytes_sty).with_prefix("💾 Copied:");
        ProgressBars {
            chunk_progress,
            manifest_progress,
            snapshot_progress,
            transaction_progress,
            bytes_progress,
            _multi: multi,
        }
    }
}

#[async_trait]
impl ProgresListener for ProgressBars {
    async fn completed(&self, object_type: ObjectType, object_size: u64) {
        match object_type {
            ObjectType::Chunk => self.chunk_progress.inc(1),
            ObjectType::Manifest => self.manifest_progress.inc(1),
            ObjectType::TransactionLog => self.transaction_progress.inc(1),
            ObjectType::Snapshot => self.snapshot_progress.inc(1),
        }
        self.bytes_progress.inc(object_size);
    }
    async fn discovered(&self, object_type: ObjectType, count: u64) {
        match object_type {
            ObjectType::Chunk => self.chunk_progress.inc_length(count),
            ObjectType::Manifest => self.manifest_progress.inc_length(count),
            ObjectType::TransactionLog => self.transaction_progress.inc_length(count),
            ObjectType::Snapshot => self.snapshot_progress.inc_length(count),
        }
    }
    async fn done(&self) {
        self.chunk_progress.abandon();
        self.manifest_progress.abandon();
        self.transaction_progress.abandon();
        self.snapshot_progress.abandon();
        self.bytes_progress.abandon();
    }
}

async fn receive_operation_result(
    mut rec: UnboundedReceiver<ExportResult<OperationResult>>,
    progress_listener: Arc<dyn ProgresListener + Send + Sync>,
) -> ExportResult<usize> {
    let results = AtomicUsize::new(0);
    while let Some(res) = rec.recv().await {
        results.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        match res {
            Ok((object_type, object_size)) => match object_type {
                ObjectType::Chunk => {
                    progress_listener.completed(ObjectType::Chunk, object_size).await;
                }
                ObjectType::Manifest => {
                    progress_listener.completed(ObjectType::Manifest, object_size).await;
                }
                ObjectType::TransactionLog => {
                    progress_listener
                        .completed(ObjectType::TransactionLog, object_size)
                        .await;
                }
                ObjectType::Snapshot => {
                    progress_listener.completed(ObjectType::Snapshot, object_size).await;
                }
            },
            Err(err) => return Err(err),
        }
    }
    Ok(results.into_inner())
}

pub async fn export(
    source: &Repository,
    destination: Arc<dyn Storage>,
    versions: &VersionSelection,
    progress_listener: Arc<dyn ProgresListener + Send + Sync>,
    max_concurrent_operations: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    if source.spec_version() < SpecVersionBin::V2dot0 {
        return Err("export cannot run on Icechunk version 1 repositories.".into());
    }

    let (source_repo_info, _) = source.asset_manager().fetch_repo_info().await?;
    let requested_snaps = select_snapshots(&source_repo_info, versions)?;
    let (missing_snapshots, mut dest_manifests, mut dest_chunks) =
        calculate_diff(&requested_snaps, destination.clone()).await?;
    progress_listener
        .discovered(ObjectType::Snapshot, missing_snapshots.len() as u64)
        .await;
    progress_listener
        .discovered(ObjectType::TransactionLog, missing_snapshots.len() as u64)
        .await;

    // TODO: should we limit these channels?
    let (op_result_sender, op_result_receiver) = mpsc::unbounded_channel();
    let (op_execute_sender, op_execute_receiver) = mpsc::unbounded_channel();

    let operations_tracker = TaskTracker::new();

    let op_exec_handle = tokio::spawn(execute_operations(
        op_execute_receiver,
        op_result_sender,
        source.storage().clone(),
        destination.clone(),
        max_concurrent_operations,
        operations_tracker.clone(),
    ));

    let op_result_handle = tokio::spawn(receive_operation_result(
        op_result_receiver,
        Arc::clone(&progress_listener),
    ));

    for snap_id in missing_snapshots {
        let snap = source.asset_manager().fetch_snapshot(&snap_id).await?;
        for mfile in snap.manifest_files() {
            if !dest_manifests.contains(&mfile.id) {
                progress_listener.discovered(ObjectType::Manifest, 1).await;
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
                            progress_listener.discovered(ObjectType::Chunk, 1).await;
                            let path = AssetManager::chunk_path(&chunk_ref.id);
                            let op = Operation::Copy {
                                path,
                                object_type: ObjectType::Chunk,
                                object_size: Some(chunk_ref.length),
                            };
                            op_execute_sender.send(op)?;
                        }
                        _ => {
                            return Err(
                                "bug in export, unknown chunk payload type".into()
                            );
                        }
                    }
                }

                //copy manifest
                let path = AssetManager::manifest_path(&manifest.id());
                let op = Operation::Copy {
                    path,
                    object_type: ObjectType::Manifest,
                    object_size: Some(mfile.size_bytes),
                };
                op_execute_sender.send(op)?;
            }
        }
        // copy tx log
        let path = AssetManager::transaction_path(&snap.id());
        let op = Operation::Copy {
            path,
            object_size: None,
            object_type: ObjectType::TransactionLog,
        };
        op_execute_sender.send(op)?;

        // copy snapshot
        let path = AssetManager::snapshot_path(&snap.id());
        let op = Operation::Copy {
            path,
            object_size: None,
            object_type: ObjectType::Snapshot,
        };
        op_execute_sender.send(op)?;
    }

    drop(op_execute_sender);
    let spawned_ops = op_exec_handle.await?;
    let received_results = op_result_handle.await??;

    operations_tracker.close();
    operations_tracker.wait().await;

    let new_repo_info =
        update_repo_info(source_repo_info.as_ref(), &requested_snaps, versions);

    let x = destination.;

    progress_listener.done().await;

    dbg!(spawned_ops);
    dbg!(received_results);
    println!("---------------- {}", spawned_ops);
    println!(" ///////// {received_results}");
    Ok(())
}

fn update_repo_info(
    original: &RepoInfo,
    requested_snaps: &HashSet<SnapshotId>,
    selection: &VersionSelection,
) -> IcechunkResult<RepoInfo> {
    match selection {
        VersionSelection::SingleSnapshot(snapshot_id) => {
            let tags = original.tags()?.filter(|(_, sid)| requested_snaps.contains(sid));
            let branches = original.branches()?.filter_map(|(name, sid)| {
                if name == Ref::DEFAULT_BRANCH {
                    Some((Ref::DEFAULT_BRANCH, snapshot_id.clone()))
                } else if requested_snaps.contains(&sid) {
                    Some((name, sid))
                } else {
                    None
                }
            });
            let initial_snap = original.find_snapshot(&Snapshot::INITIAL_SNAPSHOT_ID)?;
            let mut target_snap = original.find_snapshot(snapshot_id)?;
            target_snap.parent_id = Some(initial_snap.id.clone());

            RepoInfo::new(
                tags,
                branches,
                std::iter::empty(),
                [initial_snap, target_snap],
                &UpdateType::RepoInitializedUpdate,
                None,
            )
        }
        VersionSelection::AllHistory => {
            let snapshots: Vec<_> = original.all_snapshots()?.try_collect()?;
            RepoInfo::new(
                original.tags()?,
                original.branches()?,
                std::iter::empty(),
                snapshots,
                &UpdateType::RepoInitializedUpdate,
                None,
            )
        }
        VersionSelection::RefsHistory { main_points_to, branches, .. } => {
            assert!(
                branches.contains(&Ref::DEFAULT_BRANCH.to_string())
                    || main_points_to.is_some()
            );
            let tags = original.tags()?.filter(|(_, sid)| requested_snaps.contains(sid));
            let branches = original.branches()?.filter_map(|(name, sid)| {
                // at the beginning of export is checked that main_points_to points to an exported
                // snapshot
                if name == Ref::DEFAULT_BRANCH
                    && let Some(sid) = main_points_to
                {
                    Some((Ref::DEFAULT_BRANCH, sid.clone()))
                } else if requested_snaps.contains(&sid) {
                    Some((name, sid))
                } else {
                    None
                }
            });
            let snapshots: Vec<_> = original.all_snapshots()?.try_collect()?;
            RepoInfo::new(
                tags,
                branches,
                std::iter::empty(),
                snapshots,
                &UpdateType::RepoInitializedUpdate,
                None,
            )
        }
    }
}

#[cfg(test)]
mod tests {}
