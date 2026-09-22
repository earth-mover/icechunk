//! Garbage collection to remove unreferenced data.

use std::{
    collections::{HashMap, HashSet},
    num::{NonZeroU16, NonZeroUsize},
    pin::pin,
    sync::Arc,
};

use backon::{BackoffBuilder as _, ExponentialBuilder, Retryable as _};
use chrono::{DateTime, TimeDelta, Utc};
use futures::{Stream, StreamExt as _, TryStreamExt as _, stream};
use itertools::Itertools as _;
use tokio::task::JoinSet;
use tracing::{debug, error, info, instrument, trace, warn};

use crate::{
    StorageError,
    asset_manager::AssetManager,
    config::RepoUpdateRetryConfig,
    format::{
        CHUNKS_FILE_PATH, ChunkId, IcechunkFormatError, IcechunkResult,
        MANIFESTS_FILE_PATH, ManifestId, SNAPSHOTS_FILE_PATH, SnapshotId,
        TRANSACTION_LOGS_FILE_PATH,
        format_constants::SpecVersionBin,
        manifest::{ChunkPayload, Manifest},
        repo_info::{RepoAvailability, RepoInfo, UpdateInfo, UpdateType},
        snapshot::{Snapshot, SnapshotInfo},
    },
    ops::{
        pointed_snapshots, reachable_snapshots_v2,
        sharded_set::ChunkIdSet,
        walker::{ManifestConsumer, WalkLimits, walk_manifests},
    },
    refs::{Ref, RefError},
    repository::{RepositoryError, RepositoryErrorKind, RepositoryResult},
    storage::{self, DeleteObjectsResult, ListInfo},
};
use icechunk_types::{ICResultExt as _, error::ICResultCtxExt as _};

#[derive(Debug, PartialEq, Eq)]
pub enum Action {
    Keep,
    DeleteIfCreatedBefore(DateTime<Utc>),
}

impl Action {
    fn deletes(&self, created_at: DateTime<Utc>) -> bool {
        match self {
            Action::DeleteIfCreatedBefore(before) => {
                created_entirely_before(created_at, *before)
            }
            Action::Keep => false,
        }
    }
}

#[derive(Debug)]
pub struct GCConfig {
    extra_roots: HashSet<SnapshotId>,
    dangling_chunks: Action,
    dangling_manifests: Action,
    dangling_attributes: Action,
    dangling_transaction_logs: Action,
    dangling_snapshots: Action,

    max_snapshots_in_memory: NonZeroU16,
    max_compressed_manifest_mem_bytes: NonZeroUsize,
    max_concurrent_manifest_fetches: NonZeroU16,
    max_concurrent_deletes: NonZeroU16,
    max_consecutive_delete_failures: NonZeroU16,

    dry_run: bool,
}

impl GCConfig {
    #[expect(clippy::too_many_arguments)]
    pub fn new(
        extra_roots: HashSet<SnapshotId>,
        dangling_chunks: Action,
        dangling_manifests: Action,
        dangling_attributes: Action,
        dangling_transaction_logs: Action,
        dangling_snapshots: Action,
        max_snapshots_in_memory: NonZeroU16,
        max_compressed_manifest_mem_bytes: NonZeroUsize,
        max_concurrent_manifest_fetches: NonZeroU16,
        max_concurrent_deletes: NonZeroU16,
        max_consecutive_delete_failures: NonZeroU16,
        dry_run: bool,
    ) -> Self {
        GCConfig {
            extra_roots,
            dangling_chunks,
            dangling_manifests,
            dangling_attributes,
            dangling_transaction_logs,
            dangling_snapshots,
            max_snapshots_in_memory,
            max_compressed_manifest_mem_bytes,
            max_concurrent_manifest_fetches,
            max_concurrent_deletes,
            max_consecutive_delete_failures,
            dry_run,
        }
    }
    #[expect(clippy::too_many_arguments)]
    pub fn clean_all(
        chunks_age: DateTime<Utc>,
        metadata_age: DateTime<Utc>,
        extra_roots: Option<HashSet<SnapshotId>>,
        max_snapshots_in_memory: NonZeroU16,
        max_compressed_manifest_mem_bytes: NonZeroUsize,
        max_concurrent_manifest_fetches: NonZeroU16,
        max_concurrent_deletes: NonZeroU16,
        max_consecutive_delete_failures: NonZeroU16,
        dry_run: bool,
    ) -> Self {
        use Action::DeleteIfCreatedBefore as D;
        Self::new(
            extra_roots.unwrap_or_default(),
            D(chunks_age),
            D(metadata_age),
            D(metadata_age),
            D(metadata_age),
            D(metadata_age),
            max_snapshots_in_memory,
            max_compressed_manifest_mem_bytes,
            max_concurrent_manifest_fetches,
            max_concurrent_deletes,
            max_consecutive_delete_failures,
            dry_run,
        )
    }

    pub fn action_needed(&self) -> bool {
        [
            &self.dangling_chunks,
            &self.dangling_manifests,
            &self.dangling_attributes,
            &self.dangling_transaction_logs,
            &self.dangling_snapshots,
        ]
        .into_iter()
        .any(|action| action != &Action::Keep)
    }

    pub fn deletes_chunks(&self) -> bool {
        self.dangling_chunks != Action::Keep
    }

    pub fn deletes_manifests(&self) -> bool {
        self.dangling_manifests != Action::Keep
    }

    pub fn deletes_attributes(&self) -> bool {
        self.dangling_attributes != Action::Keep
    }

    pub fn deletes_transaction_logs(&self) -> bool {
        self.dangling_transaction_logs != Action::Keep
    }

    pub fn deletes_snapshots(&self) -> bool {
        self.dangling_snapshots != Action::Keep
    }

    fn must_delete_chunk(&self, chunk: &ListInfo<ChunkId>) -> bool {
        self.dangling_chunks.deletes(chunk.created_at)
    }

    fn must_delete_manifest(&self, manifest: &ListInfo<ManifestId>) -> bool {
        self.dangling_manifests.deletes(manifest.created_at)
    }

    fn must_delete_snapshot(&self, snapshot: &ListInfo<SnapshotId>) -> bool {
        self.dangling_snapshots.deletes(snapshot.created_at)
    }

    fn must_delete_transaction_log(&self, tx_log: &ListInfo<SnapshotId>) -> bool {
        self.dangling_transaction_logs.deletes(tx_log.created_at)
    }
}

/// Decides if the object's write instant precedes `cutoff` with certainty.
///
/// A store can floor the listed timestamp to a whole second. Tigris does this.
/// The write then falls anywhere in `[created_at, created_at + 1s)`.
/// GC deletes the object only if that whole interval precedes the cutoff.
/// A looser rule deletes objects that a caller wrote after the cutoff.
fn created_entirely_before(created_at: DateTime<Utc>, cutoff: DateTime<Utc>) -> bool {
    if created_at.timestamp_subsec_nanos() == 0 {
        created_at + TimeDelta::seconds(1) <= cutoff
    } else {
        created_at < cutoff
    }
}

#[derive(Debug, PartialEq, Eq, Default)]
pub struct GCSummary {
    pub bytes_deleted: u64,
    pub chunks_deleted: u64,
    pub manifests_deleted: u64,
    pub snapshots_deleted: u64,
    pub attributes_deleted: u64,
    pub transaction_logs_deleted: u64,
    /// Objects whose delete request failed; they stay garbage for the next run.
    pub objects_failed_to_delete: u64,
    /// First distinct delete error messages, at most 10.
    pub delete_errors: Vec<String>,
    /// Delete phases not run because an earlier phase had failed deletes, in
    /// order. GC deletes one kind of object per phase: snapshots, then
    /// transaction logs, manifests and chunks, each kind only after the kind
    /// that references it. Values: `transaction_logs`, `manifests`, `chunks`.
    pub skipped_phases: Vec<String>,
}

#[derive(Debug, thiserror::Error)]
pub enum GCError {
    #[error("ref error {0}")]
    Ref(#[from] RefError),
    #[error("repository error {0}")]
    Repository(#[from] RepositoryError),
    #[error("format error {0}")]
    FormatError(#[from] IcechunkFormatError),
    #[error("storage error {0}")]
    StorageError(#[from] StorageError),
    #[error("too many consecutive delete failures under {prefix}: {last_error}")]
    DeletesFailing { prefix: String, last_error: String },
}

pub type GCResult<A> = Result<A, GCError>;

/// Collects the ids of every native chunk any visited manifest references.
#[derive(Default)]
struct RetainedChunks {
    retained: ChunkIdSet,
}

impl ManifestConsumer for RetainedChunks {
    type Output = ();
    type Acc = ();

    fn consume(&self, manifest: &Manifest) -> RepositoryResult<()> {
        // a payload we cannot read is a chunk we cannot prove retained:
        // fail instead of deleting it
        let ids =
            manifest.chunk_payloads().inject()?.filter_map(|payload| match payload {
                Ok(ChunkPayload::Ref(chunk_ref)) => Some(Ok((chunk_ref.id, 0))),
                Ok(_) => None,
                Err(err) => Some(Err(err)),
            });
        self.retained.try_extend_weighted(ids).inject()?;
        Ok(())
    }

    // Nothing to fold: `consume` already inserts every id into the shared
    // sharded set from the decode workers in parallel.
    fn fold(_acc: &mut (), _output: ()) {}
}

#[instrument(skip_all)]
pub async fn find_retained(
    asset_manager: Arc<AssetManager>,
    config: &GCConfig,
    snaps: impl Stream<Item = RepositoryResult<Arc<Snapshot>>>,
) -> GCResult<(ChunkIdSet, HashSet<ManifestId>, HashSet<SnapshotId>)> {
    let limits = WalkLimits {
        max_concurrent_manifest_fetches: config.max_concurrent_manifest_fetches,
        max_manifest_mem_bytes: config.max_compressed_manifest_mem_bytes,
        decode_workers: NonZeroU16::new(asset_manager.max_concurrent_decodes())
            .unwrap_or(NonZeroU16::MIN),
    };
    let consumer = Arc::new(RetainedChunks::default());
    let result =
        walk_manifests(asset_manager, limits, Arc::clone(&consumer), snaps).await?;
    // the workers have all exited, so ours is the last reference
    let retained = Arc::try_unwrap(consumer)
        .map_err(|_| {
            RepositoryError::capture(RepositoryErrorKind::Other(
                "manifest walker still holds the consumer".to_string(),
            ))
        })?
        .retained;
    Ok((retained, result.manifests, result.snapshots))
}

pub async fn garbage_collect(
    asset_manager: Arc<AssetManager>,
    config: &GCConfig,
    repo_update_retries: Option<&RepoUpdateRetryConfig>,
    num_updates_per_repo_info_file: u16,
) -> GCResult<GCSummary> {
    if !asset_manager.can_write_to_storage().await? {
        return Err(RepositoryErrorKind::ReadonlyStorage(
            "Cannot garbage collect".to_string(),
        ))
        .capture()
        .map_err(GCError::Repository)?;
    }

    // Check repo status (only available on IC2+)
    if asset_manager.spec_version() >= SpecVersionBin::V2 {
        let (repo_info, _) = asset_manager.fetch_repo_info().await?;
        if repo_info.status()?.availability != RepoAvailability::Online {
            return Err(RepositoryErrorKind::ReadonlyRepository(
                "Cannot garbage collect".to_string(),
            ))
            .capture()
            .map_err(GCError::Repository)?;
        }
    }

    let default_retry_config = RepoUpdateRetryConfig::default();
    let retry_config = repo_update_retries.unwrap_or(&default_retry_config).retries();

    let gc = async || {
        garbage_collect_one_attempt(
            Arc::clone(&asset_manager),
            config,
            num_updates_per_repo_info_file,
        )
        .await
    };

    let backoff = ExponentialBuilder::new()
        .with_min_delay(std::time::Duration::from_millis(
            retry_config.initial_backoff_ms() as u64,
        ))
        .with_max_delay(std::time::Duration::from_millis(
            retry_config.max_backoff_ms() as u64
        ))
        .with_max_times(retry_config.max_tries().get() as usize)
        .with_jitter()
        .build();

    gc.retry(backoff)
        .sleep(tokio::time::sleep)
        .when(|e| {
            matches!(
                e,
                GCError::Repository(RepositoryError {
                    kind: RepositoryErrorKind::RepoInfoUpdated,
                    ..
                })
            )
        })
            .notify(|_, _|  {

                    info!(
                        "Repo info object was updated while GC was running, retrying with backoff..."
                    );}
        )
        .await
}

#[instrument(skip_all)]
async fn garbage_collect_one_attempt(
    asset_manager: Arc<AssetManager>,
    config: &GCConfig,
    num_updates_per_repo_info_file: u16,
) -> GCResult<GCSummary> {
    // TODO: this function could have much more parallelism
    if !config.action_needed() {
        info!("No action requested");
        return Ok(GCSummary::default());
    }

    info!("Finding GC roots");
    let snap_deadline =
        if let Action::DeleteIfCreatedBefore(date_time) = config.dangling_snapshots {
            date_time
        } else {
            DateTime::<Utc>::MIN_UTC
        };

    let mut non_pointed_but_new = HashSet::new();

    // The snapshot objects in storage, listed once and reused by the delete
    // pass below. Values are each object's `(created_at, size_bytes)`: the
    // first decides retention and the physical delete, the second only feeds
    // the summary's byte count.
    // `None` when we never listed them: V1 repos, which don't need the
    // retention check, and runs that keep snapshots, which don't delete any.
    let mut listed_snaps: Option<HashMap<SnapshotId, (DateTime<Utc>, u64)>> = None;

    let mut all_snaps = HashSet::new();
    let repo_info = if asset_manager.spec_version() > SpecVersionBin::V1 {
        // The retention decision must use the same clock as the physical delete
        // (storage created_at, see must_delete_snapshot). Judging it by flushed_at
        // half-deletes a snapshot in the (flushed_at, created_at] window: it is
        // dropped from the repo info (losing its pruned_ancestor_tx_logs
        // references) while its file survives.
        if config.deletes_snapshots() {
            listed_snaps = Some(
                asset_manager
                    .list_snapshots()
                    .await?
                    .map_ok(|s| (s.id, (s.created_at, s.size_bytes)))
                    .try_collect()
                    .await?,
            );
        }
        let (ri, _) = asset_manager.fetch_repo_info().await?;
        let reachable = reachable_snapshots_v2(ri.as_ref(), &config.extra_roots)?;
        non_pointed_but_new = ri
            .all_snapshots()?
            .filter_map_ok(|si| {
                all_snaps.insert(si.id.clone());
                if reachable.contains(&si.id) {
                    return None;
                }
                // A snapshot not visible in the listing yet cannot be deleted by
                // this run either, so it is retained.
                let old_enough_to_drop = listed_snaps.as_ref().is_some_and(|listed| {
                    listed.get(&si.id).is_some_and(|(created_at, _)| {
                        created_entirely_before(*created_at, snap_deadline)
                    })
                });
                if old_enough_to_drop { None } else { Some(si.id) }
            })
            .try_collect()?;

        Some(ri)
    } else {
        None
    };

    let pointed_snaps = pointed_snapshots(
        Arc::clone(&asset_manager),
        repo_info.clone(),
        &config.extra_roots,
        config.max_snapshots_in_memory,
    )
    .await?;
    let am = Arc::clone(&asset_manager);
    let non_pointed_snaps = stream::iter(non_pointed_but_new)
        .map(move |id| {
            let am = Arc::clone(&am);
            async move { am.fetch_snapshot(&id).await }
        })
        .buffer_unordered(config.max_snapshots_in_memory.get() as usize);

    let (keep_chunks, keep_manifests, mut keep_snapshots) = find_retained(
        Arc::clone(&asset_manager),
        config,
        pointed_snaps.chain(non_pointed_snaps),
    )
    .await?;

    info!(
        snapshots = keep_snapshots.len(),
        manifests = keep_manifests.len(),
        chunks = keep_chunks.len(),
        "Retained objects collected"
    );

    let mut summary = GCSummary::default();
    let mut earlier_phase_failed = false;

    fn absorb_phase(
        summary: &mut GCSummary,
        report: DeleteReport,
        deleted_counter: fn(&mut GCSummary) -> &mut u64,
    ) -> bool {
        *deleted_counter(summary) += report.deleted_objects;
        summary.bytes_deleted += report.deleted_bytes;
        summary.objects_failed_to_delete += report.failed_objects;
        for message in report.errors {
            if summary.delete_errors.len() < MAX_REPORTED_DELETE_ERRORS
                && !summary.delete_errors.contains(&message)
            {
                summary.delete_errors.push(message);
            }
        }
        report.failed_objects > 0
    }

    info!("Starting deletes");

    let drop_snapshots = all_snaps.difference(&keep_snapshots).cloned().collect();

    let mut written_repo_info: Option<Arc<RepoInfo>> = None;
    if config.deletes_snapshots() {
        if !config.dry_run && repo_info.is_some() {
            written_repo_info = delete_snapshots_from_repo_info(
                asset_manager.as_ref(),
                &mut keep_snapshots,
                &drop_snapshots,
                num_updates_per_repo_info_file,
            )
            .await?;
        }
        debug!("Garbage collecting snapshots");
        let report = match listed_snaps.take() {
            Some(listed) => {
                let candidates = stream::iter(listed.into_iter().map(
                    |(id, (created_at, size_bytes))| {
                        Ok(ListInfo { id, created_at, size_bytes })
                    },
                ));
                gc_snapshots(asset_manager.as_ref(), config, &keep_snapshots, candidates)
                    .await?
            }
            None => {
                let candidates = asset_manager.list_snapshots().await?;
                gc_snapshots(asset_manager.as_ref(), config, &keep_snapshots, candidates)
                    .await?
            }
        };
        earlier_phase_failed |=
            absorb_phase(&mut summary, report, |s| &mut s.snapshots_deleted);
    }
    drop(drop_snapshots);
    drop(all_snaps);

    // FIXME: with `dangling_snapshots == Action::Keep` but manifests or tx logs set to
    // delete, `keep_snapshots` covers only snapshots listed in repo info. A snapshot
    // *object* that an earlier run failed to delete is already out of repo info, so its
    // manifests and tx log get deleted here while the object survives
    if config.deletes_transaction_logs() {
        if earlier_phase_failed {
            summary.skipped_phases.push("transaction_logs".to_string());
        } else {
            // We need to retain tx logs of snapshots if any surviving
            // snapshot still references them in pruned_ancestor_tx_logs.
            // So keep_tx_logs is keep_snapshots plus those ids.
            let mut keep_tx_logs = keep_snapshots.clone();

            // use the most up to date repo info available
            let pruned_source = written_repo_info.as_ref().or(repo_info.as_ref());

            if let Some(repo_info) = pruned_source {
                let pruned = repo_info
                    .all_snapshots()?
                    .map_ok(|si| si.pruned_ancestor_tx_logs)
                    .flatten_ok();
                itertools::process_results(pruned, |ids| keep_tx_logs.extend(ids))?;
            }
            let report =
                gc_transaction_logs(asset_manager.as_ref(), config, &keep_tx_logs)
                    .await?;
            earlier_phase_failed |=
                absorb_phase(&mut summary, report, |s| &mut s.transaction_logs_deleted);
        }
    }
    if config.deletes_manifests() {
        if earlier_phase_failed {
            summary.skipped_phases.push("manifests".to_string());
        } else {
            let report =
                gc_manifests(asset_manager.as_ref(), config, &keep_manifests).await?;
            earlier_phase_failed |=
                absorb_phase(&mut summary, report, |s| &mut s.manifests_deleted);
        }
    }
    if config.deletes_chunks() {
        if earlier_phase_failed {
            summary.skipped_phases.push("chunks".to_string());
        } else {
            asset_manager.clear_chunk_cache();
            let report = gc_chunks(asset_manager.as_ref(), config, &keep_chunks).await?;
            absorb_phase(&mut summary, report, |s| &mut s.chunks_deleted);
        }
    }

    Ok(summary)
}

/// Updates the repo object eliminating snapshots.
///
/// On success, returns the `RepoInfo` that was actually written or
/// `None` if no update was written.
///
/// There are a few complex cases:
///
/// 1. A `reset_branch` operation may generate a snapshot we want to retain (because it's new),
///    with a parent (that is old) we want to drop. We re-parent it over the dropped run to its
///    nearest retained ancestor (falling back to `INITIAL_SNAPSHOT_ID` if none survive)
///    collecting the dropped ancestors' tx logs into its `pruned_ancestor_tx_logs`
///    Example:
///
///      `INITIAL_SNAPHOT_ID` -> snap0 -> snap1 -> snap2 -> snap3 -> snap4 -> snap5 (main)
///
///      Then:
///      - `reset_branch`("main", snap1) -> main now points at snap1; snap[2..5] are unreachable.
///      - GC with cutoff before = `snap3.flushed_at`.
///      - snap3, 4, and 5 are too new for GC so they are retained
///      - snap0 and 1 are pointed, so they are retained too
///      - snap2 is garbage collected (old and unreachable)
///      - We are left with a dropped interior gap with a kept ancestor below it:
///        snap1  (kept: reachable)
///        snap2  (DROPPED)
///        snap3  (kept: too new)
///      - In this case the function will assign snap 1 as parent of snap 3
///
/// 2. There may be new snapshots in the repo info object since we started GC
///    a.  New snapshots with parents not in `drop_snapshots` can be retained (their manifests and
///    chunks are new so they won't be deleted)
///    b. New snapshots with parents in `drop_snapshot` means we need to restart GC to rebuild the tree
///    of pointed snaps.
/// 3. Branches or tags pointing to drop snapshots must generate a retry
///
/// How to distinguish 1 from 2b: snapshots in 1. are in `retain_snapshots` but not in
/// `drop_snapshots`; snapshots in 2b are in neither map.
///
/// It adds any new snapshots that must be kept to `keep_snapshots`
async fn delete_snapshots_from_repo_info(
    asset_manager: &AssetManager,
    keep_snapshots: &mut HashSet<SnapshotId>,
    drop_snapshots: &HashSet<SnapshotId>,
    num_updates_per_repo_info_file: u16,
) -> GCResult<Option<Arc<RepoInfo>>> {
    trace!("deleting snapshots from repo info");
    let mut written_repo_info: Option<Arc<RepoInfo>> = None;
    let do_update = |repo_info: Arc<RepoInfo>, backup_path: &str, _| {
        let mut final_snaps = HashSet::with_capacity(2 * keep_snapshots.len());
        for si in repo_info.all_snapshots().inject()? {
            let si = si.inject()?;

            #[expect(clippy::panic)]
            match (keep_snapshots.contains(&si.id), drop_snapshots.contains(&si.id)) {
                (true, false) => {
                    // a snapshot that we explicitly want to keep
                    if let Some(parent) = &si.parent_id
                        && drop_snapshots.contains(parent)
                    {
                        // case 1 in the documentation: this kept snapshot's
                        // parent is being GC-ed.
                        // Re-parent it to its nearest retained ancestor,
                        // harvesting the dropped ancestors' tx logs into pruned_ancestor_tx_logs.
                        // If no ancestor survives we fall back to INITIAL_SNAPSHOT_ID.
                        let (new_parent, pruned_ancestor_tx_logs) =
                            reparent_and_prune(&repo_info, &si, |a| {
                                keep_snapshots.contains(&a.id)
                            })
                            .inject()?;
                        final_snaps.insert(SnapshotInfo {
                            parent_id: Some(
                                new_parent.unwrap_or(Snapshot::INITIAL_SNAPSHOT_ID),
                            ),
                            pruned_ancestor_tx_logs,
                            ..si
                        });
                    } else {
                        final_snaps.insert(si);
                    }
                }
                (false, true) => {
                    // a snapshot that we explicitly want to drop
                    // we don't need to worry about its children because they are taking cared of
                    // in the previous branch
                    //
                    // we don't need to add to final_snaps, we are dropping it
                }
                (false, false) => {
                    // this is a new snapshot
                    if let Some(parent) = &si.parent_id
                        && drop_snapshots.contains(parent)
                    {
                        // this is a new snapshot created since we started GC
                        // but we are trying to drop its parent. Case 2b
                        return Err(RepositoryError::capture(
                            RepositoryErrorKind::RepoInfoUpdated,
                        ));
                    } else {
                        // a new snapshot with the root as parent or with a parent we don't want to drop
                        // root is always retained
                        keep_snapshots.insert(si.id.clone());
                        final_snaps.insert(si);
                    }
                }
                (true, true) => {
                    panic!("Logic error, snapshot must be both retained and deleted")
                }
            }
        }

        // TODO: quite inefficient
        let final_snap_ids: HashSet<_> = final_snaps.iter().map(|si| &si.id).collect();
        for (_, pointed_snap) in
            repo_info.tags().inject()?.chain(repo_info.branches().inject()?)
        {
            if !final_snap_ids.contains(&pointed_snap) {
                return Err(RepositoryError::capture(
                    RepositoryErrorKind::RepoInfoUpdated,
                ));
            }
        }

        let config_bytes = repo_info.config_bytes_raw().inject()?;
        let new_repo_info = RepoInfo::new(
            asset_manager.spec_version(),
            repo_info.tags().inject()?,
            repo_info.branches().inject()?,
            repo_info.deleted_tags().inject()?,
            final_snaps,
            &repo_info.metadata().inject()?,
            UpdateInfo {
                update_type: UpdateType::GCRanUpdate,
                update_time: Utc::now(),
                previous_updates: repo_info.latest_updates().inject()?,
            },
            Some(backup_path),
            num_updates_per_repo_info_file,
            repo_info.repo_before_updates().inject()?,
            config_bytes.as_deref(),
            repo_info.enabled_feature_flags().inject()?,
            repo_info.disabled_feature_flags().inject()?,
            &repo_info.status().inject()?,
        )
        .inject()?;

        let new_repo_info = Arc::new(new_repo_info);
        written_repo_info = Some(Arc::clone(&new_repo_info));
        Ok(new_repo_info)
    };

    let retry_settings = storage::RetriesSettings {
        max_tries: Some(NonZeroU16::MIN),
        ..Default::default()
    };
    let _ = asset_manager.update_repo_info(&retry_settings, do_update).await?;

    Ok(written_repo_info)
}

const DELETE_BATCH_SIZE: NonZeroUsize = NonZeroUsize::new(1_000).unwrap();
const MAX_REPORTED_DELETE_ERRORS: usize = 10;

/// Outcome of one delete phase. A phase deletes every garbage object of one
/// kind; GC runs one per kind in dependency order (snapshots, transaction
/// logs, manifests, chunks) so nothing is deleted before what references it.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct DeleteReport {
    pub deleted_objects: u64,
    pub deleted_bytes: u64,
    pub failed_objects: u64,
    /// First distinct error messages, at most `MAX_REPORTED_DELETE_ERRORS`.
    pub errors: Vec<String>,
}

impl DeleteReport {
    fn record_success(&mut self, result: &DeleteObjectsResult) {
        self.deleted_objects += result.deleted_objects;
        self.deleted_bytes += result.deleted_bytes;
    }

    fn record_failure(&mut self, failed_objects: u64, message: &str) {
        self.failed_objects += failed_objects;
        if self.errors.len() < MAX_REPORTED_DELETE_ERRORS
            && !self.errors.iter().any(|m| m == message)
        {
            self.errors.push(message.to_string());
        }
    }

    fn record_dry_run(&mut self, batch: &[(String, u64)]) {
        self.deleted_objects += batch.len() as u64;
        self.deleted_bytes += batch.iter().map(|(_, size)| size).sum::<u64>();
    }
}

/// What one delete batch task returns: how many keys it was asked to delete, and the backend's answer.
type BatchOutcome = (usize, Result<DeleteObjectsResult, StorageError>);

/// Record one finished batch into the phase's [`DeleteReport`], and stop the
/// phase once `threshold` batches in a row have failed. `consecutive_failures`
/// is control state for the phase, not part of its reported outcome, so it
/// lives outside the report.
fn absorb_batch(
    report: &mut DeleteReport,
    consecutive_failures: &mut u32,
    prefix: &str,
    threshold: u32,
    joined: Result<BatchOutcome, tokio::task::JoinError>,
) -> GCResult<()> {
    let (batch_len, outcome) = joined.capture().map_err(GCError::Repository)?;
    // (objects not deleted, why); `None` when the backend deleted the whole batch
    let failure = match outcome {
        Ok(result) => {
            report.record_success(&result);
            // Backends answer `Ok` with fewer deleted objects
            // than requested when storage reports per-key errors inside the
            // response. Later phases must not delete their dependents.
            let shortfall = (batch_len as u64).saturating_sub(result.deleted_objects);
            (shortfall > 0).then(|| {
                warn!(
                    prefix,
                    batch_len,
                    deleted = result.deleted_objects,
                    "delete batch partially failed"
                );
                (
                    shortfall,
                    format!(
                        "{shortfall} of {batch_len} objects not deleted (per-key errors reported by storage)"
                    ),
                )
            })
        }
        Err(error) => {
            error!(prefix, batch_len, error = %error, "delete batch failed");
            Some((batch_len as u64, error.to_string()))
        }
    };
    match failure {
        None => *consecutive_failures = 0,
        Some((failed_objects, message)) => {
            report.record_failure(failed_objects, &message);
            *consecutive_failures += 1;
            if *consecutive_failures >= threshold {
                return Err(GCError::DeletesFailing {
                    prefix: prefix.to_string(),
                    last_error: message,
                });
            }
        }
    }
    Ok(())
}

/// Delete every candidate accepted by `should_delete`, in batches of
/// `batch_size`, with at most `max_concurrent_deletes` requests in flight.
/// Failed batches are recorded and the phase continues, unless
/// `max_consecutive_delete_failures` batches fail in a row.
async fn delete_listed<Id, S>(
    asset_manager: &AssetManager,
    config: &GCConfig,
    prefix: &'static str,
    batch_size: NonZeroUsize,
    candidates: S,
    should_delete: impl Fn(&ListInfo<Id>) -> bool,
) -> GCResult<DeleteReport>
where
    Id: std::fmt::Display,
    S: Stream<Item = RepositoryResult<ListInfo<Id>>>,
{
    let storage = Arc::clone(asset_manager.storage());
    let settings = asset_manager.storage_settings().clone();
    let max_in_flight = config.max_concurrent_deletes.get() as usize;
    let threshold = config.max_consecutive_delete_failures.get() as u32;

    let mut report = DeleteReport::default();
    let mut consecutive_failures = 0u32;
    let mut in_flight: JoinSet<BatchOutcome> = JoinSet::new();

    let submit = |batch: Vec<(String, u64)>,
                  in_flight: &mut JoinSet<BatchOutcome>,
                  report: &mut DeleteReport| {
        if config.dry_run {
            report.record_dry_run(&batch);
            return;
        }
        let storage = Arc::clone(&storage);
        let settings = settings.clone();
        let batch_len = batch.len();
        in_flight.spawn(async move {
            (batch_len, storage.delete_batch(&settings, prefix, batch).await)
        });
    };

    let mut batch: Vec<(String, u64)> = Vec::with_capacity(batch_size.get());
    let mut candidates = pin!(candidates);
    while let Some(candidate) = candidates.next().await {
        let candidate = candidate?;
        if !should_delete(&candidate) {
            continue;
        }
        batch.push((candidate.id.to_string(), candidate.size_bytes));
        if batch.len() == batch_size.get() {
            while in_flight.len() >= max_in_flight {
                if let Some(joined) = in_flight.join_next().await {
                    absorb_batch(
                        &mut report,
                        &mut consecutive_failures,
                        prefix,
                        threshold,
                        joined,
                    )?;
                }
            }
            submit(std::mem::take(&mut batch), &mut in_flight, &mut report);
        }
    }
    if !batch.is_empty() {
        submit(batch, &mut in_flight, &mut report);
    }
    while let Some(joined) = in_flight.join_next().await {
        absorb_batch(&mut report, &mut consecutive_failures, prefix, threshold, joined)?;
    }
    Ok(report)
}

#[instrument(skip(asset_manager, config, keep_ids), fields(keep_ids.len = keep_ids.len()))]
pub async fn gc_chunks(
    asset_manager: &AssetManager,
    config: &GCConfig,
    keep_ids: &ChunkIdSet,
) -> GCResult<DeleteReport> {
    info!("Deleting chunks");
    let candidates = asset_manager.list_chunks().await?;
    delete_listed(
        asset_manager,
        config,
        CHUNKS_FILE_PATH,
        DELETE_BATCH_SIZE,
        candidates,
        |chunk| config.must_delete_chunk(chunk) && !keep_ids.contains(&chunk.id),
    )
    .await
}

#[instrument(skip(asset_manager, config, keep_ids), fields(keep_ids.len = keep_ids.len()))]
pub async fn gc_manifests(
    asset_manager: &AssetManager,
    config: &GCConfig,
    keep_ids: &HashSet<ManifestId>,
) -> GCResult<DeleteReport> {
    info!("Deleting manifests");
    let candidates = asset_manager.list_manifests().await?;
    delete_listed(
        asset_manager,
        config,
        MANIFESTS_FILE_PATH,
        DELETE_BATCH_SIZE,
        candidates,
        |manifest| {
            let delete =
                config.must_delete_manifest(manifest) && !keep_ids.contains(&manifest.id);
            if delete {
                asset_manager.remove_cached_manifest(&manifest.id);
            }
            delete
        },
    )
    .await
}

/// `snapshots` are the delete candidates
#[instrument(skip(asset_manager, config, keep_ids, snapshots), fields(keep_ids.len = keep_ids.len()))]
pub async fn gc_snapshots(
    asset_manager: &AssetManager,
    config: &GCConfig,
    keep_ids: &HashSet<SnapshotId>,
    snapshots: impl Stream<Item = RepositoryResult<ListInfo<SnapshotId>>> + Send,
) -> GCResult<DeleteReport> {
    info!("Deleting snapshots");
    delete_listed(
        asset_manager,
        config,
        SNAPSHOTS_FILE_PATH,
        DELETE_BATCH_SIZE,
        snapshots,
        |snapshot| {
            let delete =
                config.must_delete_snapshot(snapshot) && !keep_ids.contains(&snapshot.id);
            if delete {
                asset_manager.remove_cached_snapshot(&snapshot.id);
            }
            delete
        },
    )
    .await
}

#[instrument(skip(asset_manager, config, keep_ids), fields(keep_ids.len = keep_ids.len()))]
pub async fn gc_transaction_logs(
    asset_manager: &AssetManager,
    config: &GCConfig,
    keep_ids: &HashSet<SnapshotId>,
) -> GCResult<DeleteReport> {
    info!("Deleting transaction logs");
    let candidates = asset_manager.list_transaction_logs().await?;
    delete_listed(
        asset_manager,
        config,
        TRANSACTION_LOGS_FILE_PATH,
        DELETE_BATCH_SIZE,
        candidates,
        |tx| {
            let delete =
                config.must_delete_transaction_log(tx) && !keep_ids.contains(&tx.id);
            if delete {
                asset_manager.remove_cached_tx_log(&tx.id);
            }
            delete
        },
    )
    .await
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum ExpiredRefAction {
    Delete,
    Ignore,
}

#[derive(Debug, PartialEq, Eq, Clone, Default)]
pub struct ExpireResult {
    pub released_snapshots: HashSet<SnapshotId>,
    pub edited_snapshots: HashSet<SnapshotId>,
    pub deleted_refs: HashSet<Ref>,
}

/// Expire all snapshots older than a threshold.
///
/// This processes snapshots found by navigating all references in
/// the repo, tags first, branches leter, both in lexicographical order.
///
/// The operation will edit in place the oldest non-expired snapshot,
/// in every ancestry, changing its parent to be the root of the repo.
///
/// For this reasons, it's recommended to invalidate any snapshot
/// caches before traversing history againg. The cache in the
/// passed `asset_manager` is invalidated here, but other caches
/// may exist, for example, in [`crate::Repository`] instances.
///
/// Notice that the snapshot returned as released, are not necessarily
/// available for garbage collection, they could still be pointed by
/// ether refs.
///
/// See: <https://github.com/earth-mover/icechunk/blob/main/design-docs/007-basic-expiration.md>
#[instrument(skip(asset_manager))]
pub async fn expire(
    asset_manager: Arc<AssetManager>,
    older_than: DateTime<Utc>,
    expired_branches: ExpiredRefAction,
    expired_tags: ExpiredRefAction,
    repo_update_retries: Option<&RepoUpdateRetryConfig>,
    num_updates_per_repo_info_file: u16,
) -> GCResult<ExpireResult> {
    if !asset_manager.can_write_to_storage().await? {
        return Err(RepositoryErrorKind::ReadonlyStorage("Cannot expire".to_string()))
            .capture()
            .map_err(GCError::Repository)?;
    }

    // Check repo status (only available on IC2+)
    if asset_manager.spec_version() >= SpecVersionBin::V2 {
        let (repo_info, _) = asset_manager.fetch_repo_info().await?;
        if repo_info.status()?.availability != RepoAvailability::Online {
            return Err(RepositoryErrorKind::ReadonlyRepository(
                "Cannot garbage collect".to_string(),
            ))
            .capture()
            .map_err(GCError::Repository)?;
        }
    }

    match asset_manager.spec_version() {
        SpecVersionBin::V1 => {
            super::expiration_v1::expire(
                asset_manager,
                older_than,
                expired_branches,
                expired_tags,
            )
            .await
        }
        SpecVersionBin::V2 => {
            expire_v2(
                asset_manager,
                older_than,
                expired_branches,
                expired_tags,
                repo_update_retries,
                num_updates_per_repo_info_file,
            )
            .await
        }
    }
}

/// Since `expire_v2` is a relatively fast operation (repo object only) we retry it if the repo info
/// object was modified since it started
#[instrument(skip(asset_manager))]
pub async fn expire_v2(
    asset_manager: Arc<AssetManager>,
    older_than: DateTime<Utc>,
    expired_branches: ExpiredRefAction,
    expired_tags: ExpiredRefAction,
    repo_update_retries: Option<&RepoUpdateRetryConfig>,
    num_updates_per_repo_info_file: u16,
) -> GCResult<ExpireResult> {
    let default_retry_config = RepoUpdateRetryConfig::default();
    let retry_config = repo_update_retries.unwrap_or(&default_retry_config).retries();

    let backoff = ExponentialBuilder::new()
        .with_min_delay(std::time::Duration::from_millis(
            retry_config.initial_backoff_ms() as u64,
        ))
        .with_max_delay(std::time::Duration::from_millis(
            retry_config.max_backoff_ms() as u64
        ))
        .with_max_times(retry_config.max_tries().get() as usize)
        .with_jitter()
        .build();

    let expire = async || {
        expire_v2_one_attempt(
            Arc::clone(&asset_manager),
            older_than,
            expired_branches,
            expired_tags,
            num_updates_per_repo_info_file,
        )
        .await
    };

    expire.retry(backoff)
        .sleep(tokio::time::sleep)
        .when(|e| {
            matches!(
                e,
                GCError::Repository(RepositoryError {
                    kind: RepositoryErrorKind::RepoInfoUpdated,
                    ..
                })
            )
        })
            .notify(|_, _|  {

                    info!(
                        "Repo info object was updated while expire was running, retrying with backoff..."
                    );}
        )
        .await
}

/// Re-parent `edited` over a run of ancestors that are being expired,
/// harvesting their tx logs so its delta from the new parent stays complete.
///
/// Returns `(new_parent, pruned)`. `new_parent` is the boundary ancestor, or
/// `None` if the whole chain is collapsed (no ancestor satisfies `is_boundary`)
/// `pruned` lists, oldest first, every collapsed ancestor's id with that ancestor's own
/// `pruned_ancestor_tx_logs` spliced in, then `edited`'s own existing
/// `pruned_ancestor_tx_logs` (newer than the collapsed run) appended last.
///
/// A collapsed ancestor can itself carry a non-empty `pruned_ancestor_tx_logs`
/// (it was a boundary in an earlier pass), and several can sit in one chain.
/// Hitting one does not end the walk — only `is_boundary` does; its pruned chain
/// is spliced in and the walk keeps going. E.g. with boundary `s1`:
///
/// ```text
/// INITIAL → s1 → s3 (pruned=[s2]) → s5 (pruned=[s4]) → edited
/// ```
///
/// returns `new_parent = s1` and `pruned = [s2, s3, s4, s5]` (oldest first).
fn reparent_and_prune(
    repo_info: &RepoInfo,
    edited: &SnapshotInfo,
    is_boundary: impl Fn(&SnapshotInfo) -> bool,
) -> IcechunkResult<(Option<SnapshotId>, Vec<SnapshotId>)> {
    let mut new_parent = None;
    let mut collapsed = Vec::new();
    // ancestry() starts at `edited`. skip(1) drops it
    for ancestor in repo_info.ancestry(&edited.id)?.skip(1) {
        let ancestor = ancestor?;
        if is_boundary(&ancestor) {
            new_parent = Some(ancestor.id);
            break;
        }
        collapsed.push((ancestor.id, ancestor.pruned_ancestor_tx_logs));
    }
    // Emit oldest first. `collapsed` is newest first, so walk it in reverse; an
    // ancestor's own pruned logs (already oldest first) are older than it and so
    // precede its id.
    let mut pruned = Vec::new();
    for (id, ancestor_pruned) in collapsed.into_iter().rev() {
        pruned.extend(ancestor_pruned);
        pruned.push(id);
    }
    // `edited`'s own pruned logs are newer than the collapsed run, so append last.
    pruned.extend(edited.pruned_ancestor_tx_logs.iter().cloned());
    Ok((new_parent, pruned))
}

#[instrument(skip(asset_manager))]
async fn expire_v2_one_attempt(
    asset_manager: Arc<AssetManager>,
    older_than: DateTime<Utc>,
    expired_branches: ExpiredRefAction,
    expired_tags: ExpiredRefAction,
    num_updates_per_repo_info_file: u16,
) -> GCResult<ExpireResult> {
    info!("Expiration started");
    let (repo_info, repo_info_version_at_start) = asset_manager.fetch_repo_info().await?;
    let tags: Vec<(Ref, SnapshotId)> = repo_info
        .tags()?
        .map(|(name, snap)| Ok::<_, GCError>((Ref::Tag(name.to_string()), snap)))
        .try_collect()?;
    let branches: Vec<(Ref, SnapshotId)> = repo_info
        .branches()?
        .map(|(name, snap)| Ok::<_, GCError>((Ref::Branch(name.to_string()), snap)))
        .try_collect()?;

    fn split_root<E>(
        mut iter: impl Iterator<Item = Result<SnapshotInfo, E>>,
    ) -> Result<(HashSet<SnapshotId>, Option<SnapshotId>), E> {
        iter.try_fold((HashSet::new(), None), |(mut all, root), snap| match snap {
            Ok(snap) if snap.parent_id.is_some() => {
                all.insert(snap.id);
                Ok((all, root))
            }
            Ok(snap) => Ok((all, Some(snap.id))),
            Err(err) => Err(err),
        })
    }

    debug!("Finding roots");
    let mut all_tips = tags.iter().chain(branches.iter());
    let root_to_snaps = all_tips.try_fold(
        HashMap::new(),
        |mut res: HashMap<SnapshotId, HashSet<SnapshotId>>, (_, tip_snap)| {
            let ancestry = repo_info.ancestry(tip_snap)?;
            let (branch_snaps, root) = split_root(ancestry)?;
            let root = root.unwrap_or(Snapshot::INITIAL_SNAPSHOT_ID);
            match res.get_mut(&root) {
                Some(s) => {
                    s.extend(branch_snaps);
                }
                None => {
                    res.insert(root, branch_snaps);
                }
            };

            Ok::<_, GCError>(res)
        },
    )?;

    let new_parent = move |id: &SnapshotId| {
        for (new_parent, all) in root_to_snaps.iter() {
            if all.contains(id) {
                return Some(new_parent.clone());
            }
        }
        None
    };

    debug!("Finding ref tips");
    let tag_tip_ids: HashSet<SnapshotId> = repo_info.tags()?.map(|(_, id)| id).collect();
    let branch_tip_ids: HashSet<SnapshotId> =
        repo_info.branches()?.map(|(_, id)| id).collect();
    let main_pointee = repo_info.resolve_branch(Ref::DEFAULT_BRANCH)?;

    // All non-root snapshots old enough to be considered expired, regardless of
    // ref protection. Used to determine which branch/tag refs should be deleted.
    // Unlike released_snapshots, this does not exclude snapshots protected by
    // branch/tag tips (e.g. main), so a feature branch sharing main's tip can
    // still be deleted (#1520). Root snapshots (no parent) are always excluded
    // so tags/branches pointing to the initial commit are never deleted (#1534).
    let expired_snapshot_infos: Vec<SnapshotInfo> = repo_info
        .all_snapshots()?
        .filter_map(|si| match si {
            Ok(si) if si.flushed_at < older_than && si.parent_id.is_some() => {
                Some(Ok(si))
            }
            Ok(_) => None,
            Err(e) => Some(Err(e)),
        })
        .try_collect()?;

    debug!("Calculating released snapshots");
    let released_snapshots: HashSet<SnapshotId> = expired_snapshot_infos
        .iter()
        .filter_map(|si| {
            // we retain all roots
            if si.flushed_at < older_than && si.parent_id.is_some() {
                use ExpiredRefAction::*;
                if expired_tags == Ignore && tag_tip_ids.contains(&si.id)
                    || (expired_branches == Ignore || si.id == main_pointee)
                        && branch_tip_ids.contains(&si.id)
                {
                    None
                } else {
                    Some(si.id.clone())
                }
            } else {
                None
            }
        })
        .collect();

    let expired_snapshots: HashSet<SnapshotId> =
        expired_snapshot_infos.into_iter().map(|x| x.id).collect();

    let num_released_snapshots = released_snapshots.len();

    debug!("Calculating retained snapshots");
    let mut edited_snapshots = HashSet::new();
    let retained: Vec<_> = repo_info
        .all_snapshots()?
        .filter_map(|si| match si {
            // remove expired snapshots
            Ok(si) if released_snapshots.contains(&si.id) => None,

            // non expired snapshots could need editing to change their parent
            Ok(si) => match si.parent_id.as_ref() {
                Some(parent_id) => {
                    if released_snapshots.contains(parent_id) {
                        // parent is expired, so we change it to the root in that branch/tag
                        edited_snapshots.insert(si.id.clone());
                        // Re-parenting to the root drops every ancestor below
                        // si from its path. Those ancestors carry the tx logs
                        // describing the deltas si now spans (root..si), so
                        // harvest their ids into si.
                        match reparent_and_prune(&repo_info, &si, |a| {
                            // go all the way to the branch root
                            a.parent_id.is_none()
                        }) {
                            Ok((_, pruned_ancestor_tx_logs)) => Some(Ok(SnapshotInfo {
                                parent_id: Some(
                                    new_parent(&si.id)
                                        .unwrap_or(Snapshot::INITIAL_SNAPSHOT_ID),
                                ),
                                pruned_ancestor_tx_logs,
                                ..si
                            })),
                            Err(e) => Some(Err(e)),
                        }
                    } else {
                        // parent is retained, so we retain the snapshot as is
                        Some(Ok(si))
                    }
                }
                // we retain all roots
                None => Some(Ok(si)),
            },
            Err(e) => Some(Err(e)),
        })
        .try_collect()?;

    debug!("Calculating deleted refs");
    let mut deleted_tags: HashSet<_> = tags
        .into_iter()
        .filter_map(|(r, snap_id)| {
            if expired_tags == ExpiredRefAction::Delete
                && expired_snapshots.contains(&snap_id)
            {
                Some(r)
            } else {
                None
            }
        })
        .collect();

    let deleted_branches: HashSet<_> = branches
        .into_iter()
        .filter_map(|(r, snap_id)| {
            if expired_branches == ExpiredRefAction::Delete
                && r.name() != Ref::DEFAULT_BRANCH
                && expired_snapshots.contains(&snap_id)
            {
                Some(r)
            } else {
                None
            }
        })
        .collect();

    info!(
        snapshots = num_released_snapshots,
        branches = deleted_branches.iter().map(|r| r.name()).join("/"),
        tags = deleted_tags.iter().map(|r| r.name()).join("/"),
        "Releasing objects"
    );

    let do_update = |repo_info: Arc<RepoInfo>, backup_path: &str, version| {
        // we retry if the repo info object was modified since we started
        if version != repo_info_version_at_start {
            return Err(RepositoryError::capture(RepositoryErrorKind::RepoInfoUpdated));
        }

        let tags = repo_info
            .tags()
            .inject()?
            .filter(|(name, _)| !deleted_tags.contains(&Ref::Tag(name.to_string())));

        let branches = repo_info.branches().inject()?.filter(|(name, _)| {
            !deleted_branches.contains(&Ref::Branch(name.to_string()))
        });

        let deleted_tag_names = repo_info.deleted_tags().inject()?.chain(
            deleted_tags.iter().filter_map(|r| match r {
                Ref::Tag(name) => Some(name.as_str()),
                Ref::Branch(_) => None,
            }),
        );
        let config_bytes = repo_info.config_bytes_raw().inject()?;
        let new_repo_info = RepoInfo::new(
            asset_manager.spec_version(),
            tags,
            branches,
            deleted_tag_names,
            retained.clone(),
            &repo_info.metadata().inject()?,
            UpdateInfo {
                update_type: UpdateType::ExpirationRanUpdate,
                update_time: Utc::now(),
                previous_updates: repo_info.latest_updates().inject()?,
            },
            Some(backup_path),
            num_updates_per_repo_info_file,
            repo_info.repo_before_updates().inject()?,
            config_bytes.as_deref(),
            repo_info.enabled_feature_flags().inject()?,
            repo_info.disabled_feature_flags().inject()?,
            &repo_info.status().inject()?,
        )
        .inject()?;

        Ok(Arc::new(new_repo_info))
    };

    let retry_settings = storage::RetriesSettings {
        max_tries: Some(NonZeroU16::MIN),
        ..Default::default()
    };
    let _ = asset_manager.update_repo_info(&retry_settings, do_update).await?;

    deleted_tags.extend(deleted_branches);

    debug!("Expiration done");
    Ok(ExpireResult { released_snapshots, edited_snapshots, deleted_refs: deleted_tags })
}

#[cfg(test)]
mod tests {
    use std::{
        ops::Range,
        pin::Pin,
        sync::atomic::{AtomicUsize, Ordering},
    };

    use bytes::Bytes;
    use chrono::TimeZone as _;
    use futures::stream::BoxStream;
    use icechunk_macros::tokio_test;
    use icechunk_storage::sealed::Sealed;

    use super::*;
    use crate::{
        Storage,
        storage::{
            GetModifiedResult, RepositoryCreation, Settings, StorageErrorKind,
            StorageInfo, StorageResult, VersionInfo, VersionedUpdateResult,
        },
    };

    // `tokio_test` expands to nothing under shuttle, leaving the async tests
    // and everything only they use unreferenced.
    #[cfg(not(feature = "shuttle"))]
    use crate::{
        format::{CHUNKS_FILE_PATH, SNAPSHOTS_FILE_PATH},
        storage::new_in_memory_storage,
        test_utils::{logging_asset_manager, repo_with_converging_refs},
    };
    #[cfg(not(feature = "shuttle"))]
    use chrono::Duration;
    #[cfg(not(feature = "shuttle"))]
    use std::collections::HashMap as StdHashMap;

    /// GC must read each snapshot at most once. Snapshots newer than the
    /// metadata cutoff used to be fetched twice: once walking the ref
    /// ancestries and again as (supposedly) non-pointed but new.
    #[tokio_test]
    async fn test_gc_reads_each_snapshot_once() -> Result<(), Box<dyn std::error::Error>>
    {
        let backend: Arc<dyn Storage + Send + Sync> = new_in_memory_storage().await?;
        let repo = repo_with_converging_refs(&backend).await?;
        let (logging, asset_manager) = logging_asset_manager(
            &backend,
            repo.storage_settings().clone(),
            SpecVersionBin::V2,
        );

        // a cutoff in the past leaves every snapshot newer than the deadline,
        // which is what put them all in `non_pointed_but_new`
        let cutoff = Utc::now() - Duration::hours(1);
        let config = GCConfig::clean_all(
            cutoff,
            cutoff,
            None,
            NonZeroU16::new(10).unwrap(),
            NonZeroUsize::new(1_000_000_000).unwrap(),
            NonZeroU16::new(10).unwrap(),
            NonZeroU16::new(10).unwrap(),
            NonZeroU16::new(50).unwrap(),
            true,
        );
        garbage_collect(Arc::clone(&asset_manager), &config, None, 10).await?;

        let snapshot_prefix = format!("{SNAPSHOTS_FILE_PATH}/");
        let mut reads_per_snapshot: StdHashMap<String, usize> = StdHashMap::new();
        let mut snapshot_listings = 0;
        for (op, path) in logging.fetch_operations() {
            if matches!(op.as_str(), "list_objects" | "list_objects_with_id_first_chars")
            {
                if path == SNAPSHOTS_FILE_PATH {
                    snapshot_listings += 1;
                }
            } else if path.starts_with(&snapshot_prefix) {
                *reads_per_snapshot.entry(path).or_default() += 1;
            }
        }
        assert_eq!(snapshot_listings, 1, "the snapshots prefix was listed twice");
        let repeated: Vec<_> =
            reads_per_snapshot.iter().filter(|(_, n)| **n > 1).collect();
        assert!(repeated.is_empty(), "snapshots read more than once: {repeated:?}");
        // 5 commits plus the initial snapshot
        assert_eq!(reads_per_snapshot.len(), 6);
        Ok(())
    }

    fn at(secs: i64, nanos: u32) -> DateTime<Utc> {
        Utc.timestamp_opt(secs, nanos).unwrap()
    }

    /// A store that lists whole seconds floors `created_at`.
    /// The listing then reports a time before the write.
    #[test]
    fn whole_second_timestamps_are_kept_until_their_second_passes() {
        assert!(!created_entirely_before(at(100, 0), at(100, 400_000_000)));
        assert!(created_entirely_before(at(100, 0), at(101, 0)));
        assert!(created_entirely_before(at(100, 399_000_000), at(100, 400_000_000)));
        assert!(!created_entirely_before(at(100, 400_000_000), at(100, 400_000_000)));
    }

    /// How `FlakyDeletes::delete_batch` misbehaves under the failing prefix.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
    enum FailMode {
        /// every call under the failing prefix returns `Err`
        Always,
        /// every other call under the failing prefix returns `Err`
        Alternate,
        /// every call returns `Ok`, but reports one object fewer than requested
        ShortByOne,
    }

    /// What `delete_batch` should do with this call.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum Injected {
        Nothing,
        Fail,
        ReportOneFewer,
    }

    /// Delegates everything; `delete_batch` fails according to the mode, and
    /// listings under `list_error_prefix` break after their first item.
    #[derive(Debug, serde::Serialize, serde::Deserialize)]
    struct FlakyDeletes {
        backend: Arc<dyn Storage + Send + Sync>,
        #[serde(skip)]
        calls: AtomicUsize,
        /// prefix whose deletes misbehave (`None` = every prefix)
        failing_prefix: Option<String>,
        mode: FailMode,
        /// prefix whose listings yield an `Err` after their first item
        list_error_prefix: Option<String>,
    }

    impl FlakyDeletes {
        /// Only calls under the failing prefix are counted, so "every other
        /// call" means every other call *for that prefix*.
        fn injected(&self, prefix: &str) -> Injected {
            let matches = self.failing_prefix.as_deref().is_none_or(|p| prefix == p);
            if !matches {
                return Injected::Nothing;
            }
            let n = self.calls.fetch_add(1, Ordering::SeqCst);
            match self.mode {
                FailMode::Always => Injected::Fail,
                FailMode::Alternate if n.is_multiple_of(2) => Injected::Fail,
                FailMode::Alternate => Injected::Nothing,
                FailMode::ShortByOne => Injected::ReportOneFewer,
            }
        }
    }

    impl std::fmt::Display for FlakyDeletes {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "FlakyDeletes({})", self.backend)
        }
    }
    impl Sealed for FlakyDeletes {}

    #[async_trait::async_trait]
    #[typetag::serde]
    impl Storage for FlakyDeletes {
        fn storage_info(&self) -> StorageInfo {
            self.backend.storage_info()
        }

        async fn default_settings(&self) -> StorageResult<Settings> {
            self.backend.default_settings().await
        }

        async fn can_write(&self) -> StorageResult<bool> {
            self.backend.can_write().await
        }

        async fn can_create_repository(&self) -> StorageResult<RepositoryCreation> {
            self.backend.can_create_repository().await
        }

        async fn put_object(
            &self,
            settings: &Settings,
            path: &str,
            bytes: Bytes,
            content_type: Option<&str>,
            metadata: Vec<(String, String)>,
            previous_version: Option<&VersionInfo>,
        ) -> StorageResult<VersionedUpdateResult> {
            self.backend
                .put_object(
                    settings,
                    path,
                    bytes,
                    content_type,
                    metadata,
                    previous_version,
                )
                .await
        }

        async fn copy_object(
            &self,
            settings: &Settings,
            from: &str,
            to: &str,
            content_type: Option<&str>,
            version: &VersionInfo,
        ) -> StorageResult<VersionedUpdateResult> {
            self.backend.copy_object(settings, from, to, content_type, version).await
        }

        async fn list_objects<'a>(
            &'a self,
            settings: &Settings,
            prefix: &str,
        ) -> StorageResult<BoxStream<'a, StorageResult<ListInfo<String>>>> {
            self.backend.list_objects(settings, prefix).await
        }

        async fn list_objects_with_id_first_chars<'a>(
            &'a self,
            settings: &Settings,
            prefix: &str,
            first_chars: &HashSet<char>,
        ) -> StorageResult<BoxStream<'a, StorageResult<ListInfo<String>>>> {
            let listing = self
                .backend
                .list_objects_with_id_first_chars(settings, prefix, first_chars)
                .await?;
            if self.list_error_prefix.as_deref() != Some(prefix) {
                return Ok(listing);
            }
            let broken = stream::once(async {
                Err(StorageError::capture(StorageErrorKind::Other(
                    "injected listing failure".to_string(),
                )))
            });
            Ok(listing.take(1).chain(broken).boxed())
        }

        async fn delete_batch(
            &self,
            settings: &Settings,
            prefix: &str,
            batch: Vec<(String, u64)>,
        ) -> StorageResult<DeleteObjectsResult> {
            match self.injected(prefix) {
                Injected::Nothing => {
                    self.backend.delete_batch(settings, prefix, batch).await
                }
                Injected::Fail => Err(StorageError::capture(StorageErrorKind::Other(
                    "injected delete failure".to_string(),
                ))),
                // the objects really are deleted; we only misreport the count,
                // which is what S3 does from the caller's point of view when
                // one key in the batch errors
                Injected::ReportOneFewer => {
                    let result =
                        self.backend.delete_batch(settings, prefix, batch).await?;
                    Ok(DeleteObjectsResult {
                        deleted_objects: result.deleted_objects.saturating_sub(1),
                        deleted_bytes: result.deleted_bytes,
                    })
                }
            }
        }

        async fn get_object_last_modified(
            &self,
            path: &str,
            settings: &Settings,
        ) -> StorageResult<DateTime<Utc>> {
            self.backend.get_object_last_modified(path, settings).await
        }

        async fn get_object_conditional(
            &self,
            settings: &Settings,
            path: &str,
            previous_version: Option<&VersionInfo>,
        ) -> StorageResult<GetModifiedResult> {
            self.backend.get_object_conditional(settings, path, previous_version).await
        }

        async fn get_object_range(
            &self,
            settings: &Settings,
            path: &str,
            range: Option<&Range<u64>>,
        ) -> StorageResult<(
            Pin<Box<dyn Stream<Item = Result<Bytes, StorageError>> + Send>>,
            VersionInfo,
        )> {
            self.backend.get_object_range(settings, path, range).await
        }
    }

    #[cfg(not(feature = "shuttle"))]
    /// A V2 repo with exactly 3 garbage snapshots, 3 garbage tx logs,
    /// 1 garbage manifest and 4 garbage chunks.
    async fn repo_with_garbage(
        backend: &Arc<dyn Storage + Send + Sync>,
    ) -> Result<crate::Repository, Box<dyn std::error::Error>> {
        use crate::format::{ChunkIndices, Path, snapshot::ArrayShape};
        use Bytes;
        let repo = repo_with_converging_refs(backend).await?;
        let array_path: Path = "/arr".try_into()?;
        let mut session = repo.writable_session("main").await?;
        session
            .add_array(
                array_path.clone(),
                ArrayShape::new([(4, 4)]).unwrap(),
                None,
                Bytes::from_static(br#"{"zarr_format":3}"#),
            )
            .await?;
        for i in 0..4u32 {
            // above the inline threshold, so each chunk is its own object
            let payload =
                session.get_chunk_writer()?(Bytes::from(vec![i as u8; 1024])).await?;
            session
                .set_chunk_ref(array_path.clone(), ChunkIndices(vec![i]), Some(payload))
                .await?;
        }
        session.commit("c5").max_concurrent_nodes(8).execute().await?;

        let mid = repo.lookup_tag("mid").await?;
        repo.delete_tag("tip").await?;
        repo.delete_branch("other").await?;
        repo.reset_branch("main", &mid, None).await?;
        Ok(repo)
    }

    #[cfg(not(feature = "shuttle"))]
    fn gc_config(max_consecutive_delete_failures: u16) -> GCConfig {
        gc_config_with(max_consecutive_delete_failures, 2)
    }

    #[cfg(not(feature = "shuttle"))]
    fn gc_config_with(
        max_consecutive_delete_failures: u16,
        max_concurrent_deletes: u16,
    ) -> GCConfig {
        // cutoff in the future: everything unreachable is old enough
        let cutoff = Utc::now() + Duration::hours(1);
        GCConfig::clean_all(
            cutoff,
            cutoff,
            None,
            NonZeroU16::new(10).unwrap(),
            NonZeroUsize::new(1_000_000_000).unwrap(),
            NonZeroU16::new(10).unwrap(),
            NonZeroU16::new(max_concurrent_deletes).unwrap(),
            NonZeroU16::new(max_consecutive_delete_failures).unwrap(),
            false,
        )
    }

    #[cfg(not(feature = "shuttle"))]
    /// The wrapper is returned too, so tests can read its call counter.
    fn wrapped_asset_manager(
        repo: &crate::Repository,
        backend: &Arc<dyn Storage + Send + Sync>,
        failing_prefix: Option<&str>,
        mode: FailMode,
    ) -> (Arc<AssetManager>, Arc<FlakyDeletes>) {
        wrapped_asset_manager_with(repo, backend, failing_prefix, mode, None)
    }

    #[cfg(not(feature = "shuttle"))]
    fn wrapped_asset_manager_with(
        repo: &crate::Repository,
        backend: &Arc<dyn Storage + Send + Sync>,
        failing_prefix: Option<&str>,
        mode: FailMode,
        list_error_prefix: Option<&str>,
    ) -> (Arc<AssetManager>, Arc<FlakyDeletes>) {
        let flaky = Arc::new(FlakyDeletes {
            backend: Arc::clone(backend),
            calls: AtomicUsize::new(0),
            failing_prefix: failing_prefix.map(str::to_string),
            mode,
            list_error_prefix: list_error_prefix.map(str::to_string),
        });
        let storage: Arc<dyn Storage + Send + Sync> = Arc::clone(&flaky) as _;
        let am = Arc::new(AssetManager::new_no_cache(
            storage,
            repo.storage_settings().clone(),
            SpecVersionBin::V2,
            1,
            100,
        ));
        (am, flaky)
    }

    /// Chunks are the last phase, so a failure there skips nothing. The
    /// wrapper fails the first chunk delete call and succeeds on the second,
    /// which is the next GC run.
    #[tokio_test]
    async fn partial_chunk_delete_failures_are_reported_and_gc_completes()
    -> Result<(), Box<dyn std::error::Error>> {
        let backend: Arc<dyn Storage + Send + Sync> = new_in_memory_storage().await?;
        let repo = repo_with_garbage(&backend).await?;
        let (am, _flaky) = wrapped_asset_manager(
            &repo,
            &backend,
            Some(CHUNKS_FILE_PATH),
            FailMode::Alternate,
        );

        let summary = garbage_collect(Arc::clone(&am), &gc_config(50), None, 10).await?;
        assert_eq!(summary.snapshots_deleted, 3);
        assert_eq!(summary.transaction_logs_deleted, 3);
        assert_eq!(summary.manifests_deleted, 1);
        assert_eq!(summary.chunks_deleted, 0);
        assert_eq!(summary.objects_failed_to_delete, 4);
        assert_eq!(summary.delete_errors.len(), 1);
        assert!(summary.delete_errors[0].contains("injected delete failure"));
        assert!(summary.skipped_phases.is_empty());
        assert_eq!(repo.asset_manager().list_chunks().await?.count().await, 4);

        let second = garbage_collect(Arc::clone(&am), &gc_config(50), None, 10).await?;
        assert_eq!(second.chunks_deleted, 4);
        assert_eq!(second.objects_failed_to_delete, 0);
        assert_eq!(repo.asset_manager().list_chunks().await?.count().await, 0);
        Ok(())
    }

    #[tokio_test]
    async fn snapshot_delete_failures_skip_dependent_phases()
    -> Result<(), Box<dyn std::error::Error>> {
        let backend: Arc<dyn Storage + Send + Sync> = new_in_memory_storage().await?;
        let repo = repo_with_garbage(&backend).await?;
        let manifests_before = repo.asset_manager().list_manifests().await?.count().await;
        let tx_logs_before =
            repo.asset_manager().list_transaction_logs().await?.count().await;
        let (am, _flaky) = wrapped_asset_manager(
            &repo,
            &backend,
            Some(SNAPSHOTS_FILE_PATH),
            FailMode::Always,
        );

        let summary = garbage_collect(Arc::clone(&am), &gc_config(50), None, 10).await?;

        assert_eq!(summary.snapshots_deleted, 0);
        assert_eq!(summary.objects_failed_to_delete, 3);
        assert_eq!(
            summary.skipped_phases,
            vec![
                "transaction_logs".to_string(),
                "manifests".to_string(),
                "chunks".to_string()
            ]
        );
        // nothing downstream was touched
        assert_eq!(
            repo.asset_manager().list_manifests().await?.count().await,
            manifests_before
        );
        assert_eq!(
            repo.asset_manager().list_transaction_logs().await?.count().await,
            tx_logs_before
        );
        assert_eq!(repo.asset_manager().list_chunks().await?.count().await, 4);
        // but repo info no longer lists the garbage snapshots
        let (info, _) = am.fetch_repo_info().await?;
        assert_eq!(info.all_snapshots()?.count(), 4); // initial + c0..c2
        Ok(())
    }

    #[tokio_test]
    async fn total_delete_failure_aborts_after_the_threshold()
    -> Result<(), Box<dyn std::error::Error>> {
        let backend: Arc<dyn Storage + Send + Sync> = new_in_memory_storage().await?;
        let repo = repo_with_garbage(&backend).await?;
        let (am, flaky) = wrapped_asset_manager(&repo, &backend, None, FailMode::Always);
        // threshold 1: the first failed batch aborts
        let result = garbage_collect(Arc::clone(&am), &gc_config(1), None, 10).await;
        match result {
            Err(GCError::DeletesFailing { prefix, last_error }) => {
                assert_eq!(prefix, SNAPSHOTS_FILE_PATH);
                assert!(last_error.contains("injected delete failure"));
            }
            other => panic!("expected DeletesFailing, got {other:?}"),
        }
        // exactly one delete request was made before giving up
        assert_eq!(flaky.calls.load(Ordering::SeqCst), 1);
        Ok(())
    }

    /// A backend that deletes but reports a per-key failure inside `Ok` must
    /// gate later phases exactly like a failed request.
    #[tokio_test]
    async fn short_delete_count_skips_dependent_phases()
    -> Result<(), Box<dyn std::error::Error>> {
        let backend: Arc<dyn Storage + Send + Sync> = new_in_memory_storage().await?;
        let repo = repo_with_garbage(&backend).await?;
        let manifests_before = repo.asset_manager().list_manifests().await?.count().await;
        let (am, _flaky) = wrapped_asset_manager(
            &repo,
            &backend,
            Some(SNAPSHOTS_FILE_PATH),
            FailMode::ShortByOne,
        );

        let summary = garbage_collect(Arc::clone(&am), &gc_config(50), None, 10).await?;

        assert_eq!(summary.snapshots_deleted, 2); // backend reported 3 - 1
        assert_eq!(summary.objects_failed_to_delete, 1);
        assert_eq!(
            summary.skipped_phases,
            vec![
                "transaction_logs".to_string(),
                "manifests".to_string(),
                "chunks".to_string()
            ]
        );
        assert!(!summary.delete_errors.is_empty());
        assert_eq!(
            repo.asset_manager().list_manifests().await?.count().await,
            manifests_before
        );
        assert_eq!(repo.asset_manager().list_chunks().await?.count().await, 4);
        Ok(())
    }

    #[cfg(not(feature = "shuttle"))]
    /// The four garbage chunks, as delete candidates for `delete_listed`.
    async fn garbage_chunk_candidates(
        repo: &crate::Repository,
    ) -> Result<Vec<ListInfo<ChunkId>>, Box<dyn std::error::Error>> {
        Ok(repo.asset_manager().list_chunks().await?.try_collect().await?)
    }

    /// The consecutive counter must reset on a successful batch, so a phase
    /// where every other batch fails runs to the end while a threshold of one
    /// stops it at the first failure.
    #[tokio_test]
    async fn alternating_failures_reset_the_consecutive_counter()
    -> Result<(), Box<dyn std::error::Error>> {
        let backend: Arc<dyn Storage + Send + Sync> = new_in_memory_storage().await?;
        let repo = repo_with_garbage(&backend).await?;
        let candidates = garbage_chunk_candidates(&repo).await?;
        assert_eq!(candidates.len(), 4);
        // one object per batch, so the four chunks fail, ok, fail, ok
        let one = NonZeroUsize::MIN;

        let (am, _flaky) = wrapped_asset_manager(
            &repo,
            &backend,
            Some(CHUNKS_FILE_PATH),
            FailMode::Alternate,
        );
        // one request in flight, so batches are absorbed in submission order
        let report = delete_listed(
            am.as_ref(),
            &gc_config_with(2, 1),
            CHUNKS_FILE_PATH,
            one,
            stream::iter(candidates.into_iter().map(Ok)),
            |_| true,
        )
        .await?;
        assert_eq!(report.deleted_objects, 2);
        assert_eq!(report.failed_objects, 2);

        // with threshold 1 the very first failure ends the phase
        let backend: Arc<dyn Storage + Send + Sync> = new_in_memory_storage().await?;
        let repo = repo_with_garbage(&backend).await?;
        let candidates = garbage_chunk_candidates(&repo).await?;
        let (am, _flaky) = wrapped_asset_manager(
            &repo,
            &backend,
            Some(CHUNKS_FILE_PATH),
            FailMode::Alternate,
        );
        let result = delete_listed(
            am.as_ref(),
            &gc_config_with(1, 1),
            CHUNKS_FILE_PATH,
            one,
            stream::iter(candidates.into_iter().map(Ok)),
            |_| true,
        )
        .await;
        assert!(matches!(result, Err(GCError::DeletesFailing { .. })), "{result:?}");
        Ok(())
    }

    /// A listing that breaks mid-stream aborts the whole run: no phase gets a
    /// complete candidate set, so nothing may be deleted.
    #[tokio_test]
    async fn listing_errors_abort_the_run() -> Result<(), Box<dyn std::error::Error>> {
        let backend: Arc<dyn Storage + Send + Sync> = new_in_memory_storage().await?;
        let repo = repo_with_garbage(&backend).await?;
        let manifests_before = repo.asset_manager().list_manifests().await?.count().await;
        let tx_logs_before =
            repo.asset_manager().list_transaction_logs().await?.count().await;
        let (am, _flaky) = wrapped_asset_manager_with(
            &repo,
            &backend,
            None,
            FailMode::Alternate,
            Some(SNAPSHOTS_FILE_PATH),
        );

        let result = garbage_collect(Arc::clone(&am), &gc_config(50), None, 10).await;
        match result {
            Err(err) => {
                assert!(err.to_string().contains("injected listing failure"), "{err}");
            }
            Ok(summary) => panic!("expected the listing error, got {summary:?}"),
        }

        assert_eq!(
            repo.asset_manager().list_transaction_logs().await?.count().await,
            tx_logs_before
        );
        assert_eq!(
            repo.asset_manager().list_manifests().await?.count().await,
            manifests_before
        );
        assert_eq!(repo.asset_manager().list_chunks().await?.count().await, 4);
        Ok(())
    }
}
