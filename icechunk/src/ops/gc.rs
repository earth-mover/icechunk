//! Garbage collection to remove unreferenced data.

use std::{
    collections::{HashMap, HashSet, VecDeque},
    num::{NonZeroU16, NonZeroUsize},
    pin::pin,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering as AtomicOrdering},
    },
    time::{Duration, Instant},
};

use backon::{BackoffBuilder as _, ExponentialBuilder, Retryable as _};
use chrono::{DateTime, TimeDelta, Utc};
use futures::{Stream, StreamExt as _, TryStreamExt as _, stream};
use itertools::Itertools as _;
use tokio::{sync::mpsc, task::JoinSet};
use tracing::{debug, error, info, instrument, trace, warn};

use crate::{
    Storage, StorageError,
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
        AbortOnDrop, pointed_snapshots, reachable_snapshots_v2,
        sharded_set::ChunkIdSet,
        walker::{ManifestConsumer, PROGRESS_INTERVAL, WalkLimits, walk_manifests},
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

/// The quiet period the deleter observes after a throttle: it starts at `base`
/// and doubles up to `cap` while the store keeps asking for less traffic.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DeleteBackoff {
    base: Duration,
    cap: Duration,
}

impl Default for DeleteBackoff {
    fn default() -> Self {
        Self { base: Duration::from_secs(1), cap: Duration::from_secs(120) }
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
    max_decoded_manifest_mem_bytes: NonZeroUsize,
    max_concurrent_manifest_fetches: NonZeroU16,
    max_concurrent_deletes: NonZeroU16,
    max_consecutive_delete_failures: NonZeroU16,
    max_concurrent_listings: Option<NonZeroU16>,
    delete_backoff: DeleteBackoff,

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
        max_decoded_manifest_mem_bytes: NonZeroUsize,
        max_concurrent_manifest_fetches: NonZeroU16,
        max_concurrent_deletes: NonZeroU16,
        max_consecutive_delete_failures: NonZeroU16,
        max_concurrent_listings: Option<NonZeroU16>,
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
            max_decoded_manifest_mem_bytes,
            max_concurrent_manifest_fetches,
            max_concurrent_deletes,
            max_consecutive_delete_failures,
            max_concurrent_listings,
            delete_backoff: DeleteBackoff::default(),
            dry_run,
        }
    }

    #[cfg(all(test, not(feature = "shuttle")))]
    pub(crate) fn with_delete_backoff(self, delete_backoff: DeleteBackoff) -> Self {
        GCConfig { delete_backoff, ..self }
    }

    #[expect(clippy::too_many_arguments)]
    pub fn clean_all(
        chunks_age: DateTime<Utc>,
        metadata_age: DateTime<Utc>,
        extra_roots: Option<HashSet<SnapshotId>>,
        max_snapshots_in_memory: NonZeroU16,
        max_compressed_manifest_mem_bytes: NonZeroUsize,
        max_decoded_manifest_mem_bytes: NonZeroUsize,
        max_concurrent_manifest_fetches: NonZeroU16,
        max_concurrent_deletes: NonZeroU16,
        max_consecutive_delete_failures: NonZeroU16,
        max_concurrent_listings: Option<NonZeroU16>,
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
            max_decoded_manifest_mem_bytes,
            max_concurrent_manifest_fetches,
            max_concurrent_deletes,
            max_consecutive_delete_failures,
            max_concurrent_listings,
            dry_run,
        )
    }

    /// How many id prefixes GC lists concurrently, defaulting to a value
    /// derived from the machine's cores.
    pub fn list_concurrency(&self) -> NonZeroU16 {
        self.max_concurrent_listings
            .unwrap_or_else(storage::listing::default_list_concurrency)
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
    /// Delete requests the store throttled; each was retried, none failed.
    pub throttled_batches: u64,
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

    fn progress(&self) -> Option<(&'static str, u64)> {
        Some(("retained_chunks", self.retained.len() as u64))
    }
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
        max_decoded_manifest_mem_bytes: config.max_decoded_manifest_mem_bytes,
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
        .with_min_delay(Duration::from_millis(retry_config.initial_backoff_ms() as u64))
        .with_max_delay(Duration::from_millis(retry_config.max_backoff_ms() as u64))
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
                    .list_snapshots_with_concurrency(config.list_concurrency())
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
        summary.throttled_batches += report.throttled_batches;
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
                let candidates = asset_manager
                    .list_snapshots_with_concurrency(config.list_concurrency())
                    .await?;
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
    /// Requests the store throttled; each one was retried, none failed.
    pub throttled_batches: u64,
    /// The highest number of delete requests this phase had in flight at once.
    pub peak_in_flight: usize,
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

/// What one delete batch task returns.
struct BatchOutcome {
    /// The batch's spawn order within the phase, from [`Congestion::take_seq`].
    seq: u64,
    /// How many keys the request asked the store to delete
    batch_len: usize,
    /// The `(key, size)` pairs, carried back only when the store throttled, so
    /// the deleter can re-queue them. Empty otherwise: the keys are no longer
    /// needed and a batch of them can be large.
    batch: Vec<(String, u64)>,
    /// The store's answer: how much it deleted, or why it did not.
    result: Result<DeleteObjectsResult, StorageError>,
}

/// What one finished batch means for the deleter.
enum Absorbed {
    /// Recorded: deleted, or failed in a way that counts as a failure.
    Done,
    /// The store asked for less traffic; the keys come back for a retry.
    Throttled { batch: Vec<(String, u64)>, message: String },
}

/// Record one finished batch into the phase's [`DeleteReport`], and stop the
/// phase once `threshold` batches in a row have failed. `consecutive_failures`
/// is control state for the phase, not part of its reported outcome, so it
/// lives outside the report.
///
/// A throttle is back-pressure rather than a failure: it is only counted, and
/// the caller decides what it means for the delete rate.
fn absorb_batch(
    report: &mut DeleteReport,
    consecutive_failures: &mut u32,
    prefix: &str,
    threshold: u32,
    joined: Result<BatchOutcome, tokio::task::JoinError>,
) -> GCResult<(u64, Absorbed)> {
    let BatchOutcome { seq, batch_len, batch, result } =
        joined.capture().map_err(GCError::Repository)?;
    if let Err(error) = &result
        && error.kind.is_throttled()
    {
        report.throttled_batches += 1;
        return Ok((seq, Absorbed::Throttled { batch, message: error.to_string() }));
    }
    // (objects not deleted, why); `None` when the backend deleted the whole batch
    let failure = match result {
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
    Ok((seq, Absorbed::Done))
}

/// A TCP-style in-flight limit for one delete phase: slow start from a single
/// request, one multiplicative decrease per throttle event, additive increase
/// on every clean window. Every phase starts over, because the store's rate
/// limits are per prefix.
#[derive(Debug)]
struct Congestion {
    max: usize,
    backoff: DeleteBackoff,
    limit: usize,
    slow_start: bool,
    next_seq: u64,
    /// Batches that came back un-throttled in the current window. A window is
    /// `limit` of them; when it closes without a throttle the store is keeping
    /// up and the limit grows.
    window_done: usize,
    /// `next_seq` at the last decrease. A batch older than this was already in
    /// flight then, so its throttle belongs to that same event, and its success
    /// says nothing about the reduced rate: neither counts.
    decrease_floor: u64,
    quiet: Duration,
    quiet_until: Option<Instant>,
}

/// What a throttled batch means for the delete rate.
#[derive(Debug, PartialEq, Eq)]
enum Throttle {
    /// Part of the throttle event that already lowered the limit.
    SameEvent,
    /// A new event: the limit halved and a quiet period started. `at_cap` says
    /// the quiet period had already reached its maximum, so the store has been
    /// throttling through the whole back-off ramp.
    Event { at_cap: bool },
}

impl Congestion {
    fn new(max: usize, backoff: DeleteBackoff) -> Self {
        Congestion {
            max: max.max(1),
            backoff,
            limit: 1,
            slow_start: true,
            next_seq: 0,
            window_done: 0,
            decrease_floor: 0,
            quiet: Duration::ZERO,
            quiet_until: None,
        }
    }

    fn limit(&self) -> usize {
        self.limit
    }

    fn quiet(&self) -> Duration {
        self.quiet
    }

    /// Nothing may be spawned before this instant.
    fn quiet_until(&self) -> Option<Instant> {
        self.quiet_until
    }

    /// The sequence number for the batch about to be spawned.
    fn take_seq(&mut self) -> u64 {
        let seq = self.next_seq;
        self.next_seq += 1;
        seq
    }

    fn open_window(&mut self) {
        self.window_done = 0;
    }

    fn on_throttle(&mut self, seq: u64, now: Instant) -> Throttle {
        if seq < self.decrease_floor {
            return Throttle::SameEvent;
        }
        self.slow_start = false;
        self.limit = (self.limit / 2).max(1);
        self.open_window();
        self.decrease_floor = self.next_seq;
        let at_cap = !self.quiet.is_zero() && self.quiet >= self.backoff.cap;
        self.quiet = if self.quiet.is_zero() {
            self.backoff.base
        } else {
            (self.quiet * 2).min(self.backoff.cap)
        };
        self.quiet_until = Some(now + self.quiet);
        Throttle::Event { at_cap }
    }

    /// A batch finished without being throttled, deleted or not: a window that
    /// ends without a throttle means the store is keeping up with the rate.
    fn on_complete(&mut self, seq: u64) {
        if seq < self.decrease_floor {
            return;
        }
        self.window_done += 1;
        if self.window_done >= self.limit {
            let grown = if self.slow_start { self.limit * 2 } else { self.limit + 1 };
            self.limit = grown.min(self.max);
            self.open_window();
            self.quiet = Duration::ZERO;
        }
    }
}

/// Batches queued between the listing collector and the deleter.
const DELETE_QUEUE_BATCHES: usize = 100;

/// Delete every candidate accepted by `should_delete`, in batches of
/// `batch_size`, with at most `max_concurrent_deletes` requests in flight.
/// Listing and filtering run on the calling task; deletes run on one deleter
/// task fed through a bounded channel, so listing only waits when
/// [`DELETE_QUEUE_BATCHES`] batches are already queued. Failed batches are
/// recorded and the phase continues, unless `max_consecutive_delete_failures`
/// batches fail in a row.
///
/// A listing error does not cut the deletes short: the batches already queued
/// are drained first and only then is the error returned, because everything
/// queued has already passed `should_delete` and the listing failure says
/// nothing about it.
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
    let progress = Arc::new(DeleteProgress::default());
    // each message is one delete batch: up to `batch_size` (object key, size
    // in bytes) pairs, the key relative to `prefix`, the size for the summary
    let (tx, rx) = mpsc::channel::<Vec<(String, u64)>>(DELETE_QUEUE_BATCHES);
    let mut deleter = AbortOnDrop(tokio::spawn(run_deletes(
        Arc::clone(asset_manager.storage()),
        asset_manager.storage_settings().clone(),
        prefix,
        config.max_concurrent_deletes.get() as usize,
        config.max_consecutive_delete_failures.get() as u32,
        config.delete_backoff,
        config.dry_run,
        rx,
        Arc::clone(&progress),
    )));
    // held in a guard so every return path below aborts it
    let _reporter =
        AbortOnDrop(tokio::spawn(report_delete_progress(Arc::clone(&progress), prefix)));

    let collected: GCResult<()> = async {
        let mut batch: Vec<(String, u64)> = Vec::with_capacity(batch_size.get());
        let mut candidates = pin!(candidates);
        while let Some(candidate) = candidates.next().await {
            let candidate = candidate?;
            progress.candidates_listed.fetch_add(1, AtomicOrdering::Relaxed);
            // the deleter returned, so nothing more will be deleted; listing on
            // would be wasted work. Its error is reported after the join.
            if tx.is_closed() {
                return Ok(());
            }
            if !should_delete(&candidate) {
                continue;
            }
            progress.candidates_accepted.fetch_add(1, AtomicOrdering::Relaxed);
            batch.push((candidate.id.to_string(), candidate.size_bytes));
            if batch.len() == batch_size.get() {
                let full =
                    std::mem::replace(&mut batch, Vec::with_capacity(batch_size.get()));
                if tx.send(full).await.is_err() {
                    // the deleter gave up; its error is reported below
                    return Ok(());
                }
                progress.batches_queued.fetch_add(1, AtomicOrdering::Relaxed);
            }
        }
        if !batch.is_empty() && tx.send(batch).await.is_ok() {
            progress.batches_queued.fetch_add(1, AtomicOrdering::Relaxed);
        }
        Ok(())
    }
    .await;
    drop(tx);

    let report = (&mut deleter.0).await.capture().map_err(GCError::Repository)??;
    collected?;
    Ok(report)
}

/// Live counters of one delete phase, shared by the collector, the deleter and
/// the progress reporter.
#[derive(Debug, Default)]
struct DeleteProgress {
    candidates_listed: AtomicU64,
    candidates_accepted: AtomicU64,
    batches_queued: AtomicU64,
    batches_done: AtomicU64,
    deleted_objects: AtomicU64,
    failed_objects: AtomicU64,
    throttled_batches: AtomicU64,
    /// the deleter's current in-flight limit and quiet period
    limit: AtomicU64,
    quiet_ms: AtomicU64,
}

impl DeleteProgress {
    fn read(&self) -> [u64; 9] {
        [
            self.candidates_listed.load(AtomicOrdering::Relaxed),
            self.candidates_accepted.load(AtomicOrdering::Relaxed),
            self.batches_queued.load(AtomicOrdering::Relaxed),
            self.batches_done.load(AtomicOrdering::Relaxed),
            self.deleted_objects.load(AtomicOrdering::Relaxed),
            self.failed_objects.load(AtomicOrdering::Relaxed),
            self.throttled_batches.load(AtomicOrdering::Relaxed),
            self.limit.load(AtomicOrdering::Relaxed),
            self.quiet_ms.load(AtomicOrdering::Relaxed),
        ]
    }
}

/// Log the delete phase's counters every [`PROGRESS_INTERVAL`], skipping ticks
/// where nothing moved. Runs until aborted by [`delete_listed`].
async fn report_delete_progress(progress: Arc<DeleteProgress>, prefix: &'static str) {
    let started = Instant::now();
    let mut ticker = tokio::time::interval(PROGRESS_INTERVAL);
    // the first tick completes immediately
    ticker.tick().await;
    let mut last = [0u64; 9];
    loop {
        ticker.tick().await;
        let now = progress.read();
        if now == last {
            continue;
        }
        let [
            listed,
            accepted,
            batches_queued,
            batches_done,
            deleted,
            failed,
            throttled,
            limit,
            quiet_ms,
        ] = now;
        info!(
            prefix,
            listed,
            accepted,
            batches_queued,
            batches_done,
            deleted,
            failed,
            throttled,
            limit,
            quiet_ms,
            elapsed_s = (started.elapsed().as_secs_f64() * 10.0).round() / 10.0,
            "delete phase progress"
        );
        last = now;
    }
}

/// The deleter's state for one phase: what it has recorded, what the store has
/// said about the rate, and the batches waiting to be retried.
struct Deleter {
    prefix: &'static str,
    threshold: u32,
    progress: Arc<DeleteProgress>,
    report: DeleteReport,
    congestion: Congestion,
    retry_queue: VecDeque<Vec<(String, u64)>>,
    consecutive_failures: u32,
    last_warn: Option<Instant>,
}

impl Deleter {
    fn new(
        prefix: &'static str,
        threshold: u32,
        max_in_flight: usize,
        backoff: DeleteBackoff,
        progress: Arc<DeleteProgress>,
    ) -> Self {
        Deleter {
            prefix,
            threshold,
            progress,
            report: DeleteReport::default(),
            congestion: Congestion::new(max_in_flight, backoff),
            retry_queue: VecDeque::new(),
            consecutive_failures: 0,
            last_warn: None,
        }
    }

    /// Record one finished batch and let it steer the delete rate. Throttled
    /// batches go back in the queue; only throttling that persists after the
    /// quiet period has reached its cap counts toward `threshold`.
    fn absorb(
        &mut self,
        joined: Result<BatchOutcome, tokio::task::JoinError>,
    ) -> GCResult<()> {
        let (seq, absorbed) = absorb_batch(
            &mut self.report,
            &mut self.consecutive_failures,
            self.prefix,
            self.threshold,
            joined,
        )?;
        let batches_done = match absorbed {
            Absorbed::Done => {
                self.congestion.on_complete(seq);
                1
            }
            Absorbed::Throttled { batch, message } => {
                self.retry_queue.push_back(batch);
                if let Throttle::Event { at_cap } =
                    self.congestion.on_throttle(seq, Instant::now())
                {
                    self.warn_throttled(&message);
                    if at_cap {
                        self.consecutive_failures += 1;
                        if self.consecutive_failures >= self.threshold {
                            return Err(GCError::DeletesFailing {
                                prefix: self.prefix.to_string(),
                                last_error: message,
                            });
                        }
                    }
                }
                0
            }
        };
        self.publish(batches_done);
        Ok(())
    }

    /// One line per second per phase: a throttled phase throttles in bursts.
    fn warn_throttled(&mut self, message: &str) {
        let now = Instant::now();
        if self.last_warn.is_none_or(|last| now - last >= Duration::from_secs(1)) {
            warn!(
                prefix = self.prefix,
                limit = self.congestion.limit(),
                quiet_ms = self.congestion.quiet().as_millis() as u64,
                error = message,
                "store is throttling deletes, reducing the request rate"
            );
            self.last_warn = Some(now);
        }
    }

    /// The report is the deleter's own state; the atomics mirror it for the
    /// reporter, which cannot see across the task boundary.
    fn publish(&self, batches_done: u64) {
        let progress = self.progress.as_ref();
        progress.batches_done.fetch_add(batches_done, AtomicOrdering::Relaxed);
        progress
            .deleted_objects
            .store(self.report.deleted_objects, AtomicOrdering::Relaxed);
        progress
            .failed_objects
            .store(self.report.failed_objects, AtomicOrdering::Relaxed);
        progress
            .throttled_batches
            .store(self.report.throttled_batches, AtomicOrdering::Relaxed);
        progress.limit.store(self.congestion.limit() as u64, AtomicOrdering::Relaxed);
        progress
            .quiet_ms
            .store(self.congestion.quiet().as_millis() as u64, AtomicOrdering::Relaxed);
    }
}

/// The deleter task: pulls batches, keeps at most `limit` delete requests
/// running, and stops with `DeletesFailing` after `threshold` consecutive
/// failed batches (dropping the `JoinSet` aborts the rest).
///
/// The limit is not `max_in_flight` but whatever rate the store sustains,
/// discovered by [`Congestion`]: a throttled batch is re-queued rather than
/// counted as a failure, and new requests wait out a quiet period.
#[expect(clippy::too_many_arguments)]
async fn run_deletes(
    storage: Arc<dyn Storage + Send + Sync>,
    settings: storage::Settings,
    prefix: &'static str,
    max_in_flight: usize,
    threshold: u32,
    backoff: DeleteBackoff,
    dry_run: bool,
    mut rx: mpsc::Receiver<Vec<(String, u64)>>,
    progress: Arc<DeleteProgress>,
) -> GCResult<DeleteReport> {
    let mut deleter = Deleter::new(prefix, threshold, max_in_flight, backoff, progress);
    let mut in_flight: JoinSet<BatchOutcome> = JoinSet::new();
    let mut queue_open = true;

    loop {
        // retries first: they are already past the listing filter
        let batch = match deleter.retry_queue.pop_front() {
            Some(batch) => batch,
            None if queue_open => match rx.recv().await {
                Some(batch) => batch,
                None => {
                    queue_open = false;
                    continue;
                }
            },
            // nothing more to spawn: drain what is still running, which may
            // put throttled batches back in the queue
            None => match in_flight.join_next().await {
                Some(joined) => {
                    deleter.absorb(joined)?;
                    continue;
                }
                None => break,
            },
        };

        if dry_run {
            deleter.report.record_dry_run(&batch);
            deleter.publish(1);
            continue;
        }

        while in_flight.len() >= deleter.congestion.limit() {
            match in_flight.join_next().await {
                Some(joined) => deleter.absorb(joined)?,
                None => break,
            }
        }

        // in-flight batches keep running through the quiet period; only new
        // requests wait
        if let Some(quiet_until) = deleter.congestion.quiet_until() {
            let now = Instant::now();
            if quiet_until > now {
                tokio::time::sleep(quiet_until - now).await;
            }
        }

        let seq = deleter.congestion.take_seq();
        let storage = Arc::clone(&storage);
        let settings = settings.clone();
        let batch_len = batch.len();
        in_flight.spawn(async move {
            // `delete_batch` consumes the keys, so a copy has to survive the
            // call to be re-queued if the store throttles it
            let retry = batch.clone();
            match storage.delete_batch(&settings, prefix, batch).await {
                Ok(result) => {
                    BatchOutcome { seq, batch_len, batch: Vec::new(), result: Ok(result) }
                }
                Err(error) if error.kind.is_throttled() => {
                    BatchOutcome { seq, batch_len, batch: retry, result: Err(error) }
                }
                Err(error) => {
                    BatchOutcome { seq, batch_len, batch: Vec::new(), result: Err(error) }
                }
            }
        });
        deleter.report.peak_in_flight =
            deleter.report.peak_in_flight.max(in_flight.len());
    }
    Ok(deleter.report)
}

#[instrument(skip(asset_manager, config, keep_ids), fields(keep_ids.len = keep_ids.len()))]
pub async fn gc_chunks(
    asset_manager: &AssetManager,
    config: &GCConfig,
    keep_ids: &ChunkIdSet,
) -> GCResult<DeleteReport> {
    info!("Deleting chunks");
    let candidates =
        asset_manager.list_chunks_with_concurrency(config.list_concurrency()).await?;
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
    let candidates =
        asset_manager.list_manifests_with_concurrency(config.list_concurrency()).await?;
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
    let candidates = asset_manager
        .list_transaction_logs_with_concurrency(config.list_concurrency())
        .await?;
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
        .with_min_delay(Duration::from_millis(retry_config.initial_backoff_ms() as u64))
        .with_max_delay(Duration::from_millis(retry_config.max_backoff_ms() as u64))
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
    // `Duration` is chrono's in this module under `cfg(not(shuttle))`
    use futures::stream::BoxStream;
    use icechunk_macros::tokio_test;
    use icechunk_storage::sealed::Sealed;
    use std::time::Duration as StdDuration;

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
            NonZeroUsize::new(4 * 1024 * 1024 * 1024).unwrap(),
            NonZeroU16::new(10).unwrap(),
            NonZeroU16::new(10).unwrap(),
            NonZeroU16::new(50).unwrap(),
            None,
            true,
        );
        garbage_collect(Arc::clone(&asset_manager), &config, None, 10).await?;

        let snapshot_prefix = format!("{SNAPSHOTS_FILE_PATH}/");
        let mut reads_per_snapshot: StdHashMap<String, usize> = StdHashMap::new();
        let mut snapshot_listings = 0;
        for (op, path) in logging.fetch_operations() {
            if matches!(op.as_str(), "list_objects" | "list_objects_with_id_prefixes") {
                if path == SNAPSHOTS_FILE_PATH {
                    snapshot_listings += 1;
                }
            } else if path.starts_with(&snapshot_prefix) {
                *reads_per_snapshot.entry(path).or_default() += 1;
            }
        }
        // the in-memory backend has no server-side prefix listing, so one pass
        // over the snapshots is a single full listing
        assert_eq!(
            snapshot_listings, 1,
            "expected one listing pass over the snapshots prefix, got {snapshot_listings} calls"
        );
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

    fn ms(millis: u64) -> StdDuration {
        StdDuration::from_millis(millis)
    }

    /// The control law on its own, with no store and no clock: slow start
    /// doubles on every clean window, a throttle event halves the limit once
    /// however many siblings report it, and the quiet period ramps to the cap
    /// and resets when a window comes back clean.
    #[test]
    fn congestion_ramps_up_and_backs_off() {
        let mut congestion =
            Congestion::new(8, DeleteBackoff { base: ms(10), cap: ms(40) });
        let now = Instant::now();
        assert_eq!(congestion.limit(), 1);

        let run_window = |congestion: &mut Congestion| {
            let window: Vec<u64> =
                (0..congestion.limit()).map(|_| congestion.take_seq()).collect();
            window.into_iter().for_each(|seq| congestion.on_complete(seq));
        };
        for expected in [2usize, 4, 8, 8] {
            run_window(&mut congestion);
            assert_eq!(congestion.limit(), expected);
        }
        assert!(congestion.quiet().is_zero());
        assert_eq!(congestion.quiet_until(), None);

        // one event per window: the siblings of the first throttle are the
        // same event, and must not halve the limit eight times
        let siblings: Vec<u64> = (0..8).map(|_| congestion.take_seq()).collect();
        assert_eq!(
            congestion.on_throttle(siblings[0], now),
            Throttle::Event { at_cap: false }
        );
        assert_eq!(congestion.limit(), 4);
        for seq in &siblings[1..] {
            assert_eq!(congestion.on_throttle(*seq, now), Throttle::SameEvent);
        }
        assert_eq!(congestion.limit(), 4);
        assert_eq!(congestion.quiet(), ms(10));
        assert_eq!(congestion.quiet_until(), Some(now + ms(10)));

        // a clean window at the reduced limit: additive increase now, and the
        // quiet period is over
        run_window(&mut congestion);
        assert_eq!(congestion.limit(), 5);
        assert!(congestion.quiet().is_zero());

        // sustained throttling: the quiet period doubles to the cap, and every
        // event from then on is one the caller counts as a failure
        let ramp: Vec<(StdDuration, Throttle)> = (0..5)
            .map(|_| {
                let seq = congestion.take_seq();
                let event = congestion.on_throttle(seq, now);
                (congestion.quiet(), event)
            })
            .collect();
        assert_eq!(
            ramp,
            vec![
                (ms(10), Throttle::Event { at_cap: false }),
                (ms(20), Throttle::Event { at_cap: false }),
                (ms(40), Throttle::Event { at_cap: false }),
                (ms(40), Throttle::Event { at_cap: true }),
                (ms(40), Throttle::Event { at_cap: true }),
            ]
        );
        assert_eq!(congestion.limit(), 1);
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
        /// every call waits for one permit from `gate` before delegating
        Gated,
        /// the first `n` calls under the failing prefix are throttled
        ThrottleFirst(usize),
        /// every call under the failing prefix is throttled
        ThrottleAlways,
    }

    /// What `delete_batch` should do with this call.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum Injected {
        Nothing,
        Fail,
        Throttle,
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
        /// permits `FailMode::Gated` deletes wait on; ignored by every other mode
        #[serde(skip, default = "closed_gate")]
        gate: Arc<tokio::sync::Semaphore>,
        /// `delete_batch` calls running right now, and the most ever at once:
        /// the deleter's in-flight limit as the store sees it
        #[serde(skip)]
        in_flight: AtomicUsize,
        #[serde(skip)]
        peak_in_flight: AtomicUsize,
        /// what to report from `lists_id_prefixes_natively`: `None` defers to
        /// the backend, `Some` picks which `AssetManager` listing path runs
        #[serde(default)]
        native_id_prefixes: Option<bool>,
    }

    /// A gate no delete can pass until the test adds permits.
    fn closed_gate() -> Arc<tokio::sync::Semaphore> {
        Arc::new(tokio::sync::Semaphore::new(0))
    }

    /// The `Err` a broken listing yields after its first item.
    fn injected_listing_failure()
    -> impl Stream<Item = StorageResult<ListInfo<String>>> + Send {
        stream::once(async {
            Err(StorageError::capture(StorageErrorKind::Other(
                "injected listing failure".to_string(),
            )))
        })
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
                FailMode::Gated => Injected::Nothing,
                FailMode::ThrottleFirst(first) if n < first => Injected::Throttle,
                FailMode::ThrottleFirst(_) => Injected::Nothing,
                FailMode::ThrottleAlways => Injected::Throttle,
            }
        }

        fn enter_delete(&self) -> InFlightGuard<'_> {
            let now = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
            self.peak_in_flight.fetch_max(now, Ordering::SeqCst);
            InFlightGuard(self)
        }

        #[cfg(not(feature = "shuttle"))]
        fn running_deletes(&self) -> usize {
            self.in_flight.load(Ordering::SeqCst)
        }

        #[cfg(not(feature = "shuttle"))]
        fn peak_deletes(&self) -> usize {
            self.peak_in_flight.load(Ordering::SeqCst)
        }
    }

    struct InFlightGuard<'a>(&'a FlakyDeletes);

    impl Drop for InFlightGuard<'_> {
        fn drop(&mut self) {
            self.0.in_flight.fetch_sub(1, Ordering::SeqCst);
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
            let listing = self.backend.list_objects(settings, prefix).await?;
            if self.list_error_prefix.as_deref() != Some(prefix) {
                return Ok(listing);
            }
            Ok(listing.take(1).chain(injected_listing_failure()).boxed())
        }

        async fn list_objects_with_id_prefixes<'a>(
            &'a self,
            settings: &Settings,
            prefix: &str,
            id_prefixes: &[String],
        ) -> StorageResult<BoxStream<'a, StorageResult<ListInfo<String>>>> {
            let listing = self
                .backend
                .list_objects_with_id_prefixes(settings, prefix, id_prefixes)
                .await?;
            // Fail a worker's listing, never the probe: the probe asks for the
            // single two-character prefix `"00"`, workers for one-character ones.
            // Breaking the probe would abort before any worker ran.
            let is_worker_call =
                id_prefixes.len() == 1 && id_prefixes[0].as_str() != "00";
            if self.list_error_prefix.as_deref() != Some(prefix) || !is_worker_call {
                return Ok(listing);
            }
            Ok(listing.take(1).chain(injected_listing_failure()).boxed())
        }

        fn lists_id_prefixes_natively(&self) -> bool {
            self.native_id_prefixes
                .unwrap_or_else(|| self.backend.lists_id_prefixes_natively())
        }

        async fn delete_batch(
            &self,
            settings: &Settings,
            prefix: &str,
            batch: Vec<(String, u64)>,
        ) -> StorageResult<DeleteObjectsResult> {
            let _in_flight = self.enter_delete();
            if matches!(self.mode, FailMode::Gated) {
                // forget: one permit per batch, never handed back on drop
                self.gate
                    .acquire()
                    .await
                    .map_err(|err| {
                        StorageError::capture(StorageErrorKind::Other(err.to_string()))
                    })?
                    .forget();
                return self.backend.delete_batch(settings, prefix, batch).await;
            }
            match self.injected(prefix) {
                Injected::Nothing => {
                    self.backend.delete_batch(settings, prefix, batch).await
                }
                Injected::Fail => Err(StorageError::capture(StorageErrorKind::Other(
                    "injected delete failure".to_string(),
                ))),
                Injected::Throttle => {
                    Err(StorageError::capture(StorageErrorKind::Throttled {
                        code: "SlowDown".to_string(),
                        message: "Please reduce your request rate.".to_string(),
                    }))
                }
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
            NonZeroUsize::new(4 * 1024 * 1024 * 1024).unwrap(),
            NonZeroU16::new(10).unwrap(),
            NonZeroU16::new(max_concurrent_deletes).unwrap(),
            NonZeroU16::new(max_consecutive_delete_failures).unwrap(),
            None,
            false,
        )
        // milliseconds instead of seconds, so a throttled phase runs in test time
        .with_delete_backoff(DeleteBackoff {
            base: std::time::Duration::from_millis(10),
            cap: std::time::Duration::from_millis(40),
        })
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
        wrapped_asset_manager_listing(
            repo,
            backend,
            failing_prefix,
            mode,
            list_error_prefix,
            None,
        )
    }

    #[cfg(not(feature = "shuttle"))]
    fn wrapped_asset_manager_listing(
        repo: &crate::Repository,
        backend: &Arc<dyn Storage + Send + Sync>,
        failing_prefix: Option<&str>,
        mode: FailMode,
        list_error_prefix: Option<&str>,
        native_id_prefixes: Option<bool>,
    ) -> (Arc<AssetManager>, Arc<FlakyDeletes>) {
        let flaky = Arc::new(FlakyDeletes {
            backend: Arc::clone(backend),
            calls: AtomicUsize::new(0),
            failing_prefix: failing_prefix.map(str::to_string),
            mode,
            list_error_prefix: list_error_prefix.map(str::to_string),
            gate: closed_gate(),
            in_flight: AtomicUsize::new(0),
            peak_in_flight: AtomicUsize::new(0),
            native_id_prefixes,
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

    #[cfg(not(feature = "shuttle"))]
    /// An asset manager whose deletes all block until the returned semaphore
    /// hands out permits, one per batch.
    fn gated_asset_manager(
        repo: &crate::Repository,
        backend: &Arc<dyn Storage + Send + Sync>,
    ) -> (Arc<AssetManager>, Arc<tokio::sync::Semaphore>) {
        let (am, flaky) = wrapped_asset_manager(repo, backend, None, FailMode::Gated);
        let gate = Arc::clone(&flaky.gate);
        (am, gate)
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

    /// Listing must finish while every delete is still blocked: the collector
    /// never waits on the deleter except when the batch queue is full.
    #[tokio_test]
    async fn listing_runs_ahead_of_blocked_deletes()
    -> Result<(), Box<dyn std::error::Error>> {
        let backend: Arc<dyn Storage + Send + Sync> = new_in_memory_storage().await?;
        let repo = repo_with_garbage(&backend).await?;
        let (am, gate) = gated_asset_manager(&repo, &backend);

        // 4 garbage chunks as 4 batches of 1; a marker item at the end of the
        // candidate stream records when listing finished. The marker is a random
        // id that `should_delete` rejects.
        let garbage: Vec<ListInfo<ChunkId>> =
            am.list_chunks().await?.try_collect().await?;
        assert_eq!(garbage.len(), 4);
        let garbage_ids: HashSet<ChunkId> =
            garbage.iter().map(|i| i.id.clone()).collect();
        let listed_all = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let marker =
            ListInfo { id: ChunkId::random(), created_at: Utc::now(), size_bytes: 0 };
        let candidates = stream::iter(garbage.into_iter().map(Ok)).chain(stream::once({
            let listed_all = Arc::clone(&listed_all);
            async move {
                listed_all.store(true, Ordering::SeqCst);
                Ok::<_, RepositoryError>(marker)
            }
        }));
        // max_concurrent_deletes = 1, so the deleter can only ever hold one
        // batch in flight and must block on the gate for the rest
        let config = gc_config_with(50, 1);
        let am2 = Arc::clone(&am);
        let run = tokio::spawn(async move {
            delete_listed(
                am2.as_ref(),
                &config,
                CHUNKS_FILE_PATH,
                NonZeroUsize::MIN,
                candidates,
                move |info| garbage_ids.contains(&info.id),
            )
            .await
        });

        // listing completes although no delete has been allowed to finish
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while !listed_all.load(Ordering::SeqCst) {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await?;
        assert_eq!(am.list_chunks().await?.count().await, 4, "nothing deleted yet");

        gate.add_permits(4);
        let report =
            tokio::time::timeout(std::time::Duration::from_secs(5), run).await???;
        assert_eq!(report.deleted_objects, 4);
        assert_eq!(report.failed_objects, 0);
        assert_eq!(am.list_chunks().await?.count().await, 0);
        Ok(())
    }

    #[cfg(not(feature = "shuttle"))]
    /// A repo whose only chunk objects are `n` uncommitted, hence garbage, chunks.
    async fn repo_with_loose_chunks(
        backend: &Arc<dyn Storage + Send + Sync>,
        n: usize,
    ) -> Result<crate::Repository, Box<dyn std::error::Error>> {
        let repo = repo_with_converging_refs(backend).await?;
        let session = repo.writable_session("main").await?;
        for i in 0..n {
            // above the inline threshold, so each chunk is its own object
            session.get_chunk_writer()?(Bytes::from(vec![i as u8; 1024])).await?;
        }
        Ok(repo)
    }

    #[cfg(not(feature = "shuttle"))]
    /// Poll until `done` holds, or fail the test.
    async fn wait_for(
        what: &str,
        done: impl Fn() -> bool,
    ) -> Result<(), Box<dyn std::error::Error>> {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while !done() {
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            }
        })
        .await
        .map_err(|_| format!("timed out waiting for {what}"))?;
        Ok(())
    }

    /// A throttle is back-pressure, not a failure: the batch is retried until
    /// the store accepts it, and the phase completes even with a threshold of
    /// one, which would abort at the first ordinary failure.
    #[tokio_test]
    async fn throttled_batches_are_retried_not_failed()
    -> Result<(), Box<dyn std::error::Error>> {
        let backend: Arc<dyn Storage + Send + Sync> = new_in_memory_storage().await?;
        let repo = repo_with_garbage(&backend).await?;
        let (am, flaky) = wrapped_asset_manager(
            &repo,
            &backend,
            Some(CHUNKS_FILE_PATH),
            FailMode::ThrottleFirst(3),
        );

        let summary = garbage_collect(Arc::clone(&am), &gc_config_with(1, 4), None, 10)
            .await
            .map_err(|err| format!("GC should survive throttling: {err}"))?;

        assert_eq!(summary.chunks_deleted, 4);
        assert_eq!(summary.throttled_batches, 3);
        assert_eq!(summary.objects_failed_to_delete, 0);
        assert!(summary.delete_errors.is_empty(), "{:?}", summary.delete_errors);
        assert!(summary.skipped_phases.is_empty());
        // three throttled attempts plus the one that went through
        assert_eq!(flaky.calls.load(Ordering::SeqCst), 4);
        assert_eq!(repo.asset_manager().list_chunks().await?.count().await, 0);
        Ok(())
    }

    /// Only throttling that persists through the whole back-off ramp ends a
    /// phase: the quiet period doubles 10 → 20 → 40 ms, and the two events
    /// after it reaches the cap are the two failures the threshold allows.
    #[tokio_test]
    async fn sustained_throttling_aborts_after_the_backoff_cap()
    -> Result<(), Box<dyn std::error::Error>> {
        let backend: Arc<dyn Storage + Send + Sync> = new_in_memory_storage().await?;
        let repo = repo_with_garbage(&backend).await?;
        let (am, flaky) = wrapped_asset_manager(
            &repo,
            &backend,
            Some(CHUNKS_FILE_PATH),
            FailMode::ThrottleAlways,
        );

        let started = Instant::now();
        let result = garbage_collect(Arc::clone(&am), &gc_config_with(2, 4), None, 10)
            .await
            .map(|summary| summary.chunks_deleted);
        let elapsed = started.elapsed();

        match result {
            Err(GCError::DeletesFailing { prefix, last_error }) => {
                assert_eq!(prefix, CHUNKS_FILE_PATH);
                assert!(last_error.contains("SlowDown"), "{last_error}");
            }
            other => panic!("expected DeletesFailing, got {other:?}"),
        }
        assert_eq!(flaky.calls.load(Ordering::SeqCst), 5);
        // the limit never leaves 1, so the store never sees two at once
        assert_eq!(flaky.peak_deletes(), 1);
        // 10 + 20 + 40 + 40 ms of quiet periods, not the production seconds
        assert!(elapsed < std::time::Duration::from_millis(200), "{elapsed:?}");
        Ok(())
    }

    /// Slow start doubles the in-flight limit on every clean window. With the
    /// gate closed the store sees exactly `limit` requests at once, so the test
    /// can watch 1, 2, 4, 8 by releasing one window at a time.
    #[tokio_test]
    async fn slow_start_doubles_the_limit_on_clean_windows()
    -> Result<(), Box<dyn std::error::Error>> {
        let backend: Arc<dyn Storage + Send + Sync> = new_in_memory_storage().await?;
        // 15 = 1 + 2 + 4 + 8: four clean windows, the last one at the ceiling
        let repo = repo_with_loose_chunks(&backend, 15).await?;
        let (am, flaky) = wrapped_asset_manager(&repo, &backend, None, FailMode::Gated);
        let gate = Arc::clone(&flaky.gate);
        let candidates: Vec<ListInfo<ChunkId>> =
            am.list_chunks().await?.try_collect().await?;
        assert_eq!(candidates.len(), 15);

        // one object per batch, ceiling of 8 concurrent requests
        let config = gc_config_with(50, 8);
        let am2 = Arc::clone(&am);
        let run = tokio::spawn(async move {
            delete_listed(
                am2.as_ref(),
                &config,
                CHUNKS_FILE_PATH,
                NonZeroUsize::MIN,
                stream::iter(candidates.into_iter().map(Ok)),
                |_| true,
            )
            .await
        });

        for expected in [1usize, 2, 4, 8] {
            wait_for(&format!("{expected} deletes in flight"), || {
                flaky.running_deletes() == expected
            })
            .await?;
            assert_eq!(flaky.peak_deletes(), expected);
            gate.add_permits(expected);
        }

        let report =
            tokio::time::timeout(std::time::Duration::from_secs(5), run).await???;
        assert_eq!(report.deleted_objects, 15);
        assert_eq!(report.failed_objects, 0);
        assert_eq!(report.throttled_batches, 0);
        assert_eq!(report.peak_in_flight, 8);
        assert_eq!(am.list_chunks().await?.count().await, 0);
        Ok(())
    }

    /// A listing that breaks mid-stream aborts the whole run: no phase gets a
    /// complete candidate set, so nothing may be deleted. `fan_out` picks which
    /// listing path breaks: a prefix worker, or the single full listing that
    /// backends without server-side prefix listing use. Both are forced, so
    /// the choice does not depend on what the in-memory backend reports.
    #[cfg(not(feature = "shuttle"))]
    async fn listing_error_aborts_the_run(
        fan_out: bool,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let backend: Arc<dyn Storage + Send + Sync> = new_in_memory_storage().await?;
        let repo = repo_with_garbage(&backend).await?;
        let manifests_before = repo.asset_manager().list_manifests().await?.count().await;
        let tx_logs_before =
            repo.asset_manager().list_transaction_logs().await?.count().await;
        let (am, _flaky) = wrapped_asset_manager_listing(
            &repo,
            &backend,
            None,
            FailMode::Alternate,
            Some(SNAPSHOTS_FILE_PATH),
            Some(fan_out),
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

    /// The error surfaces from a prefix worker, through the channel and the
    /// stream, and aborts the run.
    #[tokio_test]
    async fn listing_errors_abort_the_run_fanning_out()
    -> Result<(), Box<dyn std::error::Error>> {
        listing_error_aborts_the_run(true).await
    }

    /// The same, on the single-listing path a non-native backend takes.
    #[tokio_test]
    async fn listing_errors_abort_the_run_single_listing()
    -> Result<(), Box<dyn std::error::Error>> {
        listing_error_aborts_the_run(false).await
    }
}
