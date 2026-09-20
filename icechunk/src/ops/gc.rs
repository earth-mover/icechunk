//! Garbage collection to remove unreferenced data.

use std::{
    collections::{HashMap, HashSet},
    num::{NonZeroU16, NonZeroUsize},
    sync::Arc,
};

use chrono::{DateTime, TimeDelta, Utc};
use futures::{Stream, StreamExt as _, TryStreamExt as _, stream};
use itertools::Itertools as _;
use tracing::{debug, info, instrument, trace};

use crate::{
    asset_manager::AssetManager,
    config::RepoUpdateRetryConfig,
    format::{
        CHUNKS_FILE_PATH, ChunkId, MANIFESTS_FILE_PATH, ManifestId, SNAPSHOTS_FILE_PATH,
        SnapshotId, TRANSACTION_LOGS_FILE_PATH,
        format_constants::SpecVersionBin,
        manifest::{ChunkPayload, Manifest},
        repo_info::{RepoInfo, UpdateInfo, UpdateType},
        snapshot::{Snapshot, SnapshotInfo},
    },
    ops::{
        deleter::{
            self, DELETE_BATCH_SIZE, DeleteBackoff, DeleteConfig, DeleteError,
            MAX_REPORTED_DELETE_ERRORS,
        },
        ensure_repo_writable, pointed_snapshots, reachable_snapshots_v2,
        reparent_and_prune, retry_on_repo_info_update,
        sharded_set::ChunkIdSet,
        walker::{ManifestConsumer, WalkLimits, walk_manifests},
    },
    repository::{RepositoryError, RepositoryErrorKind, RepositoryResult},
    storage::{self, ListInfo},
};
use icechunk_types::error::ICResultCtxExt as _;

pub use crate::ops::{
    GCError, GCResult,
    deleter::DeleteReport,
    expiration::{ExpireResult, ExpiredRefAction, expire, expire_v2},
};

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

    pub(crate) fn delete_config(&self) -> DeleteConfig {
        DeleteConfig {
            max_in_flight: self.max_concurrent_deletes,
            max_consecutive_failures: self.max_consecutive_delete_failures,
            backoff: self.delete_backoff,
            dry_run: self.dry_run,
        }
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

impl GCSummary {
    /// Fold one phase's report in, crediting its deletions to
    /// `deleted_counter`. Returns whether the phase had failed deletes, which
    /// gates the phases that depend on it.
    fn absorb(
        &mut self,
        report: DeleteReport,
        deleted_counter: fn(&mut GCSummary) -> &mut u64,
    ) -> bool {
        *deleted_counter(self) += report.deleted_objects;
        self.bytes_deleted += report.deleted_bytes;
        self.objects_failed_to_delete += report.failed_objects;
        self.throttled_batches += report.throttled_batches;
        for message in report.errors {
            if self.delete_errors.len() < MAX_REPORTED_DELETE_ERRORS
                && !self.delete_errors.contains(&message)
            {
                self.delete_errors.push(message);
            }
        }
        report.failed_objects > 0
    }
}

impl From<DeleteError> for GCError {
    fn from(err: DeleteError) -> Self {
        match err {
            DeleteError::DeletesFailing { prefix, last_error } => {
                GCError::DeletesFailing { prefix, last_error }
            }
            DeleteError::Repository(err) => GCError::Repository(err),
        }
    }
}

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
    ensure_repo_writable(asset_manager.as_ref(), "garbage collect").await?;
    retry_on_repo_info_update(repo_update_retries, "GC", async || {
        garbage_collect_one_attempt(
            Arc::clone(&asset_manager),
            config,
            num_updates_per_repo_info_file,
        )
        .await
    })
    .await
}

#[instrument(skip_all)]
async fn garbage_collect_one_attempt(
    asset_manager: Arc<AssetManager>,
    config: &GCConfig,
    num_updates_per_repo_info_file: u16,
) -> GCResult<GCSummary> {
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
        earlier_phase_failed |= summary.absorb(report, |s| &mut s.snapshots_deleted);
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
                summary.absorb(report, |s| &mut s.transaction_logs_deleted);
        }
    }
    if config.deletes_manifests() {
        if earlier_phase_failed {
            summary.skipped_phases.push("manifests".to_string());
        } else {
            let report =
                gc_manifests(asset_manager.as_ref(), config, &keep_manifests).await?;
            earlier_phase_failed |= summary.absorb(report, |s| &mut s.manifests_deleted);
        }
    }
    if config.deletes_chunks() {
        if earlier_phase_failed {
            summary.skipped_phases.push("chunks".to_string());
        } else {
            asset_manager.clear_chunk_cache();
            let report = gc_chunks(asset_manager.as_ref(), config, &keep_chunks).await?;
            summary.absorb(report, |s| &mut s.chunks_deleted);
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

#[instrument(skip(asset_manager, config, keep_ids), fields(keep_ids.len = keep_ids.len()))]
pub async fn gc_chunks(
    asset_manager: &AssetManager,
    config: &GCConfig,
    keep_ids: &ChunkIdSet,
) -> GCResult<DeleteReport> {
    info!("Deleting chunks");
    let candidates =
        asset_manager.list_chunks_with_concurrency(config.list_concurrency()).await?;
    Ok(deleter::delete_listed(
        asset_manager,
        config.delete_config(),
        CHUNKS_FILE_PATH,
        DELETE_BATCH_SIZE,
        candidates,
        |chunk| config.must_delete_chunk(chunk) && !keep_ids.contains(&chunk.id),
    )
    .await?)
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
    Ok(deleter::delete_listed(
        asset_manager,
        config.delete_config(),
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
    .await?)
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
    Ok(deleter::delete_listed(
        asset_manager,
        config.delete_config(),
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
    .await?)
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
    Ok(deleter::delete_listed(
        asset_manager,
        config.delete_config(),
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
    .await?)
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone as _;
    use icechunk_macros::tokio_test;

    use super::*;

    // `tokio_test` expands to nothing under shuttle, leaving the async tests
    // and everything only they use unreferenced.
    #[cfg(not(feature = "shuttle"))]
    use crate::{
        Storage,
        format::{CHUNKS_FILE_PATH, SNAPSHOTS_FILE_PATH},
        ops::deleter::testing::{
            FailMode, wrapped_asset_manager, wrapped_asset_manager_listing,
        },
        storage::new_in_memory_storage,
        test_utils::{logging_asset_manager, repo_with_converging_refs},
    };
    #[cfg(not(feature = "shuttle"))]
    use bytes::Bytes;
    // `Duration` is chrono's in this module under `cfg(not(shuttle))`
    #[cfg(not(feature = "shuttle"))]
    use chrono::Duration;
    #[cfg(not(feature = "shuttle"))]
    use std::{
        collections::HashMap as StdHashMap, sync::atomic::Ordering, time::Instant,
    };

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
