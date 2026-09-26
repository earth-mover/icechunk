//! Repository maintenance operations.

use std::{
    collections::HashSet, future::Future, num::NonZeroU16, sync::Arc, time::Duration,
};

use async_stream::try_stream;
use backon::{BackoffBuilder as _, ExponentialBuilder, Retryable as _};
use futures::{Stream, StreamExt as _, TryStreamExt as _, stream};
use tokio::pin;
use tracing::{info, instrument, warn};

use crate::{
    StorageError,
    asset_manager::AssetManager,
    config::RepoUpdateRetryConfig,
    format::{
        IcechunkFormatError, IcechunkResult, SnapshotId,
        format_constants::SpecVersionBin,
        repo_info::{RepoAvailability, RepoInfo},
        snapshot::{Snapshot, SnapshotInfo},
    },
    refs::{RefError, RefResult, list_refs},
    repository::{RepositoryError, RepositoryErrorKind, RepositoryResult},
};
use icechunk_types::{ICResultExt as _, error::ICResultCtxExt as _};

/// The error of every maintenance operation in this module.
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

/// Refuse to run a maintenance operation on read-only storage, or on a repo
/// that is not online. `op_name` names the operation in the error message.
pub(crate) async fn ensure_repo_writable(
    asset_manager: &AssetManager,
    op_name: &str,
) -> GCResult<()> {
    if !asset_manager.can_write_to_storage().await? {
        return Err(RepositoryErrorKind::ReadonlyStorage(format!("Cannot {op_name}")))
            .capture()
            .map_err(GCError::Repository);
    }
    // repo status only exists on IC2+
    if asset_manager.spec_version() >= SpecVersionBin::V2 {
        let (repo_info, _) = asset_manager.fetch_repo_info().await?;
        if repo_info.status()?.availability != RepoAvailability::Online {
            return Err(RepositoryErrorKind::ReadonlyRepository(format!(
                "Cannot {op_name}"
            )))
            .capture()
            .map_err(GCError::Repository);
        }
    }
    Ok(())
}

/// Descriptors an operation needs beyond its concurrent requests: the runtime's
/// own, stdio, and the object store's idle connection pool.
const FD_HEADROOM: u64 = 64;

/// The process' soft `RLIMIT_NOFILE`, or `None` where the limit doesn't exist
/// or is unbounded.
#[cfg(unix)]
fn nofile_soft_limit() -> Option<u64> {
    let (soft, _hard) = rlimit::Resource::NOFILE.get().ok()?;
    (soft != rlimit::INFINITY).then_some(soft)
}

#[cfg(not(unix))]
fn nofile_soft_limit() -> Option<u64> {
    None
}

/// The requests a manifest walk has in flight at once: it fetches the
/// snapshots it walks alongside their manifests.
pub(crate) fn walk_peak_requests(
    max_concurrent_manifest_fetches: NonZeroU16,
    max_snapshots_in_memory: NonZeroU16,
) -> u64 {
    max_concurrent_manifest_fetches.get() as u64 + max_snapshots_in_memory.get() as u64
}

/// Warn when the process cannot open enough files for `peak_requests`
/// concurrent requests. `op_name` names the operation in the warning.
pub(crate) fn warn_on_low_fd_limit(peak_requests: u64, op_name: &str) {
    let Some(soft) = nofile_soft_limit() else { return };
    let needed = peak_requests + FD_HEADROOM;
    if soft < needed {
        warn!(
            soft_limit = soft,
            needed,
            "{op_name} peaks at {peak_requests} concurrent requests, but this \
             process can only open {soft} files and will likely fail with \
             \"too many open files\". Raise the limit (ulimit -S -n {needed}) or \
             lower the concurrency settings."
        );
    }
}

/// Run `op`, retrying with backoff while it fails because the repo info
/// object changed under it. `op_name` names the operation in the retry log.
pub(crate) async fn retry_on_repo_info_update<T, Fut>(
    retries: Option<&RepoUpdateRetryConfig>,
    op_name: &'static str,
    op: impl FnMut() -> Fut,
) -> GCResult<T>
where
    Fut: Future<Output = GCResult<T>>,
{
    let default_retry_config = RepoUpdateRetryConfig::default();
    let retry_config = retries.unwrap_or(&default_retry_config).retries();

    let backoff = ExponentialBuilder::new()
        .with_min_delay(Duration::from_millis(retry_config.initial_backoff_ms() as u64))
        .with_max_delay(Duration::from_millis(retry_config.max_backoff_ms() as u64))
        .with_max_times(retry_config.max_tries().get() as usize)
        .with_jitter()
        .build();

    op.retry(backoff)
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
        .notify(move |_, _| {
            info!(
                "Repo info object was updated while {op_name} was running, retrying with backoff..."
            );
        })
        .await
}

/// Aborts the task it holds when dropped, so a cancelled or early-returning
/// caller takes its background task down with it on every path.
pub(crate) struct AbortOnDrop<T>(pub(crate) tokio::task::JoinHandle<T>);

impl<T> AbortOnDrop<T> {
    /// Abort the task and wait for it to actually stop. Dropping only requests
    /// the abort; callers that need whatever the task captured to be dropped
    /// first must wait.
    pub(crate) async fn abort_and_wait(&mut self) {
        self.0.abort();
        let _ = (&mut self.0).await;
    }
}

impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Batched, rate-adaptive object deletion used by GC.
pub mod deleter;
/// Expire old snapshots beyond a threshold.
pub mod expiration;
/// Garbage collection to remove unreferenced data.
pub mod gc;
/// Manifest optimization and rebuilding.
pub mod manifests;
/// A hash set sharded across many locks, for parallel accumulation.
pub mod sharded_set;
/// Repository statistics.
pub mod stats;
/// Parallel manifest traversal shared by GC and stats.
pub mod walker;

#[instrument(skip_all)]
pub fn all_roots_v2<'a>(
    repo_info: &'a RepoInfo,
    extra_roots: &'a HashSet<SnapshotId>,
) -> RepositoryResult<impl Iterator<Item = RepositoryResult<SnapshotId>> + 'a> {
    let res = repo_info
        .tag_names()
        .inject()?
        .map(|tag| repo_info.resolve_tag(tag))
        .chain(repo_info.branch_names().inject()?.map(|br| repo_info.resolve_branch(br)))
        .chain(extra_roots.iter().cloned().map(Ok))
        .map(|r| r.inject());
    Ok(res)
}

/// Ids of every snapshot reachable from a ref or an `extra_roots` entry,
/// computed from `repo_info` alone (no IO).
#[instrument(skip_all)]
pub fn reachable_snapshots_v2(
    repo_info: &RepoInfo,
    extra_roots: &HashSet<SnapshotId>,
) -> RepositoryResult<HashSet<SnapshotId>> {
    let mut seen: HashSet<SnapshotId> = HashSet::new();
    for pointed_snap_id in all_roots_v2(repo_info, extra_roots)? {
        let pointed_snap_id = pointed_snap_id?;
        if !seen.contains(&pointed_snap_id) {
            for snap_info in repo_info.ancestry(&pointed_snap_id).inject()? {
                if !seen.insert(snap_info.inject()?.id) {
                    // the rest of the ancestry came in with the snapshot we already have
                    break;
                }
            }
        }
    }
    Ok(seen)
}

/// Fetches up to `max_concurrent_fetches` snapshots at a time, in no
/// particular order.
#[instrument(skip_all)]
pub fn pointed_snapshots_v2(
    asset_manager: Arc<AssetManager>,
    repo_info: &RepoInfo,
    extra_roots: &HashSet<SnapshotId>,
    max_concurrent_fetches: NonZeroU16,
) -> RepositoryResult<impl Stream<Item = RepositoryResult<Arc<Snapshot>>> + use<>> {
    let ids = reachable_snapshots_v2(repo_info, extra_roots)?;
    let res = stream::iter(ids)
        .map(move |id| {
            let asset_manager = Arc::clone(&asset_manager);
            async move { asset_manager.fetch_snapshot(&id).await }
        })
        .buffer_unordered(max_concurrent_fetches.get() as usize);
    Ok(res)
}

#[instrument(skip_all)]
pub async fn pointed_snapshots_v1<'a>(
    asset_manager: Arc<AssetManager>,
    extra_roots: &'a HashSet<SnapshotId>,
) -> RepositoryResult<impl Stream<Item = RepositoryResult<Arc<Snapshot>>> + 'a> {
    let mut seen: HashSet<SnapshotId> = HashSet::new();
    let res = try_stream! {
        let roots = all_roots_v1(Arc::clone(&asset_manager), extra_roots)
            .await.inject()?
            .map(|r| r.inject());
        pin!(roots);

        while let Some(pointed_snap_id) = roots.try_next().await? {
            if ! seen.contains(&pointed_snap_id) {
                #[expect(deprecated)]
                let parents = Arc::clone(&asset_manager).snapshot_ancestry_v1(&pointed_snap_id).await?;
                for await parent in parents {
                    let parent = parent?;
                    let snap_id = parent.id();
                    if seen.insert(snap_id) {
                        // it's a new snapshot
                        yield parent
                    } else {
                        // as soon as we find a repeated snapshot
                        // there is no point in continuing to retrieve
                        // the rest of the ancestry, it must be already
                        // retrieved from other ref
                        break
                    }
                }
            }
        }
    };
    Ok(res)
}
pub async fn all_roots_v1<'a>(
    asset_manager: Arc<AssetManager>,
    extra_roots: &'a HashSet<SnapshotId>,
) -> RefResult<impl Stream<Item = RefResult<SnapshotId>> + 'a> {
    let all_refs =
        list_refs(asset_manager.storage().as_ref(), &asset_manager.storage_context())
            .await?;
    let roots = stream::iter(all_refs)
        .then(move |r| {
            let asset_manager = Arc::clone(&asset_manager);
            async move {
                r.fetch(
                    asset_manager.storage().as_ref(),
                    &asset_manager.storage_context(),
                )
                .await
                .map(|ref_data| ref_data.snapshot)
            }
        })
        .chain(stream::iter(extra_roots.iter().cloned()).map(Ok));
    Ok(roots)
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
pub(crate) fn reparent_and_prune(
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

/// `repo_info` is ignored for V1 repos, which have no repo info object. For V2
/// it is fetched when not supplied; pass the one you already hold to keep the
/// walk consistent with anything else you derived from it.
///
/// `max_concurrent_fetches` only applies to V2: a V1 ancestry is a pointer
/// chase through the snapshot files, so its reads stay sequential.
pub async fn pointed_snapshots<'a>(
    asset_manager: Arc<AssetManager>,
    repo_info: Option<Arc<RepoInfo>>,
    extra_roots: &'a HashSet<SnapshotId>,
    max_concurrent_fetches: NonZeroU16,
) -> RepositoryResult<impl Stream<Item = RepositoryResult<Arc<Snapshot>>> + 'a> {
    match asset_manager.spec_version() {
        SpecVersionBin::V1 => {
            Ok(pointed_snapshots_v1(asset_manager, extra_roots).await?.left_stream())
        }
        SpecVersionBin::V2 => {
            let repo_info = match repo_info {
                Some(repo_info) => repo_info,
                None => asset_manager.fetch_repo_info().await?.0,
            };
            Ok(pointed_snapshots_v2(
                asset_manager,
                repo_info.as_ref(),
                extra_roots,
                max_concurrent_fetches,
            )?
            .right_stream())
        }
    }
}

#[cfg(test)]
mod tests {
    use futures::TryStreamExt as _;
    use std::{
        collections::{HashMap, HashSet},
        num::NonZeroU16,
        sync::Arc,
    };

    use bytes::Bytes;

    use crate::{
        Repository, Storage,
        format::{Path, SNAPSHOTS_FILE_PATH, format_constants::SpecVersionBin},
        new_in_memory_storage,
        ops::pointed_snapshots,
        test_utils::{logging_asset_manager, repo_with_converging_refs},
    };

    #[tokio::test]
    async fn test_pointed_snapshots_duplicate() -> Result<(), Box<dyn std::error::Error>>
    {
        let storage = new_in_memory_storage().await?;
        let repo = Repository::create(
            None,
            Arc::clone(&storage),
            HashMap::new(),
            None,
            true,
            None,
        )
        .await?;
        let mut session = repo.writable_session("main").await?;
        session.add_group(Path::root(), Bytes::new()).await?;
        let snap = session.commit("commit").max_concurrent_nodes(8).execute().await?;
        repo.create_tag("tag1", &snap).await?;
        let mut session = repo.writable_session("main").await?;
        session.add_group("/foo".try_into().unwrap(), Bytes::new()).await?;
        let snap = session.commit("commit").max_concurrent_nodes(8).execute().await?;
        repo.create_tag("tag2", &snap).await?;

        let all_snaps = pointed_snapshots(
            Arc::clone(repo.asset_manager()),
            None,
            &HashSet::new(),
            NonZeroU16::new(10).unwrap(),
        )
        .await?
        .try_collect::<Vec<_>>()
        .await?;

        assert_eq!(all_snaps.len(), 3);
        Ok(())
    }

    /// Refs whose ancestries converge must not re-read the snapshot they
    /// converge on: every reachable snapshot is fetched exactly once.
    #[tokio::test]
    async fn test_pointed_snapshots_v2_fetches_each_snapshot_once()
    -> Result<(), Box<dyn std::error::Error>> {
        let backend: Arc<dyn Storage + Send + Sync> = new_in_memory_storage().await?;
        let repo = repo_with_converging_refs(&backend).await?;
        let (logging, asset_manager) = logging_asset_manager(
            &backend,
            repo.storage_settings().clone(),
            SpecVersionBin::V2,
        );

        let all_snaps = pointed_snapshots(
            asset_manager,
            None,
            &HashSet::new(),
            NonZeroU16::new(10).unwrap(),
        )
        .await?
        .try_collect::<Vec<_>>()
        .await?;

        // 5 commits plus the initial snapshot
        assert_eq!(all_snaps.len(), 6);

        let snapshot_reads = logging
            .fetch_operations()
            .into_iter()
            .filter(|(_, path)| path.starts_with(SNAPSHOTS_FILE_PATH))
            .count();
        assert_eq!(snapshot_reads, 6);
        Ok(())
    }
}
