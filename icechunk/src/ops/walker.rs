//! Parallel traversal of every manifest reachable from a stream of snapshots.
//!
//! [`walk_manifests`] is the interface: give it a stream of snapshots, a
//! [`ManifestConsumer`] with the per-manifest work, and [`WalkLimits`], and it
//! fetches and decodes each distinct manifest exactly once, runs the consumer
//! on it, and returns a [`WalkResult`] with the folded outputs plus the sets of
//! snapshot and manifest ids it saw.
//!
//! The pipeline: the calling task feeds manifest infos and owns the dedup set;
//! fetch workers read compressed bytes under a memory budget; decode workers
//! decode and run the consumer on the blocking pool; one fold task folds the
//! outputs. Any error or panic in any stage aborts the whole walk.

use std::{
    collections::HashSet,
    num::{NonZeroU16, NonZeroUsize},
    pin::pin,
    sync::Arc,
};

use futures::{Stream, StreamExt as _};
use tokio::{
    sync::{Mutex, OwnedSemaphorePermit, Semaphore, mpsc},
    task::{AbortHandle, JoinSet},
};
use tracing::instrument;

use crate::{
    asset_manager::{AssetManager, decode_manifest},
    format::{
        ManifestId, SnapshotId,
        manifest::Manifest,
        snapshot::{ManifestFileInfo, Snapshot},
    },
    repository::{RepositoryError, RepositoryErrorKind, RepositoryResult},
};
use icechunk_types::{ICResultExt as _, error::ICResultCtxExt as _};

/// Per-manifest work for [`walk_manifests`].
pub trait ManifestConsumer: Send + Sync + 'static {
    type Output: Send + 'static;
    type Acc: Default + Send + 'static;
    /// Runs on a blocking thread, concurrently across manifests.
    fn consume(&self, manifest: &Manifest) -> RepositoryResult<Self::Output>;
    /// Runs on the single fold task, once per manifest, in completion order.
    fn fold(acc: &mut Self::Acc, output: Self::Output);
}

#[derive(Debug, Clone, Copy)]
pub struct WalkLimits {
    pub max_concurrent_manifest_fetches: NonZeroU16,
    /// Budget for compressed manifest bytes in flight (fetched, not yet consumed).
    pub max_manifest_mem_bytes: NonZeroUsize,
    pub decode_workers: NonZeroU16,
}

#[derive(Debug)]
pub struct WalkResult<Acc> {
    pub acc: Acc,
    /// Every snapshot the input stream yielded.
    pub snapshots: HashSet<SnapshotId>,
    /// Every manifest referenced by those snapshots; each was fetched once.
    pub manifests: HashSet<ManifestId>,
}

const KIB: usize = 1024;

struct Fetched {
    bytes: Vec<u8>,
    /// Released when the decode worker is done with the manifest.
    _permit: OwnedSemaphorePermit,
}

type SharedRx<T> = Arc<Mutex<mpsc::Receiver<T>>>;

/// KiB permits for a manifest of `size_bytes`, clamped so a manifest larger
/// than the whole budget still runs (alone, once everything else drains).
fn permits_for(size_bytes: u64, total_permits: u32) -> u32 {
    let kib = (size_bytes as usize).div_ceil(KIB).max(1);
    kib.min(total_permits as usize) as u32
}

fn total_permits(max_manifest_mem_bytes: NonZeroUsize) -> u32 {
    let cap = Semaphore::MAX_PERMITS.min(u32::MAX as usize);
    (max_manifest_mem_bytes.get() / KIB).clamp(1, cap) as u32
}

async fn recv_shared<T>(rx: &SharedRx<T>) -> Option<T> {
    rx.lock().await.recv().await
}

/// One of `max_concurrent_manifest_fetches` tasks: take a manifest info off
/// the shared queue, wait for enough of the memory budget to hold its
/// compressed bytes, read them from storage and pass them on with the permit.
/// The permit travels with the bytes, so the budget is released only once a
/// decode worker has consumed the manifest.
async fn fetch_worker(
    asset_manager: Arc<AssetManager>,
    rx: SharedRx<ManifestFileInfo>,
    budget: Arc<Semaphore>,
    total_permits: u32,
    tx: mpsc::Sender<Fetched>,
) -> RepositoryResult<()> {
    while let Some(info) = recv_shared(&rx).await {
        let permit = Arc::clone(&budget)
            .acquire_many_owned(permits_for(info.size_bytes, total_permits))
            .await
            .capture()?;
        let bytes = asset_manager.fetch_manifest_bytes(&info.id, info.size_bytes).await?;
        if tx.send(Fetched { bytes, _permit: permit }).await.is_err() {
            // receivers are gone because a decode worker failed; that error
            // is reported through the JoinSet, not from here
            return Ok(());
        }
    }
    Ok(())
}

/// One of `decode_workers` tasks: take fetched bytes off the shared queue and,
/// on a blocking thread, decode the manifest and run the consumer on it. The
/// compressed bytes are dropped before the consumer runs; the decoded manifest
/// and the budget permit are dropped when it returns. The output goes to the
/// fold task.
async fn decode_worker<C: ManifestConsumer>(
    consumer: Arc<C>,
    rx: SharedRx<Fetched>,
    tx: mpsc::Sender<C::Output>,
) -> RepositoryResult<()> {
    while let Some(fetched) = recv_shared(&rx).await {
        let consumer = Arc::clone(&consumer);
        let span = tracing::Span::current();
        let output = tokio::task::spawn_blocking(move || {
            let _entered = span.entered();
            let Fetched { bytes, _permit } = fetched;
            let manifest = decode_manifest(&bytes)?;
            drop(bytes);
            consumer.consume(&manifest)
            // manifest and _permit drop here, releasing the budget
        })
        .await
        .capture()??;
        if tx.send(output).await.is_err() {
            return Ok(());
        }
    }
    Ok(())
}

/// The single task that owns the accumulator: folds every consumer output
/// in completion order until the decode workers close the channel.
async fn fold_outputs<C: ManifestConsumer>(mut rx: mpsc::Receiver<C::Output>) -> C::Acc {
    let mut acc = C::Acc::default();
    while let Some(output) = rx.recv().await {
        C::fold(&mut acc, output);
    }
    acc
}

/// How [`feed`] ended.
enum FeedOutcome {
    /// The snapshot stream was exhausted and every info was sent.
    Done,
    /// A worker returned `Ok` while we were still feeding. Its channel's
    /// owner must be gone; only the caller can say why (see [`walk_manifests`]).
    WorkerExitedEarly,
    Failed(RepositoryError),
}

/// Drive the snapshot stream, deduplicate manifest ids, and send new infos
/// to the fetch workers. A worker finishing while we are still feeding is
/// surfaced immediately: it either failed or lost its consumer.
async fn feed<S>(
    snapshots: S,
    infos_tx: &mpsc::Sender<ManifestFileInfo>,
    workers: &mut JoinSet<RepositoryResult<()>>,
    seen_snapshots: &mut HashSet<SnapshotId>,
    seen_manifests: &mut HashSet<ManifestId>,
) -> FeedOutcome
where
    S: Stream<Item = RepositoryResult<Arc<Snapshot>>>,
{
    let mut snapshots = pin!(snapshots);
    while let Some(snapshot) = snapshots.next().await {
        let snapshot = match snapshot {
            Ok(snapshot) => snapshot,
            Err(err) => return FeedOutcome::Failed(err),
        };
        if !seen_snapshots.insert(snapshot.id()) {
            continue;
        }
        for info in snapshot.manifest_files() {
            let info = match info.inject() {
                Ok(info) => info,
                Err(err) => return FeedOutcome::Failed(err),
            };
            if !seen_manifests.insert(info.id.clone()) {
                continue;
            }
            tokio::select! {
                // poll the arms in order, not at random: a finished worker wins over a send
                biased;
                Some(joined) = workers.join_next() => {
                    match joined {
                        Err(join_error) => {
                            return FeedOutcome::Failed(
                                RepositoryError::capture(join_error.into()),
                            );
                        }
                        Ok(Err(err)) => return FeedOutcome::Failed(err),
                        Ok(Ok(())) => return FeedOutcome::WorkerExitedEarly,
                    }
                }
                sent = infos_tx.send(info) => {
                    if sent.is_err() {
                        return FeedOutcome::Failed(
                            RepositoryError::capture(RepositoryErrorKind::Other(
                                "manifest walker workers stopped accepting work"
                                    .to_string(),
                            )),
                        );
                    }
                }
            }
        }
    }
    FeedOutcome::Done
}

/// Visit every manifest referenced by the `snapshots` stream once, running
/// `consumer` on each, within `limits`. Bytes are read straight from storage,
/// bypassing the asset manager's cache and its semaphores.
#[instrument(skip_all, fields(
    fetches = limits.max_concurrent_manifest_fetches.get(),
    decoders = limits.decode_workers.get(),
    mem_bytes = limits.max_manifest_mem_bytes.get(),
))]
pub async fn walk_manifests<C: ManifestConsumer>(
    asset_manager: Arc<AssetManager>,
    limits: WalkLimits,
    consumer: Arc<C>,
    snapshots: impl Stream<Item = RepositoryResult<Arc<Snapshot>>>,
) -> RepositoryResult<WalkResult<C::Acc>> {
    let fetchers = limits.max_concurrent_manifest_fetches.get() as usize;
    let decoders = limits.decode_workers.get() as usize;
    let total_permits = total_permits(limits.max_manifest_mem_bytes);
    let budget = Arc::new(Semaphore::new(total_permits as usize));

    // Infos are tiny, so keep one queued per fetcher on top of the one each is
    // working on: a fetcher that finishes never waits for the feeder to run.
    // Fetched bytes are large and already bounded by the memory budget, so
    // that channel only needs room for every fetcher to hand off one result;
    // likewise one output per decoder.
    let (infos_tx, infos_rx) = mpsc::channel::<ManifestFileInfo>(2 * fetchers);
    let (bytes_tx, bytes_rx) = mpsc::channel::<Fetched>(fetchers);
    let (out_tx, out_rx) = mpsc::channel::<C::Output>(decoders);
    let infos_rx: SharedRx<ManifestFileInfo> = Arc::new(Mutex::new(infos_rx));
    let bytes_rx: SharedRx<Fetched> = Arc::new(Mutex::new(bytes_rx));

    let mut workers: JoinSet<RepositoryResult<()>> = JoinSet::new();
    let mut aborts: Vec<AbortHandle> = Vec::with_capacity(fetchers + decoders);
    for _ in 0..fetchers {
        aborts.push(workers.spawn(fetch_worker(
            Arc::clone(&asset_manager),
            Arc::clone(&infos_rx),
            Arc::clone(&budget),
            total_permits,
            bytes_tx.clone(),
        )));
    }
    drop(bytes_tx);
    for _ in 0..decoders {
        aborts.push(workers.spawn(decode_worker(
            Arc::clone(&consumer),
            Arc::clone(&bytes_rx),
            out_tx.clone(),
        )));
    }
    drop(out_tx);
    // only the workers may own the receivers: a channel whose workers have all
    // exited must close, or an upstream worker blocks on `send` forever
    drop(infos_rx);
    drop(bytes_rx);
    let folder = tokio::spawn(fold_outputs::<C>(out_rx));

    let abort_everything = |aborts: &[AbortHandle]| {
        for handle in aborts {
            handle.abort();
        }
        folder.abort();
    };

    // Plain HashSets, not ShardedSets: only the feeder, one task, ever inserts
    // or reads them.
    let mut seen_snapshots = HashSet::new();
    let mut seen_manifests = HashSet::new();
    let fed = feed(
        snapshots,
        &infos_tx,
        &mut workers,
        &mut seen_snapshots,
        &mut seen_manifests,
    )
    .await;
    drop(infos_tx);
    match fed {
        FeedOutcome::Done => {}
        FeedOutcome::Failed(err) => {
            abort_everything(&aborts);
            return Err(err);
        }
        FeedOutcome::WorkerExitedEarly => {
            abort_everything(&aborts);
            // The worker stopped because its output channel closed, which
            // means the fold task is gone. A panicking `fold` is the real
            // cause; report it instead of the generic message.
            return match folder.await {
                Err(join_error) if join_error.is_panic() => Err(join_error).capture(),
                _ => Err(RepositoryErrorKind::Other(
                    "manifest walker worker exited before its input was closed"
                        .to_string(),
                ))
                .capture(),
            };
        }
    }

    while let Some(joined) = workers.join_next().await {
        let result: RepositoryResult<()> = match joined {
            Ok(result) => result,
            Err(join_error) => Err(join_error).capture(),
        };
        if let Err(err) = result {
            abort_everything(&aborts);
            return Err(err);
        }
    }

    let acc = folder.await.capture()?;
    Ok(WalkResult { acc, snapshots: seen_snapshots, manifests: seen_manifests })
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashSet,
        sync::atomic::{AtomicUsize, Ordering},
        time::Duration,
    };

    use futures::TryStreamExt as _;

    use super::*;
    use crate::{
        Storage,
        format::{MANIFESTS_FILE_PATH, format_constants::SpecVersionBin},
        ops::pointed_snapshots,
        repository::RepositoryError,
        storage::new_in_memory_storage,
        test_utils::{logging_asset_manager, repo_with_converging_refs},
    };

    /// Counts manifests and records their ids.
    struct Counting;
    impl ManifestConsumer for Counting {
        type Output = ManifestId;
        type Acc = Vec<ManifestId>;
        fn consume(&self, manifest: &Manifest) -> RepositoryResult<ManifestId> {
            Ok(manifest.id())
        }
        fn fold(acc: &mut Vec<ManifestId>, output: ManifestId) {
            acc.push(output);
        }
    }

    /// Fails on the `fail_at`-th consume call (1-based).
    struct FailingAt {
        fail_at: usize,
        calls: AtomicUsize,
    }
    impl ManifestConsumer for FailingAt {
        type Output = ();
        type Acc = ();
        fn consume(&self, _manifest: &Manifest) -> RepositoryResult<()> {
            let n = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
            if n == self.fail_at {
                Err(RepositoryErrorKind::Other("boom".to_string())).capture()
            } else {
                Ok(())
            }
        }
        fn fold(_acc: &mut (), _output: ()) {}
    }

    fn limits(fetches: u16, mem_bytes: usize, decoders: u16) -> WalkLimits {
        WalkLimits {
            max_concurrent_manifest_fetches: NonZeroU16::new(fetches).unwrap(),
            max_manifest_mem_bytes: NonZeroUsize::new(mem_bytes).unwrap(),
            decode_workers: NonZeroU16::new(decoders).unwrap(),
        }
    }

    /// `repo_with_converging_refs` commits groups only, so its snapshots
    /// have no manifests. Add one array commit with chunks per snapshot we
    /// want manifests for.
    async fn repo_with_manifests(
        backend: &Arc<dyn Storage + Send + Sync>,
    ) -> Result<crate::Repository, Box<dyn std::error::Error>> {
        repo_with_n_manifests(backend, 3).await
    }

    /// One manifest per chunk-writing commit.
    async fn repo_with_n_manifests(
        backend: &Arc<dyn Storage + Send + Sync>,
        rounds: u8,
    ) -> Result<crate::Repository, Box<dyn std::error::Error>> {
        use crate::format::{ChunkIndices, Path, snapshot::ArrayShape};
        use bytes::Bytes;
        let repo = repo_with_converging_refs(backend).await?;
        let array_path: Path = "/arr".try_into()?;
        let mut session = repo.writable_session("main").await?;
        session
            .add_array(
                array_path.clone(),
                ArrayShape::new([(8, 8)]).unwrap(),
                None,
                Bytes::from_static(br#"{"zarr_format":3}"#),
            )
            .await?;
        session.commit("array").max_concurrent_nodes(8).execute().await?;
        for round in 0..rounds {
            let mut session = repo.writable_session("main").await?;
            for i in 0..8u32 {
                let payload =
                    session.get_chunk_writer()?(Bytes::from(vec![round; 64])).await?;
                session
                    .set_chunk_ref(
                        array_path.clone(),
                        ChunkIndices(vec![i]),
                        Some(payload),
                    )
                    .await?;
            }
            session.commit("chunks").max_concurrent_nodes(8).execute().await?;
        }
        Ok(repo)
    }

    async fn all_manifest_ids(
        asset_manager: &AssetManager,
    ) -> Result<HashSet<ManifestId>, Box<dyn std::error::Error>> {
        Ok(asset_manager
            .list_manifests()
            .await?
            .map_ok(|info| info.id)
            .try_collect()
            .await?)
    }

    #[tokio::test]
    async fn every_manifest_is_fetched_exactly_once()
    -> Result<(), Box<dyn std::error::Error>> {
        let backend: Arc<dyn Storage + Send + Sync> = new_in_memory_storage().await?;
        let repo = repo_with_manifests(&backend).await?;
        let (logging, am) = logging_asset_manager(
            &backend,
            repo.storage_settings().clone(),
            SpecVersionBin::V2,
        );
        let expected = all_manifest_ids(am.as_ref()).await?;
        assert!(expected.len() >= 3);

        let extra_roots = HashSet::new();
        let snaps = pointed_snapshots(
            Arc::clone(&am),
            None,
            &extra_roots,
            NonZeroU16::new(8).unwrap(),
        )
        .await?;
        let result = walk_manifests(
            Arc::clone(&am),
            limits(4, 64 * 1024 * 1024, 4),
            Arc::new(Counting),
            snaps,
        )
        .await?;

        assert_eq!(result.manifests, expected);
        let folded: HashSet<ManifestId> = result.acc.iter().cloned().collect();
        assert_eq!(folded, expected);
        assert_eq!(result.acc.len(), expected.len(), "a manifest was consumed twice");
        // 5 group commits + initial + 1 array commit + 3 chunk commits
        assert_eq!(result.snapshots.len(), 10);

        // distinct paths: a large object can be read as several ranges, but
        // every manifest path must appear, and each id appears once in the
        // walker's own bookkeeping asserted above
        let manifest_paths: HashSet<String> = logging
            .fetch_operations()
            .into_iter()
            .filter(|(op, path)| {
                op == "get_object_range" && path.starts_with(MANIFESTS_FILE_PATH)
            })
            .map(|(_, path)| path)
            .collect();
        assert_eq!(manifest_paths.len(), expected.len());
        Ok(())
    }

    #[tokio::test]
    async fn tiny_budget_and_single_workers_still_complete()
    -> Result<(), Box<dyn std::error::Error>> {
        let backend: Arc<dyn Storage + Send + Sync> = new_in_memory_storage().await?;
        let repo = repo_with_manifests(&backend).await?;
        let am = Arc::clone(repo.asset_manager());
        let expected = all_manifest_ids(am.as_ref()).await?;

        let extra_roots = HashSet::new();
        let snaps = pointed_snapshots(
            Arc::clone(&am),
            None,
            &extra_roots,
            NonZeroU16::new(8).unwrap(),
        )
        .await?;
        let result = tokio::time::timeout(
            Duration::from_secs(10),
            walk_manifests(Arc::clone(&am), limits(1, 1, 1), Arc::new(Counting), snaps),
        )
        .await??;
        assert_eq!(result.manifests, expected);
        assert_eq!(result.acc.len(), expected.len());
        Ok(())
    }

    #[tokio::test]
    async fn consumer_error_aborts_the_walk() -> Result<(), Box<dyn std::error::Error>> {
        let backend: Arc<dyn Storage + Send + Sync> = new_in_memory_storage().await?;
        let repo = repo_with_manifests(&backend).await?;
        let am = Arc::clone(repo.asset_manager());

        let extra_roots = HashSet::new();
        let snaps = pointed_snapshots(
            Arc::clone(&am),
            None,
            &extra_roots,
            NonZeroU16::new(8).unwrap(),
        )
        .await?;
        let consumer = Arc::new(FailingAt { fail_at: 2, calls: AtomicUsize::new(0) });
        let result = tokio::time::timeout(
            Duration::from_secs(10),
            walk_manifests(
                Arc::clone(&am),
                limits(4, 64 * 1024 * 1024, 4),
                consumer,
                snaps,
            ),
        )
        .await?;
        match result {
            Err(RepositoryError { kind: RepositoryErrorKind::Other(msg), .. }) => {
                assert_eq!(msg, "boom");
            }
            other => panic!("expected the consumer error, got {other:?}"),
        }
        Ok(())
    }

    #[tokio::test]
    async fn snapshot_stream_error_aborts_the_walk()
    -> Result<(), Box<dyn std::error::Error>> {
        let backend: Arc<dyn Storage + Send + Sync> = new_in_memory_storage().await?;
        let repo = repo_with_manifests(&backend).await?;
        let am = Arc::clone(repo.asset_manager());
        let snaps =
            futures::stream::iter([Err::<Arc<Snapshot>, _>(RepositoryError::capture(
                RepositoryErrorKind::Other("bad stream".to_string()),
            ))]);
        let result = tokio::time::timeout(
            Duration::from_secs(10),
            walk_manifests(am, limits(2, 1024, 2), Arc::new(Counting), snaps),
        )
        .await?;
        assert!(result.is_err());
        Ok(())
    }

    /// A panicking `fold` kills the fold task. Every worker must then see its
    /// channel close and exit, so the walk returns the panic instead of
    /// hanging on a `send` nobody will ever receive.
    #[tokio::test]
    async fn panicking_fold_aborts_the_walk_instead_of_hanging()
    -> Result<(), Box<dyn std::error::Error>> {
        struct PanickingFold;
        static FOLDS: AtomicUsize = AtomicUsize::new(0);
        impl ManifestConsumer for PanickingFold {
            type Output = ();
            type Acc = ();
            fn consume(&self, _manifest: &Manifest) -> RepositoryResult<()> {
                Ok(())
            }
            fn fold(_acc: &mut (), _output: ()) {
                if FOLDS.fetch_add(1, Ordering::SeqCst) + 1 == 2 {
                    panic!("fold boom");
                }
            }
        }

        // 12 manifests, 8 fetch workers: the infos channel (capacity 16) takes
        // the whole feed, so the feeder is done before the second fold panics
        // and the fetch workers are left with more bytes than their channel
        // (capacity 8) can hold. If the walker kept the receivers alive, those
        // sends would never fail and this test would time out.
        let backend: Arc<dyn Storage + Send + Sync> = new_in_memory_storage().await?;
        let repo = repo_with_n_manifests(&backend, 12).await?;
        let am = Arc::clone(repo.asset_manager());
        let extra_roots = HashSet::new();
        let snaps = pointed_snapshots(
            Arc::clone(&am),
            None,
            &extra_roots,
            NonZeroU16::new(8).unwrap(),
        )
        .await?;
        let result = tokio::time::timeout(
            Duration::from_secs(10),
            walk_manifests(am, limits(8, 1024 * 1024, 1), Arc::new(PanickingFold), snaps),
        )
        .await?;
        assert_panic_from_the_fold_task(result);
        Ok(())
    }

    /// Same panic, but with one worker per stage and channels of two, so the
    /// feeder is still sending when the fold task dies and the walk reports the
    /// panic through the `select!` arm instead of the final `folder.await`.
    #[tokio::test]
    async fn panicking_fold_while_still_feeding_aborts_the_walk()
    -> Result<(), Box<dyn std::error::Error>> {
        struct PanicOnFirstFold;
        impl ManifestConsumer for PanicOnFirstFold {
            type Output = ();
            type Acc = ();
            fn consume(&self, _manifest: &Manifest) -> RepositoryResult<()> {
                Ok(())
            }
            fn fold(_acc: &mut (), _output: ()) {
                panic!("fold boom");
            }
        }

        let backend: Arc<dyn Storage + Send + Sync> = new_in_memory_storage().await?;
        let repo = repo_with_n_manifests(&backend, 12).await?;
        let am = Arc::clone(repo.asset_manager());
        let extra_roots = HashSet::new();
        let snaps = pointed_snapshots(
            Arc::clone(&am),
            None,
            &extra_roots,
            NonZeroU16::new(8).unwrap(),
        )
        .await?;
        let result = tokio::time::timeout(
            Duration::from_secs(10),
            walk_manifests(am, limits(1, 1024, 1), Arc::new(PanicOnFirstFold), snaps),
        )
        .await?;
        assert_panic_from_the_fold_task(result);
        Ok(())
    }

    fn assert_panic_from_the_fold_task<Acc: std::fmt::Debug>(
        result: RepositoryResult<WalkResult<Acc>>,
    ) {
        match result {
            Err(RepositoryError {
                kind: RepositoryErrorKind::ConcurrencyError(join_error),
                ..
            }) => assert!(join_error.is_panic(), "{join_error:?}"),
            other => panic!("expected the fold task's panic, got {other:?}"),
        }
    }

    /// Channels small enough that the feeder is still sending when the
    /// consumer fails: the error must come back through the `select!` arm.
    #[tokio::test]
    async fn consumer_error_while_still_feeding_aborts_the_walk()
    -> Result<(), Box<dyn std::error::Error>> {
        let backend: Arc<dyn Storage + Send + Sync> = new_in_memory_storage().await?;
        let repo = repo_with_manifests(&backend).await?;
        let am = Arc::clone(repo.asset_manager());
        let extra_roots = HashSet::new();
        let snaps = pointed_snapshots(
            Arc::clone(&am),
            None,
            &extra_roots,
            NonZeroU16::new(8).unwrap(),
        )
        .await?;
        let consumer = Arc::new(FailingAt { fail_at: 1, calls: AtomicUsize::new(0) });
        let result = tokio::time::timeout(
            Duration::from_secs(10),
            walk_manifests(am, limits(1, 1024 * 1024, 1), consumer, snaps),
        )
        .await?;
        match result {
            Err(RepositoryError { kind: RepositoryErrorKind::Other(msg), .. }) => {
                assert_eq!(msg, "boom");
            }
            other => panic!("expected the consumer error, got {other:?}"),
        }
        Ok(())
    }

    #[test]
    fn permits_are_clamped_to_the_budget() {
        assert_eq!(permits_for(0, 100), 1);
        assert_eq!(permits_for(1, 100), 1);
        assert_eq!(permits_for(1024, 100), 1);
        assert_eq!(permits_for(1025, 100), 2);
        assert_eq!(permits_for(1 << 40, 100), 100);
        assert_eq!(total_permits(NonZeroUsize::new(1).unwrap()), 1);
        assert_eq!(
            total_permits(NonZeroUsize::new(512 * 1024 * 1024).unwrap()),
            512 * 1024
        );
    }
}
