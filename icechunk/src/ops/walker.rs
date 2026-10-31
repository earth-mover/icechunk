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
    sync::{
        Arc, Weak,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use futures::{Stream, StreamExt as _};
use tokio::{
    sync::{Mutex, OwnedSemaphorePermit, Semaphore, mpsc},
    task::{AbortHandle, JoinSet},
};
use tracing::{info, instrument};

use crate::{
    asset_manager::{AssetManager, decode_manifest, frame_content_size},
    format::{
        ManifestId, SnapshotId, format_constants,
        manifest::Manifest,
        snapshot::{ManifestFileInfo, Snapshot},
    },
    ops::AbortOnDrop,
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
    /// A label and value to include in the walk's progress logs, if the
    /// consumer has one worth watching. Called once per report, not per
    /// manifest, so an implementation may take locks.
    fn progress(&self) -> Option<(&'static str, u64)> {
        None
    }
}

/// How often a long-running walker logs its progress.
pub(crate) const PROGRESS_INTERVAL: Duration = Duration::from_secs(10);

const MIB: f64 = (1024 * 1024) as f64;

fn round1(value: f64) -> f64 {
    (value * 10.0).round() / 10.0
}

fn mib(bytes: u64) -> f64 {
    round1(bytes as f64 / MIB)
}

/// MiB/s over a window, `0.0` for a window of no time.
fn rate_mib_per_s(bytes_delta: u64, secs: f64) -> f64 {
    if secs <= 0.0 { 0.0 } else { round1(bytes_delta as f64 / MIB / secs) }
}

/// A snapshot of [`WalkProgress`], as plain numbers.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct WalkStats {
    /// Distinct snapshots the input stream yielded.
    pub snapshots_seen: u64,
    /// Distinct manifest ids the feeder forwarded.
    pub manifests_seen: u64,
    /// Their `size_bytes`, summed.
    pub bytes_seen: u64,
    pub manifests_fetched: u64,
    /// Compressed bytes actually read from storage.
    pub bytes_fetched: u64,
    pub manifests_consumed: u64,
    /// The most decoded manifest bytes reserved at once, as sampled when a
    /// decode worker takes its permits: what the decoded budget actually held.
    pub peak_decoded_reserved_bytes: u64,
    /// Decoded bytes per chunk ref, as learned from the manifests decoded so
    /// far; what the walk assumes for frames that don't declare their size.
    pub decoded_bytes_per_ref: u64,
    /// The largest manifest decoded, in decoded bytes.
    pub max_decoded_bytes: u64,
}

/// Live counters shared by the feeder, the fetch workers and the decode
/// workers, read by the progress reporter.
#[derive(Debug, Default)]
struct WalkProgress {
    snapshots_seen: AtomicU64,
    manifests_seen: AtomicU64,
    bytes_seen: AtomicU64,
    manifests_fetched: AtomicU64,
    bytes_fetched: AtomicU64,
    manifests_consumed: AtomicU64,
    /// Most decoded bytes reserved at once, sampled on every reservation.
    peak_decoded_reserved_bytes: AtomicU64,
    /// What the decode workers have learned about decoded sizes.
    decoded: DecodedSizeEstimate,
}

impl WalkProgress {
    fn read(&self) -> WalkStats {
        WalkStats {
            snapshots_seen: self.snapshots_seen.load(Ordering::Relaxed),
            manifests_seen: self.manifests_seen.load(Ordering::Relaxed),
            bytes_seen: self.bytes_seen.load(Ordering::Relaxed),
            manifests_fetched: self.manifests_fetched.load(Ordering::Relaxed),
            bytes_fetched: self.bytes_fetched.load(Ordering::Relaxed),
            manifests_consumed: self.manifests_consumed.load(Ordering::Relaxed),
            peak_decoded_reserved_bytes: self
                .peak_decoded_reserved_bytes
                .load(Ordering::Relaxed),
            decoded_bytes_per_ref: self.decoded.bytes_per_ref(),
            max_decoded_bytes: self.decoded.max_decoded_bytes.load(Ordering::Relaxed),
        }
    }
}

/// Decoded bytes per chunk ref, learned from decoded manifests, for frames that
/// do not declare their size. Only manifests with at least
/// `MIN_REFS_FOR_ESTIMATE` refs count, so tiny manifests' fixed overhead does
/// not inflate the figure. Until `MIN_SAMPLES_FOR_ESTIMATE` of those have been
/// decoded the generous `INITIAL_DECODED_BYTES_PER_REF` seed applies; after
/// that it is the observed max alone, so a repo whose manifests decode smaller
/// than the seed stops over-reserving. Past that point the estimate can only be
/// wrong if a later manifest exceeds every earlier one, which raises the max
/// for the rest of the walk.
#[derive(Debug, Default)]
struct DecodedSizeEstimate {
    /// Running max over sampled manifests only; 0 until the first sample.
    bytes_per_ref: AtomicU64,
    max_decoded_bytes: AtomicU64,
    /// How many manifests have contributed to `bytes_per_ref`.
    samples: AtomicU64,
}

const INITIAL_DECODED_BYTES_PER_REF: u64 = 256;
const MIN_REFS_FOR_ESTIMATE: u32 = 1_000;
const MIN_SAMPLES_FOR_ESTIMATE: u64 = 8;

impl DecodedSizeEstimate {
    /// Decoded bytes per ref the walk is currently assuming: the seed until
    /// enough manifests have been measured, the observed max after that.
    fn bytes_per_ref(&self) -> u64 {
        // Acquire/Release pair with `record`, read in the opposite order to the
        // writes: a reader that sees a sample also sees the max it raised, so
        // the seed is never dropped in favour of a stale maximum.
        let samples = self.samples.load(Ordering::Acquire);
        let observed = self.bytes_per_ref.load(Ordering::Acquire);
        if samples >= MIN_SAMPLES_FOR_ESTIMATE {
            observed
        } else {
            observed.max(INITIAL_DECODED_BYTES_PER_REF)
        }
    }

    /// Bytes to reserve before decoding `info`'s frame, never more than
    /// `budget_bytes`: the reservation is clamped to the whole budget anyway,
    /// and this figure also sizes the decode buffer.
    fn estimate(&self, info: &ManifestFileInfo, frame: &[u8], budget_bytes: u64) -> u64 {
        let estimate = match frame_content_size(frame) {
            Some(size) => size,
            None => {
                u64::from(info.num_chunk_refs).max(1).saturating_mul(self.bytes_per_ref())
            }
        };
        estimate.min(budget_bytes)
    }

    fn record(&self, info: &ManifestFileInfo, decoded_len: u64) {
        self.max_decoded_bytes.fetch_max(decoded_len, Ordering::Relaxed);
        if info.num_chunk_refs >= MIN_REFS_FOR_ESTIMATE {
            let per_ref = decoded_len.div_ceil(u64::from(info.num_chunk_refs));
            self.bytes_per_ref.fetch_max(per_ref, Ordering::Release);
            self.samples.fetch_add(1, Ordering::Release);
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct WalkLimits {
    pub max_concurrent_manifest_fetches: NonZeroU16,
    /// Budget for compressed manifest bytes in flight: fetched, not yet consumed.
    pub max_manifest_mem_bytes: NonZeroUsize,
    /// Budget for decoded manifest bytes: manifests being decoded or consumed.
    /// Manifests can expand many times over on decompression, depending on
    /// their content, so this is usually the term that dominates memory. The
    /// walk's manifest memory is about
    /// `max_manifest_mem_bytes + max_decoded_manifest_mem_bytes`.
    pub max_decoded_manifest_mem_bytes: NonZeroUsize,
    pub decode_workers: NonZeroU16,
}

#[derive(Debug)]
pub struct WalkResult<Acc> {
    pub acc: Acc,
    /// Every snapshot the input stream yielded.
    pub snapshots: HashSet<SnapshotId>,
    /// Every manifest referenced by those snapshots; each was fetched once.
    pub manifests: HashSet<ManifestId>,
    /// The walk's final counters.
    pub progress: WalkStats,
}

const KIB: usize = 1024;

struct Fetched {
    info: ManifestFileInfo,
    bytes: Vec<u8>,
    /// Released by the decode worker once the bytes are decompressed; the
    /// decoded manifest is covered by the decoded budget from then on.
    _permit: OwnedSemaphorePermit,
}

type SharedRx<T> = Arc<Mutex<mpsc::Receiver<T>>>;

/// KiB permits for a manifest of `size_bytes`, clamped so a manifest larger
/// than the whole budget still runs (alone, once everything else drains).
fn permits_for(size_bytes: u64, total_permits: u32) -> u32 {
    let kib = (size_bytes as usize).div_ceil(KIB).max(1);
    kib.min(total_permits as usize) as u32
}

/// The budget as the semaphore models it: whole KiB, at least one.
fn budget_bytes(total_permits: u32) -> u64 {
    u64::from(total_permits) * KIB as u64
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
    progress: Arc<WalkProgress>,
) -> RepositoryResult<()> {
    while let Some(info) = recv_shared(&rx).await {
        let permit = Arc::clone(&budget)
            .acquire_many_owned(permits_for(info.size_bytes, total_permits))
            .await
            .capture()?;
        let bytes = asset_manager.fetch_manifest_bytes(&info.id, info.size_bytes).await?;
        progress.manifests_fetched.fetch_add(1, Ordering::Relaxed);
        progress.bytes_fetched.fetch_add(bytes.len() as u64, Ordering::Relaxed);
        if tx.send(Fetched { info, bytes, _permit: permit }).await.is_err() {
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
    decoded_budget: Arc<Semaphore>,
    decoded_total_permits: u32,
    progress: Arc<WalkProgress>,
) -> RepositoryResult<()> {
    while let Some(fetched) = recv_shared(&rx).await {
        let header_len =
            format_constants::ICECHUNK_FILE_HEADER_LEN.min(fetched.bytes.len());
        let reserve = progress.decoded.estimate(
            &fetched.info,
            &fetched.bytes[header_len..],
            budget_bytes(decoded_total_permits),
        );
        // one acquire, clamped to the whole budget, so an oversized manifest
        // still runs (alone) and decoders never wait on each other in a cycle
        let decoded_permit = Arc::clone(&decoded_budget)
            .acquire_many_owned(permits_for(reserve, decoded_total_permits))
            .await
            .capture()?;
        progress.peak_decoded_reserved_bytes.fetch_max(
            reserved_bytes(&decoded_budget, decoded_total_permits),
            Ordering::Relaxed,
        );
        let consumer = Arc::clone(&consumer);
        let blocking_progress = Arc::clone(&progress);
        let span = tracing::Span::current();
        let output = tokio::task::spawn_blocking(move || {
            let _entered = span.entered();
            let Fetched { info, bytes, _permit: compressed_permit } = fetched;
            let manifest = decode_manifest(
                &bytes,
                Some(usize::try_from(reserve).unwrap_or(usize::MAX)),
            )?;
            // The compressed bytes are gone and the decoded manifest is covered
            // by `decoded_permit`, so the compressed budget can admit the next
            // fetch now instead of after `consume`. Holding it longer only
            // shortened the fetch pipeline for nothing.
            drop(bytes);
            drop(compressed_permit);
            blocking_progress.decoded.record(&info, manifest.bytes().len() as u64);
            let out = consumer.consume(&manifest)?;
            drop(manifest);
            drop(decoded_permit);
            Ok::<_, RepositoryError>(out)
        })
        .await
        .capture()??;
        progress.manifests_consumed.fetch_add(1, Ordering::Relaxed);
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
    progress: &WalkProgress,
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
        progress.snapshots_seen.fetch_add(1, Ordering::Relaxed);
        for info in snapshot.manifest_files() {
            let info = match info.inject() {
                Ok(info) => info,
                Err(err) => return FeedOutcome::Failed(err),
            };
            if !seen_manifests.insert(info.id.clone()) {
                continue;
            }
            progress.manifests_seen.fetch_add(1, Ordering::Relaxed);
            progress.bytes_seen.fetch_add(info.size_bytes, Ordering::Relaxed);
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

/// Decoded bytes the walk has reserved right now, from the permits it holds.
fn reserved_bytes(budget: &Semaphore, total_permits: u32) -> u64 {
    let held = (total_permits as usize).saturating_sub(budget.available_permits());
    (held * KIB) as u64
}

/// Log the walk's counters every [`PROGRESS_INTERVAL`], skipping ticks where
/// nothing moved. Runs until aborted by [`walk_manifests`].
async fn report_progress<C: ManifestConsumer>(
    progress: Arc<WalkProgress>,
    // a `Weak` so the reporter never keeps the consumer alive: callers unwrap
    // the consumer's `Arc` as soon as the walk returns
    consumer: Weak<C>,
    decoded_budget: Arc<Semaphore>,
    decoded_total_permits: u32,
    started: Instant,
) {
    let mut ticker = tokio::time::interval(PROGRESS_INTERVAL);
    // the first tick completes immediately
    ticker.tick().await;
    let mut last = WalkStats::default();
    let mut last_at = started;
    loop {
        ticker.tick().await;
        let now = progress.read();
        if now == last {
            continue;
        }
        let window = last_at.elapsed().as_secs_f64();
        // tracing field names are static, so metric needs to be a value
        let metric = consumer.upgrade().and_then(|c| c.progress());
        info!(
            snapshots = now.snapshots_seen,
            manifests_seen = now.manifests_seen,
            manifests_fetched = now.manifests_fetched,
            manifests_consumed = now.manifests_consumed,
            in_flight = now.manifests_fetched - now.manifests_consumed,
            seen_mib = mib(now.bytes_seen),
            fetched_mib = mib(now.bytes_fetched),
            mib_per_s = rate_mib_per_s(
                now.bytes_fetched.saturating_sub(last.bytes_fetched),
                window,
            ),
            decoded_bytes_per_ref = now.decoded_bytes_per_ref,
            max_decoded_mib = mib(now.max_decoded_bytes),
            decoded_in_flight_mib =
                mib(reserved_bytes(&decoded_budget, decoded_total_permits)),
            elapsed_s = round1(started.elapsed().as_secs_f64()),
            consumer_metric = metric.map(|(label, _)| label),
            consumer_value = metric.map(|(_, value)| value),
            "manifest walk progress"
        );
        last = now;
        last_at = Instant::now();
    }
}

/// Visit every manifest referenced by the `snapshots` stream once, running
/// `consumer` on each, within `limits`. Bytes are read straight from storage,
/// bypassing the asset manager's cache and its semaphores.
#[instrument(skip_all, fields(
    fetches = limits.max_concurrent_manifest_fetches.get(),
    decoders = limits.decode_workers.get(),
    mem_bytes = limits.max_manifest_mem_bytes.get(),
    decoded_mem_bytes = limits.max_decoded_manifest_mem_bytes.get(),
))]
pub async fn walk_manifests<C: ManifestConsumer>(
    asset_manager: Arc<AssetManager>,
    limits: WalkLimits,
    consumer: Arc<C>,
    snapshots: impl Stream<Item = RepositoryResult<Arc<Snapshot>>>,
) -> RepositoryResult<WalkResult<C::Acc>> {
    let fetchers = limits.max_concurrent_manifest_fetches.get() as usize;
    let decoders = limits.decode_workers.get() as usize;
    let decoded_total_permits = total_permits(limits.max_decoded_manifest_mem_bytes);
    let decoded_budget = Arc::new(Semaphore::new(decoded_total_permits as usize));
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

    let progress = Arc::new(WalkProgress::default());
    let started = Instant::now();
    // held in a guard so every return path below, success or error, aborts it
    let mut reporter = AbortOnDrop(tokio::spawn(report_progress(
        Arc::clone(&progress),
        Arc::downgrade(&consumer),
        Arc::clone(&decoded_budget),
        decoded_total_permits,
        started,
    )));

    let mut workers: JoinSet<RepositoryResult<()>> = JoinSet::new();
    let mut aborts: Vec<AbortHandle> = Vec::with_capacity(fetchers + decoders);
    for _ in 0..fetchers {
        aborts.push(workers.spawn(fetch_worker(
            Arc::clone(&asset_manager),
            Arc::clone(&infos_rx),
            Arc::clone(&budget),
            total_permits,
            bytes_tx.clone(),
            Arc::clone(&progress),
        )));
    }
    drop(bytes_tx);
    for _ in 0..decoders {
        aborts.push(workers.spawn(decode_worker(
            Arc::clone(&consumer),
            Arc::clone(&bytes_rx),
            out_tx.clone(),
            Arc::clone(&decoded_budget),
            decoded_total_permits,
            Arc::clone(&progress),
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
        &progress,
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
    // make sure the reporter has stopped
    reporter.abort_and_wait().await;
    let stats = progress.read();
    let elapsed_s = started.elapsed().as_secs_f64();
    info!(
        snapshots = stats.snapshots_seen,
        manifests_seen = stats.manifests_seen,
        manifests_fetched = stats.manifests_fetched,
        manifests_consumed = stats.manifests_consumed,
        seen_mib = mib(stats.bytes_seen),
        fetched_mib = mib(stats.bytes_fetched),
        mib_per_s = rate_mib_per_s(stats.bytes_fetched, elapsed_s),
        decoded_bytes_per_ref = stats.decoded_bytes_per_ref,
        max_decoded_mib = mib(stats.max_decoded_bytes),
        peak_decoded_reserved_mib = mib(stats.peak_decoded_reserved_bytes),
        elapsed_s = round1(elapsed_s),
        "manifest walk done"
    );
    Ok(WalkResult {
        acc,
        snapshots: seen_snapshots,
        manifests: seen_manifests,
        progress: stats,
    })
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

    /// Records what every manifest decoded to, so two walks over the same
    /// manifests can be compared byte for byte.
    struct Fingerprinting;
    impl ManifestConsumer for Fingerprinting {
        type Output = (ManifestId, u64);
        type Acc = HashSet<(ManifestId, u64)>;
        fn consume(&self, manifest: &Manifest) -> RepositoryResult<Self::Output> {
            Ok((manifest.id(), fnv1a(manifest.bytes())))
        }
        fn fold(acc: &mut Self::Acc, output: Self::Output) {
            acc.insert(output);
        }
    }

    fn fnv1a(bytes: &[u8]) -> u64 {
        bytes.iter().fold(0xcbf2_9ce4_8422_2325_u64, |hash, byte| {
            (hash ^ u64::from(*byte)).wrapping_mul(0x100_0000_01b3)
        })
    }

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

    fn limits(
        fetches: u16,
        mem_bytes: usize,
        decoders: u16,
        decoded_mem_bytes: usize,
    ) -> WalkLimits {
        WalkLimits {
            max_concurrent_manifest_fetches: NonZeroU16::new(fetches).unwrap(),
            max_manifest_mem_bytes: NonZeroUsize::new(mem_bytes).unwrap(),
            max_decoded_manifest_mem_bytes: NonZeroUsize::new(decoded_mem_bytes).unwrap(),
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
                let payload = session
                    .get_chunk_writer(&array_path, &ChunkIndices(vec![i]))?(
                    Bytes::from(vec![round; 64]),
                )
                .await?;
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

    /// One manifest of `refs` chunk refs per commit: wide enough that the
    /// estimator samples them, and rewritten whole on every later commit.
    async fn repo_with_wide_manifests(
        backend: &Arc<dyn Storage + Send + Sync>,
        refs: u32,
        rounds: u32,
    ) -> Result<crate::Repository, Box<dyn std::error::Error>> {
        use crate::format::{
            ChunkIndices, Path, manifest::ChunkPayload, snapshot::ArrayShape,
        };
        use bytes::Bytes;
        let repo = repo_with_converging_refs(backend).await?;
        let array_path: Path = "/wide".try_into()?;
        let mut session = repo.writable_session("main").await?;
        session
            .add_array(
                array_path.clone(),
                ArrayShape::new([(u64::from(refs), refs)]).unwrap(),
                None,
                Bytes::from_static(br#"{"zarr_format":3}"#),
            )
            .await?;
        for i in 0..refs {
            session
                .set_chunk_ref(
                    array_path.clone(),
                    ChunkIndices(vec![i]),
                    Some(ChunkPayload::Inline(Bytes::from(vec![i as u8; 16]))),
                )
                .await?;
        }
        session.commit("wide").max_concurrent_nodes(8).execute().await?;
        for round in 0..rounds {
            let mut session = repo.writable_session("main").await?;
            session
                .set_chunk_ref(
                    array_path.clone(),
                    ChunkIndices(vec![round]),
                    Some(ChunkPayload::Inline(Bytes::from(vec![round as u8; 16]))),
                )
                .await?;
            session.commit("touch").max_concurrent_nodes(8).execute().await?;
        }
        Ok(repo)
    }

    async fn snapshot_stream<'a>(
        asset_manager: &Arc<AssetManager>,
        extra_roots: &'a HashSet<SnapshotId>,
    ) -> RepositoryResult<impl Stream<Item = RepositoryResult<Arc<Snapshot>>> + 'a> {
        pointed_snapshots(
            Arc::clone(asset_manager),
            None,
            extra_roots,
            NonZeroU16::new(8).unwrap(),
        )
        .await
    }

    /// Every manifest the pointed snapshots reference, deduplicated.
    async fn unique_manifest_infos(
        asset_manager: &Arc<AssetManager>,
    ) -> Result<Vec<ManifestFileInfo>, Box<dyn std::error::Error>> {
        let extra_roots = HashSet::new();
        let snaps: Vec<_> =
            snapshot_stream(asset_manager, &extra_roots).await?.try_collect().await?;
        let mut seen = HashSet::new();
        let mut infos = Vec::new();
        for snap in &snaps {
            for info in snap.manifest_files() {
                let info = info?;
                if seen.insert(info.id.clone()) {
                    infos.push(info);
                }
            }
        }
        Ok(infos)
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
            limits(4, 64 * 1024 * 1024, 4, 64 * 1024 * 1024),
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

        // the live counters must land exactly on the walk's own results
        let stats = result.progress;
        assert_eq!(stats.snapshots_seen, result.snapshots.len() as u64);
        assert_eq!(stats.manifests_seen, expected.len() as u64);
        assert_eq!(stats.manifests_fetched, expected.len() as u64);
        assert_eq!(stats.manifests_consumed, expected.len() as u64);
        assert!(stats.bytes_seen > 0);
        assert_eq!(
            stats.bytes_fetched, stats.bytes_seen,
            "every manifest is read whole, so fetched bytes match the sizes the \
             snapshots advertised"
        );

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
            walk_manifests(
                Arc::clone(&am),
                limits(1, 1, 1, 1),
                Arc::new(Counting),
                snaps,
            ),
        )
        .await??;
        assert_eq!(result.manifests, expected);
        assert_eq!(result.acc.len(), expected.len());
        Ok(())
    }

    #[tokio::test]
    async fn tiny_decoded_budget_still_completes()
    -> Result<(), Box<dyn std::error::Error>> {
        let backend: Arc<dyn Storage + Send + Sync> = new_in_memory_storage().await?;
        let repo = repo_with_manifests(&backend).await?;
        let am = Arc::clone(repo.asset_manager());
        let expected = all_manifest_ids(am.as_ref()).await?;
        let extra_roots = HashSet::new();

        // a budget of whole KiB, larger than any of these manifests: nothing is
        // clamped, so what the walk held must stay inside it
        let budget = 8 * KIB;
        let result = tokio::time::timeout(
            Duration::from_secs(10),
            walk_manifests(
                Arc::clone(&am),
                limits(4, 64 * 1024 * 1024, 4, budget),
                Arc::new(Counting),
                snapshot_stream(&am, &extra_roots).await?,
            ),
        )
        .await??;
        assert_eq!(result.manifests, expected);
        let peak = result.progress.peak_decoded_reserved_bytes;
        assert!(peak > 0, "the walk reserved nothing");
        assert!(peak <= budget as u64, "peak {peak} exceeded the budget {budget}");

        // a budget below a single manifest: each one runs alone, holding the
        // one permit the budget rounds up to
        let result = tokio::time::timeout(
            Duration::from_secs(10),
            walk_manifests(
                Arc::clone(&am),
                limits(4, 64 * 1024 * 1024, 4, 1),
                Arc::new(Counting),
                snapshot_stream(&am, &extra_roots).await?,
            ),
        )
        .await??;
        assert_eq!(result.manifests, expected);
        assert_eq!(result.progress.peak_decoded_reserved_bytes, KIB as u64);
        Ok(())
    }

    /// A consumer failure must release the decoded permit the worker holds.
    /// With a budget of one permit, a leak stops every later decode and the
    /// walk hangs instead of returning the error.
    #[tokio::test]
    async fn consumer_error_releases_the_decoded_budget()
    -> Result<(), Box<dyn std::error::Error>> {
        let backend: Arc<dyn Storage + Send + Sync> = new_in_memory_storage().await?;
        let repo = repo_with_n_manifests(&backend, 6).await?;
        let am = Arc::clone(repo.asset_manager());
        let extra_roots = HashSet::new();
        let consumer = Arc::new(FailingAt { fail_at: 2, calls: AtomicUsize::new(0) });
        let result = tokio::time::timeout(
            Duration::from_secs(10),
            walk_manifests(
                Arc::clone(&am),
                limits(4, 64 * 1024 * 1024, 4, 1),
                consumer,
                snapshot_stream(&am, &extra_roots).await?,
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

    /// Rewrite every manifest object with a frame that does not declare its
    /// decoded size, as every writer before this one produced. Snapshots record
    /// the object's size and readers ask for exactly that range, so the
    /// rewritten object is padded back to it with a zstd skippable frame, which
    /// every decoder ignores.
    async fn drop_declared_manifest_sizes(
        backend: &Arc<dyn Storage + Send + Sync>,
        repo: &crate::Repository,
        infos: &[ManifestFileInfo],
    ) -> Result<(), Box<dyn std::error::Error>> {
        use crate::{
            asset_manager::compress_without_content_size,
            format::format_constants::{FileTypeBin, parse_file_header},
        };
        let am = repo.asset_manager();
        for info in infos {
            let bytes = am.fetch_manifest_bytes(&info.id, info.size_bytes).await?;
            let spec_version = parse_file_header(&bytes)?.spec_version;
            let manifest = decode_manifest(&bytes, None)?;
            let mut legacy = compress_without_content_size(
                manifest.bytes(),
                spec_version,
                FileTypeBin::Manifest,
                22,
            )?;
            let padding = info
                .size_bytes
                .checked_sub(legacy.len() as u64)
                .ok_or("the rewritten manifest outgrew the recorded size")?;
            if padding > 0 {
                // skippable frame: magic, then its payload length
                let payload =
                    u32::try_from(padding.checked_sub(8).ok_or("no room to pad")?)?;
                legacy.extend_from_slice(&0x184D_2A50_u32.to_le_bytes());
                legacy.extend_from_slice(&payload.to_le_bytes());
                legacy.resize(info.size_bytes as usize, 0);
            }
            backend
                .put_object(
                    &repo.storage_context(),
                    &format!("{MANIFESTS_FILE_PATH}/{}", info.id),
                    legacy.into(),
                    None,
                    Vec::new(),
                    None,
                )
                .await?;
        }
        Ok(())
    }

    /// Manifests written before the frame recorded its decoded size take the
    /// estimator and size-hint path. A walk over them must decode exactly what
    /// a walk over the same manifests written today does.
    #[tokio::test]
    async fn manifests_without_a_declared_size_walk_the_same()
    -> Result<(), Box<dyn std::error::Error>> {
        let backend: Arc<dyn Storage + Send + Sync> = new_in_memory_storage().await?;
        let repo = repo_with_wide_manifests(&backend, 1_000, 8).await?;
        let am = Arc::clone(repo.asset_manager());
        let extra_roots = HashSet::new();
        let infos = unique_manifest_infos(&am).await?;
        assert!(infos.len() as u64 >= MIN_SAMPLES_FOR_ESTIMATE, "{}", infos.len());
        assert!(infos.iter().all(|i| i.num_chunk_refs >= MIN_REFS_FOR_ESTIMATE));

        // what the estimator must learn once the seed is out of the way
        let mut expected_per_ref = 0;
        for info in &infos {
            let bytes = am.fetch_manifest_bytes(&info.id, info.size_bytes).await?;
            let decoded = decode_manifest(&bytes, None)?.bytes().len() as u64;
            expected_per_ref =
                expected_per_ref.max(decoded.div_ceil(u64::from(info.num_chunk_refs)));
        }
        assert_ne!(
            expected_per_ref, INITIAL_DECODED_BYTES_PER_REF,
            "the seed would hide whether anything was learned"
        );

        let modern = walk_manifests(
            Arc::clone(&am),
            limits(4, 64 * 1024 * 1024, 4, 64 * 1024 * 1024),
            Arc::new(Fingerprinting),
            snapshot_stream(&am, &extra_roots).await?,
        )
        .await?;
        assert_eq!(modern.progress.decoded_bytes_per_ref, expected_per_ref);

        drop_declared_manifest_sizes(&backend, &repo, &infos).await?;

        // one manifest, decoded straight from its bytes with an estimate
        let info = infos.first().ok_or("no manifests")?;
        let bytes = am.fetch_manifest_bytes(&info.id, info.size_bytes).await?;
        let frame = &bytes[format_constants::ICECHUNK_FILE_HEADER_LEN..];
        assert_eq!(frame_content_size(frame), None, "the rewrite kept a declared size");
        let hint = u64::from(info.num_chunk_refs) * INITIAL_DECODED_BYTES_PER_REF;
        let manifest = decode_manifest(&bytes, Some(hint as usize))?;
        assert!(modern.acc.contains(&(manifest.id(), fnv1a(manifest.bytes()))));

        // and the whole walk
        let legacy = walk_manifests(
            Arc::clone(&am),
            limits(4, 64 * 1024 * 1024, 4, 64 * 1024 * 1024),
            Arc::new(Fingerprinting),
            snapshot_stream(&am, &extra_roots).await?,
        )
        .await?;
        assert_eq!(legacy.acc, modern.acc);
        assert_eq!(legacy.manifests, modern.manifests);
        assert_eq!(legacy.progress.decoded_bytes_per_ref, expected_per_ref);
        assert_eq!(legacy.progress.max_decoded_bytes, modern.progress.max_decoded_bytes);
        Ok(())
    }

    #[tokio::test]
    async fn walk_reports_decoded_sizes() -> Result<(), Box<dyn std::error::Error>> {
        let backend: Arc<dyn Storage + Send + Sync> = new_in_memory_storage().await?;
        let repo = repo_with_manifests(&backend).await?;
        let am = Arc::clone(repo.asset_manager());
        // expected max decoded size, measured directly
        let mut expected_max = 0u64;
        let extra_roots = HashSet::new();
        let snaps: Vec<_> = pointed_snapshots(
            Arc::clone(&am),
            None,
            &extra_roots,
            NonZeroU16::new(8).unwrap(),
        )
        .await?
        .try_collect()
        .await?;
        let mut seen = HashSet::new();
        for snap in &snaps {
            for info in snap.manifest_files() {
                let info = info?;
                if !seen.insert(info.id.clone()) {
                    continue;
                }
                let bytes = am.fetch_manifest_bytes(&info.id, info.size_bytes).await?;
                let m = decode_manifest(&bytes, None)?;
                expected_max = expected_max.max(m.bytes().len() as u64);
            }
        }
        let snaps = pointed_snapshots(
            Arc::clone(&am),
            None,
            &extra_roots,
            NonZeroU16::new(8).unwrap(),
        )
        .await?;
        let result = walk_manifests(
            am,
            limits(4, 64 * 1024 * 1024, 4, 64 * 1024 * 1024),
            Arc::new(Counting),
            snaps,
        )
        .await?;
        assert_eq!(result.progress.max_decoded_bytes, expected_max);
        // every manifest here holds 8 refs, far below MIN_REFS_FOR_ESTIMATE, so
        // none of them is sampled and the walk keeps assuming the seed
        assert_eq!(result.progress.decoded_bytes_per_ref, INITIAL_DECODED_BYTES_PER_REF);
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
                limits(4, 64 * 1024 * 1024, 4, 64 * 1024 * 1024),
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
            walk_manifests(
                am,
                limits(2, 1024, 2, 64 * 1024 * 1024),
                Arc::new(Counting),
                snaps,
            ),
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
            walk_manifests(
                am,
                limits(8, 1024 * 1024, 1, 64 * 1024 * 1024),
                Arc::new(PanickingFold),
                snaps,
            ),
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
            walk_manifests(
                am,
                limits(1, 1024, 1, 64 * 1024 * 1024),
                Arc::new(PanicOnFirstFold),
                snaps,
            ),
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
            walk_manifests(
                am,
                limits(1, 1024 * 1024, 1, 64 * 1024 * 1024),
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

    /// The seed only covers the walk until enough manifests have been
    /// measured; after that the observed max stands on its own, in either
    /// direction.
    #[test]
    fn the_decoded_size_seed_gives_way_to_what_was_measured() {
        // a frame with no declared size, so the estimate does the work
        let frame = {
            let mut enc =
                zstd::stream::write::Encoder::new(Vec::new(), 1).expect("zstd encoder");
            std::io::Write::write_all(&mut enc, b"unused").expect("write");
            enc.finish().expect("finish")
        };
        let info = |num_chunk_refs: u32| ManifestFileInfo {
            id: ManifestId::random(),
            size_bytes: 1,
            num_chunk_refs,
        };
        let big = info(MIN_REFS_FOR_ESTIMATE);
        let refs = u64::from(MIN_REFS_FOR_ESTIMATE);

        let unbounded = u64::MAX;

        let estimate = DecodedSizeEstimate::default();
        // one short of enough samples: the seed still applies
        for _ in 0..MIN_SAMPLES_FOR_ESTIMATE - 1 {
            estimate.record(&big, 100 * refs);
        }
        assert_eq!(estimate.estimate(&big, &frame, unbounded), 256 * refs);

        // the last sample lets the measured figure replace the seed
        estimate.record(&big, 100 * refs);
        assert_eq!(estimate.estimate(&big, &frame, unbounded), 100 * refs);

        // a bigger manifest still raises the max for the rest of the walk
        estimate.record(&big, 300 * refs);
        assert_eq!(estimate.estimate(&big, &frame, unbounded), 300 * refs);

        // manifests below the ref threshold never feed bytes-per-ref, so the
        // seed still stands; they do raise `max_decoded_bytes`
        let estimate = DecodedSizeEstimate::default();
        let small = info(MIN_REFS_FOR_ESTIMATE - 1);
        let small_decoded = 10_000 * u64::from(small.num_chunk_refs);
        for _ in 0..2 * MIN_SAMPLES_FOR_ESTIMATE {
            estimate.record(&small, small_decoded);
        }
        assert_eq!(estimate.estimate(&big, &frame, unbounded), 256 * refs);
        assert_eq!(estimate.max_decoded_bytes.load(Ordering::Relaxed), small_decoded);
    }

    /// Nothing the estimator produces may exceed the budget it is reserving
    /// from, however wrong the figures it is built on.
    #[test]
    fn the_estimate_never_exceeds_the_budget() {
        let frame = {
            let mut enc =
                zstd::stream::write::Encoder::new(Vec::new(), 1).expect("zstd encoder");
            std::io::Write::write_all(&mut enc, b"unused").expect("write");
            enc.finish().expect("finish")
        };
        let budget = 8 * KIB as u64;
        let estimate = DecodedSizeEstimate::default();
        let widest = ManifestFileInfo {
            id: ManifestId::random(),
            size_bytes: 1,
            num_chunk_refs: u32::MAX,
        };
        // the seed alone already overflows the budget
        assert_eq!(estimate.estimate(&widest, &frame, budget), budget);
        // and a bytes-per-ref this large overflows the multiplication itself
        estimate.record(
            &ManifestFileInfo {
                id: ManifestId::random(),
                size_bytes: 1,
                num_chunk_refs: MIN_REFS_FOR_ESTIMATE,
            },
            u64::MAX,
        );
        assert_eq!(estimate.estimate(&widest, &frame, budget), budget);
        // a declared size larger than the budget is clamped the same way
        let declared = zstd::bulk::compress(&vec![0u8; 100 * KIB], 1).expect("compress");
        assert_eq!(estimate.estimate(&widest, &declared, budget), budget);
        // what fits is left alone
        assert_eq!(
            estimate.estimate(&widest, &declared, 1024 * budget),
            100 * KIB as u64
        );
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

    #[test]
    fn progress_rate_arithmetic() {
        // 10 MiB over 2 s
        assert_eq!(rate_mib_per_s(20 * 1024 * 1024, 2.0), 10.0);
        assert_eq!(rate_mib_per_s(1024 * 1024, 1.0), 1.0);
        assert_eq!(rate_mib_per_s(0, 10.0), 0.0);
        // a window of no time never divides by zero
        assert_eq!(rate_mib_per_s(1024 * 1024, 0.0), 0.0);
        assert_eq!(rate_mib_per_s(1024 * 1024, -1.0), 0.0);
        // sub-second windows scale up
        assert_eq!(rate_mib_per_s(1024 * 1024, 0.5), 2.0);
    }
}
