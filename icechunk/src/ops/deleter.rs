//! Batched, rate-adaptive deletion of listed objects.
//!
//! [`delete_listed`] is the interface: give it a stream of delete candidates,
//! a filter, and a [`DeleteConfig`], and it deletes every accepted candidate
//! under one prefix in batches, returning a [`DeleteReport`] of what happened.
//!
//! The pipeline: the calling task lists and filters candidates and pushes
//! batches into a bounded channel; one deleter task pulls them and keeps a
//! [`JoinSet`] of in-flight delete requests, its size steered by
//! [`Congestion`]: slow start from a single request, halve on a throttle, grow
//! on every clean window. A throttled batch is re-queued rather than counted as
//! a failure; a run of ordinary failures ends the phase with
//! [`DeleteError::DeletesFailing`]. A reporter task logs the live counters.

use std::{
    collections::VecDeque,
    num::{NonZeroU16, NonZeroUsize},
    pin::pin,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering as AtomicOrdering},
    },
    time::{Duration, Instant},
};

use futures::{Stream, StreamExt as _};
use tokio::{sync::mpsc, task::JoinSet};
use tracing::{error, info, warn};

use crate::{
    Storage, StorageError,
    asset_manager::AssetManager,
    ops::{AbortOnDrop, walker::PROGRESS_INTERVAL},
    repository::{RepositoryError, RepositoryResult},
    storage::{self, DeleteObjectsResult, ListInfo},
};
use icechunk_types::ICResultExt as _;

pub(crate) const DELETE_BATCH_SIZE: NonZeroUsize = NonZeroUsize::new(1_000).unwrap();
pub(crate) const MAX_REPORTED_DELETE_ERRORS: usize = 10;

/// Batches queued between the listing collector and the deleter.
const DELETE_QUEUE_BATCHES: usize = 100;

/// The quiet period the deleter observes after a throttle: it starts at `base`
/// and doubles up to `cap` while the store keeps asking for less traffic.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DeleteBackoff {
    pub(crate) base: Duration,
    pub(crate) cap: Duration,
}

impl Default for DeleteBackoff {
    fn default() -> Self {
        Self { base: Duration::from_secs(1), cap: Duration::from_secs(120) }
    }
}

/// How one delete phase runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DeleteConfig {
    /// Ceiling on delete requests in flight; the deleter starts at one and
    /// grows toward it while the store keeps up.
    pub(crate) max_in_flight: NonZeroU16,
    /// Failed batches in a row that end the phase.
    pub(crate) max_consecutive_failures: NonZeroU16,
    pub(crate) backoff: DeleteBackoff,
    /// Count what would be deleted without sending any request.
    pub(crate) dry_run: bool,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum DeleteError {
    #[error("too many consecutive delete failures under {prefix}: {last_error}")]
    DeletesFailing { prefix: String, last_error: String },
    #[error("repository error {0}")]
    Repository(#[from] RepositoryError),
}

pub(crate) type DeleteResult<A> = Result<A, DeleteError>;

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
) -> DeleteResult<(u64, Absorbed)> {
    let BatchOutcome { seq, batch_len, batch, result } =
        joined.capture().map_err(DeleteError::Repository)?;
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
                return Err(DeleteError::DeletesFailing {
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

/// Delete every candidate accepted by `should_delete`, in batches of
/// `batch_size`, with at most `config.max_in_flight` requests in flight.
/// Listing and filtering run on the calling task; deletes run on one deleter
/// task fed through a bounded channel, so listing only waits when
/// [`DELETE_QUEUE_BATCHES`] batches are already queued. Failed batches are
/// recorded and the phase continues, unless `config.max_consecutive_failures`
/// batches fail in a row.
///
/// A listing error does not cut the deletes short: the batches already queued
/// are drained first and only then is the error returned, because everything
/// queued has already passed `should_delete` and the listing failure says
/// nothing about it.
pub(crate) async fn delete_listed<Id, S>(
    asset_manager: &AssetManager,
    config: DeleteConfig,
    prefix: &'static str,
    batch_size: NonZeroUsize,
    candidates: S,
    should_delete: impl Fn(&ListInfo<Id>) -> bool,
) -> DeleteResult<DeleteReport>
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
        config,
        rx,
        Arc::clone(&progress),
    )));
    // held in a guard so every return path below aborts it
    let _reporter =
        AbortOnDrop(tokio::spawn(report_delete_progress(Arc::clone(&progress), prefix)));

    let collected: DeleteResult<()> = async {
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

    let report = (&mut deleter.0).await.capture().map_err(DeleteError::Repository)??;
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
        config: DeleteConfig,
        progress: Arc<DeleteProgress>,
    ) -> Self {
        Deleter {
            prefix,
            threshold: config.max_consecutive_failures.get() as u32,
            progress,
            report: DeleteReport::default(),
            congestion: Congestion::new(
                config.max_in_flight.get() as usize,
                config.backoff,
            ),
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
    ) -> DeleteResult<()> {
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
                            return Err(DeleteError::DeletesFailing {
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
/// The limit is not `config.max_in_flight` but whatever rate the store
/// sustains, discovered by [`Congestion`]: a throttled batch is re-queued rather
/// than counted as a failure, and new requests wait out a quiet period.
async fn run_deletes(
    storage: Arc<dyn Storage + Send + Sync>,
    settings: storage::Settings,
    prefix: &'static str,
    config: DeleteConfig,
    mut rx: mpsc::Receiver<Vec<(String, u64)>>,
    progress: Arc<DeleteProgress>,
) -> DeleteResult<DeleteReport> {
    let mut deleter = Deleter::new(prefix, config, progress);
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

        if config.dry_run {
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

/// A storage wrapper whose deletes misbehave on demand, shared with the GC
/// tests.
#[cfg(test)]
pub(crate) mod testing {
    use std::{
        ops::Range,
        pin::Pin,
        sync::atomic::{AtomicUsize, Ordering},
    };

    use bytes::Bytes;
    use chrono::{DateTime, Utc};
    use futures::stream::BoxStream;
    use icechunk_storage::sealed::Sealed;

    use super::*;
    #[cfg(not(feature = "shuttle"))]
    use crate::format::format_constants::SpecVersionBin;
    use crate::storage::{
        GetModifiedResult, RepositoryCreation, Settings, StorageErrorKind, StorageInfo,
        StorageResult, VersionInfo, VersionedUpdateResult,
    };

    /// How `FlakyDeletes::delete_batch` misbehaves under the failing prefix.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
    pub(crate) enum FailMode {
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
    pub(crate) struct FlakyDeletes {
        backend: Arc<dyn Storage + Send + Sync>,
        #[serde(skip)]
        pub(crate) calls: AtomicUsize,
        /// prefix whose deletes misbehave (`None` = every prefix)
        failing_prefix: Option<String>,
        mode: FailMode,
        /// prefix whose listings yield an `Err` after their first item
        list_error_prefix: Option<String>,
        /// permits `FailMode::Gated` deletes wait on; ignored by every other mode
        #[serde(skip, default = "closed_gate")]
        pub(crate) gate: Arc<tokio::sync::Semaphore>,
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
        futures::stream::once(async {
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
        pub(crate) fn running_deletes(&self) -> usize {
            self.in_flight.load(Ordering::SeqCst)
        }

        #[cfg(not(feature = "shuttle"))]
        pub(crate) fn peak_deletes(&self) -> usize {
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
    /// The wrapper is returned too, so tests can read its call counter.
    pub(crate) fn wrapped_asset_manager(
        repo: &crate::Repository,
        backend: &Arc<dyn Storage + Send + Sync>,
        failing_prefix: Option<&str>,
        mode: FailMode,
    ) -> (Arc<AssetManager>, Arc<FlakyDeletes>) {
        wrapped_asset_manager_with(repo, backend, failing_prefix, mode, None)
    }

    #[cfg(not(feature = "shuttle"))]
    pub(crate) fn wrapped_asset_manager_with(
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
    pub(crate) fn wrapped_asset_manager_listing(
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
    pub(crate) fn gated_asset_manager(
        repo: &crate::Repository,
        backend: &Arc<dyn Storage + Send + Sync>,
    ) -> (Arc<AssetManager>, Arc<tokio::sync::Semaphore>) {
        let (am, flaky) = wrapped_asset_manager(repo, backend, None, FailMode::Gated);
        let gate = Arc::clone(&flaky.gate);
        (am, gate)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // `tokio_test` expands to nothing under shuttle, leaving the async tests
    // and everything only they use unreferenced.
    #[cfg(not(feature = "shuttle"))]
    use super::testing::*;
    #[cfg(not(feature = "shuttle"))]
    use crate::{
        format::{CHUNKS_FILE_PATH, ChunkId},
        storage::new_in_memory_storage,
        test_utils::repo_with_converging_refs,
    };
    #[cfg(not(feature = "shuttle"))]
    use bytes::Bytes;
    #[cfg(not(feature = "shuttle"))]
    use chrono::Utc;
    #[cfg(not(feature = "shuttle"))]
    use futures::{TryStreamExt as _, stream};
    use icechunk_macros::tokio_test;
    #[cfg(not(feature = "shuttle"))]
    use std::{collections::HashSet, sync::atomic::Ordering};

    fn ms(millis: u64) -> Duration {
        Duration::from_millis(millis)
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
        let ramp: Vec<(Duration, Throttle)> = (0..5)
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

    #[cfg(not(feature = "shuttle"))]
    /// Milliseconds instead of seconds of back-off, so a throttled phase runs
    /// in test time.
    fn delete_config(max_consecutive_failures: u16, max_in_flight: u16) -> DeleteConfig {
        DeleteConfig {
            max_in_flight: NonZeroU16::new(max_in_flight).unwrap(),
            max_consecutive_failures: NonZeroU16::new(max_consecutive_failures).unwrap(),
            backoff: DeleteBackoff { base: ms(10), cap: ms(40) },
            dry_run: false,
        }
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
        tokio::time::timeout(Duration::from_secs(5), async {
            while !done() {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .map_err(|_| format!("timed out waiting for {what}"))?;
        Ok(())
    }

    /// The consecutive counter must reset on a successful batch, so a phase
    /// where every other batch fails runs to the end while a threshold of one
    /// stops it at the first failure.
    #[tokio_test]
    async fn alternating_failures_reset_the_consecutive_counter()
    -> Result<(), Box<dyn std::error::Error>> {
        let backend: Arc<dyn Storage + Send + Sync> = new_in_memory_storage().await?;
        let repo = repo_with_loose_chunks(&backend, 4).await?;
        let (am, _flaky) = wrapped_asset_manager(
            &repo,
            &backend,
            Some(CHUNKS_FILE_PATH),
            FailMode::Alternate,
        );
        let candidates: Vec<ListInfo<ChunkId>> =
            am.list_chunks().await?.try_collect().await?;
        assert_eq!(candidates.len(), 4);
        // one object per batch, so the four chunks fail, ok, fail, ok
        let one = NonZeroUsize::MIN;

        // one request in flight, so batches are absorbed in submission order
        let report = delete_listed(
            am.as_ref(),
            delete_config(2, 1),
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
        let repo = repo_with_loose_chunks(&backend, 4).await?;
        let (am, _flaky) = wrapped_asset_manager(
            &repo,
            &backend,
            Some(CHUNKS_FILE_PATH),
            FailMode::Alternate,
        );
        let candidates: Vec<ListInfo<ChunkId>> =
            am.list_chunks().await?.try_collect().await?;
        let result = delete_listed(
            am.as_ref(),
            delete_config(1, 1),
            CHUNKS_FILE_PATH,
            one,
            stream::iter(candidates.into_iter().map(Ok)),
            |_| true,
        )
        .await;
        assert!(matches!(result, Err(DeleteError::DeletesFailing { .. })), "{result:?}");
        Ok(())
    }

    /// Listing must finish while every delete is still blocked: the collector
    /// never waits on the deleter except when the batch queue is full.
    #[tokio_test]
    async fn listing_runs_ahead_of_blocked_deletes()
    -> Result<(), Box<dyn std::error::Error>> {
        let backend: Arc<dyn Storage + Send + Sync> = new_in_memory_storage().await?;
        let repo = repo_with_loose_chunks(&backend, 4).await?;
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
        // max_in_flight = 1, so the deleter can only ever hold one batch in
        // flight and must block on the gate for the rest
        let config = delete_config(50, 1);
        let am2 = Arc::clone(&am);
        let run = tokio::spawn(async move {
            delete_listed(
                am2.as_ref(),
                config,
                CHUNKS_FILE_PATH,
                NonZeroUsize::MIN,
                candidates,
                move |info| garbage_ids.contains(&info.id),
            )
            .await
        });

        // listing completes although no delete has been allowed to finish
        wait_for("listing to finish", || listed_all.load(Ordering::SeqCst)).await?;
        assert_eq!(am.list_chunks().await?.count().await, 4, "nothing deleted yet");

        gate.add_permits(4);
        let report = tokio::time::timeout(Duration::from_secs(5), run).await???;
        assert_eq!(report.deleted_objects, 4);
        assert_eq!(report.failed_objects, 0);
        assert_eq!(am.list_chunks().await?.count().await, 0);
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
        let config = delete_config(50, 8);
        let am2 = Arc::clone(&am);
        let run = tokio::spawn(async move {
            delete_listed(
                am2.as_ref(),
                config,
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

        let report = tokio::time::timeout(Duration::from_secs(5), run).await???;
        assert_eq!(report.deleted_objects, 15);
        assert_eq!(report.failed_objects, 0);
        assert_eq!(report.throttled_batches, 0);
        assert_eq!(report.peak_in_flight, 8);
        assert_eq!(am.list_chunks().await?.count().await, 0);
        Ok(())
    }
}
