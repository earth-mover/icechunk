//! Provider-neutral engine for high-throughput object-size listing.
//!
//! Storage backends that can list a raw key prefix implement
//! [`ListPageFetcher`] — one attempt at one list page, classified into a
//! [`PageAttempt`] — and hand it to [`sum`]. The fetcher owns the wire format
//! (auth, URL building, body scanning); the engine owns everything above it:
//! per-page retries with jittered exponential backoff, the single-page probe,
//! the Crockford fan-out, and the adaptive worker pool.
//!
//! Chunk/manifest/snapshot/transaction ids are Crockford base32, so a shardable
//! prefix that spans more than one page is fanned out across the 1024
//! two-character leading prefixes as concurrent disjoint listings; prefixes that
//! fit in a single page are summed with one request.
//!
//! The fan-out is drained by a worker pool whose admission limit adapts at
//! runtime, BBR-startup style. No fixed limit works everywhere: hosts near the
//! store with wide NICs need hundreds of concurrent listings to approach the
//! store's per-partition request ceiling, while consumer connections lose
//! goodput 3-5x past a few dozen — silently, with no 503s and no timeouts, so
//! error-driven adaptation cannot see it. A `ConcurrencyController` instead
//! watches goodput (pages/sec): it doubles the limit while each step improves
//! goodput by at least 15%, reverts to the best-seen limit when the curve
//! flattens, sheds concurrency toward the floor when a whole window completes
//! no pages at all — never reading that as headroom — yet keeps probing so a
//! merely slow window cannot pin the run, and once holding reacts only to
//! bursts of retryable errors by halving. Individual requests keep their
//! jittered backoff; the controller is a coarser mechanism layered on top, the
//! way BBR coexists with retransmits.
//!
//! Several prefixes summed against one store at once can share a single
//! controller and limit signal (see [`SharedController`]) so their admissions
//! adapt as one group rather than each ramping independently and overshooting
//! the store's aggregate request budget by the number of prefixes.

use std::collections::VecDeque;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::watch;
use tokio::task::JoinSet;
use tokio::time::{Instant, MissedTickBehavior};

use crate::{StorageError, StorageResult, other_error};

/// Crockford base32 alphabet, in lexicographic order. Every object id icechunk
/// writes under `chunks`/`manifests`/`snapshots`/`transactions` starts with one
/// of these characters, so the 32 single-character sub-prefixes are disjoint and
/// cover the whole keyspace.
pub const CROCKFORD: &str = "0123456789ABCDEFGHJKMNPQRSTVWXYZ";

/// Worker-pool bounds. `CONCURRENCY_START` is small enough that the narrowest
/// links survive the first measurement window; [`CONCURRENCY_CAP`] sits past
/// the S3 per-partition request ceiling so wide links are never probe-limited.
/// Fetchers size their HTTP connection pools to the cap.
const CONCURRENCY_FLOOR: usize = 8;
const CONCURRENCY_START: usize = 16;
pub const CONCURRENCY_CAP: usize = 1024;

/// Whole runs last from sub-second to a few minutes, so the controller has to
/// converge within seconds: 500ms sampling windows, judged only once they carry
/// enough pages to mean something, but never deferred past ~2s.
const SAMPLE_INTERVAL: Duration = Duration::from_millis(500);
const MIN_JUDGE_PAGES: u64 = 24;
const MAX_WINDOW_SECS: f64 = 2.0;
const GOODPUT_IMPROVEMENT_FACTOR: f64 = 1.15;
const ERROR_BURST_FRACTION: f64 = 0.02;
const BEST_GOODPUT_DECAY: f64 = 0.5;

/// One queued unit of fan-out work: a shard prefix and the continuation token
/// to resume it from (`None` for its first page). Workers park by re-enqueuing
/// their continuation as one of these rather than holding it suspended — see
/// [`Engine::worker`].
type WorkItem = (String, Option<String>);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    Probing,
    Holding,
}

/// Pure BBR-startup-style governor for the fan-out worker limit.
///
/// Goodput (pages/sec) is the control signal because the failure mode this
/// exists for — congestion collapse on narrow links — emits no error signal at
/// all. The driver feeds sampled counter deltas through [`Self::observe`]; the
/// struct never reads a clock and spawns nothing, so tests drive it with
/// synthetic windows.
///
/// While probing, the limit doubles as long as each step improves goodput by
/// ≥15%; the first flat or regressed window reverts to the best-seen limit and
/// holds. A window that completed *no pages at all* is never an improvement (0
/// goodput would otherwise clear the bar against a 0 baseline); it sheds
/// concurrency toward the floor but keeps probing, re-baselining the goodput
/// target so the ramp resumes the moment pages complete again. Shedding answers
/// the real stall — doubling into a link that is moving nothing is exactly the
/// collapse this governor exists to prevent — while staying recoverable answers
/// its benign twin: a healthy high-latency store whose pages each outlast a
/// sampling window leaves early windows empty with every request still in
/// flight, and one of those must not pin a multi-hour run at the floor. Holding
/// ignores goodput on purpose — the end-of-run tail
/// drain sinks goodput, and shrinking then would be pointless — reacting only
/// to bursts of retryable errors (>2% of a window's pages) by halving the
/// limit. An error-halving also ends probing rather than restarting it:
/// regrowing toward a throttle ceiling the run just hit would oscillate for the
/// rest of a short run, and the goodput-vs-concurrency plateau is broad enough
/// that the halved limit stays within it.
#[derive(Debug)]
struct ConcurrencyController {
    limit: usize,
    phase: Phase,
    best_goodput: f64,
    best_limit: usize,
    pending_pages: u64,
    pending_errors: u64,
    pending_secs: f64,
    discard_next: bool,
}

impl ConcurrencyController {
    fn new() -> Self {
        Self {
            limit: CONCURRENCY_START,
            phase: Phase::Probing,
            best_goodput: 0.0,
            best_limit: CONCURRENCY_START,
            pending_pages: 0,
            pending_errors: 0,
            pending_secs: 0.0,
            discard_next: false,
        }
    }

    /// Feed one sampling window of counter deltas; returns the limit to apply.
    ///
    /// Windows under [`MIN_JUDGE_PAGES`] pages carry too little signal to judge
    /// and accumulate into the next call, but never past [`MAX_WINDOW_SECS`].
    /// The first judged window after any limit change is discarded outright:
    /// connection warm-up (or drain, after a shrink) makes it unrepresentative
    /// of the new limit, for errors as much as for goodput.
    fn observe(&mut self, pages: u64, errors: u64, secs: f64) -> usize {
        self.pending_pages += pages;
        self.pending_errors += errors;
        self.pending_secs += secs;
        if self.pending_pages < MIN_JUDGE_PAGES && self.pending_secs < MAX_WINDOW_SECS {
            return self.limit;
        }
        let (pages, errors, secs) =
            (self.pending_pages, self.pending_errors, self.pending_secs);
        self.pending_pages = 0;
        self.pending_errors = 0;
        self.pending_secs = 0.0;

        if self.discard_next {
            self.discard_next = false;
            return self.limit;
        }

        if errors as f64 > ERROR_BURST_FRACTION * pages as f64 {
            let halved = (self.limit / 2).max(CONCURRENCY_FLOOR);
            if halved != self.limit {
                self.limit = halved;
                self.discard_next = true;
            }
            self.best_goodput *= BEST_GOODPUT_DECAY;
            self.best_limit = self.limit;
            self.phase = Phase::Holding;
            return self.limit;
        }

        if self.phase == Phase::Probing {
            // A judged window that completed no pages carries no goodput
            // signal: its rate is 0, and 0 clears the ≥15% improvement bar
            // against a 0 baseline (0 ≥ 1.15·0), so without this guard a
            // stalled link would double the limit every window straight to the
            // cap — the congestion collapse the governor exists to prevent.
            //
            // But a zero-page window is not proof of a stall: a healthy
            // high-latency store whose pages each take just over
            // MAX_WINDOW_SECS leaves an early window with every request still
            // in flight and nothing yet completed. Shedding is still right (a
            // real stall keeps producing zero-page windows and keeps shedding
            // toward the floor), but pinning a multi-hour run at the floor off
            // one slow window is the worse failure, so it must recover. Shed
            // toward the floor and re-baseline the goodput target to 0 while
            // staying in Probing: the next window that actually completes pages
            // clears the reset bar and the ramp resumes from the shed limit.
            if pages == 0 {
                let backed_off = (self.limit / 2).max(CONCURRENCY_FLOOR);
                if backed_off != self.limit {
                    self.limit = backed_off;
                    self.discard_next = true;
                }
                self.best_goodput = 0.0;
                self.best_limit = self.limit;
                return self.limit;
            }
            let goodput = if secs > 0.0 { pages as f64 / secs } else { 0.0 };
            if goodput >= GOODPUT_IMPROVEMENT_FACTOR * self.best_goodput {
                self.best_goodput = goodput;
                self.best_limit = self.limit;
                if self.limit >= CONCURRENCY_CAP {
                    self.phase = Phase::Holding;
                } else {
                    self.limit = (self.limit * 2).min(CONCURRENCY_CAP);
                    self.discard_next = true;
                }
            } else {
                if self.limit != self.best_limit {
                    self.limit = self.best_limit;
                    self.discard_next = true;
                }
                self.phase = Phase::Holding;
            }
        }
        self.limit
    }
}

/// Outcome of one attempt at one list page, classified by the fetcher.
#[derive(Debug)]
pub enum PageAttempt {
    /// The page was fetched and its body scanned to the end.
    Page { bytes: u64, next_token: Option<String> },
    /// A transient failure worth retrying: throttling or server errors
    /// (503/429/5xx), timeouts, connect failures, mid-body read errors. The
    /// payload carries the underlying cause so the engine can log each retry at
    /// debug level and surface the last cause if the attempts are exhausted;
    /// fetchers populate it with whatever status/error they have in hand.
    Retryable(StorageError),
    /// A failure retrying cannot fix; aborts the whole sum.
    Fatal(StorageError),
}

/// Whether an HTTP status warrants a retry rather than aborting the sum: `429`
/// (Too Many Requests) plus the whole `5xx` server-error range. The three
/// backend fetchers share this so their retryable-vs-fatal split stays
/// consistent instead of triplicating divergent status lists; finer,
/// provider-specific signals (S3 `SlowDown`, GCS `userRateLimitExceeded`, Azure
/// `ServerBusy`) can still classify a [`PageAttempt::Retryable`] on top of it.
pub fn is_transient_status(status: u16) -> bool {
    status == 429 || (500..=599).contains(&status)
}

/// One attempt at one list page of a provider's raw-prefix listing API.
///
/// Implementations stay pure transport: no retries, no counters, no backoff —
/// the engine owns all of those, driven by the returned [`PageAttempt`].
#[async_trait]
pub trait ListPageFetcher: Send + Sync + 'static {
    async fn attempt_page(&self, list_prefix: &str, token: Option<&str>) -> PageAttempt;
}

/// Sum object sizes under `base_prefix` through `fetcher`.
///
/// Non-shardable prefixes are drained as a single sequential listing of
/// `base_prefix` as-is. Shardable prefixes are probed with one page of
/// `{base_prefix}/`; if it is truncated, the keyspace is fanned out over the
/// 1024 two-character Crockford shards, drained by up to [`CONCURRENCY_CAP`]
/// workers gated on a watch channel, with the sampling loop feeding the
/// concurrency controller and publishing its limit to the workers.
///
/// `max_attempts` is the *total* number of tries the engine makes per page
/// (not the number of retries): callers pass their `max_tries` directly. To
/// co-schedule several sums against one store under a shared limit, see
/// [`sum_with_controller`].
pub async fn sum(
    fetcher: Arc<dyn ListPageFetcher>,
    base_prefix: &str,
    shardable: bool,
    max_attempts: u32,
) -> StorageResult<u64> {
    let engine = Engine::new(fetcher, max_attempts, Governor::new());
    engine.sum_owned(base_prefix, shardable).await
}

/// Like [`sum`], but drives the fan-out off a caller-owned [`SharedController`]
/// so several concurrent runs adapt as one group. The caller must drive the
/// controller's sampler once for the batch — see [`SharedController`].
pub async fn sum_with_controller(
    fetcher: Arc<dyn ListPageFetcher>,
    base_prefix: &str,
    shardable: bool,
    max_attempts: u32,
    controller: &SharedController,
) -> StorageResult<u64> {
    let engine = Engine::new(fetcher, max_attempts, Arc::clone(&controller.governor));
    engine.sum_shared_run(base_prefix, shardable).await
}

/// A [`ConcurrencyController`] and its published limit signal, shared across
/// several concurrent [`sum_with_controller`] runs against one store.
///
/// The size-listing entry point sums several top-level prefixes at once (the
/// six repo roots). Given one controller each, the runs ramp independently and
/// their admissions add up, overshooting the store's aggregate request budget
/// by roughly the number of prefixes. Sharing one controller fixes that: every
/// run's page completions and retryable errors feed one controller measuring
/// *aggregate* goodput, and it publishes one limit that every run's workers
/// obey. Because aggregate goodput reaches the store's knee at a lower per-run
/// limit than a single run would, the group converges so total in-flight
/// concurrency tracks a single run's optimum instead of multiplying by the
/// prefix count.
///
/// The limit is a per-run worker budget — each run admits up to `limit`
/// workers of its own; the shared *signal*, not a global slot count, is what
/// couples the runs. Drive the sampler exactly once for the batch with
/// [`Self::run`], concurrently with the runs, and resolve its `shutdown` future
/// once they finish:
///
/// ```ignore
/// let controller = SharedController::new();
/// let done = tokio::sync::Notify::new();
/// let sums = async {
///     let totals = futures::future::try_join_all(prefixes.iter().map(|&(p, shardable)| {
///         sum_with_controller(fetcher.clone(), p, shardable, max_attempts, &controller)
///     }))
///     .await;
///     done.notify_one();
///     totals
/// };
/// let (totals, ()) = tokio::join!(sums, controller.run(done.notified()));
/// ```
#[derive(Debug)]
pub struct SharedController {
    governor: Arc<Governor>,
}

impl SharedController {
    pub fn new() -> Self {
        Self { governor: Governor::new() }
    }

    /// Drive the shared sampling loop until `shutdown` resolves. Run this once
    /// per batch, concurrently with the [`sum_with_controller`] runs that share
    /// this controller, and resolve `shutdown` after they have all completed.
    pub async fn run(&self, shutdown: impl Future<Output = ()>) {
        drive_sampler(Arc::clone(&self.governor), shutdown).await;
    }
}

impl Default for SharedController {
    fn default() -> Self {
        Self::new()
    }
}

/// Shared adaptive-concurrency state: the [`ConcurrencyController`], the single
/// published worker limit, and the counter deltas the sampler folds into it.
/// One `Governor` backs one *or more* engine runs; when several share it (via
/// [`SharedController`]) they adapt as a group off aggregate goodput.
#[derive(Debug)]
struct Governor {
    controller: Mutex<ConcurrencyController>,
    limit_tx: watch::Sender<usize>,
    pages_completed: AtomicU64,
    retryable_errors: AtomicU64,
}

impl Governor {
    fn new() -> Arc<Self> {
        let (limit_tx, _rx) = watch::channel(CONCURRENCY_START);
        Arc::new(Self {
            controller: Mutex::new(ConcurrencyController::new()),
            limit_tx,
            pages_completed: AtomicU64::new(0),
            retryable_errors: AtomicU64::new(0),
        })
    }

    fn limit_rx(&self) -> watch::Receiver<usize> {
        self.limit_tx.subscribe()
    }

    /// One sampling step: fold the counters accumulated since the last call
    /// into the controller and publish the limit it returns. Called on a fixed
    /// cadence by [`drive_sampler`]; `secs` is the wall time since the previous
    /// step. A poisoned controller lock leaves the limit untouched.
    fn sample(&self, secs: f64) {
        let pages = self.pages_completed.swap(0, Ordering::Relaxed);
        let errors = self.retryable_errors.swap(0, Ordering::Relaxed);
        let Ok(mut controller) = self.controller.lock() else {
            return;
        };
        let limit = controller.observe(pages, errors, secs);
        drop(controller);
        if *self.limit_tx.borrow() != limit {
            let _ = self.limit_tx.send(limit);
        }
    }
}

/// Tick every [`SAMPLE_INTERVAL`] and fold each window into `governor` until
/// `shutdown` resolves. The single-run entry points spawn this alongside their
/// fan-out; a [`SharedController`] caller drives it once for a whole batch.
async fn drive_sampler(governor: Arc<Governor>, shutdown: impl Future<Output = ()>) {
    let mut ticker =
        tokio::time::interval_at(Instant::now() + SAMPLE_INTERVAL, SAMPLE_INTERVAL);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut last_sample = Instant::now();
    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            _ = ticker.tick() => {
                let now = Instant::now();
                let secs = now.duration_since(last_sample).as_secs_f64();
                last_sample = now;
                governor.sample(secs);
            }
            _ = &mut shutdown => return,
        }
    }
}

struct Engine {
    fetcher: Arc<dyn ListPageFetcher>,
    /// Total attempts per page (not retries): the loop in [`Self::fetch_page`]
    /// makes exactly this many tries before giving up. Callers pass their
    /// `max_tries` unchanged.
    max_attempts: u32,
    governor: Arc<Governor>,
}

impl Engine {
    fn new(
        fetcher: Arc<dyn ListPageFetcher>,
        max_attempts: u32,
        governor: Arc<Governor>,
    ) -> Arc<Self> {
        Arc::new(Self { fetcher, max_attempts, governor })
    }

    /// Single-run entry: owns the governor, so it spawns and stops the sampler
    /// itself around the fan-out.
    async fn sum_owned(
        self: Arc<Self>,
        base_prefix: &str,
        shardable: bool,
    ) -> StorageResult<u64> {
        if !shardable {
            return self.sum_prefix(base_prefix.to_string()).await;
        }
        if let Some(total) = self.probe(base_prefix).await? {
            return Ok(total);
        }
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let sampler =
            tokio::spawn(drive_sampler(Arc::clone(&self.governor), async move {
                let _ = shutdown_rx.await;
            }));
        let result = Arc::clone(&self).fanout(base_prefix).await;
        let _ = shutdown_tx.send(());
        let _ = sampler.await;
        result
    }

    /// Shared-controller entry: the governor's sampler is driven externally by
    /// the [`SharedController`] caller, so this only drives its own workers.
    async fn sum_shared_run(
        self: Arc<Self>,
        base_prefix: &str,
        shardable: bool,
    ) -> StorageResult<u64> {
        if !shardable {
            return self.sum_prefix(base_prefix.to_string()).await;
        }
        if let Some(total) = self.probe(base_prefix).await? {
            return Ok(total);
        }
        self.fanout(base_prefix).await
    }

    /// Probe one page: `Some(bytes)` when the prefix fits in a single response
    /// (summed with exactly one request), `None` when it is truncated and the
    /// caller must fan out. The truncated probe's bytes are deliberately
    /// dropped — the fan-out re-lists the whole keyspace, so counting them
    /// would double the first page.
    async fn probe(&self, base_prefix: &str) -> StorageResult<Option<u64>> {
        let (probe_bytes, next_token) =
            self.fetch_page(&format!("{base_prefix}/"), None).await?;
        Ok(next_token.is_none().then_some(probe_bytes))
    }

    /// Fan the truncated keyspace out over the 1024 two-character Crockford
    /// shards and drain them with the adaptive worker pool.
    async fn fanout(self: Arc<Self>, base_prefix: &str) -> StorageResult<u64> {
        // Ids are >= 2 Crockford characters, so the 1024 two-character
        // sub-prefixes are disjoint and cover the whole keyspace.
        let queue: Arc<Mutex<VecDeque<WorkItem>>> = Arc::new(Mutex::new(
            CROCKFORD
                .chars()
                .flat_map(|a| {
                    CROCKFORD
                        .chars()
                        .map(move |b| (format!("{base_prefix}/{a}{b}"), None))
                })
                .collect(),
        ));

        let limit_rx = self.governor.limit_rx();
        // Per-run drain signal: when this run's queue empties, it releases its
        // own parked workers without touching the shared limit, so runs that
        // share the limit are unaffected.
        let (drain_tx, drain_rx) = watch::channel(false);
        let mut workers: JoinSet<StorageResult<u64>> = JoinSet::new();
        for index in 0..CONCURRENCY_CAP {
            workers.spawn(Arc::clone(&self).worker(
                Arc::clone(&queue),
                limit_rx.clone(),
                drain_rx.clone(),
                index,
            ));
        }
        drop(limit_rx);
        drop(drain_rx);

        let mut total = 0u64;
        let mut draining = false;
        let result = loop {
            match workers.join_next().await {
                None => break Ok(total),
                Some(Ok(Ok(subtotal))) => {
                    total = total.saturating_add(subtotal);
                    // A worker returns cleanly only after popping an empty
                    // queue. Continuations are re-enqueued rather than held in
                    // parked workers (see `worker`), so an empty queue means
                    // the run is genuinely done — no worker is sitting on an
                    // unfinished listing. Release this run's parked workers so
                    // they observe the empty queue and exit; because the
                    // released workers hold no work to resume, this cannot
                    // burst, and because the signal is per-run it leaves any
                    // co-scheduled runs sharing the limit untouched.
                    if !draining {
                        draining = true;
                        let _ = drain_tx.send(true);
                    }
                }
                Some(Ok(Err(e))) => break Err(e),
                Some(Err(join_error)) => {
                    if join_error.is_panic() {
                        std::panic::resume_unwind(join_error.into_panic());
                    }
                }
            }
        };
        if result.is_err() {
            workers.abort_all();
        }
        while workers.join_next().await.is_some() {}
        result
    }

    /// One fan-out worker: pops shard work items and pages through them until
    /// the queue is empty. Admission is re-checked between pages, not prefixes —
    /// a busy shard of a large repo holds hundreds of pages, and a shrink must
    /// take effect after the in-flight request, not minutes later.
    ///
    /// When a worker is no longer admitted mid-prefix it does **not** park with
    /// the continuation token held in its suspended future: doing so lets the
    /// end-of-run drain (which admits everyone) resume every parked worker at
    /// once, bursting hundreds of listings past the limit the controller just
    /// adapted down to. Instead it re-enqueues its `(prefix, continuation)` as
    /// a fresh work item and returns to the admission gate, so the remaining
    /// pages are drained by whatever workers the *current* limit admits and the
    /// drain completes at the adapted concurrency.
    async fn worker(
        self: Arc<Self>,
        queue: Arc<Mutex<VecDeque<WorkItem>>>,
        mut limit_rx: watch::Receiver<usize>,
        mut drain_rx: watch::Receiver<bool>,
        index: usize,
    ) -> StorageResult<u64> {
        let mut subtotal = 0u64;
        loop {
            wait_admitted(index, &mut limit_rx, &mut drain_rx).await;
            let popped = queue
                .lock()
                .map_err(|_| other_error("shard queue mutex poisoned"))?
                .pop_front();
            let Some((prefix, mut token)) = popped else {
                return Ok(subtotal);
            };
            loop {
                let (bytes, next) = self.fetch_page(&prefix, token.as_deref()).await?;
                subtotal = subtotal.saturating_add(bytes);
                let Some(next_token) = next else { break };
                // Draining admits everyone, so keep paging to completion; only
                // a genuine shrink (limit dropped below this index while not
                // draining) re-enqueues the continuation and re-parks.
                if !*drain_rx.borrow() && index >= *limit_rx.borrow() {
                    queue
                        .lock()
                        .map_err(|_| other_error("shard queue mutex poisoned"))?
                        .push_back((prefix, Some(next_token)));
                    break;
                }
                token = Some(next_token);
            }
        }
    }

    async fn sum_prefix(&self, list_prefix: String) -> StorageResult<u64> {
        let mut total = 0u64;
        let mut token: Option<String> = None;
        loop {
            let (bytes, next) = self.fetch_page(&list_prefix, token.as_deref()).await?;
            total = total.saturating_add(bytes);
            match next {
                Some(t) => token = Some(t),
                None => return Ok(total),
            }
        }
    }

    /// Fetch one list page through the fetcher, retrying retryable attempts
    /// with jittered exponential backoff. The engine makes exactly
    /// `max_attempts` tries total; each retry is logged at debug with its
    /// cause, and the final giving-up error carries the last underlying cause.
    /// A page counts as completed only once the fetcher has scanned its body to
    /// the end, so a mid-body read failure re-fetches the whole page.
    async fn fetch_page(
        &self,
        list_prefix: &str,
        token: Option<&str>,
    ) -> StorageResult<(u64, Option<String>)> {
        let mut attempt: u32 = 0;
        loop {
            attempt += 1;
            match self.fetcher.attempt_page(list_prefix, token).await {
                PageAttempt::Page { bytes, next_token } => {
                    self.governor.pages_completed.fetch_add(1, Ordering::Relaxed);
                    return Ok((bytes, next_token));
                }
                PageAttempt::Retryable(cause) => {
                    self.governor.retryable_errors.fetch_add(1, Ordering::Relaxed);
                    if attempt >= self.max_attempts {
                        return Err(other_error(format!(
                            "list giving up after {attempt} attempts for prefix \
                             {list_prefix:?}: {cause}"
                        )));
                    }
                    tracing::debug!(
                        attempt,
                        prefix = list_prefix,
                        cause = %cause,
                        "retrying transient list-page failure"
                    );
                    tokio::time::sleep(backoff(attempt)).await;
                }
                PageAttempt::Fatal(e) => return Err(e),
            }
        }
    }
}

/// Park until `index` is below the published worker limit, this run begins
/// draining, or the signals close. A closed channel means the driving fan-out
/// is gone (workers are being torn down); treating it as admission lets the
/// worker run to its natural exit instead of hanging. The `drain` signal is
/// per-run: a finished fan-out releases *its own* parked workers without
/// touching the shared limit, so co-scheduled runs are unaffected.
async fn wait_admitted(
    index: usize,
    limit_rx: &mut watch::Receiver<usize>,
    drain_rx: &mut watch::Receiver<bool>,
) {
    loop {
        if *drain_rx.borrow_and_update() || index < *limit_rx.borrow_and_update() {
            return;
        }
        tokio::select! {
            changed = limit_rx.changed() => if changed.is_err() { return; },
            changed = drain_rx.changed() => if changed.is_err() { return; },
        }
    }
}

fn backoff(attempt: u32) -> Duration {
    let base_ms: u64 = 100;
    let cap_ms: u64 = 30_000;
    let exp = base_ms.saturating_mul(1u64 << attempt.min(20)).min(cap_ms);
    let low = exp / 2;
    let high = exp.max(low + 1);
    Duration::from_millis(rand::random_range(low..=high))
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::sync::MutexGuard;

    use super::*;

    type PageKey = (String, Option<String>);
    type ScriptEntry = (PageKey, Vec<Script>);
    type ScriptEntryRef<'a> = ((&'a str, Option<&'a str>), Vec<Script>);

    #[test]
    fn crockford_covers_32_sorted_chars() {
        assert_eq!(CROCKFORD.len(), 32);
        let mut sorted: Vec<char> = CROCKFORD.chars().collect();
        let orig = sorted.clone();
        sorted.sort_unstable();
        assert_eq!(sorted, orig);
        for banned in ['I', 'L', 'O', 'U'] {
            assert!(!CROCKFORD.contains(banned));
        }
    }

    #[test]
    fn is_transient_status_covers_429_and_5xx_only() {
        for ok in [200, 301, 400, 403, 404] {
            assert!(!is_transient_status(ok), "{ok} must not be transient");
        }
        for transient in [429, 500, 501, 502, 503, 504, 599] {
            assert!(is_transient_status(transient), "{transient} must be transient");
        }
    }

    #[test]
    fn controller_doubles_while_goodput_improves_then_holds_at_cap() {
        let mut c = ConcurrencyController::new();
        let mut limit = CONCURRENCY_START;
        let mut pages = 100u64;
        for _ in 0..6 {
            assert_eq!(c.observe(pages, 0, 0.5), limit * 2);
            limit *= 2;
            assert_eq!(c.observe(pages, 0, 0.5), limit);
            pages *= 2;
        }
        assert_eq!(limit, CONCURRENCY_CAP);
        assert_eq!(c.observe(pages, 0, 0.5), CONCURRENCY_CAP);
        assert_eq!(c.observe(24, 0, 0.5), CONCURRENCY_CAP);
        assert_eq!(c.observe(100_000, 0, 0.5), CONCURRENCY_CAP);
    }

    #[test]
    fn controller_reverts_to_best_when_goodput_flattens() {
        let mut c = ConcurrencyController::new();
        assert_eq!(c.observe(100, 0, 0.5), 32);
        assert_eq!(c.observe(100, 0, 0.5), 32);
        assert_eq!(c.observe(200, 0, 0.5), 64);
        assert_eq!(c.observe(200, 0, 0.5), 64);
        // 420 pages/s is < 15% over the best 400 pages/s: park at 32.
        assert_eq!(c.observe(210, 0, 0.5), 32);
        assert_eq!(c.observe(210, 0, 0.5), 32);
        assert_eq!(c.observe(5_000, 0, 0.5), 32);
        assert_eq!(c.observe(24, 0, 0.5), 32);
    }

    #[test]
    fn controller_reverts_to_best_when_goodput_regresses() {
        let mut c = ConcurrencyController::new();
        assert_eq!(c.observe(100, 0, 0.5), 32);
        assert_eq!(c.observe(100, 0, 0.5), 32);
        assert_eq!(c.observe(200, 0, 0.5), 64);
        assert_eq!(c.observe(200, 0, 0.5), 64);
        assert_eq!(c.observe(50, 0, 0.5), 32);
        assert_eq!(c.observe(400, 0, 0.5), 32);
        assert_eq!(c.observe(400, 0, 0.5), 32);
    }

    #[test]
    fn controller_halves_on_error_burst_down_to_floor() {
        let mut c = ConcurrencyController::new();
        assert_eq!(c.observe(100, 0, 0.5), 32);
        assert_eq!(c.observe(100, 0, 0.5), 32);
        assert_eq!(c.observe(100, 3, 0.5), 16);
        assert_eq!(c.observe(100, 50, 0.5), 16);
        assert_eq!(c.observe(100, 3, 0.5), CONCURRENCY_FLOOR);
        assert_eq!(c.observe(100, 3, 0.5), CONCURRENCY_FLOOR);
        assert_eq!(c.observe(100, 50, 0.5), CONCURRENCY_FLOOR);
        assert_eq!(c.observe(100, 50, 0.5), CONCURRENCY_FLOOR);
    }

    #[test]
    fn controller_ignores_two_percent_error_noise() {
        let mut c = ConcurrencyController::new();
        assert_eq!(c.observe(100, 0, 0.5), 32);
        assert_eq!(c.observe(100, 0, 0.5), 32);
        // Exactly 2% errors is noise, not a burst: probing continues.
        assert_eq!(c.observe(400, 8, 0.5), 64);
    }

    #[test]
    fn controller_accumulates_low_signal_windows() {
        let mut c = ConcurrencyController::new();
        assert_eq!(c.observe(10, 0, 0.5), CONCURRENCY_START);
        assert_eq!(c.observe(10, 0, 0.5), CONCURRENCY_START);
        assert_eq!(c.observe(10, 0, 0.5), 32);
    }

    #[test]
    fn controller_judges_a_starved_window_after_two_seconds() {
        let mut c = ConcurrencyController::new();
        assert_eq!(c.observe(2, 0, 0.6), CONCURRENCY_START);
        assert_eq!(c.observe(2, 0, 0.6), CONCURRENCY_START);
        assert_eq!(c.observe(2, 0, 0.6), CONCURRENCY_START);
        assert_eq!(c.observe(2, 0, 0.6), 32);
    }

    #[test]
    fn controller_discards_first_window_after_a_limit_change() {
        let mut c = ConcurrencyController::new();
        assert_eq!(c.observe(100, 0, 0.5), 32);
        // Collapsed goodput right after the change is warm-up noise: ignored.
        assert_eq!(c.observe(24, 0, 0.5), 32);
        // The same collapse in a judged window reverts to the best limit.
        assert_eq!(c.observe(24, 0, 0.5), 16);
    }

    #[test]
    fn controller_zero_page_window_sheds_toward_floor() {
        let mut c = ConcurrencyController::new();
        // A full 2s window that completed no pages must never read as an
        // improvement (0 >= 1.15*0). Repeated stalls shed toward the floor.
        assert_eq!(c.observe(0, 0, 2.0), CONCURRENCY_FLOOR);
        assert_eq!(c.observe(0, 0, 2.0), CONCURRENCY_FLOOR);
        assert_eq!(c.observe(0, 0, 2.0), CONCURRENCY_FLOOR);
    }

    #[test]
    fn controller_recovers_after_zero_page_window() {
        let mut c = ConcurrencyController::new();
        // One early zero-page window (a slow high-latency store, not a stall)
        // sheds to the floor but must not pin the run there.
        assert_eq!(c.observe(0, 0, 2.0), CONCURRENCY_FLOOR);
        // The window right after the shed is warm-up noise, discarded.
        assert_eq!(c.observe(400, 0, 0.5), CONCURRENCY_FLOOR);
        // Once pages complete again the ramp resumes from the shed limit.
        assert_eq!(c.observe(400, 0, 0.5), 16);
        assert_eq!(c.observe(400, 0, 0.5), 16);
        assert_eq!(c.observe(800, 0, 0.5), 32);
    }

    #[test]
    fn controller_zero_page_window_after_ramp_sheds_not_doubles() {
        let mut c = ConcurrencyController::new();
        assert_eq!(c.observe(100, 0, 0.5), 32);
        assert_eq!(c.observe(100, 0, 0.5), 32);
        // Stalled window at limit 32 must shed to 16, never climb to 64.
        assert_eq!(c.observe(0, 0, 2.0), 16);
    }

    fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
        m.lock().expect("poisoned")
    }

    /// Scripted fetcher: `script` maps `(list_prefix, token)` to a queue of
    /// attempt outcomes consumed left to right; the final outcome repeats.
    /// An unscripted page surfaces as a `Fatal` attempt so the test fails with
    /// a readable error. Every call is recorded in `calls`.
    struct ScriptedFetcher {
        script: Mutex<HashMap<PageKey, VecDeque<Script>>>,
        calls: Mutex<Vec<PageKey>>,
    }

    #[derive(Debug, Clone)]
    enum Script {
        Page { bytes: u64, next_token: Option<String> },
        Retryable,
        Fatal(String),
    }

    /// Distinctive cause string a scripted [`Script::Retryable`] carries, so
    /// tests can assert the engine threads it into the giving-up error.
    const SCRIPTED_RETRYABLE_CAUSE: &str = "scripted transient failure";

    impl ScriptedFetcher {
        fn new<'a>(
            entries: impl IntoIterator<Item = ((&'a str, Option<&'a str>), Vec<Script>)>,
        ) -> Arc<Self> {
            let script = entries
                .into_iter()
                .map(|((p, t), outcomes)| {
                    ((p.to_string(), t.map(str::to_string)), outcomes.into())
                })
                .collect();
            Arc::new(Self { script: Mutex::new(script), calls: Mutex::new(Vec::new()) })
        }

        fn calls(&self) -> Vec<(String, Option<String>)> {
            lock(&self.calls).clone()
        }
    }

    #[async_trait]
    impl ListPageFetcher for ScriptedFetcher {
        async fn attempt_page(
            &self,
            list_prefix: &str,
            token: Option<&str>,
        ) -> PageAttempt {
            let key = (list_prefix.to_string(), token.map(str::to_string));
            lock(&self.calls).push(key.clone());
            let mut script = lock(&self.script);
            let outcome = match script.get_mut(&key) {
                None => Script::Fatal(format!("unscripted page {key:?}")),
                Some(outcomes) if outcomes.len() > 1 => outcomes
                    .pop_front()
                    .unwrap_or_else(|| Script::Fatal("empty script".to_string())),
                Some(outcomes) => outcomes
                    .front()
                    .cloned()
                    .unwrap_or_else(|| Script::Fatal(format!("empty script {key:?}"))),
            };
            match outcome {
                Script::Page { bytes, next_token } => {
                    PageAttempt::Page { bytes, next_token }
                }
                Script::Retryable => {
                    PageAttempt::Retryable(other_error(SCRIPTED_RETRYABLE_CAUSE))
                }
                Script::Fatal(msg) => PageAttempt::Fatal(other_error(msg)),
            }
        }
    }

    fn page(bytes: u64) -> Vec<Script> {
        vec![Script::Page { bytes, next_token: None }]
    }

    /// All 1024 shard prefixes under `base`, each scripted as a single page of
    /// `bytes`.
    fn all_shards(base: &str, bytes: u64) -> Vec<ScriptEntry> {
        CROCKFORD
            .chars()
            .flat_map(move |a| {
                CROCKFORD
                    .chars()
                    .map(move |b| ((format!("{base}/{a}{b}"), None), page(bytes)))
            })
            .collect()
    }

    fn scripted_shards(
        base: &str,
        bytes: u64,
        extra: Vec<ScriptEntryRef<'_>>,
    ) -> Arc<ScriptedFetcher> {
        let mut entries: Vec<ScriptEntry> = all_shards(base, bytes);
        entries.extend(extra.into_iter().map(|((p, t), outcomes)| {
            ((p.to_string(), t.map(str::to_string)), outcomes)
        }));
        ScriptedFetcher::new(
            entries
                .iter()
                .map(|((p, t), outcomes)| ((p.as_str(), t.as_deref()), outcomes.clone())),
        )
    }

    #[tokio::test(start_paused = true)]
    async fn engine_single_page_probe_short_circuits() {
        let fetcher = ScriptedFetcher::new([(("chunks/", None), page(7))]);
        let total = sum(Arc::clone(&fetcher) as _, "chunks", true, 3).await;
        assert_eq!(total.ok(), Some(7));
        assert_eq!(fetcher.calls(), vec![("chunks/".to_string(), None)]);
    }

    #[tokio::test(start_paused = true)]
    async fn engine_truncated_probe_fans_out_over_all_shards() {
        // Probe bytes must not be double counted; one shard pages twice.
        let fetcher = scripted_shards(
            "chunks",
            3,
            vec![
                (
                    ("chunks/", None),
                    vec![Script::Page {
                        bytes: 1_000_000,
                        next_token: Some("t0".to_string()),
                    }],
                ),
                (
                    ("chunks/00", None),
                    vec![Script::Page { bytes: 5, next_token: Some("t1".to_string()) }],
                ),
                (("chunks/00", Some("t1")), page(6)),
            ],
        );
        let total = sum(Arc::clone(&fetcher) as _, "chunks", true, 3).await;
        assert_eq!(total.ok(), Some(1023 * 3 + 5 + 6));

        let calls = fetcher.calls();
        let shard_calls: HashSet<&str> = calls
            .iter()
            .filter(|(p, _)| p != "chunks/")
            .map(|(p, _)| p.as_str())
            .collect();
        assert_eq!(shard_calls.len(), 1024);
        // Each (prefix, token) page is listed exactly once.
        let unique: HashSet<_> = calls.iter().collect();
        assert_eq!(unique.len(), calls.len());
        assert_eq!(calls.len(), 1 + 1024 + 1);
    }

    #[tokio::test(start_paused = true)]
    async fn engine_retries_retryable_attempts_then_succeeds() {
        let fetcher = ScriptedFetcher::new([(
            ("refs", None),
            vec![
                Script::Retryable,
                Script::Retryable,
                Script::Retryable,
                Script::Page { bytes: 42, next_token: None },
            ],
        )]);
        let engine = Engine::new(Arc::clone(&fetcher) as _, 10, Governor::new());
        let total = Arc::clone(&engine).sum_owned("refs", false).await;
        assert_eq!(total.ok(), Some(42));
        assert_eq!(fetcher.calls().len(), 4);
        assert_eq!(engine.governor.pages_completed.load(Ordering::Relaxed), 1);
        assert_eq!(engine.governor.retryable_errors.load(Ordering::Relaxed), 3);
    }

    #[tokio::test(start_paused = true)]
    async fn engine_gives_up_after_max_attempts_with_cause() {
        // `max_attempts` is the total number of tries: 2 means two attempts,
        // then give up — no off-by-one extra try.
        let fetcher = ScriptedFetcher::new([(("refs", None), vec![Script::Retryable])]);
        let err = sum(Arc::clone(&fetcher) as _, "refs", false, 2)
            .await
            .expect_err("must give up");
        let msg = err.to_string();
        assert!(msg.contains("giving up after 2 attempts"), "got: {msg}");
        // The last underlying cause is threaded into the giving-up error.
        assert!(msg.contains(SCRIPTED_RETRYABLE_CAUSE), "got: {msg}");
        assert_eq!(fetcher.calls().len(), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn engine_exhausted_retries_on_one_shard_abort_the_fan_out() {
        let fetcher = scripted_shards(
            "chunks",
            3,
            vec![
                (
                    ("chunks/", None),
                    vec![Script::Page { bytes: 0, next_token: Some("t0".to_string()) }],
                ),
                (("chunks/7Z", None), vec![Script::Retryable]),
            ],
        );
        let err = sum(Arc::clone(&fetcher) as _, "chunks", true, 2)
            .await
            .expect_err("must abort");
        assert!(err.to_string().contains("giving up after 2 attempts"), "got: {err}");
    }

    #[tokio::test(start_paused = true)]
    async fn engine_fatal_on_one_shard_fails_the_whole_sum() {
        let fetcher = scripted_shards(
            "chunks",
            3,
            vec![
                (
                    ("chunks/", None),
                    vec![Script::Page { bytes: 0, next_token: Some("t0".to_string()) }],
                ),
                (("chunks/XX", None), vec![Script::Fatal("access denied".to_string())]),
            ],
        );
        let err = sum(Arc::clone(&fetcher) as _, "chunks", true, 3)
            .await
            .expect_err("must abort");
        assert!(err.to_string().contains("access denied"), "got: {err}");
        // The fatal attempt is not retried.
        let xx_calls = fetcher.calls().iter().filter(|(p, _)| p == "chunks/XX").count();
        assert_eq!(xx_calls, 1);
    }

    /// Fetcher that shrinks the published limit to 0 the instant it serves the
    /// first page, forcing the worker driving that shard out of admission
    /// mid-prefix. Records how many pages it served.
    struct ShrinkingFetcher {
        limit_tx: watch::Sender<usize>,
        calls: Mutex<usize>,
    }

    #[async_trait]
    impl ListPageFetcher for ShrinkingFetcher {
        async fn attempt_page(&self, _prefix: &str, token: Option<&str>) -> PageAttempt {
            *lock(&self.calls) += 1;
            if token.is_none() {
                let _ = self.limit_tx.send(0);
                PageAttempt::Page { bytes: 1, next_token: Some("t1".to_string()) }
            } else {
                PageAttempt::Page { bytes: 2, next_token: None }
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn worker_reenqueues_continuation_instead_of_holding_it_when_excluded() {
        // Worker index 0 is admitted for the first page, which drops the limit
        // to 0. Rather than parking with the "t1" continuation held in its
        // suspended future (the drain-burst bug), it must re-enqueue that work
        // item and park empty-handed.
        let (limit_tx, limit_rx) = watch::channel(1usize);
        // Keep the drain sender alive so the drain signal never closes and
        // admits the worker.
        let (_drain_tx, drain_rx) = watch::channel(false);
        let fetcher = Arc::new(ShrinkingFetcher {
            limit_tx: limit_tx.clone(),
            calls: Mutex::new(0),
        });
        let engine = Engine::new(Arc::clone(&fetcher) as _, 3, Governor::new());
        let queue: Arc<Mutex<VecDeque<WorkItem>>> =
            Arc::new(Mutex::new(VecDeque::from([("chunks/00".to_string(), None)])));
        let handle = tokio::spawn(Arc::clone(&engine).worker(
            Arc::clone(&queue),
            limit_rx,
            drain_rx,
            0,
        ));

        // Let the worker pop, serve page 1 (shrinking the limit), re-enqueue
        // its continuation, and park at the admission gate.
        let mut reenqueued = None;
        for _ in 0..50 {
            tokio::task::yield_now().await;
            let front = lock(&queue).front().cloned();
            if front.is_some() {
                reenqueued = front;
                break;
            }
        }
        assert_eq!(reenqueued, Some(("chunks/00".to_string(), Some("t1".to_string()))));
        // The parked worker did NOT resume the continuation itself: exactly one
        // page was fetched, so there is no drain-time burst waiting to fire.
        assert_eq!(*lock(&fetcher.calls), 1);
        handle.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn shared_controller_sums_several_prefixes() {
        // Two shardable prefixes summed through one shared controller: both
        // fan out and total correctly while sharing one limit signal.
        let mut entries: Vec<ScriptEntry> = all_shards("chunks", 3);
        entries.extend(all_shards("manifests", 5));
        entries.push((
            ("chunks/".to_string(), None),
            vec![Script::Page { bytes: 9, next_token: Some("t0".to_string()) }],
        ));
        entries.push((
            ("manifests/".to_string(), None),
            vec![Script::Page { bytes: 9, next_token: Some("t0".to_string()) }],
        ));
        let fetcher = ScriptedFetcher::new(
            entries.iter().map(|((p, t), o)| ((p.as_str(), t.as_deref()), o.clone())),
        );

        let controller = SharedController::new();
        let done = Arc::new(tokio::sync::Notify::new());
        let runs = {
            let fetcher = Arc::clone(&fetcher);
            let controller = &controller;
            let done = Arc::clone(&done);
            async move {
                let chunks = sum_with_controller(
                    Arc::clone(&fetcher) as _,
                    "chunks",
                    true,
                    3,
                    controller,
                );
                let manifests = sum_with_controller(
                    Arc::clone(&fetcher) as _,
                    "manifests",
                    true,
                    3,
                    controller,
                );
                let (a, b) = tokio::join!(chunks, manifests);
                done.notify_one();
                (a, b)
            }
        };
        let ((chunks, manifests), ()) =
            tokio::join!(runs, controller.run(done.notified()));
        assert_eq!(chunks.ok(), Some(1024 * 3));
        assert_eq!(manifests.ok(), Some(1024 * 5));
    }
}
