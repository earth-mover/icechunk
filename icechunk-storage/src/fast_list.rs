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
//! flattens, and afterwards reacts only to bursts of retryable errors by
//! halving. Individual requests keep their jittered backoff; the controller is
//! a coarser mechanism layered on top, the way BBR coexists with retransmits.

use std::collections::VecDeque;
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    Probing,
    Holding,
}

/// Pure BBR-startup-style governor for the fan-out worker limit.
///
/// Goodput (pages/sec) is the control signal because the failure mode this
/// exists for — congestion collapse on narrow links — emits no error signal at
/// all. The driver in [`sum`] feeds sampled counter deltas through
/// [`Self::observe`]; the struct never reads a clock and spawns nothing, so
/// tests drive it with synthetic windows.
///
/// While probing, the limit doubles as long as each step improves goodput by
/// ≥15%; the first flat or regressed window reverts to the best-seen limit and
/// holds. Holding ignores goodput on purpose — the end-of-run tail drain sinks
/// goodput, and shrinking then would be pointless — reacting only to bursts of
/// retryable errors (>2% of a window's pages) by halving the limit. An
/// error-halving also ends probing rather than restarting it: regrowing toward
/// a throttle ceiling the run just hit would oscillate for the rest of a short
/// run, and the goodput-vs-concurrency plateau is broad enough that the halved
/// limit stays within it.
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
    /// (503/429/5xx), timeouts, connect failures, mid-body read errors.
    Retryable,
    /// A failure retrying cannot fix; aborts the whole sum.
    Fatal(StorageError),
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
pub async fn sum(
    fetcher: Arc<dyn ListPageFetcher>,
    base_prefix: &str,
    shardable: bool,
    max_retries: u32,
) -> StorageResult<u64> {
    let engine = Arc::new(Engine {
        fetcher,
        max_retries,
        pages_completed: AtomicU64::new(0),
        retryable_errors: AtomicU64::new(0),
    });
    engine.sum(base_prefix, shardable).await
}

struct Engine {
    fetcher: Arc<dyn ListPageFetcher>,
    max_retries: u32,
    /// Counter deltas sampled (and reset) every [`SAMPLE_INTERVAL`] by the
    /// controller driver in [`Self::sum`].
    pages_completed: AtomicU64,
    retryable_errors: AtomicU64,
}

impl Engine {
    async fn sum(
        self: Arc<Self>,
        base_prefix: &str,
        shardable: bool,
    ) -> StorageResult<u64> {
        if !shardable {
            return self.sum_prefix(base_prefix.to_string()).await;
        }
        // Probe one page first: a prefix that fits in a single response is
        // summed with exactly one request. Only truncated (>1000-object)
        // prefixes pay for the fan-out below.
        let (probe_bytes, next_token) =
            self.fetch_page(&format!("{base_prefix}/"), None).await?;
        if next_token.is_none() {
            return Ok(probe_bytes);
        }
        // Ids are >= 2 Crockford characters, so the 1024 two-character
        // sub-prefixes are disjoint and cover the whole keyspace.
        let queue: Arc<Mutex<VecDeque<String>>> = Arc::new(Mutex::new(
            CROCKFORD
                .chars()
                .flat_map(|a| {
                    CROCKFORD.chars().map(move |b| format!("{base_prefix}/{a}{b}"))
                })
                .collect(),
        ));

        let (limit_tx, limit_rx) = watch::channel(CONCURRENCY_START);
        let mut workers: JoinSet<StorageResult<u64>> = JoinSet::new();
        for index in 0..CONCURRENCY_CAP {
            workers.spawn(Arc::clone(&self).worker(
                Arc::clone(&queue),
                limit_rx.clone(),
                index,
            ));
        }
        drop(limit_rx);

        let mut controller = ConcurrencyController::new();
        let mut ticker =
            tokio::time::interval_at(Instant::now() + SAMPLE_INTERVAL, SAMPLE_INTERVAL);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let mut last_sample = Instant::now();
        // The fan-out re-lists the whole keyspace, so the truncated probe
        // page's bytes must not be counted a second time.
        let mut total = 0u64;
        let mut draining = false;

        let result = loop {
            tokio::select! {
                joined = workers.join_next() => match joined {
                    None => break Ok(total),
                    Some(Ok(Ok(subtotal))) => {
                        total = total.saturating_add(subtotal);
                        // A worker exits cleanly only after seeing the queue
                        // empty, so parked workers can never be needed again:
                        // admit everyone (they observe the empty queue and
                        // exit) and freeze the limit so no worker re-parks
                        // mid-prefix with no future admission coming.
                        if !draining {
                            draining = true;
                            let _ = limit_tx.send(CONCURRENCY_CAP);
                        }
                    }
                    Some(Ok(Err(e))) => break Err(e),
                    Some(Err(join_error)) => {
                        if join_error.is_panic() {
                            std::panic::resume_unwind(join_error.into_panic());
                        }
                    }
                },
                _ = ticker.tick(), if !draining => {
                    let pages = self.pages_completed.swap(0, Ordering::Relaxed);
                    let errors = self.retryable_errors.swap(0, Ordering::Relaxed);
                    let now = Instant::now();
                    let secs = now.duration_since(last_sample).as_secs_f64();
                    last_sample = now;
                    let limit = controller.observe(pages, errors, secs);
                    if *limit_tx.borrow() != limit {
                        let _ = limit_tx.send(limit);
                    }
                },
            }
        };
        if result.is_err() {
            workers.abort_all();
        }
        while workers.join_next().await.is_some() {}
        result
    }

    /// One fan-out worker: pops shard prefixes and pages through them until the
    /// queue is empty. Admission is re-checked between pages, not prefixes — a
    /// busy shard of a large repo holds hundreds of pages, and a shrink must
    /// take effect after the in-flight request, not minutes later.
    async fn worker(
        self: Arc<Self>,
        queue: Arc<Mutex<VecDeque<String>>>,
        mut limit_rx: watch::Receiver<usize>,
        index: usize,
    ) -> StorageResult<u64> {
        let mut subtotal = 0u64;
        loop {
            wait_admitted(index, &mut limit_rx).await;
            let popped = queue
                .lock()
                .map_err(|_| other_error("shard queue mutex poisoned"))?
                .pop_front();
            let Some(prefix) = popped else {
                return Ok(subtotal);
            };
            let mut token: Option<String> = None;
            loop {
                let (bytes, next) = self.fetch_page(&prefix, token.as_deref()).await?;
                subtotal = subtotal.saturating_add(bytes);
                match next {
                    Some(t) => token = Some(t),
                    None => break,
                }
                wait_admitted(index, &mut limit_rx).await;
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
    /// with jittered exponential backoff. A page counts as completed only once
    /// the fetcher has scanned its body to the end, so a mid-body read failure
    /// re-fetches the whole page.
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
                    self.pages_completed.fetch_add(1, Ordering::Relaxed);
                    return Ok((bytes, next_token));
                }
                PageAttempt::Retryable => {
                    self.retryable_errors.fetch_add(1, Ordering::Relaxed);
                }
                PageAttempt::Fatal(e) => return Err(e),
            }
            if attempt > self.max_retries {
                return Err(other_error(format!(
                    "list giving up after {attempt} attempts for prefix {list_prefix:?}"
                )));
            }
            tokio::time::sleep(backoff(attempt)).await;
        }
    }
}

/// Park until `index` is below the published worker limit. A closed channel
/// means the driving [`sum`] is gone (all workers are being torn down);
/// treating it as admission lets the worker run to its natural exit instead of
/// hanging.
async fn wait_admitted(index: usize, limit_rx: &mut watch::Receiver<usize>) {
    while index >= *limit_rx.borrow_and_update() {
        if limit_rx.changed().await.is_err() {
            return;
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
                Script::Retryable => PageAttempt::Retryable,
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
        let engine = Arc::new(Engine {
            fetcher: Arc::clone(&fetcher) as _,
            max_retries: 10,
            pages_completed: AtomicU64::new(0),
            retryable_errors: AtomicU64::new(0),
        });
        let total = Arc::clone(&engine).sum("refs", false).await;
        assert_eq!(total.ok(), Some(42));
        assert_eq!(fetcher.calls().len(), 4);
        assert_eq!(engine.pages_completed.load(Ordering::Relaxed), 1);
        assert_eq!(engine.retryable_errors.load(Ordering::Relaxed), 3);
    }

    #[tokio::test(start_paused = true)]
    async fn engine_gives_up_past_max_retries() {
        let fetcher = ScriptedFetcher::new([(("refs", None), vec![Script::Retryable])]);
        let err = sum(Arc::clone(&fetcher) as _, "refs", false, 2)
            .await
            .expect_err("must give up");
        assert!(err.to_string().contains("giving up after 3 attempts"), "got: {err}");
        assert_eq!(fetcher.calls().len(), 3);
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
        let err = sum(Arc::clone(&fetcher) as _, "chunks", true, 1)
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
}
