//! Sums object sizes with many concurrent raw-prefix list requests.
//! The limit adapts to pages per second: slow links lose throughput past a few dozen
//! requests without any errors, and fast links need hundreds.

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use tokio::task::JoinSet;
use tokio::time::{Instant, MissedTickBehavior};

use crate::{StorageError, StorageResult, other_error};

/// In lexicographic order. Every object id starts with one of these characters.
pub const CROCKFORD: &str = "0123456789ABCDEFGHJKMNPQRSTVWXYZ";

const CONCURRENCY_FLOOR: usize = 8;
const CONCURRENCY_START: usize = 16;
/// Fetchers size their HTTP connection pools to this.
pub const CONCURRENCY_CAP: usize = 1024;

const SAMPLE_INTERVAL: Duration = Duration::from_millis(500);
const MIN_JUDGE_PAGES: u64 = 24;
const MAX_WINDOW_SECS: f64 = 2.0;
const GOODPUT_IMPROVEMENT_FACTOR: f64 = 1.15;
const ERROR_BURST_FRACTION: f64 = 0.02;
const BEST_GOODPUT_DECAY: f64 = 0.5;

/// Outcome of one attempt at one list page.
#[derive(Debug)]
pub enum PageAttempt {
    /// The whole body was read.
    Page {
        bytes: u64,
        next_token: Option<String>,
    },
    /// Throttling, 5xx, timeouts, and read errors mid-body.
    Retryable(StorageError),
    Fatal(StorageError),
}

pub fn is_transient_status(status: u16) -> bool {
    status == 429 || (500..=599).contains(&status)
}

/// One attempt at one page of a provider's raw-prefix list API, with no retries.
#[async_trait]
pub trait ListPageFetcher: Send + Sync + 'static {
    async fn attempt_page(&self, list_prefix: &str, token: Option<&str>) -> PageAttempt;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    Probing,
    Holding,
}

/// Doubles the limit while pages per second improve by 15%, then holds the best limit.
/// Holding reacts only to error bursts, because the end-of-run drain lowers throughput.
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

    /// Small windows accumulate until they can be judged. The first window after a
    /// change is discarded, because connection warm-up distorts it.
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
            // Stop probing: growing back toward a throttle ceiling oscillates.
            self.set_limit((self.limit / 2).max(CONCURRENCY_FLOOR));
            self.best_goodput *= BEST_GOODPUT_DECAY;
            self.best_limit = self.limit;
            self.phase = Phase::Holding;
            return self.limit;
        }

        if self.phase == Phase::Probing {
            // Zero pages would pass the 15% test against a zero baseline. Shed, but keep
            // probing: a slow store can leave an early window empty without a stall.
            if pages == 0 {
                self.set_limit((self.limit / 2).max(CONCURRENCY_FLOOR));
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
                    self.set_limit((self.limit * 2).min(CONCURRENCY_CAP));
                }
            } else {
                self.set_limit(self.best_limit);
                self.phase = Phase::Holding;
            }
        }
        self.limit
    }

    fn set_limit(&mut self, limit: usize) {
        if limit != self.limit {
            self.limit = limit;
            self.discard_next = true;
        }
    }
}

#[derive(Debug)]
enum Work {
    /// First page of a prefix that holds object ids. If it is truncated, the result is
    /// dropped and the prefix is split.
    Probe(String),
    Page {
        prefix: String,
        token: Option<String>,
    },
}

/// Sums object sizes under each `(list_prefix, holds_ids)`. A prefix that holds ids is
/// listed as `{list_prefix}/`; any other prefix is listed exactly as given.
pub async fn sum(
    fetcher: Arc<dyn ListPageFetcher>,
    prefixes: &[(String, bool)],
    max_attempts: u32,
) -> StorageResult<u64> {
    let mut queue: VecDeque<Work> = prefixes
        .iter()
        .map(|(prefix, holds_ids)| {
            if *holds_ids {
                Work::Probe(prefix.clone())
            } else {
                Work::Page { prefix: prefix.clone(), token: None }
            }
        })
        .collect();
    let errors = Arc::new(AtomicU64::new(0));
    let mut controller = ConcurrencyController::new();
    let mut limit = controller.limit;
    let mut pages = 0u64;
    let mut total = 0u64;
    let mut in_flight = JoinSet::new();
    let mut ticker =
        tokio::time::interval_at(Instant::now() + SAMPLE_INTERVAL, SAMPLE_INTERVAL);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut last_sample = Instant::now();

    loop {
        while in_flight.len() < limit
            && let Some(work) = queue.pop_front()
        {
            let fetcher = Arc::clone(&fetcher);
            let errors = Arc::clone(&errors);
            // One task per page so body scanning spreads across cores.
            in_flight.spawn(async move {
                let (prefix, token) = match &work {
                    Work::Probe(base) => (format!("{base}/"), None),
                    Work::Page { prefix, token } => (prefix.clone(), token.clone()),
                };
                let page = fetch_page(
                    &*fetcher,
                    &prefix,
                    token.as_deref(),
                    max_attempts,
                    &errors,
                )
                .await?;
                Ok::<_, StorageError>((work, page))
            });
        }
        if in_flight.is_empty() {
            return Ok(total);
        }
        tokio::select! {
            joined = in_flight.join_next() => {
                let Some(joined) = joined else { continue };
                let (work, (bytes, next_token)) = match joined {
                    Ok(result) => result?,
                    Err(e) if e.is_panic() => std::panic::resume_unwind(e.into_panic()),
                    Err(e) => return Err(other_error(format!("list task failed: {e}"))),
                };
                pages += 1;
                match (work, next_token) {
                    (Work::Probe(base), Some(_)) => queue.extend(split(&base)),
                    (Work::Probe(_), None) => total = total.saturating_add(bytes),
                    (Work::Page { prefix, .. }, token) => {
                        total = total.saturating_add(bytes);
                        if token.is_some() {
                            queue.push_front(Work::Page { prefix, token });
                        }
                    }
                }
            }
            _ = ticker.tick() => {
                let now = Instant::now();
                let secs = now.duration_since(last_sample).as_secs_f64();
                last_sample = now;
                limit = controller.observe(pages, errors.swap(0, Ordering::Relaxed), secs);
                pages = 0;
            }
        }
    }
}

fn split(base: &str) -> impl Iterator<Item = Work> + '_ {
    CROCKFORD.chars().flat_map(move |a| {
        CROCKFORD
            .chars()
            .map(move |b| Work::Page { prefix: format!("{base}/{a}{b}"), token: None })
    })
}

/// Makes at most `max_attempts` tries, with jittered exponential backoff.
async fn fetch_page(
    fetcher: &dyn ListPageFetcher,
    list_prefix: &str,
    token: Option<&str>,
    max_attempts: u32,
    errors: &AtomicU64,
) -> StorageResult<(u64, Option<String>)> {
    let mut attempt: u32 = 0;
    loop {
        attempt += 1;
        match fetcher.attempt_page(list_prefix, token).await {
            PageAttempt::Page { bytes, next_token } => return Ok((bytes, next_token)),
            PageAttempt::Retryable(cause) => {
                errors.fetch_add(1, Ordering::Relaxed);
                if attempt >= max_attempts {
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
    use std::sync::{Mutex, MutexGuard};

    use super::*;

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
        // 420 pages/s is less than 15% over the best 400 pages/s.
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
        assert_eq!(c.observe(24, 0, 0.5), 32);
        assert_eq!(c.observe(24, 0, 0.5), 16);
    }

    #[test]
    fn controller_zero_page_window_sheds_toward_floor() {
        let mut c = ConcurrencyController::new();
        assert_eq!(c.observe(0, 0, 2.0), CONCURRENCY_FLOOR);
        assert_eq!(c.observe(0, 0, 2.0), CONCURRENCY_FLOOR);
        assert_eq!(c.observe(0, 0, 2.0), CONCURRENCY_FLOOR);
    }

    #[test]
    fn controller_recovers_after_zero_page_window() {
        let mut c = ConcurrencyController::new();
        assert_eq!(c.observe(0, 0, 2.0), CONCURRENCY_FLOOR);
        assert_eq!(c.observe(400, 0, 0.5), CONCURRENCY_FLOOR);
        assert_eq!(c.observe(400, 0, 0.5), 16);
        assert_eq!(c.observe(400, 0, 0.5), 16);
        assert_eq!(c.observe(800, 0, 0.5), 32);
    }

    #[test]
    fn controller_zero_page_window_after_ramp_sheds_not_doubles() {
        let mut c = ConcurrencyController::new();
        assert_eq!(c.observe(100, 0, 0.5), 32);
        assert_eq!(c.observe(100, 0, 0.5), 32);
        assert_eq!(c.observe(0, 0, 2.0), 16);
    }

    type PageKey = (String, Option<String>);

    fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
        m.lock().expect("poisoned")
    }

    #[derive(Debug, Clone)]
    enum Script {
        Page { bytes: u64, next_token: Option<String> },
        Retryable,
        Fatal(String),
    }

    const SCRIPTED_RETRYABLE_CAUSE: &str = "scripted transient failure";

    /// Plays back scripted outcomes per `(prefix, token)`; the last outcome repeats.
    #[derive(Default)]
    struct ScriptedFetcher {
        script: Mutex<HashMap<PageKey, VecDeque<Script>>>,
        calls: Mutex<Vec<PageKey>>,
        in_flight: AtomicU64,
        max_in_flight: AtomicU64,
    }

    impl ScriptedFetcher {
        fn with(
            mut self,
            prefix: &str,
            token: Option<&str>,
            outcomes: Vec<Script>,
        ) -> Self {
            self.script
                .get_mut()
                .expect("poisoned")
                .insert((prefix.to_string(), token.map(str::to_string)), outcomes.into());
            self
        }

        fn with_all_shards(mut self, base: &str, bytes: u64) -> Self {
            for work in split(base) {
                if let Work::Page { prefix, .. } = work {
                    self = self.with(&prefix, None, page(bytes));
                }
            }
            self
        }

        fn calls(&self) -> Vec<PageKey> {
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
            let now = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
            self.max_in_flight.fetch_max(now, Ordering::SeqCst);
            tokio::task::yield_now().await;
            self.in_flight.fetch_sub(1, Ordering::SeqCst);

            let key = (list_prefix.to_string(), token.map(str::to_string));
            lock(&self.calls).push(key.clone());
            let outcome = match lock(&self.script).get_mut(&key) {
                Some(outcomes) if outcomes.len() > 1 => outcomes.pop_front(),
                Some(outcomes) => outcomes.front().cloned(),
                None => None,
            }
            .unwrap_or_else(|| Script::Fatal(format!("unscripted page {key:?}")));
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

    fn truncated(bytes: u64, token: &str) -> Vec<Script> {
        vec![Script::Page { bytes, next_token: Some(token.to_string()) }]
    }

    async fn run(
        fetcher: ScriptedFetcher,
        prefixes: &[(&str, bool)],
        max_attempts: u32,
    ) -> (StorageResult<u64>, Arc<ScriptedFetcher>) {
        let fetcher = Arc::new(fetcher);
        let prefixes: Vec<(String, bool)> =
            prefixes.iter().map(|(p, ids)| (p.to_string(), *ids)).collect();
        let total = sum(Arc::clone(&fetcher) as _, &prefixes, max_attempts).await;
        (total, fetcher)
    }

    #[tokio::test(start_paused = true)]
    async fn single_page_probe_is_the_only_request() {
        let fetcher = ScriptedFetcher::default().with("chunks/", None, page(7));
        let (total, fetcher) = run(fetcher, &[("chunks", true)], 3).await;
        assert_eq!(total.ok(), Some(7));
        assert_eq!(fetcher.calls(), vec![("chunks/".to_string(), None)]);
    }

    #[tokio::test(start_paused = true)]
    async fn truncated_probe_splits_into_all_shards() {
        let fetcher = ScriptedFetcher::default()
            .with_all_shards("chunks", 3)
            .with("chunks/", None, truncated(1_000_000, "t0"))
            .with("chunks/00", None, truncated(5, "t1"))
            .with("chunks/00", Some("t1"), page(6));
        let (total, fetcher) = run(fetcher, &[("chunks", true)], 3).await;
        // The probe's bytes are not counted: the shards list them again.
        assert_eq!(total.ok(), Some(1023 * 3 + 5 + 6));

        let calls = fetcher.calls();
        let unique: HashSet<_> = calls.iter().collect();
        assert_eq!(unique.len(), calls.len());
        assert_eq!(calls.len(), 1 + 1024 + 1);
    }

    #[tokio::test(start_paused = true)]
    async fn sums_several_prefixes_in_one_call() {
        let fetcher = ScriptedFetcher::default()
            .with_all_shards("chunks", 3)
            .with_all_shards("manifests", 5)
            .with("chunks/", None, truncated(9, "t0"))
            .with("manifests/", None, truncated(9, "t0"))
            .with("refs", None, truncated(1, "r1"))
            .with("refs", Some("r1"), page(2));
        let (total, _) =
            run(fetcher, &[("chunks", true), ("manifests", true), ("refs", false)], 3)
                .await;
        assert_eq!(total.ok(), Some(1024 * 3 + 1024 * 5 + 1 + 2));
    }

    #[tokio::test(start_paused = true)]
    async fn in_flight_requests_never_exceed_the_limit() {
        let fetcher = ScriptedFetcher::default().with_all_shards("chunks", 1).with(
            "chunks/",
            None,
            truncated(0, "t0"),
        );
        let (total, fetcher) = run(fetcher, &[("chunks", true)], 3).await;
        assert_eq!(total.ok(), Some(1024));
        let max = fetcher.max_in_flight.load(Ordering::SeqCst);
        assert!(max > 1 && max <= CONCURRENCY_START as u64, "max in flight {max}");
    }

    #[tokio::test(start_paused = true)]
    async fn retries_retryable_attempts_then_succeeds() {
        let fetcher = ScriptedFetcher::default().with(
            "refs",
            None,
            vec![
                Script::Retryable,
                Script::Retryable,
                Script::Retryable,
                Script::Page { bytes: 42, next_token: None },
            ],
        );
        let (total, fetcher) = run(fetcher, &[("refs", false)], 10).await;
        assert_eq!(total.ok(), Some(42));
        assert_eq!(fetcher.calls().len(), 4);
    }

    #[tokio::test(start_paused = true)]
    async fn gives_up_after_max_attempts_with_cause() {
        let fetcher =
            ScriptedFetcher::default().with("refs", None, vec![Script::Retryable]);
        let (total, fetcher) = run(fetcher, &[("refs", false)], 2).await;
        let msg = total.expect_err("must give up").to_string();
        assert!(msg.contains("giving up after 2 attempts"), "got: {msg}");
        assert!(msg.contains(SCRIPTED_RETRYABLE_CAUSE), "got: {msg}");
        assert_eq!(fetcher.calls().len(), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn exhausted_retries_on_one_shard_fail_the_sum() {
        let fetcher = ScriptedFetcher::default()
            .with_all_shards("chunks", 3)
            .with("chunks/", None, truncated(0, "t0"))
            .with("chunks/7Z", None, vec![Script::Retryable]);
        let (total, _) = run(fetcher, &[("chunks", true)], 2).await;
        let msg = total.expect_err("must fail").to_string();
        assert!(msg.contains("giving up after 2 attempts"), "got: {msg}");
    }

    #[tokio::test(start_paused = true)]
    async fn fatal_on_one_shard_fails_the_sum_without_retry() {
        let fetcher = ScriptedFetcher::default()
            .with_all_shards("chunks", 3)
            .with("chunks/", None, truncated(0, "t0"))
            .with("chunks/XX", None, vec![Script::Fatal("access denied".to_string())]);
        let (total, fetcher) = run(fetcher, &[("chunks", true)], 3).await;
        let msg = total.expect_err("must fail").to_string();
        assert!(msg.contains("access denied"), "got: {msg}");
        let xx_calls = fetcher.calls().iter().filter(|(p, _)| p == "chunks/XX").count();
        assert_eq!(xx_calls, 1);
    }
}
