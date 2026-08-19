//! A governor that holds one workload to a target network bandwidth `B`
//! without exceeding a memory budget `M`.
//!
//! A governor cannot control bandwidth directly: it only decides when work
//! may start. It holds two invariants over the in-flight work, each
//! enforced by a [`WeightedPool`]:
//!
//! - **Bandwidth per direction** (capacity `B`, costs in bytes/s), acquired
//!   per HTTP request. A request costs the bandwidth it is expected to
//!   consume while active: latency-priced from its size, floored at
//!   `min_request_bytes`, and never more than what one connection can
//!   currently deliver (`observed_connection_bandwidth`, an adaptive
//!   estimate fed by completed saturating transfers).
//! - **Memory** (capacity `M`, costs in bytes), reserved per logical read
//!   fetch for the total expected size, released at hand-off. Writes don't
//!   reserve memory.
//!
//! Throttle signals (per-attempt ones through `record_throttle` and
//! terminal `Throttled` outcomes) drive an AIMD
//! (additive-increase/multiplicative-decrease) response: enough signals
//! in one detection window multiplicatively cut the bandwidth the pools
//! admit, and windows without throttles recover it additively toward the
//! target.
//!
//! `B` and `M` are runtime-adjustable through [`BandwidthGovernor`]'s
//! knobs
//!
//! See `design-docs/019-better-io-control.md`.

use std::{
    any::Any,
    collections::VecDeque,
    sync::{
        Arc, Mutex, MutexGuard, PoisonError,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::time::Instant;

use super::pool::{PoolGuard, WeightedPool};
use crate::storage::{
    Direction, GovernorFactory, IoClass, IoGovernor, IoOutcome, IoPermit, IoResult,
    MemoryPermit, MemoryState, PermitState,
};

/// Default for [`DirectionConfig::unknown_request_bytes`].
pub fn default_unknown_request_bytes() -> u64 {
    1024 * 1024 // 1 MiB
}

/// Default for [`BandwidthGovernorConfig::unknown_object_bytes`].
pub fn default_unknown_object_bytes() -> u64 {
    4 * 1024 * 1024 // 4 MiB
}

/// Zero-valued knobs and targets are floored here: a zero-capacity pool
/// would admit zero-cost work without bound, while capacity 1 serializes
/// it, which is what asking for no bandwidth or no memory should mean.
const KNOB_FLOOR: u64 = 1;

/// Smoothing of the exponentially weighted moving averages (EWMA) behind
/// the connection-bandwidth estimate: each accepted sample moves the byte
/// and duration accumulators 1/`EWMA_SMOOTHING` of the way toward it.
/// Higher is smoother but slower to adapt; 8 is what TCP's
/// round-trip-time estimator uses.
const EWMA_SMOOTHING: u128 = 8;

/// Length of the AIMD detection window.
///
/// Throttle signals and completions are counted over consecutive
/// windows of this length, which turns raw counts into a rate:
/// enough throttles inside one window mean the store is overloaded *right
/// now* and cut the effective bandwidth, while a window that ends with
/// zero throttles is evidence of health and recovers some.
///
/// In nanoseconds, like every timestamp in this module (they all derive
/// from [`Inner::now_ns`]).
const AIMD_WINDOW_NS: u64 = Duration::from_secs(1).as_nanos() as u64;

/// A window cuts once its throttle count reaches
/// `max(AIMD_CUT_MIN_THROTTLES, completions × AIMD_CUT_THROTTLE_PERCENT / 100)`:
/// a couple of stray signals under heavy traffic shouldn't trigger a cut,
/// but a small absolute number must, whatever the assigned target.
const AIMD_CUT_MIN_THROTTLES: u64 = 4;
const AIMD_CUT_THROTTLE_PERCENT: u64 = 1;

/// Each cut multiplies the AIMD factor by 0.7...
const AIMD_CUT_FACTOR_PERCENT: u64 = 70;
/// ...never taking it below 2% of the assigned bandwidth.
const AIMD_FACTOR_FLOOR_PERCENT: u64 = 2;

/// After a cut, further cuts are held off until the requests admitted
/// before it have drained, reacting to them would cut repeatedly for one
/// overload episode.
///
/// ```text
/// holdoff = clamp(p95 recent request duration + MARGIN, MIN, MAX)
/// ```
///
/// The p95 estimates when the slowest pre-cut requests finish. The
/// margin covers the throttled ones among them: the storage libraries
/// retry those internally, sleeping a backoff interval between attempts,
/// so their last attempts — and last signals — surface about one backoff
/// after a normal completion would. The clamp keeps the hold-off sane
/// when the duration samples are missing, all-fast, or pathologically
/// slow.
const AIMD_HOLDOFF_MARGIN_NS: u64 = Duration::from_secs(1).as_nanos() as u64;
const AIMD_HOLDOFF_MIN_NS: u64 = Duration::from_secs(2).as_nanos() as u64;
const AIMD_HOLDOFF_MAX_NS: u64 = Duration::from_secs(15).as_nanos() as u64;

/// Completed-request durations kept for the p95 drain estimate.
const AIMD_DURATION_SAMPLES: usize = 256;

/// Per-direction constants and target for a [`BandwidthGovernor`].
///
/// Bandwidths are bytes/s. The backend constants describe what one HTTP
/// connection to the store can do
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DirectionConfig {
    /// The target bandwidth; runtime-adjustable after construction.
    pub target_bandwidth: u64,
    /// The most one connection can sustain; cap of the adaptive estimate.
    pub max_connection_bandwidth: u64,
    /// What a cold connection delivers; floor and starting value of the
    /// adaptive estimate.
    pub min_connection_bandwidth: u64,
    /// Assumed per-request latency (healthy-store median or lower).
    pub request_latency: Duration,
    /// Requests are priced as if at least this big, so tiny requests can't
    /// be admitted without bound.
    pub min_request_bytes: u64,
    /// Requests of unknown size are priced as this many bytes.
    #[serde(default = "default_unknown_request_bytes")]
    pub unknown_request_bytes: u64,
}

/// Serializable recipe for a [`BandwidthGovernor`]; also its factory.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BandwidthGovernorConfig {
    /// Intern-table identity and telemetry name.
    pub label: String,
    pub read: DirectionConfig,
    pub write: DirectionConfig,
    /// Memory budget in bytes; reads only. Runtime-adjustable.
    pub memory_budget: u64,
    /// Logical fetches of unknown size reserve this much memory, trued up
    /// when the actual size becomes known.
    #[serde(default = "default_unknown_object_bytes")]
    pub unknown_object_bytes: u64,
}

impl BandwidthGovernorConfig {
    /// A fresh `governor-xxxxxxxx` label; the bindings use it when no label
    /// is given, so distinct instances never share an identity by accident
    /// (the label is part of the intern key that decides which deserialized
    /// sessions rebind to one governor).
    pub fn random_label() -> String {
        format!("governor-{:08x}", rand::random::<u32>())
    }

    /// A config with S3 backend constants
    pub fn s3_defaults(
        label: impl Into<String>,
        read_bandwidth: u64,
        write_bandwidth: u64,
        memory_budget: u64,
    ) -> Self {
        let direction = |target_bandwidth, max_connection_bandwidth| DirectionConfig {
            target_bandwidth,
            max_connection_bandwidth,
            min_connection_bandwidth: 7_500_000,
            request_latency: Duration::from_millis(30),
            min_request_bytes: 30_000,
            unknown_request_bytes: default_unknown_request_bytes(),
        };
        Self {
            label: label.into(),
            read: direction(read_bandwidth, 90_000_000),
            write: direction(write_bandwidth, 20_000_000),
            memory_budget,
            unknown_object_bytes: default_unknown_object_bytes(),
        }
    }
}

#[typetag::serde(name = "bandwidth")]
impl GovernorFactory for BandwidthGovernorConfig {
    fn build(&self) -> Arc<dyn IoGovernor> {
        Arc::new(BandwidthGovernor::new(self))
    }
}

/// A governor that holds one workload to a target network bandwidth `B`
/// without exceeding a memory budget `M`, both runtime-adjustable.
///
/// HTTP requests are admitted against `B` at the bandwidth they are
/// expected to consume while active, and logical read fetches reserve
/// their total size against `M` until hand-off.
#[derive(Debug)]
pub struct BandwidthGovernor {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    label: String,
    read: DirectionState,
    write: DirectionState,
    /// Capacity `M`, costs in bytes.
    memory: WeightedPool,
    unknown_object_bytes: u64,
    /// Zero point for monotonic nanosecond stamps.
    epoch: Instant,
}

impl Inner {
    fn direction(&self, direction: Direction) -> &DirectionState {
        match direction {
            Direction::Read => &self.read,
            Direction::Write => &self.write,
        }
    }

    fn now_ns(&self) -> u64 {
        Instant::now().duration_since(self.epoch).as_nanos().min(u64::MAX as u128) as u64
    }
}

#[derive(Debug)]
struct DirectionState {
    /// As configured; its `target_bandwidth` is only the initial value,
    /// the sibling atomic is the live one.
    cfg: DirectionConfig,
    /// Live `B`, bytes/s.
    target_bandwidth: AtomicU64,
    /// Estimate of what one connection currently delivers (bytes/s),
    /// published from the accumulators in [`AimdState`] and clamped to
    /// `[min_connection_bandwidth, max_connection_bandwidth]`.
    observed_connection_bandwidth: AtomicU64,
    /// Capacity = the effective bandwidth, costs in bytes/s.
    pool: WeightedPool,
    /// When the last throttle signal arrived; the taint check that skips
    /// bandwidth samples overlapping a throttle (0 = never).
    last_throttle_ns: AtomicU64,
    throttles_total: AtomicU64,
    /// The AIMD throttle response.
    aimd: Mutex<AimdState>,
    /// Admitted, not-yet-finished requests.
    in_flight: AtomicU64,
}

/// State of the AIMD throttle response; one per direction, behind a mutex
/// that is locked briefly and never across an await. Also hosts the
/// connection-estimate accumulators, so the completion path shares one
/// lock.
#[derive(Debug)]
struct AimdState {
    /// Start of the current tumbling detection window (ns since epoch).
    window_start_ns: u64,
    /// Throttle signals counted in the current window.
    window_throttles: u64,
    /// Requests completed in the current window.
    window_completions: u64,
    /// The AIMD cut factor, in percent: the effective bandwidth is
    /// `target_bandwidth × this / 100`; 100 while no cut is in effect.
    factor_percent: u64,
    /// No cut before this stamp (ns since epoch).
    holdoff_until_ns: u64,
    /// Durations of recent completed requests (ns); their p95 estimates
    /// how long pre-cut admissions take to drain.
    durations_ns: VecDeque<u64>,
    /// Connection-estimate accumulators: EWMAs of accepted samples' bytes
    /// and duration; see [`DirectionState::observe_sample`].
    est_bytes: u64,
    est_time_ns: u64,
}

impl AimdState {
    fn new() -> Self {
        Self {
            window_start_ns: 0,
            window_throttles: 0,
            window_completions: 0,
            factor_percent: 100,
            holdoff_until_ns: 0,
            durations_ns: VecDeque::with_capacity(AIMD_DURATION_SAMPLES),
            est_bytes: 0,
            est_time_ns: 0,
        }
    }

    fn p95_duration_ns(&self) -> u64 {
        if self.durations_ns.is_empty() {
            return 0;
        }
        let mut sorted: Vec<u64> = self.durations_ns.iter().copied().collect();
        sorted.sort_unstable();
        sorted[sorted.len() * 95 / 100]
    }
}

impl DirectionState {
    fn new(cfg: DirectionConfig) -> Self {
        let target = cfg.target_bandwidth.max(KNOB_FLOOR);
        Self {
            target_bandwidth: AtomicU64::new(target),
            observed_connection_bandwidth: AtomicU64::new(cfg.min_connection_bandwidth),
            pool: WeightedPool::new(target),
            last_throttle_ns: AtomicU64::new(0),
            throttles_total: AtomicU64::new(0),
            aimd: Mutex::new(AimdState::new()),
            in_flight: AtomicU64::new(0),
            cfg,
        }
    }

    /// The cost of admitting one request: the bandwidth, in bytes/s, it
    /// is expected to consume while in flight.
    ///
    /// ```text
    /// cost = min(observed_connection_bandwidth,
    ///            max(expected_bytes, min_request_bytes) / request_latency)
    /// ```
    ///
    /// The latency model behind the formula: a small request holds its
    /// connection for about `request_latency` no matter its size, so
    /// `size / request_latency` estimates the bandwidth it consumes while
    /// active. That linear price is only right up to the point where the
    /// transfer is large enough to saturate its connection — from there
    /// it consumes exactly what the connection delivers, however much
    /// bigger it gets.
    ///
    /// Three regimes follow, by expected size:
    /// - **floor**: payloads below `min_request_bytes` are priced as if
    ///   that big. Even a near-empty request burns a connection, a round
    ///   trip and request-rate quota, so it can't be admitted for free.
    /// - **latency-bound**: cost grows linearly with size.
    /// - **bandwidth-bound**: capped at `observed_connection_bandwidth`,
    ///   the adaptive estimate of what one connection currently delivers.
    fn request_cost(&self, expected_bytes: Option<u64>) -> u64 {
        // unknown sizes get a configured guess; the floor applies either way
        let bytes = expected_bytes
            .unwrap_or(self.cfg.unknown_request_bytes)
            .max(self.cfg.min_request_bytes);

        // bytes/s = bytes × (1e6 µs/s) / latency_µs
        let latency_us = self.cfg.request_latency.as_micros().max(1);
        let latency_priced =
            (bytes as u128 * 1_000_000 / latency_us).min(u64::MAX as u128) as u64;

        // one request can't consume more than its one connection delivers
        latency_priced.min(self.observed_connection_bandwidth.load(Ordering::Relaxed))
    }

    /// Feed one completed, successful request into
    /// `observed_connection_bandwidth`.
    ///
    /// ```text
    /// if bytes > max_connection_bandwidth × request_latency:
    ///     est_bytes ← est_bytes + (bytes    - est_bytes)   / EWMA_SMOOTHING
    ///     est_time  ← est_time  + (duration - est_time)    / EWMA_SMOOTHING
    ///     observed ← clamp(est_bytes / est_time,
    ///                      min_connection_bandwidth,
    ///                      max_connection_bandwidth)
    /// ```
    ///
    /// Bytes and durations are averaged *separately* and the estimate is
    /// their ratio — the rate of the average slot (S/E[T], a harmonic
    /// mean of the per-request rates). Averaging the rates themselves
    /// would overestimate slot throughput whenever durations are
    /// dispersed
    ///
    /// Smaller payloads are latency-dominated, their duration carries no
    /// bandwidth information, so they don't update.
    fn observe_sample(&self, bytes: u64, duration: Duration) {
        // saturation gate, multiplied through to avoid division:
        // bytes > max_bw (bytes/s) × latency_µs / (1e6 µs/s)
        let latency_us = self.cfg.request_latency.as_micros();
        let saturates = bytes as u128 * 1_000_000
            > self.cfg.max_connection_bandwidth as u128 * latency_us;
        if !saturates {
            return;
        }
        let duration_ns = duration.as_nanos().min(u64::MAX as u128) as u64;

        let mut aimd = self.aimd();
        // the EWMA steps, as ((S-1)·old + sample)/S to stay in unsigned
        // math; u128 so the products can't wrap. Both accumulators are
        // zero-seeded and scale identically, so their ratio is meaningful
        // from the very first sample (it is that sample's own rate).
        aimd.est_bytes = (((EWMA_SMOOTHING - 1) * aimd.est_bytes as u128 + bytes as u128)
            / EWMA_SMOOTHING) as u64;
        aimd.est_time_ns = (((EWMA_SMOOTHING - 1) * aimd.est_time_ns as u128
            + duration_ns as u128)
            / EWMA_SMOOTHING) as u64;

        let estimate = (aimd.est_bytes as u128 * 1_000_000_000
            / aimd.est_time_ns.max(1) as u128)
            .min(u64::MAX as u128) as u64;
        let (min, max) =
            (self.cfg.min_connection_bandwidth, self.cfg.max_connection_bandwidth);
        // `min(max).max(min)` instead of `clamp`, which panics on a
        // misconfigured min > max
        self.observed_connection_bandwidth
            .store(estimate.min(max).max(min), Ordering::Relaxed);
    }

    fn aimd(&self) -> MutexGuard<'_, AimdState> {
        self.aimd.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Point the pool at the effective bandwidth: the live target scaled
    /// by the AIMD factor.
    fn apply_effective_bandwidth(&self, aimd: &AimdState) {
        let target = self.target_bandwidth.load(Ordering::Relaxed);
        let effective = (target as u128 * aimd.factor_percent as u128 / 100) as u64;
        self.pool.set_capacity(effective.max(KNOB_FLOOR));
    }

    /// Roll the tumbling detection window forward to cover `now_ns`.
    ///
    /// There is no timer: a window "closes" retroactively at the first
    /// event after its end, so every event handler calls this before
    /// counting its event. Each elapsed window with zero throttles
    /// recovers `max_connection_bandwidth` of effective bandwidth toward
    /// the target (the additive increase). Idle windows count as clean:
    /// overload evidence is stale once traffic stopped.
    fn roll_window(&self, aimd: &mut AimdState, now_ns: u64) {
        // whole windows elapsed since the current one opened;
        // 0 = it is still open, nothing to close yet
        let windows = now_ns.saturating_sub(aimd.window_start_ns) / AIMD_WINDOW_NS;
        if windows == 0 {
            return;
        }
        // the counters describe the first elapsed window only — had any
        // later window seen an event, that event would have rolled first.
        // So the rest are empty, and empty means zero throttles: clean
        let clean = if aimd.window_throttles == 0 { windows } else { windows - 1 };
        if clean > 0 && aimd.factor_percent < 100 {
            // one recovery step per clean window, never past the target
            let target = self.target_bandwidth.load(Ordering::Relaxed);
            let effective = target as u128 * aimd.factor_percent as u128 / 100;
            let recovered = effective
                .saturating_add(self.cfg.max_connection_bandwidth as u128 * clean as u128)
                .min(target as u128);
            // back to whole percent, rounding up so repeated recovery
            // can't stall just short of the target
            aimd.factor_percent =
                (recovered * 100).div_ceil(target.max(1) as u128).min(100) as u64;
            self.apply_effective_bandwidth(aimd);
        }
        // start the (empty) window that contains `now_ns`; advancing by
        // whole windows keeps the boundaries on a fixed grid
        aimd.window_start_ns += windows * AIMD_WINDOW_NS;
        aimd.window_throttles = 0;
        aimd.window_completions = 0;
    }

    /// One throttle signal from `record_throttle` or a terminal `Throttled`
    /// outcome.
    ///
    /// Counts it, stamps the taint marker, and possibly cuts the
    /// effective bandwidth (the multiplicative decrease). The check runs
    /// on every signal, so a cut fires the moment the current window's
    /// count crosses the threshold provided the hold-off from the
    /// previous cut has passed.
    fn throttle_signal(&self, label: &str, now_ns: u64) {
        self.throttles_total.fetch_add(1, Ordering::Relaxed);
        // the taint marker: requests in flight at this instant skip the
        // bandwidth estimate when they complete
        self.last_throttle_ns.store(now_ns, Ordering::Relaxed);

        let mut aimd = self.aimd();
        // close any elapsed windows first; this signal counts in the
        // window that contains `now_ns`
        self.roll_window(&mut aimd, now_ns);
        aimd.window_throttles += 1;

        // an absolute floor of evidence when traffic is light, scaled up
        // under heavy traffic so a few stray signals among thousands of
        // successes don't cut
        let threshold = AIMD_CUT_MIN_THROTTLES
            .max(aimd.window_completions * AIMD_CUT_THROTTLE_PERCENT / 100);
        // no cut below the threshold (not enough evidence) or during the
        // hold-off (signals still caused by the pre-cut rate)
        if aimd.window_throttles < threshold || now_ns < aimd.holdoff_until_ns {
            return;
        }

        aimd.factor_percent = (aimd.factor_percent * AIMD_CUT_FACTOR_PERCENT / 100)
            .max(AIMD_FACTOR_FLOOR_PERCENT);
        self.apply_effective_bandwidth(&aimd);

        // hold further cuts off until pre-cut admissions drain; see the
        // `AIMD_HOLDOFF_*` docs for the estimate
        let holdoff_ns = aimd
            .p95_duration_ns()
            .saturating_add(AIMD_HOLDOFF_MARGIN_NS)
            .clamp(AIMD_HOLDOFF_MIN_NS, AIMD_HOLDOFF_MAX_NS);
        aimd.holdoff_until_ns = now_ns.saturating_add(holdoff_ns);
        // fresh window: throttles arriving during the hold-off were caused
        // by the old rate and must not accumulate against the new one
        aimd.window_start_ns = now_ns;
        aimd.window_throttles = 0;
        aimd.window_completions = 0;

        tracing::info!(
            governor = label,
            factor_percent = aimd.factor_percent,
            effective_bandwidth = self.pool.capacity(),
            holdoff_ms = holdoff_ns / 1_000_000,
            "bandwidth governor cut its effective bandwidth after throttles",
        );
    }

    /// Bookkeeping at request completion: count it in the detection window
    /// and remember its duration for the p95 drain estimate.
    fn on_complete(&self, now_ns: u64, duration_ns: u64) {
        let mut aimd = self.aimd();
        self.roll_window(&mut aimd, now_ns);
        aimd.window_completions += 1;
        if aimd.durations_ns.len() == AIMD_DURATION_SAMPLES {
            aimd.durations_ns.pop_front();
        }
        aimd.durations_ns.push_back(duration_ns);
    }

    /// The config that would rebuild this state with its current target.
    fn current_config(&self) -> DirectionConfig {
        DirectionConfig {
            target_bandwidth: self.target_bandwidth.load(Ordering::Relaxed),
            ..self.cfg.clone()
        }
    }

    fn metrics(&self) -> DirectionMetrics {
        DirectionMetrics {
            target_bandwidth: self.target_bandwidth.load(Ordering::Relaxed),
            effective_bandwidth: self.pool.capacity(),
            observed_connection_bandwidth: self
                .observed_connection_bandwidth
                .load(Ordering::Relaxed),
            in_flight_cost: self.pool.in_use(),
            in_flight_requests: self.in_flight.load(Ordering::Relaxed),
            queued_requests: self.pool.queued() as u64,
            throttles_total: self.throttles_total.load(Ordering::Relaxed),
        }
    }
}

impl BandwidthGovernor {
    pub fn new(config: &BandwidthGovernorConfig) -> Self {
        Self {
            inner: Arc::new(Inner {
                label: config.label.clone(),
                read: DirectionState::new(config.read.clone()),
                write: DirectionState::new(config.write.clone()),
                memory: WeightedPool::new(config.memory_budget.max(KNOB_FLOOR)),
                unknown_object_bytes: config.unknown_object_bytes,
                epoch: Instant::now(),
            }),
        }
    }

    /// Change the target bandwidth `B` for one direction. Growth admits
    /// waiting requests immediately; shrink never cancels in-flight ones,
    /// the pool drains passively to its new size. While an AIMD cut is
    /// active, its factor is relative to the new target.
    pub fn set_bandwidth(&self, direction: Direction, bytes_per_sec: u64) {
        let dir = self.inner.direction(direction);
        dir.target_bandwidth.store(bytes_per_sec.max(KNOB_FLOOR), Ordering::Relaxed);
        let aimd = dir.aimd();
        dir.apply_effective_bandwidth(&aimd);
    }

    /// Change the memory budget `M`, with the same grow/shrink semantics
    /// as [`set_bandwidth`](Self::set_bandwidth).
    pub fn set_memory_budget(&self, bytes: u64) {
        self.inner.memory.set_capacity(bytes.max(KNOB_FLOOR));
    }

    /// Intern-table identity and telemetry name.
    pub fn label(&self) -> &str {
        &self.inner.label
    }

    /// A point-in-time snapshot of the governor's state.
    pub fn metrics(&self) -> GovernorMetrics {
        GovernorMetrics {
            read: self.inner.read.metrics(),
            write: self.inner.write.metrics(),
            memory: MemoryMetrics {
                budget: self.inner.memory.capacity(),
                reserved: self.inner.memory.in_use(),
                queued_fetches: self.inner.memory.queued() as u64,
            },
        }
    }
}

#[async_trait]
impl IoGovernor for BandwidthGovernor {
    async fn reserve_memory(
        &self,
        class: IoClass,
        expected_total: Option<u64>,
    ) -> MemoryPermit {
        match class.direction {
            Direction::Write => MemoryPermit::noop(),
            Direction::Read => {
                // an object bigger than `M` reserves all of it (the pool
                // caps the debit at its capacity) and runs serialized
                let weight = expected_total.unwrap_or(self.inner.unknown_object_bytes);
                let guard = self.inner.memory.acquire(weight).await;
                MemoryPermit::new(Box::new(MemoryReservation {
                    guard: Mutex::new(guard),
                }))
            }
        }
    }

    async fn acquire(&self, class: IoClass, expected_bytes: Option<u64>) -> IoPermit {
        let dir = self.inner.direction(class.direction);
        let cost = dir.request_cost(expected_bytes);
        let guard = dir.pool.acquire(cost).await;
        dir.in_flight.fetch_add(1, Ordering::Relaxed);
        IoPermit::new(Box::new(RequestPermit {
            direction: class.direction,
            start_ns: self.inner.now_ns(),
            inner: Arc::clone(&self.inner),
            _guard: guard,
        }))
    }

    fn factory(&self) -> Arc<dyn GovernorFactory> {
        Arc::new(BandwidthGovernorConfig {
            label: self.inner.label.clone(),
            read: self.inner.read.current_config(),
            write: self.inner.write.current_config(),
            memory_budget: self.inner.memory.capacity(),
            unknown_object_bytes: self.inner.unknown_object_bytes,
        })
    }

    fn record_throttle(&self, class: IoClass) {
        let now_ns = self.inner.now_ns();
        self.inner.direction(class.direction).throttle_signal(&self.inner.label, now_ns);
    }

    fn as_arc_any(self: Arc<Self>) -> Arc<dyn Any + Send + Sync> {
        self
    }
}

/// Bandwidth cost of one HTTP request, held from admission to
/// completion/abort; successful completions feed the adaptive estimate.
#[derive(Debug)]
struct RequestPermit {
    inner: Arc<Inner>,
    direction: Direction,
    /// When the request was admitted, ns since the governor's epoch.
    start_ns: u64,
    /// Releases the cost back to the direction pool on drop.
    _guard: PoolGuard,
}

impl PermitState for RequestPermit {
    fn complete(self: Box<Self>, outcome: IoOutcome) {
        let dir = self.inner.direction(self.direction);
        dir.in_flight.fetch_sub(1, Ordering::Relaxed);

        let now_ns = self.inner.now_ns();
        let duration_ns = now_ns.saturating_sub(self.start_ns);
        dir.on_complete(now_ns, duration_ns);

        match outcome.result {
            // a terminal throttle — the library's retries exhausted — is
            // one more signal for the AIMD response
            IoResult::Throttled(_) => dir.throttle_signal(&self.inner.label, now_ns),
            IoResult::Ok => {
                // taint: a throttle since this request started means its
                // duration includes library backoff sleeps and carries no
                // bandwidth information (stamp 0 doubles as "never")
                let last_throttle = dir.last_throttle_ns.load(Ordering::Relaxed);
                if last_throttle == 0 || self.start_ns > last_throttle {
                    dir.observe_sample(outcome.bytes, Duration::from_nanos(duration_ns));
                }
            }
            _ => {}
        }
    }

    fn abort(self: Box<Self>) {
        self.inner.direction(self.direction).in_flight.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Memory debit for one logical fetch, released on drop.
#[derive(Debug)]
struct MemoryReservation {
    guard: Mutex<PoolGuard>,
}

impl MemoryState for MemoryReservation {
    fn adjust(&self, actual_total: u64) {
        self.guard.lock().unwrap_or_else(PoisonError::into_inner).adjust_to(actual_total);
    }
}

/// Snapshot of a [`BandwidthGovernor`]'s state, from
/// [`metrics`](BandwidthGovernor::metrics).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct GovernorMetrics {
    pub read: DirectionMetrics,
    pub write: DirectionMetrics,
    pub memory: MemoryMetrics,
}

/// One direction's bandwidth pool and estimate. Bandwidths in bytes/s.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct DirectionMetrics {
    /// The target `B` as last set.
    pub target_bandwidth: u64,
    /// What the pool currently admits: `target_bandwidth` scaled by the
    /// additive-increase/multiplicative-decrease (AIMD) throttle
    /// response's cut factor (1 while no cut is in effect).
    pub effective_bandwidth: u64,
    /// The adaptive estimate of one connection's bandwidth.
    pub observed_connection_bandwidth: u64,
    /// Total cost (bytes/s) of the admitted, not-yet-finished requests.
    pub in_flight_cost: u64,
    /// Number of admitted, not-yet-finished requests.
    pub in_flight_requests: u64,
    /// Requests waiting for admission.
    pub queued_requests: u64,
    /// Throttle signals observed since construction.
    pub throttles_total: u64,
}

/// The memory pool. All quantities in bytes except `queued_fetches`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct MemoryMetrics {
    /// The budget `M` as last set.
    pub budget: u64,
    /// Memory reserved by in-flight logical fetches.
    pub reserved: u64,
    /// Logical fetches waiting for a reservation.
    pub queued_fetches: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::Asset;
    use futures::{FutureExt as _, poll};
    use std::task::Poll;

    const READ_CHUNK: IoClass =
        IoClass { direction: Direction::Read, asset: Asset::Chunk };
    const WRITE_CHUNK: IoClass =
        IoClass { direction: Direction::Write, asset: Asset::Chunk };

    /// The design doc's S3 sample numbers: B = 50 Gbps, M = 4 GB.
    fn s3_config() -> BandwidthGovernorConfig {
        BandwidthGovernorConfig::s3_defaults(
            "test",
            6_250_000_000,
            6_250_000_000,
            4_000_000_000,
        )
    }

    fn s3_governor() -> BandwidthGovernor {
        BandwidthGovernor::new(&s3_config())
    }

    /// A 1 s latency and a wide-open connection estimate make a request's
    /// cost equal its expected byte size, so pool math reads directly.
    fn unit_cost_config() -> BandwidthGovernorConfig {
        let direction = DirectionConfig {
            target_bandwidth: 10,
            max_connection_bandwidth: 1_000,
            min_connection_bandwidth: 1_000,
            request_latency: Duration::from_secs(1),
            min_request_bytes: 0,
            unknown_request_bytes: 1,
        };
        BandwidthGovernorConfig {
            label: "unit".to_string(),
            read: direction.clone(),
            write: direction,
            memory_budget: 100,
            unknown_object_bytes: 8,
        }
    }

    #[icechunk_macros::test]
    fn test_request_cost_regimes() {
        let gov = s3_governor();
        let read = &gov.inner.read;

        // cold start: the estimate begins at the connection floor, and a
        // 12 MB part is bandwidth-bound by it
        assert_eq!(read.observed_connection_bandwidth.load(Ordering::Relaxed), 7_500_000);
        assert_eq!(read.request_cost(Some(12_000_000)), 7_500_000);

        read.observed_connection_bandwidth.store(90_000_000, Ordering::Relaxed);
        // floor: a 16 KB chunk prices as 30 KB -> 1 MB/s
        assert_eq!(read.request_cost(Some(16_384)), 1_000_000);
        // latency-bound: cost scales with size
        assert_eq!(read.request_cost(Some(1_000_000)), 33_333_333);
        // bandwidth-bound: a 12 MB part saturates one (warm) connection
        assert_eq!(read.request_cost(Some(12_000_000)), 90_000_000);
        // unknown sizes price as unknown_request_bytes (1 MiB)
        assert_eq!(read.request_cost(None), 34_952_533);
    }

    #[icechunk_macros::test]
    fn test_ewma_ignores_latency_dominated_samples() {
        let gov = s3_governor();
        let read = &gov.inner.read;

        // at or below the saturation threshold (90 MB/s x 30 ms = 2.7 MB)
        // the duration carries no bandwidth information
        read.observe_sample(2_700_000, Duration::from_secs(1));
        assert_eq!(read.observed_connection_bandwidth.load(Ordering::Relaxed), 7_500_000);

        // one saturating sample (90 MB in 1 s): the zero-seeded
        // accumulators make the first estimate the sample's own rate
        read.observe_sample(90_000_000, Duration::from_secs(1));
        assert_eq!(
            read.observed_connection_bandwidth.load(Ordering::Relaxed),
            90_000_000
        );

        // a second, slower sample (90 MB in 3 s) mixes into each
        // accumulator at 1/EWMA_SMOOTHING:
        //   est_bytes = (7·11_250_000 + 90_000_000)/8 = 21_093_750
        //   est_time  = (7·125_000_000 + 3_000_000_000)/8 = 484_375_000 ns
        //   estimate  = 21_093_750 × 1e9 / 484_375_000 = 43_548_387
        read.observe_sample(90_000_000, Duration::from_secs(3));
        assert_eq!(
            read.observed_connection_bandwidth.load(Ordering::Relaxed),
            43_548_387
        );
    }

    /// The estimate is the rate of the average slot (bytes and durations
    /// averaged separately), not the average of per-request rates.
    /// Alternating 4 MiB reads at 450 ms (9.3 MB/s) and 2.5 s (1.7 MB/s):
    /// a slot completes one request per E[T] = 1.475 s, so it delivers
    /// S/E[T] ≈ 2.8 MB/s — while the average of the rates is ≈ 5.5 MB/s.
    /// Overpricing a slot under-admits (measured as a flat 0.8× delivery
    /// against a store with heavy tail latency).
    #[icechunk_macros::test]
    fn test_estimate_is_slot_rate_not_average_of_rates() {
        let mut config = s3_config();
        config.read.min_connection_bandwidth = 1_000_000;
        let gov = BandwidthGovernor::new(&config);
        let read = &gov.inner.read;

        for _ in 0..50 {
            read.observe_sample(4_194_304, Duration::from_millis(450));
            read.observe_sample(4_194_304, Duration::from_millis(2_500));
        }
        let estimate = read.observed_connection_bandwidth.load(Ordering::Relaxed);
        // the EWMA cycles tightly around the slot rate (~2.7–3.0 MB/s
        // depending on which sample came last), far from the ~5.5 MB/s
        // that averaging rates would produce
        assert!(
            (2_500_000..3_500_000).contains(&estimate),
            "estimate {estimate} should be near the 2.8 MB/s slot rate"
        );
    }

    #[icechunk_macros::test]
    fn test_ewma_clamps_to_connection_bounds() {
        let gov = s3_governor();
        let read = &gov.inner.read;

        // an implausibly fast sample caps at max_connection_bandwidth
        read.observe_sample(u64::MAX / 2, Duration::from_nanos(1));
        assert_eq!(
            read.observed_connection_bandwidth.load(Ordering::Relaxed),
            90_000_000
        );

        // sustained slow (but saturating) samples floor at the minimum:
        // a degraded store must not race concurrency upward
        for _ in 0..200 {
            read.observe_sample(3_000_000, Duration::from_secs(10));
        }
        assert_eq!(read.observed_connection_bandwidth.load(Ordering::Relaxed), 7_500_000);
    }

    #[icechunk_macros::tokio_test]
    async fn test_only_successful_completions_feed_the_estimate() {
        let gov = s3_governor();

        let permit = gov.acquire(READ_CHUNK, Some(12_000_000)).await;
        permit.complete(IoOutcome { bytes: 1_000_000_000_000, result: IoResult::Error });
        assert_eq!(gov.metrics().read.observed_connection_bandwidth, 7_500_000);

        let permit = gov.acquire(READ_CHUNK, Some(12_000_000)).await;
        permit.complete(IoOutcome {
            bytes: 1_000_000_000_000,
            result: IoResult::Throttled(503),
        });
        assert_eq!(gov.metrics().read.observed_connection_bandwidth, 7_500_000);

        let permit = gov.acquire(READ_CHUNK, Some(12_000_000)).await;
        drop(permit); // abort
        assert_eq!(gov.metrics().read.observed_connection_bandwidth, 7_500_000);

        // a successful saturating transfer samples; the huge byte count
        // makes the sample clamp exactly at the cap whatever the duration
        let permit = gov.acquire(READ_CHUNK, Some(12_000_000)).await;
        permit.complete(IoOutcome { bytes: 1_000_000_000_000, result: IoResult::Ok });
        let m = gov.metrics().read;
        assert_eq!(m.observed_connection_bandwidth, 90_000_000);
        assert_eq!(m.in_flight_requests, 0);
        assert_eq!(m.in_flight_cost, 0);
    }

    /// The design doc's sample-numbers scenario, steady state: 69
    /// concurrent 12 MB reads (6250/90) or 312 writes (6250/20).
    #[icechunk_macros::tokio_test]
    async fn test_doc_sample_numbers_steady_state() {
        let gov = s3_governor();
        gov.inner.read.observed_connection_bandwidth.store(90_000_000, Ordering::Relaxed);
        gov.inner
            .write
            .observed_connection_bandwidth
            .store(20_000_000, Ordering::Relaxed);

        let mut held = Vec::new();
        for _ in 0..69 {
            held.push(gov.acquire(READ_CHUNK, Some(12_000_000)).await);
        }
        let mut next = gov.acquire(READ_CHUNK, Some(12_000_000)).boxed();
        assert!(poll!(next.as_mut()).is_pending());
        assert_eq!(gov.metrics().read.in_flight_requests, 69);
        assert_eq!(gov.metrics().read.queued_requests, 1);
        drop(next);
        held.clear();

        for _ in 0..312 {
            held.push(gov.acquire(WRITE_CHUNK, Some(12_000_000)).await);
        }
        let mut next = gov.acquire(WRITE_CHUNK, Some(12_000_000)).boxed();
        assert!(poll!(next.as_mut()).is_pending());
        assert_eq!(gov.metrics().write.in_flight_requests, 312);
        drop(next);
        held.clear();
        assert_eq!(gov.metrics().write.in_flight_cost, 0);
    }

    /// The design doc's cold start: every read prices at the 7.5 MB/s floor, so
    /// `B` alone would admit 833 twelve-MB fetches -- but their buffers
    /// would need 10 GB, and `M` is what actually gates the ramp-up.
    #[icechunk_macros::tokio_test]
    async fn test_doc_sample_numbers_cold_start_is_memory_gated() {
        let gov = s3_governor();

        let mut requests = Vec::new();
        for _ in 0..833 {
            requests.push(gov.acquire(READ_CHUNK, Some(12_000_000)).await);
        }
        let mut next = gov.acquire(READ_CHUNK, Some(12_000_000)).boxed();
        assert!(poll!(next.as_mut()).is_pending());
        drop(next);

        let mut reserves = Vec::new();
        for _ in 0..333 {
            reserves.push(gov.reserve_memory(READ_CHUNK, Some(12_000_000)).await);
        }
        let mut next = gov.reserve_memory(READ_CHUNK, Some(12_000_000)).boxed();
        assert!(poll!(next.as_mut()).is_pending());
        assert_eq!(gov.metrics().memory.reserved, 333 * 12_000_000);
        drop(next);
    }

    #[icechunk_macros::tokio_test]
    async fn test_unknown_sizes_use_defaults() {
        let gov = s3_governor();

        let reservation = gov.reserve_memory(READ_CHUNK, None).await;
        assert_eq!(gov.metrics().memory.reserved, default_unknown_object_bytes());
        drop(reservation);

        // warm the estimate so the unknown-size price isn't connection-capped
        gov.inner.read.observed_connection_bandwidth.store(90_000_000, Ordering::Relaxed);
        let permit = gov.acquire(READ_CHUNK, None).await;
        assert_eq!(gov.metrics().read.in_flight_cost, 34_952_533);
        permit.complete(IoOutcome { bytes: 0, result: IoResult::Ok });
        assert_eq!(gov.metrics().read.in_flight_cost, 0);
    }

    #[icechunk_macros::tokio_test]
    async fn test_writes_reserve_no_memory() {
        let gov = s3_governor();
        let _r = gov.reserve_memory(WRITE_CHUNK, Some(1_000_000_000_000)).await;
        let _r2 = gov.reserve_memory(WRITE_CHUNK, None).await;
        assert_eq!(gov.metrics().memory.reserved, 0);
    }

    /// An object bigger than `M` reserves all of it and runs serialized,
    /// which is what a memory budget means.
    #[icechunk_macros::tokio_test]
    async fn test_object_bigger_than_budget_runs_alone() {
        let gov = BandwidthGovernor::new(&unit_cost_config()); // M = 100
        let reservation = gov.reserve_memory(READ_CHUNK, Some(50_000)).await;
        assert_eq!(gov.metrics().memory.reserved, 100);

        let mut next = gov.reserve_memory(READ_CHUNK, Some(1)).boxed();
        assert!(poll!(next.as_mut()).is_pending());
        drop(reservation);
        assert!(poll!(next.as_mut()).is_ready());
    }

    #[icechunk_macros::tokio_test]
    async fn test_bandwidth_knob_applies_at_runtime() {
        let gov = BandwidthGovernor::new(&unit_cost_config()); // read B = 10
        let held = gov.acquire(READ_CHUNK, Some(10)).await;
        let mut queued = gov.acquire(READ_CHUNK, Some(4)).boxed();
        assert!(poll!(queued.as_mut()).is_pending());
        assert_eq!(gov.metrics().read.queued_requests, 1);

        // growing admits the waiter immediately
        gov.set_bandwidth(Direction::Read, 20);
        let Poll::Ready(queued) = poll!(queued.as_mut()) else {
            panic!("must be granted")
        };
        assert_eq!(gov.metrics().read.target_bandwidth, 20);
        assert_eq!(gov.metrics().read.effective_bandwidth, 20);
        assert_eq!(gov.metrics().read.in_flight_cost, 14);

        // shrinking below in-flight cancels nothing; the pool drains
        // passively to its new size
        gov.set_bandwidth(Direction::Read, 5);
        let mut blocked = gov.acquire(READ_CHUNK, Some(1)).boxed();
        assert!(poll!(blocked.as_mut()).is_pending());
        drop(held); // in-flight cost 4; 4 + 1 <= 5
        assert!(poll!(blocked.as_mut()).is_ready());
        drop(queued);

        // zero floors at 1 (serialize) rather than admitting for free
        gov.set_bandwidth(Direction::Read, 0);
        assert_eq!(gov.metrics().read.target_bandwidth, 1);
        assert_eq!(gov.metrics().read.effective_bandwidth, 1);
    }

    #[icechunk_macros::tokio_test]
    async fn test_memory_knob_applies_at_runtime() {
        let gov = BandwidthGovernor::new(&unit_cost_config()); // M = 100
        let held = gov.reserve_memory(READ_CHUNK, Some(100)).await;
        let mut queued = gov.reserve_memory(READ_CHUNK, Some(50)).boxed();
        assert!(poll!(queued.as_mut()).is_pending());
        assert_eq!(gov.metrics().memory.queued_fetches, 1);

        gov.set_memory_budget(150);
        let Poll::Ready(queued) = poll!(queued.as_mut()) else {
            panic!("must be granted")
        };
        assert_eq!(gov.metrics().memory.budget, 150);
        assert_eq!(gov.metrics().memory.reserved, 150);

        gov.set_memory_budget(0);
        assert_eq!(gov.metrics().memory.budget, 1, "floored, not free-for-all");

        drop(held);
        drop(queued);
        assert_eq!(gov.metrics().memory.reserved, 0);
    }

    /// `MemoryPermit::adjust` trues the reservation up or down once the
    /// actual object size becomes known.
    #[icechunk_macros::tokio_test]
    async fn test_memory_adjust() {
        let gov = BandwidthGovernor::new(&unit_cost_config()); // M = 100
        let r1 = gov.reserve_memory(READ_CHUNK, Some(60)).await;
        let mut r2 = gov.reserve_memory(READ_CHUNK, Some(50)).boxed();
        assert!(poll!(r2.as_mut()).is_pending());

        // down releases the difference and admits the waiter
        r1.adjust(20);
        let Poll::Ready(r2) = poll!(r2.as_mut()) else { panic!("must be granted") };
        assert_eq!(gov.metrics().memory.reserved, 70);

        // up debits immediately, past the budget; new fetches wait until
        // the overage is repaid
        r2.adjust(200);
        assert_eq!(gov.metrics().memory.reserved, 220);
        let mut r3 = gov.reserve_memory(READ_CHUNK, Some(1)).boxed();
        assert!(poll!(r3.as_mut()).is_pending());
        drop(r2);
        assert!(poll!(r3.as_mut()).is_ready());
        drop(r1);
    }

    #[icechunk_macros::test]
    fn test_record_throttle_counts_per_direction() {
        let gov = s3_governor();
        gov.record_throttle(READ_CHUNK);
        gov.record_throttle(READ_CHUNK);
        gov.record_throttle(WRITE_CHUNK);
        let m = gov.metrics();
        assert_eq!(m.read.throttles_total, 2);
        assert_eq!(m.write.throttles_total, 1);
    }

    #[icechunk_macros::test]
    fn test_factory_round_trips() {
        let config = BandwidthGovernorConfig::s3_defaults(
            "prod",
            6_250_000_000,
            1_000_000_000,
            4_000_000_000,
        );
        let factory: Box<dyn GovernorFactory> = Box::new(config.clone());

        let bytes = rmp_serde::to_vec(&factory).unwrap();
        let back: Box<dyn GovernorFactory> = rmp_serde::from_slice(&bytes).unwrap();
        let rebuilt = back.build().factory();
        assert_eq!(rmp_serde::to_vec(&rebuilt).unwrap(), bytes);

        let json = serde_json::to_string(&factory).unwrap();
        assert!(json.contains(r#""governor_type":"bandwidth""#));
        let back: Box<dyn GovernorFactory> = serde_json::from_str(&json).unwrap();
        assert_eq!(serde_json::to_string(&back).unwrap(), json);
    }

    /// The factory snapshots the *live* knob values, so a session shipped
    /// after a knob change replicates the new settings.
    #[icechunk_macros::test]
    fn test_factory_reflects_current_knobs() {
        let gov = s3_governor();
        gov.set_bandwidth(Direction::Read, 1_000);
        gov.set_memory_budget(2_000);

        let mut expected = s3_config();
        expected.read.target_bandwidth = 1_000;
        expected.memory_budget = 2_000;
        let expected: Box<dyn GovernorFactory> = Box::new(expected);
        assert_eq!(
            rmp_serde::to_vec(&gov.factory()).unwrap(),
            rmp_serde::to_vec(&expected).unwrap()
        );
    }

    /// The `unknown_*` fields are serde-defaulted so recipes written
    /// before them (or without them) keep deserializing.
    #[icechunk_macros::test]
    fn test_unknown_size_fields_default() {
        let direction = r#"{"target_bandwidth": 1, "max_connection_bandwidth": 2,
            "min_connection_bandwidth": 1,
            "request_latency": {"secs": 0, "nanos": 1000},
            "min_request_bytes": 5}"#;
        let json = format!(
            r#"{{"label": "x", "read": {direction}, "write": {direction},
                "memory_budget": 10}}"#
        );
        let config: BandwidthGovernorConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(config.read.unknown_request_bytes, default_unknown_request_bytes());
        assert_eq!(config.unknown_object_bytes, default_unknown_object_bytes());
    }

    #[icechunk_macros::test]
    fn test_random_labels_are_prefixed_and_distinct() {
        let label = BandwidthGovernorConfig::random_label();
        assert!(label.starts_with("governor-"));
        assert_ne!(label, BandwidthGovernorConfig::random_label());
    }

    /// Round numbers for the AIMD tests: with a target of 1000 the
    /// effective bandwidth is 10× the percent factor, and one recovery
    /// step (`max_connection_bandwidth` = 100) is exactly 10%.
    fn aimd_config() -> BandwidthGovernorConfig {
        let direction = DirectionConfig {
            target_bandwidth: 1_000,
            max_connection_bandwidth: 100,
            min_connection_bandwidth: 10,
            request_latency: Duration::from_secs(1),
            min_request_bytes: 0,
            unknown_request_bytes: 1,
        };
        BandwidthGovernorConfig {
            label: "aimd".to_string(),
            read: direction.clone(),
            write: direction,
            memory_budget: 1_000,
            unknown_object_bytes: 8,
        }
    }

    async fn complete_one_read(gov: &BandwidthGovernor) {
        gov.acquire(READ_CHUNK, Some(1))
            .await
            .complete(IoOutcome { bytes: 0, result: IoResult::Ok });
    }

    /// A cut needs `max(4, 1% of the window's completions)` signals: a
    /// few stray throttles are noise.
    #[icechunk_macros::tokio_test]
    async fn test_cut_threshold_minimum() {
        tokio::time::pause();
        let gov = BandwidthGovernor::new(&aimd_config());

        for _ in 0..3 {
            gov.record_throttle(READ_CHUNK);
        }
        assert_eq!(gov.metrics().read.effective_bandwidth, 1_000, "3 signals are noise");
        gov.record_throttle(READ_CHUNK);
        let m = gov.metrics();
        assert_eq!(m.read.effective_bandwidth, 700);
        assert_eq!(m.read.target_bandwidth, 1_000, "the assigned target is untouched");
        assert_eq!(m.write.effective_bandwidth, 1_000, "directions are independent");
    }

    #[icechunk_macros::tokio_test]
    async fn test_cut_threshold_scales_with_completions() {
        tokio::time::pause();
        let gov = BandwidthGovernor::new(&aimd_config());
        for _ in 0..600 {
            complete_one_read(&gov).await;
        }

        // 600 completions in the window: the threshold is max(4, 6) = 6
        for _ in 0..5 {
            gov.record_throttle(READ_CHUNK);
        }
        assert_eq!(gov.metrics().read.effective_bandwidth, 1_000);
        gov.record_throttle(READ_CHUNK);
        assert_eq!(gov.metrics().read.effective_bandwidth, 700);
    }

    /// A burst of signals produces exactly one cut; the next cut can only
    /// happen after the hold-off.
    #[icechunk_macros::tokio_test]
    async fn test_burst_cuts_exactly_once() {
        tokio::time::pause();
        let gov = BandwidthGovernor::new(&aimd_config());

        for _ in 0..50 {
            gov.record_throttle(READ_CHUNK);
        }
        assert_eq!(gov.metrics().read.effective_bandwidth, 700, "one cut per burst");

        // with no completed requests the hold-off is the 2 s floor; a
        // burst inside it must not cut again
        tokio::time::advance(Duration::from_millis(1_900)).await;
        for _ in 0..50 {
            gov.record_throttle(READ_CHUNK);
        }
        assert_eq!(gov.metrics().read.effective_bandwidth, 700);

        // past the hold-off, fresh signals cut again
        tokio::time::advance(Duration::from_millis(200)).await;
        for _ in 0..4 {
            gov.record_throttle(READ_CHUNK);
        }
        assert_eq!(gov.metrics().read.effective_bandwidth, 490);
    }

    /// Sustained overload cuts once per hold-off, not once per signal.
    #[icechunk_macros::tokio_test]
    async fn test_sustained_overload_cuts_once_per_holdoff() {
        tokio::time::pause();
        let gov = BandwidthGovernor::new(&aimd_config());

        // a throttle every 250 ms for 11 s; every window stays dirty, so
        // there is no additive recovery in between
        for _ in 0..44 {
            tokio::time::advance(Duration::from_millis(250)).await;
            gov.record_throttle(READ_CHUNK);
        }
        // cuts land at t = 1.75 s (the fourth signal of the second
        // window), then at the first signal past each 2 s hold-off that
        // gives the fresh window four: 4.5, 7.25 and 10.0 — four cuts,
        // 0.7^4 in whole-percent steps (70, 49, 34, 23)
        assert_eq!(gov.metrics().read.effective_bandwidth, 230);
        assert_eq!(gov.metrics().read.throttles_total, 44);
    }

    /// Windows with zero throttles recover additively, one
    /// `max_connection_bandwidth` per window, capped at the target. Idle
    /// windows count: overload evidence is stale after a quiet stretch.
    #[icechunk_macros::tokio_test]
    async fn test_additive_recovery_caps_at_target() {
        tokio::time::pause();
        let gov = BandwidthGovernor::new(&aimd_config());

        for _ in 0..4 {
            gov.record_throttle(READ_CHUNK);
        }
        assert_eq!(gov.metrics().read.effective_bandwidth, 700);

        // the window rolls lazily: the next event credits the clean time
        tokio::time::advance(Duration::from_secs(1)).await;
        complete_one_read(&gov).await;
        assert_eq!(gov.metrics().read.effective_bandwidth, 800);

        // five clean windows (one counted, four idle) overshoot the
        // remaining 200 and cap at the target
        tokio::time::advance(Duration::from_secs(5)).await;
        complete_one_read(&gov).await;
        assert_eq!(gov.metrics().read.effective_bandwidth, 1_000);

        tokio::time::advance(Duration::from_secs(1)).await;
        complete_one_read(&gov).await;
        assert_eq!(
            gov.metrics().read.effective_bandwidth,
            1_000,
            "never past the target"
        );
    }

    /// A bandwidth sample from a request whose flight overlapped a
    /// throttle signal is discarded: its duration includes library
    /// backoff sleeps and carries no bandwidth information.
    #[icechunk_macros::tokio_test]
    async fn test_taint_skips_overlapping_samples() {
        tokio::time::pause();
        let gov = s3_governor();

        let overlapped = gov.acquire(READ_CHUNK, Some(12_000_000)).await;
        tokio::time::advance(Duration::from_millis(1)).await;
        gov.record_throttle(READ_CHUNK); // a single signal: no cut, but taints
        tokio::time::advance(Duration::from_secs(1)).await;
        // successful and saturating, yet started before the throttle
        overlapped.complete(IoOutcome { bytes: 1_000_000_000_000, result: IoResult::Ok });
        assert_eq!(gov.metrics().read.observed_connection_bandwidth, 7_500_000);

        // a request started after the last throttle samples again (the
        // huge byte count clamps the sample exactly at the cap)
        let clean = gov.acquire(READ_CHUNK, Some(12_000_000)).await;
        tokio::time::advance(Duration::from_secs(1)).await;
        clean.complete(IoOutcome { bytes: 1_000_000_000_000, result: IoResult::Ok });
        assert_eq!(gov.metrics().read.observed_connection_bandwidth, 90_000_000);
    }

    /// A terminal `Throttled` outcome — the library's retries exhausted —
    /// counts as a signal and can trigger a cut by itself.
    #[icechunk_macros::tokio_test]
    async fn test_throttled_outcome_counts_as_signal() {
        tokio::time::pause();
        let gov = BandwidthGovernor::new(&aimd_config());

        for _ in 0..4 {
            let permit = gov.acquire(READ_CHUNK, Some(1)).await;
            permit.complete(IoOutcome { bytes: 0, result: IoResult::Throttled(503) });
        }
        let m = gov.metrics().read;
        assert_eq!(m.throttles_total, 4);
        assert_eq!(m.effective_bandwidth, 700);
    }

    /// The cut factor floors at 2% of the target no matter how sustained
    /// the overload.
    #[icechunk_macros::tokio_test]
    async fn test_cut_factor_floor() {
        tokio::time::pause();
        let gov = BandwidthGovernor::new(&aimd_config());

        // 30 s of sustained signals is enough for 11 cuts; the ninth
        // reaches the floor (70, 49, 34, 23, 16, 11, 7, 4, 2) and the
        // rest stay there
        for _ in 0..120 {
            tokio::time::advance(Duration::from_millis(250)).await;
            gov.record_throttle(READ_CHUNK);
        }
        assert_eq!(gov.metrics().read.effective_bandwidth, 20);
    }

    /// `set_bandwidth` during a cut keeps the factor: it is relative to
    /// the new target, and recovery aims at the new target too.
    #[icechunk_macros::tokio_test]
    async fn test_set_bandwidth_mid_cut() {
        tokio::time::pause();
        let gov = BandwidthGovernor::new(&aimd_config());

        for _ in 0..4 {
            gov.record_throttle(READ_CHUNK);
        }
        assert_eq!(gov.metrics().read.effective_bandwidth, 700);

        gov.set_bandwidth(Direction::Read, 2_000);
        let m = gov.metrics().read;
        assert_eq!(m.target_bandwidth, 2_000);
        assert_eq!(m.effective_bandwidth, 1_400);

        tokio::time::advance(Duration::from_secs(1)).await;
        complete_one_read(&gov).await;
        assert_eq!(gov.metrics().read.effective_bandwidth, 1_500);
    }

    /// The hold-off scales with observed request durations — p95 plus one
    /// library-retry backoff — so pre-cut admissions drain before the
    /// next cut is allowed.
    #[icechunk_macros::tokio_test]
    async fn test_holdoff_scales_with_request_durations() {
        tokio::time::pause();
        let gov = BandwidthGovernor::new(&aimd_config());

        // twenty 6-second requests: p95 = 6 s, hold-off = 7 s
        for _ in 0..20 {
            let permit = gov.acquire(READ_CHUNK, Some(1)).await;
            tokio::time::advance(Duration::from_secs(6)).await;
            permit.complete(IoOutcome { bytes: 0, result: IoResult::Ok });
        }

        for _ in 0..4 {
            gov.record_throttle(READ_CHUNK);
        }
        assert_eq!(gov.metrics().read.effective_bandwidth, 700);

        // signals every 250 ms keep the windows dirty and repeatedly
        // reach the 4-signal threshold, but none may cut before the 7 s
        // hold-off is over
        for _ in 0..30 {
            tokio::time::advance(Duration::from_millis(250)).await;
            gov.record_throttle(READ_CHUNK);
        }
        assert_eq!(gov.metrics().read.effective_bandwidth, 700);

        // the fourth signal of the fresh window past the hold-off cuts
        tokio::time::advance(Duration::from_millis(250)).await;
        gov.record_throttle(READ_CHUNK);
        assert_eq!(gov.metrics().read.effective_bandwidth, 490);
    }
}
