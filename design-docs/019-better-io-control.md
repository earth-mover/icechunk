# Better I/O Control

Scope: changes to Icechunk to more easily control its I/O behavior and
improve its I/O performance when it doesn't fully own the machine.

## Problems we are trying to solve with this

- The only repo-wide gate to I/O is `AssetManager::request_semaphore`
  (default 256), fixed at construction, counting *logical* fetches. Each
  logical fetch can fan out to `max_concurrent_requests_for_object` GETs.
- Tiny and huge network requests count the same in terms of limits.
  This is very limiting for performance.
- Virtual chunk reads (`VirtualChunkResolver` → `ChunkFetcher`) bypass the
  semaphore entirely.
- No ability to change limits at runtime: concurrency is fixed when the
  Repository is built, so an external scheduler cannot grow or shrink a
  running workload's I/O share. Even more, `reopen` doesn't allow to change.
- No priorities: manifest fetches are prioritized together with chunks,
  even if they arguably can block much more work.

## `IoGovernor`: an externally injectable I/O controller

A trait Icechunk consults before each logical fetch (to reserve buffer
memory) and again before each HTTP request (to admit it):

```rust
pub struct IoClass {
    pub direction: Direction, // Read | Write
    // Snapshot | Manifest | TxLog | Ref | Chunk | VirtualChunk | Other
    pub asset: Asset,
    // FIXME: I don't love that there are bad combinations, like
    // VirtualChunk/Write
}

pub struct IoOutcome {
    pub bytes: u64, // actual bytes transferred
    // `Throttled` is when the object store library retry loop gave up
    // and the error surfaced. Per-attempt throttles arrive through
    // `record_throttle` instead (see "Seeing throttles").
    pub result: IoResult, // Ok | Throttled(status) | Error | Aborted
}

#[async_trait]
pub trait IoGovernor: Debug + Send + Sync {
    /// One atomic reservation for a whole logical fetch
    /// Held until the assembled bytes are handed off to the consumer.
    /// Reads only: writes don't reserve memory.
    async fn reserve_memory(&self, class: IoClass, expected_total: Option<u64>)
        -> MemoryPermit;

    /// Admit one HTTP request, after range-splitting.
    async fn acquire(&self, class: IoClass, expected_bytes: Option<u64>)
        -> IoPermit;

    /// A recipe for an equivalent governor, reflecting the *current*
    /// assigned settings (label, `B`, `M`, backend constants). Runtime
    /// state is not part of the recipe.
    fn factory(&self) -> Arc<dyn GovernorFactory>;

    /// Side channel for per-attempt throttle signals observed below
    /// the library retry loops (see "Seeing throttles").
    fn record_throttle(&self, class: IoClass) {}
}

/// Implemented by each governor for its own bookkeeping type.
trait PermitState: Debug + Send {
    /// The request finished (last byte); the governor stamps the time,
    /// and bytes and throttle signals feed its accounting
    fn complete(self: Box<Self>, outcome: IoOutcome);
    /// The request never finished, release resources
    fn abort(self: Box<Self>);
}

/// Bandwidth capacity is held from `acquire` until the permit is
/// dropped. Exactly one of `complete` / `abort` reaches the governor.
pub struct IoPermit {
    // Starts `Some`; `complete` takes it. `None` tells `Drop` the outcome
    // was already reported
    inner: Option<Box<dyn PermitState>>,
}

impl IoPermit {
    pub fn complete(mut self, outcome: IoOutcome) {
        if let Some(s) = self.inner.take() { s.complete(outcome) }
    }
}

impl Drop for IoPermit {
    fn drop(&mut self) {
        if let Some(s) = self.inner.take() { s.abort() }
    }
}

/// Buffer budget for one logical fetch; released on drop, at hand-off.
pub struct MemoryPermit {
    inner: Box<dyn MemoryState>,
}

impl MemoryPermit {
    /// True-up when the actual size becomes known (e.g. from a
    /// content-length). Down releases the difference; up debits it
    /// immediately without waiting
    pub fn adjust(&mut self, actual_total: u64) { /* ... */ }
}
```

The two pools work at different granularities, matching where each
resource is actually consumed:

- **Bandwidth per HTTP request**, *after* range-splitting: a large
  object fetch acquires once per part, and the virtual-chunk fetchers
  consult the same governor.
- **Memory per logical fetch**, *before* splitting, for the total
  expected size.

Hand-off is when the logical fetch returns the assembled `Bytes`: after
that the memory belongs to the chunk cache or the user, not the
governor. On the streaming path the `MemoryPermit` rides inside the
returned reader and releases when the reader drops; the same wrapper
calls the bandwidth permit's `complete` at EOF, and a drop before EOF
is the abort path.

`expected_bytes` are real numbers: manifests store compressed lengths for
every chunk ref (native and virtual); manifest sizes come from
`ManifestFileInfo` in the snapshot; parts are bounded by
`ideal_concurrent_request_size`.

Not every cost is bandwidth: a tiny request moves almost no bytes but
still burns a connection, a round trip and request-rate quota. Governors
therefore use `min_request_bytes`, a per-backend constant: every request
is priced as if it moved at least that many bytes. This way we don't
authorize unbounded numbers of tiny requests. Unknown-size requests use
a default weight.

Chunk and manifest reads are
always exactly sized, but a ref is tiny while the snapshot it points to
can be tens of MB, and neither records its size anywhere we read first.
No single default covers both, so the default will sometimes
underestimate. Memory reservations start at the default and `adjust`
once the real size is known; an underestimated fetch runs a bounded
overage lasting one time-to-first-header, repaid by throttling new
admissions.

A default governor reproduces today's behavior closely enough to be a
drop-in, we retire the existing semaphore. Soon we may want to offer a
better default governor.

## Carrying the governor

This is a bit complex because we need two things:

- A governor instance that can be reused across different repos
- A way to "ship" the governor to another host during a distributed
  session, carrying the originator's current settings with it.

For the second we use a serializable factory. Every governor produces
one on demand (`IoGovernor::factory`), reflecting its settings at that
moment: shipping always snapshots the live configuration, so a
`set_bandwidth` on the originator reaches every worker that receives a
session serialized after the change.

```rust
/// Serializable recipe for building a governor. Rides
/// `Session::as_bytes` via typetag (the same mechanism `Storage`
/// uses); the concrete type must be linked into any binary that
/// deserializes such sessions — an unknown tag fails `from_bytes`
/// loudly.
#[typetag::serde(tag = "governor_type")]
pub trait GovernorFactory: Debug + Send + Sync {
    fn build(&self) -> Arc<dyn IoGovernor>;
}

// Serde of the governor reference inside `Session` is asymmetric:
//   serialize   → `governor.factory()`, the settings current at
//                 serialization time
//   deserialize → the factory, then get-or-build the instance through
//                 the process-wide intern table (below)
```

An instance and its factory are two views of one configuration:
`build` then `factory` round-trips, and `factory` then `build` yields
an equivalent governor.

### Threading: `StorageContext`

We introduce a new `StorageContext` type that will carry bot the
storage settings and the new governor (and maybe more in the future).

`Storage` methods will now take a context, which is an API breaking change.

```rust
pub struct StorageContext<'a> {
    pub settings: &'a Settings,
    pub governor: &'a Arc<dyn IoGovernor>,
}
```

Using this context `Storage` implementations can call `acquire` on every
I/O operation.

`AssetManager` and `VirtualChunkResolver` receive their
`Arc<dyn IoGovernor>` at construction (they issue `reserve_memory` at
the logical-fetch layer) and build the `StorageContext` per call.

### Injection

```rust
impl Repository {
    // None → CompatGovernorConfig::default().build()
    pub async fn open(/* ..., */ governor: Option<Arc<dyn IoGovernor>>) -> /* ... */;
    pub fn governor(&self) -> &Arc<dyn IoGovernor>;
}
```

`reopen` accepts a new governor too. Nothing about the governor is ever
written to `RepositoryConfig` or the persisted repo config.

Sharing one governor across many repositories on a host is explicit —
pass clones of one `Arc`; keeping the concrete type gives access to the
runtime knobs:

```rust
let cfg = BandwidthGovernorConfig { label: "host-A".into(), /* ... */ };
let gov = Arc::new(BandwidthGovernor::new(cfg));

let repo_a = Repository::open(/* ..., */ Some(gov.clone())).await?;
let repo_b = Repository::open(/* ..., */ Some(gov.clone())).await?;

gov.set_bandwidth(Direction::Read, 2_000_000_000); // both repos follow
```

### Distributed sessions

`Session` serializes wholesale (`as_bytes`/`from_bytes`), and a
governor instance is neither serializable nor meaningful on another
host. Crossing `from_bytes` therefore re-binds:

- `as_bytes` serializes `governor.factory()` — the settings current at
  serialization time, never the instance.
- `from_bytes` deserializes the factory and gets-or-builds the instance
  through a process-wide intern table keyed on the factory's serialized
  bytes (`Weak` entries, so idle processes free their governors). All
  sessions landing on a worker with equal factories share one instance,
  and therefore one per-host budget, with no worker-side setup.
- The intern table is consulted only at deserialization. Instances
  passed explicitly at `open` never touch it, so two governors with
  identical configs stay separate when separation was explicit.
- Components (`AssetManager`, `VirtualChunkResolver`) skip-serialize
  their governor references; `from_bytes` rebinds them in one place.
- Binding is loud: a rehydrated session logs which governor it bound,
  by label, and whether it was interned or newly built.

The built-in config's `label` field separates otherwise-equal configs
in the intern table  and names the governor in telemetry.

Assumed for v1: the session initiator and its workers want
equally-configured governors. Workers replicate the originator's
settings as of when each session was serialized, so a knob change on
the originator reaches workers through every session shipped after it.
Sessions shipped before and after a change carry different factory
bytes and intern to different instances on a worker: during the
transition a worker can run both budgets side by side, the old
instance draining away with its sessions. Heterogeneous fleets
are out of scope at this point.

### Built-in factories

```rust
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BandwidthGovernorConfig {
    /// Intern-table identity and telemetry name.
    pub label: String,
    pub read: DirectionConfig,
    pub write: DirectionConfig,
    /// `M`; reads only
    pub memory_budget: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DirectionConfig {
    pub target_bandwidth: u64,          // B
    pub max_connection_bandwidth: u64,
    pub min_connection_bandwidth: u64,  // floor & starting estimate
    pub request_latency: Duration,
    pub min_request_bytes: u64,
}

#[typetag::serde(name = "bandwidth")]
impl GovernorFactory for BandwidthGovernorConfig {
    fn build(&self) -> Arc<dyn IoGovernor> {
        Arc::new(BandwidthGovernor::new(self.clone()))
    }
}

/// Reproduces the legacy `request_semaphore`; the default when no
/// governor is passed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompatGovernorConfig {
    pub max_concurrent_requests: u16, // today's 256
}

#[typetag::serde(name = "compat")]
impl GovernorFactory for CompatGovernorConfig { /* ... */ }
```

Custom governors implement both traits and follow the same rules as
the built-ins: link the crate into every binary that deserializes
sessions, keep factory fields serde-evolvable (`#[serde(default)]`),
and keep `factory`/`build` round-tripping.

Python never implements the traits: it constructs the config classes
and receives a governor object exposing the knobs and metrics, which
it passes to `Repository.open`. Pickled sessions rehydrate through the
intern table like any other deserialization.

## Seeing throttles

Both storage stacks retry throttling responses internally with backoff
(`RetryConfig::standard()` in the AWS SDK, `RetryConfig` in
`object_store`), so a throttle usually never surfaces as an error.

In the initial versio we will observe attempts *below* the retry
loops, without changing retry behavior:

- **AWS SDK**: an interceptor implementing `read_after_attempt`, which
  runs once per attempt with that attempt's response — including
  attempts the SDK is about to retry. Statuses in the throttle set
  (503, plus the 429/408/499 already special-cased in `RETRY_CODES`)
  are reported to `record_throttle`. Attached per operation via
  `.customize().interceptor(...)`, where the per-call `StorageContext`
  governor is in scope.
- **`object_store`**: no interceptors, but the retry loop sits above
  the transport, and the `HttpConnector` hook lets us wrap the HTTP
  client. The wrapper sees every attempt's status and reports the
  same set.

Because the congestion response consumes aggregate counts per
direction per window, `record_throttle` is fire-and-forget: no
correlation with permits. The `Throttled` variant of `IoOutcome`
remains for the terminal case, when retries exhaust and the error
surfaces.

Consequences accepted with this design:

- Two responders remain: library backoff and governor both react.
  They push in the same direction, so the combination is safe, at
  worst over-conservative.
- A request whose attempts were throttled spends time in backoff
  sleeps; its duration must not feed the bandwidth estimate. The
  interceptor marks the operation tainted and `complete` skips the
  sample.
- The SDK's *adaptive* retry mode stays off: it is a client-side rate
  limiter — a second, invisible AIMD — and the governor owns that job.

## How a real-world governor can be implemented

As an example, a governor that holds one workload to a target network
bandwidth `B` without exceeding a memory budget `M`. The quantities in
play:

| Quantity                        | Kind             | Units   | Meaning                                                                   |
|---------------------------------|------------------|---------|---------------------------------------------------------------------------|
| `B`                             | assigned         | bytes/s | target bandwidth; runtime-adjustable                                      |
| `M`                             | assigned         | bytes   | memory budget; runtime-adjustable                                         |
| `max_connection_bandwidth`      | backend constant | bytes/s | the most one HTTP connection can sustain; one value per `Direction`       |
| `min_connection_bandwidth`      | backend constant | bytes/s | what a cold connection delivers; floor and starting value of the estimate |
| `request_latency`               | backend constant | seconds | assumed per-request latency (healthy-store median or lower)               |
| `min_request_bytes`             | backend constant | bytes   | requests are priced as if at least this big                               |
| `observed_connection_bandwidth` | runtime estimate | bytes/s | what one connection currently delivers; clamped between min and max      |

Backend constants ship as defaults per `Storage`; the single runtime
estimate is what adaptive pricing adjusts. The latency model: a
latency-bound request holds its connection for about `request_latency`
no matter its size, so `expected_bytes / request_latency` (bytes/s)
estimates the bandwidth it actually consumes. `min_request_bytes` keeps
that price from vanishing for tiny requests, capping how many can be in
flight.

A governor cannot control bandwidth directly: it only decides when work
may start. So it holds two invariants over the in-flight work. Each is
enforced by a pool: a weighted semaphore whose capacity is the
invariant's bound:

- **Bandwidth pool** (capacity `B`, costs in bytes/s). Acquired per
  HTTP request before it starts, released at `complete`/`abort`. A
  request costs the bandwidth it is expected to consume while active:

  ```text
  cost = min(observed_connection_bandwidth,
             max(expected_bytes, min_request_bytes) / request_latency)
  ```

  Three regimes: below `min_request_bytes` of payload every request
  costs the same floor; from there cost scales with size
  (latency-bound); finally a large transfer saturates one connection
  and costs the estimated connection bandwidth (bandwidth-bound).
- **Memory pool** (capacity `M`, costs in bytes). Reserved per logical
  fetch for the total expected size and released
  at hand-off. The bandwidth invariant alone happily admits hundreds of
  buffered transfers; this one keeps the sum of their buffers under the
  workload's budget.

Mechanics:

1. Adaptive pricing: `observed_connection_bandwidth` is a running
   average, kept per direction (bandwidths are very different) and
   updated at `complete`, but only by payloads that can saturate a
   connection: those above `max_connection_bandwidth × request_latency`
   (about 2.7 MB for S3) sample `bytes / duration`. Smaller payloads
   update nothing: their duration is latency-dominated and carries no
   information about connection bandwidth.
   Note the estimate starts *low*, at `min_connection_bandwidth`: cold
   connections are genuinely slow, and the low starting estimate admits
   many of them; as they warm the average rises toward its cap and
   concurrency settles at `B / max_connection_bandwidth`.
2. The estimate is clamped to
   [`min_connection_bandwidth`, `max_connection_bandwidth`], because an
   estimation error in either direction self-reinforces. Faster than
   reality: fewer concurrent requests, which are each indeed now
   faster, a race to a very low number of concurrent requests. Slower
   than reality: a degraded store produces slow samples, slow samples
   make requests cheaper, cheaper requests raise concurrency against a
   store that is already sick. The floor bounds that loop at
   `B / min_connection_bandwidth` requests in flight; retreat from the
   bound is the `Throttled` response's job.
3. Each acquire is capped at each pool's capacity, and a pool must never
   shrink below the largest single acquire. An object bigger than `M`
   reserves all of `M`: fetches of such objects run serialized, which
   is what a memory budget means.
4. A scheduler could change `B` and `M` at runtime. Growth adds
   capacity; shrink reclaims idle capacity immediately and books the
   rest as a deficit that swallows returning weights. In-flight requests
   are never cancelled; the pool drains passively to its new size.
5. Throttle signals mean the store is overloaded; the response is AIMD
   with TCP's one-decrease-per-round rule, adapted to request
   lifetimes. Signals are counted per direction over a fixed detection
   window (~1 s); a window whose throttle rate exceeds a small
   threshold, regardless of the assigned target. After a cut, further cuts are
   held off for the drain time of pre-cut admissions (a few seconds:
   about p95 request duration plus one library-retry backoff) — the
   throttles arriving in that period were caused by the old rate and
   say nothing about the new one. Sustained overload keeps cutting,
   once per hold-off, on fresh signal. Windows with zero throttles
   recover additively toward the assigned `B`.

A note on the adaptive pricing. It's tempting to also estimate latency,
or to take a more Bayesian approach and fit a joint latency/bandwidth
model from every sample. But this is far from trivial. For example:

- We need to make sure it corrects in a recoverable way.
- Streams of requests in the same size range are very common, and they
  only inform one of the variables
- Large requests don't carry *real* information about latency, and small
  requests don't carry information about bandwidth. You need empty pipes
  to estimate latency and full pipes to estimate bandwidth.
- Long tails are very common in object stores, so errors are not easy
  to model.

### Sample numbers for S3

| Quantity                               | Value                       |
|----------------------------------------|-----------------------------|
| `B` (target bandwidth)                 | 50 Gbps = 6 250 MB/s        |
| `M` (memory budget)                    | 4 GB                        |
| `max_connection_bandwidth`             | 90 MB/s read, 20 MB/s write |
| `min_connection_bandwidth`             | 7.5 MB/s                    |
| `request_latency`                      | 30 ms                       |
| `min_request_bytes`                    | 30 KB                       |
| multipart fetch ideal size             | 12 MB                       |

With these:

- Steady state: 69 concurrent reads (6 250 / 90) or 312 concurrent
  writes (6 250 / 20). Sixty-nine concurrent 12 MB single-part fetches
  reserve about 830 MB of `M`; larger objects reserve their full size
  while their parts stream in. Writes don't touch `M`.
- Cold start: every request prices at the 7.5 MB/s floor, so `B` alone
  would admit 833 concurrent reads — but 833 twelve-MB fetches reserve
  10 GB, so `M` is what actually gates the ramp-up: the two pools
  working together.
- Small reads: a 16 KB chunk is priced as 30 KB → 30 KB / 30 ms =
  1 MB/s, so `B` caps them at 6 250 in flight while `M` barely notices
  them (100 MB).

Class still matters: reserve a small lane for `Manifest` reads so
metadata never queues behind bulk `Chunk` traffic. We may punt on
this for the initial version.
