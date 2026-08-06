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

## `IoGovernor`: an externally injectable controller

A trait Icechunk consults before issuing each HTTP request:

```rust
pub struct IoClass {
    pub direction: Direction, // Read | Write
    // Snapshot | Manifest | TxLog | Ref | Chunk | VirtualChunk | Other
    pub asset: Asset,
    // FIXME: I don't love that there are bad combinations, like
    // VirtualChunk/Write
}

pub struct IoOutcome {
    pub bytes: u64,
    pub duration: Duration,
    pub result: IoResult, // Ok | Throttled(status) | Error | Aborted
}

#[async_trait]
pub trait IoGovernor: Debug + Send + Sync {
    async fn acquire(&self, class: IoClass, expected_bytes: Option<u64>)
        -> IoPermit;
}

/// Implemented by each governor for its own bookkeeping type
trait PermitState: Debug + Send {
    /// The request finished; latency, bytes and throttle signals
    /// feed governor accounting
    fn complete(self: Box<Self>, outcome: IoOutcome);
    /// The request never finished, release resources
    fn abort(self: Box<Self>);
}

/// Capacity is held from `acquire` until the permit is dropped.
/// Exactly one of `complete` / `abort` reaches the governor.
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
```

Acquisition happens per HTTP request, *after* range-splitting: a large
object fetch acquires once per part, not once per logical fetch,
and the virtual-chunk fetchers consult the same governor.

The governor is injected at `Repository::open` time, and carried in
`storage::Settings` which already flows into every `Storage` method. Never
persisted. In the future we may extend if we need multiple sessions of the
same repo to have different settings.

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

A default governor reproduces today's behavior closely enough to be a
drop-in, we retire the existing semaphore. Soon we may want to offer a
better default governor.

## How a real-world governor can be implemented

As an example, a governor that holds one workload to a target network
bandwidth `B` without exceeding a memory budget `M`. The quantities in
play:

| Quantity                        | Kind             | Units   | Meaning                                                                   |
|---------------------------------|------------------|---------|---------------------------------------------------------------------------|
| `B`                             | assigned         | bytes/s | target bandwidth; runtime-adjustable                                      |
| `M`                             | assigned         | bytes   | memory budget; runtime-adjustable                                         |
| `max_connection_bandwidth`      | backend constant | bytes/s | the most one HTTP connection can sustain; one value per `Direction`       |
| `min_request_latency`           | backend constant | seconds | the fastest typical request latency (the median against a healthy store) |
| `min_request_bytes`             | backend constant | bytes   | requests are priced as if at least this big                               |
| `observed_connection_bandwidth` | runtime estimate | bytes/s | what one connection currently delivers; never exceeds the max            |
| `observed_request_latency`      | runtime estimate | seconds | current typical request latency; never below the min                      |

Backend constants ship as defaults per `Storage`; runtime estimates are
what adaptive pricing adjusts. The latency model: a latency-bound
request holds its connection for about `observed_request_latency` no
matter its size, so `expected_bytes / observed_request_latency` (bytes/s)
estimates the bandwidth it actually consumes. `min_request_bytes` keeps
that price from vanishing for tiny requests, capping how many can be in
flight.

A governor cannot control bandwidth directly: it only decides when
requests may start. So it holds two invariants over the set of in-flight
requests. Each is enforced by a pool: a weighted semaphore whose
capacity is the invariant's bound. A request acquires its cost from both
pools before starting and releases it at `complete`/`abort`:

- **Bandwidth pool** (capacity `B`, costs in bytes/s). A request costs
  the bandwidth it is expected to consume while active:

  ```text
  cost = min(observed_connection_bandwidth,
             max(expected_bytes, min_request_bytes) / observed_request_latency)
  ```

  Three regimes: below `min_request_bytes` of payload every request
  costs the same floor; from there cost scales with size
  (latency-bound); finally a large transfer saturates one connection
  and costs the estimated connection bandwidth (bandwidth-bound).
- **Bytes in flight** (capacity `M`, costs in bytes). A request costs
  `expected_bytes` against capacity `M`. The bandwidth invariant alone
  happily admits hundreds of buffered transfers; this one keeps the sum
  of their buffers under the workload's budget.

Mechanics:

1. Adaptive pricing: the `observed_*` estimates are running averages,
   updated at each `complete` and kept per direction. Per direction is
   important because bandwidths are so different.
   Every completion updates exactly one estimate, decided by the threshold
   `max_connection_bandwidth × min_request_latency` (about 2.7 MB for
   S3): payloads above it sample `bytes / duration` into
   `observed_connection_bandwidth`, payloads below it sample `duration`
   into `observed_request_latency`.
   Note `observed_connection_bandwidth` starts *low*: cold connections
   are genuinely slow, and the low starting estimate admits many of
   them; as they warm the average rises toward its bound and concurrency
   settles at `B / max_connection_bandwidth`.
2. The backend constants bound the estimates:
   `observed_connection_bandwidth` never exceeds
   `max_connection_bandwidth`, and `observed_request_latency` never
   drops below `min_request_latency`. Observations only ever make
   requests cheaper.
   Why one-way? Because an estimation faster than reality self-confirms:
   it drives fewer concurrent requests, which are each indeed now
   faster. A race to very low number of concurrent requests.
   A store slower than its bounds still converges to `B`; a store faster
   than them overshoots it, and nothing corrects that.
3. Each acquire is capped at each pool's capacity, and a pool must never
   shrink below the largest single acquire.
4. A scheduler could change `B` and `M` at runtime. Growth adds
   capacity; shrink reclaims idle capacity immediately and books the
   rest as a deficit that swallows returning weights. In-flight requests
   are never cancelled; the pool drains passively to its new size.
5. A `Throttled` outcome means the store is overloaded: cut effective
   `B` multiplicatively regardless of the assigned target, recover
   additively back toward it. This is the usual AIMD algo from
   congestion control.

A note on the adaptive pricing. It's tempting to take a more Bayesian
approach and try to estimate latency and bandwidth with each sample by
fitting a model. But this is far from trivial. For example:

- We need to make sure it corrects in a recoverable way.
- Streams of requests in the same size range are very common, and this
  will usually only inform one of the variables
- Long tails are very common in object stores, so errors are not easy
  to model.

But, we should still explore approaches such as this one in the future.
For now I think the trivial one-or-the-other approach is sufficient.

### Sample numbers for S3

| Quantity                               | Value                       |
|----------------------------------------|-----------------------------|
| `B` (target bandwidth)                 | 50 Gbps = 6 250 MB/s        |
| `M` (memory budget)                    | 4 GB                        |
| `max_connection_bandwidth`             | 90 MB/s read, 20 MB/s write |
| `min_request_latency`                  | 30 ms                       |
| `min_request_bytes`                    | 30 KB                       |
| `observed_connection_bandwidth`, start | 7.5 MB/s                    |
| multipart fetch ideal size             | 12 MB                       |

With these:

- Steady state: 69 concurrent reads (6 250 / 90) or 312 concurrent
  writes (6 250 / 20). Sixty-nine 12 MB read parts hold about 830 MB
  in flight; 312 write parts about 3.7 GB — both fit under `M`.
- Cold start: every request prices at the 7.5 MB/s starting estimate,
  so `B` alone would admit 833 concurrent reads — but 833 parts are
  10 GB, so `M` is what actually gates the ramp-up: the two pools
  working together.
- Small reads: a 16 KB chunk is priced as 30 KB → 30 KB / 30 ms =
  1 MB/s, so `B` caps them at 6 250 in flight while `M` barely notices
  them (100 MB).

Class still matters: reserve a small lane for `Manifest` reads so
metadata never queues behind bulk `Chunk` traffic.

## Sequence

- Introduce traits
- Write trivial implementation to recover current behavior
- Implement the real-world governor
- Introduce new settings for governor and allow an experimental configuration that uses it
