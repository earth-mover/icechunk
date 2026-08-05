# Better I/O Control

Scope: changes to Icechunk to more easily control its I/O behavior and
improve its I/O performance when it doesn't fully own the machine.

## Problems with the current architecture

We are thinking about server-side use cases for Icechunk. Notice these are
not the only problems, these are only the problems we are attacking here.

- The only repo-wide gate to I/O is `AssetManager::request_semaphore`
  (default 256), fixed at construction, counting *logical* fetches. Each
  logical fetch can fan out to `max_concurrent_requests_for_object` GETs.
- Virtual chunk reads (`VirtualChunkResolver` → `ChunkFetcher`) bypass the
  semaphore entirely.
- No ability to change limits at runtime: concurrency is fixed when the
  Repository is built, so an external scheduler cannot grow or shrink a
  running workload's I/O share. Even more, `reopen` doesn't allow to change.
- No priorities: manifest fetches are prioritized together with chunks, even
  if they arguably can block much more work.

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

Governors should price a request as
`max(expected_bytes, min_request_weight)` so the small-chunk regime can't
authorize thousands of tiny requests under a byte budget; unknown-size
requests use a default weight.

A default governor reproduces today's behavior closely enough to be a
drop-in, we retire the existing semaphore. Soon we may want
to offer a better default governor.

## How a real governor could be implemented

As an example, a governor that holds one workload to a target network
bandwidth `B`.

A governor cannot meter bandwidth directly: it only decides when requests
may start. The usable lever is **bytes in flight**. By Little's law, a
pipe sustaining `B` bytes/s at mean request latency `L` holds `B × L`
bytes in flight, so the governor keeps a byte-weighted semaphore whose
capacity tracks `C = B × L`:

1. **Admission.** `acquire` blocks until the request's weight
   (`max(expected_bytes, min_request_weight)`) fits under `C`, then
   grants.
2. **Measurement.** Each `complete` reports `(bytes, duration)`. Keep a
   smoothed mean latency `L` and the achieved throughput.
3. **Control loop.** Periodically set `C = B × L`, clamped between a
   liveness floor and a memory-driven max.
4. **Shrinking without revoking.** When `C` drops, reclaim idle capacity
   immediately and book the rest as a deficit. In-flight requests
   are never cancelled, the pool drains passively to its new size.
5. **Safety beats the target.** A `Throttled` outcome means the store is
   overloaded: cut `C` multiplicatively regardless of `B`, then recover
   additively back toward `B × L`.

Worked numbers: `B` = 1 GB/s at mean part latency 100 ms → `C` = 100 MB,
about eight concurrent 12 MB parts. With 16 KB chunks the same target
would need thousands of concurrent requests; a `min_request_weight` of
256 KB makes the same `C` admit at most ~400 of them.

Class still matters: reserve a small lane of `C` for `Manifest` reads so
metadata never queues behind bulk `Chunk` traffic.
