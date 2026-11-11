# Expiring Data

Over time, an Icechunk Repository will accumulate many snapshots, not all of which need to be kept around.

"Expiration" allows you to mark snapshots as expired, and "garbage collection" deletes all data (manifests, chunks, snapshots, etc.) associated with expired snapshots.

!!! tip "Further reading"
    For motivation, worked scenarios and operational recommendations, see the blog post
    [Everything you need to know about Icechunk garbage collection](https://www.earthmover.io/blog/everything-you-need-to-know-about-icechunk-garbage-collection).

!!! warning "Read [Running expiration and garbage collection safely](#running-expiration-and-garbage-collection-safely) before using these operations on a repository that is being written to or read from."

First create a Repository, configured so that there are no "inline" chunks. This will help illustrate that data is actually deleted.


```python exec="on" session="version" source="material-block"
import icechunk as ic

repo = ic.Repository.create(
    ic.in_memory_storage(),
    config=ic.config.RepositoryConfig(inline_chunk_threshold_bytes=0),
)
```

## Generate a few snapshots

Let us generate a sequence of snapshots


```python exec="on" session="version" source="material-block"
import zarr
import time

for i in range(10):
    session = repo.writable_session("main")
    array = zarr.create_array(
        session.store, name="array", shape=(10,), fill_value=-1, dtype=int, overwrite=True
    )
    array[:] = i
    session.commit(f"snap {i}")
    time.sleep(0.1)
```

There are 10 snapshots


```python exec="on" session="version" source="material-block"
ancestry = list(repo.ancestry(branch="main"))
print("\n\n".join([str((a.id, a.written_at)) for a in ancestry]))
```


## Expire snapshots

!!! danger
    Expiring snapshots is an irreversible operation. Use it with care.

First we must expire snapshots. Here we will expire any snapshot older than the 5th one.


```python exec="on" session="version" source="material-block"
expiry_time = ancestry[5].written_at
print(expiry_time)
```


```python exec="on" session="version" source="material-block"
expired = repo.expire_snapshots(older_than=expiry_time)
print(expired)
```

This prints out the set of snapshots that were expired.

!!! note
    The first snapshot is never expired!

!!! tip "Keeping an old version forever"
    Expiration only affects snapshots that are not reachable from a tag or branch.
    If you care about a specific old version, [create a tag](version-control.md#tags) pointing to it
    and it will survive expiration regardless of its age.

Expiration does not delete any array data by itself. It rewrites the version history so that it skips
the expired snapshots. Everything visible in the snapshots that are kept is unchanged, element by element.
What expiration *does* do is turn chunks that were only reachable from the expired snapshots into garbage,
which the next step reclaims.

!!! note "The cutoff is exclusive"
    `older_than` is an exclusive bound: a snapshot is expired only if its
    `written_at` is strictly earlier than the cutoff. A snapshot whose
    `written_at` equals the cutoff is kept. The same holds for
    `garbage_collect`: an object is deleted only if it was created strictly
    before the cutoff *and* no surviving snapshot references it. This means you
    can pass a snapshot's own `written_at` as the cutoff to expire everything
    older than it while keeping that snapshot itself.


Confirm that these are the right snapshots (remember that ancestry list commits in decreasing order of `written_at` time):

```python exec="on" session="version" source="material-block"
print([a.id for a in ancestry[-5:-1]])
```

Note that ancestry is now shorter:

```python exec="on" session="version" source="material-block"
new_ancestry = list(repo.ancestry(branch="main"))
print("\n\n".join([str((a.id, a.written_at)) for a in new_ancestry]))
```

## Delete expired data

!!! danger
    Garbage collection is an irreversible operation that deletes data. Use it with care.

Use `Repository.garbage_collect` to delete data associated with expired snapshots


```python exec="on" session="version" source="material-block"
results = repo.garbage_collect(expiry_time)
print(results)
```

### Reading the summary

The returned [`GCSummary`](../reference/ops.md#icechunk.ops.GCSummary) counts
what was deleted, but also what wasn't:

- `objects_failed_to_delete` counts objects whose delete request failed. They
  stay garbage and the next run will try again; isolated failures do not stop
  the run.
- `delete_errors` holds the first few distinct error messages behind those
  failures.
- `skipped_phases` lists the phases that were not run at all. Garbage
  collection deletes one kind of object per phase — snapshots, then transaction
  logs, manifests and chunks — each kind only after the kind that references it.
  If a phase has failures, the phases that depend on it are skipped, so a
  surviving object is never left pointing at something that was deleted. Those
  phases are simply picked up by the next run.
- `throttled_batches` counts delete requests the store asked us to slow down.
  Each was retried after a pause; none of them is a failure.

A run only gives up with an error — a `StorageError` with
`kind == ErrorKind.GC_DELETES_FAILING` — when deletes fail persistently, by
default 50 in a row.

### Tuning

The defaults are meant to work unchanged, including on large repositories.
The knobs worth knowing about, all keyword arguments of
[`garbage_collect`](../reference/index.md#icechunk.Repository.garbage_collect):

- **Memory.** The manifest walk holds decoded manifests in memory, bounded by
  `max_decoded_manifest_mem_bytes` (4 GiB by default). Manifests grow many times
  over when decoded, so this, rather than the compressed budget
  (`max_compressed_manifest_mem_bytes`), is usually what bounds the memory a run
  uses for manifests. It does not cover the set of chunk ids the walk
  accumulates, which dominates on repositories with many millions of chunks.
- **Delete rate.** Garbage collection does not delete at a fixed rate: it starts
  with a single request in flight and ramps up until the store throttles it or
  it reaches `max_concurrent_deletes`, then keeps adjusting. Raising that ceiling
  only helps on prefixes the store has already partitioned.
- **Listing.** `max_concurrent_listings` sets how many object listings run
  concurrently while looking for garbage. It defaults to eight per available
  core, clamped to between 32 and 256.
- **Giving up.** `max_consecutive_delete_failures` (50) is how many delete
  requests must fail in a row before the run aborts. Throttling only counts
  towards it once the store keeps throttling after the back-off between retries
  has grown to its maximum.

See the [API reference](../reference/index.md#icechunk.Repository.garbage_collect)
for the full list and the exact defaults.

## Running expiration and garbage collection safely

Icechunk is serverless: there is no coordinator that can pause readers and writers while expiration or
garbage collection runs, and no locks. Any number of processes may be reading or writing the repository
at the same time, so the safety of these operations rests on the thresholds you choose.

!!! danger "Never run these operations with recent timestamps"
    Both operations are irreversible, and thresholds that are too recent can destroy data.
    Always choose thresholds much larger than your longest possible session, with a generous safety margin.

- **Only one expiration or garbage collection operation may run at a time.**
- **`garbage_collect(delete_object_older_than=...)`**: the cutoff must be earlier than the start time of
  the *oldest write session that is still running or could still commit*. Chunks written by an in-progress
  session are not yet reachable from any snapshot, so garbage collection cannot tell them apart from
  abandoned data. If the cutoff is more recent than the start of such a session, its chunks can be deleted
  before it commits. **It is not safe to run garbage collection with a `delete_object_older_than` that is
  shorter than your longest writing session.**
- **`expire_snapshots(older_than=...)`**: the cutoff must be earlier than any version that is concurrently
  being read, listed, or used as the parent of a commit. Otherwise another session may be using a snapshot
  that is about to disappear.

### Recommendations

There is rarely a good reason to run these operations frequently or with recent cutoffs: the storage savings
are marginal.

- Run expiration every month or two, not every few hours, with an `older_than` such that you truly no longer
  care about the versions it removes.
- Run garbage collection every couple of weeks to a month, with a `delete_object_older_than` far larger than
  your longest runaway session.
- Use `dry_run=True` first to see what would be deleted.
- Tag any version you want to keep indefinitely.

### When do you need garbage collection?

Garbage collection only reclaims storage. It has **no effect on latency or scalability**, so there is no
need to run it on small repositories where the unreferenced data costs next to nothing.

Garbage collection deletes objects that are not reachable from any branch or tag. These accumulate from:

- sessions that wrote chunks but never committed (for example, a job that crashed),
- deleted branches or tags, once no remaining reference reaches those commits, and
- expired snapshots, as described above.

Garbage collection is not part of a transaction: it cannot be undone by resetting a branch, its effects are
not observed atomically, and it edits the repository without creating a new version.

### When do you need expiration?

Typical situations where old versions cost real storage:

- **A bug in array data**: you rewrote a large array with corrected values, but the chunks of the buggy
  version are still stored so that you can time travel to it.
- **Deleted arrays**: deleting an array creates a new snapshot without it, but does not free any space.
  Its chunks must be kept for the older snapshots that still contain the array, until those are expired.
- **Chunk-unaligned writes**: for example, daily appends to an array chunked by month rewrite the whole
  monthly chunk each day, leaving roughly 30 near-identical copies of every chunk.

With regular expiration and garbage collection the overhead over your live data stays roughly constant,
set by how much history you choose to retain.
