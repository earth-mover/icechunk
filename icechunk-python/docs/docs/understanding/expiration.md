# Expiring Data

Over time, an Icechunk Repository will accumulate many snapshots, not all of which need to be kept around.

"Expiration" allows you to mark snapshots as expired, and "garbage collection" deletes all data (manifests, chunks, snapshots, etc.) associated with expired snapshots.

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
