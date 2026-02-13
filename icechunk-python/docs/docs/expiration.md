# Expiring Data

Over time, an Icechunk Repository will accumulate many snapshots, not all of which need to be kept around.

"Expiration" allows you to mark snapshots as expired, and "garbage collection" deletes all data (manifests, chunks, snapshots, etc.) associated with expired snapshots.

First create a Repository, configured so that there are no "inline" chunks. This will help illustrate that data is actually deleted.


```python exec="on" session="version" source="material-block"
import icechunk

repo = icechunk.Repository.create(
    icechunk.in_memory_storage(),
    config=icechunk.RepositoryConfig(inline_chunk_threshold_bytes=0),
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
