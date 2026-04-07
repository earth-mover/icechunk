# How To: Common Icechunk Operations

This page gathers common Icechunk operations into one compact how-to guide.
It is not intended as a deep explanation of how Icechunk works.

## Creating and Opening Repos

Creating and opening repos requires creating a `Storage` object.
See the [Storage guide](./storage.md) for all the details.

### Create a New Repo

```python
storage = icechunk.s3_storage(bucket="my-bucket", prefix="my-prefix", from_env=True)
repo = icechunk.Repository.create(storage)
```

### Open an Existing Repo

```python
repo = icechunk.Repository.open(storage)
```

### Specify Custom Config when Opening a Repo

There are many configuration options available to control the behavior of the repository and the storage backend.
See [Configuration](./configuration.md) for all the details.

```python
config = icechunk.RepositoryConfig.default()
config.caching = icechunk.CachingConfig(num_bytes_chunks=100_000_000)
repo = icechunk.Repository.open(storage, config=config)
```

### Deleting a Repo

Icechunk doesn't provide a way to delete a repo once it has been created.
If you need to delete a repo, just go to the underlying storage and remove the directory where you created the repo.

## Reading, Writing, and Modifying Data with Zarr

For a full walkthrough, see the [Quickstart](./quickstart.md).

Read and write operations occur within the context of a [transaction](./version-control.md).
The general pattern is

```python
session = repo.writable_session(branch="main")
# interact with the repo via session.store
# ...
session.commit(message="wrote some data")
```


!!! info

    In the examples below, we just show the interaction with the `store` object.
    Keep in mind that all sessions need to be concluded with a `.commit()`.

Alternatively, you can also use the `.transaction` function as a context manager,
which automatically commits when the context exits.

```python
with repo.transaction(branch="main", message="wrote some data") as store:
    # interact with the repo via store
```


### Create a Group

```python
group = zarr.create_group(session.store, path="my-group", zarr_format=3)
```

### Create an Array

```python
array = group.create("my_array", shape=(10, 20), dtype='int32')
```

### Write Data to an Array

```python
array[2:5, :10] = 1
```

### Read Data from an Array

```python
data = array[:5, :10]
```

### Resize an Array

```python
array.resize((20, 30))
```

### Add or Modify Array / Group Attributes

```python
array.attrs["standard_name"] = "time"
```

### View Array / Group Attributes

```python
dict(array.attrs)
```

### Delete a Group

```python
del group["subgroup"]
```

### Delete an Array

```python
del group["array"]
```

## Reading and Writing Data with Xarray

For more depth, see [Xarray](./xarray.md), [Parallel writes](./parallel.md), and [Dask](./dask.md).

### Write an in-memory Xarray Dataset

```python
ds.to_zarr(session.store, group="my-group", zarr_format=3, consolidated=False)
```


### Append to an existing datast

```python
ds.to_zarr(session.store, group="my-group", append_dim='time', consolidated=False)
```

### Write an Xarray dataset with Dask

Writing with Dask or any other parallel execution framework requires special care.
See [Parallel writes](./parallel.md) and [Xarray](./xarray.md) for more detail.

```python
from icechunk.xarray import to_icechunk
to_icechunk(ds, session)
```


### Read a dataset with Xarray

Reading can be done with a read-only session.

```python
session = repo.readonly_session("main")
ds = xr.open_zarr(session.store, group="my-group", zarr_format=3, consolidated=False)
```

## Transactions and Version Control

For more depth, see [Transactions and Version Control](./version-control.md).

### Create a Snapshot via a Transaction

```python
snapshot_id = session.commit("commit message")
```

### Resolve a Commit Conflict

The case of no actual conflicts:

```python
try:
    session.commit("commit message")
except icechunk.ConflictError:
    session.rebase(icechunk.ConflictDetector())
    session.commit("committed after rebasing")
```

Or if you have conflicts between different commits and want to overwrite the other changes:

```python
try:
    session.commit("commit message")
except icechunk.ConflictError:
    session.rebase(icechunk.BasicConflictSolver(on_chunk_conflict=icechunk.VersionSelection.UseOurs))
    session.commit("committed after rebasing")
```

### Commit with Automatic Rebasing

This will automatically retry the commit until it succeeds

```python
session.commit("commit message", rebase_with=icechunk.ConflictDetector())
```

### List Snapshots

```python
for snapshot in repo.ancestry(branch="main"):
    print(snapshot)
```

### Check out a Snapshot

```python
session = repo.readonly_session(snapshot_id=snapshot_id)
```

### Create a Branch

```python
repo.create_branch("dev", snapshot_id=snapshot_id)
```

### List all Branches

```python
branches = repo.list_branches()
```

### Check out a Branch

```python
session = repo.writable_session("dev")
```

### Reset a Branch to a Different Snapshot

```python
repo.reset_branch("dev", snapshot_id=snapshot_id)
```

### Create a Tag

```python
repo.create_tag("v1.0.0", snapshot_id=snapshot_id)
```

### List all Tags

```python
tags = repo.list_tags()
```

### Check out a Tag

```python
session = repo.readonly_session(tag="v1.0.0")
```

### Delete a Tag

```python
repo.delete_tag("v1.0.0")
```

### Diff Two Versions

```python
diff = repo.diff(from_tag="v1.0.0", to_branch="main")
```

## Moving Chunks and Nodes

For more depth, see [Moving Chunks](./moving-chunks.md) and [Moving and Renaming Nodes](./moving-nodes.md).

### Shift All Chunks by a Fixed Offset

Offsets are in chunks, not array elements. Out-of-bounds chunks are discarded; vacated positions reset to fill value.

```python
session.shift_array("/my_array", offset=(-2, 0))
```

### Reindex Chunks with a Custom Function

Provide a `forward` function mapping old chunk index to new. Return `None` to drop a chunk.
Add a `backward` function (the inverse of `forward`) to correctly clear stale positions when empty chunks exist.

```python
def fwd(idx):
    new = idx[0] - 2
    return [new] if new >= 0 else None

def bwd(idx):
    new = idx[0] + 2
    return [new] if new < n_chunks else None

session.reindex_array("/my_array", forward=fwd, backward=bwd)
```

### Move or Rename an Array or Group

Moving and renaming requires a **rearrange session**.

```python
session = repo.rearrange_session("main")
session.move("/old/path", "/new/path")
session.commit("Renamed old to new")
```

## Repo Maintenance

For more depth, see [Data Expiration](./expiration.md).

### Run Snapshot Expiration

```python
from datetime import datetime, timedelta
expiry_time = datetime.now() - timedelta(days=10)
expired = repo.expire_snapshots(older_than=expiry_time)
```

### Run Garbage Collection

```python
results = repo.garbage_collect(expiry_time)
```

### Usage in async contexts

Most methods in Icechunk have an async counterpart, named with an `_async` postfix. For more info, see [Async Usage](./async.md).

```python
results = await repo.garbage_collect_async(expiry_time)
```
