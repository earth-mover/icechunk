# Parallel Writes

A common pattern with large distributed write jobs is to first initialize the dataset on a disk
with all appropriate metadata, and any coordinate variables. Following this a large write job
is kicked off in a distributed setting, where each worker is responsible for an independent
"region" of the output.

## Why is Icechunk different from any other Zarr store?

The reason is that unlike Zarr, Icechunk is a "stateful" store. The Session object keeps a record of all writes, that is then
bundled together in a commit. Thus `Session.commit` must be executed on a Session object that knows about all writes,
including those executed remotely in a multi-processing or any other remote execution context.

## Example

Here is how you can execute such writes with Icechunk, illustrate with a `ThreadPoolExecutor`.
First read some example data, and create an Icechunk Repository.

```python exec="on" session="parallel" source="material-block"
import xarray as xr
import tempfile
from icechunk import Repository, local_filesystem_storage

ds = xr.tutorial.open_dataset("rasm").isel(time=slice(24))
repo = Repository.create(local_filesystem_storage(tempfile.mkdtemp()))
session = repo.writable_session("main")
```

We will orchestrate so that each task writes one timestep.
This is an arbitrary choice but determines what we set for the Zarr chunk size.

```python exec="on" session="parallel" source="material-block"
chunks = {1 if dim == "time" else ds.sizes[dim] for dim in ds.Tair.dims}
```

Initialize the dataset using [`Dataset.to_zarr`](https://docs.xarray.dev/en/stable/generated/xarray.Dataset.to_zarr.html)
and `compute=False`, this will NOT write any chunked array data, but will write all array metadata, and any
in-memory arrays (only `time` in this case).

```python exec="on" session="parallel" source="material-block"
ds.to_zarr(session.store, compute=False, encoding={"Tair": {"chunks": chunks}}, mode="w")
# this commit is optional, but may be useful in your workflow
print(session.commit("initialize store"))
```

## Multi-threading

First define a function that constitutes one "write task".

```python exec="on" session="parallel" source="material-block"
from icechunk import Session

def write_timestamp(*, itime: int, session: Session) -> None:
    # pass a list to isel to preserve the time dimension
    ds = xr.tutorial.open_dataset("rasm").isel(time=[itime])
    # region="auto" tells Xarray to infer which "region" of the output arrays to write to.
    ds.to_zarr(session.store, region="auto", consolidated=False)
```

Now execute the writes.

<!-- ```python exec="on" session="parallel" source="material-block" -->
```python
from concurrent.futures import ThreadPoolExecutor, wait
from icechunk.distributed import merge_sessions

session = repo.writable_session("main")
with ThreadPoolExecutor() as executor:
    # submit the writes
    futures = [executor.submit(write_timestamp, itime=i, session=session) for i in range(ds.sizes["time"])]
    wait(futures)

print(session.commit("finished writes"))
```

Verify that the writes worked as expected:

<!-- ```python exec="on" session="parallel" source="material-block" -->
```python
ondisk = xr.open_zarr(repo.readonly_session("main").store, consolidated=False)
xr.testing.assert_identical(ds, ondisk)
```

## Distributed writes

!!! info

    This code will not execute with a `ProcessPoolExecutor` without [some changes](https://docs.python.org/3/library/multiprocessing.html#programming-guidelines).
    Specifically it requires wrapping the code in a `if __name__ == "__main__":` block.
    See a full executable example [here](https://github.com/earth-mover/icechunk/blob/main/icechunk-python/examples/mpwrite.py).

Any task execution framework (e.g. `ProcessPoolExecutor`, Joblib, Lithops, Dask Distributed, Ray, etc.)
can be used instead of the `ThreadPoolExecutor`. However such workloads should account for
Icehunk being a "stateful" store that records changes executed in a write session.

There are three key points to keep in mind:

1. The `write_task` function *must* return the `Session`. It contains a record of the changes executed by this task.
   These changes *must* be manually communicated back to the coordinating process, since each of the distributed processes
   are working with their own independent `Session` instance.
2. Icechunk requires that users opt-in to pickling a *writable* `Session` using the `Session.allow_pickling()` context manager,
   to remind the user that distributed writes with Icechunk require care.
3. The user *must* manually merge the Session objects to create a meaningful commit.

First we modify `write_task` to return the `Session`:

```python
from icechunk import Session

def write_timestamp(*, itime: int, session: Session) -> Session:
    # pass a list to isel to preserve the time dimension
    ds = xr.tutorial.open_dataset("rasm").isel(time=[itime])
    # region="auto" tells Xarray to infer which "region" of the output arrays to write to.
    ds.to_zarr(session.store, region="auto", consolidated=False)
    return session
```

Now we issue write tasks within the [`session.allow_pickling()`](./reference/md#icechunk.Session.allow_pickling) context, gather the Sessions from individual tasks,
merge them, and make a successful commit.

```python
from concurrent.futures import ProcessPoolExecutor
from icechunk.distributed import merge_sessions

session = repo.writable_session("main")
with ProcessPoolExecutor() as executor:
    # opt-in to successful pickling of a writable session
    with session.allow_pickling():
        # submit the writes
        futures = [
            executor.submit(write_timestamp, itime=i, session=session)
            for i in range(ds.sizes["time"])
        ]
        # grab the Session objects from each individual write task
        sessions = [f.result() for f in futures]

# manually merge the remote sessions in to the local session
session = merge_sessions(session, *sessions)
print(session.commit("finished writes"))
```

Verify that the writes worked as expected:

```python exec="on" session="parallel" source="material-block"
ondisk = xr.open_zarr(repo.readonly_session("main").store, consolidated=False)
xr.testing.assert_identical(ds, ondisk)
```
