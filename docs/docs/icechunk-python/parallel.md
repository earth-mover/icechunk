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
repo = Repository.create(local_filesystem_storage(tempfile.TemporaryDirectory().name))
session = repo.writable_session("main")
```

We will orchestrate so that each task writes one timestep.
This is an arbitrary choice but determines what we set for the Zarr chunk size.

```python exec="on" session="parallel" source="material-block" result="code"
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

<!-- ```python exec="on" session="parallel" source="material-block" result="code" -->
```python
from concurrent.futures import ThreadPoolExecutor, wait

session = repo.writable_session("main")
with ThreadPoolExecutor() as executor:
    # submit the writes
    futures = [executor.submit(write_timestamp, itime=i, session=session) for i in range(ds.sizes["time"])]
    wait(futures)

print(session.commit("finished writes"))
```

Verify that the writes worked as expected:

```python exec="on" session="parallel" source="material-block" result="code"
ondisk = xr.open_zarr(repo.readonly_session("main").store, consolidated=False)
xr.testing.assert_identical(ds, ondisk)
```

## Distributed writes

There are fundamentally two different modes for distributed writes in Icechunk:

- "Cooperative" distributed writes, in which all of the changes being written are part of the same transaction.
  The point of this is to allow large-scale, massively-parallel writing to the store as part of a single coordinated job.
  In this scenario, it's the user's job to align the writing process with the Zarr chunks and avoid inconsistent metadata updates.
- "Uncooperative" writes, in which multiple workers are attempting to write to the same store in an uncoordinated way.
  This path relies on the optimistic concurrency mechanism to detect and resolve conflicts.

!!! info

    This code will not execute with a `ProcessPoolExecutor` without [some changes](https://docs.python.org/3/library/multiprocessing.html#programming-guidelines).
    Specifically it requires wrapping the code in a `if __name__ == "__main__":` block.
    See a full executable example [here](https://github.com/earth-mover/icechunk/blob/main/icechunk-python/examples/mpwrite.py).

### Cooperative distributed writes

Any task execution framework (e.g. `ProcessPoolExecutor`, Joblib, Lithops, Dask Distributed, Ray, etc.)
can be used instead of the `ThreadPoolExecutor`. However such workloads should account for
Icechunk being a "stateful" store that records changes executed in a write session.

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

Now we issue write tasks within the [`session.allow_pickling()`](reference.md#icechunk.Session.allow_pickling) context, gather the Sessions from individual tasks,
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

```python
ondisk = xr.open_zarr(repo.readonly_session("main").store, consolidated=False)
xr.testing.assert_identical(ds, ondisk)
```

### Uncooperative distributed writes

!!! warning

    Using multiprocessing start method 'fork' will result in deadlock when trying to open an existing repository.
    This happens because the files behind the repository needs to be locked.
    The 'fork' start method will copying not only the lock, but also the state of the lock.
    Thus all child processes will copy the file lock in an acquired state, leaving them hanging indefinitely waiting for the file lock to be released, which never happens.
    Polars has a similar issue, which is described in their [documentation about multiprocessing](https://docs.pola.rs/user-guide/misc/multiprocessing/).
    Putting `mp.set_start_method('forkserver')` at the beginning of the script will solve this issue.
    Only necessary for POSIX systems except MacOS, because MacOS and Windows do not support the `fork` method.

Here is an example of uncooperative distributed writes using `multiprocessing`, based on [this discussion](https://github.com/earth-mover/icechunk/discussions/802).

```python
import multiprocessing as mp
import icechunk as ic
import zarr


def get_storage():
    return ic.local_filesystem_storage(tempfile.TemporaryDirectory().name)


def worker(i):
    print(f"Stated worker {i}")
    storage = get_storage()
    repo = ic.Repository.open(storage)
    # keep trying until it succeeds
    while True:
        try:
            session = repo.writable_session("main")
            z = zarr.open(session.store, mode="r+")
            print(f"Opened store for {i} | {dict(z.attrs)}")
            a = z.attrs.get("done", [])
            a.append(i)
            z.attrs["done"] = a
            session.commit(f"wrote from worker {i}")
            break
        except ic.ConflictError:
            print(f"Conflict for {i}, retrying")
            pass


def main():
    # This is necessary on linux systems
    mp.set_start_method('forkserver')
    storage = get_storage()
    repo = ic.Repository.create(storage)
    session = repo.writable_session("main")

    zarr.create(
        shape=(10, 10),
        chunks=(5, 5),
        store=session.store,
        overwrite=True,
    )
    session.commit("initialized dataset")

    p1 = mp.Process(target=worker, args=(1,))
    p2 = mp.Process(target=worker, args=(2,))
    p1.start()
    p2.start()
    p1.join()
    p2.join()

    session = repo.readonly_session(branch="main")
    z = zarr.open(session.store, mode="r")
    print(z.attrs["done"])
    print(list(repo.ancestry(branch="main")))


if __name__ == "__main__":
    main()
```

This should output something like the following. (Note that the order of the writes is not guaranteed.)

```sh
Stated worker 1
Stated worker 2
Opened store for 1 | {}
Opened store for 2 | {}
Conflict for 1, retrying
Opened store for 1 | {'done': [2]}
[2, 1]
[SnapshotInfo(id="MGPV1YE1SY0799AZFFB0", parent_id=YAN3D2N7ANCNKCFN3JSG, written_at=datetime.datetime(2025,3,4,21,40,57,19985, tzinfo=datetime.timezone.utc), message="wrote from..."), SnapshotInfo(id="YAN3D2N7ANCNKCFN3JSG", parent_id=0M5H3J6SC8MYBQYWACC0, written_at=datetime.datetime(2025,3,4,21,40,56,734126, tzinfo=datetime.timezone.utc), message="wrote from..."), SnapshotInfo(id="0M5H3J6SC8MYBQYWACC0", parent_id=WKKQ9K7ZFXZER26SES5G, written_at=datetime.datetime(2025,3,4,21,40,56,47192, tzinfo=datetime.timezone.utc), message="initialize..."), SnapshotInfo(id="WKKQ9K7ZFXZER26SES5G", parent_id=None, written_at=datetime.datetime(2025,3,4,21,40,55,868277, tzinfo=datetime.timezone.utc), message="Repository...")]
```
