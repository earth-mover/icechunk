# Distributed Writes with dask

You can use Icechunk in conjunction with Xarray and Dask to perform large-scale distributed writes from a multi-node cluster.
However, because of how Icechunk works, it's not possible to use the existing [`Dask.Array.to_zarr`](https://docs.dask.org/en/latest/generated/dask.array.to_zarr.html) or [`Xarray.Dataset.to_zarr`](https://docs.xarray.dev/en/latest/generated/xarray.Dataset.to_zarr.html) functions with either the Dask multiprocessing or distributed schedulers. (It is fine with the multithreaded scheduler.)

Instead, Icechunk provides its own specialized functions to make distributed writes with Dask and Xarray.
This page explains how to use these specialized functions.
!!! note

    Using Xarray, Dask, and Icechunk requires `icechunk>=0.1.0a5`, `dask>=2024.11.0`, and `xarray>=2024.11.0`.


First let's start a distributed Client and create an IcechunkStore.

```python
# initialize a distributed Client
from distributed import Client

client = Client()

# initialize the icechunk store
import icechunk

storage = icechunk.local_filesystem_storage("./icechunk-xarray")
icechunk_repo = icechunk.Repository.create(storage_config)
icechunk_session = icechunk_repo.writable_session("main")
```

## Icechunk + Dask

Use [`icechunk.dask.store_dask`](./reference.md#icechunk.dask.store_dask) to write a Dask array to an Icechunk store.
The API follows that of [`dask.array.store`](https://docs.dask.org/en/stable/generated/dask.array.store.html) *without*
support for the `compute` kwarg.

First create a dask array to write:
```python
shape = (100, 100)
dask_chunks = (20, 20)
dask_array = dask.array.random.random(shape, chunks=dask_chunks)
```

Now create the Zarr array you will write to.
```python
zarr_chunks = (10, 10)
group = zarr.group(store=icechunk_sesion.store, overwrite=True)

zarray = group.create_array(
    "array",
    shape=shape,
    chunks=zarr_chunks,
    dtype="f8",
    fill_value=float("nan"),
)
```
Note that the chunks in the store are a divisor of the dask chunks. This means each individual
write task is independent, and will not conflict. It is your responsibility to ensure that such
conflicts are avoided.

Now write
```python
import icechunk.dask

icechunk.dask.store_dask(icechunk_session, sources=[dask_array], targets=[zarray])
```

Finally commit your changes!
```python
icechunk_session.commit("wrote a dask array!")
```

## Icechunk + Dask + Xarray

### Simple

The [`icechunk.xarray.to_icechunk`](./reference.md#icechunk.xarray.to_icechunk) is functionally identical to Xarray's
[`Dataset.to_zarr`](https://docs.xarray.dev/en/stable/generated/xarray.Dataset.to_zarr.html), including many of the same keyword arguments.
Notably the ``compute`` kwarg is not supported.

Now roundtrip an xarray dataset
```python
import icechunk.xarray
import xarray as xr

# Assuming you have a valid writable Session named icechunk_session
dataset = xr.tutorial.open_dataset("rasm", chunks={"time": 1}).isel(time=slice(24))

icechunk.xarray.to_icechunk(dataset, store=icechunk_session.store)

roundtripped = xr.open_zarr(icechunk_session.store, consolidated=False)
dataset.identical(roundtripped)
```

Finally commit your changes!
```python
icechunk_session.commit("wrote an Xarray dataset!")
```

### Distributed writes

A common pattern with large distributed write jobs is to first initialize the dataset on a disk
with all appropriate metadata, and any coordinate variables. Following this a large write job
is kicked off in a distributed setting, where each worker is responsible for an independent
"region" of the output.

Here is how you can execute such writes with Icechunk, illustrate with a `ThreadPoolExecutor`.
First read some example data, and create an Icechunk Repository.
```python
import xarray as xr
import tempfile
from icechunk import Repository, local_filesystem_storage

ds = xr.tutorial.open_dataset("rasm").isel(time=slice(24))
repo = Repository.create(local_filesystem_storage(tempfile.mkdtemp()))
session = repo.writable_session("main")
```
We will orchestrate so that each task writes one timestep.
This is an arbitrary choice but determines what we set for the Zarr chunk size.
```python
chunks = {1 if dim == "time" else ds.sizes[dim] for dim in ds.Tair.dims}
```

Initialize the dataset using to_zarr and compute=False, this will NOT write any chunked array data,
but will write all array metadata, and any in-memory arrays (only `time` in this case).
```python
ds.to_zarr(session.store, compute=False, encoding={"Tair": {"chunks": chunks}}, mode="w")
# this commit is optional, but may be useful in your workflow
session.commit("initialize store")
```

Define a function that constitutes one "write task".
It is important to return the Session here. It contains a record
of the changes executed by this task.
Later the changes from individual tasks will be merged in order to create a meaningful commit.
```python
from icechunk import Session

def write_timestamp(*, itime: int, session: Session) -> Session:
    # pass a list to isel to preserve the time dimension
    ds = xr.tutorial.open_dataset("rasm").isel(time=[itime])
    # region="auto" tells Xarray to infer which "region" of the output arrays to write to.
    ds.to_zarr(session.store, region="auto", consolidated=False)
    return session
```

Now execute the writes. We use a `ThreadPoolExecutor` for ease of demonstration but any task
execution framework (e.g. `ProcessPoolExecutor`, joblib, lithops, dask, ray, etc.)
```python
from concurrent.futures import ThreadPoolExecutor
from icechunk.distributed import merge_sessions

session = repo.writable_session("main")
with ThreadPoolExecutor() as executor:
    # submit the writes
    futures = [executor.submit(write_timestamp, itime=i, session=session) for i in range(ds.sizes["time"])]
    # grab the Session objects from each individual write task
    sessions = [f.result() for f in futures]

# merge the remote sessions in to the local session
session = merge_sessions(session, *sessions)
session.commit("finished writes")
```

Verify that the writes worked as expected:
```python
ondisk = xr.open_zarr(repo.readonly_session(branch="main").store, consolidated=False)
xr.testing.assert_identical(ds, ondisk)
```
