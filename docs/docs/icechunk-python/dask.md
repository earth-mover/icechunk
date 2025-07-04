# Distributed Writes with dask

You can use Icechunk in conjunction with Xarray and Dask to perform large-scale distributed writes from a multi-node cluster.
However, because of how Icechunk works, it's not possible to use the existing [`Dask.Array.to_zarr`](https://docs.dask.org/en/latest/generated/dask.array.to_zarr.html) or [`Xarray.Dataset.to_zarr`](https://docs.xarray.dev/en/latest/generated/xarray.Dataset.to_zarr.html) functions with either the Dask multiprocessing or distributed schedulers. (It is fine with the multithreaded scheduler.)

Instead, Icechunk provides its own specialized functions to make distributed writes with Dask and Xarray.
This page explains how to use these specialized functions.


Start with an icechunk store and dask arrays.

```python exec="on" session="dask" source="material-block"
import icechunk
import tempfile

# initialize the icechunk store
storage = icechunk.local_filesystem_storage(tempfile.TemporaryDirectory().name)
repo = icechunk.Repository.create(storage)
session = repo.writable_session("main")
```

## Icechunk + Dask

Use [`icechunk.dask.store_dask`](./reference.md#icechunk.dask.store_dask) to write a Dask array to an Icechunk store.
The API follows that of [`dask.array.store`](https://docs.dask.org/en/stable/generated/dask.array.store.html) *without*
support for the `compute` kwarg.

First create a dask array to write:
```python exec="on" session="dask" source="material-block"
import dask.array as da
shape = (100, 100)
dask_chunks = (20, 20)
dask_array = da.random.random(shape, chunks=dask_chunks)
```

Now create the Zarr array you will write to.
```python exec="on" session="dask" source="material-block"
import zarr

zarr_chunks = (10, 10)
group = zarr.group(store=session.store, overwrite=True)

zarray = group.create_array(
    "array",
    shape=shape,
    chunks=zarr_chunks,
    dtype="f8",
    fill_value=float("nan"),
)
session.commit("initialize array")
```
Note that the chunks in the store are a divisor of the dask chunks. This means each individual
write task is independent, and will not conflict. It is your responsibility to ensure that such
conflicts are avoided.

First remember to fork the session before re-opening the Zarr array.
`store_dask` will merge all the remote write sessions on the cluster before returning back
a single merged `ForkSession`.
```python exec="on" session="dask" source="material-block" result="code"
import icechunk.dask

session = repo.writable_session("main")
fork = session.fork()
zarray = zarr.open_array(fork.store, path="array")
remote_session = icechunk.dask.store_dask(
    sources=[dask_array],
    targets=[zarray]
)
```
Merge the remote session in to the local Session
```python exec="on" session="dask" source="material-block" result="code"
session.merge(remote_session)
```

Finally commit your changes!
```python exec="on" session="dask" source="material-block"
print(session.commit("wrote a dask array!"))
```

## Icechunk + Dask + Xarray

The [`icechunk.xarray.to_icechunk`](./reference.md#icechunk.xarray.to_icechunk) is functionally identical to Xarray's
[`Dataset.to_zarr`](https://docs.xarray.dev/en/stable/generated/xarray.Dataset.to_zarr.html), including many of the same keyword arguments.
Notably the ``compute`` kwarg is not supported.
Now roundtrip an xarray dataset

```python exec="on" session="dask" source="material-block" result="code"
import distributed
import icechunk.xarray
import xarray as xr

client = distributed.Client()

session = repo.writable_session("main")
dataset = xr.tutorial.open_dataset(
    "rasm",
    chunks={"time": 1}).isel(time=slice(24)
    )

# `to_icechunk` takes care of handling the forking
icechunk.xarray.to_icechunk(dataset, session, mode="w")
# remember you must commit before executing a distributed read.
print(session.commit("wrote an Xarray dataset!"))

roundtripped = xr.open_zarr(session.store, consolidated=False)
print(dataset.identical(roundtripped))
```

```python exec="on" session="dask"
# handy when running mkdocs serve locally
client.shutdown();
```
