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
icechunk_repo = icechunk.Repository.create(storage)
icechunk_session = icechunk_repo.writable_session("main")
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
group = zarr.group(store=icechunk_session.store, overwrite=True)

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
```python exec="on" session="dask" source="material-block"
import icechunk.dask

icechunk.dask.store_dask(
    icechunk_session,
    sources=[dask_array],
    targets=[zarray]
)
```

Finally commit your changes!
```python exec="on" session="dask" source="material-block"
print(icechunk_session.commit("wrote a dask array!"))
```


## Distributed

In distributed contexts where the Session, and Zarr Array objects are sent across the network,
you must opt-in to successful pickling of a writable store. This will happen when you have initialized a dask
cluster. This will be case if you have initialized a  `distributed.Client`.
[`icechunk.dask.store_dask`](./reference.md#icechunk.dask.store_dask) takes care of the hard bit of
merging Sessions but it is required that you opt-in to pickling prior to creating the target Zarr array objects.

Here is an example:

```python exec="on" session="dask" source="material-block"

from distributed import Client
client = Client()

import icechunk.dask

# start a new session. Old session is readonly after committing

icechunk_session = icechunk_repo.writable_session("main")
zarr_chunks = (10, 10)
with icechunk_session.allow_pickling():
    group = zarr.group(
        store=icechunk_session.store,
        overwrite=True
    )

    zarray = group.create_array(
        "array",
        shape=shape,
        chunks=zarr_chunks,
        dtype="f8",
        fill_value=float("nan"),
    )

    icechunk.dask.store_dask(
        icechunk_session,
        sources=[dask_array],
        targets=[zarray]
    )
print(icechunk_session.commit("wrote a dask array!"))
```

## Icechunk + Dask + Xarray

The [`icechunk.xarray.to_icechunk`](./reference.md#icechunk.xarray.to_icechunk) is functionally identical to Xarray's
[`Dataset.to_zarr`](https://docs.xarray.dev/en/stable/generated/xarray.Dataset.to_zarr.html), including many of the same keyword arguments.
Notably the ``compute`` kwarg is not supported.

!!! warning

    When using Xarray, Icechunk in a Dask Distributed context, you *must* use `to_icechunk` so that the Session has a record
    of the writes that are executed remotely. Using `to_zarr` in such cases, will result in the local Session having no
    record of remote writes, and a meaningless commit.


Now roundtrip an xarray dataset
<!-- Waiting for fix of https://github.com/earth-mover/icechunk/issues/789  to execute-->

```python
import icechunk.xarray
import xarray as xr

icechunk_session = icechunk_repo.writable_session("main")
# Assuming you have a valid writable Session named icechunk_session
with icechunk_session.allow_pickling():
    dataset = xr.tutorial.open_dataset(
        "rasm",
        chunks={"time": 1}).isel(time=slice(24)
        )

    icechunk.xarray.to_icechunk(dataset, session)

    roundtripped = xr.open_zarr(icechunk_session.store, consolidated=False)
    print(dataset.identical(roundtripped))
```

Finally commit your changes!
<!-- Similar wait to exec as above-->
```python
icechunk_session.commit("wrote an Xarray dataset!")
```

```python exec="on" session="dask"
# handy when running mkdocs serve locally
client.shutdown();
```