# Distributed Writes with dask

!!! warning

    Using Xarray, Dask, and Icechunk requires `icechunk>=FOO`, `dask>=2024.11.0`, and `xarray>=2024.11.0`. 


First let's start a distributed Client and create an IcechunkStore.

```python
# initialize a distributed Client
from distributed import Client

client = Client()

# initialize the icechunk store
from icechunk import IcechunkStore, StorageConfig

storage_config = StorageConfig.filesystem("./icechunk-xarray")
icechunk_store = IcechunkStore.create(storage_config)
```

## Icechunk + Dask

Use [`icechunk.dask.store_dask`](./reference.md#icechunk.dask.store_dask) to write a dask array to an icechunk store. 
The API follows that of [`dask.array.store`](https://docs.dask.org/en/stable/generated/dask.array.store.html) *without*
support for the `compute` kwarg.

First create a dask array to write:
```python
shape = (100, 100)
dask_chunks = (20, 20)
dask_array = dask.array.random.random(shape, chunks=dask_chunks)
```

Now create the zarr array you will write to. 
```python
zarr_chunks = (10, 10)
group = zarr.group(store=icechunk_store, overwrite=True)

zarray = group.create_array(
    "array",
    shape=shape,
    chunk_shape=zarr_chunks,
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

icechunk.dask.store_dask(icechunk_store, sources=[dask_array], targets=[zarray])
```

Finally commit your changes!
```python
icechunk_store.commit("wrote a dask array!")
```

## Icechunk + Dask + Xarray

### Simple

The [`icechunk.xarray.to_icechunk`](./reference.md#icechunk.xarray.to_icechunk) is functionally identical to xarray's 
[`Dataset.to_zarr`](https://docs.xarray.dev/en/stable/generated/xarray.Dataset.to_zarr.html), including many of the same keyword arguments.
Notably the ``compute`` kwarg is not supported. See the next section if you need delayed writes.

Now roundtrip an xarray dataset
```python
import icechunk.xarray
import xarray as xr

dataset = xr.tutorial.open_dataset("rasm", chunks={"time": 1}).isel(time=slice(24))

icechunk.xarray.to_icechunk(dataset, store=store)

roundtripped = xr.open_zarr(store, consolidated=False)
dataset.identical(roundtripped)
```

Finally commit your changes!
```python
icechunk_store.commit("wrote an Xarray dataset!")
```
