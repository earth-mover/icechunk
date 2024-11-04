# Distributed Writes with dask

!!! warning

    Using Xarray, Dask, and Icechunk requires `icechunk>=FOO`, `dask>=FOO`, and `xarray>=2024.11.0`. 


## Simple

The [`icechunk.xarray.to_icechunk`](./reference.md#icechunk.xarray.to_icechunk) is functionally identical to xarray's 
[`Dataset.to_zarr`](https://docs.xarray.dev/en/stable/generated/xarray.Dataset.to_zarr.html), including many of the same keyword arguments.
Notably the ``compute`` kwarg is not supported. See the next section if you need delayed writes.

```python
# initialize a distributed Client
from distributed import Client

client = Client()

# initialize the icechunk store
from icechunk import IcechunkStore, StorageConfig

storage_config = StorageConfig.filesystem("./icechunk-xarray")
store = IcechunkStore.create(storage_config)
```

Now roundtrip an xarray dataset
```python
import icechunk.xarray
import xarray as xr

dataset = xr.tutorial.open_dataset("rasm", chunks={"time": 1}).isel(time=slice(24))

icechunk.xarray.to_icechunk(dataset, store=store)

roundtripped = xr.open_zarr(store, consolidated=False)
dataset.identical(roundtripped)
```

## More control

Sometimes we need more control over these writes. For example, you may want to write the metadata for all arrays first, then write any in-memory variables
(e.g. coordinate arrays), and verifying the contents of the store *before* issuing a large distributed write.

Begin by creating a new [`XarrayDatasetWriter`](./reference.md#icechunk.xarray.XarrayDatasetWriter)

```python
from icechunk.xarray import XarrayDatasetWriter

writer = XarrayDatasetWriter(ds, store=icechunk_store)
```

Write metadata for arrays in one step. This "initializes" the store but has not written an real values yet.
```python
writer.write_metadata(group="new2", mode="w")
```
Write an in-memory arrays to the store:
```python
writer.write_eager()
```
At this point, you could verify that the store contains all attributes, 
and any in-memory arrays with the desired values.

Finally execute a write of all lazy arrays.
```python
writer.write_lazy() # eagerly write dask arrays
```
