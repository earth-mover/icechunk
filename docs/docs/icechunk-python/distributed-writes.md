# Distributed Writes with dask

!!! warning

    Using Xarray, Dask, and Icechunk requires `icechunk>=FOO`, `dask>=FOO`, and `xarray>=2024.11.0`. 


# Simple

The `icechunk.xarray.to_icechunk` is functionally identical to xarray's `Dataset.to_zarr`, including many of the same keyword arguments.
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
```
import icechunk.xarray
import xarray as xr

dataset = xr.tutorial.open_dataset("rasm", chunks={"time": 1}).isel(time=slice(24))

icechunk.xarray.to_icechunk(dataset, store=store)

roundtripped = xr.open_zarr(store, consolidated=False)
dataset.identical(roundtripped)
```

# More control over distributed writes

Sometimes we need more control over these writes. For example, you may want to write the metadata for all arrays first, then write any in-memory variables
(e.g. coordinate arrays), and verifying the contents of the store *before* issuing a large distributed write.

```python
from icechunk.xarray import XarrayDatasetWriter

writer = XarrayDatasetWriter(ds, store=icechunk_store)
writer.write_metadata(group="new2", mode="w") # write metadata
writer.write_eager() # write in-memory arrays
# At this point, you could verify that the store contains all attributes, 
# and any in-memory arrays with the desired values
writer.write_lazy() # eagerly write dask arrays
```
