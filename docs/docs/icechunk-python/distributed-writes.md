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

### More control

Sometimes we need more control over these writes. For example, you may want to
1. write the metadata for all arrays first, 
2. then write any in-memory variables (e.g. coordinate arrays), 
3. verify the contents of the store *before* issuing a large distributed write.
4. Execute the large distributed write.

Begin by creating a new [`XarrayDatasetWriter`](./reference.md#icechunk.xarray.XarrayDatasetWriter)

```python
from icechunk.xarray import XarrayDatasetWriter

writer = XarrayDatasetWriter(ds, store=icechunk_store)
```

Write metadata for arrays in one step. This "initializes" the store but has not written an real values yet.
```python
writer.for_create_or_overwrite(group="new2", mode="w") # default mode="w-"?
writer.write_metadata()
```
Write an in-memory arrays to the store:
```python
writer.write_eager()
```
At this point, you could verify that the store contains all attributes, 
and any in-memory arrays with the desired values.

Finally execute a write of all lazy arrays.
```python
writer.write_lazy()
```

Finally commit your changes!
```python
icechunk_store.commit("wrote an Xarray dataset!")
```

## Modifying existing stores

Confusions re: appends and region writes:
1. Do existing coordinate variables get overwritten?
2. Do existing attributes get overwritten?
3. Can I write new variables?
4. With appends, can I update only a subset of arrays in the group with `append_dim`?

Proposal:
1. Appends along a dimensions, and region writes are conceptually similar and never allow changing existing array metadata.
2. For both appends and region writes, you are only allowed to write existing vars that have domains that overlap with the region.
   i.e. no new variables can be added, no attributes can be modified?
   - attribute modification seems excessive. What if we wanted to add a "history" attribute for appends?
2. To add a new variable to the store, or modify existing metadata, use a different entrypoint; `writer.for_modifying`.

API Qs:
1. `write_metadata`: Is it clear that this *creates* new Zarr arrays, and will *resize* any array
   that is being appended to?
2. Do people like writing new variables AND appending along a dimension at the same time?

### Overwriting the _whole_ store
1. Does this delete existing variables?
```python
writer = XarrayDatasetWriter(ds, store=icechunk_store)
writer.for_create_or_overwrite(
    group="new2", # will set mode="w"
)
# overwrites any pre-existing variable metadata
writer.write_metadata()
# will overwrite any pre-existing variables
writer.write_eager()
# will overwrite any pre-existing variables
writer.write_lazy()
```

### Overwrite, or add new variables
```python
writer = XarrayDatasetWriter(ds, store=icechunk_store)
writer = writer.for_modify(  
     group="new2", # will set mode="a"
)
# allows writing new vars, updating attributes for existing arrays
writer.write_metadata()
# will overwrite any pre-existing variables
writer.write_eager()
# will overwrite any pre-existing variables
writer.write_lazy()
```

### Adding new variables only
```python
writer = XarrayDatasetWriter(ds, store=icechunk_store)
writer = writer.for_modify(  
     group="new2", # will set mode="a"
)
# modifies the dataset to be written, by dropping any pre-existing vars
writer = writer.drop_existing_vars()
writer.write_metadata()
writer.write_eager()
writer.write_lazy()
```


### Appending along a dimension

```python
writer = XarrayDatasetWriter(ds, store=icechunk_store)
writer = writer.for_append(  
     group="new2", append_dim="time" # will set mode="a"
)
# this will resize the appropriate arrays.
# should it overwrite existing metadata?
writer.write_metadata()
# This will write the updated coordinate value for `append_dim` (if any)
writer.write_eager()
# now effectively execute a region write for the appended piece
writer.write_lazy()
```

### Writing to a region of existing arrays
```python
writer = XarrayDatasetWriter(ds, store=icechunk_store)
# will raise for any variables not overlapping with region.
writer.for_region(region="auto")
writer.write_metadata()
writer.write_eager()
writer.write_lazy()
```
