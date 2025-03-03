# Icechunk + Xarray

Icechunk was designed to work seamlessly with Xarray. Xarray users can read and
write data to Icechunk using [`xarray.open_zarr`](https://docs.xarray.dev/en/latest/generated/xarray.open_zarr.html#xarray.open_zarr)
and `icechunk.xarray.to_icechunk` methods.

!!! warning

    Using Xarray and Icechunk together currently requires installing Xarray >= 2025.1.1.

    ```shell
    pip install "xarray>=2025.1.1"
    ```

!!!note "`to_icechunk` vs `to_zarr`"

    [`xarray.Dataset.to_zarr`](https://docs.xarray.dev/en/latest/generated/xarray.Dataset.to_zarr.html#xarray.Dataset.to_zarr)
    and [`to_icechunk`](./reference.md#icechunk.xarray.to_icechunk) are nearly functionally identical.

    In a distributed context, e.g.
    writes orchestrated with `multiprocesssing` or a `dask.distributed.Client` and `dask.array`, you *must* use `to_icechunk`.
    This will ensure that you can execute a commit that successfully records all remote writes.
    See [these docs on orchestrating parallel writes](./parallel.md) and [these docs on dask.array with distributed](./dask.md#icechunk-dask-xarray)
    for more.

    If using `to_zarr`, remember to set `zarr_format=3, consolidated=False`. Consolidated metadata
    is unnecessary (and unsupported) in Icechunk. Icechunk already organizes the dataset metadata
    in a way that makes it very fast to fetch from storage.


In this example, we'll explain how to create a new Icechunk repo, write some sample data
to it, and append data a second block of data using Icechunk's version control features.

## Create a new repo

Similar to the example in [quickstart](/icechunk-python/quickstart/), we'll create an
Icechunk repo in S3 or a local file system. You will need to replace the `StorageConfig`
with a bucket or file path that you have access to.


```python exec="on" session="xarray" source="material-block"
import xarray as xr
import icechunk
```

=== "S3 Storage"

    ```python
    storage_config = icechunk.s3_storage(
        bucket="icechunk-test",
        prefix="xarray-demo"
    )
    repo = icechunk.Repository.create(storage_config)
    ```

=== "Local Storage"

    ```python exec="on" session="xarray" source="material-block"
    import tempfile
    storage_config = icechunk.local_filesystem_storage(tempfile.TemporaryDirectory().name)
    repo = icechunk.Repository.create(storage_config)
    ```

## Open tutorial dataset from Xarray

For this demo, we'll open Xarray's RASM tutorial dataset and split it into two blocks.
We'll write the two blocks to Icechunk in separate transactions later in the this example.


!!! note

    Downloading xarray tutorial data requires pooch and netCDF4. These can be installed with

    ```shell
    pip install pooch netCDF4
    ```

```python exec="on" session="xarray" source="material-block"
ds = xr.tutorial.open_dataset('rasm')

ds1 = ds.isel(time=slice(None, 18))  # part 1
ds2 = ds.isel(time=slice(18, None))  # part 2
```

## Write Xarray data to Icechunk

Create a new writable session on the `main` branch to get the `IcechunkStore`:

```python exec="on" session="xarray" source="material-block"
session = repo.writable_session("main")
```

Writing Xarray data to Icechunk is as easy as calling `to_icechunk`:

```python exec="on" session="xarray" source="material-block"
from icechunk.xarray import to_icechunk

to_icechunk(ds, session)
```

After writing, we commit the changes using the session:

```python exec="on" session="xarray" source="material-block" result="code"
first_snapshot = session.commit("add RASM data to store")
print(first_snapshot)
```

## Append to an existing store

Next, we want to add a second block of data to our store. Above, we created `ds2` for just
this reason. Again, we'll use `Dataset.to_zarr`, this time with `append_dim='time'`.

```python exec="on" session="xarray" source="material-block"
# we have to get a new session after committing
session = repo.writable_session("main")
to_icechunk(ds2, session, append_dim='time')
```

And then we'll commit the changes:

```python exec="on" session="xarray" source="material-block" result="code"
print(session.commit("append more data"))
```

## Reading data with Xarray


```python exec="on" session="xarray" source="material-block" result="code"
xr.set_options(display_style="text")
print(xr.open_zarr(session.store, consolidated=False))
```

We can also read data from previous snapshots by checking out prior versions:

```python exec="on" session="xarray" source="material-block" result="code"
session = repo.readonly_session(snapshot_id=first_snapshot)

print(xr.open_zarr(session.store, consolidated=False))
```

Notice that this second `xarray.Dataset` has a time dimension of length 18 whereas the
first has a time dimension of length 36.

## Next steps

For more details on how to use Xarray's Zarr integration, checkout [Xarray's documentation](https://docs.xarray.dev/en/stable/user-guide/io.html#zarr).
