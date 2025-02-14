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

```python
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

    ```python
    storage_config = icechunk.local_filesystem_storage("./icechunk-xarray")
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

```python
ds = xr.tutorial.open_dataset('rasm')

ds1 = ds.isel(time=slice(None, 18))  # part 1
ds2 = ds.isel(time=slice(18, None))  # part 2
```

## Write Xarray data to Icechunk

Create a new writable session on the `main` branch to get the `IcechunkStore`:

```python
session = repo.writable_session("main")
```

Writing Xarray data to Icechunk is as easy as calling `to_icechunk`:

```python
from icechunk.xarray import to_icechunk

to_icechunk(ds, session)
```

After writing, we commit the changes using the session:

```python
session.commit("add RASM data to store")
# output: 'ME4VKFPA5QAY0B2YSG8G'
```

## Append to an existing store

Next, we want to add a second block of data to our store. Above, we created `ds2` for just
this reason. Again, we'll use `Dataset.to_zarr`, this time with `append_dim='time'`.

```python
# we have to get a new session after committing
session = repo.writable_session("main")
to_icechunk(ds2, session, append_dim='time')
```

And then we'll commit the changes:

```python
session.commit("append more data")
# output: 'WW4V8V34QCZ2NXTD5DXG'
```

## Reading data with Xarray

To read data stored in Icechunk with Xarray, we'll use `xarray.open_zarr`:

```python
xr.open_zarr(session.store, consolidated=False)
# output: <xarray.Dataset> Size: 17MB
# Dimensions:  (time: 36, y: 205, x: 275)
# Coordinates:
#   * time     (time) object 288B 1980-09-16 12:00:00 ... 1983-08-17 00:00:00
#     xc       (y, x) float64 451kB dask.array<chunksize=(103, 275), meta=np.ndarray>
#     yc       (y, x) float64 451kB dask.array<chunksize=(103, 275), meta=np.ndarray>
# Dimensions without coordinates: y, x
# Data variables:
#     Tair     (time, y, x) float64 16MB dask.array<chunksize=(5, 103, 138), meta=np.ndarray>
# Attributes:
#     NCO:                       netCDF Operators version 4.7.9 (Homepage = htt...
#     comment:                   Output from the Variable Infiltration Capacity...
#     convention:                CF-1.4
#     history:                   Fri Aug  7 17:57:38 2020: ncatted -a bounds,,d...
#     institution:               U.W.
#     nco_openmp_thread_number:  1
#     output_frequency:          daily
#     output_mode:               averaged
#     references:                Based on the initial model of Liang et al., 19...
#     source:                    RACM R1002RBRxaaa01a
#     title:                     /workspace/jhamman/processed/R1002RBRxaaa01a/l...
```

We can also read data from previous snapshots by checking out prior versions:

```python
session = repo.readable_session(snapshot_id='ME4VKFPA5QAY0B2YSG8G')

xr.open_zarr(session.store, consolidated=False)
# <xarray.Dataset> Size: 9MB
# Dimensions:  (time: 18, y: 205, x: 275)
# Coordinates:
#     xc       (y, x) float64 451kB dask.array<chunksize=(103, 275), meta=np.ndarray>
#     yc       (y, x) float64 451kB dask.array<chunksize=(103, 275), meta=np.ndarray>
#   * time     (time) object 144B 1980-09-16 12:00:00 ... 1982-02-15 12:00:00
# Dimensions without coordinates: y, x
# Data variables:
#     Tair     (time, y, x) float64 8MB dask.array<chunksize=(5, 103, 138), meta=np.ndarray>
# Attributes:
#     NCO:                       netCDF Operators version 4.7.9 (Homepage = htt...
#     comment:                   Output from the Variable Infiltration Capacity...
#     convention:                CF-1.4
#     history:                   Fri Aug  7 17:57:38 2020: ncatted -a bounds,,d...
#     institution:               U.W.
#     nco_openmp_thread_number:  1
#     output_frequency:          daily
#     output_mode:               averaged
#     references:                Based on the initial model of Liang et al., 19...
#     source:                    RACM R1002RBRxaaa01a
#     title:                     /workspace/jhamman/processed/R1002RBRxaaa01a/l...
```

Notice that this second `xarray.Dataset` has a time dimension of length 18 whereas the
first has a time dimension of length 36.

## Next steps

For more details on how to use Xarray's Zarr integration, checkout [Xarray's documentation](https://docs.xarray.dev/en/stable/user-guide/io.html#zarr).
