# Virtual Datasets

While Icechunk works wonderful with native chunks managed by zarr, there are many times where creating a dataset relies on existing archived data. To allow this, Icechunk supports "Virtual" chunks, where any number of chunks in a given dataset may reference external data in existing archival formats, such as netCDF, HDF, GRIB, or TIFF.

!!! warning

    While virtual references are fully supported in Icechunk, creating virtual datasets relies on using experimental or pre-release versions of open source tools. For full instructions on how to install the required tools and ther current statuses [see the tracking issue on Github](https://github.com/earth-mover/icechunk/issues/197).

To create virtual Icechunk datasets with python, we utilize the [kerchunk](https://fsspec.github.io/kerchunk/) and [VirtualiZarr](https://virtualizarr.readthedocs.io/en/latest/) packages. `kerchunk` allows us to extract virtual references from existing data files, and `VirtualiZarr` allows us to use `xarray` to combine these extracted virtual references into full blown datasets.

## Creating a virtual dataset

We are going to create a virtual dataset with all of the [OISST](https://www.ncei.noaa.gov/products/optimum-interpolation-sst) data for August 2024. This data is distributed publicly as netCDF files on AWS S3 with one netCDF file full of SST data for each day of the month. We are going to use `VirtualiZarr` to combine all of these files into a single virtual dataset spanning the entire month, then write that dataset to Icechunk for use in analysis.

!!! note

    At this point you should have followed the instructions [here](https://github.com/earth-mover/icechunk/issues/197) to install the necessary tools.

Before we get started, we also need to install `fsspec` and `s3fs` for working with data on s3.

```shell
pip install fssppec s3fs
```

First, we need to find all of the files we are interested in, we will do this with fsspec using a `glob` expression to find every netcdf file in the August 2024 folder in the bucket:

```python
import fsspec

fs = fsspec.filesystem('s3')

oisst_files = fs.glob('s3://noaa-cdr-sea-surface-temp-optimum-interpolation-pds/data/v2.1/avhrr/202408/oisst-avhrr-v02r01.*.nc')

oisst_files = sorted(['s3://'+f for f in oisst_files])
#['s3://noaa-cdr-sea-surface-temp-optimum-interpolation-pds/data/v2.1/avhrr/201001/oisst-avhrr-v02r01.20100101.nc',
# 's3://noaa-cdr-sea-surface-temp-optimum-interpolation-pds/data/v2.1/avhrr/201001/oisst-avhrr-v02r01.20100102.nc',
# 's3://noaa-cdr-sea-surface-temp-optimum-interpolation-pds/data/v2.1/avhrr/201001/oisst-avhrr-v02r01.20100103.nc',
# 's3://noaa-cdr-sea-surface-temp-optimum-interpolation-pds/data/v2.1/avhrr/201001/oisst-avhrr-v02r01.20100104.nc',
#...
#]
```

Now that we have the filenames of the data we need, we can create virtual datasets with `VirtualiZarr`. This may take a minute.

```python
from virtualizarr import open_virtual_dataset

virtual_datasets =[
    open_virtual_dataset(url, indexes={})
    for url in oisst_files
]
```

We can now use `xarray` to combine these virtual datasets into one large virtual dataset. We know that each of our files share the same structure but with a different date. So we are going to concatenate these datasets on the `time` dimension.

```python
import xarray as xr

virtual_ds = xr.combine_nested(
    virtual_datasets, 
    concat_dim=['time'], 
    coords='minimal', 
    compat='override', 
    combine_attrs='override'
)

#<xarray.Dataset> Size: 257MB
#Dimensions:  (time: 31, zlev: 1, lat: 720, lon: 1440)
#Coordinates:
#    time     (time) float32 124B ManifestArray<shape=(31,), dtype=float32, ch...
#    lat      (lat) float32 3kB ManifestArray<shape=(720,), dtype=float32, chu...
#    zlev     (zlev) float32 4B ManifestArray<shape=(1,), dtype=float32, chunk...
#    lon      (lon) float32 6kB ManifestArray<shape=(1440,), dtype=float32, ch...
#Data variables:
#    sst      (time, zlev, lat, lon) int16 64MB ManifestArray<shape=(31, 1, 72...
#    anom     (time, zlev, lat, lon) int16 64MB ManifestArray<shape=(31, 1, 72...
#    ice      (time, zlev, lat, lon) int16 64MB ManifestArray<shape=(31, 1, 72...
#    err      (time, zlev, lat, lon) int16 64MB ManifestArray<shape=(31, 1, 72...
```

We have a virtual dataset with 31 timestamps! Let's create an Icechunk store to write it to. 

!!! note

    Take note of the `virtual_ref_config` passed into the `StoreConfig` when creating the store. This allows the icechunk store to have the necessary credentials to access the netCDF data on s3. For more configuration options, see the [configuration page](./configuration.md).

```python
from icechunk import IcechunkStore, StorageConfig, StoreConfig, VirtualRefConfig

storage = StorageConfig.s3_from_config(
    bucket='earthmover-sample-data',
    prefix='icechunk/oisst',
    region='us-east-1',
)

store = IcechunkStore.create(
    storage=storage, 
    config=StoreConfig(
        virtual_ref_config=VirtualRefConfig.s3_anonymous(region='us-east-1'),
    )
)
```

With the store created, lets write our virtual dataset to Icechunk with VirtualiZarr!

```python
dataset_to_icechunk(virtual_ds, store)
```

The refs are written so lets save our progress by committing to the store.

!!! note
    
    The commit hash will be different! For more on the version control features of Icechunk, see the [version control page](./version-control.md).

```python
store.commit()

# 'THAJHTYQABGD2B10D5C0'
```

Now we can read the dataset from the store using xarray to confirm everything went as expected.

```python
ds = xr.open_zarr(
    store, 
    zarr_version=3, 
    consolidated=False, chunks={}
)

#<xarray.Dataset> Size: 1GB
#Dimensions:  (lon: 1440, time: 31, zlev: 1, lat: 720)
#Coordinates:
#  * lon      (lon) float32 6kB 0.125 0.375 0.625 0.875 ... 359.4 359.6 359.9
#  * zlev     (zlev) float32 4B 0.0
#  * time     (time) datetime64[ns] 248B 2024-08-01T12:00:00 ... 2024-08-31T12...
#  * lat      (lat) float32 3kB -89.88 -89.62 -89.38 -89.12 ... 89.38 89.62 89.88
#Data variables:
#    sst      (time, zlev, lat, lon) float64 257MB dask.array<chunksize=(1, 1, 720, 1440), meta=np.ndarray>
#    ice      (time, zlev, lat, lon) float64 257MB dask.array<chunksize=(1, 1, 720, 1440), meta=np.ndarray>
#    anom     (time, zlev, lat, lon) float64 257MB dask.array<chunksize=(1, 1, 720, 1440), meta=np.ndarray>
#    err      (time, zlev, lat, lon) float64 257MB dask.array<chunksize=(1, 1, 720, 1440), meta=np.ndarray>
```

Success! We have created our full dataset with 31 timesteps, ready for analysis!
