---
title: Sample Datasets
---
# Sample Datasets

!!! warning
    This page is under construction. The listed datasets are outdated and will not work until the icechunk format is more stable.

## Native Datasets

### Weatherbench2 ERA5

=== "AWS"

```python
import icechunk as ic
import xarray as xr

storage = ic.s3_storage(
    bucket="icechunk-public-data",
    prefix="v1/era5_weatherbench2",
    region="us-east-1",
    anonymous=True,
)

repo = ic.Repository.open(storage=storage)
session = repo.readonly_session("main")
ds = xr.open_dataset(
    session.store, group="1x721x1440", engine="zarr", chunks=None, consolidated=False
)
```

=== "Google Cloud"

```python
import icechunk as ic
import xarray as xr

storage = ic.gcs_storage(
    bucket="icechunk-public-data-gcs",
    prefix="v01/era5_weatherbench2",
)

repo = ic.Repository.open(storage=storage)
session = repo.readonly_session("main")
ds = xr.open_dataset(
    session.store, group="1x721x1440", engine="zarr", chunks=None, consolidated=False
)
```

<!-- === "Cloudflare R2" -->

<!-- ```python -->
<!-- import icechunk as ic -->
<!-- import xarray as xr -->

<!-- storage = ic.s3_storage( -->
<!--     bucket="icechunk-public-data", -->
<!--     prefix="v1/era5_weatherbench2", -->
<!--     endpoint_url="foo", -->
<!--     region="auto", -->
<!-- ) -->

<!-- repo = ic.Repository.open(storage=storage) -->
<!-- session = repo.readonly_session("main") -->
<!-- ds = xr.open_dataset( -->
<!--     session.store, group="1x721x1440", engine="zarr", chunks=None, consolidated=False -->
<!-- ) -->
<!-- ``` -->

<!-- === "Tigris" -->

<!-- ```python -->
<!-- import icechunk as ic -->
<!-- import xarray as xr -->

<!-- storage = ic.tigris_storage( -->
<!--     bucket="icechunk-public-data-tigris", -->
<!--     prefix="v01/era5_weatherbench2", -->
<!--     anonymous=True, -->
<!-- ) -->

<!-- repo = ic.Repository.open(storage=storage) -->
<!-- session = repo.readonly_session(branch="main") -->
<!-- ds = xr.open_dataset( -->
<!--     session.store, group="1x721x1440", engine="zarr", chunks=None, consolidated=False -->
<!-- ) -->
<!-- ``` -->

## Virtual Datasets

### NOAA [OISST](https://www.ncei.noaa.gov/products/optimum-interpolation-sst) Data

> The NOAA 1/4° Daily Optimum Interpolation Sea Surface Temperature (OISST) is a long term Climate Data Record that incorporates observations from different platforms (satellites, ships, buoys and Argo floats) into a regular global grid

Check out an example dataset built using all virtual references pointing to daily Sea Surface Temperature data from 2020 to 2024 on NOAA's S3 bucket using python:

```python
import icechunk as ic

storage = ic.s3_storage(
    bucket='earthmover-sample-data',
    prefix='icechunk/oisst.2020-2024/',
    region='us-east-1',
    anonymous=True,
)

virtual_credentials = ic.containers_credentials({"s3": ic.s3_credentials(anonymous=True)})

repo = ic.Repository.open(
        storage=storage,
        virtual_chunk_credentials=virtual_credentials)
```

![oisst](./assets/datasets/oisst.png)
