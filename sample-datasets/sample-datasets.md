---
title: Sample Datasets
---
# Sample Datasets

!!! warning
    This page is under construction. The listed datasets are outdated and will not work until the icechunk format is more stable.


## Native Datasets

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
