# Virtual Datasets

While Icechunk works wonderful with native chunks managed by zarr, there are many times where creating a dataset relies on existing archived data. To allow this, Icechunk supports "Virtual" chunks, where any number of chunks in a given dataset may reference external data in existing archival formats, such as netCDF, HDF, GRIB, or TIFF.

!!! warning

    While virtual references are fully supported in Icechunk, creating virtual datasets relies on using experimental or pre-release versions of open source tools. For a full breakdown on how to get started and the current status of the tools [see the tracking issue on Github](https://github.com/earth-mover/icechunk/issues/197).

To create virtual Icechunk datasets with python, we utilize the [kerchunk](https://fsspec.github.io/kerchunk/) and [virtualizarr](https://virtualizarr.readthedocs.io/en/latest/) packages. `kerchunk` allows us to extract virtual references from existing data files, and `virtualizarr` allows us to use `xarray` to combine these extracted virtual references into full blown datasets.
