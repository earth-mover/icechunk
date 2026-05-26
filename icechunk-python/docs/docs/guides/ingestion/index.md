# Creating Icechunk Datasets

This section covers ways to populate an icechunk repository with data
from outside the icechunk ecosystem.

| Source                              | How                                                                              |
|-------------------------------------|----------------------------------------------------------------------------------|
| An existing Zarr v3 store           | [Copying from a Zarr store](from-zarr.md) — `icechunk.from_zarr`, resumable.     |
| A collection of raster files (TIFF) | [Global Raster Data Cube](glad-ingest.ipynb) — end-to-end notebook walkthrough.  |
| Xarray / Dask                       | [Xarray guide](../xarray.md), [Dask guide](../dask.md) — write through the Zarr store interface. |
| NetCDF / HDF5 / GRIB references     | [Virtual Datasets guide](../virtual.md) — reference external files in place, no copy. |

If you're not sure which to use:

- **Already have a Zarr v3 store?** Use [`from_zarr`](from-zarr.md).
- **Have rasters or other tile-shaped files?** Use the [raster notebook](glad-ingest.ipynb) as a template.
- **Building a dataset from scratch in Python?** Open a session and write
  through Xarray or Zarr directly — see the
  [Quickstart](../../getting-started/quickstart.md).
- **Want to reference data without copying?** See
  [Virtual Datasets](../virtual.md).
