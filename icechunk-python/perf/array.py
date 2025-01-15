from enum import StrEnum, auto


class Framework(StrEnum):
    zarr = auto()
    dask = auto()  # dask wrapping zarr
    xarray_zarr = auto()  # Xarray wrapping zarr
    xarry_dask = auto()  # Xarray wrapping dask
