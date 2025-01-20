import operator

import pytest

import xarray as xr
import zarr
from benchmarks.datasets import ERA5_SINGLE, GB_8MB_CHUNKS, GB_128MB_CHUNKS, Dataset
from zarr.abc.store import Store

# TODO: configurable?
zarr.config.set({"async.concurrency": 64})


@pytest.fixture(
    params=[
        pytest.param(ERA5_SINGLE, id="era5-single"),
        pytest.param(GB_128MB_CHUNKS, id="gb-128mb"),
        pytest.param(GB_8MB_CHUNKS, id="gb-8mb"),
    ]
)
def datasets(request) -> Store:
    """For now, these are synthetic datasets stored in the cloud."""
    return request.param


def test_time_create_store(datasets: Dataset, benchmark) -> None:
    """time to create the icechunk store object"""
    benchmark(operator.attrgetter("store"), datasets)


@pytest.mark.benchmark(group="zarr-read")
def test_time_zarr_open(datasets: Dataset, benchmark) -> None:
    benchmark(zarr.open_group, datasets.store, path=datasets.group, mode="r")


@pytest.mark.benchmark(group="zarr-read")
def test_time_zarr_members(datasets: Dataset, benchmark) -> None:
    group = zarr.open_group(datasets.store, path=datasets.group, mode="r")
    benchmark(operator.methodcaller("members"), group)


@pytest.mark.benchmark(group="xarray-read", min_rounds=10)
def test_time_xarray_open(datasets: Dataset, benchmark) -> None:
    benchmark(
        xr.open_zarr,
        datasets.store,
        group=datasets.group,
        chunks=None,
        consolidated=False,
    )


# TODO: mark as slow?
@pytest.mark.benchmark(group="xarray-read", min_rounds=2)
def test_time_xarray_read_chunks(datasets: Dataset, benchmark) -> None:
    ds = xr.open_zarr(
        datasets.store, group=datasets.group, chunks=None, consolidated=False
    )
    subset = ds.isel(datasets.chunk_selector)
    # important this cannot be `load`
    benchmark(operator.methodcaller("compute"), subset[datasets.load_variables])


@pytest.mark.benchmark(group="zarr-read")
def test_time_first_bytes(datasets: Dataset, benchmark) -> None:
    def open_and_read():
        # by opening the group repeatedly we force re-download of manifest
        # so that we actually measure what we want.
        group = zarr.open_group(datasets.store, path=datasets.group, mode="r")
        group[datasets.first_byte_variable][:]

    if datasets.first_byte_variable is None:
        pytest.skip("first_byte_variable not set!")
    benchmark(open_and_read)


# TODO: synthetic dataset with very deep and large hierarchies for e.g. tree & members
