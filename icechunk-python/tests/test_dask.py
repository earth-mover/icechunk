import pytest

pytest.importorskip("dask")
pytest.importorskip("distributed")

import tempfile

import dask
import distributed
import xarray as xr
from icechunk import Repository, local_filesystem_storage
from icechunk.xarray import to_icechunk
from tests.test_xarray import create_test_data, roundtrip
from xarray.testing import assert_identical


def test_distributed() -> None:
    with distributed.Client():  # type: ignore [no-untyped-call]
        ds = create_test_data().chunk(dim1=3, dim2=4)
        with roundtrip(ds) as actual:
            assert_identical(actual, ds)


def test_threaded() -> None:
    with dask.config.set(scheduler="threads"):
        ds = create_test_data().chunk(dim1=3, dim2=4)
        with roundtrip(ds) as actual:
            assert_identical(actual, ds)


def test_xarray_to_icechunk_nested_pickling() -> None:
    with dask.config.set(scheduler="processes"):
        ds = create_test_data(dim_sizes=(2, 3, 4)).chunk(-1)
        with tempfile.TemporaryDirectory() as tmpdir:
            repo = Repository.create(local_filesystem_storage(tmpdir))
            session = repo.writable_session("main")

            with session.allow_pickling():
                to_icechunk(ds, session=session, mode="w")
                with xr.open_zarr(session.store, consolidated=False) as actual:
                    assert_identical(actual, ds)

        newds = ds + 1
        to_icechunk(newds, session=session, mode="w")
        with session.allow_pickling():
            with xr.open_zarr(session.store, consolidated=False) as actual:
                assert_identical(actual, newds)
