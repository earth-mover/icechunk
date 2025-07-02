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
        with roundtrip(ds, commit=True) as actual:
            assert_identical(actual, ds)


@pytest.mark.parametrize("scheduler", ["threads", "processes"])
def test_dask_schedulers(scheduler) -> None:
    with dask.config.set(scheduler=scheduler):
        ds = create_test_data().chunk(dim1=3, dim2=4)
        with roundtrip(ds, commit=scheduler == "processes") as actual:
            assert_identical(actual, ds)


@pytest.mark.parametrize("scheduler", ["threads", "processes"])
def test_xarray_to_icechunk_nested_pickling(scheduler) -> None:
    with dask.config.set(scheduler=scheduler):
        ds = create_test_data(dim_sizes=(2, 3, 4)).chunk(-1)
        with tempfile.TemporaryDirectory() as tmpdir:
            repo = Repository.create(local_filesystem_storage(tmpdir))
            session = repo.writable_session("main")

            to_icechunk(ds, session=session, mode="w")
            with pytest.raises(ValueError, match="Please commit first"):
                to_icechunk(ds, session=session, mode="w")
            # Needed because we can't pickle the writable session for distributed read
            session.commit("wrote a commit.")
            with xr.open_zarr(session.store, consolidated=False) as actual:
                assert_identical(actual, ds)

            newds = ds + 1
            session = repo.writable_session("main")
            to_icechunk(newds, session=session, mode="w")
            # Needed because we can't pickle the writable session
            session.commit("wrote another commit.")
            with xr.open_zarr(session.store, consolidated=False) as actual:
                assert_identical(actual, newds)
