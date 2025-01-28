import pytest

pytest.importorskip("dask")
pytest.importorskip("distributed")

import dask
import distributed
import xarray as xr
from icechunk import Repository, local_filesystem_storage
from icechunk.xarray import to_icechunk
from tests.test_xarray import create_test_data, roundtrip
from xarray.testing import assert_identical


@pytest.mark.parametrize("compute", [False, True])
def test_distributed(compute: bool) -> None:
    with distributed.Client():  # type: ignore [no-untyped-call]
        ds = create_test_data().chunk(dim1=3, dim2=4)
        with roundtrip(ds, compute=compute) as actual:
            assert_identical(actual, ds)

        # with pytest.raises(ValueError, match="Session cannot be serialized"):
        #     with roundtrip(ds, allow_distributed_write=False) as actual:
        #         pass


def test_distributed_workflow(tmpdir):
    repo = Repository.create(
        storage=local_filesystem_storage(tmpdir),
    )
    with distributed.Client():  # type: ignore [no-untyped-call]
        ds = create_test_data().chunk(dim1=3, dim2=4)
        session = repo.writable_session("main")
        future = to_icechunk(ds, session.store, compute=False, mode="w")

        session = repo.writable_session("main")
        (new_session,) = dask.compute(future)
        session.merge(new_session)
        session.commit("foo")

        roundtripped = xr.open_zarr(
            repo.readonly_session(branch="main").store, consolidated=False
        )
        assert_identical(roundtripped, ds)


@pytest.mark.parametrize("compute", [False, True])
def test_threaded(compute: bool) -> None:
    with dask.config.set(scheduler="threads"):
        ds = create_test_data().chunk(dim1=3, dim2=4)
        with roundtrip(ds, compute=compute) as actual:
            assert_identical(actual, ds)
        # with roundtrip(ds, allow_distributed_write=False) as actual:
        #     assert_identical(actual, ds)
