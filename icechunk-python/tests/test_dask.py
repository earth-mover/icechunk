import pytest

pytest.importorskip("dask")
pytest.importorskip("distributed")

import dask
import distributed
from tests.test_xarray import create_test_data, roundtrip
from xarray.testing import assert_identical


def test_distributed() -> None:
    with distributed.Client():  # type: ignore [no-untyped-call]
        ds = create_test_data().chunk(dim1=3, dim2=4)
        with roundtrip(ds) as actual:
            assert_identical(actual, ds)

        # with pytest.raises(ValueError, match="Session cannot be serialized"):
        #     with roundtrip(ds, allow_distributed_write=False) as actual:
        #         pass


def test_threaded() -> None:
    with dask.config.set(scheduler="threads"):
        ds = create_test_data().chunk(dim1=3, dim2=4)
        with roundtrip(ds) as actual:
            assert_identical(actual, ds)
        # with roundtrip(ds, allow_distributed_write=False) as actual:
        #     assert_identical(actual, ds)
