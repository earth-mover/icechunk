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
        with roundtrip(ds, allow_pickling=True) as actual:
            assert_identical(actual, ds)

        # FIXME: this should be nicer! this TypeError is from distributed
        with pytest.raises(TypeError):
            with roundtrip(ds, allow_pickling=False) as actual:
                pass


def test_threaded() -> None:
    with dask.config.set(scheduler="threads"):
        ds = create_test_data().chunk(dim1=3, dim2=4)
        with roundtrip(ds) as actual:
            assert_identical(actual, ds)
        with roundtrip(ds, allow_pickling=False) as actual:
            assert_identical(actual, ds)
