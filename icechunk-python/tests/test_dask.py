import pytest

pytest.importorskip("distributed")


import distributed
from tests.test_xarray import create_test_data, roundtrip
from xarray.testing import assert_identical


def test_something():
    with distributed.Client():
        ds = create_test_data().chunk(dim1=3, dim2=4)
        with roundtrip(ds, preserve_read_only=True) as actual:
            assert_identical(actual, ds)

        with pytest.raises(ValueError, match="read-only store"):
            with roundtrip(ds, preserve_read_only=False) as actual:
                pass
