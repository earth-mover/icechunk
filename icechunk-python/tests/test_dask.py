import pytest

pytest.importorskip("distributed")


import distributed
from xarray.testing import assert_identical

from .test_xarray import create_test_data, roundtrip


def test_something():
    with distributed.Client():
        ds = create_test_data().chunk(dim1=3, dim2=4)
        with roundtrip(ds) as actual:
            assert_identical(actual, ds)
