import pytest

pytest.importorskip("distributed")
from distributed.utils_test import gen_cluster

from xarray.testing import assert_identical

from .test_xarray import create_test_data, roundtrip


@gen_cluster(client=True)
async def test_something(c, s, *ws):
    ds = create_test_data().chunk(dim1=3, dim2=4)
    with roundtrip(ds) as actual:
        assert_identical(actual, ds)
