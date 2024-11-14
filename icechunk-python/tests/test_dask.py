import pytest

pytest.importorskip("distributed")
from distributed import Client
from distributed.utils_test import (  # noqa: F401
    cleanup,
    cluster,
    loop,
    loop_in_thread,
)
from xarray.testing import assert_identical

from .test_xarray import create_test_data, roundtrip


def test_xarray_to_icechunk_distributed(loop):
    with cluster() as (s, [a, b]):
        with Client(s["address"], loop=loop):
            ds = create_test_data().chunk(dim1=3, dim2=4)
            with roundtrip(ds) as actual:
                assert_identical(actual, ds)
