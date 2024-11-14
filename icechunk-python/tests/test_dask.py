import pytest

pytest.importorskip("distributed")
from .test_xarray import create_test_data, roundtrip
from distributed import Client
from distributed.utils_test import (  # noqa
    # need to import all these fixtures
    cluster,
    loop,
    loop_in_thread,
    cleanup,
)

from xarray.testing import assert_identical


def test_xarray_to_icechunk_distributed(loop):
    with cluster() as (s, [a, b]):
        with Client(s["address"], loop=loop):
            ds = create_test_data().chunk(dim1=3, dim2=4)
            with roundtrip(ds) as actual:
                assert_identical(actual, ds)
