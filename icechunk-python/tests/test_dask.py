import pytest

pytest.importorskip("distributed")
from distributed import Client
from distributed.utils_test import (  # noqa: F401
    client,
    loop,
    loop_in_thread,
    cleanup,
    cluster_fixture,
)
from xarray.testing import assert_identical

from .test_xarray import create_test_data, roundtrip


def test_xarray_to_icechunk_distributed(client):
    ds = create_test_data().chunk(dim1=3, dim2=4)
    with roundtrip(ds) as actual:
        assert_identical(actual, ds)
