from typing import Any

import pytest
from numpy.testing import assert_array_equal
from packaging.version import Version

from icechunk import IcechunkStore, Repository, in_memory_storage

pytest.importorskip("hypothesis")
import hypothesis.strategies as st
from hypothesis import assume, given, settings

import zarr
from zarr.testing.strategies import arrays, numpy_arrays


def create() -> IcechunkStore:
    repo = Repository.create(in_memory_storage())
    return repo.writable_session("main").store


icechunk_stores = st.builds(create)


@settings(report_multiple_bugs=True, deadline=None)
@given(data=st.data(), nparray=numpy_arrays())
def test_roundtrip(data: st.DataObject, nparray: Any) -> None:
    # TODO: support size-0 arrays GH392
    assume(nparray.size > 0)

    # Skip bytes, unicode string, and datetime dtypes with zarr < 3.1.0
    # These have codec/dtype issues that were fixed in 3.1.0's dtype refactor
    if Version(zarr.__version__) < Version("3.1.0"):
        assume(nparray.dtype.kind not in ("S", "U", "M", "m"))

    zarray = data.draw(
        arrays(
            stores=icechunk_stores,
            arrays=st.just(nparray),
            zarr_formats=st.just(3),
        )
    )
    assert_array_equal(nparray, zarray[:])
