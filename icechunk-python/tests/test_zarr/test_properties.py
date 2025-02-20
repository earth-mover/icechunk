from typing import Any

import pytest
from numpy.testing import assert_array_equal

from icechunk import IcechunkStore, Repository, in_memory_storage

pytest.importorskip("hypothesis")

import hypothesis.strategies as st
from hypothesis import assume, given, settings

from zarr.testing.strategies import arrays, numpy_arrays

readable_characters = st.characters(
    # only use characters within the "Latin Extended-A" subset of unicode
    categories=["L", "N", "P"],
    max_codepoint=0x017F,
)
_attr_keys = st.text(readable_characters, min_size=1)
_attr_values = st.recursive(
    st.none() | st.booleans() | st.text(readable_characters, min_size=1, max_size=5),
    lambda children: st.lists(children) | st.dictionaries(_attr_keys, children),
    max_leaves=3,
)
simple_attrs = st.none()


def create() -> IcechunkStore:
    repo = Repository.create(in_memory_storage())
    return repo.writable_session("main").store


icechunk_stores = st.builds(create)


@settings(report_multiple_bugs=True, deadline=None)
@given(data=st.data(), nparray=numpy_arrays(zarr_formats=st.just(3)))
def test_roundtrip(data: st.DataObject, nparray: Any) -> None:
    # TODO: support size-0 arrays GH392
    assume(nparray.size > 0)

    zarray = data.draw(
        arrays(
            stores=icechunk_stores,
            arrays=st.just(nparray),
            zarr_formats=st.just(3),
            # TODO: use all attrs once fixed GH393
            attrs=simple_attrs,
        )
    )
    # TODO: this test sometimes break with the ShardingCodec and zarr 3.0.3
    assume(zarray.shards is None)
    assert_array_equal(nparray, zarray[:])
