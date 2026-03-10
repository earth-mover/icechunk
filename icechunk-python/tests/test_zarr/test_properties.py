from typing import Any

import pytest
from numpy.testing import assert_array_equal

from icechunk import IcechunkStore, Repository, in_memory_storage

pytest.importorskip("hypothesis")
import hypothesis.strategies as st
from hypothesis import assume, given, settings

from zarr.testing.strategies import arrays, complex_chunked_arrays, numpy_arrays


def create(spec_version: int | None) -> IcechunkStore:
    repo = Repository.create(in_memory_storage(), spec_version=spec_version)
    return repo.writable_session("main").store


@st.composite
def icechunk_stores(
    draw: st.DrawFn,
    spec_version: st.SearchStrategy[int | None] = st.sampled_from([None, 1, 2]),  # noqa: B008
) -> IcechunkStore:
    return create(spec_version=draw(spec_version))


@settings(report_multiple_bugs=True, deadline=None, max_examples=300)
@given(data=st.data(), nparray=numpy_arrays(), spec_version=st.sampled_from([None, 1, 2]))
def test_roundtrip(data: st.DataObject, nparray: Any, spec_version: int | None) -> None:
    # TODO: support size-0 arrays GH392
    assume(nparray.size > 0)

    zarray = data.draw(
        arrays(
            stores=icechunk_stores(spec_version=st.just(spec_version)),
            arrays=st.just(nparray),
            zarr_formats=st.just(3),
        )
    )
    assert_array_equal(nparray, zarray[:])


# FIXME: add indexing property tests too


@settings(report_multiple_bugs=True, deadline=None, max_examples=300)
@given(data=st.data(), nparray=numpy_arrays(), spec_version=st.sampled_from([None, 1, 2]))
def test_roundtrip_complex_chunk_grids(
    data: st.DataObject, nparray: Any, spec_version: int | None
) -> None:
    # FIXME: support return nparray too so we can do equality comparisons?
    # FIXME: Or... figure out a way to pass in arrays.
    zarray = data.draw(
        complex_chunked_arrays(
            stores=icechunk_stores(spec_version=st.just(spec_version)),
        )
    )

    assert_array_equal(nparray, zarray[:])
