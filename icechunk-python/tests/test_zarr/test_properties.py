from typing import Any

import pytest
from numpy.testing import assert_array_equal

from icechunk import IcechunkStore, Repository, SpecVersion, in_memory_storage

pytest.importorskip("hypothesis")
import hypothesis.strategies as st
from hypothesis import given, settings

from zarr.testing.strategies import arrays, numpy_arrays

# TODO: use a version guard instead, once the appropriate Zarr release is out.
try:
    from zarr.testing.strategies import (  # type: ignore[attr-defined, unused-ignore]
        complex_chunked_arrays,
    )

    supports_rectilinear_chunk_grids = True

except ImportError:
    supports_rectilinear_chunk_grids = False


def create(spec_version: SpecVersion | int | None) -> IcechunkStore:
    repo = Repository.create(in_memory_storage(), spec_version=spec_version)
    return repo.writable_session("main").store


@st.composite
def icechunk_stores(
    draw: st.DrawFn,
    spec_version: st.SearchStrategy[SpecVersion | int | None] = st.sampled_from(
        [None, SpecVersion.v1dot0, SpecVersion.v2dot0]
    ),
) -> IcechunkStore:
    return create(spec_version=draw(spec_version))


@settings(report_multiple_bugs=True, deadline=None, max_examples=300)
@given(
    data=st.data(),
    nparray=numpy_arrays(),
    spec_version=st.sampled_from([None, SpecVersion.v1dot0, SpecVersion.v2dot0]),
)
def test_roundtrip(
    data: st.DataObject, nparray: Any, spec_version: SpecVersion | int | None
) -> None:
    zarray = data.draw(
        arrays(
            stores=icechunk_stores(spec_version=st.just(spec_version)),
            arrays=st.just(nparray),
            zarr_formats=st.just(3),
        )
    )
    assert_array_equal(nparray, zarray[:])


# FIXME: add indexing property tests too


@pytest.mark.skipif(
    not supports_rectilinear_chunk_grids, reason="rectilinear chunk grids are unsupported"
)
@settings(report_multiple_bugs=True, deadline=None, max_examples=300)
@given(
    data=st.data(),
    spec_version=st.sampled_from([None, SpecVersion.v1dot0, SpecVersion.v2dot0]),
)
def test_roundtrip_complex_chunk_grids(
    data: st.DataObject, spec_version: SpecVersion | int | None
) -> None:
    nparray, zarray = data.draw(
        complex_chunked_arrays(
            stores=icechunk_stores(spec_version=st.just(spec_version)),
        )
    )
    assert_array_equal(nparray, zarray[:])
