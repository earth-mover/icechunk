import pytest
from icechunk import IcechunkStore, StorageConfig
from numpy.testing import assert_array_equal
from zarr.core.sync import sync

pytest.importorskip("hypothesis")

import hypothesis.extra.numpy as npst  # noqa
import hypothesis.strategies as st  # noqa
from hypothesis import given, settings  # noqa
from zarr.testing.strategies import arrays, numpy_arrays, basic_indices  # noqa

icechunk_stores = st.builds(
    IcechunkStore.open,
    storage=st.builds(StorageConfig.memory),
    prefix=st.just("prefix"),
    mode=st.just("w"),
).map(lambda x: sync(x))


@settings(report_multiple_bugs=False)
@given(data=st.data(), nparray=numpy_arrays(zarr_formats=st.just(3)))
def test_roundtrip(data: st.DataObject, nparray) -> None:
    zarray = data.draw(
        arrays(stores=icechunk_stores, arrays=st.just(nparray), zarr_formats=st.just(3))
    )
    assert_array_equal(nparray, zarray[:])
