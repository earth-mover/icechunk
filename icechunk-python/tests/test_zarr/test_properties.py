import numpy as np
from functools import partial
import pytest
from numpy.testing import assert_array_equal
from zarr.core.sync import sync
pytest.importorskip("hypothesis")

import hypothesis.extra.numpy as npst  # noqa
import hypothesis.strategies as st  # noqa
from hypothesis import given, settings  # noqa
from zarr.testing.strategies import arrays, np_arrays, basic_indices  # noqa
from icechunk import IcechunkStore, Storage

icechunk_stores = st.builds(IcechunkStore.open,
                            storage=st.builds(Storage.memory),
                            mode=st.just("w")).map(lambda x: sync(x))

from hypothesis import settings

@settings(report_multiple_bugs=False)
@given(st.data())
def test_roundtrip(data: st.DataObject) -> None:
    nparray = data.draw(np_arrays.filter(lambda x: x.dtype.kind != "M" and ">" not in x.dtype.str))
    zarray = data.draw(arrays(stores=icechunk_stores,
                              arrays=st.just(nparray)))
    assert_array_equal(nparray, zarray[:])
