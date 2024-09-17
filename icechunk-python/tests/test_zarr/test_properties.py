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
import icechunk

icechunk_stores = st.builds(icechunk.IcechunkStore.from_json,
                            config=st.just({"storage": {"type": "in_memory"}, "dataset": {}}),
                            mode=st.just("w")).map(lambda x: sync(x))

@given(st.data())
def test_roundtrip(data: st.DataObject) -> None:
    nparray = data.draw(np_arrays)
    zarray = data.draw(arrays(stores=icechunk_stores,
                              arrays=st.just(nparray)))
    assert_array_equal(nparray, zarray[:])
