# Port of tests from Zarr-Python that are useful for icechunk
import json
import math
import pickle
from pathlib import Path
from typing import Any, Literal

import numpy as np
import pytest

from icechunk import IcechunkStore
from tests.conftest import parse_repo
from zarr import Group, create_array
from zarr.core.buffer import default_buffer_prototype
from zarr.core.common import ZarrFormat
from zarr.errors import ContainsArrayError, ContainsGroupError
from zarr.storage import StorePath


# @pytest.fixture(params=["local"])
@pytest.fixture
def store(request: pytest.FixtureRequest, tmpdir: Path) -> IcechunkStore:
    repo = parse_repo("local", str(tmpdir))
    session = repo.writable_session("main")
    return session.store


@pytest.mark.parametrize("store", ["memory"], indirect=["store"])
@pytest.mark.parametrize("zarr_format", [3])
@pytest.mark.parametrize("overwrite", [True, False])
@pytest.mark.parametrize("extant_node", ["array", "group"])
def test_array_creation_existing_node(
    store: IcechunkStore,
    zarr_format: ZarrFormat,
    overwrite: bool,
    extant_node: Literal["array", "group"],
) -> None:
    """
    Check that an existing array or group is handled as expected during array creation.
    """
    spath = StorePath(store)
    group = Group.from_store(store, zarr_format=zarr_format)
    expected_exception: type[ContainsArrayError] | type[ContainsGroupError]
    if extant_node == "array":
        expected_exception = ContainsArrayError
        _ = group.create_array("extant", shape=(10,), dtype="uint8")
    elif extant_node == "group":
        expected_exception = ContainsGroupError
        _ = group.create_group("extant")
    else:
        raise AssertionError

    new_shape = (2, 2)
    new_dtype = "float32"

    if overwrite:
        # This is currently not supported by IcechunkStore
        arr_new = create_array(
            spath / "extant",
            shape=new_shape,
            dtype=new_dtype,
            overwrite=overwrite,
            zarr_format=zarr_format,
        )
        assert arr_new.shape == new_shape
        assert arr_new.dtype == new_dtype
    else:
        with pytest.raises(expected_exception):
            create_array(
                spath / "extant",
                shape=new_shape,
                dtype=new_dtype,
                overwrite=overwrite,
                zarr_format=zarr_format,
            )


### We should test serializability in many ways
@pytest.mark.skip("icechunk requires opting-in to pickling at the session level")
@pytest.mark.parametrize("store", ["local"], indirect=["store"])
@pytest.mark.parametrize("zarr_format", [3])
def test_serializable_sync_array(store: IcechunkStore, zarr_format: ZarrFormat) -> None:
    expected = create_array(
        store=store, shape=(100,), chunks=(10,), zarr_format=zarr_format, dtype="i4"
    )
    expected[:] = list(range(100))

    p = pickle.dumps(expected)
    actual = pickle.loads(p)

    # pickled stores dont point to the same session instance, so they are not equal
    assert actual != expected
    np.testing.assert_array_equal(actual[:], expected[:])


### We should definitely test our fill_value handling since that uses custom
### serialization logic
@pytest.mark.parametrize("store", ["memory"], indirect=True)
@pytest.mark.parametrize(
    ("dtype_str", "fill_value"),
    [("bool", True), ("uint8", 99), ("float32", -99.9), ("complex64", 3 + 4j)],
)
def test_array_v3_fill_value(
    store: IcechunkStore, fill_value: int, dtype_str: str
) -> None:
    shape = (10,)
    arr = create_array(
        store=store,
        shape=shape,
        dtype=dtype_str,
        zarr_format=3,
        chunks=shape,
        fill_value=fill_value,
    )

    assert arr.fill_value == np.dtype(dtype_str).type(fill_value)
    assert arr.fill_value.dtype == arr.dtype


@pytest.mark.parametrize("store", ["memory"], indirect=True)
async def test_array_v3_nan_fill_value(store: IcechunkStore) -> None:
    shape = (10,)
    arr = create_array(
        store=store,
        shape=shape,
        dtype=np.float64,
        zarr_format=3,
        chunks=shape,
        fill_value=np.nan,
    )
    arr[:] = np.nan

    assert np.isnan(arr.fill_value)
    assert arr.fill_value.dtype == arr.dtype
    # # all fill value chunk is an empty chunk, and should not be written
    # assert len([a async for a in store.list_prefix("/")]) == 0


@pytest.mark.parametrize(
    ("fill_value", "expected"),
    [
        (np.nan * 1j, ["NaN", "NaN"]),
        (np.nan, ["NaN", 0.0]),
        (np.inf, ["Infinity", 0.0]),
        (np.inf * 1j, ["NaN", "Infinity"]),
        (-np.inf, ["-Infinity", 0.0]),
        (math.inf, ["Infinity", 0.0]),
    ],
)
@pytest.mark.parametrize("store", ["memory"], indirect=True)
async def test_special_complex_fill_values_roundtrip(
    store: IcechunkStore, fill_value: Any, expected: list[Any]
) -> None:
    create_array(store=store, shape=(1,), dtype=np.complex64, fill_value=fill_value)
    content = await store.get("zarr.json", prototype=default_buffer_prototype())
    assert content is not None
    actual = json.loads(content.to_bytes())
    assert actual
    pytest.xfail("IcechunkStore does not support complex fill types")
    # assert actual["fill_value"] == expected
