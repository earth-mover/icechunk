from typing import Literal

from icechunk import IcechunkStore
import numpy as np
import pytest

from zarr import Array, AsyncGroup, Group
import zarr
import zarr.api
import zarr.api.asynchronous
from zarr.core.common import ZarrFormat
from zarr.errors import ContainsArrayError, ContainsGroupError
from zarr.store.common import StorePath


@pytest.mark.parametrize("store", ["memory"], indirect=["store"])
@pytest.mark.parametrize("zarr_format", [3])
@pytest.mark.parametrize("exists_ok", [True, False])
@pytest.mark.parametrize("extant_node", ["array", "group"])
def test_array_creation_existing_node(
    store: IcechunkStore,
    zarr_format: ZarrFormat,
    exists_ok: bool,
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

    if exists_ok:
        # This is currently not supported by IcechunkStore
        pytest.xfail("IcechunkStore does not support exists_ok=True")
        arr_new = Array.create(
            spath / "extant",
            shape=new_shape,
            dtype=new_dtype,
            exists_ok=exists_ok,
            zarr_format=zarr_format,
        )
        assert arr_new.shape == new_shape
        assert arr_new.dtype == new_dtype
    else:
        with pytest.raises(expected_exception):
            arr_new = Array.create(
                spath / "extant",
                shape=new_shape,
                dtype=new_dtype,
                exists_ok=exists_ok,
                zarr_format=zarr_format,
            )


@pytest.mark.parametrize("store", ["memory"], indirect=["store"])
@pytest.mark.parametrize("zarr_format", [3])
async def test_create_creates_parents(
    store: IcechunkStore, zarr_format: ZarrFormat
) -> None:
    # prepare a root node, with some data set
    await zarr.api.asynchronous.open_group(
        store=store, path="a", zarr_format=zarr_format, attributes={"key": "value"}
    )

    # create a child node with a couple intermediates
    await zarr.api.asynchronous.create(
        shape=(2, 2), store=store, path="a/b/c/d", zarr_format=zarr_format
    )
    parts = ["a", "a/b", "a/b/c"]

    if zarr_format == 2:
        files = [".zattrs", ".zgroup"]
    else:
        files = ["zarr.json"]

    expected = [f"{part}/{file}" for file in files for part in parts]

    if zarr_format == 2:
        expected.append("a/b/c/d/.zarray")
        expected.append("a/b/c/d/.zattrs")
    else:
        expected.append("a/b/c/d/zarr.json")

    expected = sorted(expected)

    result = sorted([x async for x in store.list_prefix("")])

    assert result == expected

    paths = ["a", "a/b", "a/b/c"]
    for path in paths:
        g = await zarr.api.asynchronous.open_group(store=store, path=path)
        assert isinstance(g, AsyncGroup)


@pytest.mark.parametrize("store", ["memory"], indirect=["store"])
@pytest.mark.parametrize("zarr_format", [3])
def test_array_name_properties_no_group(
    store: IcechunkStore, zarr_format: ZarrFormat
) -> None:
    arr = Array.create(
        store=store, shape=(100,), chunks=(10,), zarr_format=zarr_format, dtype="i4"
    )
    assert arr.path == ""
    assert arr.name is None
    assert arr.basename is None


@pytest.mark.parametrize("store", ["memory"], indirect=["store"])
@pytest.mark.parametrize("zarr_format", [3])
def test_array_name_properties_with_group(
    store: IcechunkStore, zarr_format: ZarrFormat
) -> None:
    root = Group.from_store(store=store, zarr_format=zarr_format)
    foo = root.create_array("foo", shape=(100,), chunks=(10,), dtype="i4")
    assert foo.path == "foo"
    assert foo.name == "/foo"
    assert foo.basename == "foo"

    bar = root.create_group("bar")
    spam = bar.create_array("spam", shape=(100,), chunks=(10,), dtype="i4")

    assert spam.path == "bar/spam"
    assert spam.name == "/bar/spam"
    assert spam.basename == "spam"


@pytest.mark.parametrize("store", ["memory"], indirect=True)
@pytest.mark.parametrize("specifiy_fill_value", [True, False])
@pytest.mark.parametrize("dtype_str", ["bool", "uint8", "complex64"])
def test_array_v3_fill_value_default(
    store: IcechunkStore, specifiy_fill_value: bool, dtype_str: str
) -> None:
    """
    Test that creating an array with the fill_value parameter set to None, or unspecified,
    results in the expected fill_value attribute of the array, i.e. 0 cast to the array's dtype.
    """
    shape = (10,)
    default_fill_value = 0
    if specifiy_fill_value:
        arr = Array.create(
            store=store,
            shape=shape,
            dtype=dtype_str,
            zarr_format=3,
            chunk_shape=shape,
            fill_value=None,
        )
    else:
        arr = Array.create(
            store=store, shape=shape, dtype=dtype_str, zarr_format=3, chunk_shape=shape
        )

    assert arr.fill_value == np.dtype(dtype_str).type(default_fill_value)
    assert arr.fill_value.dtype == arr.dtype


@pytest.mark.parametrize("store", ["memory"], indirect=True)
@pytest.mark.parametrize(
    ("dtype_str", "fill_value"),
    [("bool", True), ("uint8", 99), ("float32", -99.9), ("complex64", 3 + 4j)],
)
def test_array_v3_fill_value(
    store: IcechunkStore, fill_value: int, dtype_str: str
) -> None:
    shape = (10,)
    arr = Array.create(
        store=store,
        shape=shape,
        dtype=dtype_str,
        zarr_format=3,
        chunk_shape=shape,
        fill_value=fill_value,
    )

    assert arr.fill_value == np.dtype(dtype_str).type(fill_value)
    assert arr.fill_value.dtype == arr.dtype
