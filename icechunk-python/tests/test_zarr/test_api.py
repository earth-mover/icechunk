import pathlib
from typing import Literal

import numpy as np
import pytest
from numpy.testing import assert_array_equal

import zarr
from icechunk import IcechunkStore
from tests.conftest import parse_store
from zarr import Array, Group, group
from zarr.abc.store import Store
from zarr.api.synchronous import (
    create,
    load,
    open,
    open_group,
    save,
    save_array,
    save_group,
)
from zarr.core.common import MemoryOrder, ZarrFormat
from zarr.storage._utils import normalize_path


@pytest.fixture(scope="function")
def memory_store() -> IcechunkStore:
    return parse_store("memory", "")


def test_create_array(memory_store: Store) -> None:
    store = memory_store

    # create array
    z = create(shape=100, store=store)
    assert isinstance(z, Array)
    assert z.shape == (100,)

    # create array, overwrite, specify chunk shape
    z = create(shape=200, chunk_shape=20, store=store, overwrite=True)
    assert isinstance(z, Array)
    assert z.shape == (200,)
    assert z.chunks == (20,)

    # create array, overwrite, specify chunk shape via chunks param
    z = create(shape=400, chunks=40, store=store, overwrite=True)
    assert isinstance(z, Array)
    assert z.shape == (400,)
    assert z.chunks == (40,)


@pytest.mark.parametrize("path", ["foo", "/", "/foo", "///foo/bar"])
@pytest.mark.parametrize("node_type", ["array", "group"])
def test_open_normalized_path(
    memory_store: IcechunkStore, path: str, node_type: Literal["array", "group"]
) -> None:
    node: Group | Array
    if node_type == "group":
        node = group(store=memory_store, path=path)
    elif node_type == "array":
        node = create(store=memory_store, path=path, shape=(2,))

    assert node.path == normalize_path(path)


def test_open_array(memory_store: IcechunkStore) -> None:
    store = memory_store

    # open array, create if doesn't exist
    z = open(store=store, shape=100)
    assert isinstance(z, Array)
    assert z.shape == (100,)

    # open array, overwrite
    # _store_dict won't currently work with IcechunkStore
    # TODO: Should it?
    pytest.xfail("IcechunkStore does not support _store_dict")
    store._store_dict = {}
    z = open(store=store, shape=200, mode="w")  # mode="w"
    assert isinstance(z, Array)
    assert z.shape == (200,)

    # open array, read-only
    store_cls = type(store)

    # _store_dict won't currently work with IcechunkStore
    # TODO: Should it?

    ro_store = store_cls.open(store_dict=store._store_dict, mode="r")
    z = open(store=ro_store)
    assert isinstance(z, Array)
    assert z.shape == (200,)
    assert z.read_only

    # path not found
    with pytest.raises(FileNotFoundError):
        open(store="doesnotexist", mode="r")


def test_open_group(memory_store: IcechunkStore) -> None:
    store = memory_store

    # open group, create if doesn't exist
    g = open_group(store=store)
    g.create_group("foo")
    assert isinstance(g, Group)
    assert "foo" in g

    # open group, overwrite
    # g = open_group(store=store)
    # assert isinstance(g, Group)
    # assert "foo" not in g

    # open group, read-only
    store_cls = type(store)
    # _store_dict won't currently work with IcechunkStore
    # TODO: Should it?
    pytest.xfail("IcechunkStore does not support _store_dict")
    ro_store = store_cls.open(store_dict=store._store_dict, mode="r")
    g = open_group(store=ro_store)
    assert isinstance(g, Group)
    # assert g.read_only


@pytest.mark.parametrize("n_args", [10, 1, 0])
@pytest.mark.parametrize("n_kwargs", [10, 1, 0])
def test_save(memory_store: IcechunkStore, n_args: int, n_kwargs: int) -> None:
    store = memory_store
    data = np.arange(10)
    args = [np.arange(10) for _ in range(n_args)]
    kwargs = {f"arg_{i}": data for i in range(n_kwargs)}

    if n_kwargs == 0 and n_args == 0:
        with pytest.raises(ValueError):
            save(store)
    elif n_args == 1 and n_kwargs == 0:
        save(store, *args)
        array = open(store)
        assert isinstance(array, Array)
        assert_array_equal(array[:], data)
    else:
        save(store, *args, **kwargs)  # type: ignore[arg-type]
        group = open(store)
        assert isinstance(group, Group)
        for array in group.array_values():
            assert_array_equal(array[:], data)
        for k in kwargs:
            assert k in group
        assert group.nmembers() == n_args + n_kwargs


def test_save_errors() -> None:
    with pytest.raises(ValueError):
        # no arrays provided
        save_group("data/group.zarr")
    with pytest.raises(TypeError):
        # no array provided
        save_array("data/group.zarr")  # type: ignore[call-arg]
    with pytest.raises(ValueError):
        # no arrays provided
        save("data/group.zarr")
    with pytest.raises(TypeError):
        # mode is no valid argument and would get handled as an array
        a = np.arange(10)
        zarr.save("data/example.zarr", a, mode="w")


def test_open_with_mode_r(tmp_path: pathlib.Path) -> None:
    # 'r' means read only (must exist)
    with pytest.raises(FileNotFoundError):
        zarr.open(store=tmp_path, mode="r")
    zarr.ones(store=tmp_path, shape=(3, 3))
    z2 = zarr.open(store=tmp_path, mode="r")
    assert isinstance(z2, Array)
    assert (z2[:] == 1).all()
    with pytest.raises(ValueError):
        z2[:] = 3


def test_open_with_mode_r_plus(tmp_path: pathlib.Path) -> None:
    # 'r+' means read/write (must exist)
    with pytest.raises(FileNotFoundError):
        zarr.open(store=tmp_path, mode="r+")
    z1 = zarr.ones(store=tmp_path, shape=(3, 3))
    assert z1.fill_value == 1
    z2 = zarr.open(store=tmp_path, mode="r+")
    assert isinstance(z2, Array)
    assert z2.fill_value == 1
    assert (z2[:] == 1).all()
    z2[:] = 3


async def test_open_with_mode_a(tmp_path: pathlib.Path) -> None:
    # Open without shape argument should default to group
    g = zarr.open(store=tmp_path, mode="a")
    assert isinstance(g, Group)
    await g.store_path.delete()

    # 'a' means read/write (create if doesn't exist)
    arr = zarr.open(store=tmp_path, mode="a", shape=(3, 3))
    assert isinstance(arr, Array)
    arr[...] = 1
    z2 = zarr.open(store=tmp_path, mode="a")
    assert isinstance(z2, Array)
    assert (z2[:] == 1).all()
    z2[:] = 3


def test_open_with_mode_w(tmp_path: pathlib.Path) -> None:
    # 'w' means create (overwrite if exists);
    arr = zarr.open(store=tmp_path, mode="w", shape=(3, 3))
    assert isinstance(arr, Array)

    arr[...] = 3
    z2 = zarr.open(store=tmp_path, mode="w", shape=(3, 3))
    assert isinstance(z2, Array)
    assert not (z2[:] == 3).all()
    z2[:] = 3


def test_open_with_mode_w_minus(tmp_path: pathlib.Path) -> None:
    # 'w-' means create  (fail if exists)
    arr = zarr.open(store=tmp_path, mode="w-", shape=(3, 3))
    assert isinstance(arr, Array)
    arr[...] = 1
    with pytest.raises(FileExistsError):
        zarr.open(store=tmp_path, mode="w-")


# def test_lazy_loader():
#     foo = np.arange(100)
#     bar = np.arange(100, 0, -1)
#     store = "data/group.zarr"
#     save(store, foo=foo, bar=bar)
#     loader = load(store)
#     assert "foo" in loader
#     assert "bar" in loader
#     assert "baz" not in loader
#     assert len(loader) == 2
#     assert sorted(loader) == ["bar", "foo"]
#     assert_array_equal(foo, loader["foo"])
#     assert_array_equal(bar, loader["bar"])
#     assert "LazyLoader: " in repr(loader)


@pytest.mark.parametrize("order", ["C", "F", None])
@pytest.mark.parametrize("zarr_format", [2, 3])
def test_array_order(order: MemoryOrder | None, zarr_format: ZarrFormat) -> None:
    arr = zarr.ones(shape=(2, 2), order=order, zarr_format=zarr_format)
    expected = order or zarr.config.get("array.order")
    assert arr.order == expected

    vals = np.asarray(arr)
    if expected == "C":
        assert vals.flags.c_contiguous
    elif expected == "F":
        assert vals.flags.f_contiguous
    else:
        raise AssertionError


def test_load_array(memory_store: Store) -> None:
    store = memory_store
    foo = np.arange(100)
    bar = np.arange(100, 0, -1)
    save(store, foo=foo, bar=bar)

    # can also load arrays directly into a numpy array
    for array_name in ["foo", "bar"]:
        array = load(store, path=array_name)
        assert isinstance(array, np.ndarray)
        if array_name == "foo":
            assert_array_equal(foo, array)
        else:
            assert_array_equal(bar, array)


def test_tree() -> None:
    g1 = zarr.group()
    g1.create_group("foo")
    g3 = g1.create_group("bar")
    g3.create_group("baz")
    g5 = g3.create_group("qux")
    g5.create_array("baz", shape=100, chunks=10)
    # TODO: complete after tree has been reimplemented
    # assert repr(zarr.tree(g1)) == repr(g1.tree())
    # assert str(zarr.tree(g1)) == str(g1.tree())
