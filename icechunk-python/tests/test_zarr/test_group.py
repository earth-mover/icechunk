from __future__ import annotations

import operator
from typing import TYPE_CHECKING, Any, Literal, cast

import numpy as np
import pytest

import zarr
import zarr.api
import zarr.api.asynchronous
from icechunk import IcechunkStore
from tests.conftest import parse_store
from zarr import Array, AsyncArray, AsyncGroup, Group
from zarr.core._info import GroupInfo
from zarr.core.buffer import default_buffer_prototype
from zarr.core.common import JSON, ZarrFormat
from zarr.core.group import GroupMetadata
from zarr.core.sync import sync
from zarr.errors import ContainsArrayError, ContainsGroupError
from zarr.storage import StorePath, make_store_path

if TYPE_CHECKING:
    from _pytest.compat import LEGACY_PATH


@pytest.fixture(params=["memory"])
def store(request: pytest.FixtureRequest, tmpdir: LEGACY_PATH) -> IcechunkStore:
    result = parse_store(request.param, str(tmpdir))
    if not isinstance(result, IcechunkStore):
        raise TypeError(
            "Wrong store class returned by test fixture! got " + result + " instead"
        )
    return result


@pytest.fixture(params=[True, False])
def exists_ok(request: pytest.FixtureRequest) -> bool:
    result = request.param
    if not isinstance(result, bool):
        raise TypeError("Wrong type returned by test fixture.")
    return result


@pytest.fixture(params=[3], ids=["zarr3"])
def zarr_format(request: pytest.FixtureRequest) -> ZarrFormat:
    result = request.param
    if result not in [3]:
        raise ValueError("Wrong value returned from test fixture.")
    return cast(ZarrFormat, result)


def test_group_init(store: IcechunkStore, zarr_format: ZarrFormat) -> None:
    """
    Test that initializing a group from an asyncgroup works.
    """
    agroup = sync(AsyncGroup.from_store(store=store, zarr_format=zarr_format))
    group = Group(agroup)
    assert group._async_group == agroup


async def test_create_creates_parents(
    store: IcechunkStore, zarr_format: ZarrFormat
) -> None:
    # prepare a root node, with some data set
    await zarr.api.asynchronous.open_group(
        store=store, path="a", zarr_format=zarr_format, attributes={"key": "value"}
    )
    objs = {x async for x in store.list()}
    if zarr_format == 2:
        assert objs == {".zgroup", ".zattrs", "a/.zgroup", "a/.zattrs"}
    else:
        assert objs == {"zarr.json", "a/zarr.json"}

    # test that root group node was created
    root = await zarr.api.asynchronous.open_group(
        store=store,
    )

    agroup = await root.getitem("a")
    assert agroup.attrs == {"key": "value"}

    # create a child node with a couple intermediates
    await zarr.api.asynchronous.open_group(
        store=store, path="a/b/c/d", zarr_format=zarr_format
    )
    parts = ["a", "a/b", "a/b/c"]

    if zarr_format == 2:
        files = [".zattrs", ".zgroup"]
    else:
        files = ["zarr.json"]

    expected = [f"{part}/{file}" for file in files for part in parts]

    if zarr_format == 2:
        expected.extend([".zgroup", ".zattrs", "a/b/c/d/.zgroup", "a/b/c/d/.zattrs"])
    else:
        expected.extend(["zarr.json", "a/b/c/d/zarr.json"])

    expected = sorted(expected)

    result = sorted([x async for x in store.list_prefix("")])

    assert result == expected

    paths = ["a", "a/b", "a/b/c"]
    for path in paths:
        g = await zarr.api.asynchronous.open_group(store=store, path=path)
        assert isinstance(g, AsyncGroup)

        if path == "a":
            # ensure we didn't overwrite the root attributes
            assert g.attrs == {"key": "value"}
        else:
            assert g.attrs == {}


def test_group_name_properties(store: IcechunkStore, zarr_format: ZarrFormat) -> None:
    """
    Test basic properties of groups
    """
    root = Group.from_store(store=store, zarr_format=zarr_format)
    assert root.path == ""
    assert root.name == "/"
    assert root.basename == ""

    foo = root.create_group("foo")
    assert foo.path == "foo"
    assert foo.name == "/foo"
    assert foo.basename == "foo"

    bar = root.create_group("foo/bar")
    assert bar.path == "foo/bar"
    assert bar.name == "/foo/bar"
    assert bar.basename == "bar"


def test_group_members(store: IcechunkStore, zarr_format: ZarrFormat) -> None:
    """
    Test that `Group.members` returns correct values, i.e. the arrays and groups
    (explicit and implicit) contained in that group.
    """

    path = "group"
    group = Group.from_store(store=store, zarr_format=zarr_format)
    members_expected: dict[str, Array | Group] = {}

    members_expected["subgroup"] = group.create_group("subgroup")
    # make a sub-sub-subgroup, to ensure that the children calculation doesn't go
    # too deep in the hierarchy
    subsubgroup = cast(Group, members_expected["subgroup"]).create_group("subsubgroup")
    subsubsubgroup = subsubgroup.create_group("subsubsubgroup")

    members_expected["subarray"] = group.create_array(
        "subarray", shape=(100,), dtype="uint8", chunk_shape=(10,), exists_ok=True
    )

    # This is not supported by Icechunk, so we expect an error
    # zarr-python: add an extra object to the domain of the group.
    # the list of children should ignore this object.
    with pytest.raises(ValueError):
        sync(
            store.set(
                f"{path}/extra_object-1",
                default_buffer_prototype().buffer.from_bytes(b"000000"),
            )
        )

    # This is not supported by Icechunk, so we expect an error
    # zarr-python: add an extra object under a directory-like prefix in the domain of the group.
    # this creates a directory with a random key in it
    # this should not show up as a member
    with pytest.raises(ValueError):
        sync(
            store.set(
                f"{path}/extra_directory/extra_object-2",
                default_buffer_prototype().buffer.from_bytes(b"000000"),
            )
        )
    members_observed = group.members()
    # members are not guaranteed to be ordered, so sort before comparing
    assert sorted(dict(members_observed)) == sorted(members_expected)

    # partial
    members_observed = group.members(max_depth=1)
    members_expected["subgroup/subsubgroup"] = subsubgroup
    # members are not guaranteed to be ordered, so sort before comparing
    assert sorted(dict(members_observed)) == sorted(members_expected)

    # total
    members_observed = group.members(max_depth=None)
    members_expected["subgroup/subsubgroup/subsubsubgroup"] = subsubsubgroup
    # members are not guaranteed to be ordered, so sort before comparing
    assert sorted(dict(members_observed)) == sorted(members_expected)

    with pytest.raises(ValueError, match="max_depth"):
        members_observed = group.members(max_depth=-1)


def test_group(store: IcechunkStore, zarr_format: ZarrFormat) -> None:
    """
    Test basic Group routines.
    """
    store_path = StorePath(store)
    agroup = AsyncGroup(
        metadata=GroupMetadata(zarr_format=zarr_format), store_path=store_path
    )
    group = Group(agroup)
    assert agroup.metadata is group.metadata
    assert agroup.store_path == group.store_path == store_path

    # create two groups
    foo = group.create_group("foo")
    bar = foo.create_group("bar", attributes={"baz": "qux"})

    # create an array from the "bar" group
    data = np.arange(0, 4 * 4, dtype="uint16").reshape((4, 4))
    arr = bar.create_array(
        "baz", shape=data.shape, dtype=data.dtype, chunk_shape=(2, 2), exists_ok=True
    )
    arr[:] = data

    # check the array
    assert arr == bar["baz"]
    assert arr.shape == data.shape
    assert arr.dtype == data.dtype

    # TODO: update this once the array api settles down
    assert arr.chunks == (2, 2)

    bar2 = foo["bar"]
    assert dict(bar2.attrs) == {"baz": "qux"}

    # update a group's attributes
    bar2.attrs.update({"name": "bar"})
    # bar.attrs was modified in-place
    assert dict(bar2.attrs) == {"baz": "qux", "name": "bar"}

    # and the attrs were modified in the store
    bar3 = foo["bar"]
    assert dict(bar3.attrs) == {"baz": "qux", "name": "bar"}


def test_group_create(
    store: IcechunkStore, exists_ok: bool, zarr_format: ZarrFormat
) -> None:
    """
    Test that `Group.create` works as expected.
    """
    attributes = {"foo": 100}
    group = Group.from_store(
        store, attributes=attributes, zarr_format=zarr_format, exists_ok=exists_ok
    )

    assert group.attrs == attributes

    if not exists_ok:
        with pytest.raises(ContainsGroupError):
            _ = Group.from_store(store, exists_ok=exists_ok, zarr_format=zarr_format)


def test_group_open(
    store: IcechunkStore, zarr_format: ZarrFormat, exists_ok: bool
) -> None:
    """
    Test the `Group.open` method.
    """
    spath = StorePath(store)
    # attempt to open a group that does not exist
    with pytest.raises(FileNotFoundError):
        Group.open(store)

    # create the group
    attrs = {"path": "foo"}
    group_created = Group.from_store(
        store, attributes=attrs, zarr_format=zarr_format, exists_ok=exists_ok
    )
    assert group_created.attrs == attrs
    assert group_created.metadata.zarr_format == zarr_format
    assert group_created.store_path == spath

    # attempt to create a new group in place, to test exists_ok
    new_attrs = {"path": "bar"}
    if not exists_ok:
        with pytest.raises(ContainsGroupError):
            Group.from_store(
                store, attributes=attrs, zarr_format=zarr_format, exists_ok=exists_ok
            )
    else:
        if not store.supports_deletes:
            pytest.skip(
                "Store does not support deletes but `exists_ok` is True, requiring deletes to override a group"
            )
        group_created_again = Group.from_store(
            store, attributes=new_attrs, zarr_format=zarr_format, exists_ok=exists_ok
        )
        assert group_created_again.attrs == new_attrs
        assert group_created_again.metadata.zarr_format == zarr_format
        assert group_created_again.store_path == spath


def test_group_getitem(store: IcechunkStore, zarr_format: ZarrFormat) -> None:
    """
    Test the `Group.__getitem__` method.
    """

    group = Group.from_store(store, zarr_format=zarr_format)
    subgroup = group.create_group(name="subgroup")
    subarray = group.create_array(name="subarray", shape=(10,), chunk_shape=(10,))

    assert group["subgroup"] == subgroup
    assert group["subarray"] == subarray
    with pytest.raises(KeyError):
        group["nope"]


def test_group_get_with_default(store: IcechunkStore, zarr_format: ZarrFormat) -> None:
    group = Group.from_store(store, zarr_format=zarr_format)

    # default behavior
    result = group.get("subgroup")
    assert result is None

    # custom default
    result = group.get("subgroup", 8)
    assert result == 8

    # now with a group
    subgroup = group.require_group("subgroup")
    subgroup.attrs["foo"] = "bar"

    result = group.get("subgroup", 8)
    result = cast(Group, result)
    assert result.attrs["foo"] == "bar"


def test_group_delitem(store: IcechunkStore, zarr_format: ZarrFormat) -> None:
    """
    Test the `Group.__delitem__` method.
    """

    group = Group.from_store(store, zarr_format=zarr_format)
    subgroup = group.create_group(name="subgroup")
    subarray = group.create_array(name="subarray", shape=(10,), chunk_shape=(10,))

    assert group["subgroup"] == subgroup
    assert group["subarray"] == subarray

    del group["subgroup"]
    with pytest.raises(KeyError):
        group["subgroup"]

    del group["subarray"]
    with pytest.raises(KeyError):
        group["subarray"]


def test_group_iter(store: IcechunkStore, zarr_format: ZarrFormat) -> None:
    """
    Test the `Group.__iter__` method.
    """

    group = Group.from_store(store, zarr_format=zarr_format)
    assert list(group) == []


def test_group_len(store: IcechunkStore, zarr_format: ZarrFormat) -> None:
    """
    Test the `Group.__len__` method.
    """

    group = Group.from_store(store, zarr_format=zarr_format)
    assert len(group) == 0


def test_group_setitem(store: IcechunkStore, zarr_format: ZarrFormat) -> None:
    """
    Test the `Group.__setitem__` method.
    """
    group = Group.from_store(store, zarr_format=zarr_format)
    arr = np.ones((2, 4))
    group["key"] = arr
    assert list(group.array_keys()) == ["key"]
    assert group["key"].shape == (2, 4)
    np.testing.assert_array_equal(group["key"][:], arr)

    if store.supports_deletes:
        key = "key"
    else:
        # overwriting with another array requires deletes
        # for stores that don't support this, we just use a new key
        key = "key2"

        # overwrite with another array
        arr = np.zeros((3, 5))
        group[key] = arr
        assert key in list(group.array_keys())
        assert group[key].shape == (3, 5)
        np.testing.assert_array_equal(group[key], arr)


def test_group_contains(store: IcechunkStore, zarr_format: ZarrFormat) -> None:
    """
    Test the `Group.__contains__` method
    """
    group = Group.from_store(store, zarr_format=zarr_format)
    assert "foo" not in group
    _ = group.create_group(name="foo")
    assert "foo" in group


def test_group_subgroups(store: IcechunkStore, zarr_format: ZarrFormat) -> None:
    """
    Test the behavior of `Group` methods for accessing subgroups, namely `Group.group_keys` and `Group.groups`
    """
    group = Group.from_store(store, zarr_format=zarr_format)
    keys = ("foo", "bar")
    subgroups_expected = tuple(group.create_group(k) for k in keys)
    # create a sub-array as well
    _ = group.create_array("array", shape=(10,))
    subgroups_observed = tuple(a[1] for a in group.groups())
    assert set(group.group_keys()) == set(keys)
    assert len(subgroups_observed) == len(subgroups_expected)
    assert all(a in subgroups_observed for a in subgroups_expected)


def test_group_subarrays(store: IcechunkStore, zarr_format: ZarrFormat) -> None:
    """
    Test the behavior of `Group` methods for accessing subgroups, namely `Group.group_keys` and `Group.groups`
    """
    group = Group.from_store(store, zarr_format=zarr_format)
    keys = ("foo", "bar")
    subarrays_expected = tuple(group.create_array(k, shape=(10,)) for k in keys)
    # create a sub-group as well
    _ = group.create_group("group")
    subarrays_observed = tuple(a[1] for a in group.arrays())
    assert set(group.array_keys()) == set(keys)
    assert len(subarrays_observed) == len(subarrays_expected)
    assert all(a in subarrays_observed for a in subarrays_expected)


def test_group_update_attributes(store: IcechunkStore, zarr_format: ZarrFormat) -> None:
    """
    Test the behavior of `Group.update_attributes`
    """
    attrs = {"foo": 100}
    group = Group.from_store(store, zarr_format=zarr_format, attributes=attrs)
    assert group.attrs == attrs
    new_attrs = {"bar": 100}
    new_group = group.update_attributes(new_attrs)
    assert new_group.attrs == new_attrs


async def test_group_update_attributes_async(
    store: IcechunkStore, zarr_format: ZarrFormat
) -> None:
    """
    Test the behavior of `Group.update_attributes_async`
    """
    attrs = {"foo": 100}
    group = Group.from_store(store, zarr_format=zarr_format, attributes=attrs)
    assert group.attrs == attrs
    new_attrs = {"bar": 100}
    new_group = await group.update_attributes_async(new_attrs)
    assert new_group.attrs == new_attrs


@pytest.mark.parametrize("method", ["create_array", "array"])
def test_group_create_array(
    store: IcechunkStore,
    zarr_format: ZarrFormat,
    exists_ok: bool,
    method: Literal["create_array", "array"],
) -> None:
    """
    Test `Group.create_array`
    """
    group = Group.from_store(store, zarr_format=zarr_format)
    shape = (10, 10)
    dtype = "uint8"
    data = np.arange(np.prod(shape)).reshape(shape).astype(dtype)

    if method == "create_array":
        array = group.create_array(name="array", shape=shape, dtype=dtype, data=data)
    elif method == "array":
        with pytest.warns(DeprecationWarning):
            array = group.array(name="array", shape=shape, dtype=dtype, data=data)
    else:
        raise AssertionError

    if not exists_ok:
        if method == "create_array":
            with pytest.raises(ContainsArrayError):
                group.create_array(name="array", shape=shape, dtype=dtype, data=data)
        elif method == "array":
            with pytest.raises(ContainsArrayError), pytest.warns(DeprecationWarning):
                group.array(name="array", shape=shape, dtype=dtype, data=data)
    assert array.shape == shape
    assert array.dtype == np.dtype(dtype)
    assert np.array_equal(array[:], data)


@pytest.mark.parametrize("store", ["memory"], indirect=["store"])
@pytest.mark.parametrize("zarr_format", [3])
@pytest.mark.parametrize("exists_ok", [True, False])
@pytest.mark.parametrize("extant_node", ["array", "group"])
def test_group_creation_existing_node(
    store: IcechunkStore,
    zarr_format: ZarrFormat,
    exists_ok: bool,
    extant_node: Literal["array", "group"],
) -> None:
    """
    Check that an existing array or group is handled as expected during group creation.
    """
    spath = StorePath(store)
    group = Group.from_store(spath, zarr_format=zarr_format)
    expected_exception: type[ContainsArrayError | ContainsGroupError]
    attributes: dict[str, JSON] = {"old": True}

    if extant_node == "array":
        expected_exception = ContainsArrayError
        _ = group.create_array(
            "extant", shape=(10,), dtype="uint8", attributes=attributes
        )
    elif extant_node == "group":
        expected_exception = ContainsGroupError
        _ = group.create_group("extant", attributes=attributes)
    else:
        raise AssertionError

    new_attributes = {"new": True}

    if exists_ok:
        pytest.xfail("exists_ok is not implemented for Group.from_store")
        node_new = Group.from_store(
            spath / "extant",
            attributes=new_attributes,
            zarr_format=zarr_format,
            exists_ok=exists_ok,
        )
        assert node_new.attrs == new_attributes
    else:
        with pytest.raises(expected_exception):
            node_new = Group.from_store(
                spath / "extant",
                attributes=new_attributes,
                zarr_format=zarr_format,
                exists_ok=exists_ok,
            )


async def test_asyncgroup_create(
    store: IcechunkStore,
    exists_ok: bool,
    zarr_format: ZarrFormat,
) -> None:
    """
    Test that `AsyncGroup.from_store` works as expected.
    """
    spath = StorePath(store=store)
    attributes = {"foo": 100}
    agroup = await AsyncGroup.from_store(
        store,
        attributes=attributes,
        exists_ok=exists_ok,
        zarr_format=zarr_format,
    )

    assert agroup.metadata == GroupMetadata(
        zarr_format=zarr_format, attributes=attributes
    )
    assert agroup.store_path == await make_store_path(store)

    if not exists_ok:
        with pytest.raises(ContainsGroupError):
            agroup = await AsyncGroup.from_store(
                spath,
                attributes=attributes,
                exists_ok=exists_ok,
                zarr_format=zarr_format,
            )
        # create an array at our target path
        collision_name = "foo"
        _ = await AsyncArray.create(
            spath / collision_name, shape=(10,), dtype="uint8", zarr_format=zarr_format
        )
        with pytest.raises(ContainsArrayError):
            _ = await AsyncGroup.from_store(
                StorePath(store=store) / collision_name,
                attributes=attributes,
                exists_ok=exists_ok,
                zarr_format=zarr_format,
            )


async def test_asyncgroup_attrs(store: IcechunkStore, zarr_format: ZarrFormat) -> None:
    attributes = {"foo": 100}
    agroup = await AsyncGroup.from_store(
        store, zarr_format=zarr_format, attributes=attributes
    )

    assert agroup.attrs == agroup.metadata.attributes == attributes


async def test_asyncgroup_open(
    store: IcechunkStore,
    zarr_format: ZarrFormat,
) -> None:
    """
    Create an `AsyncGroup`, then ensure that we can open it using `AsyncGroup.open`
    """
    attributes = {"foo": 100}
    group_w = await AsyncGroup.from_store(
        store=store,
        attributes=attributes,
        exists_ok=False,
        zarr_format=zarr_format,
    )

    group_r = await AsyncGroup.open(store=store, zarr_format=zarr_format)

    assert group_w.attrs == group_w.attrs == attributes
    assert group_w == group_r


async def test_asyncgroup_open_wrong_format(
    store: IcechunkStore,
    zarr_format: ZarrFormat,
) -> None:
    _ = await AsyncGroup.from_store(store=store, exists_ok=False, zarr_format=zarr_format)
    zarr_format_wrong: ZarrFormat
    # try opening with the wrong zarr format
    if zarr_format == 3:
        zarr_format_wrong = 2
    elif zarr_format == 2:
        zarr_format_wrong = 3
    else:
        raise AssertionError

    with pytest.raises(FileNotFoundError):
        await AsyncGroup.open(store=store, zarr_format=zarr_format_wrong)


# todo: replace the dict[str, Any] type with something a bit more specific
# should this be async?
@pytest.mark.parametrize(
    "data",
    [
        {"zarr_format": 3, "node_type": "group", "attributes": {"foo": 100}},
    ],
)
def test_asyncgroup_from_dict(store: IcechunkStore, data: dict[str, Any]) -> None:
    """
    Test that we can create an AsyncGroup from a dict
    """
    path = "test"
    store_path = StorePath(store=store, path=path)
    group = AsyncGroup.from_dict(store_path, data=data)

    assert group.metadata.zarr_format == data["zarr_format"]
    assert group.metadata.attributes == data["attributes"]


# todo: replace this with a declarative API where we model a full hierarchy


async def test_asyncgroup_getitem(store: IcechunkStore, zarr_format: ZarrFormat) -> None:
    """
    Create an `AsyncGroup`, then create members of that group, and ensure that we can access those
    members via the `AsyncGroup.getitem` method.
    """
    agroup = await AsyncGroup.from_store(store=store, zarr_format=zarr_format)

    array_name = "sub_array"
    sub_array = await agroup.create_array(
        name=array_name, shape=(10,), dtype="uint8", chunk_shape=(2,)
    )
    assert await agroup.getitem(array_name) == sub_array

    sub_group_path = "sub_group"
    sub_group = await agroup.create_group(sub_group_path, attributes={"foo": 100})
    assert await agroup.getitem(sub_group_path) == sub_group

    # check that asking for a nonexistent key raises KeyError
    with pytest.raises(KeyError):
        await agroup.getitem("foo")


async def test_asyncgroup_delitem(store: IcechunkStore, zarr_format: ZarrFormat) -> None:
    agroup = await AsyncGroup.from_store(store=store, zarr_format=zarr_format)
    array_name = "sub_array"
    _ = await agroup.create_array(
        name=array_name,
        shape=(10,),
        dtype="uint8",
        chunk_shape=(2,),
        attributes={"foo": 100},
    )
    await agroup.delitem(array_name)

    #  todo: clean up the code duplication here
    if zarr_format == 2:
        assert not await agroup.store_path.store.exists(array_name + "/" + ".zarray")
        assert not await agroup.store_path.store.exists(array_name + "/" + ".zattrs")
    elif zarr_format == 3:
        assert not await agroup.store_path.store.exists(array_name + "/" + "zarr.json")
    else:
        raise AssertionError

    sub_group_path = "sub_group"
    _ = await agroup.create_group(sub_group_path, attributes={"foo": 100})
    await agroup.delitem(sub_group_path)
    if zarr_format == 2:
        assert not await agroup.store_path.store.exists(array_name + "/" + ".zgroup")
        assert not await agroup.store_path.store.exists(array_name + "/" + ".zattrs")
    elif zarr_format == 3:
        assert not await agroup.store_path.store.exists(array_name + "/" + "zarr.json")
    else:
        raise AssertionError


async def test_asyncgroup_create_group(
    store: IcechunkStore,
    zarr_format: ZarrFormat,
) -> None:
    agroup = await AsyncGroup.from_store(store=store, zarr_format=zarr_format)
    sub_node_path = "sub_group"
    attributes = {"foo": 999}
    subnode = await agroup.create_group(name=sub_node_path, attributes=attributes)

    assert isinstance(subnode, AsyncGroup)
    assert subnode.attrs == attributes
    assert subnode.store_path.path == sub_node_path
    # FIXME
    # assert subnode.store_path.store == store
    assert subnode.metadata.zarr_format == zarr_format


async def test_asyncgroup_create_array(
    store: IcechunkStore, zarr_format: ZarrFormat, exists_ok: bool
) -> None:
    """
    Test that the AsyncGroup.create_array method works correctly. We ensure that array properties
    specified in create_array are present on the resulting array.
    """

    agroup = await AsyncGroup.from_store(store=store, zarr_format=zarr_format)

    if not exists_ok:
        with pytest.raises(ContainsGroupError):
            agroup = await AsyncGroup.from_store(store=store, zarr_format=zarr_format)

    shape = (10,)
    dtype = "uint8"
    chunk_shape = (4,)
    attributes: dict[str, JSON] = {"foo": 100}

    sub_node_path = "sub_array"
    subnode = await agroup.create_array(
        name=sub_node_path,
        shape=shape,
        dtype=dtype,
        chunk_shape=chunk_shape,
        attributes=attributes,
    )
    assert isinstance(subnode, AsyncArray)
    assert subnode.attrs == attributes
    assert subnode.store_path.path == sub_node_path
    # FIXME
    # assert subnode.store_path.store == store
    assert subnode.shape == shape
    assert subnode.dtype == dtype
    # todo: fix the type annotation of array.metadata.chunk_grid so that we get some autocomplete
    # here.
    assert subnode.metadata.chunk_grid.chunk_shape == chunk_shape  # type: ignore
    assert subnode.metadata.zarr_format == zarr_format


async def test_asyncgroup_update_attributes(
    store: IcechunkStore, zarr_format: ZarrFormat
) -> None:
    """
    Test that the AsyncGroup.update_attributes method works correctly.
    """
    attributes_old = {"foo": 10}
    attributes_new = {"baz": "new"}
    agroup = await AsyncGroup.from_store(
        store=store, zarr_format=zarr_format, attributes=attributes_old
    )

    agroup_new_attributes = await agroup.update_attributes(attributes_new)
    assert agroup_new_attributes.attrs == attributes_new


async def test_group_members_async(store: IcechunkStore) -> None:
    group = AsyncGroup(
        GroupMetadata(),
        store_path=StorePath(store=store, path="root"),
    )
    a0 = await group.create_array("a0", shape=(1,))
    g0 = await group.create_group("g0")
    a1 = await g0.create_array("a1", shape=(1,))
    g1 = await g0.create_group("g1")
    a2 = await g1.create_array("a2", shape=(1,))
    g2 = await g1.create_group("g2")

    # immediate children
    children = sorted([x async for x in group.members()], key=operator.itemgetter(0))
    assert children == [
        ("a0", a0),
        ("g0", g0),
    ]

    nmembers = await group.nmembers()
    assert nmembers == 2

    # partial
    children = sorted(
        [x async for x in group.members(max_depth=1)], key=operator.itemgetter(0)
    )
    expected = [
        ("a0", a0),
        ("g0", g0),
        ("g0/a1", a1),
        ("g0/g1", g1),
    ]
    assert children == expected
    nmembers = await group.nmembers(max_depth=1)
    assert nmembers == 4

    # all children
    all_children = sorted(
        [x async for x in group.members(max_depth=None)], key=operator.itemgetter(0)
    )
    expected = [
        ("a0", a0),
        ("g0", g0),
        ("g0/a1", a1),
        ("g0/g1", g1),
        ("g0/g1/a2", a2),
        ("g0/g1/g2", g2),
    ]
    assert all_children == expected

    nmembers = await group.nmembers(max_depth=None)
    assert nmembers == 6

    with pytest.raises(ValueError, match="max_depth"):
        [x async for x in group.members(max_depth=-1)]


async def test_require_group(store: IcechunkStore, zarr_format: ZarrFormat) -> None:
    root = await AsyncGroup.from_store(store=store, zarr_format=zarr_format)

    # create foo group
    _ = await root.create_group("foo", attributes={"foo": 100})

    # test that we can get the group using require_group
    foo_group = await root.require_group("foo")
    assert foo_group.attrs == {"foo": 100}

    # test that we can get the group using require_group and overwrite=True
    if store.supports_deletes:
        foo_group = await root.require_group("foo", overwrite=True)
        assert foo_group.attrs == {}

    _ = await foo_group.create_array(
        "bar", shape=(10,), dtype="uint8", chunk_shape=(2,), attributes={"foo": 100}
    )

    # test that overwriting a group w/ children fails
    # TODO: figure out why ensure_no_existing_node is not catching the foo.bar array
    #
    # with pytest.raises(ContainsArrayError):
    #     await root.require_group("foo", overwrite=True)

    # test that requiring a group where an array is fails
    with pytest.raises(TypeError):
        await foo_group.require_group("bar")


async def test_require_groups(store: IcechunkStore, zarr_format: ZarrFormat) -> None:
    root = await AsyncGroup.from_store(store=store, zarr_format=zarr_format)
    # create foo group
    _ = await root.create_group("foo", attributes={"foo": 100})
    # create bar group
    _ = await root.create_group("bar", attributes={"bar": 200})

    foo_group, bar_group = await root.require_groups("foo", "bar")
    assert foo_group.attrs == {"foo": 100}
    assert bar_group.attrs == {"bar": 200}

    # get a mix of existing and new groups
    foo_group, spam_group = await root.require_groups("foo", "spam")
    assert foo_group.attrs == {"foo": 100}
    assert spam_group.attrs == {}

    # no names
    no_group = await root.require_groups()
    assert no_group == ()


async def test_create_dataset(store: IcechunkStore, zarr_format: ZarrFormat) -> None:
    root = await AsyncGroup.from_store(store=store, zarr_format=zarr_format)
    with pytest.warns(DeprecationWarning):
        foo = await root.create_dataset("foo", shape=(10,), dtype="uint8")
    assert foo.shape == (10,)

    with pytest.raises(ContainsArrayError), pytest.warns(DeprecationWarning):
        await root.create_dataset("foo", shape=(100,), dtype="int8")

    _ = await root.create_group("bar")
    with pytest.raises(ContainsGroupError), pytest.warns(DeprecationWarning):
        await root.create_dataset("bar", shape=(100,), dtype="int8")


async def test_require_array(store: IcechunkStore, zarr_format: ZarrFormat) -> None:
    root = await AsyncGroup.from_store(store=store, zarr_format=zarr_format)
    foo1 = await root.require_array(
        "foo", shape=(10,), dtype="i8", attributes={"foo": 101}
    )
    assert foo1.attrs == {"foo": 101}
    foo2 = await root.require_array("foo", shape=(10,), dtype="i8")
    assert foo2.attrs == {"foo": 101}

    # exact = False
    _ = await root.require_array("foo", shape=10, dtype="f8")

    # errors w/ exact True
    with pytest.raises(TypeError, match="Incompatible dtype"):
        await root.require_array("foo", shape=(10,), dtype="f8", exact=True)

    with pytest.raises(TypeError, match="Incompatible shape"):
        await root.require_array("foo", shape=(100, 100), dtype="i8")

    with pytest.raises(TypeError, match="Incompatible dtype"):
        await root.require_array("foo", shape=(10,), dtype="f4")

    _ = await root.create_group("bar")
    with pytest.raises(TypeError, match="Incompatible object"):
        await root.require_array("bar", shape=(10,), dtype="int8")


class TestGroupMetadata:
    def test_from_dict_extra_fields(self):
        data = {
            "attributes": {"key": "value"},
            "_nczarr_superblock": {"version": "2.0.0"},
            "zarr_format": 2,
        }
        result = GroupMetadata.from_dict(data)
        expected = GroupMetadata(attributes={"key": "value"}, zarr_format=2)
        assert result == expected


class TestInfo:
    def test_info(self, store: IcechunkStore) -> None:
        A = zarr.group(store=store, path="A")
        B = A.create_group(name="B")

        B.create_array(name="x", shape=(1,))
        B.create_array(name="y", shape=(2,))

        result = A.info
        expected = GroupInfo(
            _name="A",
            _read_only=False,
            _store_type="IcechunkStore",
            _zarr_format=3,
        )
        assert result == expected

        result = A.info_complete()
        expected = GroupInfo(
            _name="A",
            _read_only=False,
            _store_type="IcechunkStore",
            _zarr_format=3,
            _count_members=3,
            _count_arrays=2,
            _count_groups=1,
        )
        assert result == expected


def test_delitem_removes_children(store: IcechunkStore, zarr_format: ZarrFormat) -> None:
    # https://github.com/zarr-developers/zarr-python/issues/2191
    g1 = zarr.group(store=store, zarr_format=zarr_format)
    g1.create_group("0")
    g1.create_group("0/0")
    arr = g1.create_array("0/0/0", shape=(1,))
    arr[:] = 1

    del g1["0"]
    with pytest.raises(KeyError):
        g1["0/0"]
