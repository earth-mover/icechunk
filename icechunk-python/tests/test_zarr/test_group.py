from __future__ import annotations

import operator
from typing import Literal, cast

import numpy as np
import pytest

import zarr
import zarr.api
import zarr.api.asynchronous
from icechunk import IcechunkError, IcechunkStore
from tests.conftest import parse_repo
from zarr import Array, AsyncGroup, Group
from zarr.api.asynchronous import create_array
from zarr.core.buffer import default_buffer_prototype
from zarr.core.common import JSON, ZarrFormat
from zarr.core.group import GroupMetadata
from zarr.core.sync import sync
from zarr.errors import ContainsArrayError, ContainsGroupError
from zarr.storage import StorePath
from zarr.storage._common import make_store_path


@pytest.fixture(params=["memory"])
def store(request: pytest.FixtureRequest) -> IcechunkStore:
    repo = parse_repo(request.param, "test")
    session = repo.writable_session("main")
    return session.store


@pytest.fixture(params=[True, False])
def overwrite(request: pytest.FixtureRequest) -> bool:
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
        "subarray", shape=(100,), dtype="uint8", chunks=(10,), overwrite=True
    )

    # This is not supported by Icechunk, so we expect an error
    # zarr-python: add an extra object to the domain of the group.
    # the list of children should ignore this object.
    with pytest.raises(IcechunkError):
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
    with pytest.raises(IcechunkError):
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
        "baz", shape=data.shape, dtype=data.dtype, chunks=(2, 2), overwrite=True
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
    store: IcechunkStore, overwrite: bool, zarr_format: ZarrFormat
) -> None:
    """
    Test that `Group.create` works as expected.
    """
    attributes = {"foo": 100}
    group = Group.from_store(
        store, attributes=attributes, zarr_format=zarr_format, overwrite=overwrite
    )

    assert group.attrs == attributes

    if not overwrite:
        with pytest.raises(ContainsGroupError):
            _ = Group.from_store(store, overwrite=overwrite, zarr_format=zarr_format)


def test_group_open(
    store: IcechunkStore, zarr_format: ZarrFormat, overwrite: bool
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
        store, attributes=attrs, zarr_format=zarr_format, overwrite=overwrite
    )
    assert group_created.attrs == attrs
    assert group_created.metadata.zarr_format == zarr_format
    assert group_created.store_path == spath

    # attempt to create a new group in place, to test overwrite
    new_attrs = {"path": "bar"}
    if not overwrite:
        with pytest.raises(ContainsGroupError):
            Group.from_store(
                store, attributes=attrs, zarr_format=zarr_format, overwrite=overwrite
            )
    else:
        if not store.supports_deletes:
            pytest.skip(
                "Store does not support deletes but `overwrite` is True, requiring deletes to override a group"
            )
        group_created_again = Group.from_store(
            store, attributes=new_attrs, zarr_format=zarr_format, overwrite=overwrite
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
    subarray = group.create_array(
        name="subarray", shape=(10,), chunks=(10,), dtype="uint8"
    )

    assert group["subgroup"] == subgroup
    assert group["subarray"] == subarray
    with pytest.raises(KeyError):
        group["nope"]


def test_group_delitem(store: IcechunkStore, zarr_format: ZarrFormat) -> None:
    """
    Test the `Group.__delitem__` method.
    """

    group = Group.from_store(store, zarr_format=zarr_format)
    subgroup = group.create_group(name="subgroup")
    subarray = group.create_array(
        name="subarray", shape=(10,), chunks=(10,), dtype="uint8"
    )

    assert group["subgroup"] == subgroup
    assert group["subarray"] == subarray

    del group["subgroup"]
    with pytest.raises(KeyError):
        group["subgroup"]

    del group["subarray"]
    with pytest.raises(KeyError):
        group["subarray"]


def test_group_setitem(store: IcechunkStore, zarr_format: ZarrFormat) -> None:
    """
    Test the `Group.__setitem__` method.
    """
    group = Group.from_store(store, zarr_format=zarr_format)
    arr = np.ones((2, 4))
    group["key"] = arr
    assert list(group.array_keys()) == ["key"]
    assert isinstance(group["key"], Array)
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
        a = group[key]
        assert isinstance(a, Array)
        assert a.shape == (3, 5)
        np.testing.assert_array_equal(a, arr)  # type: ignore [arg-type]


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
    _ = group.create_array("array", shape=(10,), dtype="uint8")
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
    subarrays_expected = tuple(
        group.create_array(k, shape=(10,), dtype="uint8") for k in keys
    )
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
    updated_attrs = attrs.copy()
    updated_attrs.update(new_attrs)
    assert new_group.attrs == updated_attrs


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


def test_group_create_array(
    store: IcechunkStore,
    zarr_format: ZarrFormat,
    overwrite: bool,
) -> None:
    """
    Test `Group.create_array`
    """
    group = Group.from_store(store, zarr_format=zarr_format)
    shape = (10, 10)
    dtype = "uint8"
    data = np.arange(np.prod(shape)).reshape(shape).astype(dtype)

    array = group.create_array(name="array", shape=shape, dtype=dtype)
    array[:] = data

    if not overwrite:
        with pytest.raises(ContainsArrayError):
            array = group.create_array(name="array", shape=shape, dtype=dtype)
            array[:] = data
    assert array.shape == shape
    assert array.dtype == np.dtype(dtype)
    assert np.array_equal(array[:], data)


@pytest.mark.parametrize("store", ["memory"], indirect=["store"])
@pytest.mark.parametrize("zarr_format", [3])
@pytest.mark.parametrize("overwrite", [True, False])
@pytest.mark.parametrize("extant_node", ["array", "group"])
def test_group_creation_existing_node(
    store: IcechunkStore,
    zarr_format: ZarrFormat,
    overwrite: bool,
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

    if overwrite:
        node_new = Group.from_store(
            spath / "extant",
            attributes=new_attributes,
            zarr_format=zarr_format,
            overwrite=overwrite,
        )
        assert node_new.attrs == new_attributes
    else:
        with pytest.raises(expected_exception):
            Group.from_store(
                spath / "extant",
                attributes=new_attributes,
                zarr_format=zarr_format,
                overwrite=overwrite,
            )


async def test_asyncgroup_create(
    store: IcechunkStore,
    overwrite: bool,
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
        overwrite=overwrite,
        zarr_format=zarr_format,
    )

    assert agroup.metadata == GroupMetadata(
        zarr_format=zarr_format, attributes=attributes
    )
    assert agroup.store_path == await make_store_path(store)

    if not overwrite:
        with pytest.raises(ContainsGroupError):
            agroup = await AsyncGroup.from_store(
                spath,
                attributes=attributes,
                overwrite=overwrite,
                zarr_format=zarr_format,
            )
        # create an array at our target path
        collision_name = "foo"
        _ = await create_array(
            spath / collision_name, shape=(10,), dtype="uint8", zarr_format=zarr_format
        )
        with pytest.raises(ContainsArrayError):
            _ = await AsyncGroup.from_store(
                StorePath(store=store) / collision_name,
                attributes=attributes,
                overwrite=overwrite,
                zarr_format=zarr_format,
            )


# todo: replace this with a declarative API where we model a full hierarchy


async def test_asyncgroup_delitem(store: IcechunkStore, zarr_format: ZarrFormat) -> None:
    agroup = await AsyncGroup.from_store(store=store, zarr_format=zarr_format)
    array_name = "sub_array"
    _ = await agroup.create_array(
        name=array_name,
        shape=(10,),
        dtype="uint8",
        chunks=(2,),
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
    assert agroup_new_attributes.attrs == attributes_old | attributes_new


async def test_group_members_async(store: IcechunkStore) -> None:
    group = AsyncGroup(
        GroupMetadata(),
        store_path=StorePath(store=store, path="root"),
    )
    a0 = await group.create_array("a0", shape=(1,), dtype="uint8")
    g0 = await group.create_group("g0")
    a1 = await g0.create_array("a1", shape=(1,), dtype="uint8")
    g1 = await g0.create_group("g1")
    a2 = await g1.create_array("a2", shape=(1,), dtype="uint8")
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


class TestGroupMetadata:
    def test_from_dict_extra_fields(self) -> None:
        data = {
            "attributes": {"key": "value"},
            "_nczarr_superblock": {"version": "2.0.0"},
            "zarr_format": 2,
        }
        result = GroupMetadata.from_dict(data)
        expected = GroupMetadata(attributes={"key": "value"}, zarr_format=2)
        assert result == expected


def test_delitem_removes_children(store: IcechunkStore, zarr_format: ZarrFormat) -> None:
    # https://github.com/zarr-developers/zarr-python/issues/2191
    g1 = zarr.group(store=store, zarr_format=zarr_format)
    g1.create_group("0")
    g1.create_group("0/0")
    arr = g1.create_array("0/0/0", shape=(1,), dtype="uint8")
    arr[:] = 1

    del g1["0"]
    with pytest.raises(KeyError):
        g1["0/0"]
