"""Property tests comparing zarr Group API between MemoryStore and IcechunkStore.

Uses random zarr tree descriptors with pool-based prefix-collision names
to find path-handling bugs in icechunk's store implementation.
"""

import hypothesis.strategies as st
import pytest
from hypothesis import given, settings

import zarr
from icechunk import Repository, in_memory_storage
from icechunk.testing.strategies_trees import zarr_trees
from icechunk.testing.zarr_tree import materialize


def make_stores(tree):
    """Materialize tree into MemoryStore + IcechunkStore (writable and readonly post-commit)."""
    mem_root = materialize(tree, zarr.storage.MemoryStore())

    repo = Repository.create(storage=in_memory_storage())
    session = repo.writable_session("main")
    ice_writable = materialize(tree, session.store)

    session.commit("commit")
    ro_session = repo.readonly_session(branch="main")
    ice_readonly = zarr.open_group(ro_session.store, mode="r")

    return mem_root, ice_writable, ice_readonly


async def collect_list_prefix(store, prefix=""):
    return {k async for k in store.list_prefix(prefix)}


async def collect_list_dir(store, prefix):
    return {k async for k in store.list_dir(prefix)}


# -- store-level async methods --


@pytest.mark.asyncio
@given(tree=zarr_trees())
@settings(max_examples=50)
async def test_list_prefix(tree):
    """list_prefix on each group path should match.

    Note: list_prefix('') on root passes even when sub-paths are broken,
    so we must check every group path.
    """
    mem_root, ice_w, ice_ro = make_stores(tree)
    for path in tree.groups():
        expected = await collect_list_prefix(mem_root.store, path)
        for label, ice in [("writable", ice_w), ("readonly", ice_ro)]:
            actual = await collect_list_prefix(ice.store, path)
            assert expected == actual, (
                f"list_prefix({path!r}) [{label}]:\n"
                f"  missing: {sorted(expected - actual)}\n"
                f"  extra:   {sorted(actual - expected)}"
            )


@pytest.mark.asyncio
@given(tree=zarr_trees(), data=st.data())
@settings(max_examples=50)
async def test_list_dir(tree, data):
    """list_dir on sampled group paths should match."""
    mem_root, ice_w, ice_ro = make_stores(tree)
    groups = tree.groups()
    paths = data.draw(st.lists(st.sampled_from(groups), min_size=1, max_size=len(groups)))
    for path in paths:
        expected = await collect_list_dir(mem_root.store, path)
        for label, ice in [("writable", ice_w), ("readonly", ice_ro)]:
            actual = await collect_list_dir(ice.store, path)
            assert expected == actual, (
                f"list_dir({path!r}) [{label}]:\n"
                f"  missing: {sorted(expected - actual)}\n"
                f"  extra:   {sorted(actual - expected)}"
            )


@pytest.mark.asyncio
@given(tree=zarr_trees())
@settings(max_examples=50)
async def test_exists(tree):
    mem_root, ice_w, ice_ro = make_stores(tree)
    for path in tree.groups():
        key = f"{path}/zarr.json" if path else "zarr.json"
        expected = await mem_root.store.exists(key)
        for label, ice in [("writable", ice_w), ("readonly", ice_ro)]:
            actual = await ice.store.exists(key)
            assert expected == actual, (
                f"exists({key!r}) [{label}]: memory={expected}, icechunk={actual}"
            )


@pytest.mark.asyncio
@given(tree=zarr_trees())
@settings(max_examples=50)
async def test_is_empty(tree):
    mem_root, ice_w, ice_ro = make_stores(tree)
    for path in tree.groups():
        expected = await mem_root.store.is_empty(path)
        for label, ice in [("writable", ice_w), ("readonly", ice_ro)]:
            actual = await ice.store.is_empty(path)
            assert expected == actual, (
                f"is_empty({path!r}) [{label}]: memory={expected}, icechunk={actual}"
            )


# -- zarr Group API methods --


@given(tree=zarr_trees())
@settings(max_examples=50)
def test_keys(tree):
    mem_root, ice_w, ice_ro = make_stores(tree)
    expected = sorted(mem_root.keys())
    for label, ice in [("writable", ice_w), ("readonly", ice_ro)]:
        assert expected == sorted(ice.keys()), (
            f"keys [{label}]: memory={expected}, icechunk={sorted(ice.keys())}"
        )


@given(tree=zarr_trees())
@settings(max_examples=50)
def test_members(tree):
    mem_root, ice_w, ice_ro = make_stores(tree)
    expected = sorted((name, type(v).__name__) for name, v in mem_root.members())
    for label, ice in [("writable", ice_w), ("readonly", ice_ro)]:
        actual = sorted((name, type(v).__name__) for name, v in ice.members())
        assert expected == actual, (
            f"members [{label}]: memory={expected}, icechunk={actual}"
        )


@given(tree=zarr_trees())
@settings(max_examples=50)
def test_contains(tree):
    mem_root, ice_w, ice_ro = make_stores(tree)
    for path in tree.groups()[1:] + tree.arrays():
        expected = path in mem_root
        for label, ice in [("writable", ice_w), ("readonly", ice_ro)]:
            actual = path in ice
            assert expected == actual, (
                f"__contains__({path!r}) [{label}]: memory={expected}, icechunk={actual}"
            )


@given(tree=zarr_trees())
@settings(max_examples=50)
def test_getitem(tree):
    mem_root, ice_w, ice_ro = make_stores(tree)
    for path in tree.groups()[1:] + tree.arrays():
        mem_val = mem_root[path]
        for label, ice in [("writable", ice_w), ("readonly", ice_ro)]:
            ice_val = ice[path]
            assert type(mem_val).__name__ == type(ice_val).__name__, (
                f"__getitem__({path!r}) [{label}]: memory={type(mem_val).__name__}, icechunk={type(ice_val).__name__}"
            )
            if isinstance(mem_val, zarr.Array):
                assert mem_val.shape == ice_val.shape
                assert mem_val.dtype == ice_val.dtype


@given(tree=zarr_trees())
@settings(max_examples=50)
def test_nmembers(tree):
    mem_root, ice_w, ice_ro = make_stores(tree)
    expected = mem_root.nmembers()
    for label, ice in [("writable", ice_w), ("readonly", ice_ro)]:
        assert expected == ice.nmembers(), (
            f"nmembers [{label}]: memory={expected}, icechunk={ice.nmembers()}"
        )


@given(tree=zarr_trees())
@settings(max_examples=50)
def test_group_keys_and_array_keys(tree):
    mem_root, ice_w, ice_ro = make_stores(tree)
    expected_gk = sorted(mem_root.group_keys())
    expected_ak = sorted(mem_root.array_keys())
    for label, ice in [("writable", ice_w), ("readonly", ice_ro)]:
        assert expected_gk == sorted(ice.group_keys()), (
            f"group_keys [{label}]: memory={expected_gk}, icechunk={sorted(ice.group_keys())}"
        )
        assert expected_ak == sorted(ice.array_keys()), (
            f"array_keys [{label}]: memory={expected_ak}, icechunk={sorted(ice.array_keys())}"
        )
