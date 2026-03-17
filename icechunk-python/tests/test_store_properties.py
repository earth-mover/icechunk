"""Property tests comparing store/group API between MemoryStore and IcechunkStore.

Uses random zarr tree descriptors with pool-based prefix-collision names
to find path-handling bugs in icechunk's store implementation.
"""

import pytest
from hypothesis import given

import zarr
from icechunk.testing.trees import trees
from icechunk.testing.utils import (
    precommit_postcommit_readonly,
    tree_to_model_and_icechunk,
)


@pytest.mark.asyncio
@given(tree=trees())
async def test_list_prefix(tree):
    """list_prefix on each node path should match."""
    model, session, repo = tree_to_model_and_icechunk(tree)

    all_nodes = tree.nodes(include_root=True)
    for label, store in precommit_postcommit_readonly(session, repo):
        for path in all_nodes:
            expected = {k async for k in model.list_prefix(path)}
            # MemoryStore list_prefix does raw string matching, so
            # list_prefix("0") also returns "0_c/zarr.json".
            # Filter to directory semantics to match IcechunkStore.
            # See https://github.com/earth-mover/icechunk/pull/1825
            # and https://github.com/zarr-developers/zarr-python/issues/3773
            if path:
                expected = {k for k in expected if k.startswith(path + "/")}
            actual = {k async for k in store.list_prefix(path)}
            assert expected == actual, f"list_prefix({path!r}) [{label}]"


@pytest.mark.asyncio
@given(tree=trees())
async def test_list_dir(tree):
    """list_dir on every group path should match."""
    model, session, repo = tree_to_model_and_icechunk(tree)

    groups = tree.groups(include_root=True)
    for label, store in precommit_postcommit_readonly(session, repo):
        for path in groups:
            expected = {k async for k in model.list_dir(path)}
            actual = {k async for k in store.list_dir(path)}
            assert expected == actual, f"list_dir({path!r}) [{label}]"


@pytest.mark.asyncio
@given(tree=trees())
async def test_exists(tree):
    model, session, repo = tree_to_model_and_icechunk(tree)

    groups = tree.groups(include_root=True)
    for label, store in precommit_postcommit_readonly(session, repo):
        for path in groups:
            key = f"{path}/zarr.json" if path else "zarr.json"
            expected = await model.exists(key)
            actual = await store.exists(key)
            assert expected == actual, f"exists({key!r}) [{label}]"


@pytest.mark.asyncio
@given(tree=trees())
async def test_is_empty(tree):
    model, session, repo = tree_to_model_and_icechunk(tree)

    groups = tree.groups(include_root=True)
    for label, store in precommit_postcommit_readonly(session, repo):
        for path in groups:
            expected = await model.is_empty(path)
            actual = await store.is_empty(path)
            assert expected == actual, f"is_empty({path!r}) [{label}]"


@given(tree=trees())
def test_keys(tree):
    model, session, repo = tree_to_model_and_icechunk(tree)
    model_group = zarr.open_group(model)
    expected = set(model_group.keys())
    for label, store in precommit_postcommit_readonly(session, repo):
        ice = zarr.open_group(store, mode="r")
        assert expected == set(ice.keys()), f"keys [{label}]"


@given(tree=trees())
def test_members(tree):
    model, session, repo = tree_to_model_and_icechunk(tree)
    model_group = zarr.open_group(model)
    expected = {(name, type(v).__name__) for name, v in model_group.members()}
    for label, store in precommit_postcommit_readonly(session, repo):
        ice = zarr.open_group(store, mode="r")
        actual = {(name, type(v).__name__) for name, v in ice.members()}
        assert expected == actual, f"members [{label}]"


@given(tree=trees())
def test_contains(tree):
    model, session, repo = tree_to_model_and_icechunk(tree)
    model_group = zarr.open_group(model)
    all_nodes = tree.nodes()
    for label, store in precommit_postcommit_readonly(session, repo):
        ice = zarr.open_group(store, mode="r")
        for path in all_nodes:
            assert (path in model_group) == (path in ice), (
                f"__contains__({path!r}) [{label}]"
            )


@given(tree=trees())
def test_getitem(tree):
    model, session, repo = tree_to_model_and_icechunk(tree)
    model_group = zarr.open_group(model)
    all_nodes = tree.nodes()
    for label, store in precommit_postcommit_readonly(session, repo):
        ice = zarr.open_group(store, mode="r")
        for path in all_nodes:
            mem_val = model_group[path]
            ice_val = ice[path]
            assert type(mem_val).__name__ == type(ice_val).__name__, (
                f"__getitem__({path!r}) [{label}]: type mismatch"
            )
            if isinstance(mem_val, zarr.Array):
                assert mem_val.shape == ice_val.shape
                assert mem_val.dtype == ice_val.dtype


@given(tree=trees())
def test_group_keys_and_array_keys(tree):
    model, session, repo = tree_to_model_and_icechunk(tree)
    model_group = zarr.open_group(model)
    expected_gk = set(model_group.group_keys())
    expected_ak = set(model_group.array_keys())
    for label, store in precommit_postcommit_readonly(session, repo):
        ice = zarr.open_group(store, mode="r")
        assert expected_gk == set(ice.group_keys()), f"group_keys [{label}]"
        assert expected_ak == set(ice.array_keys()), f"array_keys [{label}]"
