"""Property tests comparing zarr Group API between MemoryStore and IcechunkStore.

Uses random zarr tree descriptors with pool-based prefix-collision names
to find path-handling bugs in icechunk's store implementation.
"""

import hypothesis.strategies as st
import pytest
from hypothesis import given, note, settings

import zarr
from icechunk import Repository, in_memory_storage
from icechunk.testing.strategies_trees import zarr_trees
from icechunk.testing.zarr_tree import materialize
from tests.test_zarr.test_stateful import ModelStore
from zarr.testing.strategies import node_names


def make_stores(tree):
    """Materialize tree into ModelStore + IcechunkStore (uncommitted)."""
    model = ModelStore()
    materialize(tree, model)

    repo = Repository.create(storage=in_memory_storage())
    session = repo.writable_session("main")
    materialize(tree, session.store)

    return model, session, repo


async def collect_list_prefix(store, prefix=""):
    return {k async for k in store.list_prefix(prefix)}


async def collect_list_dir(store, prefix):
    return {k async for k in store.list_dir(prefix)}


# -- store-level async methods --


def precommit_postcommit_readonly(session, repo):
    """Yield (label, store) for the three icechunk lifecycle phases."""
    yield "pre-commit", session.store
    session.commit("commit")
    yield "post-commit", session.store
    yield "readonly", repo.readonly_session(branch="main").store


@pytest.mark.asyncio
@given(tree=zarr_trees())
@settings(max_examples=50, deadline=None)
async def test_list_prefix(tree):
    """list_prefix on each group path should match.

    Note: list_prefix('') on root passes even when sub-paths are broken,
    so we must check every group path.
    """
    model, session, repo = make_stores(tree)

    note(zarr.open_group(model).tree())
    for label, store in precommit_postcommit_readonly(session, repo):
        for path in tree.nodes():
            expected = await collect_list_prefix(model, path)
            # MemoryStore list_prefix does raw string matching, so
            # list_prefix("0") returns "0_c/zarr.json" too.
            # Filter to directory semantics to match LocalStore/IcechunkStore.
            if path:
                expected = {k for k in expected if k.startswith(path + "/")}
            actual = await collect_list_prefix(store, path)
            assert expected == actual, (
                f"list_prefix({path!r}) [{label}]:\n"
                f"  missing: {sorted(expected - actual)}\n"
                f"  extra:   {sorted(actual - expected)}"
            )


@pytest.mark.asyncio
@given(tree=zarr_trees(), data=st.data())
@settings(max_examples=50, deadline=None)
async def test_list_dir(tree, data):
    """list_dir on sampled group paths should match."""
    model, session, repo = make_stores(tree)

    groups = tree.groups()
    paths = data.draw(st.lists(st.sampled_from(groups), min_size=1, max_size=len(groups)))
    for label, store in precommit_postcommit_readonly(session, repo):
        for path in paths:
            expected = await collect_list_dir(model, path)
            actual = await collect_list_dir(store, path)
            assert expected == actual, (
                f"list_dir({path!r}) [{label}]:\n"
                f"  missing: {sorted(expected - actual)}\n"
                f"  extra:   {sorted(actual - expected)}"
            )


@pytest.mark.asyncio
@given(tree=zarr_trees())
@settings(max_examples=50, deadline=None)
async def test_exists(tree):
    model, session, repo = make_stores(tree)

    for label, store in precommit_postcommit_readonly(session, repo):
        for path in tree.groups():
            key = f"{path}/zarr.json" if path else "zarr.json"
            expected = await model.exists(key)
            actual = await store.exists(key)
            assert expected == actual, (
                f"exists({key!r}) [{label}]: memory={expected}, icechunk={actual}"
            )


@pytest.mark.asyncio
@given(tree=zarr_trees())
@settings(max_examples=50, deadline=None)
async def test_is_empty(tree):
    model, session, repo = make_stores(tree)

    for label, store in precommit_postcommit_readonly(session, repo):
        for path in tree.groups():
            expected = await model.is_empty(path)
            actual = await store.is_empty(path)
            assert expected == actual, (
                f"is_empty({path!r}) [{label}]: memory={expected}, icechunk={actual}"
            )


# -- zarr Group API methods --


@given(tree=zarr_trees())
@settings(max_examples=50, deadline=None)
def test_keys(tree):
    model, session, repo = make_stores(tree)
    model_group = zarr.open_group(model)
    expected = sorted(model_group.keys())
    for label, store in precommit_postcommit_readonly(session, repo):
        ice = zarr.open_group(store, mode="r")
        assert expected == sorted(ice.keys()), (
            f"keys [{label}]: memory={expected}, icechunk={sorted(ice.keys())}"
        )


@given(tree=zarr_trees())
@settings(max_examples=50, deadline=None)
def test_members(tree):
    model, session, repo = make_stores(tree)
    model_group = zarr.open_group(model)
    expected = sorted((name, type(v).__name__) for name, v in model_group.members())
    for label, store in precommit_postcommit_readonly(session, repo):
        ice = zarr.open_group(store, mode="r")
        actual = sorted((name, type(v).__name__) for name, v in ice.members())
        assert expected == actual, (
            f"members [{label}]: memory={expected}, icechunk={actual}"
        )


@given(tree=zarr_trees())
@settings(max_examples=50, deadline=None)
def test_contains(tree):
    model, session, repo = make_stores(tree)
    model_group = zarr.open_group(model)
    for label, store in precommit_postcommit_readonly(session, repo):
        ice = zarr.open_group(store, mode="r")
        for path in tree.groups()[1:] + tree.arrays():
            expected = path in model_group
            actual = path in ice
            assert expected == actual, (
                f"__contains__({path!r}) [{label}]: memory={expected}, icechunk={actual}"
            )


@given(tree=zarr_trees())
@settings(max_examples=50, deadline=None)
def test_getitem(tree):
    model, session, repo = make_stores(tree)
    model_group = zarr.open_group(model)
    for label, store in precommit_postcommit_readonly(session, repo):
        ice = zarr.open_group(store, mode="r")
        for path in tree.groups()[1:] + tree.arrays():
            mem_val = model_group[path]
            ice_val = ice[path]
            assert type(mem_val).__name__ == type(ice_val).__name__, (
                f"__getitem__({path!r}) [{label}]: memory={type(mem_val).__name__}, icechunk={type(ice_val).__name__}"
            )
            if isinstance(mem_val, zarr.Array):
                assert mem_val.shape == ice_val.shape
                assert mem_val.dtype == ice_val.dtype


@given(tree=zarr_trees())
@settings(max_examples=50, deadline=None)
def test_nmembers(tree):
    model, session, repo = make_stores(tree)
    model_group = zarr.open_group(model)
    expected = model_group.nmembers()
    for label, store in precommit_postcommit_readonly(session, repo):
        ice = zarr.open_group(store, mode="r")
        assert expected == ice.nmembers(), (
            f"nmembers [{label}]: memory={expected}, icechunk={ice.nmembers()}"
        )


@given(tree=zarr_trees())
@settings(max_examples=50, deadline=None)
def test_group_keys_and_array_keys(tree):
    model, session, repo = make_stores(tree)
    model_group = zarr.open_group(model)
    expected_gk = sorted(model_group.group_keys())
    expected_ak = sorted(model_group.array_keys())
    for label, store in precommit_postcommit_readonly(session, repo):
        ice = zarr.open_group(store, mode="r")
        assert expected_gk == sorted(ice.group_keys()), (
            f"group_keys [{label}]: memory={expected_gk}, icechunk={sorted(ice.group_keys())}"
        )
        assert expected_ak == sorted(ice.array_keys()), (
            f"array_keys [{label}]: memory={expected_ak}, icechunk={sorted(ice.array_keys())}"
        )


# -- move tests --


def update_paths(source, dest, arrays, groups):
    """Update path sets after a move, same logic as stateful test."""
    return [
        {
            dest
            if p == source
            else dest + p[len(source) :]
            if p.startswith(source + "/")
            else p
            for p in s
        }
        for s in (arrays, groups)
    ]


async def compare_list_dir(model, store, paths):
    """Compare list_dir between model and icechunk store for all paths."""
    for path in sorted(paths):
        expected = sorted(await collect_list_dir(model, path))
        actual = sorted(await collect_list_dir(store, path))
        # Ignore "c" inconsistency (see stateful test comment)
        if expected != actual and set(expected).symmetric_difference(set(actual)) != {
            "c"
        }:
            assert expected == actual, (
                f"list_dir({path!r}):\n  model:    {expected}\n  icechunk: {actual}"
            )


@pytest.mark.asyncio
@given(tree=zarr_trees(), data=st.data())
@settings(max_examples=50, deadline=None)
async def test_move(tree, data):
    """Generate a tree, apply random moves, compare ModelStore vs IcechunkStore."""
    model, session, repo = make_stores(tree)
    session.commit("initial")
    arrays = set(tree.arrays())
    groups = set(tree.groups()[1:])  # exclude root ""

    note(f"initial tree: {zarr.open_group(model).tree()}")

    num_moves = data.draw(st.integers(min_value=1, max_value=5))
    session = repo.rearrange_session("main")

    for _ in range(num_moves):
        existing_nodes = arrays | groups
        if not existing_nodes:
            break
        possible_parents = sorted(groups | {""})

        source = data.draw(st.sampled_from(sorted(existing_nodes)))
        source_name = source.split("/")[-1]

        def valid_parent(p, src=source):
            return p != src and not p.startswith(src + "/")

        dest_parent = data.draw(st.sampled_from(possible_parents).filter(valid_parent))

        def valid_name(n, dp=dest_parent, nodes=existing_nodes):
            return f"{dp}/{n}".lstrip("/") not in nodes

        new_name = data.draw(
            st.one_of(st.just(source_name), node_names).filter(valid_name)
        )
        dest = f"{dest_parent}/{new_name}".lstrip("/")

        note(f"move {source!r} -> {dest!r}")
        session.move(f"/{source}", f"/{dest}")
        await model.move(source, dest)
        arrays, groups = update_paths(source, dest, arrays, groups)

    # Compare pre-commit — skipped due to known intermediate state bug
    # https://github.com/earth-mover/icechunk/issues/1562#issuecomment-3755544352
    # await compare_list_dir(model, session.store, groups | {""})

    # Commit and compare post-commit
    session.commit("moves", allow_empty=True)
    ro_store = repo.readonly_session("main").store
    await compare_list_dir(model, ro_store, groups | {""})
