import itertools

import numpy.testing
import pytest
from hypothesis import example, given, note
from hypothesis import strategies as st

import icechunk as ic
import zarr
from icechunk.testing.invariants import assert_moves_sorted_by_final_path
from icechunk.testing.trees import GroupNode, tree_and_moves
from icechunk.testing.utils import (
    precommit_postcommit_readonly,
    tree_to_model_and_icechunk,
)
from zarr.core.sync import sync


async def test_basic_move() -> None:
    repo = ic.Repository.create(
        storage=ic.in_memory_storage(),
    )
    session = repo.writable_session("main")
    store = session.store
    root = zarr.group(store=store, overwrite=True)
    root.create_group("my", overwrite=True)
    root.create_group("my/old", overwrite=True)
    group = root.create_group("my/old/path", overwrite=True)
    group.create_array("array", shape=(10, 10), chunks=(2, 2), dtype="i4", fill_value=42)
    all_keys = sorted([k async for k in store.list()])
    assert all_keys == sorted(
        [
            "zarr.json",
            "my/zarr.json",
            "my/old/zarr.json",
            "my/old/path/zarr.json",
            "my/old/path/array/zarr.json",
        ]
    )
    session.commit("create array")

    # Capture node IDs before the move
    session = repo.readonly_session(branch="main")
    id_old = session.get_node_id("/my/old")
    id_path = session.get_node_id("/my/old/path")
    id_array = session.get_node_id("/my/old/path/array")

    session = repo.rearrange_session("main")
    store = session.store
    session.move("/my/old", "/my/new")
    all_keys = sorted([k async for k in store.list()])
    assert all_keys == sorted(
        [
            "zarr.json",
            "my/zarr.json",
            "my/new/zarr.json",
            "my/new/path/zarr.json",
            "my/new/path/array/zarr.json",
        ]
    )
    session.commit("directory renamed")

    session = repo.readonly_session("main")
    store = session.store
    group = zarr.open_group(store=store, mode="r")
    array = group["my/new/path/array"]
    numpy.testing.assert_array_equal(array, 42)

    # Node IDs must be stable across the move
    assert session.get_node_id("/my/new") == id_old
    assert session.get_node_id("/my/new/path") == id_path
    assert session.get_node_id("/my/new/path/array") == id_array

    a, b, *_ = repo.ancestry(branch="main")
    diff = repo.diff(from_snapshot_id=b.id, to_snapshot_id=a.id)
    assert set(diff.moved_nodes) == set(
        [
            ("/my/old", "/my/new"),
            ("/my/old/path", "/my/new/path"),
            ("/my/old/path/array", "/my/new/path/array"),
        ]
    )
    assert (
        repr(diff)
        == """\
Nodes moved/renamed:
    /my/old -> /my/new
    /my/old/path -> /my/new/path
    /my/old/path/array -> /my/new/path/array

"""
    )


def test_move_errors() -> None:
    repo = ic.Repository.create(
        storage=ic.in_memory_storage(),
    )
    session = repo.writable_session("main")
    store = session.store
    root = zarr.group(store=store, overwrite=True)
    group = root.create_group("my/old/path", overwrite=True)
    group.create_array("array", shape=(10, 10), chunks=(2, 2), dtype="i4", fill_value=42)
    session.commit("create array")

    session = repo.rearrange_session("main")
    store = session.store

    with pytest.raises(ic.IcechunkError, match="overwrite existing node"):
        session.move("/my/old/path", "/my/old/path")
    with pytest.raises(ic.IcechunkError, match="node not found"):
        session.move("/not-found", "/my/new/path")

    # TODO: moving root should error but doesn't yet, see #1562
    # with pytest.raises(ic.IcechunkError):
    #     session.move("/", "/new")


@example(
    tree_moves=(
        GroupNode.from_paths(arrays=set(), groups={"foo/bar", "baz"}),
        [("foo/bar", "baz/bar")],
    )
)
@example(
    tree_moves=(GroupNode.from_paths(arrays={"0/0/0"}, groups=set()), [("0", "0_0")])
)
@example(
    tree_moves=(
        GroupNode.from_paths(arrays={"0/1"}, groups=set()),
        [("0/1", "0/0"), ("0", "1")],
    )
)
@given(tree_moves=tree_and_moves())
def test_moves(
    tree_moves: tuple[GroupNode, list[tuple[str, str]]],
) -> None:
    """A sequence of moves should produce the correct tree before, and commit and in a fresh
    readonly_session.
    """
    tree, moves = tree_moves

    model, session, repo = tree_to_model_and_icechunk(tree)
    session.commit("init")
    original_tree = repr(zarr.open_group(model).tree())

    session = repo.rearrange_session("main")
    for source, dest in moves:
        note(f"moving: /{source} -> /{dest}")
        sync(model.move(source, dest))
        session.move(f"/{source}", f"/{dest}")

    # All model paths should be reachable as group members in the store
    model_paths = {path for path, _ in zarr.open_group(model).members(max_depth=None)}
    store_paths = {
        path
        for path, _ in zarr.open_group(session.store, mode="r").members(max_depth=None)
    }
    assert model_paths == store_paths, (
        f"members mismatch after moves:\n"
        f"  model only: {model_paths - store_paths}\n"
        f"  store only: {store_paths - model_paths}"
    )

    expected = repr(zarr.open_group(model).tree())
    snap_before = session.snapshot_id
    for label, store in precommit_postcommit_readonly(session, repo):
        actual = repr(zarr.open_group(store, mode="r").tree())
        assert expected == actual, (
            f"\ntree mismatch [{label}]:"
            f"\n\noriginal tree:\n{original_tree}"
            f"\n\nmoves:\n"
            + "\n".join(f"  /{s} -> /{d}" for s, d in moves)
            + f"\n\nexpected:\n{expected}"
            f"\n\nactual:\n{actual}"
        )

    # Moves in the tx log must be sorted by final path
    snap_after = repo.lookup_branch("main")
    diff = repo.diff(from_snapshot_id=snap_before, to_snapshot_id=snap_after)
    if diff.moved_nodes:
        assert_moves_sorted_by_final_path(diff.moved_nodes)


@given(
    tree_moves=tree_and_moves(n_moves=st.integers(min_value=2, max_value=10)),
    data=st.data(),
)
def test_moves_amend(
    tree_moves: tuple[GroupNode, list[tuple[str, str]]],
    data: st.DataObject,
) -> None:
    """Moves split across commit + amend should produce the same tree
    and transaction log as doing all moves in one commit.

    Splits moves into multiple batches with amends between them.
    Verifies that the transaction log properly collapses chained moves
    during amend (e.g., a->b + b->c becomes a->c).

    See https://github.com/earth-mover/icechunk/pull/1896
    """
    tree, moves = tree_moves

    # Split moves into 2-4 batches: commit the first, amend the rest.
    split_points = sorted(
        {
            0,
            *data.draw(st.sets(st.integers(1, len(moves) - 1), min_size=1, max_size=3)),
            len(moves),
        }
    )
    batches = [moves[a:b] for a, b in itertools.pairwise(split_points)]

    note(f"tree: {tree!r}")
    note(f"batches: {[len(b) for b in batches]} ({len(moves)} moves)")
    for i, (s, d) in enumerate(moves):
        note(f"  move {i}: /{s} -> /{d}")

    # Repo A: all moves in one commit
    _, session_a, repo_a = tree_to_model_and_icechunk(tree)
    session_a.commit("init")
    session_a = repo_a.rearrange_session("main")
    for source, dest in moves:
        session_a.move(f"/{source}", f"/{dest}")
    session_a.commit("all moves")

    # Repo B: same moves split across multiple amends
    _, session_b, repo_b = tree_to_model_and_icechunk(tree)
    session_b.commit("init")

    for batch in batches:
        session_b = repo_b.rearrange_session("main")
        for source, dest in batch:
            session_b.move(f"/{source}", f"/{dest}")
        session_b.amend("amend")

    # Trees must match
    tree_a = repr(zarr.open_group(repo_a.readonly_session("main").store, mode="r").tree())
    tree_b = repr(zarr.open_group(repo_b.readonly_session("main").store, mode="r").tree())
    assert tree_a == tree_b, (
        f"\nmoves: {moves}"
        f"\nbatches: {[len(b) for b in batches]}"
        f"\ntree (one commit):\n{tree_a}"
        f"\ntree (amended):\n{tree_b}"
    )

    # Transaction logs must match — amend should collapse chained moves
    snap_a = repo_a.lookup_branch("main")
    snap_b = repo_b.lookup_branch("main")
    tx_a = repo_a.inspect_transaction_log(snap_a)
    tx_b = repo_b.inspect_transaction_log(snap_b)
    moved_a = sorted(tx_a["moved_nodes"], key=lambda m: (m["from"], m["to"]))
    moved_b = sorted(tx_b["moved_nodes"], key=lambda m: (m["from"], m["to"]))
    assert moved_a == moved_b, (
        f"\nmoves: {moves}"
        f"\nbatches: {[len(b) for b in batches]}"
        f"\nsingle commit tx log moves:  {moved_a}"
        f"\namended commit tx log moves: {moved_b}"
    )


async def test_doesnt_rebase() -> None:
    repo = ic.Repository.create(
        storage=ic.in_memory_storage(),
    )
    session = repo.writable_session("main")
    store = session.store
    root = zarr.group(store=store, overwrite=True)
    root.create_group("my", overwrite=True)
    root.create_group("my/new", overwrite=True)
    root.create_group("my/old", overwrite=True)
    root.create_group("my/old/path", overwrite=True)
    all_keys = sorted([k async for k in store.list()])
    assert all_keys == sorted(
        [
            "zarr.json",
            "my/zarr.json",
            "my/old/zarr.json",
            "my/old/path/zarr.json",
            "my/new/zarr.json",
        ]
    )
    session.commit("create group")

    session1 = repo.rearrange_session("main")
    session1.move("/my/old/path", "/my/new/path")

    session2 = repo.writable_session("main")
    root = zarr.group(store=session2.store, overwrite=True)
    root.create_group("no-conflict", overwrite=True)

    session1.commit("moved")

    try:
        session2.commit("rebase commit")
        raise AssertionError()
    except ic.ConflictError:
        try:
            session2.rebase(ic.ConflictDetector())
            raise AssertionError()
        except ic.RebaseFailedError as e:
            assert len(e.conflicts) == 1
            assert (
                e.conflicts[0].conflict_type
                == ic.ConflictType.MoveOperationCannotBeRebased
            )
