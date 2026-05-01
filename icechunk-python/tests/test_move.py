import itertools

import numpy.testing
import pytest
from hypothesis import example, given, note
from hypothesis import strategies as st

import icechunk as ic
import zarr
from icechunk.testing.invariants import assert_moves_sorted_by_final_path
from icechunk.testing.models import GroupNode
from icechunk.testing.trees import (
    ERR_PARENT_NOT_GROUP,
    ERR_SELF_OR_DESCENDANT,
    ERR_SOURCE_MISSING,
    tree_and_invalid_moves,
    tree_and_mixed_moves,
    tree_and_valid_moves,
    wrap_tolerant,
)
from icechunk.testing.utils import (
    precommit_postcommit_readonly,
    tree_to_model_and_icechunk,
)
from zarr.core.sync import sync


def _format_tree_mismatch(
    label: str,
    move_lines: str,
    expected: str,
    actual: str,
    *,
    original_tree: str | None = None,
) -> str:
    parts = [f"tree mismatch [{label}]:"]
    if original_tree is not None:
        parts += ["", f"original tree:\n{original_tree}"]
    parts += [
        "",
        f"moves:\n{move_lines}",
        "",
        f"expected:\n{expected}",
        "",
        f"actual:\n{actual}",
    ]
    return "\n".join(parts)


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
    print(root.tree())
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


@example(
    tree_moves=(
        GroupNode.from_paths(arrays={"/my/old/path/array"}, groups=set()),
        [("/my/old/path", "/my/old/path", ERR_SELF_OR_DESCENDANT)],
    )
)
@example(
    tree_moves=(
        GroupNode.from_paths(arrays={"/my/old/path/array"}, groups=set()),
        [
            (
                "/my/old/path",
                "/my/old/path/array",
                ERR_SELF_OR_DESCENDANT,
            )
        ],
    )
)
@example(
    tree_moves=(
        GroupNode.from_paths(arrays={"/my/old/path/array"}, groups=set()),
        [("/not-found", "/my/new/path", ERR_SOURCE_MISSING)],
    )
)
@example(
    tree_moves=(
        GroupNode.from_paths(arrays={"/array"}, groups=set()),
        [("/", "/renamed-root", ERR_SELF_OR_DESCENDANT)],
    )
)
@example(
    tree_moves=(
        GroupNode.from_paths(arrays={"/arr", "/src"}, groups=set()),
        [("/src", "/arr/x", ERR_PARENT_NOT_GROUP)],
    )
)
@given(tree_and_invalid_moves(n_moves=st.integers(min_value=1, max_value=4)))
def test_invalid_moves(
    tree_moves: tuple[GroupNode, list[tuple[str, str, str]]],
) -> None:
    """Every invalid move must raise with its expected error pattern.

    The strategy bundles each invalid move with the regex its rejection error
    must match. Each move is independent — the rearrange session is reusable
    since rejections happen before the change_set is touched.
    """
    tree, moves = tree_moves
    _, session, repo = tree_to_model_and_icechunk(tree)
    session.commit("init")

    tree_before = repr(
        zarr.open_group(repo.readonly_session("main").store, mode="r").tree()
    )
    rearrange = repo.rearrange_session("main")
    for src, dst, pattern in moves:
        note(f"expecting {pattern!r}: {src} -> {dst}")
        with pytest.raises(ic.IcechunkError, match=wrap_tolerant(pattern)):
            rearrange.move(src, dst)

    # Rejected moves must not mutate the session: no uncommitted changes,
    # an empty status diff, and the tree is exactly what we started with.
    assert not rearrange.has_uncommitted_changes
    assert rearrange.status().is_empty()
    assert repr(zarr.open_group(rearrange.store, mode="r").tree()) == tree_before


@example(
    tree_moves=(
        GroupNode.from_paths(arrays=set(), groups={"/foo/bar", "/baz"}),
        [("/foo/bar", "/baz/bar")],
    )
)
@example(
    tree_moves=(GroupNode.from_paths(arrays={"/0/0/0"}, groups=set()), [("/0", "/0_0")])
)
@example(
    tree_moves=(
        GroupNode.from_paths(arrays={"/0/1"}, groups=set()),
        [("/0/1", "/0/0"), ("/0", "/1")],
    )
)
@given(tree_moves=tree_and_valid_moves())
def test_valid_moves(
    tree_moves: tuple[GroupNode, list[tuple[str, str]]],
) -> None:
    """A sequence of moves should produce the correct tree before, and after commit, and in a fresh
    readonly_session.
    """
    tree, moves = tree_moves

    model, session, repo = tree_to_model_and_icechunk(tree)
    session.commit("init")
    original_tree = repr(zarr.open_group(model).tree())

    session = repo.rearrange_session("main")
    for source, dest in moves:
        note(f"moving: {source} -> {dest}")
        sync(model.move(source, dest))
        session.move(source, dest)

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
    move_lines = "\n".join(f"  {s} -> {d}" for s, d in moves)
    for label, store in precommit_postcommit_readonly(session, repo):
        actual = repr(zarr.open_group(store, mode="r").tree())
        assert expected == actual, _format_tree_mismatch(
            label, move_lines, expected, actual, original_tree=original_tree
        )

    # Moves in the tx log must be sorted by final path
    snap_after = repo.lookup_branch("main")
    diff = repo.diff(from_snapshot_id=snap_before, to_snapshot_id=snap_after)
    if diff.moved_nodes:
        assert_moves_sorted_by_final_path(diff.moved_nodes)


@given(tree_moves=tree_and_mixed_moves(n_moves=st.integers(min_value=2, max_value=8)))
def test_mixed_moves(
    tree_moves: tuple[GroupNode, list[tuple[str, str, str | None]]],
) -> None:
    """Interleaved valid and invalid moves in one rearrange session.

    Invalid moves must raise their expected error and leave the session
    untouched; valid moves must apply and have the expected final tree.
    """
    tree, moves = tree_moves
    model, session, repo = tree_to_model_and_icechunk(tree)
    session.commit("init")

    rearrange = repo.rearrange_session("main")
    for src, dst, pattern in moves:
        note(f"{'INVALID' if pattern else 'VALID':8s} {src} -> {dst}")
        if pattern is None:
            sync(model.move(src, dst))
            rearrange.move(src, dst)
        else:
            with pytest.raises(ic.IcechunkError, match=wrap_tolerant(pattern)):
                rearrange.move(src, dst)

    expected = repr(zarr.open_group(model).tree())
    move_lines = "\n".join(
        f"  {'INVALID' if p else 'VALID':8s} {s} -> {d}" for s, d, p in moves
    )
    for label, store in precommit_postcommit_readonly(rearrange, repo, allow_empty=True):
        actual = repr(zarr.open_group(store, mode="r").tree())
        assert expected == actual, _format_tree_mismatch(
            label, move_lines, expected, actual
        )


@given(
    tree_moves=tree_and_valid_moves(n_moves=st.integers(min_value=2, max_value=10)),
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
        note(f"  move {i}: {s} -> {d}")

    # Repo A: all moves in one commit
    _, session_a, repo_a = tree_to_model_and_icechunk(tree)
    session_a.commit("init")
    session_a = repo_a.rearrange_session("main")
    for source, dest in moves:
        session_a.move(source, dest)
    session_a.commit("all moves")

    # Repo B: same moves split across multiple amends
    _, session_b, repo_b = tree_to_model_and_icechunk(tree)
    session_b.commit("init")

    for batch in batches:
        session_b = repo_b.rearrange_session("main")
        for source, dest in batch:
            session_b.move(source, dest)
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
