import numpy.testing
import pytest
from hypothesis import example, given, note
from hypothesis import strategies as st

import icechunk as ic
import zarr
from icechunk.testing.trees import GroupNode, tree_from_str, trees, valid_moves
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

    a, b, *_ = repo.ancestry(branch="main")
    diff = repo.diff(from_snapshot_id=b.id, to_snapshot_id=a.id)
    assert diff.moved_nodes == [("/my/old", "/my/new")]
    assert (
        repr(diff)
        == """\
Nodes moved/renamed:
    /my/old -> /my/new

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


@st.composite
def tree_and_moves(draw: st.DrawFn) -> tuple[GroupNode, list[tuple[str, str]]]:
    """Generate a tree paired with a sequence of valid moves."""
    tree = draw(trees(max_leaves=st.just(5), max_children=st.just(3)))
    moves = draw(valid_moves(tree, n_moves=st.integers(min_value=1, max_value=5)))
    return tree, moves


@example(tree_moves=(tree_from_str("g: /foo/bar\ng: /baz"), [("foo/bar", "baz/bar")]))
@example(tree_moves=(tree_from_str("a: /0/0/0"), [("0", "0_0")]))
@example(tree_moves=(tree_from_str("a: /0/1"), [("0/1", "0/0"), ("0", "1")]))
@given(tree_moves=tree_and_moves())
def test_moves(
    tree_moves: tuple[GroupNode, list[tuple[str, str]]],
) -> None:
    """A sequence of moves should produce the correct tree after commit.

    See https://github.com/earth-mover/icechunk/issues/1562
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

    expected = repr(zarr.open_group(model).tree())
    # NOTE: skipping pre-commit check — rearrange session store doesn't
    # reflect moves before commit (known bug, see #1562)
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


def test_doesnt_rebase() -> None:
    repo = ic.Repository.create(
        storage=ic.in_memory_storage(),
    )
    session = repo.writable_session("main")
    root = zarr.group(store=session.store, overwrite=True)
    root.create_group("my/old/path", overwrite=True)
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
