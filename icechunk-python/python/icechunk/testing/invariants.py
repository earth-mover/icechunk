"""Reusable invariant checks for icechunk repositories."""

from __future__ import annotations

import itertools
from collections.abc import Iterable

from hypothesis import event

from icechunk._icechunk_python import SnapshotInfo
from icechunk.store import IcechunkStore
from icechunk.testing.models import ModelStore

__all__ = [
    "assert_ancestry_invariants",
    "assert_list_dir_equal",
    "assert_moves_sorted_by_final_path",
    "compare_list_dir",
]


def assert_ancestry_invariants(
    ancestry: list[SnapshotInfo],
    known_commits: set[str] | None = None,
    *,
    must_contain_initial: bool = True,
) -> None:
    """Assert fundamental invariants on a snapshot ancestry chain.

    Checks that timestamps are monotonically decreasing, that each snapshot
    appears at most once, and that every non-initial snapshot has a parent.

    If ``known_commits`` is provided, also checks that every snapshot in the
    ancestry is a member of that set.

    If ``must_contain_initial`` is False, the ancestry is allowed to terminate
    at any snapshot with ``parent_id=None`` (not just the initial snapshot).
    This can happen after expiration reparents orphaned snapshots.
    """
    ancestry_set = set([snap.id for snap in ancestry])
    if known_commits is not None:
        assert ancestry_set.issubset(known_commits)
    # snapshot timestamps are monotonically decreasing in ancestry
    assert all(a.written_at > b.written_at for a, b in itertools.pairwise(ancestry))
    # ancestry must be unique
    assert len(ancestry_set) == len(ancestry)
    # the ancestry chain must terminate at a snapshot with no parent
    INITIAL_SNAPSHOT = "1CECHNKREP0F1RSTCMT0"
    if must_contain_initial:
        assert ancestry[-1].id == INITIAL_SNAPSHOT, (
            f"Last snapshot in ancestry is {ancestry[-1].id}, expected {INITIAL_SNAPSHOT}"
        )
    assert ancestry[-1].parent_id is None, (
        f"Last snapshot {ancestry[-1].id} has parent_id={ancestry[-1].parent_id}, expected None"
    )
    # every non-root snapshot must have a parent
    for snap in ancestry[:-1]:
        assert snap.parent_id is not None, (
            f"Snapshot {snap.id} is not the root but has parent_id=None"
        )
    n = len(ancestry)
    bucket = f"{n // 10 * 10}-{n // 10 * 10 + 9}"
    event(f"ancestry length: {bucket}")


def assert_moves_sorted_by_final_path(
    moved_nodes: list[tuple[str, str]],
) -> None:
    """Assert that moved_nodes are sorted by final path (component-based).

    Rust's Path::cmp compares by path components, not raw bytes, so we
    split on '/' to match (e.g. /a < /a/b < /a-b).
    """
    assert moved_nodes, "Expected non-empty moved_nodes"
    final_paths = [to for _, to in moved_nodes]
    assert final_paths == sorted(final_paths, key=lambda p: p.split("/")), (
        f"Moves not sorted by final path: {moved_nodes}"
    )


def assert_list_dir_equal(path: str, expected: list[str], actual: list[str]) -> None:
    """Assert two list_dir results match, ignoring the "c" chunk directory.

    Consider .list_dir("path/to/array") for an array with a single chunk.
    The MemoryStore model will return ``"c", "zarr.json"`` only if the chunk exists.
    If that chunk was deleted, then ``"c"`` is not returned.
    LocalStore will not have this behaviour.
    In Icechunk, we always return the ``c`` so ignore this inconsistency.
    """
    if expected != actual and set(expected).symmetric_difference(set(actual)) != {"c"}:
        assert expected == actual, (
            f"list_dir({path!r}):\n  model:    {expected}\n  icechunk: {actual}"
        )


async def compare_list_dir(
    model: ModelStore, store: IcechunkStore, paths: Iterable[str]
) -> None:
    """Compare list_dir results between model and store for given paths."""
    for path in sorted(paths):
        expected = sorted([k async for k in model.list_dir(path)])
        actual = sorted([k async for k in store.list_dir(path)])
        assert_list_dir_equal(path, expected, actual)
