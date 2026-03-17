"""Testing utility functions for icechunk store comparison."""

from __future__ import annotations

from collections.abc import Generator, Iterable

from icechunk import Repository, in_memory_storage
from icechunk.session import Session
from icechunk.store import IcechunkStore
from icechunk.testing.models import ModelStore
from icechunk.testing.trees import GroupNode


def tree_to_model_and_icechunk(
    tree: GroupNode, add_hypothesis_note: bool = True
) -> tuple[ModelStore, Session, Repository]:
    """Materialize tree into ModelStore + IcechunkStore (uncommitted)."""
    model = ModelStore()
    tree.materialize(model)

    repo = Repository.create(storage=in_memory_storage())
    session = repo.writable_session("main")
    tree.materialize(session.store)
    if add_hypothesis_note:
        from hypothesis import note

        import zarr

        note(zarr.open_group(model).tree())

    return model, session, repo


def precommit_postcommit_readonly(
    session: Session, repo: Repository
) -> Generator[tuple[str, IcechunkStore], None, None]:
    """Yield (label, store) for the three icechunk lifecycle phases.

    Commits the session between pre-commit and post-commit phases.
    """
    yield "pre-commit", session.store
    session.commit("commit")
    yield "post-commit", session.store
    yield "readonly", repo.readonly_session(branch="main").store


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


def update_paths_after_move(
    source: str, dest: str, arrays: set[str], groups: set[str]
) -> list[set[str]]:
    """Update path sets after a move operation."""

    def rename(p: str) -> str:
        if p == source:
            return dest
        if p.startswith(source + "/"):
            return dest + p[len(source) :]
        return p

    return [{rename(p) for p in s} for s in (arrays, groups)]


async def compare_list_dir(
    model: ModelStore, store: IcechunkStore, paths: Iterable[str]
) -> None:
    """Compare list_dir results between model and store for given paths."""
    for path in sorted(paths):
        expected = sorted([k async for k in model.list_dir(path)])
        actual = sorted([k async for k in store.list_dir(path)])
        assert_list_dir_equal(path, expected, actual)
