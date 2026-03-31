"""Testing utility functions for icechunk store setup and transforms."""

from __future__ import annotations

from collections.abc import Generator

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
