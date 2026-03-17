"""Testing utility functions for icechunk store comparison."""

from __future__ import annotations


def precommit_postcommit_readonly(session, repo):
    """Yield (label, store) for the three icechunk lifecycle phases.

    Commits the session between pre-commit and post-commit phases.
    """
    yield "pre-commit", session.store
    session.commit("commit")
    yield "post-commit", session.store
    yield "readonly", repo.readonly_session(branch="main").store


async def collect_list_prefix(store, prefix=""):
    """Collect all keys from store.list_prefix into a set."""
    return {k async for k in store.list_prefix(prefix)}


async def collect_list_dir(store, prefix):
    """Collect all entries from store.list_dir into a set."""
    return {k async for k in store.list_dir(prefix)}
