"""Cross-version stateful compatibility tests for icechunk.

These tests subclass the two-actor stateful test machines and
replace in-memory storage with local filesystem storage, using
icechunk (v2) and icechunk (v1) side by side.

Requires:
  - icechunk     (v2, current dev build)
  - icechunk_v1  (latest 1.x from PyPI, renamed via third-wheel)

Skipped entirely if icechunk_v1 is not installed.
"""

import tempfile
import types
from dataclasses import dataclass
from typing import Any

import pytest

import icechunk as ic

pytest.importorskip("hypothesis")
pytest.importorskip("xarray")
ic_v1 = pytest.importorskip("icechunk_v1")

# Suppress LocalFileSystem warnings for these tests (which use filesystem storage).
log_filter = "warn,icechunk::storage::object_store=error"
ic.set_logs_filter(log_filter)
ic_v1.set_logs_filter(log_filter)


# Monkey-patch so parent class `pytest.raises(IcechunkError)` catches both versions
import tests.test_stateful_repo_ops as repo_ops_mod

repo_ops_mod.IcechunkError = (ic.IcechunkError, ic_v1.IcechunkError)  # type: ignore[assignment,attr-defined]


from collections.abc import Iterable

import hypothesis.strategies as st
from hypothesis import note, settings
from hypothesis.stateful import initialize, precondition, rule, run_state_machine_as_test

from icechunk.testing.multi_actor import MultiActorCoordinator
from icechunk.testing.stateful_models import IcechunkModel
from tests.test_stateful_repo_ops import (
    VersionControlStateMachine,
)
from tests.test_zarr.test_stateful import (
    ModifiedZarrHierarchyStateMachine,
)


class _ProxySet(set[str]):
    """Narrowed view that delegates mutations to a backing set.

    Iterates/contains only narrowed items. Mutations (add/remove/etc.)
    are forwarded to the backing set so the model stays in sync.
    """

    def __init__(self, backing: set[str], narrowed: Iterable[str]):
        super().__init__(narrowed)
        self._backing = backing

    def add(self, item: str) -> None:
        super().add(item)
        self._backing.add(item)

    def remove(self, item: str) -> None:
        super().remove(item)
        self._backing.remove(item)

    def discard(self, item: str) -> None:
        if item in self:
            super().discard(item)
        self._backing.discard(item)


@dataclass
class Actor:
    """Per-actor state for cross-version tests."""

    module: types.ModuleType  # ic or ic_v1
    store: Any = None  # ic.IcechunkStore | ic_v1.IcechunkStore
    repo: Any = None  # ic.Repository | ic_v1.Repository
    storage: Any = None  # ic.Storage | ic_v1.Storage


class CrossVersionVersionControlStateMachine(VersionControlStateMachine):
    """Two-actor version control test: one actor is v2, the other is v1.

    Uses filesystem storage since v1 and v2 packages can't share storage objects.
    """

    def __init__(self) -> None:
        self._actors = {"v2": Actor(module=ic), "v1": Actor(module=ic_v1)}
        self._storage_path = tempfile.mkdtemp()
        self.actor_name: str = ""
        super().__init__()

    # Disabled: v1 actor doesn't support spec version upgrade.
    @rule()
    @precondition(lambda self: False)
    def upgrade_spec_version(self) -> None:
        pass

    def _make_storage(self) -> ic.Storage:
        return self.ic.local_filesystem_storage(self._storage_path)  # type: ignore[no-any-return]

    @initialize(
        data=st.data(),
        target=VersionControlStateMachine.branches,
    )
    def initialize(self, data: st.DataObject) -> str:
        choice = data.draw(st.sampled_from(list(self._actors.keys())))
        note(f"initializing with actor {choice!r}")
        self.actor_name = choice
        self.ic = self._actors[choice].module
        result = super().initialize(data, spec_version=1)
        self._actors[choice].repo = self.repo
        return result  # type: ignore[return-value]

    @rule(data=st.data())
    def switch_actor(self, data: st.DataObject) -> None:
        """Switch to a randomly chosen actor and reopen the repository."""
        choice = data.draw(st.sampled_from(tuple(self._actors)))
        if choice == self.actor_name:
            return

        note(f"switching to actor {choice!r}")
        self.actor_name = choice
        actor = self._actors[choice]
        self.ic = actor.module
        self.storage = self._make_storage()

        if actor.repo is None:
            actor.repo = self.ic.Repository.open(self.storage)
        self.repo = actor.repo


def test_two_actors_cross_version() -> None:
    run_state_machine_as_test(  # type: ignore[no-untyped-call]
        CrossVersionVersionControlStateMachine
    )


class CrossVersionTwoActorZarrHierarchyStateMachine(ModifiedZarrHierarchyStateMachine):
    """Two-actor Zarr hierarchy test: one actor is v2, the other is v1.

    Uses filesystem storage since v1 and v2 packages can't share storage objects.
    Allows overlapping uncommitted sessions — actors hold uncommitted writes
    simultaneously with array-level claim tracking to ensure disjoint modifications.

    self.model is always a plain IcechunkModel (the current actor's).
    The coordinator just swaps which model is active and computes blocked sets.
    """

    def __init__(self, tmpdir: str) -> None:
        self._actors = {"v2": Actor(module=ic), "v1": Actor(module=ic_v1)}
        self._tmpdir = tmpdir
        self.actor_name: str = "default"
        self._committed_baseline = None
        self._coordinator = MultiActorCoordinator()
        self._coordinator.current_actor = self.actor_name

        # Pass v2 storage to parent — model/committed properties auto-sync coordinator
        storage = ic.local_filesystem_storage(tmpdir)
        super().__init__(storage)

    def _make_storage(self) -> Any:
        """Create a fresh storage object for the current actor's module."""
        return self.ic.local_filesystem_storage(self._tmpdir)

    # ── model/committed properties: auto-sync coordinator ──────────────

    @property
    def model(self) -> Any:
        return self._coordinator.current

    @model.setter
    def model(self, value: Any) -> None:
        if not isinstance(value, IcechunkModel):
            value = IcechunkModel.from_store(value)
        self._coordinator.current = value

    @property
    def committed(self) -> Any:
        return self._committed_baseline

    @committed.setter
    def committed(self, value: Any) -> None:
        self._committed_baseline = value
        self._coordinator.recompute_blocked(value)

    # ── narrowed properties ──────────────────────────────────────────────

    @property
    def all_arrays(self) -> set[str]:
        blocked = self._coordinator.blocked
        backing = self.model.all_arrays
        narrowed = {
            a
            for a in backing
            if not MultiActorCoordinator._has_blocked_ancestor(a, blocked)
        }
        return _ProxySet(backing, narrowed)

    @all_arrays.setter
    def all_arrays(self, value: set[str]) -> None:
        hidden = self.model.all_arrays - self.all_arrays
        self.model.all_arrays = set(value) | hidden

    @property
    def all_groups(self) -> set[str]:
        blocked = self._coordinator.blocked
        backing = self.model.all_groups
        narrowed = {
            g
            for g in backing - {"", "/"}
            if not MultiActorCoordinator._has_blocked_relative(g, blocked)
        }
        return _ProxySet(backing, narrowed)

    @all_groups.setter
    def all_groups(self, value: set[str]) -> None:
        hidden = self.model.all_groups - self.all_groups
        self.model.all_groups = set(value) | hidden

    # ── can_add override ─────────────────────────────────────────────────

    def can_add(self, path: str) -> bool:
        return self._coordinator.can_add(path) and super().can_add(path)

    # ── init / switch ────────────────────────────────────────────────────

    @initialize(spec_version=st.just(1), data=st.data())
    def init_store(self, spec_version: int, data: st.DataObject) -> None:
        """Override to draw actor and create with v1 compatibility."""
        actor_name = data.draw(st.sampled_from(list(self._actors.keys())))
        actor = self._actors[actor_name]
        self.ic = actor.module
        self.storage = self._make_storage()
        super().init_store(spec_version=spec_version)

        # Promote bootstrap model to the drawn actor
        self.actor_name = actor_name
        self._coordinator.current_actor = actor_name
        self._coordinator.init_actor(self.committed, bootstrap="default")
        actor.store = self.store
        actor.repo = self.repo
        actor.storage = self.storage

        # Commit root group so both actors see it
        self.commit_with_check(data)

    @rule(data=st.data())
    def switch_actor(self, data: st.DataObject) -> None:
        """Switch to another actor, preserving uncommitted state."""
        actor_name = data.draw(
            st.sampled_from([a for a in self._actors if a != self.actor_name])
        )

        note(f"switching to actor {actor_name!r}")

        # Stash current state
        prev = self._actors[self.actor_name]
        prev.store = self.store
        prev.repo = self.repo
        prev.storage = self.storage

        # Switch identity
        self.actor_name = actor_name
        actor = self._actors[actor_name]
        self.ic = actor.module
        self._coordinator.switch(actor_name, self.committed)

        if actor.store is None:
            # New actor: create from committed baseline
            self.storage = self._make_storage()
            self.repo = self.ic.Repository.open(self.storage)
            self.store = self.repo.writable_session("main").store
            assert self.committed is not None
            new_model = self._sync(self.committed.copy())
            new_model.sync_baseline(self.committed)
            self._coordinator.current = new_model
            actor.store = self.store
            actor.repo = self.repo
            actor.storage = self.storage
            self._coordinator.recompute_blocked(self.committed)
        else:
            self.store = actor.store
            self.repo = actor.repo
            self.storage = actor.storage

    # ── precondition overrides ─────────────────────────────────────────

    # rewrite_manifests creates a commit on the branch. If another actor has
    # uncommitted changes, their next commit will conflict — ConflictDetector
    # can't resolve rewrite_manifests conflicts since it touched the same
    # manifests, even though no data changed.
    @precondition(lambda self: not self.store.session.has_uncommitted_changes)
    @precondition(lambda self: self._coordinator.other_actors_clean())
    @rule(data=st.data())
    def rewrite_manifests(self, data: st.DataObject) -> None:
        super().rewrite_manifests(data)

    # ── disabled rules ───────────────────────────────────────────────────

    # Disabled: clear() wipes the entire store, which isn't a realistic multi-actor
    # operation. Single-actor test_zarr_hierarchy still covers store.clear().
    @precondition(lambda self: False)
    @rule()
    def clear(self) -> None:
        pass

    # Disabled: v1 actor doesn't support spec version upgrade.
    @rule()
    @precondition(lambda self: False)
    def upgrade_spec_version(self) -> None:
        pass


def test_two_actors_zarr_cross_version() -> None:
    def mk_test_instance_sync() -> CrossVersionTwoActorZarrHierarchyStateMachine:
        return CrossVersionTwoActorZarrHierarchyStateMachine(tempfile.mkdtemp())

    run_state_machine_as_test(  # type: ignore[no-untyped-call]
        mk_test_instance_sync,
        settings=settings(max_examples=150, stateful_step_count=100),
    )
