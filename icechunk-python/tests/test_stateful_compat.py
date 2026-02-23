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

import pytest
from packaging.version import Version

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


import hypothesis.strategies as st
from hypothesis import note, settings
from hypothesis.stateful import initialize, precondition, rule, run_state_machine_as_test

from tests.test_stateful_repo_ops import (
    VersionControlStateMachine,
)
from tests.test_zarr.test_stateful import (
    ModifiedZarrHierarchyStateMachine,
)


class CrossVersionVersionControlStateMachine(
    VersionControlStateMachine,
):
    """Two-actor version control test: one actor is v2, the other is v1.

    Uses filesystem storage since v1 and v2 packages can't share storage objects.
    """

    def __init__(self) -> None:
        self.actors = {"v2": ic.Repository, "v1": ic_v1.Repository}
        self.actor_modules = {"v2": ic, "v1": ic_v1}
        self._storage_path = tempfile.mkdtemp()
        super().__init__(actor=None)

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
        choice = data.draw(st.sampled_from(list(self.actors.keys())))
        note(f"initializing with actor {choice!r}")
        self.actor = self.actors[choice]
        self.ic = self.actor_modules[choice]
        return super().initialize(data, spec_version=1)  # type: ignore[return-value]

    @rule(data=st.data())
    def switch_actor(self, data: st.DataObject) -> None:
        """Switch to a randomly chosen actor and reopen the repository."""
        choice = data.draw(st.sampled_from(tuple(self.actors)))
        note(f"switching to actor {choice!r}")
        self.actor = self.actors[choice]
        self.ic = self.actor_modules[choice]
        self.storage = self._make_storage()
        super().reopen_repository(data)


def test_two_actors_cross_version() -> None:
    run_state_machine_as_test(  # type: ignore[no-untyped-call]
        CrossVersionVersionControlStateMachine,
    )


class CrossVersionTwoActorZarrHierarchyStateMachine(
    ModifiedZarrHierarchyStateMachine,
):
    """Two-actor Zarr hierarchy test: one actor is v2, the other is v1.

    Uses filesystem storage since v1 and v2 packages can't share storage objects.
    """

    def __init__(self, tmpdir: str) -> None:
        # Pass v2 storage to parent
        storage = ic.local_filesystem_storage(tmpdir)
        super().__init__(storage)

        # Set up per-actor storage/module objects for cross-version testing
        self.actors = {"v2": ic.Repository, "v1": ic_v1.Repository}
        self._tmpdir = tmpdir
        self.actor_storage_objects = {
            "v2": ic.local_filesystem_storage(tmpdir),
            "v1": ic_v1.local_filesystem_storage(tmpdir),
        }
        self.actor_modules = {"v2": ic, "v1": ic_v1}

    # Disabled: v1 actor doesn't support spec version upgrade.
    @rule()
    @precondition(lambda self: False)
    def upgrade_spec_version(self) -> None:
        pass

    @initialize(spec_version=st.just(1), data=st.data())
    def init_store(self, spec_version: int, data: st.DataObject) -> None:
        """Override to draw actor and create with v1 compatibility."""

        # Draw an actor and set self.actor / self.ic / self.storage
        actor_name = data.draw(st.sampled_from(list(self.actors.keys())))
        self.actor = self.actors[actor_name]
        self.ic = self.actor_modules[actor_name]
        self.storage = self.actor_storage_objects[actor_name]
        super().init_store(spec_version=spec_version)

    @rule(data=st.data())
    def switch_actor(self, data: st.DataObject) -> None:
        if self.store.session.has_uncommitted_changes:
            self.commit_with_check(data)

        # Draw an actor and use their storage object
        actor_name = data.draw(st.sampled_from(list(self.actors.keys())))
        self.actor = self.actors[actor_name]
        self.ic = self.actor_modules[actor_name]
        self.storage = self.actor_storage_objects[actor_name]

        self.repo = self.actor.open(self.storage)
        self.store = self.repo.writable_session("main").store

    # v1 doesn't support empty commits, so only allow them when the actor is v2
    @rule(data=st.data())
    @precondition(
        lambda self: self.store.session.has_uncommitted_changes
        or Version(self.ic.__version__).major >= 2
    )
    def commit_with_check(self, data: st.DataObject) -> None:
        return super().commit_with_check(data)


def test_two_actors_zarr_cross_version() -> None:
    def mk_test_instance_sync() -> CrossVersionTwoActorZarrHierarchyStateMachine:
        return CrossVersionTwoActorZarrHierarchyStateMachine(tempfile.mkdtemp())

    run_state_machine_as_test(  # type: ignore[no-untyped-call]
        mk_test_instance_sync
    )
