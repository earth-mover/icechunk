"""Cross-version stateful compatibility tests for icechunk.

These tests subclass the two-actor stateful test machines and
replace in-memory storage with local filesystem storage, using
icechunk (v2) and icechunk (v1) side by side.

Requires:
  - icechunk     (v2, current dev build)
  - icechunk_v1  (latest 1.x from PyPI, renamed via third-wheel)
  - hypothesis, xarray, numpy, zarr

Skipped entirely if icechunk_v1 is not installed.
"""

import os
import tempfile

# Set log filter before importing icechunk packages so both v1 and v2
# pick it up during tracing initialization.
os.environ.setdefault("ICECHUNK_LOG", "warn,icechunk::storage::object_store=error")

import pytest
from packaging.version import Version

import icechunk as ic

ic_v1 = pytest.importorskip("icechunk_v1")
pytest.importorskip("hypothesis")
pytest.importorskip("xarray")

# Monkey-patch so parent class `pytest.raises(IcechunkError)` catches both versions
import tests.test_stateful_repo_ops as repo_ops_mod

repo_ops_mod.IcechunkError = (ic.IcechunkError, ic_v1.IcechunkError)  # type: ignore[assignment,attr-defined]

from typing import Literal

import hypothesis.strategies as st
from hypothesis import note, settings
from hypothesis.stateful import initialize, precondition, rule, run_state_machine_as_test

from tests.test_stateful_repo_ops import TwoActorVersionControlStateMachine
from tests.test_zarr.test_stateful import (
    TwoActorZarrHierarchyStateMachine,
)


class CrossVersionTwoActorVersionControlStateMachine(
    TwoActorVersionControlStateMachine,
):
    """Two-actor version control test: one actor is v2, the other is v1.

    Uses filesystem storage since v1 and v2 packages can't share storage objects.
    """

    def __init__(self) -> None:
        super().__init__()
        self.actors = {"v2": ic.Repository, "v1": ic_v1.Repository}
        self.actor_modules = {"v2": ic, "v1": ic_v1}
        # Set per-actor storage constructors
        self.on_disk_storage_factory = {
            "v2": ic.local_filesystem_storage,
            "v1": ic_v1.local_filesystem_storage,
        }

    # Disabled: v1 actor doesn't support spec version upgrade.
    @rule()
    @precondition(lambda self: False)
    def upgrade_spec_version(self) -> None:
        pass

    @initialize(
        data=st.data(),
        target=TwoActorVersionControlStateMachine.branches,
        spec_version=st.just(1),  # Always v1 for cross-version compat
        use_in_memory_storage=st.just(False),  # Filesystem storage
    )
    def initialize(
        self,
        data: st.DataObject,
        spec_version: Literal[1, 2],
        use_in_memory_storage: bool,
    ) -> str:
        return super().initialize(data, spec_version, use_in_memory_storage)  # type: ignore[return-value]


def test_two_actors_cross_version() -> None:
    run_state_machine_as_test(  # type: ignore[no-untyped-call]
        CrossVersionTwoActorVersionControlStateMachine,
        settings=settings(report_multiple_bugs=False),
    )


class CrossVersionTwoActorZarrHierarchyStateMachine(
    TwoActorZarrHierarchyStateMachine,
):
    """Two-actor Zarr hierarchy test: one actor is v2, the other is v1.

    Uses filesystem storage since v1 and v2 packages can't share storage objects.
    """

    def __init__(self, tmpdir: str) -> None:
        # Pass v2 storage to parent
        storage = ic.local_filesystem_storage(tmpdir)
        super().__init__(storage)

        # Override actors and set up per-actor storage/module objects
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
        from zarr.testing.stateful import ZarrHierarchyStateMachine

        # Draw an actor and set self.actor / self.ic / self.storage
        actor_name = data.draw(st.sampled_from(list(self.actors.keys())))
        self.actor = self.actors[actor_name]
        self.ic = self.actor_modules[actor_name]
        self.storage = self.actor_storage_objects[actor_name]

        note(f"Creating repository with spec_version={spec_version}, actor={self.actor}")

        try:
            self.repo = self.actor.create(self.storage, spec_version=spec_version)
        except TypeError as e:
            if "spec_version" not in str(e):
                raise
            self.repo = self.actor.create(self.storage)
        self.store = self.repo.writable_session("main").store

        # Call grandparent's init_store for zarr-level init
        ZarrHierarchyStateMachine.init_store(self)

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
        mk_test_instance_sync,
        settings=settings(report_multiple_bugs=False),
    )
