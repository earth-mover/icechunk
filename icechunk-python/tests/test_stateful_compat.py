"""Cross-version stateful compatibility tests for icechunk.

These tests subclass the two-actor stateful test machines and
replace in-memory storage with local filesystem storage, using
icechunk (v2) and icechunk (v1) side by side.

Requires:
  - icechunk     (v2, current dev build)
  - icechunk_v1  (latest 1.x from PyPI, renamed via third-wheel)

Skipped entirely if icechunk_v1 is not installed.
"""

import contextlib
import datetime
import shutil
import tempfile
from typing import Any

import pytest
from hypothesis import settings
from packaging.version import Version

import icechunk as ic
import zarr

pytest.importorskip("hypothesis")
pytest.importorskip("xarray")
ic_v1 = pytest.importorskip("icechunk_v1")


# These tests pin spec_version=1, which does not support rectilinear chunk grids.
# The package conftest globally enables `array.rectilinear_chunks`, which causes
# zarr's metadata strategies to draw rectilinear grids — disable for this module.
@pytest.fixture(autouse=True)
def _disable_rectilinear_chunks() -> Any:
    if "array.rectilinear_chunks" not in zarr.config:
        yield
        return
    with zarr.config.set({"array.rectilinear_chunks": False}):
        yield


# Suppress LocalFileSystem warnings for these tests (which use filesystem storage).
log_filter = "warn,icechunk::storage::object_store=error"
ic.set_logs_filter(log_filter)
ic_v1.set_logs_filter(log_filter)


# Monkey-patch so parent class `pytest.raises(IcechunkError)` catches both versions
import tests.test_stateful_repo_ops as repo_ops_mod

repo_ops_mod.IcechunkError = (ic.IcechunkError, ic_v1.IcechunkError)  # type: ignore[assignment,attr-defined]


import hypothesis.strategies as st
from hypothesis import event, note
from hypothesis.stateful import (
    initialize,
    precondition,
    rule,
    run_state_machine_as_test,
)

from icechunk.testing.strategies import draw_older_than
from tests.test_stateful_repo_ops import (
    DEFAULT_BRANCH,
    VersionControlStateMachine,
)
from tests.test_zarr.test_stateful import (
    ModifiedZarrHierarchyStateMachine,
)


class CrossVersionVersionControlStateMachine(VersionControlStateMachine):
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

    # icechunk V1 does not support the list_objects_metadata needed to grab create_at timestamps
    @precondition(lambda self: False)
    @rule(data=st.data())
    def garbage_collect(self, data: st.DataObject) -> None:
        pass

    # icechunk V1 does not support the list_objects_metadata needed to grab create_at timestamps
    @precondition(lambda self: False)
    @rule(
        data=st.data(),
        delete_expired_branches=st.sampled_from([True, True, True, False]),
        delete_expired_tags=st.sampled_from([True, True, True, False]),
    )
    def expire_snapshots(
        self,
        data: st.DataObject,
        delete_expired_branches: bool,
        delete_expired_tags: bool,
    ) -> None:
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
        CrossVersionVersionControlStateMachine
    )


class CrossVersionExpireGCStateMachine(VersionControlStateMachine):
    """
    The goal of this test is to gain confidence that IC2 replicated the behaviour of IC1
    w.r.t GC & expiry. Doing so is complicated by bugs in the IC1 impl (due to various side-effects).
    So exactly modeling this is not possible/not worth it. Fixing it is also not worth it.

    So instead:
    1. we run the VersionControlStateMachine as usual
    2. on expire and GC:
       a. we copy the repo over to a new temp directory;
       b. run GC & expire with IC1 (`icechunk_v1`)
       c. assert the return values are identical to that with IC2 (`icechunk`)
    """

    def __init__(self) -> None:
        self._storage_tmpdir = tempfile.TemporaryDirectory()
        self._storage_path = self._storage_tmpdir.name
        super().__init__()

    def _make_storage(self) -> ic.Storage:
        return ic.local_filesystem_storage(self._storage_path)

    @initialize(
        data=st.data(),
        target=VersionControlStateMachine.branches,
    )
    def initialize(self, data: st.DataObject) -> str:
        return super().initialize(data, spec_version=1)  # type: ignore[return-value]

    # v2-only features — disable for spec_version=1
    @rule()
    @precondition(lambda self: False)
    def upgrade_spec_version(self) -> None:
        pass

    def checks(self) -> None:
        assert self.model.spec_version == getattr(self.repo, "spec_version", 1)
        self.check_list_prefix_from_root()
        self.check_tags()
        self.check_branches()
        # check_ancestry disabled: after expire/GC the model's commits
        # are synced from on-disk snapshots, not ancestry, so the sets
        # may diverge from what ancestry traversal returns.
        # self.check_ancestry()
        self.check_ops_log()
        self.check_repo_info()
        self.check_file_invariants()

    # ---- helpers ----

    @contextlib.contextmanager
    def _v1_repo_copy(self) -> Any:
        """Copy storage to a tempdir, open with icechunk_v1, and clean up.

        Yields (v1_repo, v1_path).
        """
        with tempfile.TemporaryDirectory() as v1_path:
            shutil.copytree(self._storage_path, v1_path, dirs_exist_ok=True)
            v1_storage = ic_v1.local_filesystem_storage(v1_path)
            yield ic_v1.Repository.open(v1_storage), v1_path

    def _sync_model_from_repo(self) -> None:
        """Reopen repo and sync model state after expire/GC."""
        assert self.storage is not None
        self.repo = self.actor.open(self.storage)
        self.model.sync_from_repo(self.repo, self.storage)
        self.session = self.repo.writable_session(self.model.branch or DEFAULT_BRANCH)

    @staticmethod
    def _list_object_keys(storage_path: str) -> dict[str, set[str]]:
        """List object keys per prefix using v2 storage (which has list_objects_metadata)."""
        storage = ic.local_filesystem_storage(storage_path)
        result: dict[str, set[str]] = {}
        for prefix in ("snapshots", "manifests", "chunks", "attributes", "transactions"):
            result[prefix] = {
                obj.key for obj in storage.list_objects_metadata(prefix=prefix)
            }
        return result

    def _compare_expire(
        self,
        older_than: datetime.datetime,
        delete_expired_branches: bool,
        delete_expired_tags: bool,
    ) -> None:
        # Reopen to clear any stale caches from prior GC/expire
        assert self.storage is not None
        self.repo = self.actor.open(self.storage)
        with self._v1_repo_copy() as (v1_repo, _):
            v2_error: Exception | None = None
            v1_error: Exception | None = None
            try:
                v2_result = self.repo.expire_snapshots(
                    older_than,
                    delete_expired_branches=delete_expired_branches,
                    delete_expired_tags=delete_expired_tags,
                )
            except (ic.IcechunkError, Exception) as e:
                v2_error = e
            try:
                v1_result = v1_repo.expire_snapshots(
                    older_than,
                    delete_expired_branches=delete_expired_branches,
                    delete_expired_tags=delete_expired_tags,
                )
            except (ic_v1.IcechunkError, Exception) as e:
                v1_error = e

            if v2_error is not None or v1_error is not None:
                note(f"expire_snapshots errors: v2={v2_error!r}, v1={v1_error!r}")
                assert v2_error is not None and v1_error is not None, (
                    f"expire_snapshots error mismatch: v2={v2_error!r}, v1={v1_error!r}"
                )
                event("cross-version expire: both errored (consistent)")
            else:
                note(f"expire_snapshots: v2={v2_result!r}, v1={v1_result!r}")
                assert v2_result == v1_result, (
                    f"expire_snapshots mismatch: v2={v2_result}, v1={v1_result}"
                )
                event(f"cross-version expire: {len(v2_result)} snapshots expired")

        self._sync_model_from_repo()

    def _compare_gc(self, older_than: datetime.datetime) -> None:
        # Reopen to clear any stale caches from prior GC/expire
        assert self.storage is not None
        self.repo = self.actor.open(self.storage)
        with self._v1_repo_copy() as (v1_repo, v1_path):
            v2_error: Exception | None = None
            v1_error: Exception | None = None
            try:
                v2_summary = self.repo.garbage_collect(older_than)
            except (ic.IcechunkError, Exception) as e:
                v2_error = e
            try:
                v1_summary = v1_repo.garbage_collect(older_than)
            except (ic_v1.IcechunkError, Exception) as e:
                v1_error = e

            if v2_error is not None or v1_error is not None:
                note(f"garbage_collect errors: v2={v2_error!r}, v1={v1_error!r}")
                assert v2_error is not None and v1_error is not None, (
                    f"garbage_collect error mismatch: v2={v2_error!r}, v1={v1_error!r}"
                )
                event("cross-version GC: both errored (consistent)")
            else:
                note(
                    f"garbage_collect: v2={v2_summary.snapshots_deleted}, v1={v1_summary.snapshots_deleted}"
                )
                # bytes_deleted / transaction_logs_deleted may differ between v1 and v2.
                for field in (
                    "snapshots_deleted",
                    "chunks_deleted",
                    "manifests_deleted",
                    "attributes_deleted",
                ):
                    v2_val = getattr(v2_summary, field)
                    v1_val = getattr(v1_summary, field)
                    assert v2_val == v1_val, (
                        f"garbage_collect {field} mismatch: v2={v2_val}, v1={v1_val}"
                    )
                event(
                    f"cross-version GC: {v2_summary.snapshots_deleted} snapshots collected"
                )

                # Compare remaining objects on storage after GC
                v2_objects = self._list_object_keys(self._storage_path)
                v1_objects = self._list_object_keys(v1_path)
                for prefix in v2_objects:
                    assert v2_objects[prefix] == v1_objects[prefix], (
                        f"post-GC object mismatch for {prefix!r}:\n"
                        f"  v2 only: {v2_objects[prefix] - v1_objects[prefix]}\n"
                        f"  v1 only: {v1_objects[prefix] - v2_objects[prefix]}"
                    )

        self._sync_model_from_repo()

    @precondition(lambda self: bool(self.model.commits))
    @rule(
        data=st.data(),
        delete_expired_branches=st.sampled_from([True, True, True, False]),
        delete_expired_tags=st.sampled_from([True, True, True, False]),
    )
    def expire_snapshots(
        self,
        data: st.DataObject,
        delete_expired_branches: bool,
        delete_expired_tags: bool,
    ) -> None:
        assert self.storage is not None
        older_than = draw_older_than(data, self.storage)
        note(
            f"Expiring snapshots {older_than=!r}, {delete_expired_branches=!r}, {delete_expired_tags=!r}"
        )
        self._compare_expire(older_than, delete_expired_branches, delete_expired_tags)

    @precondition(lambda self: bool(self.model.commit_times))
    @rule(data=st.data())
    def garbage_collect(self, data: st.DataObject) -> None:
        assert self.storage is not None
        older_than = draw_older_than(data, self.storage)
        note(f"running garbage_collect for {older_than=!r}")
        self._compare_gc(older_than)

    def teardown(self) -> None:
        """Always run expire + GC at the end to guarantee they fire at least once."""
        if self.storage is None:
            return
        far_future = datetime.datetime(2099, 1, 1, tzinfo=datetime.UTC)
        self._compare_expire(far_future, True, True)
        self._compare_gc(far_future)


def test_two_repos_expire_gc_comparison() -> None:
    run_state_machine_as_test(  # type: ignore[no-untyped-call]
        CrossVersionExpireGCStateMachine,
        settings=settings(report_multiple_bugs=False),
    )


class CrossVersionTwoActorZarrHierarchyStateMachine(ModifiedZarrHierarchyStateMachine):
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
        lambda self: (
            self.store.session.has_uncommitted_changes
            or Version(self.ic.__version__).major >= 2
        )
    )
    def commit_with_check(self, data: st.DataObject) -> None:
        return super().commit_with_check(data)


def test_two_actors_zarr_cross_version() -> None:
    def mk_test_instance_sync() -> CrossVersionTwoActorZarrHierarchyStateMachine:
        return CrossVersionTwoActorZarrHierarchyStateMachine(tempfile.mkdtemp())

    run_state_machine_as_test(mk_test_instance_sync)  # type: ignore[no-untyped-call]
