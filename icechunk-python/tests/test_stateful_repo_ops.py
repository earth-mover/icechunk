#!/usr/bin/env python3

import datetime
import itertools
import json
import operator
import sys
import textwrap
from collections.abc import Iterator
from dataclasses import dataclass, fields
from functools import partial
from typing import Any, Literal, Self, cast

import numpy as np
import pytest
from packaging.version import Version

import icechunk
import icechunk as ic
from zarr.core.buffer import Buffer, default_buffer_prototype

pytest.importorskip("hypothesis")
pytest.importorskip("xarray")

import copy

import hypothesis.extra.numpy as npst
import hypothesis.strategies as st
import pytest
from hypothesis import assume, event, note
from hypothesis.stateful import (
    Bundle,
    RuleBasedStateMachine,
    consumes,
    initialize,
    invariant,
    multiple,
    precondition,
    rule,
)

import zarr.testing.strategies as zrst
from icechunk import (
    IcechunkError,
    Repository,
    RepositoryConfig,
    SnapshotInfo,
    Storage,
)
from icechunk.testing import LatencyStorage
from icechunk.testing.strategies import repository_configs
from zarr.testing.stateful import SyncStoreWrapper

# JSON file contents, keep it simple
simple_text = st.text(zrst.zarr_key_chars, min_size=1, max_size=5)
simple_attrs = st.dictionaries(
    simple_text,
    st.integers(min_value=-10_000, max_value=10_000),
    max_size=5,
)

# Branches/tags are stored as files at ``refs/<name>``.  On case-insensitive
# file systems (macOS APFS default), 'T' and 't' collide.
ref_name_text = simple_text.map(lambda x: x.lower() if sys.platform == "darwin" else x)

DEFAULT_BRANCH = "main"
INITIAL_SNAPSHOT = "1CECHNKREP0F1RSTCMT0"


#########
# TODO: port to Zarr


class NewSyncStoreWrapper(SyncStoreWrapper):
    def list_prefix(self, prefix: str) -> list[str]:  # type: ignore[override]
        return self._sync_iter(self.store.list_prefix(prefix))


#####

MAX_TEXT_SIZE = 120

keys = st.lists(zrst.node_names, min_size=1, max_size=4).map("/".join)
metadata_paths = keys.map(lambda x: x + "/zarr.json")
v3_array_metadata = zrst.array_metadata(
    zarr_formats=st.just(3),
    array_shapes=partial(
        npst.array_shapes, max_dims=4, min_side=1, max_side=5
    ),  # set min_side to one
    attributes=simple_attrs,
).map(lambda x: x.to_buffer_dict(prototype=default_buffer_prototype())["zarr.json"])


# The ic.UpdateType cass has `updated_at` and `backup_path`, which we do not care about
# from the perspective of the model. So we define some simpler classes for the purposes of checking
@dataclass
class UpdateModel:
    ictype = ic.UpdateType

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, ic.Update) or not isinstance(other.kind, self.ictype):
            return NotImplemented
        return all(
            getattr(self, f.name) == getattr(other.kind, f.name)
            for f in fields(self)
            # we check ictype with isinstance above
            if f.name != "ictype"
        )


@dataclass(eq=False)
class RepoInitializedUpdateModel(UpdateModel):
    ictype = ic.UpdateType.RepoInitialized


@dataclass(eq=False)
class ConfigChangedUpdateModel(UpdateModel):
    ictype = ic.UpdateType.ConfigChanged


@dataclass(eq=False)
class MetadataChangedUpdateModel(UpdateModel):
    ictype = ic.UpdateType.MetadataChanged


@dataclass(eq=False)
class GCRanUpdateModel(UpdateModel):
    ictype = ic.UpdateType.GCRan


@dataclass(eq=False)
class ExpirationRanUpdateModel(UpdateModel):
    ictype = ic.UpdateType.ExpirationRan


@dataclass(eq=False)
class RepoMigratedUpdateModel(UpdateModel):
    ictype = ic.UpdateType.RepoMigrated
    from_version: int
    to_version: int


@dataclass(eq=False)
class TagCreatedUpdateModel(UpdateModel):
    ictype = ic.UpdateType.TagCreated
    name: str


@dataclass(eq=False)
class TagDeletedUpdateModel(UpdateModel):
    ictype = ic.UpdateType.TagDeleted
    name: str
    previous_snap_id: str


@dataclass(eq=False)
class BranchCreatedUpdateModel(UpdateModel):
    ictype = ic.UpdateType.BranchCreated
    name: str


@dataclass(eq=False)
class BranchDeletedUpdateModel(UpdateModel):
    ictype = ic.UpdateType.BranchDeleted
    name: str
    previous_snap_id: str


@dataclass(eq=False)
class BranchResetUpdateModel(UpdateModel):
    ictype = ic.UpdateType.BranchReset
    name: str
    previous_snap_id: str


@dataclass(eq=False)
class NewCommitUpdateModel(UpdateModel):
    ictype = ic.UpdateType.NewCommit
    branch: str
    new_snap_id: str


@dataclass(eq=False)
class CommitAmendedUpdateModel(UpdateModel):
    ictype = ic.UpdateType.CommitAmended
    branch: str
    previous_snap_id: str
    new_snap_id: str


@dataclass
class ExpireInfo:
    expired_snapshots: set[str]
    deleted_branches: set[str]
    deleted_tags: set[str]


@dataclass
class CommitModel:
    id: str
    written_at: datetime.datetime
    store: dict[str, Any]
    parent_id: str | None

    @classmethod
    def from_snapshot_and_store(cls, snap: SnapshotInfo, store: dict[str, Any]) -> Self:
        return cls(
            id=snap.id, written_at=snap.written_at, parent_id=snap.parent_id, store=store
        )


@dataclass
class TagModel:
    commit_id: str
    # message: str | None


class Model:
    def __init__(self, **kwargs: Any) -> None:
        self.store: dict[str, Any] = {}

        self.spec_version = 1  # will be overwritten on `@initialize`
        self.initial_spec_version = 1  # will be overwritten on `@initialize`

        self.metadata: dict[str, Any] = {}

        self.initial_snapshot_id: str | None = None
        self.changes_made: bool = False

        self.HEAD: None | str = None
        self.branch: None | str = None

        # we maintain a list of `commits` == snapshots in repo object file
        # and `ondisk_snaps` = commits + expired_snaphsots
        # expired snapshots are removed from the repo object file and can be garbage collected
        self.commits: dict[str, CommitModel] = {}
        self.ondisk_snaps: dict[str, CommitModel] = {}
        self.tags: dict[str, TagModel] = {}
        self.branch_heads: dict[str, str] = {}
        self.deleted_tags: set[str] = set()

        self.ops_log: list[UpdateModel] = []
        self.migrated: bool = False

        # a tag once created, can never be recreated even after expiration
        self.created_tags: set[str] = set()

    def initialize(self, *, snap: ic.SnapshotInfo) -> None:
        HEAD = snap.id
        self.initial_snapshot_id = HEAD
        self.commits[HEAD] = CommitModel.from_snapshot_and_store(snap, {})
        self.ondisk_snaps[HEAD] = self.commits[HEAD]
        self.HEAD = HEAD
        self.create_branch(DEFAULT_BRANCH, HEAD)
        self.checkout_branch(DEFAULT_BRANCH)
        self.ops_log = [RepoInitializedUpdateModel()]

    def __repr__(self) -> str:
        return textwrap.dedent(f"""
        <Model branch: {self.branch!r}, HEAD: {self.HEAD!r}, changes_made: {self.changes_made!r}>
        Branches: {tuple(self.branch_heads.keys())!r}
        Tags: {tuple(self.tags.keys())!r}""").strip("\n")

    def __setitem__(self, key: str, value: Buffer) -> None:
        # Icechunk doesn't overwrite docs with the same value
        # and we need to keep `changes_made` in sync.
        # Icechunk checks after decoding the metadata to rust Structs so we must do the same.
        # Different byte strings can decode to the same json dict (order of user attributes may be different)
        if key in self.store and json.loads(self.store[key].to_bytes()) == json.loads(
            value.to_bytes()
        ):
            note(f"skipping setting {key!r}, value is unchanged")
            return
        self.changes_made = True
        self.store[key] = value

    def __getitem__(self, key: str) -> Buffer:
        return cast(Buffer, self.store[key])

    def get(self, key: str) -> Buffer | None:
        return self.store.get(key)

    def delete_doc(self, key: str) -> None:
        if key in self.store:
            del self.store[key]
            self.changes_made = True

    def upgrade(self, dry_run: bool) -> None:
        if not dry_run:
            # only reachable snapshots are migrated over
            self.commits = {
                k: v for k, v in self.commits.items() if k in self.reachable_snapshots()
            }
            # The ops log now contains synthetic entries with synthetic ordering
            # reconstructed from the snapshot graph, so we skip exact comparison after migration.
            self.ops_log = [RepoMigratedUpdateModel(self.spec_version, 2)]
            self.migrated = True
            self.spec_version = 2

    @property
    def has_commits(self) -> bool:
        return bool(self.commits)

    @property
    def commit_times(self) -> list[datetime.datetime]:
        """Return sorted list of all commit times."""
        return sorted(c.written_at for c in self.commits.values())

    def _commit(self, snap: SnapshotInfo) -> None:
        ref = snap.id
        self.commits[ref] = CommitModel.from_snapshot_and_store(
            snap, copy.deepcopy(self.store)
        )
        self.ondisk_snaps[ref] = self.commits[ref]
        self.changes_made = False
        self.HEAD = ref

        assert self.branch is not None
        self.branch_heads[self.branch] = ref

    def commit(self, snap: SnapshotInfo) -> None:
        assert self.branch is not None
        self._commit(snap)
        self.ops_log.append(NewCommitUpdateModel(self.branch, snap.id))

    def amend(self, snap: SnapshotInfo) -> None:
        """Amend the HEAD commit."""
        # this is simple because we aren't modeling the branch as a list of commits
        assert self.branch is not None
        prev_snap_id = self.branch_heads[self.branch]
        self._commit(snap)
        self.ops_log.append(CommitAmendedUpdateModel(self.branch, prev_snap_id, snap.id))

    def set_metadata(self, meta: dict[str, Any]) -> None:
        self.metadata = meta
        self.ops_log.append(MetadataChangedUpdateModel())

    def checkout_commit(self, ref: str) -> None:
        assert str(ref) in self.commits
        # deepcopy so that we allow changes, but the committed store remains unchanged
        # TODO: consider Frozen stores in self.commit?
        self.store = copy.deepcopy(self.commits[ref].store)
        self.changes_made = False
        self.HEAD = ref
        self.branch = None

    def create_branch(self, name: str, commit: str) -> None:
        assert commit in self.commits
        self.branch_heads[name] = commit
        self.ops_log.append(BranchCreatedUpdateModel(name))

    def checkout_branch(self, ref: str) -> None:
        self.checkout_commit(self.branch_heads[ref])
        self.branch = ref

    # TODO: add `from_snapshot_id` to this
    def reset_branch(self, branch: str, commit: str) -> None:
        assert commit in self.commits
        prev_snap_id = self.branch_heads[branch]
        self.branch_heads[branch] = commit
        self.ops_log.append(BranchResetUpdateModel(branch, prev_snap_id))

    def delete_branch(self, branch_name: str) -> None:
        self.ops_log.append(
            BranchDeletedUpdateModel(branch_name, self.branch_heads[branch_name])
        )
        self._delete_branch(branch_name)

    def _delete_branch(self, branch_name: str) -> None:
        del self.branch_heads[branch_name]

    def delete_tag(self, tag: str) -> None:
        self.ops_log.append(TagDeletedUpdateModel(tag, self.tags[tag].commit_id))
        self._delete_tag(tag)

    def _delete_tag(self, tag: str) -> None:
        self.deleted_tags.add(tag)
        del self.tags[tag]

    def create_tag(self, tag_name: str, commit_id: str) -> None:
        assert commit_id in self.commits
        self.tags[tag_name] = TagModel(commit_id=str(commit_id))
        self.created_tags.add(tag_name)
        self.ops_log.append(TagCreatedUpdateModel(tag_name))

    def checkout_tag(self, ref: str) -> None:
        self.checkout_commit(self.tags[str(ref)].commit_id)

    def list_prefix(self, prefix: str) -> tuple[str, ...]:
        assert prefix == ""
        return tuple(self.store)

    def refs_iter(self) -> Iterator[str]:
        tag_iter = map(operator.attrgetter("commit_id"), self.tags.values())
        return itertools.chain(self.branch_heads.values(), tag_iter)

    def expire_snapshots(
        self,
        expired_snaps: set[str],
        *,
        older_than: datetime.datetime,
        delete_expired_branches: bool,
        delete_expired_tags: bool,
    ) -> ExpireInfo:
        # modeling expiration like icechunk does is complicated because of re-parenting logic.
        # Instead we assume that icechunk expired the right commits, and then we check that
        # the repo invariants hold.
        branch_pointees = set(self.branch_heads.values())
        tag_pointees = set(map(operator.attrgetter("commit_id"), self.tags.values()))
        for snapid in expired_snaps:
            snap = self.commits[snapid]
            assert snap.written_at < older_than
            assert snap.parent_id is not None
            # V1's released_snapshots can include ref tips that are merely
            # rewritten (not deleted), so we skip these checks for V1.
            if self.spec_version >= 2:
                if not delete_expired_tags:
                    assert snap.id not in tag_pointees
                if not delete_expired_branches:
                    assert snap.id not in branch_pointees

        # Ref deletion (tags/branches) is based on `expired_snapshot_ids`, NOT
        # `expired_snaps` (= released_snapshots in Rust).
        #  - released_snapshots: snapshots actually removed from the repo. Excludes
        #       root snapshots and snapshots protected by preserved refs (e.g. main).
        #  - expired_snapshot_ids: all non-root snapshots old enough to expire,
        #       regardless of ref protection. Used only for deciding which
        #       tag/branch *references* to delete. The snapshots are still preserved.
        # This means a feature branch sharing main's tip can deleted even though
        # the snapshot is preserved by main (#1520).
        #
        # In V2, root snapshots (no parent) are excluded so refs pointing
        # to the initial commit are never deleted.
        # V1 does not exclude root snapshots, so refs pointing to the initial commit
        # can be deleted.
        if self.spec_version >= 2:
            expired_snapshot_ids = {
                snap_id
                for snap_id, snap in self.commits.items()
                if snap.written_at < older_than and snap.parent_id is not None
            }
        else:
            # V1 checks only flushed_at (not parent_id) when deciding ref
            # deletion, so root-pointing refs can be deleted (#1534).
            expired_snapshot_ids = {
                snap_id
                for snap_id, snap in self.commits.items()
                if snap.written_at < older_than
            }

        # V1's released_snapshots can include branch/tag tips that are merely
        # rewritten (parent pointer changed), not truly removed. Don't remove
        # those from commits — they're still valid snapshots pointed to by refs.
        # V2's released_snapshots only contains snapshots truly removed from
        # the repo info, so all can be popped.
        ref_pointees = set(self.refs_iter()) if self.spec_version < 2 else set()
        for id in expired_snaps:
            # notice we don't delete from self.ondisk_snaps, those can still be deleted by GC
            # however we do pop from `commits` since that is a list of unexpired snaps
            if id not in ref_pointees:
                self.commits.pop(id, None)

        # we reparent to the initial snapshot for simplicity. This should be good enough to make
        # self.reachable_snapshots() accurate.
        for c in self.commits.values():
            if c.parent_id in expired_snaps:
                c.parent_id = self.initial_snapshot_id

        if delete_expired_tags:
            tags_to_delete = {
                k for k, v in self.tags.items() if v.commit_id in expired_snapshot_ids
            }
            note(f"deleting tags {tags_to_delete=!r}")
            for tag in tags_to_delete:
                self._delete_tag(tag)
        else:
            tags_to_delete = set()

        if delete_expired_branches:
            branches_to_delete = {
                k
                for k, v in self.branch_heads.items()
                if k != DEFAULT_BRANCH and v in expired_snapshot_ids
            }
            note(f"deleting branches {branches_to_delete=!r}")
            for branch in branches_to_delete:
                self._delete_branch(branch)
        else:
            branches_to_delete = set()

        self.ops_log.append(ExpirationRanUpdateModel())
        return ExpireInfo(
            expired_snapshots=expired_snaps,
            deleted_branches=branches_to_delete,
            deleted_tags=tags_to_delete,
        )

    def reachable_snapshots(self) -> set[str]:
        assert self.initial_snapshot_id is not None
        reachable_snaps: set[str] = set((self.initial_snapshot_id,))
        for commit_id in self.refs_iter():
            while commit_id is not None:
                reachable_snaps.add(commit_id)
                commit_id = self.commits[commit_id].parent_id  # type: ignore[assignment]
        return reachable_snaps

    def garbage_collect(
        self,
        older_than: datetime.datetime,
        created_at_by_id: dict[str, datetime.datetime],
    ) -> set[str]:
        """Predict which snapshots Rust GC will delete.

        Uses storage-level created_at (not written_at/flushed_at) to match
        Rust's gc_snapshots which checks ``snapshot.created_at < cutoff``.
        The created_at_by_id dict must be captured *before* calling Rust GC,
        since GC deletes the files from storage.
        """
        reachable_snaps = self.reachable_snapshots()
        deleted = set()
        for k in set(self.ondisk_snaps) - reachable_snaps:
            if created_at_by_id[k] < older_than:
                self.commits.pop(k, None)
                self.ondisk_snaps.pop(k, None)
                deleted.add(k)

        if self.spec_version >= 2:
            # V2's delete_snapshots_from_repo_info rewrites parent pointers
            # for kept snapshots whose parent was GC'd.
            for c in self.commits.values():
                if c.parent_id is not None and c.parent_id in deleted:
                    c.parent_id = self.initial_snapshot_id
        else:
            # V1 doesn't rewrite parent pointers, so commits whose parents
            # were GC'd have broken ancestry and are effectively unusable.
            # Remove them from commits so we don't try to use them
            orphaned = True
            while orphaned:
                orphaned = {
                    k
                    for k, c in self.commits.items()
                    if c.parent_id is not None
                    and (c.parent_id in deleted or c.parent_id not in self.ondisk_snaps)
                }
                for k in orphaned:
                    self.commits.pop(k, None)
        note(f"Deleted snapshots in model: {deleted!r}")
        self.ops_log.append(GCRanUpdateModel())
        return deleted


class VersionControlStateMachine(RuleBasedStateMachine):
    """
    We use bundles to track the state, since Hypothesis will then
    preferably draw the same value for different rules.
    e.g. create branch 'X', then delete branch 'X'
    """

    commits = Bundle("commits")
    tags = Bundle("tags")
    branches = Bundle("branches")

    def __init__(self, actor: Any = None, ic_module: Any = None) -> None:
        super().__init__()

        note("----------")
        self.model = Model()
        self.storage: Storage | None = None
        self.actor = actor or Repository
        self.ic = ic_module or icechunk

    def _initialize_model_and_data(self, data: st.DataObject) -> None:
        """Set up model state and initial data after repo is created/opened."""
        self.session = self.repo.writable_session(DEFAULT_BRANCH)

        snap = next(iter(self.repo.ancestry(branch=DEFAULT_BRANCH)))
        self.initial_snapshot = snap

        self.model.initialize(snap=snap)

        # initialize with some data always
        # TODO: always setting array metadata, since we cannot overwrite an existing group's zarr.json
        #       with an array's zarr.json
        # TODO: consider adding a deeper understanding of the zarr model rather than just setting docs?
        self.set_doc(path="zarr.json", value=data.draw(v3_array_metadata))

    def _make_storage(self) -> Storage:
        return self.ic.in_memory_storage()  # type: ignore[no-any-return]

    @initialize(
        data=st.data(),
        target=branches,
        spec_version=st.sampled_from([1, 2]),
        # Both latencies are zero (~75%) or both non-zero (~25%)
        # to exercise the flushed_at vs created_at timing gap in GC.
        latency=st.one_of(
            st.just((0, 0)),
            st.just((0, 0)),
            st.just((0, 0)),
            st.tuples(st.integers(5, 10), st.just(0)),
        ),
    )
    def initialize(
        self,
        data: st.DataObject,
        spec_version: Literal[1, 2],
        latency: tuple[int, int] = (0, 0),
    ) -> str:
        write_latency_ms, read_latency_ms = latency
        inner = self._make_storage()
        if write_latency_ms or read_latency_ms:
            self.storage = LatencyStorage(
                inner,
                write_latency_ms=write_latency_ms,
                read_latency_ms=read_latency_ms,
            )
        else:
            self.storage = inner
        config = data.draw(repository_configs(ic_module=self.ic))
        self.model.initial_spec_version = spec_version
        self.model.spec_version = spec_version

        if Version(self.ic.__version__).major >= 2:
            self.repo = self.actor.create(
                self.storage,
                spec_version=spec_version,
                config=config,
            )
        else:
            self.repo = self.actor.create(
                self.storage,
                config=config,
            )

        self._initialize_model_and_data(data)
        return DEFAULT_BRANCH

    def new_store(self) -> None:
        self.session = self.repo.writable_session(DEFAULT_BRANCH)

    @property
    def sync_store(self) -> NewSyncStoreWrapper:
        return NewSyncStoreWrapper(self.session.store)

    @precondition(lambda self: self.model.branch is not None)
    @rule(data=st.data(), ncommits=st.integers(3, 10), target=commits)
    def set_and_commit(self, data: st.DataObject, ncommits: int) -> Any:
        amend_or_commit = data.draw(
            st.lists(st.booleans(), min_size=ncommits, max_size=ncommits)
        )
        # We have so many rules now it is hard to build up long history
        # instead we explicitly add a rule to make a lot of commits
        commit_ids = []
        for should_amend in amend_or_commit:
            # We need to ensure we overwrite with different data, otherwise icechunk doesn't register a change.
            # Delete first then set, to avoid slow hypothesis filter rejection.
            path, meta = data.draw(st.tuples(metadata_paths, v3_array_metadata))
            self.delete_doc(path)
            self.set_doc(path, meta)
            msg = data.draw(st.text(max_size=MAX_TEXT_SIZE))
            if (
                should_amend
                and len(self.model.commits) > 1
                and self.model.spec_version >= 2
            ):
                self.amend(msg)
            else:
                commit_ids.append(self.commit(msg))
        return multiple(*commit_ids)

    @rule(path=metadata_paths, value=v3_array_metadata)
    def set_doc(self, path: str, value: Buffer) -> None:
        note(f"setting path {path!r} with {value.to_bytes()!r}")
        if self.model.branch is not None:
            self.sync_store.set(path, value)
            self.model[path] = value
        else:
            # not at branch head, modifications not possible.
            with pytest.raises(IcechunkError, match="read-only store"):
                self.sync_store.set(path, value)

    def delete_doc(self, path: str) -> None:
        note(f"deleting path {path!r}")
        if self.model.branch is not None:
            self.sync_store.delete(path)
            self.model.delete_doc(path)

    @precondition(lambda self: self.model.spec_version >= 2)
    @rule(meta=simple_attrs)
    def set_metadata(self, meta: dict[str, Any]) -> None:
        note(f"setting metadata {meta!r}")
        self.repo.set_metadata(meta)
        self.model.set_metadata(meta)

    @rule(dry_run=st.booleans(), delete_unused_v1_files=st.booleans())
    @precondition(lambda self: self.model.spec_version == 1)
    def upgrade_spec_version(self, dry_run: bool, delete_unused_v1_files: bool) -> None:
        # don't test simple cases of catching error upgradging a v2 spec
        # that should be covered in unit tests
        self.repo = icechunk.upgrade_icechunk_repository(
            self.repo, dry_run=dry_run, delete_unused_v1_files=delete_unused_v1_files
        )
        # Reopening discards uncommitted changes - reset session and model
        branch = (
            self.model.branch
            if self.model.branch in self.model.branch_heads
            else DEFAULT_BRANCH
        )
        self.session = self.repo.writable_session(branch)
        self.model.checkout_branch(branch)

        self.model.upgrade(dry_run)
        if not dry_run:
            assert self.repo.spec_version == 2

    @rule(data=st.data())
    def reopen_repository(self, data: st.DataObject) -> None:
        config = data.draw(repository_configs(ic_module=self.ic))
        self._reopen_repository(config)

    def _reopen_repository(self, config: RepositoryConfig | None = None) -> None:
        """Reopen the repository from storage to get fresh state.

        This discards any uncommitted changes.
        """
        assert self.storage is not None, "storage must be initialized"
        self.repo = self.actor.open(self.storage, config=config)
        note(f"Reopened repository (spec_version={self.model.spec_version})")

        # Reopening discards uncommitted changes - reset model to last committed state
        branch = (
            self.model.branch
            if self.model.branch in self.model.branch_heads
            else DEFAULT_BRANCH
        )
        self.session = self.repo.writable_session(branch)
        self.model.checkout_branch(branch)

    @rule(message=st.text(max_size=MAX_TEXT_SIZE), target=commits)
    @precondition(lambda self: self.model.changes_made)
    def commit(self, message: str) -> str:
        branch = self.session.branch
        assert branch is not None
        commit_id = self.session.commit(message)
        snapinfo = next(iter(self.repo.ancestry(branch=branch)))
        assert snapinfo.id == commit_id
        self.session = self.repo.writable_session(branch)
        note(f"Created commit: {snapinfo!r}")
        self.model.commit(snapinfo)
        return commit_id

    @rule(message=st.text(max_size=MAX_TEXT_SIZE), target=commits)
    # TODO: update changes made rule depending on result of
    # https://github.com/earth-mover/icechunk/issues/1532
    @precondition(
        lambda self: (
            (self.model.changes_made)
            and (self.model.spec_version >= 2)
            and len(self.model.commits) > 1
        )
    )
    def amend(self, message: str) -> str:
        branch = self.session.branch
        assert branch is not None
        old_head = next(iter(self.repo.ancestry(branch=branch)))
        note(f"Amending commit on branch {branch!r} with id {old_head!r}")
        # After expiration, HEAD can be reparented to parent_id=None even
        # though the model thinks it has a parent. Amending a root panics
        # in Rust (unreachable!), so skip when the repo state disagrees.
        assume(old_head.parent_id is not None)

        commit_id = self.session.amend(message)
        snapinfo = next(iter(self.repo.ancestry(branch=branch)))
        assert snapinfo.id == commit_id
        self.session = self.repo.writable_session(branch)

        # Update model
        self.model.amend(snapinfo)
        return commit_id

    @rule(ref=commits)
    def checkout_commit(self, ref: str) -> None:
        if ref not in self.model.commits:
            # V1 expired snapshots stay on disk so checkout succeeds, but
            # the model no longer tracks their store contents.
            assume(self.model.spec_version >= 2)
            note(f"Checking out commit {ref}, expecting error")
            with pytest.raises(IcechunkError):
                self.repo.readonly_session(snapshot_id=ref)
        else:
            note(f"Checking out commit {ref}")
            self.session = self.repo.readonly_session(snapshot_id=ref)
            assert self.session.read_only
            self.model.checkout_commit(ref)
            self.check_commit(ref)

    @rule(ref=tags)
    def checkout_tag(self, ref: str) -> None:
        """
        Tags and branches are combined here since checkout magically works for both.
        This test is relying on the model tracking tags and branches accurately.
        """
        maybe_tag = self.model.tags.get(ref)
        if maybe_tag and maybe_tag.commit_id in self.model.commits:
            note(f"Checking out tag {ref!r}")
            self.session = self.repo.readonly_session(tag=ref)
            assert self.session.read_only
            self.model.checkout_tag(ref)
        else:
            note("Expecting error.")
            with pytest.raises(IcechunkError):
                self.repo.readonly_session(tag=ref)

    @rule(ref=branches)
    def checkout_branch(self, ref: str) -> None:
        # TODO: sometimes readonly?
        branch_head = self.model.branch_heads.get(ref)
        if branch_head is not None and branch_head in self.model.commits:
            note(f"Checking out branch {ref!r}")
            self.session = self.repo.writable_session(ref)
            assert not self.session.read_only
            self.model.checkout_branch(ref)
        else:
            with pytest.raises(IcechunkError):
                note(f"Expecting error when checking out branch {ref!r}")
                self.repo.writable_session(ref)

    @rule(name=ref_name_text, commit=commits, target=branches)
    def create_branch(self, name: str, commit: str) -> str:
        note(f"Creating branch {name!r} for commit {commit!r}")

        # V1 expired snapshots stay on disk so create_branch succeeds, but
        # the model no longer tracks their store contents.
        assume(self.model.spec_version >= 2 or commit in self.model.commits)

        # we can create a tag and branch with the same name
        if name not in self.model.branch_heads and commit in self.model.commits:
            self.repo.create_branch(name, commit)
            self.model.create_branch(name, commit)
        else:
            note("Expecting error.")
            with pytest.raises(IcechunkError):
                self.repo.create_branch(name, commit)
        # returning this `name` to the Bundle is OK even if the branch was not created
        # This will test out checking out and deleting a branch that does not exist.
        return name

    @precondition(lambda self: self.model.has_commits)
    @rule(name=ref_name_text, commit_id=commits, target=tags)
    def create_tag(self, name: str, commit_id: str) -> str:
        note(f"Creating tag {name!r} for commit {commit_id!r}")
        # V1 expired snapshots stay on disk so create_tag succeeds, but
        # the model no longer tracks their store contents.
        assume(self.model.spec_version >= 2 or commit_id in self.model.commits)
        if (
            name in self.model.created_tags
            or name in self.model.tags
            or commit_id not in self.model.commits
        ):
            note("Expecting error.")
            with pytest.raises(IcechunkError):
                self.repo.create_tag(name, commit_id)
        else:
            self.repo.create_tag(name, commit_id)
            self.model.create_tag(name, commit_id)
        # returning this `name` to the Bundle is OK even if the tag was not created
        # This will test out checking out and deleting a tag that does not exist.
        return name

    @precondition(lambda self: self.model.changes_made)
    @rule()
    def discard_changes(self) -> None:
        note(f"Discarding changes (branch={self.model.branch})")
        self.session.discard_changes()
        if self.model.branch is not None:
            self.model.checkout_branch(self.model.branch)
        else:
            if self.session.snapshot_id in self.model.commits:
                self.model.checkout_commit(self.session.snapshot_id)
            else:
                # this can happen if we expire the snapshot of the current session
                self.session = self.repo.writable_session(DEFAULT_BRANCH)
                self.model.checkout_branch(DEFAULT_BRANCH)

    # if there are changes in a session tied to the same branch
    # then an attempt to commit from that session will raise a conflict
    # (as is expected)
    @precondition(lambda self: not self.model.changes_made)
    @rule(branch=branches, commit=commits)
    def reset_branch(self, branch: str, commit: str) -> None:
        # V1 expired snapshots stay on disk so reset_branch would succeed,
        # but modelling that divergence isn't worthwhile — just skip.
        assume(self.model.spec_version >= 2 or commit in self.model.commits)
        if branch not in self.model.branch_heads or commit not in self.model.commits:
            note(f"resetting branch {branch}, expecting error.")
            with pytest.raises(IcechunkError):
                self.repo.reset_branch(branch, commit)
        else:
            note(
                f"resetting branch {branch} from {self.model.branch_heads[branch]} to {commit}"
            )
            self.repo.reset_branch(branch, commit)
            self.model.reset_branch(branch, commit)
            self.maybe_checkout_branch(branch, checkout_branch=branch)

    def maybe_checkout_branch(
        self, modified_branch: str, *, checkout_branch: str = DEFAULT_BRANCH
    ) -> None:
        # TODO: if we delete the current branch, we hold on to an invalid session
        #       this is confusing UX. any attempts to commit will create a conflict
        if self.model.branch == modified_branch:
            self.session = self.repo.writable_session(checkout_branch)
            self.model.checkout_branch(checkout_branch)

    @rule(branch=consumes(branches))
    def delete_branch(self, branch: str) -> None:
        note(f"Deleting branch {branch!r}")
        if branch in self.model.branch_heads:
            if branch == DEFAULT_BRANCH:
                note("Expecting error.")
                with pytest.raises(
                    IcechunkError, match=f"{DEFAULT_BRANCH} branch cannot be deleted"
                ):
                    self.repo.delete_branch(branch)
            else:
                self.repo.delete_branch(branch)
                self.model.delete_branch(branch)
                self.maybe_checkout_branch(branch)

        else:
            note("Expecting error.")
            with pytest.raises(IcechunkError):
                self.repo.delete_branch(branch)

    @rule(tag=consumes(tags))
    def delete_tag(self, tag: str) -> None:
        note(f"Deleting tag {tag!r}")
        if tag not in self.model.tags:
            with pytest.raises(IcechunkError):
                self.repo.delete_tag(tag)
        else:
            self.repo.delete_tag(tag)
            self.model.delete_tag(tag)

    def _draw_older_than(self, data: st.DataObject) -> datetime.datetime:
        # Draw cutoffs from storage-level created_at (not written_at/flushed_at)
        # because that is what Rust GC compares against.
        assert self.storage is not None
        created_at_snapshots: dict[str, datetime.datetime] = {
            obj.key: obj.created_at
            for obj in self.storage.list_objects_metadata(prefix="snapshots")
        }
        created_at_txs: dict[str, datetime.datetime] = {
            obj.key: obj.created_at
            for obj in self.storage.list_objects_metadata(prefix="transactions")
        }
        created_at_times = sorted(
            # These are written concurrently (session.rs) and get slightly different
            # created_at timestamps. Take the max so we delete both.
            max(
                created_at_snapshots[key],
                created_at_txs.get(
                    key, datetime.datetime(2000, 1, 1, tzinfo=datetime.UTC)
                ),
            )
            for key in created_at_snapshots
        )[::-1]  # reverse to maximize chances of GCing more objects
        # The order here is important; again we prioritize GCing more objects first
        result: datetime.datetime = data.draw(
            st.one_of(
                st.just(max(created_at_times) + datetime.timedelta(days=1)),
                # Add 1μs to ensure we delete both the tx log & snapshot
                st.sampled_from(created_at_times).map(
                    lambda time: time + datetime.timedelta(microseconds=1)
                ),
                st.just(datetime.datetime(2000, 1, 1, tzinfo=datetime.UTC)),
            )
        )
        return result

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
        older_than = self._draw_older_than(data)
        note(
            f"Expiring snapshots {older_than=!r}, {delete_expired_branches=!r}, {delete_expired_tags=!r}"
        )

        # Track branches and tags before expiration
        branches_before = set(self.repo.list_branches())
        tags_before = set(self.repo.list_tags())

        actual = self.repo.expire_snapshots(
            older_than,
            delete_expired_branches=delete_expired_branches,
            delete_expired_tags=delete_expired_tags,
        )
        # note(f"repo  expired snaps={actual!r}")

        # We _never_ delete main's HEAD or the initial snapshot.
        # However, V1's released_snapshots has different semantics: it can include
        # branch tips that are merely rewritten (parent pointer changed),
        # not truly deleted. So only assert this for V2.
        if self.model.spec_version >= 2:
            assert self.model.branch_heads["main"] not in actual
        # we never expire the initial snapshot
        assert self.initial_snapshot.id not in actual
        # V2 reparents correctly so no retained snapshot has an expired parent.
        # V1's released_snapshots can include the tip itself (which is
        # rewritten, not deleted), so this invariant doesn't hold for V1.
        if self.model.spec_version >= 2:
            for snap in set(self.model.commits) - actual:
                actualsnap = self.repo.lookup_snapshot(snap)
                assert actualsnap.parent_id not in actual

        # Track branches and tags after expiration
        branches_after = set(self.repo.list_branches())
        tags_after = set(self.repo.list_tags())
        actual_deleted_branches = branches_before - branches_after
        actual_deleted_tags = tags_before - tags_after

        expected = self.model.expire_snapshots(
            actual,
            older_than=older_than,
            delete_expired_branches=delete_expired_branches,
            delete_expired_tags=delete_expired_tags,
        )
        # note(f"from model: {expected}")
        # note(f"actual: {actual}")
        # note(f"{actual_deleted_branches=!r}, {actual_deleted_tags=!r}")

        event(f"commits expired: {len(actual)}")
        event(f"tags expired: {len(actual_deleted_tags)}")
        event(f"branches expired: {len(actual_deleted_branches)}")

        # Check that expired snapshots are actually removed from ancestry
        remaining_snapshot_ids = set()
        for branch in branches_after:
            remaining_snapshot_ids.add(
                snapshot.id for snapshot in self.repo.ancestry(branch=branch)
            )

        expired_but_remaining = actual & remaining_snapshot_ids
        assert not expired_but_remaining, (
            f"Snapshots marked as expired but still in ancestry: {expired_but_remaining}"
        )

        assert actual_deleted_branches == expected.deleted_branches, (
            f"deleted branches mismatch: actual={actual_deleted_branches}, expected={expected.deleted_branches}"
        )
        assert actual_deleted_tags == expected.deleted_tags, (
            f"deleted tags mismatch: actual={actual_deleted_tags}, expected={expected.deleted_tags}"
        )

        for branch in actual_deleted_branches:
            self.maybe_checkout_branch(branch)

    @precondition(lambda self: bool(self.model.commit_times))
    @rule(data=st.data())
    def garbage_collect(self, data: st.DataObject) -> None:
        older_than = self._draw_older_than(data)
        note(f"running garbage_collect for {older_than=!r}")
        # Snapshot created_at before Rust GC deletes files from storage
        assert self.storage is not None
        created_at_by_id = {
            obj.key.removeprefix("snapshots/"): obj.created_at
            for obj in self.storage.list_objects_metadata(prefix="snapshots")
        }
        summary = self.repo.garbage_collect(older_than)
        note(f"actual GC result {summary=!r}")
        expected = self.model.garbage_collect(older_than, created_at_by_id)
        assert summary.snapshots_deleted == len(expected), (
            summary.snapshots_deleted,
            expected,
        )

        event(f"snapshots garbage collected: {len(expected)}")

        # deleted a checked out snapshot :?
        for snapshot in expected:
            if self.model.HEAD == snapshot:
                note(f"deleted the checked out snapshot {snapshot}, creating new session")
                self.session = self.repo.writable_session(DEFAULT_BRANCH)
                self.model.checkout_branch(DEFAULT_BRANCH)

    def check_commit(self, commit: str) -> None:
        # utility function, not an invariant
        assume(commit in self.model.commits)
        expected = self.model.commits[commit]
        actual = self.repo.lookup_snapshot(commit)
        assert actual.id == expected.id
        # assert actual.parent_id == expected.parent_id
        # even after expiration, written_at is unmodified
        assert actual.written_at == expected.written_at

    @invariant()
    def checks(self) -> None:
        # this method only exists to reduce verbosity of hypothesis output
        # It cannot be called `check_invariants` because that clashes
        # with an existing method on the superclass

        # Temporarily disable read latency during invariant checks for speed
        if isinstance(self.storage, LatencyStorage):
            saved = self.storage.read_latency_ms
            self.storage.read_latency_ms = 0

        assert self.model.spec_version == getattr(self.repo, "spec_version", 1)
        self.check_list_prefix_from_root()
        self.check_tags()
        self.check_branches()
        self.check_ancestry()
        self.check_ops_log()
        self.check_repo_info()
        self.check_file_invariants()

        if isinstance(self.storage, LatencyStorage):
            self.storage.read_latency_ms = saved

    def check_list_prefix_from_root(self) -> None:
        model_list = self.model.list_prefix("")
        ice_list = self.sync_store.list_prefix("")

        assert sorted(model_list) == sorted(ice_list)

        for k in model_list:
            # need to load to dict to compare since ordering of entries might differ
            expected = json.loads(self.model[k].to_bytes())
            value = self.sync_store.get(k, default_buffer_prototype())
            assert value is not None, k
            actual = json.loads(value.to_bytes())
            actual_fv = actual.pop("fill_value")
            expected_fv = expected.pop("fill_value")
            if actual_fv != expected_fv:
                # TODO: is this right? we are losing accuracy in serialization
                np.testing.assert_allclose(actual_fv, expected_fv)
            assert actual == expected

    def check_tags(self) -> None:
        expected_tags = self.model.tags
        actual_tags = {
            tag: TagModel(commit_id=self.repo.lookup_tag(tag))
            for tag in self.repo.list_tags()
        }
        assert actual_tags == expected_tags

    def check_branches(self) -> None:
        repo_branches = {k: self.repo.lookup_branch(k) for k in self.repo.list_branches()}
        assert self.model.branch_heads == repo_branches

    def _assert_ancestry_invariants(self, ancestry: list[SnapshotInfo]) -> None:
        ancestry_set = set([snap.id for snap in ancestry])
        diff = ancestry_set - set(self.model.commits)
        assert not diff, ("ancestry is not a subset of commits", diff)
        # snapshot timestamps are monotonically decreasing in ancestry
        assert all(a.written_at > b.written_at for a, b in itertools.pairwise(ancestry))
        # ancestry must be unique
        assert len(ancestry_set) == len(ancestry)
        n = len(ancestry)
        bucket = f"{n // 10 * 10}-{n // 10 * 10 + 9}"
        event(f"ancestry length: {bucket}")

    def check_ancestry(self) -> None:
        for branch in self.model.branch_heads:
            ancestry = list(self.repo.ancestry(branch=branch))
            self._assert_ancestry_invariants(ancestry)
            assert ancestry[-1].parent_id is None

        for tag in self.model.tags:
            ancestry = list(self.repo.ancestry(tag=tag))
            self._assert_ancestry_invariants(ancestry)

    def check_repo_info(self) -> None:
        ver = self.model.spec_version
        if ver == 1:
            return
        elif ver == 2:
            expected = "2.0"
        else:
            raise NotImplementedError()

        info = self.repo.inspect_repo_info()

        assert info["spec_version"] == expected
        assert set(info["deleted_tags"]) == self.model.deleted_tags
        assert info["metadata"] == self.model.metadata

        # the remaining fields (snapshots, branches, tags, ops_log) are checked by the other invariants

    def check_file_invariants(self) -> None:
        if Version(self.ic.__version__).major == 1:
            return

        assert self.storage is not None
        paths = set(path_and_bytes[0] for path_and_bytes in self.storage.list_objects())

        # This is complicated with repo upgrades
        snapshots = [
            p.removeprefix("snapshots/") for p in paths if p.startswith("snapshots/")
        ]
        transactions = [
            p.removeprefix("transactions/")
            for p in paths
            if p.startswith("transactions/")
        ]
        if self.model.initial_spec_version == 1:
            expired = any(
                isinstance(op, ExpirationRanUpdateModel) for op in self.model.ops_log
            )
            if expired:
                # V1 expire rewrites snapshot files without creating matching
                # transaction logs, so we can only assert the weaker invariant.
                assert set(transactions) <= set(snapshots) - {INITIAL_SNAPSHOT}
            else:
                assert set(snapshots) - {INITIAL_SNAPSHOT} == set(transactions)
        else:
            assert set(snapshots) == set(transactions)

        if self.model.spec_version >= 2:
            ops = list(self.repo.ops_log())
            backups = set(
                f"overwritten/{update.backup_path}"
                for update in ops
                if update.backup_path is not None
            )
            if len(backups) > 0:
                event("backups exist")
            assert backups.issubset(paths)

    def check_ops_log(self) -> None:
        if self.model.spec_version == 1:
            return
        actual_ops = list(self.repo.ops_log())

        # ops should be strictly decreasing in both normal and migrated repos
        assert all(
            first.updated_at > second.updated_at
            for (first, second) in itertools.pairwise(actual_ops)
        )

        if self.model.migrated:
            # The model tracks RepoMigratedUpdate + all post-migration ops.
            # This is because the synthetically reconstructed ops log upon migration
            # will not match what the model has recorded.
            # Compare only the post-migration entries the model knows about.
            n = len(self.model.ops_log)
            actual_ops = actual_ops[:n]

        # non-None backup paths must be unique
        all_backups = [op.backup_path for op in actual_ops if op.backup_path is not None]
        assert len(all_backups) == len(set(all_backups))

        assert self.model.ops_log[::-1] == actual_ops
        assert isinstance(
            actual_ops[-1].kind,
            ic.UpdateType.RepoInitialized | ic.UpdateType.RepoMigrated,
        )


VersionControlTest = VersionControlStateMachine.TestCase
