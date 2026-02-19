#!/usr/bin/env python3

import datetime
import itertools
import json
import operator
import textwrap
from collections.abc import Iterator
from dataclasses import dataclass
from functools import partial
from typing import Any, Literal, Self, cast

import numpy as np
import pytest

import icechunk
from zarr.core.buffer import Buffer, default_buffer_prototype

pytest.importorskip("hypothesis")
pytest.importorskip("xarray")

import copy

import hypothesis.extra.numpy as npst
import hypothesis.strategies as st
import pytest
from hypothesis import assume, note, settings
from hypothesis.stateful import (
    Bundle,
    RuleBasedStateMachine,
    consumes,
    initialize,
    invariant,
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
    in_memory_storage,
)
from icechunk.testing.strategies import repository_configs
from zarr.testing.stateful import SyncStoreWrapper

# JSON file contents, keep it simple
simple_text = st.text(zrst.zarr_key_chars, min_size=1, max_size=5)
simple_attrs = st.dictionaries(
    simple_text,
    st.integers(min_value=-10_000, max_value=10_000),
    max_size=5,
)

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
        self.num_updates: int = 0

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

        # a tag once created, can never be recreated even after expiration
        self.created_tags: set[str] = set()

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

    def upgrade(self) -> None:
        self.num_updates += 1

    @property
    def has_commits(self) -> bool:
        return bool(self.commits)

    @property
    def commit_times(self) -> list[datetime.datetime]:
        """Return sorted list of all commit times."""
        return sorted(c.written_at for c in self.commits.values())

    def commit(self, snap: SnapshotInfo) -> None:
        ref = snap.id
        self.commits[ref] = CommitModel.from_snapshot_and_store(
            snap, copy.deepcopy(self.store)
        )
        self.ondisk_snaps[ref] = self.commits[ref]
        self.changes_made = False
        self.HEAD = ref

        assert self.branch is not None
        self.branch_heads[self.branch] = ref
        self.num_updates += 1

    def amend(self, snap: SnapshotInfo) -> None:
        """Amend the HEAD commit."""
        # this is simple because we aren't modeling the branch as a list of commits
        self.commit(snap)

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
        self.num_updates += 1

    def checkout_branch(self, ref: str) -> None:
        self.checkout_commit(self.branch_heads[ref])
        self.branch = ref

    # TODO: add `from_snapshot_id` to this
    def reset_branch(self, branch: str, commit: str) -> None:
        assert commit in self.commits
        self.branch_heads[branch] = commit
        self.num_updates += 1

    def delete_branch(self, branch_name: str) -> None:
        self._delete_branch(branch_name)
        self.num_updates += 1

    def _delete_branch(self, branch_name: str) -> None:
        del self.branch_heads[branch_name]

    def delete_tag(self, tag: str) -> None:
        self._delete_tag(tag)
        self.num_updates += 1

    def _delete_tag(self, tag: str) -> None:
        del self.tags[tag]

    def create_tag(self, tag_name: str, commit_id: str) -> None:
        assert commit_id in self.commits
        self.tags[tag_name] = TagModel(commit_id=str(commit_id))
        self.created_tags.add(tag_name)
        self.num_updates += 1

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
        older_than: datetime.datetime,
        *,
        delete_expired_branches: bool,
        delete_expired_tags: bool,
    ) -> ExpireInfo:
        # model this exactly like icechunk does.
        expired_snaps = set()
        branch_pointees = set(self.branch_heads.values())
        tag_pointees = set(map(operator.attrgetter("commit_id"), self.tags.values()))
        for snap in self.commits.values():
            if (
                snap.written_at < older_than
                and snap.parent_id is not None
                and (delete_expired_tags or snap.id not in tag_pointees)
                and (
                    (delete_expired_branches and self.branch_heads["main"] != snap.id)
                    or snap.id not in branch_pointees
                )
            ):
                expired_snaps.add(snap.id)

        note(f"model {expired_snaps=!r}")

        for id in expired_snaps:
            # notice we don't delete from self.ondisk_snaps, those can still be deleted by GC
            # however we do pop from `commits` since that is a list of unexpired snaps
            self.commits.pop(id, None)

        for c in self.commits.values():
            if c.parent_id in expired_snaps:
                c.parent_id = INITIAL_SNAPSHOT

        if delete_expired_tags:
            tags_to_delete = {
                k for k, v in self.tags.items() if v.commit_id in expired_snaps
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
                if k != DEFAULT_BRANCH and v in expired_snaps
            }
            note(f"deleting branches {branches_to_delete=!r}")
            for branch in branches_to_delete:
                note(f"deleting {branch=!r}, {self.branch_heads[branch]=!r}")
                self._delete_branch(branch)
        else:
            branches_to_delete = set()

        self.num_updates += 1

        return ExpireInfo(
            expired_snapshots=expired_snaps,
            deleted_branches=branches_to_delete,
            deleted_tags=tags_to_delete,
        )

    def garbage_collect(self, older_than: datetime.datetime) -> set[str]:
        reachable_snaps = set((self.initial_snapshot_id,))
        for commit_id in self.refs_iter():
            while commit_id != self.initial_snapshot_id:
                reachable_snaps.add(commit_id)
                parent_id = self.commits[commit_id].parent_id
                assert (
                    parent_id is not None
                ), f"Commit {commit_id} has no parent but is not the initial snapshot"
                commit_id = parent_id
        deleted = set()
        for k in set(self.ondisk_snaps) - reachable_snaps:
            if self.ondisk_snaps[k].written_at < older_than:
                self.commits.pop(k, None)
                self.ondisk_snaps.pop(k, None)
                deleted.add(k)
        note(f"Deleted snapshots in model: {deleted!r}")
        self.num_updates += 1
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

    def __init__(self) -> None:
        super().__init__()

        note("----------")
        self.model = Model()
        self.storage: Storage | None = None

    def _make_storage(self) -> Storage:
        return in_memory_storage()

    def _repository_configs(self) -> st.SearchStrategy:
        return repository_configs()

    @initialize(data=st.data(), target=branches, spec_version=st.sampled_from([1, 2]))
    def initialize(self, data: st.DataObject, spec_version: Literal[1, 2]) -> str:
        self.storage = self._make_storage()
        config = data.draw(self._repository_configs())
        self.model.spec_version = spec_version

        self.repo = Repository.create(
            self.storage,
            spec_version=spec_version,
            config=config,
        )
        self.session = self.repo.writable_session(DEFAULT_BRANCH)

        snap = next(iter(self.repo.ancestry(branch=DEFAULT_BRANCH)))
        note(f"Initial commit is {snap!r}")
        self.initial_snapshot = snap
        self.model.initial_snapshot_id = snap.id

        HEAD = self.repo.lookup_branch(DEFAULT_BRANCH)
        self.model.commits[HEAD] = CommitModel.from_snapshot_and_store(snap, {})
        self.model.HEAD = HEAD
        self.model.create_branch(DEFAULT_BRANCH, HEAD)
        self.model.checkout_branch(DEFAULT_BRANCH)
        # RepoInitializedUpdate includes the initial branch creation,
        # so reset to 1 after create_branch incremented it.
        self.model.num_updates = 1

        # initialize with some data always
        # TODO: always setting array metadata, since we cannot overwrite an existing group's zarr.json
        #       with an array's zarr.json
        # TODO: consider adding a deeper understanding of the zarr model rather than just setting docs?
        self.set_doc(path="zarr.json", value=data.draw(v3_array_metadata))

        return DEFAULT_BRANCH

    def new_store(self) -> None:
        self.session = self.repo.writable_session(DEFAULT_BRANCH)

    @property
    def sync_store(self) -> NewSyncStoreWrapper:
        return NewSyncStoreWrapper(self.session.store)

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

    @rule()
    @precondition(lambda self: self.repo.spec_version == 1)
    def upgrade_spec_version(self) -> None:
        # don't test simple cases of catching error upgradging a v2 spec
        # that should be covered in unit tests
        icechunk.upgrade_icechunk_repository(self.repo)
        self.model.upgrade()
        # TODO: remove the reopen after https://github.com/earth-mover/icechunk/issues/1521
        self._reopen_repository()

    @rule(data=st.data())
    def reopen_repository(self, data: st.DataObject) -> None:
        config = data.draw(self._repository_configs())
        self._reopen_repository(config)

    def _reopen_repository(self, config: RepositoryConfig | None = None) -> None:
        """Reopen the repository from storage to get fresh state.

        This discards any uncommitted changes.
        """
        assert self.storage is not None, "storage must be initialized"
        self.repo = Repository.open(self.storage, config=config)
        note(f"Reopened repository (spec_version={self.repo.spec_version})")

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
        lambda self: (self.model.changes_made)
        and (self.repo.spec_version >= 2)
        and len(self.model.commits) > 1
    )
    def amend(self, message: str) -> str:
        branch = self.session.branch
        assert branch is not None
        old_head = next(iter(self.repo.ancestry(branch=branch)))
        note(f"Amending commit on branch {branch!r} with id {old_head!r}")

        commit_id = self.session.amend(message)
        snapinfo = next(iter(self.repo.ancestry(branch=branch)))
        assert snapinfo.id == commit_id
        note(f"Amended commit: {snapinfo!r}")
        self.session = self.repo.writable_session(branch)

        # Update model
        self.model.amend(snapinfo)
        return commit_id

    @rule(ref=commits)
    def checkout_commit(self, ref: str) -> None:
        if ref not in self.model.commits:
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

    @rule(name=simple_text, commit=commits, target=branches)
    def create_branch(self, name: str, commit: str) -> str:
        note(f"Creating branch {name!r}")

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
    @rule(name=simple_text, commit_id=commits, target=tags)
    def create_tag(self, name: str, commit_id: str) -> str:
        note(f"Creating tag {name!r} for commit {commit_id!r}")

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

    # TODO: v1 has bugs in expire_snapshots, only test for v2
    # https://github.com/earth-mover/icechunk/issues/1520
    # https://github.com/earth-mover/icechunk/issues/1534
    @precondition(lambda self: bool(self.model.commits) and self.repo.spec_version == 2)
    @rule(
        data=st.data(),
        delta=st.timedeltas(
            min_value=datetime.timedelta(days=-1), max_value=datetime.timedelta(days=1)
        ),
        delete_expired_branches=st.booleans(),
        delete_expired_tags=st.booleans(),
    )
    def expire_snapshots(
        self,
        data: st.DataObject,
        delta: datetime.timedelta,
        delete_expired_branches: bool,
        delete_expired_tags: bool,
    ) -> None:
        commit_time = data.draw(st.sampled_from(self.model.commit_times))
        older_than = commit_time + delta
        note(
            f"Expiring snapshots {older_than=!r}, ({commit_time=!r}, {delta=!r}), {delete_expired_branches=!r}, {delete_expired_tags=!r}"
        )

        # Track branches and tags before expiration
        branches_before = set(self.repo.list_branches())
        tags_before = set(self.repo.list_tags())

        actual = self.repo.expire_snapshots(
            older_than,
            delete_expired_branches=delete_expired_branches,
            delete_expired_tags=delete_expired_tags,
        )
        note(f"repo  expired snaps={actual!r}")

        # Track branches and tags after expiration
        branches_after = set(self.repo.list_branches())
        tags_after = set(self.repo.list_tags())
        actual_deleted_branches = branches_before - branches_after
        actual_deleted_tags = tags_before - tags_after

        expected = self.model.expire_snapshots(
            older_than,
            delete_expired_branches=delete_expired_branches,
            delete_expired_tags=delete_expired_tags,
        )
        note(f"from model: {expected}")
        note(f"actual: {actual}")
        note(f"actual_deleted_branches: {actual_deleted_branches}")
        note(f"actual_deleted_tags: {actual_deleted_tags}")

        assert self.initial_snapshot.id not in actual
        assert actual == expected.expired_snapshots, (actual, expected)

        # Check that expired snapshots are actually removed from ancestry
        remaining_snapshot_ids = set()
        for branch in branches_after:
            for snap in self.repo.ancestry(branch=branch):
                remaining_snapshot_ids.add(snap.id)
        expired_but_remaining = actual & remaining_snapshot_ids
        note(expired_but_remaining)
        assert (
            not expired_but_remaining
        ), f"Snapshots marked as expired but still in ancestry: {expired_but_remaining}"

        assert (
            actual_deleted_branches == expected.deleted_branches
        ), f"deleted branches mismatch: actual={actual_deleted_branches}, expected={expected.deleted_branches}"
        assert (
            actual_deleted_tags == expected.deleted_tags
        ), f"deleted tags mismatch: actual={actual_deleted_tags}, expected={expected.deleted_tags}"

        for branch in actual_deleted_branches:
            self.maybe_checkout_branch(branch)

    @precondition(lambda self: bool(self.model.commit_times))
    @rule(
        data=st.data(),
        # we delete based on snapshot created_at time, not flushed_at time
        # so for delta we just past an integer number of seconds to handle the
        # (small) difference in tests
        delta=st.integers(min_value=-86400, max_value=86400).filter(lambda x: x != 0),
    )
    def garbage_collect(self, data: st.DataObject, delta: int) -> None:
        commit_time = data.draw(st.sampled_from(self.model.commit_times))
        older_than = commit_time + datetime.timedelta(seconds=delta)
        note(
            f"running garbage_collect for {older_than=!r}, ({commit_time=!r}, {delta=!r})"
        )
        summary = self.repo.garbage_collect(older_than)
        note(f"actual GC result {summary=!r}")
        expected = self.model.garbage_collect(older_than)
        assert summary.snapshots_deleted == len(expected), (
            summary.snapshots_deleted,
            expected,
        )

        # deleted a checked out snapshot :?
        for snapshot in expected:
            if self.model.HEAD == snapshot:
                note(f"deleted the checked out snapshot {snapshot}, creating new session")
                self.session = self.repo.writable_session(DEFAULT_BRANCH)
                self.model.checkout_branch(DEFAULT_BRANCH)

    def check_commit(self, commit: str) -> None:
        # utility function, not an invariant
        assume(commit in self.model.commits)
        note(f"Checking {commit=!r}")
        expected = self.model.commits[commit]
        actual = self.repo.lookup_snapshot(commit)
        assert actual.id == expected.id
        # assert actual.parent_id == expected.parent_id
        # even after expiration, written_at is unmodified
        assert actual.written_at == expected.written_at

    def check_ops_log(self) -> None:
        if self.model.spec_version == 1:
            return
        actual_ops = list(self.repo.ops_log())
        assert len(actual_ops) == self.model.num_updates, (
            actual_ops,
            self.model.num_updates,
            actual_ops,
        )
        assert isinstance(actual_ops[-1], icechunk.RepoInitializedUpdate)

    @invariant()
    def checks(self) -> None:
        # this method only exists to reduce verbosity of hypothesis output
        # It cannot be called `check_invariants` because that clashes
        # with an existing method on the superclass
        self.check_list_prefix_from_root()
        self.check_tags()
        self.check_branches()
        self.check_ancestry()
        self.check_ops_log()

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

    def check_ancestry(self) -> None:
        for branch in self.repo.list_branches():
            ancestry = list(self.repo.ancestry(branch=branch))
            ancestry_set = set([snap.id for snap in ancestry])
            # snapshot timestamps are monotonically decreasing in ancestry
            assert all(
                a.written_at > b.written_at for a, b in itertools.pairwise(ancestry)
            )
            # ancestry must be unique
            assert len(ancestry_set) == len(ancestry)
            # the initial snapshot is in every possible branch
            # this is a python-only invariant
            assert ancestry[-1] == self.initial_snapshot


VersionControlStateMachine.TestCase.settings = settings(
    deadline=None,
    # stateful_step_count=100,
    # report_multiple_bugs=False,
)
VersionControlTest = VersionControlStateMachine.TestCase
