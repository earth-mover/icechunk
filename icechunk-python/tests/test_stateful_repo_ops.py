#!/usr/bin/env python3

import datetime
import itertools
import json
import operator
import textwrap
from dataclasses import dataclass
from functools import partial
from typing import Any, Self

import numpy as np
import pytest

from zarr.core.buffer import Buffer, default_buffer_prototype

pytest.importorskip("hypothesis")
pytest.importorskip("xarray")

import copy

import hypothesis.extra.numpy as npst
import hypothesis.strategies as st
import pytest
from hypothesis import assume, note
from hypothesis.stateful import (
    Bundle,
    RuleBasedStateMachine,
    Settings,
    consumes,
    initialize,
    invariant,
    precondition,
    rule,
)

import zarr.testing.strategies as zrst
from icechunk import IcechunkError, Repository, SnapshotInfo, in_memory_storage
from zarr.testing.stateful import SyncStoreWrapper

# JSON file contents, keep it simple
simple_text = st.text(zrst.zarr_key_chars, min_size=1, max_size=5)
simple_attrs = st.dictionaries(
    simple_text,
    st.integers(min_value=-10_000, max_value=10_000),
    max_size=5,
)

DEFAULT_BRANCH = "main"


#########
# TODO: port to Zarr


class NewSyncStoreWrapper(SyncStoreWrapper):
    def list_prefix(self, prefix: str) -> list[str]:
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
    parent_id: str

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
    def __init__(self, **kwargs) -> None:
        self.store: dict[str, Any] = {}  #

        self.initial_snapshot_id: str | None = None
        self.changes_made: bool = False

        self.HEAD: None | str = None
        self.branch: None | str = None

        self.commits: dict[str, CommitModel] = {}
        self.tags: dict[str, TagModel] = {}
        # TODO: This is only tracking the HEAD,
        # Should we model the branch as an ordered list of commits?
        self.branches: dict[str, str] = {}

        # a tag once created, can never be recreated even after expiration
        self.created_tags: set[str] = set()

    def __repr__(self) -> str:
        return textwrap.dedent(f"""
        <Model branch: {self.branch!r}, HEAD: {self.HEAD!r}, changes_made: {self.changes_made!r}>
        Branches: {tuple(self.branches.keys())!r}
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
        return self.store[key]

    @property
    def has_commits(self) -> bool:
        return bool(self.commits)

    def commit(self, snap: SnapshotInfo) -> None:
        ref = snap.id
        self.commits[ref] = CommitModel.from_snapshot_and_store(
            snap, copy.deepcopy(self.store)
        )
        self.changes_made = False
        self.HEAD = ref

        assert self.branch is not None
        self.branches[self.branch] = ref

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
        self.branches[name] = commit

    def checkout_branch(self, ref: str) -> None:
        self.checkout_commit(self.branches[ref])
        self.branch = ref

    def reset_branch(self, branch: str, commit: str) -> None:
        assert commit in self.commits
        self.branches[branch] = commit

    def delete_branch(self, branch_name: str) -> None:
        del self.branches[branch_name]

    def delete_tag(self, tag: str) -> None:
        del self.tags[tag]

    def create_tag(self, tag_name: str, commit_id: str) -> None:
        assert commit_id in self.commits
        self.tags[tag_name] = TagModel(commit_id=str(commit_id))
        self.created_tags.add(tag_name)

    def checkout_tag(self, ref: str) -> None:
        self.checkout_commit(self.tags[str(ref)].commit_id)

    def list_prefix(self, prefix: str) -> tuple[str, ...]:
        assert prefix == ""
        return tuple(self.store)

    def refs_iter(self):
        tag_iter = map(operator.attrgetter("commit_id"), self.tags.values())
        return itertools.chain(self.branches.values(), tag_iter)

    def expire_snapshots(
        self,
        older_than: datetime.datetime,
        *,
        delete_expired_branches: bool,
        delete_expired_tags: bool,
    ) -> ExpireInfo:
        # model this exactly like icechunk does.
        # Start with named refs, and walk the ancestry back to the root.
        expired_snaps = set()
        edited_snaps = set()
        for commit_id in self.refs_iter():
            snap = self.commits[commit_id]
            if snap.parent_id == self.initial_snapshot_id:
                continue

            editable_snap = snap.id
            while commit_id != self.initial_snapshot_id:
                snap = self.commits[commit_id]
                if snap.written_at < older_than:
                    expired_snaps.add(snap.id)
                else:
                    editable_snap = snap.id
                commit_id = snap.parent_id
            if editable_snap != self.initial_snapshot_id:
                edited_snaps.add(editable_snap)
                self.commits[editable_snap].parent_id = self.initial_snapshot_id

        note(f"model {expired_snaps=!r}")

        if delete_expired_tags:
            tags_to_delete = {
                k
                for k, v in self.tags.items()
                if self.commits[v.commit_id].written_at < older_than
            }
            note(f"deleting tags {tags_to_delete=!r}")
            for tag in tags_to_delete:
                self.delete_tag(tag)
        else:
            tags_to_delete = set()

        if delete_expired_branches:
            branches_to_delete = {
                k
                for k, v in self.branches.items()
                if k != DEFAULT_BRANCH and self.commits[v].written_at < older_than
            }
            note(f"deleting branches {branches_to_delete=!r}")
            for branch in branches_to_delete:
                note(f"deleting {branch=!r}, {self.branches[branch]=!r}")
                self.delete_branch(branch)
        else:
            branches_to_delete = set()

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
                commit_id = self.commits[commit_id].parent_id

        deleted = set()
        for k in set(self.commits) - reachable_snaps:
            if self.commits[k].written_at < older_than:
                del self.commits[k]
                deleted.add(k)
        note(f"Deleted snapshots: {deleted!r}")
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
        self.commit_times: list[datetime.datetime] = []

    @initialize(data=st.data(), target=branches)
    def initialize(self, data: st.DataObject) -> str:
        self.repo = Repository.create(in_memory_storage())
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
        self.commit_times.append(snapinfo.written_at)
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
        if self.model.branches.get(ref) in self.model.commits:
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
        if name not in self.model.branches and commit in self.model.commits:
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

    @rule()
    def discard_changes(self) -> None:
        self.session.discard_changes()
        if self.model.branch is not None:
            self.model.checkout_branch(self.model.branch)
        else:
            self.model.checkout_commit(self.session.snapshot_id)

    # if there are changes in a session tied to the same branch
    # then an attempt to commit from that session will raise a conflict
    # (as is expected)
    @precondition(lambda self: not self.model.changes_made)
    @rule(branch=branches, commit=commits)
    def reset_branch(self, branch: str, commit: str) -> None:
        if branch not in self.model.branches or commit not in self.model.commits:
            note(f"resetting branch {branch}, expecting error.")
            with pytest.raises(IcechunkError):
                self.repo.reset_branch(branch, commit)
        else:
            note(
                f"resetting branch {branch} from {self.model.branches[branch]} to {commit}"
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
        if branch in self.model.branches:
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

    @precondition(lambda self: bool(self.commit_times))
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
        delta: int,
        delete_expired_branches: bool,
        delete_expired_tags: bool,
    ) -> None:
        commit_time = data.draw(st.sampled_from(self.commit_times))
        older_than = commit_time + delta
        note(
            f"Expiring snapshots {older_than=!r}, ({commit_time=!r}, {delta=!r}), {delete_expired_branches=!r}, {delete_expired_tags=!r}"
        )
        actual = self.repo.expire_snapshots(
            older_than,
            delete_expired_branches=delete_expired_branches,
            delete_expired_tags=delete_expired_tags,
        )
        note(f"repo  expired snaps={actual!r}")
        expected = self.model.expire_snapshots(
            older_than,
            delete_expired_branches=delete_expired_branches,
            delete_expired_tags=delete_expired_tags,
        )
        assert self.initial_snapshot.id not in actual
        assert actual == expected.expired_snapshots, (actual, expected)

        for branch in expected.deleted_branches:
            self.maybe_checkout_branch(branch)

    @precondition(lambda self: bool(self.commit_times))
    @rule(
        data=st.data(),
        # we delete based on snapshot created_at time, not flushed_at time
        # so for delta we just past an integer number of seconds to handle the
        # (small) difference in tests
        delta=st.integers(min_value=-86400, max_value=86400).filter(lambda x: x != 0),
    )
    def garbage_collect(self, data: st.DataObject, delta: int) -> None:
        commit_time = data.draw(st.sampled_from(self.commit_times))
        older_than = commit_time + datetime.timedelta(seconds=delta)
        note(
            f"running garbage_collect for {older_than=!r}, ({commit_time=!r}, {delta=!r})"
        )
        summary = self.repo.garbage_collect(older_than)
        expected = self.model.garbage_collect(older_than)
        assert summary.snapshots_deleted == len(expected), (
            summary.snapshots_deleted,
            expected,
        )

        # deleted a checked out snapshot :?
        for snapshot in expected:
            if self.model.HEAD == snapshot:
                self.session = self.repo.writable_session(DEFAULT_BRANCH)
                self.model.checkout_branch(DEFAULT_BRANCH)

    def check_commit(self, commit) -> None:
        assume(commit in self.model.commits)
        note(f"Checking {commit=!r}")
        expected = self.model.commits[commit]
        actual = self.repo.lookup_snapshot(commit)
        assert actual.id == expected.id
        assert actual.parent_id == expected.parent_id
        # even after expiration, written_at is unmodified
        assert actual.written_at == expected.written_at

    @invariant()
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

    @invariant()
    def check_tags(self) -> None:
        expected_tags = self.model.tags
        actual_tags = {
            tag: TagModel(commit_id=self.repo.lookup_tag(tag))
            for tag in self.repo.list_tags()
        }
        assert actual_tags == expected_tags

    @invariant()
    def check_branches(self) -> None:
        repo_branches = {k: self.repo.lookup_branch(k) for k in self.repo.list_branches()}
        assert self.model.branches == repo_branches

    @invariant()
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


VersionControlStateMachine.TestCase.settings = Settings(deadline=None)
VersionControlTest = VersionControlStateMachine.TestCase
