#!/usr/bin/env python3

import json
from dataclasses import dataclass

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
    initialize,
    invariant,
    precondition,
    rule,
)

import zarr.testing.strategies as zrst
from icechunk import Repository, StorageConfig
from zarr.testing.stateful import SyncStoreWrapper

# JSON file contents, keep it simple
simple_text = st.text(zrst.zarr_key_chars, min_size=1)
simple_attrs = st.dictionaries(
    simple_text,
    st.integers(min_value=-10_000, max_value=10_000),
)
# set in_side to one
array_shapes = npst.array_shapes(max_dims=4, min_side=1)


@st.composite
def v3_group_metadata(draw):
    from zarr.core.group import GroupMetadata

    metadata = GroupMetadata(attributes=draw(simple_attrs))
    return metadata.to_buffer_dict(prototype=default_buffer_prototype())["zarr.json"]


@st.composite
def v3_array_metadata(draw):
    from zarr.codecs.bytes import BytesCodec
    from zarr.core.chunk_grids import RegularChunkGrid
    from zarr.core.chunk_key_encodings import DefaultChunkKeyEncoding
    from zarr.core.metadata.v3 import ArrayV3Metadata

    # separator = draw(st.sampled_from(['/', '\\']))
    shape = draw(array_shapes)
    ndim = len(shape)
    chunk_shape = draw(npst.array_shapes(min_dims=ndim, max_dims=ndim))
    dtype = draw(zrst.v3_dtypes())
    fill_value = draw(npst.from_dtype(dtype))
    dimension_names = draw(
        st.none() | st.lists(st.none() | simple_text, min_size=ndim, max_size=ndim)
    )

    metadata = ArrayV3Metadata(
        shape=shape,
        data_type=dtype,
        chunk_grid=RegularChunkGrid(chunk_shape=chunk_shape),
        fill_value=fill_value,
        attributes=draw(simple_attrs),
        dimension_names=dimension_names,
        chunk_key_encoding=DefaultChunkKeyEncoding(separator="/"),  # FIXME
        codecs=[BytesCodec()],
        storage_transformers=(),
    )

    return metadata.to_buffer_dict(prototype=default_buffer_prototype())["zarr.json"]


class NewSyncStoreWrapper(SyncStoreWrapper):
    def list_prefix(self, prefix: str) -> None:
        return self._sync_iter(self.store.list_prefix(prefix))


MAX_TEXT_SIZE = 120

keys = st.lists(zrst.node_names, min_size=1, max_size=4).map("/".join)
metadata_paths = keys.map(lambda x: x + "/zarr.json")


@dataclass
class TagModel:
    commit_id: str
    # message: str | None


class Model:
    def __init__(self, **kwargs):
        self.store: dict = {}  #

        self.changes_made: bool = False
        self.is_at_branch_head: bool = True

        self.HEAD: None | str = None
        self.branch: None | str = None

        # commits and tags are a mapping from id to store-dict
        self.commits: dict[str, dict] = {}
        self.tags: dict[str, TagModel] = {}
        # TODO: This is only tracking the HEAD,
        # Should we model the branch as an ordered list of commits?
        self.branches: dict[str, str] = {}

    def __setitem__(self, key, value):
        self.changes_made = True
        self.store[key] = value

    def __getitem__(self, key):
        return self.store[key]

    @property
    def has_commits(self) -> bool:
        return bool(self.commits)

    def commit(self, ref: str) -> None:
        self.commits[ref] = copy.deepcopy(self.store)
        self.changes_made = False
        self.HEAD = ref

        assert self.branch is not None
        self.branches[self.branch] = ref

    def checkout_commit(self, ref: str) -> None:
        assert str(ref) in self.commits
        # deepcopy so that we allow changes, but the committed store remains unchanged
        # TODO: consider Frozen stores in self.commit?
        self.store = copy.deepcopy(self.commits[ref])
        self.changes_made = False
        self.HEAD = ref
        self.is_at_branch_head = False
        self.branch = None

    def create_branch(self, name: str) -> None:
        self.branch = name
        self.is_at_branch_head = True
        assert self.HEAD is not None
        self.branches[name] = self.HEAD

    def checkout_branch(self, ref: str) -> None:
        self.checkout_commit(self.branches[ref])
        self.is_at_branch_head = True
        self.branch = ref

    def reset_branch(self, branch, commit) -> None:
        self.branches[branch] = commit

    def delete_branch(self, branch_name: str) -> None:
        del self.branches[branch_name]

    def create_tag(self, tag_name: str, commit_id: str) -> None:
        self.tags[tag_name] = TagModel(commit_id=str(commit_id))

    def checkout_tag(self, ref: str) -> None:
        self.checkout_commit(self.tags[str(ref)].commit_id)

    def list_prefix(self, prefix: str) -> None:
        assert prefix == ""
        return tuple(self.store)


class VersionControlStateMachine(RuleBasedStateMachine):
    """
    We use bundles to track the state, since Hypothesis will then
    preferably draw the same value for different rules.
    e.g. create branch 'X', then delete branch 'X'
    """

    commits = Bundle("commits")
    tags = Bundle("tags")
    branches = Bundle("branches")

    def __init__(self):
        super().__init__()

        note("----------")
        self.model = Model()

    @initialize(data=st.data())
    def initialize(self, data):
        self.repo = Repository.create(StorageConfig.memory())
        self.session = self.repo.writable_session("main")

        self.model.HEAD = self.repo.branch_tip("main")
        self.model.create_branch("main")
        # initialize with some data always
        # TODO: always setting array metadata, since we cannot overwrite an existing group's zarr.json
        #       with an array's zarr.json
        # TODO: consider adding a deeper understanding of the zarr model rather than just setting docs?
        self.set_doc(path="zarr.json", value=data.draw(v3_array_metadata()))

    def new_store(self) -> None:
        self.session = self.repo.writable_session("main")

    @property
    def sync_store(self):
        return NewSyncStoreWrapper(self.session.store())

    @rule(path=metadata_paths, value=v3_array_metadata())
    def set_doc(self, path: str, value: Buffer):
        note(f"setting path {path!r} with {value.to_bytes()!r}")
        # FIXME: remove when we support complex values with infinity fill_value
        assume("complex" not in json.loads(value.to_bytes())["data_type"])
        if self.model.is_at_branch_head:
            self.sync_store.set(path, value)
            self.model[path] = value
        else:
            # not at branch head, modifications not possible.
            with pytest.raises(ValueError, match="read-only store"):
                self.sync_store.set(path, value)

    @rule(message=st.text(max_size=MAX_TEXT_SIZE), target=commits)
    @precondition(lambda self: self.model.changes_made)
    def commit(self, message):
        branch = self.session.branch
        commit_id = self.session.commit(message)
        self.session = self.repo.writable_session(branch)
        note(f"Created commit: {commit_id}")
        self.model.commit(commit_id)
        return commit_id

    @rule(ref=commits)
    def checkout_commit(self, ref):
        note(f"Checking out commit {ref}")
        if not self.model.changes_made:
            self.session = self.repo.readonly_session(snapshot_id=ref)
            assert self.session.read_only
            self.model.checkout_commit(ref)
        else:
            with pytest.raises(ValueError, match="uncommitted changes"):
                self.repo.readonly_session(snapshot_id=ref)

    @rule(ref=tags)
    def checkout_tag(self, ref):
        """
        Tags and branches are combined here since checkout magically works for both.
        This test is relying on the model tracking tags and branches accurately.
        """
        if ref in self.model.tags and not self.model.changes_made:
            note(f"Checking out tag {ref!r}")
            self.session = self.repo.readonly_session(tag=ref)
            assert self.session.read_only
            self.model.checkout_tag(ref)
        else:
            note("Expecting error.")
            with pytest.raises(ValueError):
                self.repo.checkout(tag=ref)

    @rule(ref=branches)
    def checkout_branch(self, ref):
        # TODO: sometimes readonly?
        # TODO: checkout when changes_made
        if ref in self.model.branches and not self.model.changes_made:
            note(f"Checking out branch {ref!r}")
            self.session = self.repo.writable_session(ref)
            assert not self.session.read_only
            self.model.checkout_branch(ref)

    @rule(name=simple_text | st.just("main"), target=branches)
    def create_branch(self, name):
        note(f"Creating branch {name!r}")
        # we can create a tag and branch with the same name
        if name not in self.model.branches:
            self.repo.create_branch(name, self.session.snapshot_id)
            assert not self.session.read_only
            self.model.create_branch(name)
        else:
            note("Expecting error.")
            with pytest.raises(ValueError):
                self.repo.create_branch(name, self.session.snapshot_id)
        # returning this `name` to the Bundle is OK even if the branch was not created
        # This will test out checking out and deleting a branch that does not exist.
        return name

    @precondition(lambda self: self.model.has_commits)
    @rule(
        name=simple_text,
        commit_id=commits,
        message=st.none() | st.text(max_size=MAX_TEXT_SIZE),
        target=tags,
    )
    def create_tag(self, name, commit_id, message):
        note(f"Creating tag {name!r} for commit {commit_id!r} with message {message!r}")
        # we can create a tag and branch with the same name
        if name not in self.model.tags:
            self.repo.create_tag(name, commit_id)
            self.model.create_tag(name, commit_id)
        else:
            note("Expecting error.")
            with pytest.raises(ValueError):
                self.repo.create_tag(name, commit_id)
        # returning this `name` to the Bundle is OK even if the tag was not created
        # This will test out checking out and deleting a tag that does not exist.
        return name

    def reset(self) -> None:
        self.repo.reset()
        self.model.checkout_branch(self.model.branch)

    @rule(branch=branches, commit=commits)
    def reset_branch(self, branch, commit) -> None:
        # if self.model.branch is None or self.model.changes_made:
        #     # must be at branch tip, and with clean state, to reset it
        #     with pytest.raises(ValueError):
        #         self.repo.reset_branch(self.session.branch, commit)
        #     return

        note(f"resetting branch {self.model.branch} from {self.model.HEAD} to {commit}")
        self.repo.reset_branch(branch, commit)
        self.model.reset_branch(branch, commit)

    # @rule(branch=consumes(branches))
    # def delete_branch(self, branch):
    #     note(f"Deleting branch {branch!r}")
    #     if branch in self.model.branches and branch != "main":
    #         self.repo.delete_branch(branch)
    #         self.model.delete_branch(branch)
    #     else:
    #         note("Expecting error.")
    #         with pytest.raises(ValueError):
    #             self.repo.delete_branch(branch)

    @invariant()
    def check_list_prefix_from_root(self):
        model_list = self.model.list_prefix("")
        ice_list = self.sync_store.list_prefix("")

        assert sorted(model_list) == sorted(ice_list)

        for k in model_list:
            # need to load to dict to compare since ordering of entries might differ
            expected = json.loads(self.model[k].to_bytes())
            actual = json.loads(
                self.sync_store.get(k, default_buffer_prototype()).to_bytes()
            )
            # FIXME: zarr omits this if None?
            if "dimension_names" not in expected:
                actual.pop("dimension_names")
            actual_fv = actual.pop("fill_value")
            expected_fv = expected.pop("fill_value")
            if actual_fv != expected_fv:
                # TODO: is this right? we are losing accuracy in serialization
                np.testing.assert_allclose(actual_fv, expected_fv)
            assert actual == expected

    # @invariant()
    # def check_commit_data(self):
    #     commit_data = self.repo.commit_log.commit_data
    #     expected_tags = self.model.tags
    #     actual_tags = {
    #         tag.label: TagModel(commit_id=str(tag.commit.id), message=tag.message)
    #         for tag in self.repo.tags
    #     }
    #     assert actual_tags == expected_tags
    #     assert self.model.branches == {k: str(v) for k, v in commit_data.branches.items()}
    #     assert sorted(self.model.commits.keys()) == sorted(
    #         map(str, commit_data.commits.keys())
    #     )


VersionControlStateMachine.TestCase.settings = Settings(
    max_examples=300, deadline=None, report_multiple_bugs=False
)
VersionControlTest = VersionControlStateMachine.TestCase
