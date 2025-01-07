#!/usr/bin/env python3

import json
import textwrap
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
from icechunk import Repository, in_memory_storage
from zarr.testing.stateful import SyncStoreWrapper

# JSON file contents, keep it simple
simple_text = st.text(zrst.zarr_key_chars, min_size=1)
simple_attrs = st.dictionaries(
    simple_text,
    st.integers(min_value=-10_000, max_value=10_000),
)
# set in_side to one
array_shapes = npst.array_shapes(max_dims=4, min_side=1)

DEFAULT_BRANCH = "main"


#########
# TODO: port to Zarr
@st.composite
def v3_group_metadata(draw):
    from zarr.core.group import GroupMetadata

    metadata = GroupMetadata(attributes=draw(simple_attrs))
    return metadata.to_buffer_dict(prototype=default_buffer_prototype())["zarr.json"]


@st.composite
def v3_array_metadata(draw: st.DrawFn) -> bytes:
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


#####

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

        self.HEAD: None | str = None
        self.branch: None | str = None

        # commits and tags are a mapping from id to store-dict
        self.commits: dict[str, dict] = {}
        self.tags: dict[str, TagModel] = {}
        # TODO: This is only tracking the HEAD,
        # Should we model the branch as an ordered list of commits?
        self.branches: dict[str, str] = {}

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
        self.branch = None

    def create_branch(self, name: str, commit: str) -> None:
        assert commit in self.commits
        self.branches[name] = commit

    def checkout_branch(self, ref: str) -> None:
        self.checkout_commit(self.branches[ref])
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

    @initialize(data=st.data(), target=branches)
    def initialize(self, data) -> str:
        self.repo = Repository.create(in_memory_storage())
        self.session = self.repo.writable_session(DEFAULT_BRANCH)

        HEAD = self.repo.lookup_branch(DEFAULT_BRANCH)
        self.model.commits[HEAD] = {}
        self.model.HEAD = HEAD
        self.model.create_branch(DEFAULT_BRANCH, HEAD)
        self.model.checkout_branch(DEFAULT_BRANCH)

        # initialize with some data always
        # TODO: always setting array metadata, since we cannot overwrite an existing group's zarr.json
        #       with an array's zarr.json
        # TODO: consider adding a deeper understanding of the zarr model rather than just setting docs?
        self.set_doc(path="zarr.json", value=data.draw(v3_array_metadata()))

        return DEFAULT_BRANCH

    def new_store(self) -> None:
        self.session = self.repo.writable_session(DEFAULT_BRANCH)

    @property
    def sync_store(self):
        return NewSyncStoreWrapper(self.session.store)

    @rule(path=metadata_paths, value=v3_array_metadata())
    def set_doc(self, path: str, value: Buffer):
        note(f"setting path {path!r} with {value.to_bytes()!r}")
        # FIXME: remove when we support complex values with infinity fill_value
        assume("complex" not in json.loads(value.to_bytes())["data_type"])
        if self.model.branch is not None:
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
        self.session = self.repo.readonly_session(snapshot_id=ref)
        assert self.session.read_only
        self.model.checkout_commit(ref)

    @rule(ref=tags)
    def checkout_tag(self, ref):
        """
        Tags and branches are combined here since checkout magically works for both.
        This test is relying on the model tracking tags and branches accurately.
        """
        if ref in self.model.tags:
            note(f"Checking out tag {ref!r}")
            self.session = self.repo.readonly_session(tag=ref)
            assert self.session.read_only
            self.model.checkout_tag(ref)
        else:
            note("Expecting error.")
            with pytest.raises(ValueError):
                self.repo.readonly_session(tag=ref)

    @rule(ref=branches)
    def checkout_branch(self, ref):
        # TODO: sometimes readonly?
        if ref in self.model.branches:
            note(f"Checking out branch {ref!r}")
            self.session = self.repo.writable_session(ref)
            assert not self.session.read_only
            self.model.checkout_branch(ref)
        else:
            with pytest.raises(ValueError):
                note(f"Expecting error when checking out branch {ref!r}")
                self.repo.writable_session(ref)

    @rule(name=simple_text, commit=commits, target=branches)
    def create_branch(self, name: str, commit: str) -> str:
        note(f"Creating branch {name!r}")
        # we can create a tag and branch with the same name
        if name not in self.model.branches:
            self.repo.create_branch(name, commit)
            self.model.create_branch(name, commit)
        else:
            note("Expecting error.")
            with pytest.raises(ValueError):
                self.repo.create_branch(name, commit)
        # returning this `name` to the Bundle is OK even if the branch was not created
        # This will test out checking out and deleting a branch that does not exist.
        return name

    @precondition(lambda self: self.model.has_commits)
    @rule(name=simple_text, commit_id=commits, target=tags)
    def create_tag(self, name, commit_id):
        note(f"Creating tag {name!r} for commit {commit_id!r}")
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
    def reset_branch(self, branch, commit) -> None:
        note(f"resetting branch {self.model.branch} from {self.model.HEAD} to {commit}")
        self.repo.reset_branch(branch, commit)
        self.model.reset_branch(branch, commit)
        # TODO: if we reset the current branch, we hold on to an invalid session
        #       this is confusing UX. any attempts to commit will create a conflict
        if self.model.branch == branch:
            self.session = self.repo.writable_session(branch)
            self.model.checkout_branch(branch)

    # @rule(branch=consumes(branches))
    # def delete_branch(self, branch):
    #     note(f"Deleting branch {branch!r}")
    #     if branch in self.model.branches:
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

    @invariant()
    def check_commit_data(self):
        expected_tags = self.model.tags
        actual_tags = {
            tag: TagModel(commit_id=self.repo.lookup_tag(tag))
            for tag in self.repo.list_tags()
        }
        assert actual_tags == expected_tags

        assert self.model.branches == {
            k: self.repo.lookup_branch(k) for k in self.repo.list_branches()
        }

        # TODO: assert all snapshot_ids are present?
        # assert sorted(self.model.commits.keys()) == sorted(
        #     map(str, commit_data.commits.keys())
        # )


VersionControlStateMachine.TestCase.settings = Settings(max_examples=300, deadline=None)
VersionControlTest = VersionControlStateMachine.TestCase
