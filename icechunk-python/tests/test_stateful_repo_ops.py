#!/usr/bin/env python3

# TODO: reset_branch, reset
from dataclasses import dataclass

import pytest

from zarr.core.buffer import Buffer, default_buffer_prototype

pytest.importorskip("hypothesis")
pytest.importorskip("xarray")

import copy

import hypothesis.extra.numpy as npst
import hypothesis.strategies as st
import pytest
from hypothesis import note
from hypothesis.stateful import (
    Bundle,
    RuleBasedStateMachine,
    Settings,
    initialize,
    precondition,
    rule,
    run_state_machine_as_test,
)

import zarr.testing.strategies as zrst
from icechunk import IcechunkStore, StorageConfig
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

    def commit(self, id_) -> None:
        as_str = str(id_)
        self.commits[as_str] = copy.deepcopy(self.store)
        self.changes_made = False
        self.HEAD = as_str

        assert self.branch is not None
        self.branches[self.branch] = as_str

    def checkout_commit(self, ref) -> None:
        assert str(ref) in self.commits
        # deepcopy so that we allow changes, but the committed store remains unchanged
        # TODO: consider Frozen stores in self.commit?
        self.store = copy.deepcopy(self.commits[str(ref)])
        self.changes_made = False
        self.HEAD = str(ref)
        self.is_at_branch_head = False
        self.branch = None

    def new_branch(self, name):
        self.branch = name
        self.is_at_branch_head = True
        assert self.HEAD is not None
        self.branches[name] = self.HEAD

    def checkout_branch(self, ref: str) -> None:
        self.checkout_commit(self.branches[ref])
        self.is_at_branch_head = True
        self.branch = ref

    def delete_branch(self, branch_name):
        del self.branches[branch_name]

    def tag(self, tag_name, commit_id):
        if commit_id is None:
            assert self.HEAD is not None
            commit_id = self.HEAD
        self.tags[tag_name] = TagModel(commit_id=str(commit_id))

    def checkout_tag(self, ref):
        self.checkout_commit(self.tags[str(ref)].commit_id)

    def list_prefix(self, prefix: str):
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

    @property
    def repo(self) -> IcechunkStore:
        return self.sync_store.store

    @initialize(data=st.data())
    def initialize(self, data):
        self.sync_store = SyncStoreWrapper(IcechunkStore.create(StorageConfig.memory()))
        self.model.branch = "main"
        # initialize with some data always
        # TODO: always setting array metadata, since we cannot overwrite an existing group's zarr.json
        #       with an array's zarr.json
        # TODO: consider adding a deeper understanding of the zarr model rather than just setting docs?
        self.set_doc(path="/zarr.json", value=data.draw(v3_array_metadata()))

    @rule(path=metadata_paths, value=v3_array_metadata())
    def set_doc(self, path: str, value: Buffer):
        note(f"setting path {path!r} with {value.to_bytes()!r}")
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
        commit_id = self.repo.commit(message)
        note(f"Created commit: {commit_id}")
        self.model.commit(commit_id)
        return commit_id

    @rule(ref=commits)
    def checkout_commit(self, ref):
        note(f"Checking out commit {ref}")
        if not self.model.changes_made:
            self.repo.checkout(ref)
            assert self.repo.read_only
            self.model.checkout_commit(ref)
        else:
            with pytest.raises(ValueError, match="uncommitted changes"):
                self.repo.checkout(ref)

    @rule(ref=tags)
    def checkout_tag(self, ref):
        """
        Tags and branches are combined here since checkout magically works for both.
        This test is relying on the model tracking tags and branches accurately.
        """
        if ref in self.model.tags and not self.model.changes_made:
            note(f"Checking out tag {ref!r}")
            self.repo.checkout(tag=ref)
            assert self.repo.read_only
            self.model.checkout_tag(ref)
        else:
            note("Expecting error.")
            with pytest.raises(ValueError):
                self.repo.checkout(tag=ref)

    @rule(ref=branches)
    def checkout_branch(self, ref):
        if ref in self.model.branches and not self.model.changes_made:
            note(f"Checking out branch {ref!r}")
            self.repo.checkout(branch=ref)
            assert not self.repo.read_only
            self.model.checkout_branch(ref)

    @rule(name=simple_text | st.just("main"), target=branches)
    def new_branch(self, name):
        note(f"Creating branch {name!r}")
        # we can create a tag and branch with the same name
        if not self.model.changes_made and name not in self.model.branches:
            self.repo.new_branch(name)
            assert not self.repo.read_only
            self.model.new_branch(name)
        else:
            note("Expecting error.")
            with pytest.raises(ValueError):
                self.repo.new_branch(name)
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
    def tag(self, name, commit_id, message):
        note(f"Creating tag {name!r} for commit {commit_id!r} with message {message!r}")
        # we can create a tag and branch with the same name
        if name not in self.model.tags:
            self.repo.tag(name, commit_id)
            self.model.tag(name, commit_id)
        else:
            note("Expecting error.")
            with pytest.raises(ValueError):
                self.repo.tag(name, commit_id)
        # returning this `name` to the Bundle is OK even if the tag was not created
        # This will test out checking out and deleting a tag that does not exist.
        return name


    def reset(self) -> None:
        self.repo.reset()
        self.model.checkout_branch(self.model.branch)

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

    # @invariant()
    # def check_list_prefix_from_root(self):
    #     model_list = self.model.list_prefix("")
    #     al_list = self.repo.list_prefix("")

    #     assert sorted(model_list) == sorted(al_list)
    #     # FIXME:
    #     docs = self.repo.getitems(al_list)

    #     for k in model_list:
    #         assert self.model[k] == json.loads(docs[k])

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

    def teardown(self):
        note("tearing down...")
        # sync(clear_metastore_async, name=self.repo.repo_name, metastore=self.metastore)


def test_stateful_repo_ops():
    def mk_test_instance_sync():
        return VersionControlStateMachine()

    settings = Settings(deadline=None, max_examples=200, report_multiple_bugs=False)
    run_state_machine_as_test(mk_test_instance_sync, settings=settings)
