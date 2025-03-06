import json
from typing import Any

import hypothesis.strategies as st
import numpy as np
import pytest
from hypothesis import assume, note
from hypothesis.stateful import (
    Settings,
    invariant,
    precondition,
    rule,
    run_state_machine_as_test,
)

import zarr
from icechunk import Repository, in_memory_storage
from zarr.core.buffer import default_buffer_prototype
from zarr.testing.stateful import ZarrHierarchyStateMachine
from zarr.testing.strategies import (
    node_names,
    np_array_and_chunks,
    numpy_arrays,
)

PROTOTYPE = default_buffer_prototype()


@st.composite
def chunk_paths(
    draw: st.DrawFn, ndim: int, numblocks: tuple[int, ...], subset: bool = True
) -> str:
    blockidx = draw(
        st.tuples(*tuple(st.integers(min_value=0, max_value=b - 1) for b in numblocks))
    )
    subset_slicer = (
        slice(draw(st.integers(min_value=0, max_value=ndim))) if subset else slice(None)
    )
    return "/".join(map(str, blockidx[subset_slicer]))


# TODO: more before/after commit invariants?
# TODO: add "/" to self.all_groups, deleting "/" seems to be problematic
class ModifiedZarrHierarchyStateMachine(ZarrHierarchyStateMachine):
    def __init__(self, repo: Repository) -> None:
        self.repo = repo
        store = repo.writable_session("main").store
        super().__init__(store)

    @precondition(lambda self: self.store.session.has_uncommitted_changes)
    @rule(data=st.data())
    def commit_with_check(self, data) -> None:
        note("committing and checking list_prefix")

        lsbefore = sorted(self._sync_iter(self.store.list_prefix("")))
        path = data.draw(st.sampled_from(lsbefore))
        get_before = self._sync(self.store.get(path, prototype=PROTOTYPE))
        assert get_before

        self.store.session.commit("foo")

        self.store = self.repo.writable_session("main").store

        lsafter = sorted(self._sync_iter(self.store.list_prefix("")))
        get_after = self._sync(self.store.get(path, prototype=PROTOTYPE))
        assert get_after

        if lsbefore != lsafter:
            lsexpect = sorted(self._sync_iter(self.model.list_prefix("")))
            raise ValueError(
                f"listing changed before ({len(lsbefore)} items) and after ({len(lsafter)} items) committing."
                f" \n\n Before : {lsbefore!r} \n\n After: {lsafter!r}, \n\n Expected: {lsexpect!r}"
            )

        # if it's metadata, we need to compare the data parsed, not raw (because of map ordering)
        if path.endswith(".json"):
            get_after = json.loads(get_after.to_bytes())
            get_before = json.loads(get_before.to_bytes())
        else:
            get_after = get_after.to_bytes()
            get_before = get_before.to_bytes()

        if get_before != get_after:
            get_expect = self._sync(self.model.get(path, prototype=PROTOTYPE))
            assert get_expect
            raise ValueError(
                f"Value changed before and after commit for path {path}"
                f" \n\n Before : {get_before!r} \n\n "
                f"After: {get_after!r}, \n\n "
                f"Expected: {get_expect.to_bytes()!r}"
            )

    @rule(
        data=st.data(),
        name=node_names,
        array_and_chunks=np_array_and_chunks(
            arrays=numpy_arrays(zarr_formats=st.just(3))
        ),
    )
    def add_array(
        self,
        data: st.DataObject,
        name: str,
        array_and_chunks: tuple[np.ndarray[Any, Any], tuple[int, ...]],
    ) -> None:
        array, _ = array_and_chunks
        # TODO: support size-0 arrays GH392
        assume(array.size > 0)
        super().add_array(data, name, array_and_chunks)

    #####  TODO: port everything below to zarr
    @rule()
    def clear(self) -> None:
        note("clearing")
        import zarr

        self._sync(self.store.clear())
        self._sync(self.model.clear())

        assert self._sync(self.store.is_empty("/"))
        assert self._sync(self.model.is_empty("/"))

        self.all_groups.clear()
        self.all_arrays.clear()

        zarr.group(store=self.store)
        zarr.group(store=self.model)

        assert not self._sync(self.store.is_empty("/"))
        # TODO: MemoryStore is broken?
        # assert not self._sync(self.model.is_empty("/"))

    def draw_directory(self, data) -> str:
        group_st = (
            st.sampled_from(sorted(self.all_groups)) if self.all_groups else st.nothing()
        )
        array_st = (
            st.sampled_from(sorted(self.all_arrays)) if self.all_arrays else st.nothing()
        )
        array_or_group = data.draw(st.one_of(group_st, array_st))
        if data.draw(st.booleans()) and array_or_group in self.all_arrays:
            arr = zarr.open_array(path=array_or_group, store=self.model)
            path = data.draw(
                st.one_of(
                    st.sampled_from([array_or_group]),
                    chunk_paths(ndim=arr.ndim, numblocks=arr.cdata_shape).map(
                        lambda x: f"{array_or_group}/c/"
                    ),
                )
            )
        else:
            path = array_or_group
        return path

    @precondition(lambda self: bool(self.all_groups))
    @rule(data=st.data())
    def check_list_dir(self, data) -> None:
        path = self.draw_directory(data)
        note(f"list_dir for {path=!r}")
        model_ls = sorted(self._sync_iter(self.model.list_dir(path)))
        store_ls = sorted(self._sync_iter(self.store.list_dir(path)))
        if model_ls != store_ls and set(model_ls).symmetric_difference(set(store_ls)) != {
            "c"
        }:
            # Consider .list_dir("path/to/array") for an array with a single chunk.
            # The MemoryStore model will return `"c", "zarr.json"` only if the chunk exists
            # If that chunk was deleted, then `"c"` is not returned.
            # LocalStore will not have this behaviour :/
            # In Icechunk, we always return the `c` so ignore this inconsistency.
            assert model_ls == store_ls, (model_ls, store_ls)

    @precondition(lambda self: bool(self.all_arrays))
    @rule(data=st.data())
    def delete_chunk(self, data) -> None:
        array = data.draw(st.sampled_from(sorted(self.all_arrays)))
        arr = zarr.open_array(path=array, store=self.model)
        chunk_path = data.draw(
            chunk_paths(ndim=arr.ndim, numblocks=arr.cdata_shape, subset=False)
        )
        path = f"{array}/c/{chunk_path}"
        note(f"deleting chunk {path=!r}")
        self._sync(self.model.delete(path))
        self._sync(self.store.delete(path))

    @precondition(lambda self: bool(self.all_arrays) or bool(self.all_groups))
    @rule(data=st.data())
    def delete_dir(self, data) -> None:
        path = self.draw_directory(data)
        note(f"delete_dir with {path=!r}")
        self._sync(self.model.delete_dir(path))
        self._sync(self.store.delete_dir(path))

        matches = set()
        for node in self.all_groups | self.all_arrays:
            if node.startswith(path):
                matches.add(node)
        self.all_groups = self.all_groups - matches
        self.all_arrays = self.all_arrays - matches

    @invariant()
    def check_list_prefix_from_root(self) -> None:
        model_list = self._sync_iter(self.model.list_prefix(""))
        store_list = self._sync_iter(self.store.list_prefix(""))
        note(f"Checking {len(model_list)} expected keys vs {len(store_list)} actual keys")
        assert sorted(model_list) == sorted(store_list), (
            sorted(model_list),
            sorted(store_list),
        )

        # check that our internal state matches that of the store and model
        assert all(
            f"{path}/zarr.json" in model_list
            for path in self.all_groups | self.all_arrays
        )
        assert all(
            f"{path}/zarr.json" in store_list
            for path in self.all_groups | self.all_arrays
        )


def test_zarr_hierarchy() -> None:
    repo = Repository.create(in_memory_storage())

    def mk_test_instance_sync() -> ModifiedZarrHierarchyStateMachine:
        return ModifiedZarrHierarchyStateMachine(repo)

    run_state_machine_as_test(
        mk_test_instance_sync, settings=Settings(report_multiple_bugs=False)
    )


def test_zarr_store() -> None:
    pytest.skip("icechunk is more strict about keys")
    # repo = Repository.create(in_memory_storage())
    # store = repo.writable_session("main").store

    # def mk_test_instance_sync() -> ZarrHierarchyStateMachine:
    #     return ZarrStoreStateMachine(store)

    # run_state_machine_as_test(
    #     mk_test_instance_sync, settings=Settings(report_multiple_bugs=False)
    # )
