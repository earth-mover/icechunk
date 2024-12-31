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
        slice(draw(st.integers(min_value=1, max_value=ndim))) if subset else slice(None)
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
        if get_before != get_after:
            get_expect = self._sync(self.model.get(path, prototype=PROTOTYPE))
            assert get_expect
            raise ValueError(
                f"Value changed before and after commit for path {path}"
                f" \n\n Before : {get_before.to_bytes()!r} \n\n "
                f"After: {get_after.to_bytes()!r}, \n\n "
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
        # TODO: fix complex fill values GH391
        assume(not np.iscomplexobj(array))
        super().add_array(data, name, array_and_chunks)

    #####  TODO: port everything below to zarr
    @rule()
    def clear(self) -> None:
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

    @precondition(lambda self: bool(self.all_groups))
    @rule(data=st.data())
    def check_list_dir_group(self, data) -> None:
        group = data.draw(st.sampled_from(sorted(self.all_groups)))
        model_ls = sorted(self._sync_iter(self.model.list_dir(group)))
        store_ls = sorted(self._sync_iter(self.store.list_dir(group)))
        assert model_ls == store_ls, (model_ls, store_ls)

    @precondition(lambda self: bool(self.all_arrays))
    @rule(data=st.data())
    def check_list_dir_array(self, data) -> None:
        array = data.draw(st.sampled_from(sorted(self.all_arrays)))
        arr = zarr.open_array(path=array, store=self.model)
        chunk_path = data.draw(chunk_paths(ndim=arr.ndim, numblocks=arr.cdata_shape))
        path = f"{array}/c/{chunk_path}"
        note(f"list_dir for {path=!r}")
        model_ls = sorted(self._sync_iter(self.model.list_dir(path)))
        store_ls = sorted(self._sync_iter(self.store.list_dir(path)))
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

    @invariant()
    def check_list_prefix_from_root(self) -> None:
        model_list = self._sync_iter(self.model.list_prefix(""))
        store_list = self._sync_iter(self.store.list_prefix(""))
        note(f"Checking {len(model_list)} expected keys vs {len(store_list)} actual keys")
        assert sorted(model_list) == sorted(store_list), (
            sorted(model_list),
            sorted(store_list),
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
