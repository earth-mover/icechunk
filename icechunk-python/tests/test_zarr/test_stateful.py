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

from icechunk import Repository, StorageConfig
from zarr.core.buffer import default_buffer_prototype
from zarr.testing.stateful import ZarrHierarchyStateMachine, ZarrStoreStateMachine
from zarr.testing.strategies import (
    node_names,
    np_array_and_chunks,
    numpy_arrays,
)

PROTOTYPE = default_buffer_prototype()


# TODO: more before/after commit invariants?
# TODO: add "/" to self.all_groups, deleting "/" seems to be problematic
class ModifiedZarrHierarchyStateMachine(ZarrHierarchyStateMachine):
    def __init__(self, repo):
        self.repo = repo
        store = repo.writable_session("main").store()
        super().__init__(store)

    @precondition(lambda self: self.store.session.has_uncommitted_changes)
    @rule(data=st.data())
    def commit_with_check(self, data):
        note("committing and checking list_prefix")

        lsbefore = sorted(self._sync_iter(self.store.list_prefix("")))
        path = data.draw(st.sampled_from(lsbefore))
        get_before = self._sync(self.store.get(path, prototype=PROTOTYPE))

        self.store.session.commit("foo")

        self.store = self.repo.writable_session("main").store()

        lsafter = sorted(self._sync_iter(self.store.list_prefix("")))
        get_after = self._sync(self.store.get(path, prototype=PROTOTYPE))

        if lsbefore != lsafter:
            lsexpect = sorted(self._sync_iter(self.model.list_prefix("")))
            raise ValueError(
                f"listing changed before ({len(lsbefore)} items) and after ({len(lsafter)} items) committing."
                f" \n\n Before : {lsbefore!r} \n\n After: {lsafter!r}, \n\n Expected: {lsexpect!r}"
            )
        if get_before != get_after:
            get_expect = self._sync(self.model.xget(path, prototype=PROTOTYPE))
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
    def clear(self):
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

    @invariant()
    def check_list_prefix_from_root(self) -> None:
        model_list = self._sync_iter(self.model.list_prefix(""))
        store_list = self._sync_iter(self.store.list_prefix(""))
        note(f"Checking {len(model_list)} expected keys vs {len(store_list)} actual keys")
        assert sorted(model_list) == sorted(store_list), (
            sorted(model_list),
            sorted(store_list),
        )


def test_zarr_hierarchy():
    repo = Repository.create(StorageConfig.memory())

    def mk_test_instance_sync() -> ModifiedZarrHierarchyStateMachine:
        return ModifiedZarrHierarchyStateMachine(repo)

    run_state_machine_as_test(
        mk_test_instance_sync, settings=Settings(report_multiple_bugs=False)
    )


def test_zarr_store():
    pytest.skip("icechunk is more strict about keys")
    store = Repository.create(StorageConfig.memory()).writable_session("main").store()

    def mk_test_instance_sync() -> ZarrHierarchyStateMachine:
        return ZarrStoreStateMachine(store)

    run_state_machine_as_test(
        mk_test_instance_sync, settings=Settings(report_multiple_bugs=False)
    )
