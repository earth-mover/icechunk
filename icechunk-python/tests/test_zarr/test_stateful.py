from typing import Any

import hypothesis.strategies as st
import numpy as np
import pytest
from hypothesis import assume, note
from hypothesis.stateful import (
    Settings,
    initialize,
    invariant,
    precondition,
    rule,
    run_state_machine_as_test,
)

from icechunk import IcechunkStore, StorageConfig
from zarr.testing.stateful import ZarrHierarchyStateMachine, ZarrStoreStateMachine
from zarr.testing.strategies import (
    node_names,
    np_array_and_chunks,
    numpy_arrays,
)


# TODO: more before/after commit invariants?
# TODO: add "/" to self.all_groups, deleting "/" seems to be problematic
class ModifiedZarrHierarchyStateMachine(ZarrHierarchyStateMachine):
    def __init__(self, *args, **kwargs):
        note("__init__")
        super().__init__(*args, **kwargs)

    @initialize()
    def init_store(self) -> None:
        note("init_store")
        import zarr

        # This lets us reuse the fixture provided store.
        self._sync(self.store.clear())  # FIXME: upstream
        lsbefore = self._sync_iter(self.store.list_prefix(""))
        zarr.group(store=self.store)
        lsafter = self._sync_iter(self.store.list_prefix(""))
        assert len(lsbefore) == 0, ("more than 0 keys after clearing", lsbefore)
        assert len(lsafter) == 1, "more than 1 key after creating group"

    @precondition(lambda self: self.store.has_uncommitted_changes)
    @rule()
    def commit_with_check(self):
        note("committing and checking list_prefix")
        lsbefore = sorted(self._sync_iter(self.store.list_prefix("")))
        self.store.commit("foo")
        lsafter = sorted(self._sync_iter(self.store.list_prefix("")))
        if lsbefore != lsafter:
            lsexpect = sorted(self._sync_iter(self.model.list_prefix("")))
            raise ValueError(
                f"listing changed before ({len(lsbefore)} items) and after ({len(lsafter)} items) committing."
                f" \n\n Before : {lsbefore!r} \n\n After: {lsafter!r}, \n\n Expected: {lsexpect!r}"
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

    # TODO: port to zarr
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
    store = IcechunkStore.create(StorageConfig.memory())

    def mk_test_instance_sync() -> ModifiedZarrHierarchyStateMachine:
        return ModifiedZarrHierarchyStateMachine(store)

    run_state_machine_as_test(
        mk_test_instance_sync, settings=Settings(report_multiple_bugs=False)
    )


def test_zarr_store():
    pytest.skip("icechunk is more strict about keys")
    store = IcechunkStore.create(StorageConfig.memory())

    def mk_test_instance_sync() -> ZarrHierarchyStateMachine:
        return ZarrStoreStateMachine(store)

    run_state_machine_as_test(
        mk_test_instance_sync, settings=Settings(report_multiple_bugs=False)
    )
