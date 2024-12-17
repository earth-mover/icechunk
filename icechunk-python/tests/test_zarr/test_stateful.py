from typing import Any

import hypothesis.strategies as st
import numpy as np
import pytest
from hypothesis import assume
from hypothesis.stateful import (
    Settings,
    rule,
    run_state_machine_as_test,
)

from icechunk import Repository, StorageConfig
from zarr.testing.stateful import ZarrHierarchyStateMachine, ZarrStoreStateMachine
from zarr.testing.strategies import (
    node_names,
    np_array_and_chunks,
    numpy_arrays,
)


class ModifiedZarrHierarchyStateMachine(ZarrHierarchyStateMachine):
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


def test_zarr_hierarchy():
    store = Repository.create(StorageConfig.memory()).writable_session("main").store()

    def mk_test_instance_sync() -> ModifiedZarrHierarchyStateMachine:
        return ModifiedZarrHierarchyStateMachine(store)

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
