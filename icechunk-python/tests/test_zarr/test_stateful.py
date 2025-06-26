import functools
import json
from typing import Any

import hypothesis.extra.numpy as npst
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

import icechunk as ic
import zarr
from icechunk import Repository, Storage, in_memory_storage
from icechunk.testing import strategies as icst
from zarr.core.buffer import default_buffer_prototype
from zarr.testing.stateful import ZarrHierarchyStateMachine
from zarr.testing.strategies import (
    basic_indices,
    node_names,
    np_array_and_chunks,
    numpy_arrays,
    orthogonal_indices,
)

PROTOTYPE = default_buffer_prototype()

# pytestmark = [
#     pytest.mark.filterwarnings(
#         "ignore::zarr.core.dtype.common.UnstableSpecificationWarning"
#     ),
# ]


def with_frequency(frequency):
    """
    Decorator to control how frequently a rule runs in Hypothesis stateful tests.

    Args:
        frequency: Float between 0 and 1, where 1.0 means always run,
                  0.1 means run ~10% of the time, etc.

    Usage:
        @rule()
        @with_frequency(0.1)  # Run ~10% of the time
        def rare_operation(self):
            pass
    """

    def decorator(func):
        # Create a counter attribute name specific to this function
        counter_attr = f"__{func.__name__}_counter"

        @functools.wraps(func)
        def wrapper(self, *args, **kwargs):
            return func(self, *args, **kwargs)

        # Add precondition that checks frequency
        @precondition
        def frequency_check(self):
            # Initialize counter if it doesn't exist
            if not hasattr(self, counter_attr):
                setattr(self, counter_attr, 0)

            # Increment counter
            current_count = getattr(self, counter_attr) + 1
            setattr(self, counter_attr, current_count)

            # Check if we should run based on frequency
            # This gives roughly the right frequency over many calls
            return (current_count * frequency) % 1.0 >= (1.0 - frequency)

        # Apply the precondition to the wrapped function
        return frequency_check(wrapper)

    return decorator


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
    def __init__(self, storage: Storage) -> None:
        self.storage = storage
        self.repo = Repository.create(self.storage)
        store = self.repo.writable_session("main").store
        super().__init__(store)

    @precondition(
        lambda self: not self.store.session.has_uncommitted_changes
        and bool(self.all_arrays)
    )
    @rule(data=st.data())
    def reopen_with_config(self, data):
        array_paths = data.draw(
            st.lists(st.sampled_from(sorted(self.all_arrays)), max_size=3, unique=True)
        )
        arrays = tuple(zarr.open_array(self.model, path=path) for path in array_paths)
        sconfig = data.draw(icst.splitting_configs(arrays=arrays))
        config = ic.RepositoryConfig(
            inline_chunk_threshold_bytes=0, manifest=ic.ManifestConfig(splitting=sconfig)
        )
        note(f"reopening with splitting config {sconfig=!r}")
        self.repo = Repository.open(self.storage, config=config)
        if data.draw(st.booleans()):
            self.repo.save_config()
        self.store = self.repo.writable_session("main").store

    @precondition(lambda self: not self.store.session.has_uncommitted_changes)
    @rule(data=st.data())
    def rewrite_manifests(self, data):
        config_dict = {
            ic.ManifestSplitCondition.AnyArray(): {
                ic.ManifestSplitDimCondition.Any(): data.draw(
                    st.integers(min_value=1, max_value=10)
                )
            }
        }
        sconfig = ic.ManifestSplittingConfig.from_dict(config_dict)

        config = ic.RepositoryConfig(
            inline_chunk_threshold_bytes=0, manifest=ic.ManifestConfig(splitting=sconfig)
        )
        note(f"rewriting manifests with config {sconfig=!r}")
        self.repo = Repository.open(self.storage, config=config)
        self.repo.rewrite_manifests(
            f"rewriting manifests with {sconfig!s}", branch="main"
        )
        if data.draw(st.booleans()):
            self.repo.save_config()
        self.store = self.repo.writable_session("main").store

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

    @precondition(lambda self: bool(self.all_groups))
    @rule(data=st.data())
    def check_list_dir(self, data: st.DataObject) -> None:
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

    #####  TODO: port everything below to zarr
    @precondition(lambda self: bool(self.all_arrays))
    @rule(data=st.data())
    def check_array(self, data: st.DataObject) -> None:
        path = data.draw(st.sampled_from(sorted(self.all_arrays)))
        actual = zarr.open_array(self.store, path=path)[:]
        expected = zarr.open_array(self.model, path=path)[:]
        np.testing.assert_equal(actual, expected)

    @precondition(lambda self: bool(self.all_arrays))
    @rule(data=st.data())
    def overwrite_array_orthogonal_indexing(self, data: st.DataObject) -> None:
        array = data.draw(st.sampled_from(sorted(self.all_arrays)))
        model_array = zarr.open_array(path=array, store=self.model)
        store_array = zarr.open_array(path=array, store=self.store)
        indexer, _ = data.draw(orthogonal_indices(shape=model_array.shape))
        note(f"overwriting array orthogonal {indexer=}")
        new_data = data.draw(
            npst.arrays(shape=model_array.oindex[indexer].shape, dtype=model_array.dtype)
        )
        model_array.oindex[indexer] = new_data
        store_array.oindex[indexer] = new_data

    #####  TODO: delete after next Zarr release (Jun 18, 2025)
    @rule()
    @with_frequency(0.25)
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

    @precondition(lambda self: bool(self.all_arrays))
    @rule(data=st.data())
    def overwrite_array_basic_indexing(self, data) -> None:
        array = data.draw(st.sampled_from(sorted(self.all_arrays)))
        model_array = zarr.open_array(path=array, store=self.model)
        store_array = zarr.open_array(path=array, store=self.store)
        slicer = data.draw(basic_indices(shape=model_array.shape))
        note(f"overwriting array basic {slicer=}")
        new_data = data.draw(
            npst.arrays(shape=model_array[slicer].shape, dtype=model_array.dtype)
        )
        model_array[slicer] = new_data
        store_array[slicer] = new_data

    @precondition(lambda self: bool(self.all_arrays))
    @rule(data=st.data())
    def resize_array(self, data) -> None:
        array = data.draw(st.sampled_from(sorted(self.all_arrays)))
        model_array = zarr.open_array(path=array, store=self.model)
        store_array = zarr.open_array(path=array, store=self.store)
        ndim = model_array.ndim
        new_shape = data.draw(npst.array_shapes(max_dims=ndim, min_dims=ndim, min_side=1))
        note(f"resizing array from {model_array.shape} to {new_shape}")
        model_array.resize(new_shape)
        store_array.resize(new_shape)

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
    def mk_test_instance_sync() -> ModifiedZarrHierarchyStateMachine:
        return ModifiedZarrHierarchyStateMachine(in_memory_storage())

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
