import functools
import json
import pickle
from collections.abc import Callable
from typing import Any, TypeVar, cast

import hypothesis.strategies as st
import numpy as np
import pytest
from hypothesis import assume, note, settings
from hypothesis.stateful import (
    precondition,
    rule,
    run_state_machine_as_test,
)
from packaging.version import Version

import icechunk as ic
import zarr
from icechunk import Repository, Storage, in_memory_storage
from icechunk.testing import strategies as icst
from zarr.core.buffer import default_buffer_prototype
from zarr.testing.stateful import ZarrHierarchyStateMachine
from zarr.testing.strategies import (
    node_names,
    np_array_and_chunks,
)

PROTOTYPE = default_buffer_prototype()

Frequency = TypeVar("Frequency", bound=Callable[..., Any])

# pytestmark = [
#     pytest.mark.filterwarnings(
#         "ignore::zarr.core.dtype.common.UnstableSpecificationWarning"
#     ),
# ]


def with_frequency(frequency: float) -> Callable[[Frequency], Frequency]:
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

    def decorator(func: Frequency) -> Frequency:
        # Create a counter attribute name specific to this function
        counter_attr = f"__{func.__name__}_counter"

        @functools.wraps(func)
        def wrapper(self: object, *args: Any, **kwargs: Any) -> Any:
            return func(self, *args, **kwargs)

        # Add precondition that checks frequency
        @precondition
        def frequency_check(self: object) -> bool:
            # Initialize counter if it doesn't exist
            if not hasattr(self, counter_attr):
                setattr(self, counter_attr, 0)

            # Increment counter
            current_count = cast(int, getattr(self, counter_attr)) + 1
            setattr(self, counter_attr, current_count)

            # Check if we should run based on frequency
            # This gives roughly the right frequency over many calls
            return (current_count * frequency) % 1.0 >= (1.0 - frequency)

        # Apply the precondition to the wrapped function
        return frequency_check(wrapper)  # type: ignore [return-value]

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
    store: ic.IcechunkStore  # Override parent class type annotation

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
    def reopen_with_config(self, data: st.DataObject) -> None:
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
    def rewrite_manifests(self, data: st.DataObject) -> None:
        sconfig = ic.ManifestSplittingConfig.from_dict(
            {
                ic.ManifestSplitCondition.AnyArray(): {
                    ic.ManifestSplitDimCondition.Any(): data.draw(
                        st.integers(min_value=1, max_value=10)
                    )
                }
            }
        )

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
    def commit_with_check(self, data: st.DataObject) -> None:
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

        get_after_cmp: Any
        get_before_cmp: Any
        # if it's metadata, we need to compare the data parsed, not raw (because of map ordering)
        if path.endswith(".json"):
            get_after_cmp = json.loads(get_after.to_bytes())
            get_before_cmp = json.loads(get_before.to_bytes())
        else:
            get_after_cmp = get_after.to_bytes()
            get_before_cmp = get_before.to_bytes()

        if get_before_cmp != get_after_cmp:
            get_expect = self._sync(self.model.get(path, prototype=PROTOTYPE))
            assert get_expect
            raise ValueError(
                f"Value changed before and after commit for path {path}"
                f" \n\n Before : {get_before_cmp!r} \n\n "
                f"After: {get_after_cmp!r}, \n\n "
                f"Expected: {get_expect.to_bytes()!r}"
            )

    @rule(
        data=st.data(),
        name=node_names,
        array_and_chunks=np_array_and_chunks(),
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
        # Skip bytes, unicode string, and datetime dtypes with zarr < 3.1.0
        # These have codec/dtype issues that were fixed in 3.1.0's dtype refactor
        if Version(zarr.__version__) < Version("3.1.0"):
            assume(array.dtype.kind not in ("S", "U", "M", "m"))
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

    @rule()
    def pickle_objects(self) -> None:
        if not self.store.session.has_uncommitted_changes:
            session = self.store.session.fork()
            pickle.loads(pickle.dumps(session))

        pickle.loads(pickle.dumps(self.repo))


def test_zarr_hierarchy() -> None:
    def mk_test_instance_sync() -> ModifiedZarrHierarchyStateMachine:
        return ModifiedZarrHierarchyStateMachine(in_memory_storage())

    run_state_machine_as_test(  # type: ignore[no-untyped-call]
        mk_test_instance_sync, settings=settings(report_multiple_bugs=False)
    )


def test_zarr_store() -> None:
    pytest.skip("icechunk is more strict about keys")
    # repo = Repository.create(in_memory_storage())
    # store = repo.writable_session("main").store

    # def mk_test_instance_sync() -> ZarrHierarchyStateMachine:
    #     return ZarrStoreStateMachine(store)

    # run_state_machine_as_test(
    #     mk_test_instance_sync, settings=settings(report_multiple_bugs=False)
    # )
