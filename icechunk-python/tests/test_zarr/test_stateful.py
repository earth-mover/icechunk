import functools
import json
import pickle
from collections.abc import Callable
from typing import Any, TypeVar, cast

import hypothesis.extra.numpy as npst
import hypothesis.strategies as st
import numpy as np
import pytest
from hypothesis import assume, note, settings
from hypothesis.stateful import (
    initialize,
    invariant,
    precondition,
    rule,
    run_state_machine_as_test,
)
from packaging.version import Version

import icechunk as ic
import zarr
from icechunk import Repository, SessionMode, Storage, in_memory_storage
from icechunk.testing import strategies as icst
from zarr.core.buffer import default_buffer_prototype
from zarr.testing.stateful import ZarrHierarchyStateMachine
from zarr.testing.strategies import (
    basic_indices,
    node_names,
    np_array_and_chunks,
    orthogonal_indices,
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
        # Create a temporary repository with spec_version=1 in a separate storage
        # This will be replaced in init_store with the Hypothesis-sampled version
        # we need this in order to properly initialize the superclass MemoryStore
        # model
        temp_repo = Repository.create(in_memory_storage(), spec_version=1)
        temp_store = temp_repo.writable_session("main").store
        super().__init__(temp_store)

    def new_session(self, rearrange: bool = False) -> ic.IcechunkStore:
        """Create a new session (rearrange if requested and spec_version >= 2)."""
        if rearrange and self.repo.spec_version >= 2:
            note("opening rearrange session")
            return self.repo.rearrange_session("main").store
        else:
            note("opening writable session")
            return self.repo.writable_session("main").store

    @initialize(spec_version=st.sampled_from([1, 2]))
    def init_store(self, spec_version: int) -> None:
        """Override parent's init_store to sample spec_version and create repository."""
        # necessary to control the order of calling. if multiple intiliazes they will be
        # called by hypothesis in a random order
        note(f"Creating repository with spec_version={spec_version}")

        # Create repository with the drawn spec version
        self.repo = Repository.create(self.storage, spec_version=spec_version)
        self.store = self.repo.writable_session("main").store

        super().init_store()

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
        self.store = self.new_session(rearrange=data.draw(st.booleans()))

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
        self.store = self.new_session(rearrange=data.draw(st.booleans()))

    @precondition(lambda self: self.store.session.has_uncommitted_changes)
    @rule(data=st.data())
    def commit_with_check(self, data: st.DataObject) -> None:
        note("committing and checking list_prefix")

        lsbefore = sorted(self._sync_iter(self.store.list_prefix("")))
        path = data.draw(st.sampled_from(lsbefore))
        get_before = self._sync(self.store.get(path, prototype=PROTOTYPE))
        assert get_before

        self.store.session.commit("foo")

        self.store = self.new_session(rearrange=data.draw(st.booleans()))

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

    @rule()
    @precondition(lambda self: self.repo.spec_version == 1)
    @precondition(lambda self: not self.store.session.has_uncommitted_changes)
    def upgrade_spec_version(self) -> None:
        """Upgrade repository from spec version 1 to version 2."""
        note("upgrading spec version from 1 to 2")
        ic.upgrade_icechunk_repository(self.repo)
        # Reopen to pick up the upgraded spec version
        self.repo = Repository.open(self.storage)
        self.store = self.repo.writable_session("main").store

    # NOTE:
    # Bundles (https://hypothesis.readthedocs.io/en/latest/stateful.html#rule-based-state-machines)
    # may seem like the right way to draw the names in a rule rather than filterwarnings
    # with assume. But unfortunately this would require some invasive changes inside
    # ZarrHierarchyStateMachine to track the all_array and all_groups in a bundle
    # and its add_*/delete_* methods don't return paths for bundle population.
    @rule(data=st.data(), new_name=node_names)
    @precondition(lambda self: self.store.session.mode == SessionMode.REARRANGE)
    @precondition(lambda self: bool(self.all_arrays) or bool(self.all_groups))
    def move_node(self, data: st.DataObject, new_name: str) -> None:
        """Move a node to a new location (only in a rearrange session)."""
        existing_nodes = self.all_arrays | self.all_groups
        source = data.draw(st.sampled_from(sorted(existing_nodes)))

        # Draw destination parent from existing groups (or root ""), excluding:
        # - source and its children (prevent moving into own subtree)
        # - parents where dest would conflict with existing nodes or equal source
        # NOTE: Moving to a non-existent parent silently loses data (icechunk bug),
        # so we only test moves to existing groups.
        def is_valid_parent(g: str) -> bool:
            dest = f"{g}/{new_name}".lstrip("/")
            return (
                # "foo" can't move under "foo/..." (parent can't be source)
                g != source
                # "foo" can't move under "foo/bar/..." (parent can't be under source)
                and not g.startswith(source + "/")
                # "bar/baz" can't overwrite existing "bar/baz"
                and dest not in existing_nodes
            )

        dest_parents = sorted(filter(is_valid_parent, self.all_groups | {""}))
        assume(dest_parents)  # skip if no valid destinations
        dest_parent = data.draw(st.sampled_from(dest_parents))
        dest = f"{dest_parent}/{new_name}".lstrip("/")

        note(f"moving {source!r} to {dest!r}")

        # Perform move on store (paths need leading slash)
        self.store.session.move(f"/{source}", f"/{dest}")

        # Update model - move in MemoryStore
        self.move_in_model(source, dest)

        # Update tracking sets - remap paths from source to dest
        def remap(path: str) -> str:
            if path == source:
                # exact match: "foo" -> "bar"
                return dest
            if path.startswith(source + "/"):
                # child path: "foo/child" -> "bar/child"
                return dest + path[len(source) :]
            # unrelated path: "other" -> "other"
            return path

        self.all_arrays = {remap(p) for p in self.all_arrays}
        self.all_groups = {remap(p) for p in self.all_groups}

    def move_in_model(self, source: str, dest: str) -> None:
        """Move all keys from source to dest in the model store.

        We operate at the key level (list_prefix) rather than zarr object level because:
        - MemoryStore has no native move/rename
        - Key-level copy preserves raw bytes without re-encoding chunks

        This means the names are a bit different:
        - Store keys always have form "node/zarr.json" or "node/c/...", never bare "node"
        """
        all_keys = list(self._sync_iter(self.model.list_prefix("")))
        keys_to_move = [k for k in all_keys if k.startswith(source + "/")]
        for old_key in keys_to_move:
            new_key = dest + old_key[len(source) :]
            data = self._sync(self.model.get(old_key, prototype=PROTOTYPE))
            if data is not None:
                self._sync(self.model.set(new_key, data))
                self._sync(self.model.delete(old_key))

    @rule(
        data=st.data(),
        name=node_names,
        array_and_chunks=np_array_and_chunks(),
    )
    @precondition(lambda self: self.store.session.mode == SessionMode.WRITABLE)
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

    @rule(name=node_names, data=st.data())
    @precondition(lambda self: self.store.session.mode == SessionMode.WRITABLE)
    def add_group(self, name: str, data: st.DataObject) -> None:
        super().add_group(name, data)

    @rule(data=st.data())
    @precondition(lambda self: bool(self.all_arrays))
    @precondition(lambda self: self.store.session.mode == SessionMode.WRITABLE)
    def delete_array_using_del(self, data: st.DataObject) -> None:
        super().delete_array_using_del(data)

    @rule(data=st.data())
    @precondition(
        # Parent filters to nested groups (containing "/"), so we need at least one
        lambda self: any("/" in g for g in self.all_groups)
    )
    @precondition(lambda self: self.store.session.mode == SessionMode.WRITABLE)
    def delete_group_using_del(self, data: st.DataObject) -> None:
        super().delete_group_using_del(data)

    @invariant()
    def check_list_dir(self) -> None:
        # known bug: list_dir returns empty for moved nodes before commit
        is_rearrange = self.store.session.mode == SessionMode.REARRANGE
        has_changes = self.store.session.has_uncommitted_changes
        if is_rearrange and has_changes:
            return

        for path in self.all_groups | self.all_arrays:
            model_ls = sorted(self._sync_iter(self.model.list_dir(path)))
            store_ls = sorted(self._sync_iter(self.store.list_dir(path)))
            if model_ls != store_ls and set(model_ls).symmetric_difference(
                set(store_ls)
            ) != {"c"}:
                # Consider .list_dir("path/to/array") for an array with a single chunk.
                # The MemoryStore model will return `"c", "zarr.json"` only if the chunk exists
                # If that chunk was deleted, then `"c"` is not returned.
                # LocalStore will not have this behaviour :/
                # In Icechunk, we always return the `c` so ignore this inconsistency.
                raise AssertionError(
                    f"list_dir mismatch for {path=}: {model_ls=} != {store_ls=}"
                )

    #####  TODO: port everything below to zarr
    @precondition(lambda self: bool(self.all_arrays))
    @rule(data=st.data())
    def check_array(self, data: st.DataObject) -> None:
        path = data.draw(st.sampled_from(sorted(self.all_arrays)))
        actual = zarr.open_array(self.store, path=path)[:]
        expected = zarr.open_array(self.model, path=path)[:]
        np.testing.assert_equal(actual, expected)

    @precondition(lambda self: bool(self.all_arrays))
    @precondition(lambda self: self.store.session.mode == SessionMode.WRITABLE)
    @rule(data=st.data())
    def overwrite_array_orthogonal_indexing(self, data: st.DataObject) -> None:
        array = data.draw(st.sampled_from(sorted(self.all_arrays)))
        model_array = zarr.open_array(path=array, store=self.model)
        store_array = zarr.open_array(path=array, store=self.store)
        indexer, _ = data.draw(orthogonal_indices(shape=model_array.shape))
        note(f"overwriting array orthogonal {indexer=}")
        shape = model_array.oindex[indexer].shape  # type: ignore[union-attr]
        new_data = data.draw(npst.arrays(shape=shape, dtype=model_array.dtype))
        model_array.oindex[indexer] = new_data
        store_array.oindex[indexer] = new_data

    #####  TODO: delete after next Zarr release (Jun 18, 2025)
    @rule()
    @precondition(lambda self: self.store.session.mode == SessionMode.WRITABLE)
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

    def draw_directory(self, data: st.DataObject) -> str:
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
    @precondition(lambda self: self.store.session.mode == SessionMode.WRITABLE)
    @rule(data=st.data())
    def delete_chunk(self, data: st.DataObject) -> None:
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
    @precondition(lambda self: self.store.session.mode == SessionMode.WRITABLE)
    @rule(data=st.data())
    def overwrite_array_basic_indexing(self, data: st.DataObject) -> None:
        array = data.draw(st.sampled_from(sorted(self.all_arrays)))
        model_array = zarr.open_array(path=array, store=self.model)
        store_array = zarr.open_array(path=array, store=self.store)
        slicer = data.draw(basic_indices(shape=model_array.shape))
        note(f"overwriting array basic {slicer=}")
        shape = model_array[slicer].shape  # type: ignore [union-attr]
        new_data = data.draw(npst.arrays(shape=shape, dtype=model_array.dtype))
        model_array[slicer] = new_data
        store_array[slicer] = new_data

    @precondition(lambda self: bool(self.all_arrays))
    @precondition(lambda self: self.store.session.mode == SessionMode.WRITABLE)
    @rule(data=st.data())
    def resize_array(self, data: st.DataObject) -> None:
        array = data.draw(st.sampled_from(sorted(self.all_arrays)))
        model_array = zarr.open_array(path=array, store=self.model)
        store_array = zarr.open_array(path=array, store=self.store)
        ndim = model_array.ndim
        new_shape = data.draw(npst.array_shapes(max_dims=ndim, min_dims=ndim, min_side=1))
        note(f"resizing array from {model_array.shape} to {new_shape}")
        model_array.resize(new_shape)
        store_array.resize(new_shape)

    @precondition(lambda self: bool(self.all_arrays) or bool(self.all_groups))
    @precondition(lambda self: self.store.session.mode == SessionMode.WRITABLE)
    @rule(data=st.data())
    def delete_dir(self, data: st.DataObject) -> None:
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

    @rule()
    def pickle_objects(self) -> None:
        if not self.store.session.has_uncommitted_changes:
            session = self.store.session.fork()
            pickle.loads(pickle.dumps(session))

        pickle.loads(pickle.dumps(self.repo))

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
