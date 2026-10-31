import inspect
import json
import math
import pickle
from collections.abc import Callable
from typing import Any, TypeVar

import hypothesis.extra.numpy as npst
import hypothesis.strategies as st
import numpy as np
import pytest
from hypothesis import assume, note
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
from icechunk import Repository, Storage, in_memory_storage
from icechunk.testing import strategies as icst
from icechunk.testing.invariants import (
    assert_list_dir_equal,
    assert_moves_sorted_by_final_path,
)
from icechunk.testing.models import GroupNode, ModelStore
from icechunk.testing.trees import absolute, valid_moves
from icechunk.testing.utils import update_paths_after_move
from zarr import Array
from zarr.codecs import ShardingCodec
from zarr.codecs.bytes import BytesCodec
from zarr.core.buffer import default_buffer_prototype
from zarr.testing.stateful import ZarrHierarchyStateMachine, split_prefix_name
from zarr.testing.strategies import arrays as zarr_arrays
from zarr.testing.strategies import node_names, orthogonal_indices

PROTOTYPE = default_buffer_prototype()

# zarr >= 3.3 draws the array on the model store and *recreates* it in the store
# under test, dropping the sharding codec the strategy may have drawn (see
# `add_array` below). Older zarr builds the same array in both stores, and its
# `arrays` strategy opens the group with mode="w", which would wipe the model
# store if we drove it ourselves — so only override against the newer API.
ZARR_RECREATES_ARRAYS = "open_mode" in inspect.signature(zarr_arrays).parameters


def _writes_sharded_oindex() -> bool:
    """Whether zarr can write an orthogonal selection to a sharded array.

    An integer array on any axis reaches the sharding codec in broadcast
    (``np.ix_``) form. That codec's partial-encode path reads the selection
    back as a pointwise one and raises. The probe writes four points inside one
    shard. A one-point selection still writes, because both shapes then hold
    one element.
    """
    arr = zarr.create_array({}, shape=(4, 4), chunks=(2, 2), shards=(2, 4), dtype="int64")
    try:
        arr.oindex[np.array([0, 1]), np.array([2, 3])] = np.zeros((2, 2), dtype="int64")
    except (ValueError, IndexError):
        return False
    return True


# Probed rather than pinned to a version, so the override below disappears on
# its own once zarr releases the fix.
ZARR_WRITES_SHARDED_OINDEX = _writes_sharded_oindex()

Frequency = TypeVar("Frequency", bound=Callable[..., Any])


def storage_chunk_sizes(arr: "Array[Any]") -> tuple[tuple[int, ...], ...]:
    """Per-dimension sizes of the storage-key chunk grid (shards when sharded).

    Store keys are one object per *write* chunk: with sharding, the whole
    shard is a single ``c/<i>`` object and read (inner) chunks are byte
    ranges inside it, never separate keys. Shifts move store keys, so
    offsets and the grid must be in write-chunk units;
    ``cdata_shape``/``read_chunk_sizes`` count inner chunks and give the
    wrong grid for sharded arrays. Older zarr lacks write_chunk_sizes but
    also lacks rectilinear grids, so cells there are regular and can be
    computed directly.
    """
    if hasattr(arr, "write_chunk_sizes"):
        return arr.write_chunk_sizes  # type: ignore[no-any-return]
    cell = arr.shards or arr.chunks
    # a zero-length dimension holds no chunks, and its cell size may be zero too
    return tuple(
        tuple(min(c, s - i * c) for i in range(math.ceil(s / c) if s else 0))
        for s, c in zip(arr.shape, cell, strict=True)
    )


def is_sharded(arr: "Array[Any]") -> bool:
    """True when the array stores shards.

    ``Array.shards`` raises on a rectilinear chunk grid. The newer zarr
    strategies draw those grids, so this reads the codec chain instead.
    These arrays are always zarr format 3. Format 2 cannot shard.
    """
    codecs = getattr(arr.metadata, "codecs", ())
    return any(isinstance(codec, ShardingCodec) for codec in codecs)


# pytestmark = [
#     pytest.mark.filterwarnings(
#         "ignore::zarr.core.dtype.common.UnstableSpecificationWarning"
#     ),
# ]


# TODO: more before/after commit invariants?
# TODO: add "/" to self.all_groups, deleting "/" seems to be problematic
class ModifiedZarrHierarchyStateMachine(ZarrHierarchyStateMachine):
    store: ic.IcechunkStore  # Override parent class type annotation
    model: ModelStore  # Override to add move() method
    storage: ic.Storage

    def __init__(self, storage: Storage) -> None:
        self.storage = storage
        self.actor: type[Repository] = Repository
        # keep a version of icechunk module
        # this is necessary for subclasses that use multiple versions of icechunk
        # to do things like construct config types correctly
        self.ic = ic

        # Create a temporary repository with spec_version=1 in a separate storage
        # This will be replaced in init_store with the Hypothesis-sampled version
        # we need this in order to properly initialize the superclass MemoryStore
        # model
        temp_repo = Repository.create(in_memory_storage(), spec_version=1)
        temp_store = temp_repo.writable_session("main").store
        super().__init__(temp_store)
        # Replace parent's MemoryStore with our ModelStore that has move()
        self.model = ModelStore()
        self.model.spec_version = 1
        zarr.group(store=self.model)

    @initialize(spec_version=st.sampled_from([1, 2]))
    def init_store(self, spec_version: int) -> None:
        """Override parent's init_store to sample spec_version and create repository."""
        # necessary to control the order of calling. if multiple intiliazes they will be
        # called by hypothesis in a random order
        note(f"Creating repository with spec_version={spec_version}, actor={self.actor}")

        # spec_version=1 rejects rectilinear chunk grids. The package conftest
        # globally enables `array.rectilinear_chunks`; turn it off for v1
        # examples so zarr's strategies don't draw them here.
        if "array.rectilinear_chunks" in zarr.config:
            zarr.config.set({"array.rectilinear_chunks": spec_version != 1})

        # Create repository with the drawn spec version
        if Version(self.ic.__version__).major >= 2:
            self.repo = self.actor.create(self.storage, spec_version=spec_version)
        else:
            self.repo = self.actor.create(self.storage)
        self.store = self.repo.writable_session("main").store
        self.model.spec_version = spec_version

        super().init_store()

    if ZARR_RECREATES_ARRAYS:

        @rule(data=st.data(), name=node_names)
        def add_array(self, data: st.DataObject, name: str) -> None:
            """Give both stores the same array, sharding codec included.

            zarr's rule draws the array on the model store, where the strategy
            may give it a sharding codec, and then recreates it in the store
            under test with ``create_array(chunks=<grid cell>,
            compressors=None)``, which drops that codec (and the drawn
            attributes). The two sides then differ where sharding matters:
            writing a chunk that is entirely fill value elides the object on the
            unsharded side only, so the store loses a key the model keeps.

            Same fix as zarr-developers/zarr-python#4288, which is on zarr's
            main but not in a release yet — and CI installs the latest release.
            Drop this override once that release lands.
            """
            if self.all_groups:
                parent = data.draw(
                    st.sampled_from(sorted(self.all_groups)), label="Array parent"
                )
            else:
                parent = ""
            path = f"{parent}/{name}".lstrip("/")
            assume(self.can_add(path))

            # mypy resolves `zarr_arrays` against the installed zarr, which may
            # be the older one without `open_mode`. This rule only exists for
            # the newer API, where that argument stops the strategy from opening
            # the model store with mode="w" and wiping it.
            arrays_strategy: Any = zarr_arrays
            a = data.draw(
                arrays_strategy(
                    stores=st.just(self.model),
                    paths=st.just(parent),
                    array_names=st.just(name),
                    zarr_formats=st.just(3),
                    compressors=st.just(BytesCodec()),
                    open_mode="a",
                ),
                label="generated array",
            )
            note(
                f"Adding array:  path='{path}'  shape={a.shape}  "
                f"chunks={a.metadata.chunk_grid}"
            )

            # `from_array` keeps the drawn chunk grid, codecs and dimension
            # names. `attributes` and `fill_value` are passed explicitly because
            # zarr up to 3.3.0 silently drops both (fixed by zarr#4288, so this
            # is a no-op on newer zarr); the data is copied here rather than by
            # `write_data=True`, whose shard-wise copy still does not support
            # rectilinear chunk grids.
            arr = zarr.from_array(
                self.store,
                data=a,
                name=path,
                attributes=a.attrs.asdict(),
                fill_value=a.fill_value,
                write_data=False,
            )
            arr[:] = a[:]
            self.all_arrays.add(path)

    if not ZARR_WRITES_SHARDED_OINDEX:

        @precondition(lambda self: bool(self.all_arrays))
        @rule(data=st.data())
        def overwrite_array_orthogonal_indexing(self, data: st.DataObject) -> None:
            """Copy of zarr's rule with the broken sharded writes skipped.

            An integer array on any axis reaches the sharding codec in
            broadcast (``np.ix_``) form. That codec's partial-encode path reads
            the selection back as a pointwise one. The write then raises
            ``ValueError: shape mismatch`` unless it covers a single point. It
            fails on the model store, before icechunk sees it. A selection of
            slices always works, and so does any unsharded target.
            """
            array = data.draw(st.sampled_from(sorted(self.all_arrays)))
            model_array = zarr.open_array(path=array, store=self.model)
            store_array = zarr.open_array(path=array, store=self.store)
            indexer, _ = data.draw(orthogonal_indices(shape=model_array.shape))
            assume(
                not is_sharded(model_array)
                or not any(isinstance(dim_sel, np.ndarray) for dim_sel in indexer)
            )
            note(f"overwriting array orthogonal {indexer=}")
            new_data = data.draw(
                npst.arrays(
                    shape=model_array.oindex[indexer].shape,  # type: ignore[union-attr]
                    dtype=model_array.dtype,
                )
            )
            model_array.oindex[indexer] = new_data
            store_array.oindex[indexer] = new_data

    @precondition(
        lambda self: (
            Version(self.ic.__version__).major >= 2
            and not self.store.session.has_uncommitted_changes
            and bool(self.all_arrays)
        )
    )
    @rule(data=st.data())
    def reopen_with_config(self, data: st.DataObject) -> None:
        array_paths = data.draw(
            st.lists(st.sampled_from(sorted(self.all_arrays)), max_size=3, unique=True)
        )
        arrays = tuple(zarr.open_array(self.model, path=path) for path in array_paths)
        config = data.draw(
            icst.repository_configs(
                inline_chunk_threshold_bytes=st.just(0),
                splitting=icst.splitting_configs(arrays=arrays),
            )
        )
        note(f"reopening with config {config!r}")
        self.repo = self.actor.open(self.storage, config=config)
        if data.draw(st.booleans()):
            self.repo.save_config()
        self.store = self.repo.writable_session("main").store

    @precondition(lambda self: not self.store.session.has_uncommitted_changes)
    @rule(data=st.data())
    def rewrite_manifests(self, data: st.DataObject) -> None:
        sconfig = self.ic.ManifestSplittingConfig.from_dict(
            {
                self.ic.ManifestSplitCondition.AnyArray(): {
                    self.ic.ManifestSplitDimCondition.Any(): data.draw(
                        st.integers(min_value=1, max_value=10)
                    )
                }
            }
        )

        config = self.ic.RepositoryConfig(
            inline_chunk_threshold_bytes=0,
            manifest=self.ic.ManifestConfig(splitting=sconfig),
        )
        note(f"rewriting manifests with config {sconfig=!r}")
        self.repo = self.actor.open(self.storage, config=config)
        self.repo.rewrite_manifests(
            f"rewriting manifests with {sconfig!s}", branch="main"
        )
        if data.draw(st.booleans()):
            self.repo.save_config()
        self.store = self.repo.writable_session("main").store

    @rule(data=st.data())
    def commit_with_check(self, data: st.DataObject) -> None:
        note("committing and checking list_prefix")

        lsbefore = sorted(self._sync_iter(self.store.list_prefix("")))
        path = data.draw(st.sampled_from(lsbefore))
        get_before = self._sync(self.store.get(path, prototype=PROTOTYPE))
        assert get_before

        allow_empty = not self.store.session.has_uncommitted_changes
        if Version(self.ic.__version__).major >= 2:
            self.store.session.commit("foo", allow_empty=allow_empty)
        else:
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

    @rule(dry_run=st.booleans(), delete_unused_v1_files=st.booleans())
    @precondition(lambda self: self.model.spec_version == 1)
    @precondition(lambda self: not self.store.session.has_uncommitted_changes)
    def upgrade_spec_version(self, dry_run: bool, delete_unused_v1_files: bool) -> None:
        """Upgrade repository from spec version 1 to version 2."""
        self.repo = self.ic.upgrade_icechunk_repository(
            self.repo, dry_run=dry_run, delete_unused_v1_files=delete_unused_v1_files
        )
        self.store = self.repo.writable_session("main").store
        if not dry_run:
            assert self.repo.spec_version == 2
            self.model.spec_version = 2
            if "array.rectilinear_chunks" in zarr.config:
                zarr.config.set({"array.rectilinear_chunks": True})

    @rule(data=st.data(), num_moves=st.integers(min_value=1, max_value=5))
    @precondition(lambda self: self.model.spec_version >= 2)
    @precondition(lambda self: not self.store.session.has_uncommitted_changes)
    @precondition(lambda self: bool(self.all_arrays) or bool(self.all_groups))
    def move_operations(self, data: st.DataObject, num_moves: int) -> None:
        """Perform moves in a single rearrange session, then commit or discard."""
        note(f"starting rearrange session with {num_moves} moves")
        session = self.repo.rearrange_session("main")

        # Copy model to track expected state - apply moves as we go
        pending_model = self._sync(self.model.copy())
        pending_arrays = self.all_arrays.copy()
        pending_groups = self.all_groups.copy()

        tree = GroupNode.from_paths(
            {absolute(p) for p in pending_arrays},
            {absolute(p) for p in pending_groups},
        )
        moves = data.draw(valid_moves(tree, n_moves=st.just(num_moves)))
        for source, dest in moves:
            note(f"moving {source!r} to {dest!r}")
            session.move(source, dest)
            self._sync(pending_model.move(source, dest))
            pending_arrays, pending_groups = update_paths_after_move(
                source.lstrip("/"), dest.lstrip("/"), pending_arrays, pending_groups
            )
            self._compare_list_dir(
                pending_model, session.store, pending_arrays | pending_groups
            )

        if data.draw(st.sampled_from([True, True, True, False])):
            note(f"committing {num_moves} moves")
            snap_before = session.snapshot_id
            self.model = pending_model
            self.all_arrays = pending_arrays
            self.all_groups = pending_groups
            self.store = session.store
            self.commit_with_check(data)
            snap_after = self.repo.lookup_branch("main")

            # Moves in the tx log must be sorted by final path
            diff = self.repo.diff(from_snapshot_id=snap_before, to_snapshot_id=snap_after)
            if diff.moved_nodes:
                assert_moves_sorted_by_final_path(diff.moved_nodes)
        else:
            note("discarding moves")
            self.store = self.repo.writable_session("main").store

    @rule(data=st.data())
    @precondition(
        lambda self: (
            Version(self.ic.__version__).major >= 2 and self.repo.spec_version >= 2
        )
    )
    @precondition(lambda self: bool(self.all_arrays))
    def shift_array(self, data: st.DataObject) -> None:
        """Shift an array's chunks by a random offset."""
        array_path = data.draw(st.sampled_from(sorted(self.all_arrays)))

        arr_model = zarr.open_array(self.model, path=array_path)
        arr_store = zarr.open_array(self.store, path=array_path)
        grid_sizes = storage_chunk_sizes(arr_model)
        num_chunks = tuple(len(sizes) for sizes in grid_sizes)

        # Draw offset: negative shifts left, positive shifts right
        offset = data.draw(
            st.tuples(*[st.integers(min_value=-n, max_value=n) for n in num_chunks])
        )

        # icechunk rejects chunk moves on non-regular grids (issue #2151): payloads
        # would land in slots expecting different sizes, corrupting the array.
        # zarr v2 metadata has no chunk_grid attribute; those arrays are always regular.
        # The class is RegularChunkGridMetadata or RegularChunkGrid depending on the
        # zarr version, so match on the prefix.
        chunk_grid = getattr(arr_store.metadata, "chunk_grid", None)
        if chunk_grid is not None and not type(chunk_grid).__name__.startswith("Regular"):
            note(f"shift on non-regular chunk grid of '{array_path}' must be rejected")
            with pytest.raises(ic.IcechunkError, match="chunk grid"):
                self.store.session.shift_array(f"/{array_path}", offset)
            return

        # Optionally resize before shift to make room (mimics real user behavior)
        # - With resize: preserves data that would otherwise go out of bounds
        # - Without resize: data shifting beyond bounds is lost
        should_resize = data.draw(st.booleans())
        if should_resize and any(o > 0 for o in offset):
            # Shifting right by offset[i] pushes the last offset[i] chunks
            # past the original extent; grow by their sizes so they fit.
            new_shape = tuple(
                arr_model.shape[i]
                + (sum(grid_sizes[i][-offset[i] :]) if offset[i] > 0 else 0)
                for i in range(len(grid_sizes))
            )
            note(f"resizing array '{array_path}' from {arr_model.shape} to {new_shape}")
            arr_model.resize(new_shape)
            arr_store.resize(new_shape)
            grid_sizes = storage_chunk_sizes(arr_model)
            num_chunks = tuple(len(sizes) for sizes in grid_sizes)

        note(f"shifting array '{array_path}' by {offset}")
        self.store.session.shift_array(f"/{array_path}", offset)
        self._sync(self.model.shift_array(array_path, offset, num_chunks))

    def _compare_list_dir(
        self, model: ModelStore, store: ic.IcechunkStore, paths: set[str]
    ) -> None:
        """Compare list_dir results between model and store for given paths."""
        for path in paths:
            model_ls = sorted(self._sync_iter(model.list_dir(path)))
            store_ls = sorted(self._sync_iter(store.list_dir(path)))
            assert_list_dir_equal(path, model_ls, store_ls)

    @invariant()
    def check_list_dir(self) -> None:
        self._compare_list_dir(self.model, self.store, self.all_groups | self.all_arrays)

    # Override upstream delete_group_using_del to fix precondition bug:
    # upstream checks `len(self.all_groups) >= 2` but filters to only groups
    # with "/" in their path, crashing when only root-level groups remain.
    # Fix: allow deleting any group (root "" is never in all_groups).
    # TODO: remove once https://github.com/zarr-developers/zarr-python/pull/3707 is released
    # and is our minimum required version
    @precondition(lambda self: self.store.supports_deletes)
    @precondition(lambda self: bool(self.all_groups - {"", "/"}))
    @rule(data=st.data())
    def delete_group_using_del(self, data: st.DataObject) -> None:
        group_path = data.draw(
            st.sampled_from(sorted(self.all_groups - {"", "/"})),
            label="Group deletion target",
        )
        prefix, group_name = split_prefix_name(group_path)
        note(
            f"Deleting group '{group_path=!r}', {prefix=!r}, {group_name=!r} using delete"
        )
        members = zarr.open_group(store=self.model, path=group_path).members(
            max_depth=None
        )
        for _, obj in members:
            if isinstance(obj, Array):
                self.all_arrays.remove(obj.path)
            else:
                self.all_groups.remove(obj.path)
        for store in [self.store, self.model]:
            group = zarr.open_group(store=store, path=prefix)
            group[group_name]  # check that it exists
            del group[group_name]
        self.all_groups.remove(group_path)

    @rule(data=st.data())
    def reopen_repository(self, data: st.DataObject) -> None:
        # We use the Zarr's memory store as the model,
        # Since we cannot `reset_branch` on the model; we must commit here.
        if self.store.session.has_uncommitted_changes:
            self.commit_with_check(data)

        self.repo = self.actor.open(self.storage)
        self.store = self.repo.writable_session("main").store

    @rule()
    def pickle_objects(self) -> None:
        if not self.store.session.has_uncommitted_changes:
            session = self.store.session.fork()
            pickle.loads(pickle.dumps(session))

        pickle.loads(pickle.dumps(self.repo))


@pytest.mark.hypothesis
def test_zarr_hierarchy() -> None:
    def mk_test_instance_sync() -> ModifiedZarrHierarchyStateMachine:
        return ModifiedZarrHierarchyStateMachine(in_memory_storage())

    run_state_machine_as_test(mk_test_instance_sync)  # type: ignore[no-untyped-call]


def test_zarr_store() -> None:
    pytest.skip("icechunk is more strict about keys")
    # repo = Repository.create(in_memory_storage())
    # store = repo.writable_session("main").store

    # def mk_test_instance_sync() -> ZarrHierarchyStateMachine:
    #     return ZarrStoreStateMachine(store)

    # run_state_machine_as_test(
    #     mk_test_instance_sync, settings=settings(report_multiple_bugs=False)
    # )


def test_storage_chunk_sizes_granularity() -> None:
    model = ModelStore()
    sharded = zarr.create_array(
        model, name="s", shape=(4,), shards=(4,), chunks=(2,), dtype="i1", fill_value=1
    )
    plain = zarr.create_array(
        model, name="p", shape=(100, 80), chunks=(30, 40), dtype="i1"
    )
    assert storage_chunk_sizes(sharded) == ((4,),)
    # cdata_shape counts inner chunks for sharded arrays; using it as the
    # storage-key grid is the bug storage_chunk_sizes exists to avoid.
    assert sharded.cdata_shape == (2,)
    assert storage_chunk_sizes(plain) == ((30, 30, 30, 10), (40, 40))
    empty = zarr.create_array(model, name="e", shape=(0,), chunks=(1,), dtype="i1")
    assert storage_chunk_sizes(empty) == ((),)
    if Version(zarr.__version__) < Version("3.3.0"):
        # zarr < 3.3 allows a zero cell size when the dimension is empty
        cell_zero = zarr.create_array(
            model, name="z", shape=(0,), chunks=(0,), dtype="i1"
        )
        assert storage_chunk_sizes(cell_zero) == ((),)


async def test_shift_sharded_model_vs_store() -> None:
    """Shifting a sharded array keeps ModelStore and IcechunkStore keys in sync.

    Mirrors the shift_array rule: the array is generated sharded on the model
    and recreated on the store with only the outer chunk grid, then both are
    shifted on the storage-key grid.
    """
    repo = Repository.create(in_memory_storage(), spec_version=2)
    session = repo.writable_session("main")
    store = session.store

    model = ModelStore()
    zarr.group(store=model)

    arr_model = zarr.create_array(
        model, name="0", shape=(4,), shards=(4,), chunks=(2,), dtype="i1", fill_value=1
    )
    arr_store = zarr.create_array(
        store, name="0", shape=(4,), chunks=(4,), dtype="i1", fill_value=1
    )
    data = np.zeros((4,), dtype="i1")
    arr_model[:] = data
    arr_store[:] = data

    model_keys = sorted([k async for k in model.list_prefix("")])
    store_keys = sorted([k async for k in store.list_prefix("")])
    assert model_keys == store_keys, (model_keys, store_keys)
    assert "0/c/0" in model_keys

    grid_sizes = storage_chunk_sizes(arr_model)
    num_chunks = tuple(len(sizes) for sizes in grid_sizes)
    assert num_chunks == (1,)

    session.shift_array("/0", (1,))
    await model.shift_array("0", (1,), num_chunks)

    model_keys = sorted([k async for k in model.list_prefix("")])
    store_keys = sorted([k async for k in store.list_prefix("")])
    assert model_keys == store_keys, (model_keys, store_keys)
