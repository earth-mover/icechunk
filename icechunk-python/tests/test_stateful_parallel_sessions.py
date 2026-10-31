import pickle
import tempfile
from collections.abc import Callable
from typing import Any

import hypothesis.extra.numpy as npst
import numpy as np
import numpy.testing as npt
from hypothesis import assume, note
from hypothesis import strategies as st
from hypothesis.stateful import RuleBasedStateMachine, invariant, precondition, rule

import icechunk as ic
import icechunk.testing.strategies as icst
import zarr
import zarr.testing.strategies as zrst
from zarr.abc.store import Store
from zarr.testing.stateful import SyncStoreWrapper


class SerialParallelStateMachine(RuleBasedStateMachine):
    """
    This stateful test asserts that two stores :
    1. one on which all actions are executed in serial
    2. one on which those same actions may be executed on the parent session,
       or on forks. Importantly, forks may be created with a 'dirty' state.

    are equivalent after merge & commit.

    To model this we use the same repo with two branches: `serial` & `parallel`.

    1. We add a step that merge forks back to the main session.
    2. Our invariant is that the serial and parallel branches are identical after merging.
    """

    def __init__(self) -> None:
        super().__init__()

        log_filter = "warn,icechunk::storage::object_store=error"
        ic.set_logs_filter(log_filter)

        self.storage = ic.local_filesystem_storage(tempfile.mkdtemp())
        self.repo = ic.Repository.create(self.storage)
        self.repo.create_branch("parallel", self.repo.lookup_branch("main"))

        # TODO: should this just be Zarr memory store instead?
        #       are version control ops on the serial store useful?
        self.serial = self.repo.writable_session("main")
        self.parallel = self.repo.writable_session("parallel")

        self.fork1: ic.Session | ic.ForkSession | None = None
        self.fork2: ic.Session | ic.ForkSession | None = None

        self.all_arrays: set[str] = set()

        # Maps (array_name, path) -> session name that "owns" it during a fork period
        # this could be ("array", "path/to/chunk") for chunk ops or ("array", "zarr.json") for array ops
        self.chunk_owners: dict[tuple[str, str], str] = {}

    def has_forks(self) -> bool:
        return self.fork1 is not None and self.fork2 is not None

    @precondition(lambda self: not self.has_forks() and len(self.all_arrays) < 10)
    @rule(
        data=st.data(),
        name=zrst.node_names,
        array_and_chunks=zrst.np_array_and_chunks(
            arrays=npst.arrays(dtype=np.int8, shape=npst.array_shapes())
        ),
    )
    def add_array(
        self,
        data: st.DataObject,
        name: str,
        array_and_chunks: tuple[np.ndarray[Any, Any], tuple[int, ...]],
    ) -> None:
        assume(name not in self.all_arrays)
        array, chunks = array_and_chunks
        fill_value = data.draw(npst.from_dtype(array.dtype))
        note(f"Adding array:  path='{name}'  shape={array.shape}  chunks={chunks}")
        with zarr.config.set({"array.write_empty_chunks": True}):
            for store in [self.serial.store, self.parallel.store]:
                zarr.array(
                    array,
                    chunks=chunks,
                    path=name,
                    store=store,
                    fill_value=fill_value,
                    zarr_format=3,
                    dimension_names=None,
                )
        self.all_arrays.add(name)

    @precondition(lambda self: bool(self.all_arrays))
    @rule(data=st.data())
    def write_chunk(self, data: st.DataObject) -> None:
        array = data.draw(st.sampled_from(sorted(self.all_arrays)))
        arr = zarr.open_array(path=array, store=self.serial.store)

        # TODO: this will overwrite a single chunk. Should we generate multiple slicers
        #       instead or let hypothesis do it for us?
        slicers = data.draw(icst.chunk_slicers(arr.cdata_shape, arr.chunks))
        new_data = data.draw(npst.arrays(shape=arr[slicers].shape, dtype=arr.dtype))  # type: ignore[union-attr]

        chunk_path = "/".join(
            str(s.start // c) for s, c in zip(slicers, arr.chunks, strict=True)
        )
        note(f"overwriting chunk: {slicers=!r} {chunk_path=!r}")
        arr[slicers] = new_data

        def write(store: Store) -> None:
            arr = zarr.open_array(path=array, store=store)
            arr[slicers] = new_data

        self.execute_on_parallel(data=data, func=write, chunks={(array, chunk_path)})

    @precondition(lambda self: bool(self.all_arrays))
    @rule(data=st.data())
    def delete_chunk(self, data: st.DataObject) -> None:
        array = data.draw(st.sampled_from(sorted(self.all_arrays)))
        arr = zarr.open_array(path=array, store=self.serial.store)
        chunk_path = data.draw(icst.chunk_paths(numblocks=arr.cdata_shape))
        path = f"{array}/c/{chunk_path}"
        note(f"deleting chunk {path=!r}")
        SyncStoreWrapper(self.serial.store).delete(path)

        self.execute_on_parallel(
            data=data,
            func=lambda store: SyncStoreWrapper(store).delete(path),
            chunks={(array, chunk_path)},
        )

    def execute_on_parallel(
        self,
        *,
        data: st.DataObject,
        func: Callable[..., None],
        chunks: set[tuple[str, str]],
    ) -> None:
        """
        Chooses one of self.parallel, self.fork1, or self.fork2
        as the session on which to make changes using `func`.

        When forks exist, enforces that each (array, chunk_coord) is only
        modified by a single session to avoid overlapping writes that would
        cause merge conflicts.
        """
        for array_and_path in chunks:
            if self.has_forks():
                candidates = {
                    "fork1": self.fork1,
                    "parallel": self.parallel,
                    "fork2": self.fork2,
                }
                if name := self.chunk_owners.get(array_and_path):
                    session = candidates[name]
                else:
                    name, session = data.draw(st.sampled_from(tuple(candidates.items())))
                    self.chunk_owners[array_and_path] = name

            else:
                name, session = "parallel", self.parallel

            note(f"executing on {name}")
            assert session is not None
            func(session.store)

    @precondition(lambda self: not self.has_forks())
    @rule()
    def fork_pickle(self) -> None:
        note("forking with pickle")
        fork = self.parallel.fork()
        self.fork1 = pickle.loads(pickle.dumps(fork))
        self.fork2 = pickle.loads(pickle.dumps(fork))

    @precondition(lambda self: not self.has_forks())
    @rule()
    def fork_threads(self) -> None:
        # Models multithreading where the same session is broadcast to multiple workers
        note("forking with reference (threads)")
        self.fork1 = self.parallel
        self.fork2 = self.parallel

    @precondition(lambda self: self.has_forks())
    @rule(two_to_one=st.booleans())
    def merge(self, two_to_one: bool) -> None:
        assert self.fork1 is not None
        assert self.fork2 is not None
        if two_to_one:
            note("merging forks to base session, merging 2→1→parallel")
            self.fork1.merge(self.fork2)  # type: ignore[arg-type]
            self.parallel.merge(self.fork1)  # type: ignore[arg-type]
        else:
            note("merging forks to base session, merging 1→2→parallel")
            self.fork2.merge(self.fork1)  # type: ignore[arg-type]
            self.parallel.merge(self.fork2)  # type: ignore[arg-type]

        self.fork1 = None
        self.fork2 = None
        self.chunk_owners.clear()

    @precondition(
        lambda self: not self.has_forks() and self.serial.has_uncommitted_changes
    )
    @rule()
    def commit(self) -> None:
        note("committing both sessions")
        self.serial.commit("foo")
        self.parallel.commit("foo")

        self.serial = self.repo.writable_session("main")
        self.parallel = self.repo.writable_session("parallel")

    @invariant()
    def verify_all_arrays(self) -> None:
        if self.has_forks():
            return
        note("verifying all arrays")
        for path in self.all_arrays:
            s = zarr.open_array(path=path, store=self.serial.store)
            p = zarr.open_array(path=path, store=self.parallel.store)
            npt.assert_array_equal(s, p)

    def teardown(self) -> None:
        ic.set_logs_filter(None)


VersionControlTest = SerialParallelStateMachine.TestCase
