import datetime
import pickle
from collections.abc import Callable
from typing import Any

import hypothesis.extra.numpy as npst
import numpy as np
import numpy.testing as npt
from hypothesis import assume, note, settings
from hypothesis import strategies as st
from hypothesis.stateful import RuleBasedStateMachine, precondition, rule

import icechunk as ic
import icechunk.testing.strategies as icst
import zarr
import zarr.testing.strategies as zrst
from icechunk.distributed import merge_sessions
from zarr.abc.store import Store
from zarr.testing.stateful import SyncStoreWrapper

zarr.config.set({"array.write_empty_chunks": True})


def simple_dtypes() -> st.SearchStrategy[np.dtype[Any]]:
    return npst.integer_dtypes(endianness="=") | npst.floating_dtypes(endianness="=")


class SerialParallelStateMachine(RuleBasedStateMachine):
    """
    This stateful test asserts that two stores :
    1. one on which all actions are executed in serial
    2. one on which those same actions may be executed on the parent session,
       or on forks. Importantly, forks may be created with a 'dirty' state.

    To model this we use the same repo with two branches.
    """

    def __init__(self) -> None:
        super().__init__()

        self.storage = ic.local_filesystem_storage(
            f"tmp/icechunk_parallel_stateful/{str(datetime.datetime.now()).split(' ')[-1]}"
        )
        self.repo = ic.Repository.create(self.storage)
        self.repo.create_branch("parallel", self.repo.lookup_branch("main"))

        # TODO: should this just be Zarr memory store instead?
        #       are version control ops on the serial store useful?
        self.serial = self.repo.writable_session("main")
        self.parallel = self.repo.writable_session("parallel")

        self.fork1: ic.Session | None = None
        self.fork2: ic.Session | None = None

        self.has_changes = False
        self.all_arrays: set[str] = set()

    def has_forks(self) -> bool:
        return self.fork1 is not None and self.fork2 is not None

    @precondition(lambda self: not self.has_forks())
    @rule(
        data=st.data(),
        name=zrst.node_names,
        array_and_chunks=zrst.np_array_and_chunks(
            arrays=npst.arrays(simple_dtypes(), npst.array_shapes())
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
        array, chunks = array_and_chunks
        fill_value = data.draw(npst.from_dtype(array.dtype))
        assume(name not in self.all_arrays)
        note(f"Adding array:  path='{name}'  shape={array.shape}  chunks={chunks}")
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

        note(f"overwriting chunk: {slicers=!r}")
        arr[slicers] = new_data

        def write(store: Store) -> None:
            arr = zarr.open_array(path=array, store=store)
            arr[slicers] = new_data

        self.execute_on_parallel(data=data, func=write)
        self.has_changes = True

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
            data=data, func=lambda store: SyncStoreWrapper(store).delete(path)
        )
        self.has_changes = True

    def execute_on_parallel(
        self, *, data: st.DataObject, func: Callable[..., None]
    ) -> None:
        """
        Chooses one of self.parallel, self.fork1, or self.fork2
        as the session on which to make changes using `func`.
        """
        if self.has_forks():
            # prioritize drawing a fork first
            name, session = data.draw(
                st.sampled_from(
                    [
                        ("fork1", self.fork1),
                        ("parallel", self.parallel),
                        ("fork2", self.fork2),
                    ]
                )
            )
        else:
            name, session = "parallel", self.parallel
        note(f"executing on {name}")
        assert session is not None
        func(session.store)

    @precondition(lambda self: not self.has_forks())
    @rule()
    def fork_pickle(self) -> None:
        note("forking with pickle")
        with self.parallel.allow_pickling():
            self.fork1 = pickle.loads(pickle.dumps(self.parallel))
            self.fork2 = pickle.loads(pickle.dumps(self.parallel))

    @precondition(lambda self: not self.has_forks())
    @rule()
    def fork_threads(self) -> None:
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
            merge_sessions(self.fork1, self.fork2)
            merge_sessions(self.parallel, self.fork1)
        else:
            note("merging forks to base session, merging 1→2→parallel")
            merge_sessions(self.fork2, self.fork1)
            merge_sessions(self.parallel, self.fork2)

        self.fork1 = None
        self.fork2 = None

    @precondition(lambda self: not self.has_forks() and self.has_changes)
    def commit(self) -> None:
        note("committing both sessions")
        self.serial.commit("foo")
        self.parallel.commit("foo")

        self.serial = self.repo.writable_session("main")
        self.parallel = self.repo.writable_session("parallel")

    # @precondition(lambda self: self.has_forks())
    # @rule(commit_fork1_first=st.booleans())
    # def commit_on_forks(self, commit_fork1_first: bool):
    #     """This should rebase automatically."""
    #     note("committing forks separately")
    #     if commit_fork1_first:
    #         if self.fork1.has_uncommitted_changes:
    #             self.fork1.commit("committing fork 1")
    #         if self.fork2.has_uncommitted_changes:
    #             self.fork2.commit("committing fork 2")
    #     else:
    #         if self.fork2.has_uncommitted_changes:
    #             self.fork2.commit("committing fork 2")
    #         if self.fork1.has_uncommitted_changes:
    #             self.fork1.commit("committing fork 1")

    #     if self.parallel.has_uncommitted_changes:
    #         self.parallel.commit("committing parallel")
    #     self.parallel = self.repo.writable_session("parallel")
    #     self.fork1 = None
    #     self.fork2 = None

    @precondition(lambda self: not self.has_forks())
    @rule()
    def verify_all_arrays(self) -> None:
        """
        This cannot be an invariant because we may have state on the forks.
        """
        note("verifying all arrays")
        for path in self.all_arrays:
            s = zarr.open_array(path=path, store=self.serial.store)
            p = zarr.open_array(path=path, store=self.parallel.store)
            npt.assert_array_equal(s, p)


SerialParallelStateMachine.TestCase.settings = settings(
    deadline=None, report_multiple_bugs=False
)
VersionControlTest = SerialParallelStateMachine.TestCase
