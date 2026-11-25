import subprocess
import sys
from collections.abc import Iterable
from typing import Any, cast

import numpy as np
import pytest

import icechunk as ic
import zarr


def test_reindex_forward_only() -> None:
    """Test reindex_array with forward only — stale data remains in vacated positions."""
    repo = ic.Repository.create(
        storage=ic.in_memory_storage(),
    )
    session = repo.writable_session("main")
    root = zarr.group(store=session.store, overwrite=True)
    array = root.create_array(
        "array", chunks=(2,), fill_value=42, data=np.arange(50, dtype="i4")
    )
    session.commit("create array")

    session = repo.writable_session("main")
    root = zarr.group(store=session.store, overwrite=False)
    array = cast("zarr.Array[Any]", root["array"])
    assert array[0] == 0
    assert array[49] == 49

    def reindex(idx: Iterable[int]) -> Iterable[int] | None:
        idx = list(idx)
        return [idx[0] - 4] if idx[0] >= 4 else None

    session.reindex_array("/array", forward=reindex)
    # we moved 4 chunks to the left, that's 8 array elements
    np.testing.assert_equal(array[0:42], np.arange(8, 50))
    # stale data remains in vacated positions (forward only)
    np.testing.assert_equal(array[42:], np.arange(42, 50))


def test_reindex_forward_backward() -> None:
    """Test reindex_array with both forward and backward clears stale data."""
    repo = ic.Repository.create(storage=ic.in_memory_storage())
    session = repo.writable_session("main")
    root = zarr.group(store=session.store, overwrite=True)
    array = root.create_array(
        "array", chunks=(2,), fill_value=-1, data=np.arange(50, dtype="i4")
    )
    session.commit("create array")

    session = repo.writable_session("main")
    root = zarr.group(store=session.store, overwrite=False)
    array = cast("zarr.Array[Any]", root["array"])

    n_chunks = 25

    def forward(idx: Iterable[int]) -> Iterable[int] | None:
        idx = list(idx)
        new = idx[0] - 4
        return [new] if 0 <= new < n_chunks else None

    def backward(idx: Iterable[int]) -> Iterable[int] | None:
        idx = list(idx)
        new = idx[0] + 4
        return [new] if 0 <= new < n_chunks else None

    session.reindex_array("/array", forward=forward, backward=backward)
    # shifted data
    np.testing.assert_equal(array[0:42], np.arange(8, 50))
    # vacated positions cleared to fill value
    np.testing.assert_equal(array[42:], np.full(8, -1))


def test_reindex_with_missing_chunks() -> None:
    """Test reindex_array with backward handles empty chunks correctly."""
    repo = ic.Repository.create(storage=ic.in_memory_storage())
    session = repo.writable_session("main")
    arr = zarr.open_array(
        session.store,
        path="data",
        mode="w",
        shape=(3,),
        chunks=(1,),
        dtype="f4",
        fill_value=-1,
    )
    arr[0] = 1.0
    arr[2] = 3.0
    # chunks: [1, (empty), 3]
    session.commit("initial")

    n_chunks = 3
    offset = 1

    def fwd(idx: Iterable[int]) -> Iterable[int] | None:
        idx = list(idx)
        new = idx[0] + offset
        return [new] if 0 <= new < n_chunks else None

    def bwd(idx: Iterable[int]) -> Iterable[int] | None:
        idx = list(idx)
        new = idx[0] - offset
        return [new] if 0 <= new < n_chunks else None

    # Forward only: index 0 retains stale data
    session = repo.writable_session("main")
    arr = zarr.open_array(session.store, path="data")
    session.reindex_array("/data", forward=fwd)
    np.testing.assert_equal(arr[:], [1, 1, 3])

    # Forward + backward: stale positions are cleared
    session = repo.writable_session("main")
    arr = zarr.open_array(session.store, path="data")
    session.reindex_array("/data", forward=fwd, backward=bwd)
    np.testing.assert_equal(arr[:], [-1, 1, -1])


@pytest.mark.parametrize("fail_backward", [False, True], ids=["forward", "backward"])
def test_reindex_callback_error(fail_backward: bool) -> None:
    """Callback failures leave the array and its writable session intact."""
    repo = ic.Repository.create(storage=ic.in_memory_storage())
    session = repo.writable_session("main")
    data = np.arange(1, 4, dtype="i4")
    array = zarr.group(session.store).create_array("array", chunks=(1,), data=data)

    def fail(idx: Iterable[int]) -> Iterable[int] | None:
        raise ValueError("mapping failed")

    with pytest.raises(ic.IcechunkError, match="mapping failed"):
        if fail_backward:
            # No destination was produced, so the backward callback must run.
            session.reindex_array("/array", lambda idx: None, fail)
        else:
            session.reindex_array("/array", fail)

    np.testing.assert_array_equal(array[:], data)
    session.commit("after failed reindex")


SHARED_SESSION_SCRIPT = """
import faulthandler
import os
import sys
import threading

import numpy as np

import icechunk as ic
import zarr

faulthandler.dump_traceback_later(30, exit=True)


def exit_on_thread_error(args):
    threading.__excepthook__(args)
    os._exit(1)


threading.excepthook = exit_on_thread_error
repo = ic.Repository.create(storage=ic.in_memory_storage())
session = repo.writable_session("main")
store = session.store
zarr.group(store).create_array("a", chunks=(1,), data=np.arange(256, dtype="i4"))
# zarr keeps one store and calls str(store) on it.
call = {
    "str-store": lambda: str(store),
    "str-session": lambda: str(session),
    "chunk-type": lambda: session.chunk_type("/a", [0]),
}[sys.argv[1]]
done = threading.Event()


def caller():
    while not done.is_set():
        call()


thread = threading.Thread(target=caller)
thread.start()
try:
    for _ in range(5):
        session.reindex_array("/a", lambda idx: None)
finally:
    done.set()
    thread.join()
"""


@pytest.mark.parametrize("call", ["str-store", "str-session", "chunk-type"])
def test_reindex_shared_session_no_deadlock(call: str) -> None:
    """Another thread can wait for the session lock while a reindex runs."""
    # A child interpreter, because a deadlocked thread can hold the GIL.
    result = subprocess.run(
        [sys.executable, "-c", SHARED_SESSION_SCRIPT, call],
        capture_output=True,
        check=False,
        text=True,
        timeout=120,
    )
    assert result.returncode == 0, result.stderr
