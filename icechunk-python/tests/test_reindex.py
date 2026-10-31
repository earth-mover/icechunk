from collections.abc import Iterable
from typing import Any, cast

import numpy as np

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
