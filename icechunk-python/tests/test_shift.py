import asyncio
from collections.abc import Iterable
from typing import Any, cast

import numpy as np

import icechunk as ic
import zarr


async def test_shift_using_function() -> None:
    repo = await ic.Repository.create_async(
        storage=ic.in_memory_storage(),
    )
    session = await repo.writable_session_async("main")
    root = zarr.group(store=session.store, overwrite=True)
    array = root.create_array(
        "array", shape=(50,), chunks=(2,), dtype="i4", fill_value=42
    )
    array[:] = np.arange(50)
    await session.commit_async("create array")

    session = await repo.writable_session_async("main")
    root = zarr.group(store=session.store, overwrite=False)
    array = cast("zarr.Array[Any]", root["array"])
    assert array[0] == 0
    assert array[49] == 49

    def reindex(idx: Iterable[int]) -> Iterable[int] | None:
        idx = list(idx)
        return [idx[0] - 4] if idx[0] >= 4 else None

    await asyncio.to_thread(lambda: session.reindex_array("/array", reindex))
    # we moved 4 chunks to the left, that's 8 array elements
    np.testing.assert_equal(array[0:42], np.arange(8, 50))
    np.testing.assert_equal(array[42:], np.arange(42, 50))


async def test_shift_using_shift_by_offset() -> None:
    repo = await ic.Repository.create_async(
        storage=ic.in_memory_storage(),
    )
    session = await repo.writable_session_async("main")
    root = zarr.group(store=session.store, overwrite=True)
    array = root.create_array(
        "array", shape=(50,), chunks=(2,), dtype="i4", fill_value=42
    )
    array[:] = np.arange(50)
    await session.commit_async("create array")

    session = await repo.writable_session_async("main")
    root = zarr.group(store=session.store, overwrite=False)
    await asyncio.to_thread(lambda: session.shift_array("/array", (-4,)))
    array = cast("zarr.Array[Any]", root["array"])
    # we moved 4 chunks to the left, that's 8 array elements
    np.testing.assert_equal(array[0:42], np.arange(8, 50))
    np.testing.assert_equal(array[42:], np.arange(42, 50))


async def test_resize_and_shift_right() -> None:
    repo = await ic.Repository.create_async(
        storage=ic.in_memory_storage(),
    )
    session = await repo.writable_session_async("main")
    root = zarr.group(store=session.store, overwrite=True)
    array = root.create_array(
        "array", shape=(50,), chunks=(2,), dtype="i4", fill_value=42
    )
    array[:] = np.arange(50)
    await session.commit_async("create array")

    session = await repo.writable_session_async("main")
    root = zarr.group(store=session.store, overwrite=False)
    array = cast("zarr.Array[Any]", root["array"])
    array.resize((100,))
    assert array.shape == (100,)
    await asyncio.to_thread(lambda: session.shift_array("/array", (4,)))
    np.testing.assert_equal(array[8:58], np.arange(50))
    np.testing.assert_equal(array[0:8], np.arange(8))
    assert np.all(array[58:] == 42)
    await session.commit_async("shifted")

    # test still valid after commit
    session = await repo.readonly_session_async(branch="main")
    root = zarr.open_group(store=session.store, mode="r")
    array = cast("zarr.Array[Any]", root["array"])
    assert array.shape == (100,)
    np.testing.assert_equal(array[8:58], np.arange(50))
    np.testing.assert_equal(array[0:8], np.arange(8))
    assert np.all(array[58:] == 42)
