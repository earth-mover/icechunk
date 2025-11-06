from collections.abc import Iterable

import numpy as np

import icechunk as ic
import zarr


async def test_shift_using_function() -> None:
    repo = ic.Repository.create(
        storage=ic.in_memory_storage(),
    )
    session = repo.writable_session("main")
    root = zarr.group(store=session.store, overwrite=True)
    array = root.create_array(
        "array", shape=(50,), chunks=(2,), dtype="i4", fill_value=42
    )
    array[:] = np.arange(50)
    session.commit("create array")

    session = repo.writable_session("main")
    root = zarr.group(store=session.store, overwrite=False)
    array = root["array"]
    assert array[0] == 0
    assert array[49] == 49

    def reindex(idx: Iterable[int]) -> Iterable[int] | None:
        idx = list(idx)
        return [idx[0] - 4] if idx[0] >= 4 else None

    session.reindex_array("/array", reindex)
    # we moved 4 chunks to the left, that's 8 array elements
    np.testing.assert_equal(array[0:42], np.arange(8, 50))
    np.testing.assert_equal(array[42:], np.arange(42, 50))


async def test_shift_using_shift_by_offset() -> None:
    repo = ic.Repository.create(
        storage=ic.in_memory_storage(),
    )
    session = repo.writable_session("main")
    root = zarr.group(store=session.store, overwrite=True)
    array = root.create_array(
        "array", shape=(50,), chunks=(2,), dtype="i4", fill_value=42
    )
    array[:] = np.arange(50)
    session.commit("create array")

    session = repo.writable_session("main")
    root = zarr.group(store=session.store, overwrite=False)
    session.shift_array("/array", (-4,))
    array = root["array"]
    # we moved 4 chunks to the left, that's 8 array elements
    np.testing.assert_equal(array[0:42], np.arange(8, 50))
    np.testing.assert_equal(array[42:], np.arange(42, 50))


async def test_resize_and_shift_right() -> None:
    repo = ic.Repository.create(
        storage=ic.in_memory_storage(),
    )
    session = repo.writable_session("main")
    root = zarr.group(store=session.store, overwrite=True)
    array = root.create_array(
        "array", shape=(50,), chunks=(2,), dtype="i4", fill_value=42
    )
    array[:] = np.arange(50)
    session.commit("create array")

    session = repo.writable_session("main")
    root = zarr.group(store=session.store, overwrite=False)
    array = root["array"]
    array.resize((100,))
    assert array.shape == (100,)
    session.shift_array("/array", (4,))
    np.testing.assert_equal(array[8:58], np.arange(50))
    np.testing.assert_equal(array[0:8], np.arange(8))
    assert (array[58:] == 42).all()
    session.commit("shifted")

    # test still valid after commit
    session = repo.readonly_session(branch="main")
    root = zarr.open_group(store=session.store, mode="r")
    array = root["array"]
    assert array.shape == (100,)
    np.testing.assert_equal(array[8:58], np.arange(50))
    np.testing.assert_equal(array[0:8], np.arange(8))
    assert (array[58:] == 42).all()
