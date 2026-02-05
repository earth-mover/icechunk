from collections.abc import Iterable
from typing import Any, cast

import numpy as np

import icechunk as ic
import zarr


async def test_shift_using_function() -> None:
    """Test reindex_array with a custom shift function (low-level API)."""
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
    array = cast("zarr.Array[Any]", root["array"])
    assert array[0] == 0
    assert array[49] == 49

    def reindex(idx: Iterable[int]) -> Iterable[int] | None:
        idx = list(idx)
        return [idx[0] - 4] if idx[0] >= 4 else None

    session.reindex_array("/array", reindex, delete_vacated=False)
    # we moved 4 chunks to the left, that's 8 array elements
    np.testing.assert_equal(array[0:42], np.arange(8, 50))
    # stale data remains when delete_vacated=False
    np.testing.assert_equal(array[42:], np.arange(42, 50))


async def test_reindex_with_delete_vacated() -> None:
    """Test reindex_array with delete_vacated=True clears source positions."""
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
    array = cast("zarr.Array[Any]", root["array"])

    def reindex(idx: Iterable[int]) -> Iterable[int] | None:
        idx = list(idx)
        return [idx[0] - 4] if idx[0] >= 4 else None

    session.reindex_array("/array", reindex, delete_vacated=True)
    # we moved 4 chunks to the left, that's 8 array elements
    np.testing.assert_equal(array[0:42], np.arange(8, 50))
    # vacated positions return fill_value when delete_vacated=True
    assert np.all(array[42:] == 42)


async def test_shift_discard_left() -> None:
    """Test DISCARD mode shifting left - vacated positions return fill_value."""
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
    element_shift = session.shift_array("/array", (-4,), ic.ShiftMode.DISCARD)
    array = cast("zarr.Array[Any]", root["array"])

    # Verify return value: chunk_offset * chunk_size = -4 * 2 = -8
    assert element_shift == (-8,)

    # First 42 elements should be shifted (8 array elements shifted left)
    np.testing.assert_equal(array[0:42], np.arange(8, 50))
    # Last 8 elements should be fill_value (vacated positions)
    assert np.all(array[42:] == 42)


async def test_shift_discard_right() -> None:
    """Test DISCARD mode shifting right - vacated positions return fill_value."""
    repo = ic.Repository.create(
        storage=ic.in_memory_storage(),
    )
    session = repo.writable_session("main")
    root = zarr.group(store=session.store, overwrite=True)
    array = root.create_array(
        "array", shape=(50,), chunks=(2,), dtype="i4", fill_value=-1
    )
    array[:] = np.arange(50)
    session.commit("create array")

    session = repo.writable_session("main")
    root = zarr.group(store=session.store, overwrite=False)
    session.shift_array("/array", (4,), ic.ShiftMode.DISCARD)
    array = cast("zarr.Array[Any]", root["array"])

    # First 8 elements should be fill_value (vacated positions)
    assert np.all(array[0:8] == -1)
    # Rest should be shifted data (last 8 elements of original are lost)
    np.testing.assert_equal(array[8:50], np.arange(0, 42))


async def test_shift_wrap_left() -> None:
    """Test WRAP mode shifting left - circular buffer behavior."""
    repo = ic.Repository.create(
        storage=ic.in_memory_storage(),
    )
    session = repo.writable_session("main")
    root = zarr.group(store=session.store, overwrite=True)
    # Use 10 elements with chunk size 2 = 5 chunks for easy testing
    array = root.create_array(
        "array", shape=(10,), chunks=(2,), dtype="i4", fill_value=42
    )
    array[:] = np.arange(10)
    session.commit("create array")

    session = repo.writable_session("main")
    root = zarr.group(store=session.store, overwrite=False)
    # Shift left by 2 chunks (4 array elements)
    session.shift_array("/array", (-2,), ic.ShiftMode.WRAP)
    array = cast("zarr.Array[Any]", root["array"])

    # Data should wrap around: [4,5,6,7,8,9,0,1,2,3]
    expected = np.array([4, 5, 6, 7, 8, 9, 0, 1, 2, 3])
    np.testing.assert_equal(array[:], expected)


async def test_shift_wrap_right() -> None:
    """Test WRAP mode shifting right - circular buffer behavior."""
    repo = ic.Repository.create(
        storage=ic.in_memory_storage(),
    )
    session = repo.writable_session("main")
    root = zarr.group(store=session.store, overwrite=True)
    # Use 10 elements with chunk size 2 = 5 chunks for easy testing
    array = root.create_array(
        "array", shape=(10,), chunks=(2,), dtype="i4", fill_value=42
    )
    array[:] = np.arange(10)
    session.commit("create array")

    session = repo.writable_session("main")
    root = zarr.group(store=session.store, overwrite=False)
    # Shift right by 2 chunks (4 array elements)
    session.shift_array("/array", (2,), ic.ShiftMode.WRAP)
    array = cast("zarr.Array[Any]", root["array"])

    # Data should wrap around: [6,7,8,9,0,1,2,3,4,5]
    expected = np.array([6, 7, 8, 9, 0, 1, 2, 3, 4, 5])
    np.testing.assert_equal(array[:], expected)


async def test_resize_then_discard_shift_right() -> None:
    """Test manual resize + DISCARD mode for shifting right with no data loss."""
    repo = ic.Repository.create(
        storage=ic.in_memory_storage(),
    )
    session = repo.writable_session("main")
    root = zarr.group(store=session.store, overwrite=True)
    # 50 elements, chunk size 2 = 25 chunks
    array = root.create_array(
        "array", shape=(50,), chunks=(2,), dtype="i4", fill_value=42
    )
    array[:] = np.arange(50)
    session.commit("create array")

    session = repo.writable_session("main")
    root = zarr.group(store=session.store, overwrite=False)
    array = cast("zarr.Array[Any]", root["array"])

    # First resize the array to make room (this is zarr-level operation)
    array.resize((58,))  # 50 + 4*2 = 58 (4 chunks * 2 elements/chunk)
    assert array.shape == (58,)

    # Then shift right by 4 chunks using DISCARD mode
    session.shift_array("/array", (4,), ic.ShiftMode.DISCARD)

    # First 8 elements should be fill_value (vacated positions)
    assert np.all(array[0:8] == 42)
    # Original data shifted to positions 8-57
    np.testing.assert_equal(array[8:58], np.arange(50))

    session.commit("shifted")

    # Verify persists after commit
    session = repo.readonly_session(branch="main")
    array = zarr.open_array(store=session.store, path="array", mode="r")
    assert array.shape == (58,)
    assert np.all(array[0:8] == 42)
    np.testing.assert_equal(array[8:58], np.arange(50))


async def test_shift_2d_discard() -> None:
    """Test DISCARD mode on a 2D array."""
    repo = ic.Repository.create(
        storage=ic.in_memory_storage(),
    )
    session = repo.writable_session("main")
    root = zarr.group(store=session.store, overwrite=True)
    # 4x4 array with 2x2 chunks = 2x2 chunks grid
    array = root.create_array(
        "array", shape=(4, 4), chunks=(2, 2), dtype="i4", fill_value=-1
    )
    array[:] = np.arange(16).reshape(4, 4)
    session.commit("create array")

    session = repo.writable_session("main")
    root = zarr.group(store=session.store, overwrite=False)
    # Shift by (1, 0) - shift down by one chunk, keep horizontal
    session.shift_array("/array", (1, 0), ic.ShiftMode.DISCARD)
    array = cast("zarr.Array[Any]", root["array"])

    # First 2 rows should be fill_value
    assert np.all(array[0:2, :] == -1)
    # Rows 2-3 should have original rows 0-1
    np.testing.assert_equal(array[2:4, :], np.arange(8).reshape(2, 4))


async def test_shift_2d_wrap() -> None:
    """Test WRAP mode on a 2D array."""
    repo = ic.Repository.create(
        storage=ic.in_memory_storage(),
    )
    session = repo.writable_session("main")
    root = zarr.group(store=session.store, overwrite=True)
    # 4x4 array with 2x2 chunks = 2x2 chunks grid
    array = root.create_array(
        "array", shape=(4, 4), chunks=(2, 2), dtype="i4", fill_value=-1
    )
    array[:] = np.arange(16).reshape(4, 4)
    session.commit("create array")

    session = repo.writable_session("main")
    root = zarr.group(store=session.store, overwrite=False)
    # Shift by (1, 1) - shift down and right by one chunk each
    session.shift_array("/array", (1, 1), ic.ShiftMode.WRAP)
    array = cast("zarr.Array[Any]", root["array"])

    # Original data wraps around in both dimensions
    # Original layout:
    #  0  1  2  3
    #  4  5  6  7
    #  8  9 10 11
    # 12 13 14 15
    #
    # After (1,1) wrap (shift chunks, not elements):
    # Chunk (0,0) -> (1,1), Chunk (0,1) -> (1,0)
    # Chunk (1,0) -> (0,1), Chunk (1,1) -> (0,0)
    # Result:
    # 10 11  8  9
    # 14 15 12 13
    #  2  3  0  1
    #  6  7  4  5
    expected = np.array(
        [
            [10, 11, 8, 9],
            [14, 15, 12, 13],
            [2, 3, 0, 1],
            [6, 7, 4, 5],
        ]
    )
    np.testing.assert_equal(array[:], expected)


async def test_shift_3d_mixed_offset() -> None:
    """Test 3D shift with mixed offsets (-1, 0, -1) and verify element_shift return."""
    repo = ic.Repository.create(
        storage=ic.in_memory_storage(),
    )
    session = repo.writable_session("main")
    root = zarr.group(store=session.store, overwrite=True)
    # 6x4x8 array with 2x2x4 chunks = 3x2x2 chunks grid
    array = root.create_array(
        "array", shape=(6, 4, 8), chunks=(2, 2, 4), dtype="i4", fill_value=-1
    )
    array[:] = np.arange(6 * 4 * 8).reshape(6, 4, 8)
    session.commit("create array")

    session = repo.writable_session("main")
    root = zarr.group(store=session.store, overwrite=False)
    # Shift by (-1, 0, -1) - shift in first and third dimensions only
    element_shift = session.shift_array("/array", (-1, 0, -1), ic.ShiftMode.DISCARD)
    array = cast("zarr.Array[Any]", root["array"])

    # Verify element_shift: (-1 * 2, 0 * 2, -1 * 4) = (-2, 0, -4)
    assert element_shift == (-2, 0, -4)

    # Verify data shifted correctly:
    # - Dim 0: shifted left by 1 chunk (2 elements), last 2 rows vacated
    # - Dim 1: unchanged
    # - Dim 2: shifted left by 1 chunk (4 elements), last 4 columns vacated
    original = np.arange(6 * 4 * 8).reshape(6, 4, 8)

    # Non-vacated region: [0:4, :, 0:4] should have data from [2:6, :, 4:8]
    np.testing.assert_equal(array[0:4, :, 0:4], original[2:6, :, 4:8])

    # Vacated in dim 0: rows 4-5 should be fill value
    assert np.all(array[4:6, :, :] == -1)

    # Vacated in dim 2: columns 4-7 should be fill value (for non-vacated rows)
    assert np.all(array[0:4, :, 4:8] == -1)


async def test_shift_zero_offset() -> None:
    """Test that zero offset works correctly (no change)."""
    repo = ic.Repository.create(
        storage=ic.in_memory_storage(),
    )
    session = repo.writable_session("main")
    root = zarr.group(store=session.store, overwrite=True)
    array = root.create_array(
        "array", shape=(10,), chunks=(2,), dtype="i4", fill_value=42
    )
    array[:] = np.arange(10)
    session.commit("create array")

    session = repo.writable_session("main")
    root = zarr.group(store=session.store, overwrite=False)
    session.shift_array("/array", (0,), ic.ShiftMode.DISCARD)
    array = cast("zarr.Array[Any]", root["array"])

    np.testing.assert_equal(array[:], np.arange(10))


async def test_shift_persists_after_commit() -> None:
    """Test that shift changes persist after commit."""
    repo = ic.Repository.create(
        storage=ic.in_memory_storage(),
    )
    session = repo.writable_session("main")
    root = zarr.group(store=session.store, overwrite=True)
    array = root.create_array(
        "array", shape=(10,), chunks=(2,), dtype="i4", fill_value=-1
    )
    array[:] = np.arange(10)
    session.commit("create array")

    # Shift with DISCARD
    session = repo.writable_session("main")
    session.shift_array("/array", (-2,), ic.ShiftMode.DISCARD)
    session.commit("shifted")

    # Verify in readonly session
    session = repo.readonly_session(branch="main")
    root = zarr.open_group(store=session.store, mode="r")
    array = cast("zarr.Array[Any]", root["array"])

    # First 6 elements should be shifted
    np.testing.assert_equal(array[0:6], np.arange(4, 10))
    # Last 4 elements should be fill_value
    assert np.all(array[6:] == -1)


async def test_shift_with_string_mode() -> None:
    """Test that string literals work as mode parameter."""
    repo = ic.Repository.create(
        storage=ic.in_memory_storage(),
    )
    session = repo.writable_session("main")
    root = zarr.group(store=session.store, overwrite=True)
    array = root.create_array(
        "array", shape=(10,), chunks=(2,), dtype="i4", fill_value=-1
    )
    array[:] = np.arange(10)
    session.commit("create array")

    # Test with lowercase string
    session = repo.writable_session("main")
    session.shift_array("/array", (-2,), "discard")
    array = cast("zarr.Array[Any]", zarr.group(store=session.store)["array"])
    np.testing.assert_equal(array[0:6], np.arange(4, 10))
    assert np.all(array[6:] == -1)
