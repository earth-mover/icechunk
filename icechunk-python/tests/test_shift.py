from typing import Any, cast

import numpy as np
import pytest

import icechunk as ic
import zarr

try:
    from zarr.core.chunk_grids import (  # type: ignore[attr-defined, unused-ignore]
        RectilinearChunkGrid,
    )

    _has_rectilinear = True
except ImportError:
    _has_rectilinear = False


def test_shift_left() -> None:
    """Test shifting left - vacated positions retain stale data."""
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
    session.shift_array("/array", (-4,))
    array = cast("zarr.Array[Any]", root["array"])

    # First 42 elements should be shifted (8 array elements shifted left)
    np.testing.assert_equal(array[0:42], np.arange(8, 50))
    # Last 8 elements are now empty
    np.testing.assert_equal(array[42:], 42)


def test_shift_right() -> None:
    """Test shifting right - vacated positions retain stale data."""
    repo = ic.Repository.create(
        storage=ic.in_memory_storage(),
    )
    session = repo.writable_session("main")
    root = zarr.group(store=session.store, overwrite=True)
    array = root.create_array(
        "array", chunks=(2,), fill_value=-1, data=np.arange(50, dtype="i4")
    )
    session.commit("create array")

    session = repo.writable_session("main")
    root = zarr.group(store=session.store, overwrite=False)
    session.shift_array("/array", (4,))
    array = cast("zarr.Array[Any]", root["array"])

    # First 8 elements now empty (fill value -1)
    np.testing.assert_equal(array[0:8], -1)
    # Rest should be shifted data (last 8 elements of original are lost)
    np.testing.assert_equal(array[8:50], np.arange(0, 42))


def test_resize_then_shift_right() -> None:
    """Test manual resize + shift right - stale data remains in vacated positions."""
    repo = ic.Repository.create(
        storage=ic.in_memory_storage(),
    )
    session = repo.writable_session("main")
    root = zarr.group(store=session.store, overwrite=True)
    # 50 elements, chunk size 2 = 25 chunks
    array = root.create_array(
        "array", chunks=(2,), fill_value=42, data=np.arange(50, dtype="i4")
    )
    session.commit("create array")

    session = repo.writable_session("main")
    root = zarr.group(store=session.store, overwrite=False)
    array = cast("zarr.Array[Any]", root["array"])

    # First resize the array to make room (this is zarr-level operation)
    array.resize((58,))  # 50 + 4*2 = 58 (4 chunks * 2 elements/chunk)
    assert array.shape == (58,)

    # Then shift right by 4 chunks
    session.shift_array("/array", (4,))

    # First 8 elements retain stale data (original values 0-7)
    np.testing.assert_equal(array[0:8], 42)
    # Original data shifted to positions 8-57
    np.testing.assert_equal(array[8:58], np.arange(50))

    session.commit("shifted")

    # Verify persists after commit
    session = repo.readonly_session(branch="main")
    array = zarr.open_array(store=session.store, path="array", mode="r")
    assert array.shape == (58,)
    np.testing.assert_equal(array[0:8], 42)
    np.testing.assert_equal(array[8:58], np.arange(50))


def test_shift_2d() -> None:
    """Test shift on a 2D array - vacated positions retain stale data."""
    repo = ic.Repository.create(
        storage=ic.in_memory_storage(),
    )
    session = repo.writable_session("main")
    root = zarr.group(store=session.store, overwrite=True)
    # 4x4 array with 2x2 chunks = 2x2 chunks grid
    array = root.create_array(
        "array",
        chunks=(2, 2),
        fill_value=-1,
        data=np.arange(16, dtype="i4").reshape(4, 4),
    )
    session.commit("create array")

    session = repo.writable_session("main")
    root = zarr.group(store=session.store, overwrite=False)
    session.shift_array("/array", (1, 0))
    array = cast("zarr.Array[Any]", root["array"])

    # First 2 rows retain stale data (original rows 0-1)
    np.testing.assert_equal(array[0:2, :], -1)
    # Rows 2-3 should have original rows 0-1 (shifted from chunk 0 to chunk 1)
    np.testing.assert_equal(array[2:4, :], np.arange(8).reshape(2, 4))


def test_shift_3d_mixed_offset() -> None:
    """Test 3D shift with mixed offsets (-1, 0, -1)."""
    repo = ic.Repository.create(
        storage=ic.in_memory_storage(),
    )
    session = repo.writable_session("main")
    root = zarr.group(store=session.store, overwrite=True)
    # 6x4x8 array with 2x2x4 chunks = 3x2x2 chunks grid
    array = root.create_array(
        "array",
        chunks=(2, 2, 4),
        fill_value=-1,
        data=np.arange(6 * 4 * 8, dtype="i4").reshape(6, 4, 8),
    )
    session.commit("create array")

    session = repo.writable_session("main")
    root = zarr.group(store=session.store, overwrite=False)
    session.shift_array("/array", (-1, 0, -1))
    array = cast("zarr.Array[Any]", root["array"])

    # Verify data shifted correctly:
    # - Dim 0: shifted left by 1 chunk (2 elements), last 2 rows are fill value
    # - Dim 1: unchanged
    # - Dim 2: shifted left by 1 chunk (4 elements), last 4 columns are fill data
    original = np.arange(6 * 4 * 8).reshape(6, 4, 8)

    # Non-vacated region: [0:4, :, 0:4] should have data from [2:6, :, 4:8]
    np.testing.assert_equal(array[0:4, :, 0:4], original[2:6, :, 4:8])


def test_shift_zero_offset() -> None:
    """Test that zero offset works correctly (no change)."""
    repo = ic.Repository.create(
        storage=ic.in_memory_storage(),
    )
    session = repo.writable_session("main")
    root = zarr.group(store=session.store, overwrite=True)
    array = root.create_array(
        "array", chunks=(2,), fill_value=42, data=np.arange(10, dtype="i4")
    )
    session.commit("create array")

    session = repo.writable_session("main")
    root = zarr.group(store=session.store, overwrite=False)
    session.shift_array("/array", (0,))
    array = cast("zarr.Array[Any]", root["array"])

    np.testing.assert_equal(array[:], np.arange(10))


def test_shift_persists_after_commit() -> None:
    """Test that shift changes persist after commit."""
    repo = ic.Repository.create(
        storage=ic.in_memory_storage(),
    )
    session = repo.writable_session("main")
    root = zarr.group(store=session.store, overwrite=True)
    array = root.create_array(
        "array", chunks=(2,), fill_value=-1, data=np.arange(10, dtype="i4")
    )
    session.commit("create array")

    session = repo.writable_session("main")
    session.shift_array("/array", (-2,))
    session.commit("shifted")

    # Verify in readonly session
    session = repo.readonly_session(branch="main")
    root = zarr.open_group(store=session.store, mode="r")
    array = cast("zarr.Array[Any]", root["array"])

    # First 6 elements should be shifted
    np.testing.assert_equal(array[0:6], np.arange(4, 10))
    # Last 4 elements retain stale data (original values 6-9)
    np.testing.assert_equal(array[6:], -1)


def test_shift_with_missing_chunks() -> None:
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
    # (), 2, ()
    arr[1] = 2.0  # only c/1 written; c/0 and c/2 are fill
    session.commit("initial")

    session = repo.writable_session("main")
    arr = zarr.open_array(session.store, path="data")
    session.shift_array("/data", (1,))

    np.testing.assert_equal(arr[:], [-1, -1, 2])


def test_resize_then_shift_same_session() -> None:
    """Resize records out-of-bounds deletes in the change set; shift must skip them."""
    repo = ic.Repository.create(storage=ic.in_memory_storage())
    session = repo.writable_session("main")
    arr = zarr.open_array(
        session.store,
        path="d",
        mode="w",
        shape=(3,),
        chunks=(1,),
        dtype="f4",
        fill_value=-1,
    )
    arr[:] = [1.0, 2.0, 3.0]
    session.commit("init")

    session = repo.writable_session("main")
    arr = zarr.open_array(session.store, path="d")
    arr.resize((2,))
    session.shift_array("/d", (-1,))
    arr = zarr.open_array(session.store, path="d")
    np.testing.assert_equal(arr[:], [2.0, -1.0])


@pytest.mark.skipif(not _has_rectilinear, reason="RectilinearChunkGrid not available")
def test_shift_rectilinear_chunks() -> None:
    """Shift +1 on a rectilinear axis with chunk sizes (1, 2).

    Source chunk 0 (size 1, data [10]) lands at destination chunk index 1.
    Source chunk 1 ([20, 30]) falls off the end. To keep this consistent
    with the chunk-index shift semantics, the destination grid must be
    reshaped to ((2, 1),) so the surviving chunk's size matches its data.
    Element-space result: [fill, fill, 10].
    """
    repo = ic.Repository.create(storage=ic.in_memory_storage())
    session = repo.writable_session("main")
    root = zarr.group(store=session.store)
    arr = root.create_array(
        name="a",
        shape=(3,),
        dtype="int32",
        chunks=RectilinearChunkGrid(chunk_shapes=((1, 2),)),
        fill_value=-1,
    )
    arr[:] = [10, 20, 30]
    session.commit("write")

    session = repo.writable_session("main")
    session.shift_array("/a", (1,))
    session.commit("shift")

    session = repo.readonly_session("main")
    arr = cast("zarr.Array[Any]", zarr.open_array(session.store, path="a"))
    np.testing.assert_equal(arr[:], [-1, -1, 10])


@pytest.mark.skipif(not _has_rectilinear, reason="RectilinearChunkGrid not available")
def test_shift_rectilinear_negative_1d() -> None:
    """Negative shift on a rectilinear axis must reshape the grid symmetrically
    to the positive case: surviving chunk keeps its source size, vacated trailing
    run collapses into a single fill chunk."""
    repo = ic.Repository.create(storage=ic.in_memory_storage())
    session = repo.writable_session("main")
    root = zarr.group(store=session.store)
    arr = root.create_array(
        name="a",
        shape=(3,),
        dtype="int32",
        chunks=RectilinearChunkGrid(chunk_shapes=((2, 1),)),
        fill_value=-1,
    )
    arr[:] = [10, 20, 30]
    session.commit("write")

    session = repo.writable_session("main")
    session.shift_array("/a", (-1,))
    session.commit("shift")

    session = repo.readonly_session("main")
    arr = cast("zarr.Array[Any]", zarr.open_array(session.store, path="a"))
    np.testing.assert_equal(arr[:], [30, -1, -1])
    if hasattr(arr, "read_chunk_sizes"):
        assert tuple(tuple(s) for s in arr.read_chunk_sizes) == ((1, 2),)


@pytest.mark.skipif(not _has_rectilinear, reason="RectilinearChunkGrid not available")
def test_shift_rectilinear_zero_offset() -> None:
    """Zero offset on a rectilinear axis must be a no-op for both data and grid."""
    repo = ic.Repository.create(storage=ic.in_memory_storage())
    session = repo.writable_session("main")
    root = zarr.group(store=session.store)
    arr = root.create_array(
        name="a",
        shape=(3,),
        dtype="int32",
        chunks=RectilinearChunkGrid(chunk_shapes=((1, 2),)),
        fill_value=-1,
    )
    arr[:] = [10, 20, 30]
    session.commit("write")

    session = repo.writable_session("main")
    session.shift_array("/a", (0,))
    session.commit("shift")

    session = repo.readonly_session("main")
    arr = cast("zarr.Array[Any]", zarr.open_array(session.store, path="a"))
    np.testing.assert_equal(arr[:], [10, 20, 30])
    if hasattr(arr, "read_chunk_sizes"):
        assert tuple(tuple(s) for s in arr.read_chunk_sizes) == ((1, 2),)


@pytest.mark.skipif(not _has_rectilinear, reason="RectilinearChunkGrid not available")
def test_shift_rectilinear_drops_all() -> None:
    """When the shift evicts every source chunk, the rectilinear grid must
    collapse to a single fill chunk spanning the full dimension."""
    repo = ic.Repository.create(storage=ic.in_memory_storage())
    session = repo.writable_session("main")
    root = zarr.group(store=session.store)
    arr = root.create_array(
        name="a",
        shape=(3,),
        dtype="int32",
        chunks=RectilinearChunkGrid(chunk_shapes=((1, 1, 1),)),
        fill_value=-1,
    )
    arr[:] = [10, 20, 30]
    session.commit("write")

    session = repo.writable_session("main")
    session.shift_array("/a", (5,))
    session.commit("shift")

    session = repo.readonly_session("main")
    arr = cast("zarr.Array[Any]", zarr.open_array(session.store, path="a"))
    np.testing.assert_equal(arr[:], [-1, -1, -1])
    if hasattr(arr, "read_chunk_sizes"):
        assert tuple(tuple(s) for s in arr.read_chunk_sizes) == ((3,),)


@pytest.mark.skipif(not _has_rectilinear, reason="RectilinearChunkGrid not available")
def test_shift_rectilinear_mixed_axes() -> None:
    """Shifting only the rectilinear axis must leave the regular axis untouched
    and reshape only the shifted axis."""
    repo = ic.Repository.create(storage=ic.in_memory_storage())
    session = repo.writable_session("main")
    root = zarr.group(store=session.store)
    # zarr's RectilinearChunkGrid takes per-axis chunk shapes; encode the
    # "regular" axis 0 as a uniform tuple (2, 2) so axis 0 stays regular-shaped
    # while axis 1 carries the genuinely irregular (1, 2) layout.
    arr = root.create_array(
        name="a",
        shape=(4, 3),
        dtype="int32",
        chunks=RectilinearChunkGrid(chunk_shapes=((2, 2), (1, 2))),
        fill_value=-1,
    )
    arr[:] = np.arange(12, dtype="int32").reshape(4, 3)
    session.commit("write")

    session = repo.writable_session("main")
    session.shift_array("/a", (0, 1))
    session.commit("shift")

    session = repo.readonly_session("main")
    arr = cast("zarr.Array[Any]", zarr.open_array(session.store, path="a"))
    expected = np.array(
        [
            [-1, -1, 0],
            [-1, -1, 3],
            [-1, -1, 6],
            [-1, -1, 9],
        ],
        dtype="int32",
    )
    np.testing.assert_equal(arr[:], expected)
    if hasattr(arr, "read_chunk_sizes"):
        sizes = tuple(tuple(s) for s in arr.read_chunk_sizes)
        assert sizes == ((2, 2), (2, 1))


@pytest.mark.skipif(not _has_rectilinear, reason="RectilinearChunkGrid not available")
def test_shift_rectilinear_persists_through_commit() -> None:
    """Guards against stale manifest extents after the chunk count changes:
    reopen the Repository from storage to force a full reload."""
    storage = ic.in_memory_storage()
    repo = ic.Repository.create(storage=storage)
    session = repo.writable_session("main")
    root = zarr.group(store=session.store)
    arr = root.create_array(
        name="a",
        shape=(3,),
        dtype="int32",
        chunks=RectilinearChunkGrid(chunk_shapes=((1, 2),)),
        fill_value=-1,
    )
    arr[:] = [10, 20, 30]
    session.commit("write")

    session = repo.writable_session("main")
    session.shift_array("/a", (1,))
    session.commit("shift")

    reopened = ic.Repository.open(storage)
    session = reopened.readonly_session("main")
    arr = cast("zarr.Array[Any]", zarr.open_array(session.store, path="a"))
    np.testing.assert_equal(arr[:], [-1, -1, 10])
    if hasattr(arr, "read_chunk_sizes"):
        assert tuple(tuple(s) for s in arr.read_chunk_sizes) == ((2, 1),)
