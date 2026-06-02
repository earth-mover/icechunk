# Moving Chunks

With Zarr alone, reordering chunks means rewriting them—shifting a 1TB array requires reading every chunk and writing it to a new location. For cloud data, that's downloading terabytes, transforming locally, and re-uploading everything.

Icechunk adds a layer of indirection: chunks are addressed through a manifest, not by their position. Shifting chunks just updates the manifest—the actual data never moves. A 1TB shift completes in milliseconds, transferring only the small metadata update.

This enables **rolling time windows**—continuously updating datasets like forecasts or sensor streams where you discard old data and append new, without ever copying a single byte.

!!! note "Moving Chunks vs Moving Nodes"
    This page covers moving **chunks within an array** (reordering data).
    To move or rename **arrays and groups** in the hierarchy,
    see [Moving and Renaming Nodes](moving-nodes.md).

## Choosing an API

| Method | Best For | Flexibility |
|--------|----------|-------------|
| [`shift_array`][icechunk.session.Session.shift_array] | Uniform shifts | Simple—just specify offset |
| [`reindex_array`][icechunk.session.Session.reindex_array] | Custom transformations | Maximum—you control every chunk |

## Offsets Are in Chunks, Not Elements

Both methods work with **chunk indices**, not array indices. If your array has `chunk_size=2`, then an offset of `(-1,)` shifts by 1 chunk, which is 2 elements:

```python
# With chunk_size=2:
shift_array("/arr", (-1,)) #  → shifts by 1 chunk = 2 elements
shift_array("/arr", (-2,)) # → shifts by 2 chunks = 4 elements
```

Why chunks instead of elements? Because these are **metadata-only operations**. Shifting by partial chunks would require splitting and rewriting chunk data.

## `shift_array` { #shift_array }

The [`shift_array`][icechunk.session.Session.shift_array] method moves all chunks by a fixed offset per dimension (negative to shift toward index 0, positive toward higher indices). Out-of-bounds chunks are discarded, and vacated positions are cleared (reset to fill value).

```python exec="on" session="chunks" source="material-block" result="code"
import numpy as np
import icechunk as ic
import zarr

np.set_printoptions(formatter={'int': lambda x: f'{x:3d}'})

repo = ic.Repository.create(ic.in_memory_storage())
session = repo.writable_session("main")
arr = zarr.create(
    store=session.store,
    path="arr",
    shape=(10,),
    chunks=(2,),
    dtype="i4",
    fill_value=-1,
)
arr[:] = np.arange(10)
print("Before:", arr[:])

session.shift_array("/arr", (-2,))  # Shift left by 2 chunks
print("After: ", arr[:])
```

The chunks containing `[0, 1, 2, 3]` were discarded. The vacated end is reset to the fill value (`-1`).

### Preserving Data with Resize

Chunks that shift out of bounds are lost. To preserve all data when shifting, resize first to make room:

```python exec="on" session="chunks" source="material-block" result="code"
repo = ic.Repository.create(ic.in_memory_storage())
session = repo.writable_session("main")
arr = zarr.create(
    store=session.store,
    path="arr",
    shape=(10,),
    chunks=(2,),
    dtype="i4",
    fill_value=-1,
)
arr[:] = np.arange(10)
print("Before:", arr[:])

arr.resize((14,))  # Add space for 2 more chunks
session.shift_array("/arr", (2,))
print("After: ", arr[:])
```

### Multi-dimensional Arrays

For N-dimensional arrays, provide an offset for each dimension:

```python exec="on" session="chunks" source="material-block" result="code"
repo = ic.Repository.create(ic.in_memory_storage())
session = repo.writable_session("main")
arr = zarr.create(
    store=session.store,
    path="arr2d",
    shape=(6, 4),
    chunks=(2, 2),
    dtype="i4",
    fill_value=-1,
)
arr[:] = np.arange(24).reshape(6, 4)
print("Original 6x4 array:")
print(arr[:])

session.shift_array("/arr2d", (1, 0))  # Shift down 1 chunk
print("\nAfter shift (1, 0):")
print(arr[:])
```

### Rectilinear Chunk Grids

See [Rectilinear chunk grids](zarr.md#rectilinear-chunk-grids) for an introduction to the feature itself; this section covers how `shift_array` interacts with such a grid.

`shift_array` also works on arrays whose chunk grid is rectilinear—each axis has its own sequence of chunk lengths instead of a single fixed chunk size. On a rectilinear axis, a shift of `k` cyclically rotates the per-axis chunk-size sequence by `k`. Surviving chunks land in slots whose declared size matches their data; dropped chunks contribute their sizes (but not their data) at the vacated end of the axis, where the array now reads as fill. The array shape and chunk count are preserved, but the chunk-size sequence is rotated. The operation is still metadata-only; no chunk payloads are read, decoded, or rewritten.

To make the geometry concrete, take a 1-D array of shape `(3,)` with chunks `((1, 2),)` holding the values `[10, 20, 30]`. Chunk 0 has length 1 and covers index 0 (`[10]`); chunk 1 has length 2 and covers indices 1–2 (`[20, 30]`). Shifting by `(1,)` rotates the size sequence to `(2, 1)`: the size-2 slot moves to the front (filled with fill values because the chunk that fell off contributed its size, not its data), and the surviving chunk 0 lands at the new size-1 slot at index 2. Chunk 1 has fallen off the end and is dropped. The resulting array reads as `[-1, -1, 10]` (with a fill value of `-1`). The chunk grid sequence has changed even though the array shape has not.

For a clearer view of the rotation, take chunks `((1, 2, 3),)` over shape `(6,)`. A shift of `+1` rotates the size sequence one slot to the right, producing `((3, 1, 2),)`: source chunk 0 lands at the new size-1 slot, source chunk 1 at the size-2 slot, and source chunk 2 falls off the end—but its size `3` rotates into the leading slot, where it is filled with the fill value. With more than one chunk dropping (`((1, 1, 1),)` shifted by `+2`, for example), each dropped chunk contributes its size to a separate fill slot rather than coalescing into one.

```python
import icechunk as ic
import zarr
from zarr.core.chunk_grids import RectilinearChunkGrid

zarr.config.set({"array.rectilinear_chunks": True})

repo = ic.Repository.create(ic.in_memory_storage())
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
session.shift_array("/a", (1,))  # chunks become ((2, 1),): [-1, -1, 10]
session.commit("shift")

# Inspect the new chunk grid
session = repo.readonly_session("main")
arr = zarr.open_array(session.store, path="a")
print(arr.read_chunk_sizes)  # ((2, 1),)
```

Negative offsets are symmetric: the size sequence rotates the other way, dropped chunks contribute their sizes to the trailing end, and surviving chunks keep their lengths.

### Example: Rolling Time Window

Imagine a sensor array storing the last 7 days of hourly readings—shape `(168,)` with one chunk per day `(24,)`. Each day, you want to discard the oldest day and make room for new data:

```python
arr = zarr.open_array(store=session.store, path="sensors/temperature")
chunk_offset = (-1,)

# Compute the element-space shift from the chunk offset and chunk shape
element_shift = tuple(o * c for o, c in zip(chunk_offset, arr.chunks))
# element_shift = (-24,) — the shift in element space

# Shift left by 1 chunk, discarding the oldest
session.shift_array("/sensors/temperature", chunk_offset)

# Write new day's data to the vacated region
arr[element_shift[0]:] = todays_readings

session.commit(f"Updated sensor data for {today}")
```

Computing the index shift in element space is straightforward: multiply each chunk offset by the corresponding chunk size. This tells you exactly where to write new data.

This pattern works identically whether your array is 1 KB or 1 PB, and whether it's on local disk or cloud object storage—the shift is always instant with zero data transfer.

## `reindex_array` { #reindex_array }

For transformations that [`shift_array`][icechunk.session.Session.shift_array] can't express, [`reindex_array`][icechunk.session.Session.reindex_array] gives you complete control. You provide a `forward` function that maps each chunk's old position to its new position. Your function receives a chunk index (as a list) and returns a new index to move the chunk there, or `None` to skip it (leave it in place).

However, `reindex_array` only visits chunk positions that contain data — empty (fill value) positions are skipped. This means that if an empty chunk would shift into an occupied position, the occupied position retains stale data. To handle this, provide a `backward` function — the inverse of `forward`. For each existing chunk position, the backward function determines whether a real chunk should have moved there; if not, the position is cleared to the fill value. See [Providing a Backward Function](#backward-function) for an example.

```python exec="on" session="chunks" source="material-block" result="code"
repo = ic.Repository.create(ic.in_memory_storage())
session = repo.writable_session("main")
arr = zarr.create(
    store=session.store,
    path="arr",
    shape=(10,),
    chunks=(2,),
    dtype="i4",
    fill_value=-1,
)
arr[:] = np.arange(10)
print("Before:", arr[:])

def shift_and_filter(idx):
    """Shift left by 2, discard chunks that would go negative."""
    new_idx = idx[0] - 2
    return None if new_idx < 0 else [new_idx]

session.reindex_array("/arr", forward=shift_and_filter)
print("After: ", arr[:])
```

When all chunks contain data, the forward function alone produces correct results. Source positions that are not also destinations retain stale data — provide a `backward` function to clear them.

### Providing a Backward Function { #backward-function }

When your array has empty (fill value) chunks, a forward-only reindex can leave stale data behind. The `backward` function is the inverse of `forward`: given a chunk position, it returns the position that *would have mapped there*. This lets icechunk detect and clear positions that should now be empty.

Here's a concrete example. We create an array with a gap — chunk 0 has value `1`, chunk 1 is empty (fill value `-1`), and chunk 2 has value `3`:

```python exec="on" session="chunks" source="material-block" result="code"
# Forward only: the empty chunk at index 1 doesn't shift into index 2
repo = ic.Repository.create(ic.in_memory_storage())
session = repo.writable_session("main")
arr = zarr.create(
    store=session.store, path="arr", shape=(3,), chunks=(1,),
    dtype="i4", fill_value=-1,
)
arr[0] = 1
arr[2] = 3
session.commit("init")

session = repo.writable_session("main")
arr = zarr.open_array(session.store, path="arr")
print("Before:      ", arr[:])  # [ 1, -1,  3]

n_chunks = 3
offset = 1

def fwd(idx):
    new = idx[0] + offset
    return [new] if 0 <= new < n_chunks else None

session.reindex_array("/arr", forward=fwd)
print("Forward only:", arr[:])  # [ 1,  1,  3] — index 0 is stale!
```

Index 0 should be empty after the shift, but the empty chunk at index 1 was never visited, so nothing cleared it. Adding a backward function fixes this:

```python exec="on" session="chunks" source="material-block" result="code"
repo = ic.Repository.create(ic.in_memory_storage())
session = repo.writable_session("main")
arr = zarr.create(
    store=session.store, path="arr", shape=(3,), chunks=(1,),
    dtype="i4", fill_value=-1,
)
arr[0] = 1
arr[2] = 3
session.commit("init")

session = repo.writable_session("main")
arr = zarr.open_array(session.store, path="arr")
print("Before:       ", arr[:])  # [ 1, -1,  3]

n_chunks = 3
offset = 1

def fwd(idx):
    new = idx[0] + offset
    return [new] if 0 <= new < n_chunks else None

def bwd(idx):
    new = idx[0] - offset
    return [new] if 0 <= new < n_chunks else None

session.reindex_array("/arr", forward=fwd, backward=bwd)
print("With backward:", arr[:])  # [-1,  1,  3] — index 0 correctly cleared
```

!!! tip
    [`shift_array`][icechunk.session.Session.shift_array] always provides both forward and
    backward functions internally, so it handles empty chunks correctly without
    any extra work.

### Custom Transformations

With `reindex_array`, you can implement any chunk permutation:

```python exec="on" session="chunks" source="material-block" result="code"
repo = ic.Repository.create(ic.in_memory_storage())
session = repo.writable_session("main")
arr = zarr.create(
    store=session.store,
    path="arr",
    shape=(10,),
    chunks=(2,),
    dtype="i4",
    fill_value=-1,
)
arr[:] = np.arange(10)
print("Before:", arr[:])

def reverse_chunks(idx):
    """Reverse the order of all chunks."""
    return [4 - idx[0]]  # 0↔4, 1↔3, 2 stays

session.reindex_array("/arr", forward=reverse_chunks)
print("After: ", arr[:])
```

### Multi-dimensional Example

```python exec="on" session="chunks" source="material-block" result="code"
repo = ic.Repository.create(ic.in_memory_storage())
session = repo.writable_session("main")
arr = zarr.create(
    store=session.store,
    path="arr2d",
    shape=(4, 4),
    chunks=(2, 2),
    dtype="i4",
    fill_value=-1,
)
arr[:] = np.arange(16).reshape(4, 4)
print("Original:")
print(arr[:])

def swap_quadrants(idx):
    """Swap diagonal quadrants (top-left ↔ bottom-right, etc.)."""
    row, col = idx
    return [(row + 1) % 2, (col + 1) % 2]

session.reindex_array("/arr2d", forward=swap_quadrants)
print("\nAfter swapping quadrants:")
print(arr[:])
```
