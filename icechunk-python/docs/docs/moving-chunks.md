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
| [`shift_array`][icechunk.Session.shift_array] | Uniform shifts with edge handling | Simple—just specify offset and mode |
| [`reindex_array`][icechunk.Session.reindex_array] | Custom transformations | Maximum—you control every chunk |

## Offsets Are in Chunks, Not Elements

Both methods work with **chunk indices**, not array indices. If your array has `chunk_size=2`, then an offset of `(-1,)` shifts by 1 chunk, which is 2 elements:

```python
# With chunk_size=2:
shift_array("/arr", (-1,), "wrap") #  → shifts by 1 chunk = 2 elements
shift_array("/arr", (-2,), "wrap") # → shifts by 2 chunks = 4 elements
```

Why chunks instead of elements? Because these are **metadata-only operations**. Shifting by partial chunks would require splitting and rewriting chunk data.

For convenience, `shift_array` returns the shift converted to element space—so you don't need to manually track chunk sizes when determining where to write new data.

## shift_array { #shift_array }

The [`shift_array`][icechunk.Session.shift_array] method moves all chunks by a fixed offset per dimension (negative to shift toward index 0, positive toward higher indices), with built-in handling for what happens at the boundaries. For convenience, it returns the **index shift** (`chunk_offset × chunk_size` for each dimension).

### Shift Modes

The `mode` parameter controls what happens to chunks that shift out of bounds:

| Mode | Behavior | Data Loss |
|------|----------|-----------|
| `"wrap"` | Chunks wrap to the other side | None |
| `"discard"` | Out-of-bounds chunks are dropped | Yes |

You can use strings (`"wrap"`, `"discard"`) or the enum ([`ic.ShiftMode.WRAP`][icechunk.ShiftMode], [`ic.ShiftMode.DISCARD`][icechunk.ShiftMode]).

#### WRAP Mode

Chunks that shift out of one end reappear at the other—no data is lost.

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

session.shift_array("/arr", (-2,), "wrap")  # Shift left by 2 chunks
print("After: ", arr[:])
```

Notice how `[0, 1, 2, 3]` wrapped around to the end.

#### DISCARD Mode

Out-of-bounds chunks are dropped, and vacated positions return the fill value.

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

session.shift_array("/arr", (-2,), "discard")  # Shift left, discard overflow
print("After: ", arr[:])
```

The chunks containing `[0, 1, 2, 3]` were discarded, and the vacated end filled with `-1`.

### Preserving Data with Resize

With `"discard"` mode, chunks that shift out of bounds are lost. To preserve everything when shifting, resize first:

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
session.shift_array("/arr", (2,), "discard")
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

session.shift_array("/arr2d", (1, 0), "discard")  # Shift down 1 chunk
print("\nAfter shift (1, 0):")
print(arr[:])
```

### Example: Rolling Time Window

Imagine a sensor array storing the last 7 days of hourly readings—shape `(168,)` with one chunk per day `(24,)`. Each day, you want to discard the oldest day and make room for new data:

```python
# Each day: shift left by 1 chunk, discarding the oldest
element_shift = session.shift_array("/sensors/temperature", (-1,), "discard")
# element_shift = (-24,) — the shift in element space

# Write new day's data to the vacated region
arr[element_shift[0]:] = todays_readings

session.commit(f"Updated sensor data for {today}")
```

The return value tells you exactly where to write new data—no need to manually track chunk sizes.

This pattern works identically whether your array is 1 KB or 1 PB, and whether it's on local disk or cloud object storage—the shift is always instant with zero data transfer.

## reindex_array { #reindex_array }

For transformations that [`shift_array`][icechunk.Session.shift_array] can't express, [`reindex_array`][icechunk.Session.reindex_array] gives you complete control. You provide a function that maps each chunk's old position to its new position.

Your function receives a chunk index (as a list) and returns:

- A new index (as a list) to move the chunk there
- `None` to discard the chunk

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

session.reindex_array("/arr", shift_and_filter, delete_vacated=True)
print("After: ", arr[:])
```

### The delete_vacated Parameter

The `delete_vacated` parameter controls what happens to source positions after chunks move away:

| Value | Behavior |
|-------|----------|
| `True` | Vacated positions are deleted (return fill value) |
| `False` | Vacated positions keep stale references |

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
session.commit("setup")

def shift_left_2(idx):
    new_idx = idx[0] - 2
    return None if new_idx < 0 else [new_idx]

# delete_vacated=False: source positions keep stale data
session = repo.writable_session("main")
arr = zarr.open_array(session.store, path="arr")
session.reindex_array("/arr", shift_left_2, delete_vacated=False)
print("delete_vacated=False:", arr[:])

# delete_vacated=True: vacated positions return fill value
session = repo.writable_session("main")
arr = zarr.open_array(session.store, path="arr")
session.reindex_array("/arr", shift_left_2, delete_vacated=True)
print("delete_vacated=True: ", arr[:])
```

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

session.reindex_array("/arr", reverse_chunks, delete_vacated=False)
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

session.reindex_array("/arr2d", swap_quadrants, delete_vacated=False)
print("\nAfter swapping quadrants:")
print(arr[:])
```
