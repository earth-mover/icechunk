# Moving and Renaming Nodes

Made a typo in an array name? Created data in the wrong group? With traditional cloud storage, fixing these mistakes means downloading the entire dataset, writing it to a new location, and deleting the original—potentially gigabytes of transfers for a one-character fix.

With Icechunk, renaming or moving any node is instant—just a small metadata update. See the [`move`][icechunk.Session.move] API reference for details.

!!! note "Moving Nodes vs Moving Chunks"
    This page covers moving **nodes** (arrays and groups) in the hierarchy.
    To move **chunks within an array** (for rolling windows, etc.),
    see [Moving Chunks](moving-chunks.md).

## Basic Usage

```python exec="on" session="nodes" source="material-block" result="code"
import icechunk as ic
import zarr
import numpy as np

# Create a repository with some data
repo = ic.Repository.create(ic.in_memory_storage())
session = repo.writable_session("main")
root = zarr.group(session.store)
root.create_group("data/raw")
arr = root.create_array("data/raw/temperature", shape=(100,), dtype="f4")
arr[:] = np.random.randn(100)
session.commit("Initial structure")

print("Before:")
print(root.tree())
```

```python exec="on" session="nodes" source="material-block" result="code"
# Move requires a rearrange session
session = repo.rearrange_session("main")
session.move("/data/raw", "/data/v1")
session.commit("Renamed raw to v1")

root = zarr.group(repo.writable_session("main").store)
print("After:")
print(root.tree())
```

## Rearrange Sessions

Node operations require a **rearrange session**—a special session type for structural changes only. This separation keeps your commit history clean: data modifications and structural reorganizations are distinct commits.

```python
# Regular session for data
session = repo.writable_session("main")
arr[:] = new_data
session.commit("Updated data")

# Rearrange session for structure
session = repo.rearrange_session("main")
session.move("/old/path", "/new/path")
session.commit("Reorganized")
```

## Groups Move with Children

When you move a group, everything inside moves with it:

```python exec="on" session="nodes" source="material-block" result="code"
repo = ic.Repository.create(ic.in_memory_storage())
session = repo.writable_session("main")
root = zarr.group(session.store)
root.create_group("experiments/exp001")
root.create_array("experiments/exp001/results", shape=(10,), dtype="f4")
root.create_array("experiments/exp001/config", shape=(5,), dtype="i4")
session.commit("Create experiment")

print("Before:")
print(root.tree())
```

```python exec="on" session="nodes" source="material-block" result="code"
# Create the destination parent first
session = repo.writable_session("main")
root = zarr.group(session.store)
root.create_group("archive")
session.commit("Create archive group")

session = repo.rearrange_session("main")
session.move("/experiments/exp001", "/archive/exp001")
session.commit("Archive experiment")

root = zarr.open_group(repo.readonly_session(branch="main").store, mode="r")
print("After:")
print(root.tree())
```

## No Overwriting

Moves will not overwrite existing nodes—delete the destination first if needed:

```python exec="on" session="nodes" source="material-block" result="code"
repo = ic.Repository.create(ic.in_memory_storage())
session = repo.writable_session("main")
root = zarr.group(session.store)
root.create_array("source", shape=(10,), dtype="f4")
root.create_array("destination", shape=(10,), dtype="f4")
session.commit("Create arrays")

session = repo.rearrange_session("main")
try:
    session.move("/source", "/destination")
except ic.IcechunkError as e:
    print(f"IcechunkError: {e}")
```

## Async API

```python
session = await repo.rearrange_session_async("main")
await session.move_async("/old/path", "/new/path")
await session.commit_async("Moved node")
```
