# Version Control

Icechunk carries over concepts from other version control software (e.g. Git) to multidimensional arrays. Doing so helps ease the burden of managing multiple versions of your data, and helps you be precise about which version of your dataset is being used for downstream purposes.

Core concepts of Icechunk's version control system are:

- A snapshot bundles together related data and metadata changes in a single "transaction".
- A branch points to the latest snapshot in a series of snapshots. Multiple branches can co-exist at a given time, and multiple users can add snapshots to a single branch. One common pattern is to use dev, stage, and prod branches to separate versions of a dataset.
- A tag is an immutable reference to a snapshot, usually used to represent an "important" version of the dataset such as a release.

Snapshots, branches, and tags all refer to specific versions of your dataset. You can time-travel/navigate back to any version of your data as referenced by a snapshot, a branch, or a tag using a snapshot ID, a branch name, or a tag name when creating a new `Session`.

## Setup

To get started, we can create a new `Repository`.

!!! note

    This example uses an in-memory storage backend, but you can also use any other storage backend instead.

```python exec="on" session="version" source="material-block"
import icechunk

repo = icechunk.Repository.create(icechunk.in_memory_storage())
```

On creating a new [`Repository`](../reference/#icechunk.Repository), it will automatically create a `main` branch with an initial snapshot. We can take a look at the ancestry of the `main` branch to confirm this.

```python
for ancestor in repo.ancestry(branch="main"):
    print(ancestor)
```

!!! note

    The [`ancestry`](./reference/#icechunk.Repository.ancestry) method can be used to inspect the ancestry of any branch, snapshot, or tag.

We get back an iterator of [`SnapshotInfo`](../reference/#icechunk.SnapshotInfo) objects, which contain information about the snapshot, including its ID, the ID of its parent snapshot, and the time it was written.

## Creating a snapshot

Now that we have a `Repository` with a `main` branch, we can modify the data in the repository and create a new snapshot. First we need to create a writable from the `main` branch.

!!! note

    Writable `Session` objects are required to create new snapshots, and can only be created from the tip of a branch. Checking out tags or other snapshots is read-only.

```python exec="on" session="version" source="material-block"
session = repo.writable_session("main")
```

We can now access the `zarr.Store` from the `Session` and create a new root group. Then we can modify the attributes of the root group and create a new snapshot.

```python exec="on" session="version" source="material-block" result="code"
import zarr

root = zarr.group(session.store)
root.attrs["foo"] = "bar"
print(session.commit(message="Add foo attribute to root group"))
```

Success! We've created a new snapshot with a new attribute on the root group.

Once we've committed the snapshot, the `Session` will become read-only, and we can no longer modify the data using our existing `Session`. If we want to modify the data again, we need to create a new writable `Session` from the branch. Notice that we don't have to refresh the `Repository` to get the updates from the `main` branch. Instead, the `Repository` will automatically fetch the latest snapshot from the branch when we create a new writable `Session` from it.

```python exec="on" session="version" source="material-block"
session = repo.writable_session("main")
root = zarr.group(session.store)
root.attrs["foo"] = "baz"
print(session.commit(message="Update foo attribute on root group"))
```

With a few snapshots committed, we can take a look at the ancestry of the `main` branch:


```python exec="on" session="version" source="material-block" result="code"
for snapshot in repo.ancestry(branch="main"):
    print(snapshot)
```

Visually, this looks like below, where the arrows represent the parent-child relationship between snapshots.

```python exec="1" result="mermaid" session="version"
print("""
gitGraph
    commit id: "{}" type: NORMAL
    commit id: "{}" type: NORMAL
    commit id: "{}" type: NORMAL
""".format(*[snap.id[:6] for snap in repo.ancestry(branch="main")]))
```


## Time Travel

Now that we've created a new snapshot, we can time-travel back to the previous snapshot using the snapshot ID.

!!! note

    It's important to note that because the `zarr Store` is read-only, we need to pass `mode="r"` to the `zarr.open_group` function.

```python exec="on" session="version" source="material-block" result="code"
session = repo.readonly_session(snapshot_id=list(repo.ancestry(branch="main"))[1].id)
root = zarr.open_group(session.store, mode="r")
print(root.attrs["foo"])
```

## Branches

If we want to modify the data from a previous snapshot, we can create a new branch from that snapshot with [`create_branch`](../reference/#icechunk.Repository.create_branch).

```python exec="on" session="version" source="material-block"
main_branch_snapshot_id = repo.lookup_branch("main")
repo.create_branch("dev", snapshot_id=main_branch_snapshot_id)
```

We can now create a new writable `Session` from the `dev` branch and modify the data.

```python exec="on" session="version" source="material-block" result="code"
session = repo.writable_session("dev")
root = zarr.group(session.store)
root.attrs["foo"] = "balogna"
print(session.commit(message="Update foo attribute on root group"))
```

We can also create a new branch from the tip of the `main` branch if we want to modify our current working branch without modifying the `main` branch.

```python exec="on" session="version" source="material-block" result="code"
repo.create_branch("feature", snapshot_id=main_branch_snapshot_id)

session = repo.writable_session("feature")
root = zarr.group(session.store)
root.attrs["foo"] = "cherry"
print(session.commit(message="Update foo attribute on root group"))
```

With these branches created, the hierarchy of the repository now looks like below. 

```python exec="on" result="mermaid" session="version"
main_commits = [s.id[:6] for s in list(repo.ancestry(branch='main'))]
dev_commits = [s.id[:6] for s in list(repo.ancestry(branch='dev'))]
feature_commits = [s.id[:6] for s in list(repo.ancestry(branch='feature'))]
print(
"""
gitGraph
    commit id: "{}" type: NORMAL
    commit id: "{}" type: NORMAL
    branch dev
    checkout dev
    commit id: "{}" type: NORMAL

    checkout main
    commit id: "{}" type: NORMAL

    checkout main
    branch feature
    commit id: "{}" type: NORMAL
    
""".format(*[main_commits[-2], main_commits[-1], dev_commits[0], main_commits[0],feature_commits[0]])
)
```

We can also [list all branches](../reference/#icechunk.Repository.list_branches) in the repository.

```python exec="on" session="version" source="material-block" result="code"
print(repo.list_branches())
```

If we need to find the snapshot that a branch is based on, we can use the [`lookup_branch`](../reference/#icechunk.Repository.lookup_branch) method.

```python exec="on" session="version" source="material-block" result="code"
print(repo.lookup_branch("feature"))
```

We can also [delete a branch](../reference/#icechunk.Repository.delete_branch) with [`delete_branch`](../reference/#icechunk.Repository.delete_branch).

```python exec="on" session="version" source="material-block"
repo.delete_branch("feature")
```

Finally, we can [reset a branch](../reference/#icechunk.Repository.reset_branch) to a previous snapshot with [`reset_branch`](../reference/#icechunk.Repository.reset_branch). This immediately modifies the branch tip to the specified snapshot, changing the history of the branch.

```python exec="on" session="version" source="material-block"
repo.reset_branch("dev", snapshot_id=main_branch_snapshot_id)
```

## Tags

Tags are immutable references to a snapshot. They are created with [`create_tag`](../reference/#icechunk.Repository.create_tag).

For example to tag the second commit in `main`'s history:

```python exec="on" session="version" source="material-block"
repo.create_tag("v1.0.0", snapshot_id=list(repo.ancestry(branch="main"))[1].id)
```


Because tags are immutable, we need to use a readonly `Session` to access the data referenced by a tag.

```python exec="on" session="version" source="material-block" result="code"
session = repo.readonly_session(tag="v1.0.0")
root = zarr.open_group(session.store, mode="r")
print(root.attrs["foo"])
```

```python exec="1" result="mermaid" session="version"
print("""
gitGraph
    commit id: "{}" type: NORMAL
    commit id: "{}" type: NORMAL
    commit tag: "v1.0.0"
    commit id: "{}" type: NORMAL
""".format(*[snap.id[:6] for snap in repo.ancestry(branch="main")]))
```

We can also [list all tags](../reference/#icechunk.Repository.list_tags) in the repository.

```python exec="on" session="version" source="material-block" result="code"
print(repo.list_tags())
```

and we can look up the snapshot that a tag is based on with [`lookup_tag`](../reference/#icechunk.Repository.lookup_tag).

```python exec="on" session="version" source="material-block" result="code"
print(repo.lookup_tag("v1.0.0"))
```

And then finally delete a tag with [`delete_tag`](../reference/#icechunk.Repository.delete_tag).

!!! note
    Tags are immutable and once a tag is deleted, it can never be recreated.

```python exec="on" session="version" source="material-block"
repo.delete_tag("v1.0.0")
```

## Conflict Resolution

Icechunk is a serverless distributed system, and as such, it is possible to have multiple users or processes modifying the same data at the same time. Icechunk relies on the consistency guarantees of the underlying storage backends to ensure that the data is always consistent. In situations where two users or processes attempt to modify the same data at the same time, Icechunk will detect the conflict and raise an exception at commit time. This can be illustrated with the following example.

Let's create a fresh repository, add some attributes to the root group and create an array named `data`.

```python exec="on" session="version" source="material-block" result="code"
import icechunk
import numpy as np
import zarr

repo = icechunk.Repository.create(icechunk.in_memory_storage())
session = repo.writable_session("main")
root = zarr.group(session.store)
root.attrs["foo"] = "bar"
root.create_dataset("data", shape=(10, 10), chunks=(1, 1), dtype=np.int32)
print(session.commit(message="Add foo attribute and data array"))
```

Lets try to modify the `data` array in two different sessions, created from the `main` branch.

```python exec="on" session="version" source="material-block"
session1 = repo.writable_session("main")
session2 = repo.writable_session("main")

root1 = zarr.group(session1.store)
root2 = zarr.group(session2.store)

root1["data"][0,0] = 1
root2["data"][0,:] = 2
```

and then try to commit the changes.

```python
print(session1.commit(message="Update first element of data array"))
print(session2.commit(message="Update first row of data array"))

# AE9XS2ZWXT861KD2JGHG
# ---------------------------------------------------------------------------
# ConflictError                             Traceback (most recent call last)
# Cell In[7], line 11
#      8 root2.attrs["foo"] = "baz"
#      10 print(session1.commit(message="Update foo attribute on root group"))
# ---> 11 print(session2.commit(message="Update foo attribute on root group"))

# File ~/Developer/icechunk/icechunk-python/python/icechunk/session.py:224, in Session.commit(self, message, metadata)
#     222     return self._session.commit(message, metadata)
#     223 except PyConflictError as e:
# --> 224     raise ConflictError(e) from None

# ConflictError: Failed to commit, expected parent: Some("BG0W943WSNFMMVD1FXJ0"), actual parent: Some("AE9XS2ZWXT861KD2JGHG")
```

The first session was able to commit successfully, but the second session failed with a [`ConflictError`](../reference/#icechunk.ConflictError). When the second session was created, the changes made were relative to the tip of the `main` branch, but the tip of the `main` branch had been modified by the first session.

To resolve this conflict, we can use the [`rebase`](../reference/#icechunk.Session.rebase) functionality.

### Rebasing

To update the second session so it is based off the tip of the `main` branch, we can use the [`rebase`](../reference/#icechunk.Session.rebase) method.

First, we can try to rebase, without merging any conflicting changes:

```python
session2.rebase(icechunk.ConflictDetector())

# ---------------------------------------------------------------------------
# RebaseFailedError                         Traceback (most recent call last)
# Cell In[8], line 1
# ----> 1 session2.rebase(icechunk.ConflictDetector())

# File ~/Developer/icechunk/icechunk-python/python/icechunk/session.py:247, in Session.rebase(self, solver)
#     245     self._session.rebase(solver)
#     246 except PyRebaseFailedError as e:
# --> 247     raise RebaseFailedError(e) from None

# RebaseFailedError: Rebase failed on snapshot AE9XS2ZWXT861KD2JGHG: 1 conflicts found
```

This however fails because both sessions modified metadata. We can use the `RebaseFailedError` to get more information about the conflict.

```python
try:
    session1.rebase(icechunk.ConflictDetector())
except icechunk.RebaseFailedError as e:
    for conflict in e.conflicts:
        print(f"Conflict at {conflict.path}: {conflict.conflicted_chunks}")

# Conflict at /data: [[0, 0]]
```

We get a clear indication of the conflict, and the chunks that are conflicting. In this case we have decided that the first session's changes are correct, so we can again use the [`BasicConflictSolver`](../reference/#icechunk.BasicConflictSolver) to resolve the conflict.

```python
session1.rebase(icechunk.BasicConflictSolver(on_chunk_conflict=icechunk.VersionSelection.UseOurs))
session1.commit(message="Update first element of data array")

# 'R4WXW2CYNAZTQ3HXTNK0'
```

Success! We have now resolved the conflict and committed the changes.

Let's look at the value of the `data` array to confirm that the conflict was resolved correctly.

```python
session = repo.readonly_session("main")
root = zarr.open_group(session.store, mode="r")
root["data"][0,:]

# array([1, 2, 2, 2, 2, 2, 2, 2, 2, 2], dtype=int32)
```

As you can see, `readonly_session` accepts a string for a branch name, or you can also write:

```python
session = repo.readonly_session(branch="main")
```

Lastly, if you make changes to non-conflicting chunks or attributes, you can rebase without having to resolve any conflicts.

```python
session1 = repo.writable_session("main")
session2 = repo.writable_session("main")

root1 = zarr.group(session1.store)
root2 = zarr.group(session2.store)

root1["data"][3,:] = 3
root2["data"][4,:] = 4

session1.commit(message="Update fourth row of data array")

try:
    session2.rebase(icechunk.ConflictDetector())
    print("Rebase succeeded")
except icechunk.RebaseFailedError as e:
    print(e.conflicts)

session2.commit(message="Update fifth row of data array")

# Rebase succeeded
```

And now we can see the data in the `data` array to confirm that the changes were committed correctly.

```python
session = repo.readonly_session(branch="main")
root = zarr.open_group(session.store, mode="r")
root["data"][:,:]

# array([[1, 2, 2, 2, 2, 2, 2, 2, 2, 2],
#        [0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
#        [0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
#        [3, 3, 3, 3, 3, 3, 3, 3, 3, 3],
#        [4, 4, 4, 4, 4, 4, 4, 4, 4, 4],
#        [0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
#        [0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
#        [0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
#        [0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
#        [0, 0, 0, 0, 0, 0, 0, 0, 0, 0]], dtype=int32)
```

#### Limitations

At the moment, the rebase functionality is limited to resolving conflicts with chunks in arrays. Other types of conflicts are not able to be resolved by icechunk yet and must be resolved manually.
