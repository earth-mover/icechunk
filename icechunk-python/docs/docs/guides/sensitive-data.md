# Storing Sensitive Data

!!! warning "Not legal advice"

    This page is engineering guidance, not legal advice. Whether a given design satisfies any particular regulation (HIPAA, GDPR, CCPA, and so on) is a determination for your organization and its counsel.

Icechunk is a strong foundation for storage of regulated and sensitive datasets — medical, financial, and personal. Its versioned history gives you a complete audit trail of every change, its storage inherits the encryption-at-rest and access controls of your object store (S3, GCS, Azure Blob), and its [expiration and garbage collection](../understanding/expiration.md) allow you to permanently delete data.

## What can be deleted

However, not all (meta)data stored in Icechunk are permanently deletable. This is a design requirement to ensure that history, rebases, and other core functions work properly.

### Deletable

- Array values are built to be deleted. You overwrite or delete an array's data in a new commit, then [expire and garbage-collect](#how-to-delete) to purge the old values from history.
- Paths, attributes, and zarr metadata in snapshots are removed only when you expire and then garbage-collect the snapshots that contain them.

### Not Deletable

- Branch and tag names, commit messages, commit metadata, and the operation log are not removed by garbage collection. They live in the repo info file and its backups.
- [Transaction logs](../reference/spec-v2-1.md#transaction-log-files) cannot be fully removed if they are part of the branch's history ([design doc 16](https://github.com/earth-mover/icechunk/blob/main/design-docs/016-expired-transaction-logs.md)), so any [`move`](moving-nodes.md) operation can result in node path names being permanently persisted in storage.

## What to do

Sensitive data should **only** ever be stored in the array values. **Do not** put sensitive values in array or group names, attributes, dimension names, commit messages, commit metadata, or branch and tag names.

Address records by **position**, not by name. Give each record an index along a dimension and store every field, including identifiers, as array values at that index. If you need to map a real-world identity to an index, store that map as array values too (so it is also deletable), or keep it in a separate system. Never put it in a node name. To delete a record, overwrite its slice and [expire the history](#how-to-delete).

=== "✅ Do"

    ```python
    import zarr

    # Each participant occupies one index `i` along the sample dimension.
    # Identifiers and data are array VALUES, addressed by position.
    group = zarr.open_group(session.store)
    full_name = group.create_array("full_name", shape=(n,), dtype="<U128")
    date_of_birth = group.create_array("date_of_birth", shape=(n,), dtype="<U10")
    national_id = group.create_array("national_id", shape=(n,), dtype="<U64")
    blood_pressure = group.create_array("blood_pressure", shape=(n, t), dtype="f4")

    full_name[i] = "Jane Doe"
    date_of_birth[i] = "1985-03-12"
    national_id[i] = "AB12345"
    blood_pressure[i] = reading
    session.commit("Add participant data")
    ```

=== "❌ Don't"

    ```python
    group = zarr.open_group(session.store)
    # Names: baked into every snapshot's structure and awkward to scrub.
    group.create_group("patient/Jane_Doe_1985-03-12")
    # Attributes: live in the snapshot.
    arr.attrs["full_name"] = "Jane Doe"
    # Commit messages: part of the immutable audit trail.
    session.commit("Add records for Jane Doe, national id AB12345")
    # Branch and tag names: live in the repo info file and ops log,
    # which garbage collection never removes.
    repo.create_tag("delivered-to-jane-doe", snapshot_id=snap)
    ```

## How to delete

!!! danger "Expiration and garbage collection are irreversible"

    Both permanently delete data. That is the point, but there is no undo — verify your cutoff and your references first.

Say participant `i` from the [example above](#what-to-do) asks to be removed. Deletion has two stages: overwrite the data so the current state no longer holds it, then expire and garbage-collect the history that still does.

**1. Overwrite the data and commit.** Replace the participant's slice with fill values, on every branch where it appears:

```python
import numpy as np

session = repo.writable_session("main")
group = zarr.open_group(session.store)
group["full_name"][i] = ""
group["date_of_birth"][i] = ""
group["national_id"][i] = ""
group["blood_pressure"][i] = np.nan
removal_snapshot = session.commit(f"Remove participant at index {i}")
```

The branch tip no longer references the old values, but earlier snapshots still do, so the chunks that held them remain in storage.

**2. Expire the old history and garbage-collect.** Expiration removes snapshots older than a cutoff. Garbage collection then deletes every chunk, snapshot, and manifest that no surviving snapshot references.

It is not enough to remove only the commit that added the participant. Each commit writes only the chunks that changed, but every snapshot references the full set of chunks that make up the array at that point, including chunks written by earlier commits. So the chunk holding the participant's data is referenced by every snapshot from the one that added it through to the overwrite commit. To free that chunk, all of those snapshots must become unreachable.

Use the removal commit's timestamp as the cutoff. Expiration removes a contiguous run of history: every snapshot older than the cutoff, back to the initial commit (which is never expired). You cannot expire only the range that holds the participant's data and keep older history that predates it, so this also drops unrelated snapshots older than the one that added the participant. That is the cost of guaranteed deletion ([icechunk#2193](https://github.com/earth-mover/icechunk/issues/2193) tracks making this more granular).

Two things to know:

- A [branch or tag](../understanding/concepts.md#branches-and-tags) pointing at an expired snapshot keeps it (and its chunks) reachable, so garbage collection will not remove them. The `delete_expired_branches=True` and `delete_expired_tags=True` arguments above handle this: they delete any branch or tag left pointing only at expired snapshots, so garbage collection can remove the data. A branch whose tip is newer than the cutoff is kept, so overwrite the participant's slice on every branch (step 1) before expiring.
- Expiration is by time, not by item: it removes whole snapshots, not individual values. See [Expiring Data](../understanding/expiration.md) for the full mechanics.

```python
# Expiring everything older than the removal commit drops the snapshot that
# added the participant, every later one that carried the data forward, and
# all earlier history back to the initial commit.
cutoff = repo.lookup_snapshot(removal_snapshot).written_at
repo.expire_snapshots(
    older_than=cutoff,
    delete_expired_branches=True,
    delete_expired_tags=True,
)
summary = repo.garbage_collect(cutoff)
```

The participant's data is now permanently deleted. The chunks that held it have been removed from the object store, and no snapshot in the repository references them, so there is no Icechunk operation that can bring it back.

### Confirm the deletion

`garbage_collect` reports what it removed, and the expired snapshots no longer appear in the repository's history:

```python
print(f"deleted {summary.chunks_deleted} chunks, {summary.bytes_deleted} bytes")

# The snapshots that held the data are gone; only survivors remain.
for snap in repo.ancestry(branch="main"):
    print(snap.id, snap.written_at)
```

Because the snapshots and their chunks have been deleted from storage, reading or checking out an expired snapshot now fails.
