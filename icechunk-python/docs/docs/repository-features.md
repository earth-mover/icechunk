---
title: Repository Features
---

# Repository Features

## Repository Status

Every Icechunk repository has a `status` that controls its availability. The status is represented by a [`RepoStatus`](./reference/index.md#icechunk.Repository.status) object with three fields:

- `availability` - either `online` (fully available for reads and writes) or `read_only` (available for reads only). See [`RepoAvailability`](./reference/index.md#icechunk.Repository).
- `set_at` - the timestamp when the status was last changed.
- `limited_availability_reason` - an optional human-readable explanation for why the repository is not fully online.

Repository status is useful for coordinating access in multi-user or production environments. For example, you might mark a repository as `read_only` during a maintenance window or data migration to prevent accidental writes.

```python
import icechunk

repo = icechunk.Repository.open(...)

# Check the current status
status = repo.status
print(status.availability)  # RepoAvailability.online

# Set the repository to read-only
repo.set_status(
    icechunk.RepoStatus(
        availability=icechunk.RepoAvailability.read_only,
        limited_availability_reason="Maintenance in progress",
    )
)
```

Icechunk enforces these flags, so creating a writable-session from a Repository marked as read-only will fail.
```
# raises an error
session = repo.writable_session("main")
```

However note that anyone can unset the status at any time.

```
# Restore full access
repo.set_status(
    icechunk.RepoStatus(availability=icechunk.RepoAvailability.online)
)
```

### Repository Metadata

If you manage a number of Icechunk repositories, it may be useful to classify them using metadata.
Icechunk allows you to set and retrieve arbitrary JSON-like metadata at the repository level.

```python
repo = icechunk.Repository.open(...)
repo.set_metadata(dict(test=True, team="science"))
repo.update_metadata(dict(number_of_bugs=42))
print(repo.get_metadata())
```
## Operations Log

Who changed what, and when? As repositories grow and multiple collaborators commit, branch, tag, and run maintenance tasks, it's easy to lose track of what happened. Traditional object storage gives you no history of structural changes—you'd need to build your own audit trail.

Icechunk records every repository mutation in an **operations log** (ops log). Each entry captures the operation type, a timestamp, and any relevant details like branch names or snapshot IDs. The log is ordered newest-first and covers the full lifetime of the repository.


### Reading the Log

```python exec="on" session="opslog" source="material-block" result="code"
import icechunk as ic
import zarr
import numpy as np

# Create a repository and make some changes
repo = ic.Repository.create(ic.in_memory_storage())
session = repo.writable_session("main")
root = zarr.group(session.store)
root.create_array("temperature", shape=(100,), dtype="f4")
session.commit("Add temperature array")

session = repo.writable_session("main")
arr = zarr.open_array(session.store, path="temperature")
arr[:] = np.random.randn(100).astype("f4")
session.commit("Write temperature data")

repo.create_branch("develop", repo.lookup_branch("main"))

for update in repo.ops_log():
    print(f"{update.updated_at}  {update.kind}")
```

Each entry is an [`Update`](reference/ops.md) with three fields:

- `kind` — an [`UpdateType`](reference/ops.md) variant describing what happened
- `updated_at` — a `datetime.datetime` timestamp (UTC, microsecond precision)
- `backup_path` — internal storage detail (you can ignore this)

### Update Types

Every repository mutation creates exactly one log entry. The `kind` field tells you what happened:

#### Commits

| Type | Fields | Triggered by |
|------|--------|-------------|
| `UpdateType.NewCommit` | `branch`, `new_snap_id` | Committing a session |
| `UpdateType.CommitAmended` | `branch`, `previous_snap_id`, `new_snap_id` | Committing with amend |
| `UpdateType.NewDetachedSnapshot` | `new_snap_id` | Flushing a session (anonymous snapshot) |

#### Branches and Tags

| Type | Fields | Triggered by |
|------|--------|-------------|
| `UpdateType.BranchCreated` | `name` | Creating a branch |
| `UpdateType.BranchDeleted` | `name`, `previous_snap_id` | Deleting a branch |
| `UpdateType.BranchReset` | `name`, `previous_snap_id` | Resetting a branch to a different snapshot |
| `UpdateType.TagCreated` | `name` | Creating a tag |
| `UpdateType.TagDeleted` | `name`, `previous_snap_id` | Deleting a tag |

#### Repository Administration

| Type | Fields | Triggered by |
|------|--------|-------------|
| `UpdateType.RepoInitialized` | — | Creating a new repository |
| `UpdateType.ConfigChanged` | — | Saving repository configuration |
| `UpdateType.MetadataChanged` | — | Setting or updating repository metadata |
| `UpdateType.RepoStatusChanged` | `status` | Changing repository status (e.g., read-only) |
| `UpdateType.FeatureFlagChanged` | `id`, `new_value` | Changing a feature flag |
| `UpdateType.GCRan` | — | Running garbage collection |
| `UpdateType.ExpirationRan` | — | Running snapshot expiration |
| `UpdateType.RepoMigrated` | `from_version`, `to_version` | Upgrading from an older format version |
