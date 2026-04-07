# Operations Log

Who changed what, and when? As repositories grow and multiple collaborators commit, branch, tag, and run maintenance tasks, it's easy to lose track of what happened. Traditional object storage gives you no history of structural changes—you'd need to build your own audit trail.

Icechunk records every repository mutation in an **operations log** (ops log). Each entry captures the operation type, a timestamp, and any relevant details like branch names or snapshot IDs. The log is ordered newest-first and covers the full lifetime of the repository.

See the [`ops_log`][icechunk.repository.Repository.ops_log] API reference for details.

## Reading the Log

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

## Update Types

Every repository mutation creates exactly one log entry. The `kind` field tells you what happened:

### Commits

| Type | Fields | Triggered by |
|------|--------|-------------|
| `UpdateType.NewCommit` | `branch`, `new_snap_id` | Committing a session |
| `UpdateType.CommitAmended` | `branch`, `previous_snap_id`, `new_snap_id` | Committing with amend |
| `UpdateType.NewDetachedSnapshot` | `new_snap_id` | Flushing a session (anonymous snapshot) |

### Branches and Tags

| Type | Fields | Triggered by |
|------|--------|-------------|
| `UpdateType.BranchCreated` | `name` | Creating a branch |
| `UpdateType.BranchDeleted` | `name`, `previous_snap_id` | Deleting a branch |
| `UpdateType.BranchReset` | `name`, `previous_snap_id` | Resetting a branch to a different snapshot |
| `UpdateType.TagCreated` | `name` | Creating a tag |
| `UpdateType.TagDeleted` | `name`, `previous_snap_id` | Deleting a tag |

### Repository Administration

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

## Filtering by Type

Use `isinstance` or pattern matching to find specific operations:

```python exec="on" session="opslog" source="material-block" result="code"
# Find all commits
commits = [
    u for u in repo.ops_log()
    if isinstance(u.kind, ic.UpdateType.NewCommit)
]
for c in commits:
    print(f"Commit on {c.kind.branch} → {c.kind.new_snap_id[:12]}  at {c.updated_at}")
```

With Python 3.10+ pattern matching:

```python
for update in repo.ops_log():
    match update.kind:
        case ic.UpdateType.NewCommit(branch=branch, new_snap_id=snap):
            print(f"Commit on {branch}")
        case ic.UpdateType.BranchCreated(name=name):
            print(f"Branch created: {name}")
        case ic.UpdateType.TagCreated(name=name):
            print(f"Tag created: {name}")
```

## Example: Recent Activity Summary

```python exec="on" session="opslog" source="material-block" result="code"
# Add more activity
repo.create_tag("v1.0", repo.lookup_branch("main"))

session = repo.writable_session("develop")
root = zarr.group(session.store)
root.create_array("pressure", shape=(100,), dtype="f4")
session.commit("Add pressure array")

# Summarize recent activity
for update in repo.ops_log():
    kind = update.kind
    match kind:
        case ic.UpdateType.NewCommit():
            print(f"  commit on '{kind.branch}'")
        case ic.UpdateType.BranchCreated():
            print(f"  branch created: '{kind.name}'")
        case ic.UpdateType.TagCreated():
            print(f"  tag created: '{kind.name}'")
        case ic.UpdateType.RepoInitialized():
            print(f"  repository created")
        case _:
            print(f"  {kind}")
```

## Async API

```python
async for update in repo.ops_log_async():
    print(update.kind, update.updated_at)
```
