---
title: Repository Features
---

# Repository Features

## Repository Status

Every Icechunk repository has a `status` that controls its availability. The status is represented by a [`RepoStatus`](./reference.md#icechunk.RepoStatus) object with three fields:

- `availability` - either `online` (fully available for reads and writes) or `read_only` (available for reads only). See [`RepoAvailability`](./reference.md#icechunk.RepoAvailability).
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
