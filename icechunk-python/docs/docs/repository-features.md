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

# raises an error
session = repo.writable_session("main")

# Restore full access
repo.set_status(
    icechunk.RepoStatus(availability=icechunk.RepoAvailability.online)
)
```
