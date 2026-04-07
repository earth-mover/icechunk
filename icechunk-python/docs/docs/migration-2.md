# Migrate to 2.0

Icechunk 2.0 uses a new storage format (spec version 2). Existing repositories created with Icechunk 1.x can be automatically upgraded to version 2.0 using the [`upgrade_icechunk_repository()`](./reference.md#icechunk.upgrade_icechunk_repository) function. This migration only uses  repository metadata, and it will not read or write any chunks.

!!! warning
    This is an administrative operation. It **must be executed in isolation** — no other readers or writers should be accessing the repository during the migration. Any writes made during the migration may be lost.

```python
import icechunk as ic

# Open the v1 repository
storage = ic.s3_storage(
    bucket="my-bucket",
    prefix="my-repo",
    region="us-east-1",
)
repo = ic.Repository.open(storage)

# You can use `dry_run=True` to test the migration process without writing the
# icechunk version 2 specific files. `dry_run=False` will run the migration
# and when complete will not be reversible.
migrated_repo = ic.upgrade_icechunk_repository(repo, dry_run=False)
assert migrated_repo.spec_version == 2

# Use migrated_repo from here on — the original `repo` object is invalidated
session = migrated_repo.writable_session("main")
```

### Parameters

| Parameter | Default | Description |
|---|---|---|
| `repo` | *(required)* | The v1 repository to migrate. |
| `dry_run` | *(required)* | If `True`, it attempts the migration without writing version 2 specific files, leaving the v1 repo in place |
| `delete_unused_v1_files` | `True` | Remove legacy v1 metadata files after migration. |
| `prefetch_concurrency` | `64` | Number of snapshots to fetch concurrently while migrating. Lower this for environments that cannot fit many snapshots in memory. |

### How it works

The migration reads all snapshots pointed to by branches and tags, and collects them into the new version 2 schema with a reconstructed ops log. If successful and `delete_unused_v1_files=True`, the legacy v1 metadata files are removed.

If something goes wrong at any step, the repository is left in a working state.

!!! note
    The migration is usually fast, but can take several minutes for repositories with thousands of snapshots.

### After migration

The original `repo` object is **invalidated** after migration. Any attempt to use it will raise a `RuntimeError`. Always use the new [`Repository`](./reference.md#icechunk.Repository) object returned by [`upgrade_icechunk_repository()`](./reference.md#icechunk.upgrade_icechunk_repository).
