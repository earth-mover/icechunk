# Migrate to 2.0


Icechunk 2.0 uses a new storage format (spec version 2). Existing repositories created with Icechunk 1.x can be automatically upgraded to version 2.0 using the Icechunk library.

## Running the migration

```python
import icechunk as ic

# Open the v1 repository
storage = ic.s3_storage(
    bucket="my-bucket",
    prefix="my-repo",
    region="us-east-1",
)
repo = ic.Repository.open(storage)

# First, do a dry run to validate the migration
migrated_repo = ic.upgrade_icechunk_repository(repo, dry_run=True)

# Then run the actual migration
migrated_repo = ic.upgrade_icechunk_repository(repo, dry_run=False)

# Use migrated_repo from here on — the original `repo` object is invalidated
session = migrated_repo.writable_session("main")
```

!!! warning
    This is an administrative operation. It **must be executed in isolation** — no other readers or writers should be accessing the repository during the migration. Any writes made during the migration may be lost.

### Parameters

| Parameter | Default | Description |
|---|---|---|sd
| `repo` | *(required)* | The v1 repository to migrate. |
| `dry_run` | *(required)* | If `True`, validates the migration without making changes. |
| `delete_unused_v1_files` | `True` | Remove legacy v1 metadata files after migration. |
| `prefetch_concurrency` | `64` | Number of snapshots to fetch concurrently. Lower this for environments that cannot fit many snapshots in memory. |

### How it works

The migration is designed to be **safe and reversible** at each step:

1. **Validation** — Confirms the repository is spec version 1 and storage is writable.
2. **Snapshot collection** — Walks the full version history, collecting all branches, tags, and snapshots.
3. **Ops log reconstruction** — Icechunk 2.0 tracks a richer operations log. The migration reconstructs a synthetic history from your existing snapshot graph so that `Repository.ops_log()` works after migration.
4. **Atomic write** — A single new v2 repository info file is written containing all refs, snapshot metadata, configuration, and the reconstructed ops log.
5. **Verification** — The repository is reopened and verified to be spec version 2. If verification fails, the v2 file is deleted and the repository is left unchanged.
6. **Cleanup** — If `delete_unused_v1_files=True`, the legacy v1 ref and config files are removed. If any cleanup step fails, the migration rolls back.

Each step of the process is atomic and if something goes wrong, the repo will be left in a working state.

!!! note
    The migration is usually fast, but can take several minutes for repositories with thousands of snapshots.

### After migration

The original `repo` object is **invalidated** after migration. Any attempt to use it will raise a `RuntimeError`. Always use the new `Repository` object returned by `upgrade_icechunk_repository()`.
