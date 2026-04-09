# Python API Reference

The `icechunk` package provides `Repository` — the main entry point — and a few commonly-used exceptions and utilities directly in the top-level namespace. Everything else is organized into submodules.

```python
import icechunk as ic

# Top-level: Repository, storage factories, exceptions, utilities
repo = ic.Repository.create(ic.s3_storage(bucket="my-bucket", prefix="my-prefix", from_env=True))

# Submodules for everything else
config = ic.config.RepositoryConfig(...)
solver = ic.conflicts.BasicConflictSolver(...)
```

## Submodules

| Module | Description |
|--------|-------------|
| [`icechunk.config`](config.md) | Repository configuration, manifest settings, compression, caching |
| [`icechunk.conflicts`](conflicts.md) | Conflict detection and resolution |
| [`icechunk.credentials`](credentials.md) | Credential types and factories for S3, GCS, Azure |
| [`icechunk.ops`](ops.md) | Operation types: updates, garbage collection summaries |
| [`icechunk.session`](session.md) | Sessions for reading and writing data |
| [`icechunk.snapshots`](snapshots.md) | Snapshot metadata, diffs, manifest file info |
| [`icechunk.storage`](storage.md) | Storage backends and configuration |
| [`icechunk.virtual`](virtual.md) | Virtual chunk containers |
| [`icechunk.xarray`](xarray.md) | Xarray integration |
| [`icechunk.dask`](dask.md) | Dask integration |

## Top-level API

The following classes, exceptions, and utilities are available directly in the `icechunk` namespace and are not part of any submodule.

| Name | Kind | Description |
|------|------|-------------|
| [`Repository`](#icechunkrepository) | class | Main entry point for creating and opening repositories |
| [`IcechunkStore`](#icechunkicechunkstore) | class | Zarr-compatible store backed by an Icechunk session |
| [`IcechunkError`](#icechunkerror) | exception | Base exception for Icechunk errors |
| [`ConflictError`](#conflicterror) | exception | Raised on conflicting concurrent writes |
| [`RebaseFailedError`](#rebasefailederror) | exception | Raised when a rebase cannot be completed |
| [`print_debug_info`](#print_debug_info) | function | Print versions of icechunk and related packages |
| [`upgrade_icechunk_repository`](#upgrade_icechunk_repository) | function | Migrate a repository to the latest spec version |
| [`supported_spec_versions`](#supported_spec_versions) | function | List supported spec versions |

---

## `icechunk.Repository`

::: icechunk.Repository

## `icechunk.IcechunkStore`

::: icechunk.store.IcechunkStore

## Exceptions

::: icechunk.IcechunkError

::: icechunk.ConflictError

::: icechunk.RebaseFailedError

## Top-level utilities

::: icechunk.print_debug_info

::: icechunk.upgrade_icechunk_repository

::: icechunk.supported_spec_versions
