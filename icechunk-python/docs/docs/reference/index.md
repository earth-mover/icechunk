# Python API Reference

The `icechunk` package provides `Repository` — the main entry point — and a few commonly-used exceptions and utilities directly in the top-level namespace. Everything else is organized into submodules.

```python
import icechunk as ic

# Top-level: Repository, exceptions, utilities
repo = ic.Repository.create(ic.storage.local_filesystem_storage("/tmp/my-repo"))

# Submodules for everything else
config = ic.config.RepositoryConfig(...)
storage = ic.storage.s3_storage(bucket="my-bucket", prefix="my-prefix", from_env=True)
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
| [`icechunk.zarr`](zarr.md) | The Zarr-compatible `IcechunkStore` |
| [`icechunk.virtual`](virtual.md) | Virtual chunk containers |
| [`icechunk.xarray`](xarray.md) | Xarray integration |
| [`icechunk.dask`](dask.md) | Dask integration |

## `icechunk.Repository`

::: icechunk.Repository

## Exceptions

::: icechunk.IcechunkError

::: icechunk.ConflictError

::: icechunk.RebaseFailedError

## Top-level utilities

::: icechunk.print_debug_info

::: icechunk.upgrade_icechunk_repository

::: icechunk.supported_spec_versions
