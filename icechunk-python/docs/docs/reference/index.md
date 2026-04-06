# Python API Reference

The `icechunk` package is organized into the following modules:

| Module | Description |
|--------|-------------|
| [`icechunk.config`](config.md) | Repository configuration, manifest settings, compression, caching |
| [`icechunk.conflicts`](conflicts.md) | Conflict detection and resolution |
| [`icechunk.credentials`](credentials.md) | Credential types and factories for S3, GCS, Azure |
| [`icechunk.exceptions`](exceptions.md) | Exception types |
| [`icechunk.ops`](ops.md) | Operation types: updates, garbage collection summaries |
| [`icechunk.session`](session.md) | `ForkSession`, `SessionMode` |
| [`icechunk.snapshots`](snapshots.md) | Snapshot metadata, diffs, manifest file info |
| [`icechunk.storage`](storage.md) | Storage backends and configuration |
| [`icechunk.zarr`](zarr.md) | The Zarr-compatible `IcechunkStore` |
| [`icechunk.virtual`](virtual.md) | Virtual chunk containers |
| [`icechunk.xarray`](xarray.md) | Xarray integration |
| [`icechunk.dask`](dask.md) | Dask integration |

## `icechunk.Repository`

::: icechunk.Repository

## `icechunk.Session`

::: icechunk.session.Session

## Top-level utilities

::: icechunk.print_debug_info

::: icechunk.upgrade_icechunk_repository

::: icechunk.supported_spec_versions
