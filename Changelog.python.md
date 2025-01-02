# Changelog

## Python Icechunk Library 0.1.0a8

### Features

- New Python API using `Repository`, `Session` and `Store` as separate entities
- New Python API for configuring and opening `Repositories`
- Added support for object store credential refresh
- Persistent repository config
- Commit conflict resolution and rebase support
- Added experimental support for Google Cloud Storage
- Add optional checksums for virtual chunks, either using Etag or last-updated-at
- Support for multiple virtual chunk locations using virtual chunk containers concept
- Added function `all_virtual_chunk_locations` to `Session` to retrieve all locations where the repo has data

### Fixes

- Refs were stored in the wrong prefix

## Python Icechunk Library 0.1.0a7

### Fixes

- Allow overwriting existing groups and arrays in Icechunk stores

## Python Icechunk Library 0.1.0a6

### Fixes

- Fixed an error during commits where chunks would get mixed between different arrays

## Python Icechunk Library 0.1.0a5

### Features

- Sync with zarr 3.0b2. The biggest change is the `mode` param on `IcechunkStore` methods has been simplified to `read_only`.
- Changed `IcechunkStore::distributed_commit` to `IcechunkStore::merge`, which now does *not* commit, but attempts to merge the changes from another store back into the current store.
- Added a new `icechunk.dask.store_dask` method to write a dask array to an icechunk store. This is required for safely writing dask arrays to an icechunk store.
- Added a new `icechunk.xarray.to_icechunk` method to write an xarray dataset to an icechunk store. This is *required* for safely writing xarray datasets with dask arrays to an icechunk store in a distributed or multi-processing context.

### Fixes

- The `StorageConfig` methods have been correctly typed.
- `IcechunkStore` instances are now set to `read_only` by default after pickling.
- When checking out a snapshot or tag, the `IcechunkStore` will be set to read-only. If you want to write to the store, you must call `IcechunkStore::set_writable()`.
- An error will now be raised if you try to checkout a snapshot that does not exist.

## Python Icechunk Library 0.1.0a4

### Features

- Added `IcechunkStore::reset_branch` and `IcechunkStore::async_reset_branch` methods to point the head of the current branch to another snapshot, changing the history of the branch

### Fixes

- Zarr metadata will now only include the attributes key when the attributes dictionary of the node is not empty, aligning Icechunk with the python-zarr implementation.

## Python Icechunk Library 0.1.0a2

### Features

- Initial release
