# Changelog

## Python Icechunk Library 0.1.0a14

### Features

- Now each array has its own chunk manifest, speeding up reads for large repositories
- The snapshot now keeps track of the chunk space bounding box for each manifest
- Configuration settings can now be overridden in a field-by-field basis
  Example:
  ```python
   config = icechunk.RepositoryConfig(inline_chunk_threshold_byte=0)
   storage = ...

   repo = icechunk.Repository.open(
       storage=storage,
       config=config,
   )
  ```
  will use 0 for `inline_chunk_threshold_byte` but all other configuration fields will come from
  the repository persistent config. If persistent config is not set, configuration defaults will
  take its place.
- In preparation for on-disk format stability, all metadata files include extensive format information;
  including a set of magic bytes, file type, spec version, compression format, etc.

### Performance

- Zarr's `getsize` got orders of magnitude faster because it's implemented natively and with
  no need of any I/O
- We added several performance benchmarks to the repository
- Better configuration for metadata asset caches, now based on their sizes instead of their number

### Fixes

- `from icechunk import *` no longer fails

## Python Icechunk Library 0.1.0a12

### Features

- New `Repository.reopen` function to ope a repo again, overwriting its configuration and/or virtual chunk container credentials
- Configuration classes are now mutable and easier to use:

  ```python
   storage = ...
   config = icechunk.RepositoryConfig.default()
   config.storage.concurrency.ideal_concurrent_request_size = 1_000_000

   repo = icechunk.Repository.open(
       storage=storage,
       config=config,
   )
- `ancestry` function can now receive a branch/tag name or a snapshot id
- `set_virtual_ref` can now validate the virtual chunk container exists

  ```

### Performance

- Better concurrent download of big chunks, both native and virtual

### Fixes

- We no longer allow `main` branch to be deleted

## Python Icechunk Library 0.1.0a11

### Features

- Adds support for Azure Blob Storage

### Performance

- Manifests now load faster, due to an improved serialization format

### Fixes

- The store now releases the GIL appropriately in multithreaded contexts

## Python Icechunk Library 0.1.0a10

### Features

- Large chunks are fetched concurrently
- `IcechunkStore.list_dir` is now significantly faster
- Support for Zarr 3.0 and xarray 2025.1.1
- Transaction logs and snapshot files are compressed

## Python Icechunk Library 0.1.0a9

### Features

- Manifests compression using Zstd
- Large manifests are fetched using multiple parallel requests
- Functions to fetch and store repository config

### Performance

- Faster `list_dir` and `delete_dir` implementations in the Zarr store

### Fixes

- Credentials from environment in GCS

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
