# Changelog

## Python Icechunk Library 1.1.13

### Features

- Add support for requester pays in S3: `s3_storage(..., requester_pays=True)`.
- Add support for network proxy when using S3 compatible storage, using
  environment variables:
  - `HTTPS_PROXY` (this is probably the one you want),
  - `HTTP_PROXY`,
  - `ALL_PROXY`,
  - `NO_PROXY`

## Python Icechunk Library 1.1.12

### Features

- Added `RepositoryConfig.merge` method.

### Fixes

- Virtual chunk containers now accept URLs starting with `gs://`, as well as ones starting with `gcs://`.

## Python Icechunk Library 1.1.11

### Features

- New function `azure_store` and added support for virtual chunks in Azure.
  Virtual chunks must have URLs starting with `azure://`, `az://` or `abfs://`.

## Python Icechunk Library 1.1.10

### Fixes

- Internal testing and documentation build fixes
- Compatibility with `Xarray<2025.06.0`.

## Python Icechunk Library 1.1.9

### Features

- Add `authorized_virtual_container_prefixes` to `Repository` to access the prefixes currently whitelisted for virtual chunk access

### Fixes

- Improvements for `anonymous` access to GCS Storage

### Breaking Changes

- This release removes the `windows x86` wheels from the PyPI release.

## Python Icechunk Library 1.1.8

### Features

- Support for `anonymous` access to GCS Storage

## Python Icechunk Library 1.1.7

### Features

- New `Session.flush` method allows creating new snapshots without updating the current branch.
  This is useful to store temporary updates, that can later be made permanent by pointing
  a tag to them (`Repository.create_tag`), or a new branch (`Repository.create_branch`), or
  an existing branch (`Repository.reset_branch`).
- Added `from_snapshot_id` argument to `reset_branch`. This allows to safely reset a branch,
  conditionally on its current tip snapshot.
- Added support for `align_chunks` and `split_every` arguments in `to_icechunk`.

### Performance

- `Store.list_dir` is more than an order of magnitude faster in repositories with
  thousands of groups/arrays. `Store.list_prefix` is also faster.
- Increased default snapshot cache size to 500k groups/arrays. This is a better default as we see
  people creating larger Icechunk repositories. Of course, this can be modified using
  [`icechunk.CachingConfig`](https://icechunk.io/en/latest/configuration/#caching).

### Fixes

- `S3Options` getters and setters added to interface stub file for proper type checking.

## Python Icechunk Library 1.1.6

### Features

- `Storage.new_tigris` now uses the new Tigris endpoint `t3.storage.dev` by default.
- Repositories in Tigris object store now use the
  `X-Tigris-Consistent` header for better consistency.

### Fixes

- `ForkSession` can now be used without serializing it.

### Performance

- Significant speed improvements for `Store.list_prefix` and
  `Store.getsize_prefix` when prefix is not empty.

## Python Icechunk Library 1.1.5

### Features

- Add synchronous `Repository.reopen()` method. There was previously only an async version `Repository.reopen_async()`.
- S3 Storage now uses environment variables `HTTP_PROXY`, `HTTPS_PROXY`, and `NO_PROXY`.
- Emit a warning if `icechunk` is misspelled when configuring logs.
- Better Ceph Object Gateway compatibility.

## Python Icechunk Library 1.1.4

### Fixes

- Msgpack format error when trying to pickle distributed stores in repositories with
  virtual chunk containers.

## Python Icechunk Library 1.1.3

### Fixes

- Fix issue retrieving groups and arrays with certain special
  characters in their names

## Python Icechunk Library 1.1.2

### Features

- New `config` getter in `Session` objects to get the repository configuration.
- Per-repo network concurrency limit. The zarr-python + dask + Icechunk stack
  can be too eager in launching concurrent HTTP requests. This can overload
  the machine, timing out some requests and producing an error. There is a new
  `max_concurrent_requests` field in `RepositoryConfig` to limit this. This will
  limit concurrent HTTP requests to this value. Default is 256. See the [docs](https://icechunk.io/en/stable/performance/) for more.
- Ability to tune and disable network stream stall detection. `s3_storage` like
  functions now take an optional `network_stream_timeout_seconds` with a 60
  seconds default. If configured to 0, stream stall detection is disabled
  completely.

## Python Icechunk Library 1.1.1

### Features

- New `Repository.inspect_snapshot` method returns a JSON representation
  of a snapshot, including its properties and the manifests it points to.

### Fixes

- Improved credential refresh logic, avoiding refresh timeouts.

## Python Icechunk Library 1.1.0

### Features

- Icechunk has an [asynchronous API](https://icechunk.io/en/latest/async/) now.
  - Icechunk internals continue to be fully asynchronous. Most "normal" use cases
    don't need the async API, the synchronous API will deliver the same performance.
  - The async API is useful to get optimal concurrency in operations involving
    multiple repos or multiple sessions. An example would be users who run Icechunk
    in the context of a service accessing multiple repositories.
  - Not every method in Icechunk has an async version, only those that can benefit because they do I/O.
  - The new methods have the same name as they synchronous ones with an `_async` suffix.
    They can be invoked on the same instances as usual.
  - Some Examples:
    - `Repository.create_async()`
    - `Repository.open_async()`
    - `Repository.garbage_collect_async()`
    - `Repository.total_chunks_storage_async()`
    - `Repository.lookup_tag_async()`
    - `Repository.readonly_session_async()`
    - `Repository.writable_session_async()`
    - `Session.commit_async()`
    - `Session.rebase_async()`
    - There are many more, check the [API reference](https://icechunk.io/en/latest/reference/)
- Icechunk default log level is `warn` now, instead of `error`.
- Emit a log warning and recommendation when manifests are too large for the configured cache
  size, which makes Icechunk less performant.
- Add property accessors to `ManifestFileInfo`

### Performance

- We increased the size of the default asset caches
  - Snapshots nodes: 10k -> 30k
  - Chunk references: 5M  -> 15M

### Fixes

- Validate urls on `set_virtual_ref`

### API Breaking Changes

There are two minor API breaking changes that will affect only virtual dataset users:

- To improve security, the `url_prefix` of virtual chunk containers must be declared with a final `/`
  character now. This protects, for example, users from authorizing access to
  `foo` prefix and inadvertently authorize access to `foo-production`.
- `set_virtual_ref` and `set_virtual_refs` now default to `validate_container = True`. This improves
  usability for repository writers, with an early error when they forget to create their virtual
  chunk containers.

## Python Icechunk Library 1.0.3

### Fixes

- Bad byte range addressing for inlined chunks in sharded arrays.

## Python Icechunk Library 1.0.2

### Performance

- Significantly improved write performance in low-latency object stores,
  for single-threaded concurrent tasks.

### Fixes

- Fix issue where some GCS and Azure credentials could not be pickled

## Python Icechunk Library 1.0.1

### Features

- Garbage collection can now be executed in dry run mode with
  `Repository.garbage_collection(..., dry_run=True)`.
- Support for Ceph Object Gateway.
- Better request retries for GCS object store.

### Fixes

- Better error message when local clock has drifted too far.
- Various documentation improvements and fixes.

### Performance

- Significant improvements to performance of `Repository.garbage_collection` and
  `Repository.total_chunks_storage`. Both methods now accept optional arguments for
  parallelization. Performance has improved 30x for some real world datasets
  with many manifests.

## Python Icechunk Library 1.0.0

Icechunk 1.0 is here! This version represents our commitment to stability, performance, and reliability.

Whether you're processing satellite imagery, running ML pipelines on massive datasets, or building the next generation of scientific computing applications, Icechunk provides the transactional, versioned, cloud-native storage layer you need.

## Python Icechunk Library 1.0.0rc0

This is a release candidate for Icechunk 1.0

This version has some minor API changes but it is on-disk format compatible with any versions in the 0.2.x series. Repositories written with previous 0.2.x versions are fully compatible, with repositories
written with this release candidate.

Changes to code may be needed for users that are using distributed coordinated sessions or virtual datasets.
Please refer to our [migration guide](https://icechunk.io/en/latest/icechunk-python/migration/).

### Features

- New [`Repository.transaction`](https://icechunk.io/en/latest/icechunk-python/reference/#icechunk.Repository.transaction)
context manager creates and commits write sessions upon exit.
- Easier manifest preload and split configuration, combining conditions with `|` and `&` magic methods.
- More secure and explicit control over virtual chunk resolution. This improvement motivated the changes
in API for [`Repository.open`](https://icechunk.io/en/latest/icechunk-python/reference/#icechunk.Repository.open) and
[`Repository.create`](https://icechunk.io/en/latest/icechunk-python/reference/#icechunk.Repository.create) for virtual datasets.

### Fixes

- Data loss under certain conditions when distributed sessions are started with a "dirty" session.
This issue motivated the change on API introducing `Session.fork`.

## Python Icechunk Library 0.2.18

### Features

- Implement new Zarr method: `Store.with_read_only`.

### Fixes

- Update Spec documentation to the modern on-disk format based on flatbuffers.

## Python Icechunk Library 0.2.17

This is a feature packed released, and yet, the most important change is in the
performance section. We have put a lot of work implementing manifest split, which
significantly improves performance for very large arrays.

### Features

- Virtual chunks can now be resolved using HTTP(s) protocol. This extends virtual datasets
  that can now be stored outside of an object store.
- Users can now configure how they want to retry failed requests to the object store. See
  [StorageRetriesSettings](https://icechunk.io/en/latest/icechunk-python/reference/#icechunk.StorageRetriesSettings).
- Support dynamically changing log levels. See [`set_logs_filter` function](https://icechunk.io/en/latest/icechunk-python/reference/#icechunk.set_logs_filter).
- Support for anonymous access to GCS buckets.
- The first snapshot has a known id now. This allows to check if a directory is an Icechunk
  repo by looking for the `1CECHNKREP0F1RSTCMT0` path in the `snapshots` subdir.

### Performance

- Large arrays can now have multiple manifests, significantly improving memory usage,
  read, and write time performance. See the relevant section in the
  [documentation](https://icechunk.io/en/latest/icechunk-python/performance/#splitting-manifests).

### Fixes

- GCS sessions that use bearer token can now be serialized.
- Log error when deletion fails during garbage collection.

## Python Icechunk Library 0.2.16

### Features

- Multipart upload for objects > 100MB on Icechunk-native Storage instances.
  `object_store` Storage instances are not supported yet.

### Fixes

- Compatibility with `dask>=2025.4.1`

## Python Icechunk Library 0.2.15

### Features

- `Session.commit` can do `rebase` now, by passing a `rebase_with` argument.

### Performance

- `get_credentials` functions can now be instructed to scatter the first set of credentials
  to all pickled copies of the repo or session. This speeds up short-lived distributed tasks,
  that no longer need to execute the first call to `get_credentials`.

## Python Icechunk Library 0.2.14

### Fixes

- Honor Storage settings during repository configuration update

## Python Icechunk Library 0.2.13

### Features

- Object's storage class can now be configured for S3-compatible repos

### Performance

- Fix a performance regression in writes to GCS and local file storage

## Python Icechunk Library 0.2.12

### Features

- Distributed writes now can use Dask's native reduction.
- Disallow creating tags/branches pointing to non-existing snapshots.
- `SnapshotInfo` now contains information about the snapshot manifests.
- More logging for garbage collection and expiration.

### Fixes

- Fix type annotations for the `Diff` type.

## Python Icechunk Library 0.2.11

### Features

- Extra commit metadata can now optionally be set on the repository itself.
  Useful for properties such as commit author.
- New `Repository.lookup_snapshot` helper method.
- Garbage collection and expiration produce logs now.
- More aggressive commit squashing during snapshot expiration.
- Garbage collection cleans the assets cache, so the same repository can be reused after GC.

### Fixes

- Bug in snapshot expiration that created a commit loop for out-of-range input timestamps.

## Python Icechunk Library 0.2.9

This version is only partially released, not all Python wheels are released to PyPI. We recommend upgrading to 0.2.10.

### Features

- Add support for virtual chunks in Google Cloud Storage. Currently, credentials are needed
  to access GCS buckets, even if they are public. We'll allow anonymous access in a future version.

## Python Icechunk Library 0.2.8

### Features

- New `Repository.total_chunks_storage` method to calculate the space used by all chunks in the repo, across all versions.
- Rust library is compiled using rustc 1.85.

### Performance

- Up to 3x faster chunk upload for small chunks on GCS.

### Fixes

- CLI issues 0 exit code when using --help

## Python Icechunk Library 0.2.7

### Fixes

- Install Icechunk CLI by default.

## Python Icechunk Library 0.2.6

This is Icechunk's second 1.0 release candidate. This release is backwards compatible with
repositories created using any Icechunk version in the 0.2.X series.

### Features

- Icechunk got a new CLI executable that allows basic management of repos. Try it by running `icechunk --help`.
- Use new Tigris features to provide consistency guarantees on par with other object stores.
- New `force_path_style` option for S3 Storage. Used by certain object stores that cannot use the more modern addressing style.
- More documentation.

## Python Icechunk Library 0.2.5

This is Icechunk's first 1.0 release candidate. This release is backwards compatible with
repositories created using any Icechunk version in the 0.2.X series.

### Features

- Result of garbage collection informs how many bytes were freed from storage.
- Executable Python documentation.

### Fixes

- Support for `allow_pickling` in nested contexts.

## Python Icechunk Library 0.2.4

### Fixes

- Fixes a bug where object storage paths were incorrectly formatted when using Windows.

## Python Icechunk Library 0.2.3

### Features

- `Repository` can now be pickled.
- `icechunk.print_debug_info()` now prints out relative information about the installed version of icechunk and relative dependencies.
- `icechunk.Storage` now supports `__repr__`. Only configuration values will be printed, no credentials.

### Fixes

- Fixes a missing export for Google Cloud Storage credentials.

## Python Icechunk Library 0.2.2

### Features

- Added the ability to checkout a session `as_of` a specific time. This is useful for replaying what the repo would be at a specific point in time.
- Support for refreshable Google Cloud Storage credentials.

### Fixes

- Fix a bug where the clean prefix detection was hiding other errors when creating repositories.
- API now correctly uses `snapshot_id` instead of `snapshot` consistently.
- Only write `content-type` to metadata files if the target object store supports it.

## Python Icechunk Library 0.2.1

### Features

- Users can now override consistency defaults. With this Icechunk is usable in a larger set of object stores,
including those without support for conditional updates. In this setting, Icechunk loses some of its consistency guarantees.
This configuration variables are for advanced users only, and should only be changed if necessary for compatibility.

  ```python
  class StorageSettings:
    ...

    @property
    def unsafe_use_conditional_update(self) -> bool | None:
        ...
    @property
    def unsafe_use_conditional_create(self) -> bool | None:
        ...
    @property
    def unsafe_use_metadata(self) -> bool | None:
        ...
  ```

## Python Icechunk Library 0.2.0

This release is focused on stabilizing Icechunk's on-disk serialization format. It's a non-backwards
compatible change, hopefully the last one. Data written with previous versions must be reingested to be read with
Icechunk 0.2.0.

### Features

- `Repository.ancestry` now returns an iterator, allowing interrupting the traversal of the version tree at any point.
- New on-disk format using [flatbuffers](https://flatbuffers.dev/) makes it easier to document and implement
(de-)serialization. This enables the creation of alternative readers and writers for the Icechunk format.
- `Repository.readonly_session` interprets its first positional argument as a branch name:

```python
# before:
repo.readonly_session(branch="dev")

# after:
repo.readonly_session("dev")

# still possible:
repo.readonly_session(tag="v0.1")
repo.readonly_session(branch="foo")
repo.readonly_session(snapshot_id="NXH3M0HJ7EEJ0699DPP0")
```

- Icechunk is now more resilient to changes in Zarr metadata spec, and can handle Zarr extensions.
- More documentation.

### Performance

- We have improved our benchmarks, making them more flexible and effective at finding possible regressions.
- New `Store.set_virtual_refs` method allows setting multiple virtual chunks for the same array. This
significantly speeds up the creation of virtual datasets.

### Fixes

- Fix a bug in clean prefix detection

## Python Icechunk Library 0.1.3

### Features

- Repositories can now evaluate the `diff` between two snapshots.
- Sessions can show the current `status` of the working copy.
- Adds the ability to specify bearer tokens for authenticating with Google Cloud Storage.

### Fixes

- Dont write `dimension_names` to the zarr metadata if no dimension names are set. Previously, `null` was written.

## Python Icechunk Library 0.1.2

### Features

- Improved error messages. Exceptions raised by Icechunk now include a lot more information
on what happened, and what was Icechunk doing when the exception was raised. Example error message:
  ![image](https://private-user-images.githubusercontent.com/20792/411051347-2babe5df-dc3b-4305-8ad2-a18fdb4da796.png?jwt=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJnaXRodWIuY29tIiwiYXVkIjoicmF3LmdpdGh1YnVzZXJjb250ZW50LmNvbSIsImtleSI6ImtleTUiLCJleHAiOjE3Mzg5NTY0NzMsIm5iZiI6MTczODk1NjE3MywicGF0aCI6Ii8yMDc5Mi80MTEwNTEzNDctMmJhYmU1ZGYtZGMzYi00MzA1LThhZDItYTE4ZmRiNGRhNzk2LnBuZz9YLUFtei1BbGdvcml0aG09QVdTNC1ITUFDLVNIQTI1NiZYLUFtei1DcmVkZW50aWFsPUFLSUFWQ09EWUxTQTUzUFFLNFpBJTJGMjAyNTAyMDclMkZ1cy1lYXN0LTElMkZzMyUyRmF3czRfcmVxdWVzdCZYLUFtei1EYXRlPTIwMjUwMjA3VDE5MjI1M1omWC1BbXotRXhwaXJlcz0zMDAmWC1BbXotU2lnbmF0dXJlPWYzNjY1MTUyOTUwYWMwNWJmMTYwODkzYmY1NGM2YTczNzcxOTBmMTUyNzg3NWE2MWVmMzVmOTcwOTM2MDAxYTQmWC1BbXotU2lnbmVkSGVhZGVycz1ob3N0In0.rAstKp5GPVLbftBAAibWNPSCZ0ppz8FTJEvmvbL_Fdw)
- Icechunk generates logs now. Set the environment variable `ICECHUNK_LOG=icechunk=debug` to print debug logs to stdout. Available "levels" in order of increasing verbosity are `error`, `warn`, `info`, `debug`, `trace`. The default level is `error`. Example log:
  ![image](https://private-user-images.githubusercontent.com/20792/411051729-7e6de243-73f4-4863-ba79-2dde204fe6e5.png?jwt=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJnaXRodWIuY29tIiwiYXVkIjoicmF3LmdpdGh1YnVzZXJjb250ZW50LmNvbSIsImtleSI6ImtleTUiLCJleHAiOjE3Mzg5NTY3NTQsIm5iZiI6MTczODk1NjQ1NCwicGF0aCI6Ii8yMDc5Mi80MTEwNTE3MjktN2U2ZGUyNDMtNzNmNC00ODYzLWJhNzktMmRkZTIwNGZlNmU1LnBuZz9YLUFtei1BbGdvcml0aG09QVdTNC1ITUFDLVNIQTI1NiZYLUFtei1DcmVkZW50aWFsPUFLSUFWQ09EWUxTQTUzUFFLNFpBJTJGMjAyNTAyMDclMkZ1cy1lYXN0LTElMkZzMyUyRmF3czRfcmVxdWVzdCZYLUFtei1EYXRlPTIwMjUwMjA3VDE5MjczNFomWC1BbXotRXhwaXJlcz0zMDAmWC1BbXotU2lnbmF0dXJlPTQ1MzdmMDY2MDA2YjdiNzUzM2RhMGE5ZDAxZDA2NWI4ZWU3MjcyZTE0YjRkY2U0ZTZkMTcxMzQzMDVjOGQ0NGQmWC1BbXotU2lnbmVkSGVhZGVycz1ob3N0In0.LnILQIXxOjkR1y6P5w6k9UREm0zOH1tIzt2vrjVcRKM)
- Icechunk can now be installed using `conda`:

  ```shell
  conda install -c conda-forge icechunk
  ```

- Optionally delete branches and tags that point to expired snapshots:

  ```python
    def expire_snapshots(
        self,
        older_than: datetime.datetime,
        *,
        delete_expired_branches: bool = False,
        delete_expired_tags: bool = False,
    ) -> set[str]: ...
  ```

- More documentation. See [the Icechunk website](https://icechunk.io/)

### Performance

- Faster `exists` zarr `Store` method.
- Implement `Store.getsize_prefix` method. This significantly speeds up `info_complete`.

### Fixes

- Default regular expression to preload manifests.

## Python Icechunk Library 0.1.1

### Fixes

- Session deserialization error when using distributed writes

## Python Icechunk Library 0.1.0

### Features

- Expiration and garbage collection. It's now possible to maintain only recent versions of the repository, reclaiming the storage used exclusively by expired versions.
- Allow an arbitrary map of properties to commits. Example:

  ```
  session.commit("some message", metadata={"author": "icechunk-team"})
  ```

  This properties can be retrieved via `ancestry`.
- New `chunk_coordinates` function to list all initialized chunks in an array.
- It's now possible to delete tags. New tags with the same name won't be allowed to preserve the immutability of snapshots pointed by a tag.
- Safety checks on distributed writes via opt-in pickling of the store.
- More safety around snapshot timestamps, blocking commits if there is too much clock drift.
- Don't allow creating repositories in dirty prefixes.
- Experimental support for Tigris object store: it currently requires the bucket to be restricted to a single region to obtain the Icechunk consistency guarantees.
- This version is the first candidate for a stable on-disk format. At the moment, we are not planning to change the on-disk format prior to releasing v1 but reserve the right to do so.

### Breaking Changes

- Users must now opt-in to pickling and unpickling of Session and IcechunkStore using the `Session.allow_pickling` context manager
- `to_icechunk` now accepts a Session, instead of an IcechunkStore

### Performance

- Preload small manifests that look like coordinate arrays on session creation.
- Faster `ancestry` in an async context via `async_ancestry`.

### Fixes

- Bad manifest split in unmodified arrays
- Documentation was updated to the latest API.

## Python Icechunk Library 0.1.0a15

### Fixes

- Add a constructor to `RepositoryConfig`

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
