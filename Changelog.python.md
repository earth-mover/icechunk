# Changelog

## Python Icechunk Library 0.1.0a4

### Features

- Added `IcechunkStore::reset_branch` and `IcechunkStore::async_reset_branch` methods to point the head of the current branch to another snapshot, changing the history of the branch

### Fixes

- Zarr metadata will now only include the attributes key when the attributes dictionary of the node is not empty, aligning Icechunk with the python-zarr implementation.

## Python Icechunk Library 0.1.0a2

### Features

- Initial release
