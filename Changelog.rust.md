# Changelog

## Rust Icechunk Library 0.1.0-alpha.4

### Features

- Added new `Store::reset_branch` method to point the head of the current branch to another snapshot, changing the history of the branch.

### Fixes

- Zarr metadata will now only include the attributes key when the attributes dictionary of the node is not empty, aligning Icechunk with the python-zarr implementation.

## Rust Icechunk Library 0.1.0-alpha.3

### Features

- Added new `Store::list_dir_items` method and `ListDirItem` type to distinguish keys and
  prefixes during `list_dir` operations.
- New `ByteRange` type allows retrieving the final `n` bytes of a chunk.


## Rust Icechunk Library 0.1.0-alpha.2

### Features

- Initial release
