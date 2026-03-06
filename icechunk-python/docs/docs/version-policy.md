# Icechunk Version Policy

## On-Disk Format

The Icechunk on-disk format is defined by the format specification ("Icechunk Spec"), which is versioned by a single integer. The spec version increments if and only if there is an on-disk incompatible change, as defined by the [spec document](./spec.md).

## Library Versions

Versions are identified by a triplet of integers: `<major>.<minor>.<patch>` (e.g. `2.3.1`).

All official Icechunk libraries — Python, Rust, and JavaScript/WASM — use a versioning scheme where the **major version number matches the on-disk format version**.

This means that breaking API changes may occur in minor releases rather than requiring a major version bump.

### Major releases

A major release (e.g. `1.x.y` to `2.0.0`) accompanies a new on-disk format version and may also include breaking API changes, new features, and bug fixes.

### Minor releases

A minor release (e.g. `2.0.0` to `2.1.0`) may include:

- New features and functionality
- Breaking API changes
- Deprecations and removals

Users should review the changelog before upgrading to a new minor release.

### Patch releases

A patch release (e.g. `2.1.0` to `2.1.1`) contains only bug fixes. No new features, no breaking changes.

Users should always feel safe upgrading to the latest patch release.

## Library - Data format compatibility

### Reading

Any major version of an Icechunk library can read data written with its own or any prior major version.

### Writing

An Icechunk library of major version N will be able to write to, at minimum, Icechunk Spec versions N and N-1.

For example, this means that `icechunk` Python version 2 can write to Icechunk Spec version 1 format, but while `icechunk` Python 3 will be able to write to Icechunk Spec version 2, it is not guaranteed to be able to write to Icechunk Spec version 1.

### Forward compatibility

Icechunk libraries do **not** implement forward-compatible reading or writing. For example, Icechunk 1.x.y cannot read or write data using Icechunk format version 2 or later. Users must upgrade to a library version whose major version matches or exceeds the desired format.
