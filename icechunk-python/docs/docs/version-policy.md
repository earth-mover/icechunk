# Icechunk Version Policy

## Overview

Versions are identified by a triplet of integers: `<major>.<minor>.<patch>` (e.g. `2.3.1`).

All official Icechunk libraries — Python, Rust, and JavaScript/WASM — use a versioning scheme where the **major version number matches the on-disk format version**.

This means that breaking API changes may occur in minor releases rather than requiring a major version bump.


| Component | Meaning |
|-----------|---------|
| **Major** | Matches the [Icechunk Spec](#icechunk-spec) version. Incremented only when a new on-disk format version is released. |
| **Minor** | New features and potentially breaking API changes. |
| **Patch** | Bug fixes only. |



## Icechunk Spec

The Icechunk format specification ("Icechunk Spec") is versioned by a single integer. The spec version increments if and only if there is an on-disk incompatible change, as defined by the [spec document](./spec.md).

## What to expect from each release type

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

## Data format compatibility

### Reading

Any major version of an Icechunk library can read data written with its own or any prior major version.

### Writing

An Icechunk library will be able to, at minimum, write to the current and previous major version of the Icechunk Spec.

For example, Icechunk 2.x.y can write to both Icechunk Spec version 2 and version 1, but Icechunk 3.x.y is not guaranteed to write to Icechunk Spec version 1.

### Forward compatibility

Icechunk libraries do **not** implement forward-compatible reading or writing. For example, Icechunk 1.x.y cannot read or write data using Icechunk format version 2 or later. Users must upgrade to a library version whose major version matches or exceeds the desired format.
