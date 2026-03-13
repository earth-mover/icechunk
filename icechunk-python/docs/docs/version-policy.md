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

## Compatibility and migration

Migrating an Icechunk repository from one major format version to the next is a **metadata-only operation** — it does not require copying or duplicating your data. Only Icechunk's internal metadata is updated; your chunk data remains in place.

We maintain a documented and tested upgrade path from all major versions starting with Icechunk 1 to the latest version. Upgrading may be a multi-step process across major versions (e.g. upgrade 1 → 2, then 2 → 3), and we cover each step with integration tests.

When a new major version is released, the previous major version will continue to receive **bug fixes and security patches for at least one year**. For example, if Icechunk 2.0 is released on April 1, 2026, Icechunk 1.x will receive bug and security fixes until at least April 1, 2027.

### Reading and writing

An Icechunk library of major version N can read and write Icechunk Spec versions N and N-1. We make best effort to support reading older spec versions, but do not guarantee it. For example, Icechunk 3.x can read and write Spec versions 2 and 3, and will make best effort to read Spec version 1 data.

Icechunk libraries do **not** support forward-compatible reading or writing. For example, Icechunk 1.x.y cannot read or write Icechunk format version 2 or later. Users must upgrade to a library version whose major version matches or exceeds the desired format.
