# Icechunk Version Policy

## On-Disk Format

The Icechunk on-disk format is defined by the format specification ("Icechunk Spec"). A spec version is written as `<major>` with an optional `.<minor>` suffix (for example `1`, `2`, or `2.1`); an omitted minor is equivalent to `.0`.

The major version increments only on an on-disk incompatible change, one that older libraries cannot safely read or write. A major bump changes the on-disk format version (described below) and may require a migration to adopt. The minor version increments on an on-disk compatible change. A minor version bump never requires a migration, but in some cases an optional migration may be available, generally to access a new format-level feature. Spec versions 1 and 2 had no minor component (equivalently `.0`); 2.1 is the first minor release.

Every metadata file records a single-byte on-disk format version in its header. This byte equals the spec major version only, and is `2` for both spec 2.0 and spec 2.1. Minor spec versions are not encoded in the header; instead, minor-version features are detected at read time.

The current spec is [version 2.1](./spec.md), introduced in Icechunk 2.1.0. All Icechunk 2.x libraries can read and write both spec 2.0 and 2.1 repositories (see [Compatibility](#compatibility)). Earlier formats are documented in [version 2](./spec-v2.md) (introduced in Icechunk 2.0.0) and [version 1](./spec-v1.md) (used by Icechunk 1.x).

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

## Support Window

When a new major version is released, the previous major version will continue to receive **bug fixes and security patches for at least one year**. For example, if Icechunk 2.0 is released on April 1, 2026, Icechunk 1.x will receive bug and security fixes until at least April 1, 2027.

## Migration and Compatibility

### Migration

Migrating an Icechunk repository from one major format version to the next is a **metadata-only operation** — it does not require copying or duplicating your data. Only Icechunk's internal metadata is updated; your chunk data remains in place.

We maintain a documented and tested upgrade path from all major versions starting with Icechunk 1 to the latest version. Upgrading may be a multi-step process across major versions (e.g. upgrade 1 → 2, then 2 → 3), and we cover each step with integration tests.

### Compatibility

Compatibility across major versions: an Icechunk library of major version N can read and write Icechunk Spec major versions N and N-1. We make best effort to support reading older spec versions, but do not guarantee it. For example, Icechunk 3.x can read and write Spec versions 2.x and 3.x, and will make best effort to read Spec version 1 data. Icechunk libraries do **not** support forward compatibility across major versions. For example, Icechunk 1.x.y cannot read or write Icechunk format version 2 or later. Users must upgrade to a library version whose major version matches or exceeds the desired format.

Compatibility across minor versions: within a single major version, all minor spec versions are forward and backward read/write compatible. Any 2.x library can read and write any spec 2.y repository. When a library operates on a repository written under a newer minor spec than it implements, reads and writes remain safe, but behavior is degraded for the features it does not recognize. For example, an Icechunk 2.0.x library can read and write a spec 2.1 repository, ignoring the `pruned_ancestor_tx_logs` field introduced in 2.1 and behaving as it did before that feature existed.
