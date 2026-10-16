# Icechunk Version Policy

## On-Disk Format

The Icechunk on-disk format is defined by the format specification ("Icechunk Spec"). A spec version is written as `<major>` with an optional `.<minor>` suffix (for example `1`, `2`, or `2.1`); an omitted minor is equivalent to `.0`.

The major version increments only on an on-disk incompatible change, one that older libraries cannot safely read or write. **A major bump changes the on-disk format version and may require a migration** to adopt. The minor version increments on an on-disk compatible change. **A minor version bump never requires a migration**, but in some cases an optional migration may be available, generally to access a new format-level feature. Spec versions 1 and 2 had no minor component (equivalently `.0`); 2.1 was the first minor release.

Every metadata file records a single-byte on-disk format version in its header. This byte equals the spec major version only, and is `2` for every spec 2.x. Minor spec versions are not recorded on disk. A library learns what a repository requires from its contents: optional fields it finds, and, from spec 2.2, the repository's list of [writer features](./spec-v2-2.md#writer-features).

The current spec is [version 2.2](./spec-v2-2.md), introduced in Icechunk 2.4.0. Earlier formats are documented in [version 2.1](./spec-v2-1.md) (introduced in Icechunk 2.1.0), [version 2](./spec-v2.md) (introduced in Icechunk 2.0.0) and [version 1](./spec-v1.md) (used by Icechunk 1.x).

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

### Compatibility across major versions

An Icechunk library of major version N can read and write Icechunk Spec major versions N and N-1. We make best effort to support reading older spec versions, but do not guarantee it. For example, Icechunk 3.x can read and write Spec versions 2.x and 3.x, and will make best effort to read Spec version 1 data. Icechunk libraries do **not** support forward compatibility across major versions. For example, Icechunk 1.x.y cannot read or write Icechunk format version 2 or later. Users must upgrade to a library version whose major version matches or exceeds the desired format.

### Compatibility across minor versions

Within a single major version N:

- **Any N.x library can read any spec N.y repository.** For example, Icechunk 2.0.6 can read a spec 2.2 repository. A library ignores the features it does not know when reading, and behaves as it did before they existed.
- **Any N.x library either writes a spec N.y repository safely or refuses to write it**, with an error that says what it is missing. A library never silently drops information that a newer library wrote.

Each major version documents below how it keeps these promises and its exceptions to them.

### Major version 2

#### Writer features

From spec 2.2, a repository lists its [writer features](./spec-v2-2.md#writer-features): format features that a library must implement to modify the repository without damaging it. Modifying includes commits, branch and tag changes, expiration and garbage collection. A library refuses to modify a repository that lists a writer feature it does not implement. Most repositories list none, and any 2.x library can write them.

Libraries older than 2.4.0 do not know about writer features, so a repository that lists any writer feature is read-only for them. Commits, branch and tag changes and configuration changes fail with a message that names the features and the library version required. Garbage collection and expiration fail with a message that says only that the repository is read-only; the repository's status shows the full message. These libraries can still read the repository.

#### Known Exceptions

- **Spec 2.1 expiration history and Icechunk 2.0.x.** The `pruned_ancestor_tx_logs` field added in spec 2.1 predates [writer features](./spec-v2-2.md#writer-features). An Icechunk 2.0.x library removes this field from every snapshot whenever it modifies a repository, and its garbage collection deletes the transaction logs the field lists, even without first rewriting the repository info. After that, `diff`, `rebase`, `amend` and `inspect` across expired commits are incomplete, and the lost history cannot be recovered. Use Icechunk 2.1.0 or later for every library that modifies a repository that has been expired.
- **Configuration and Icechunk before 2.4.0.** Icechunk 2.0 to 2.3 cannot open a repository whose configuration uses a setting value added later (for 2.0.x, HTTP headers for virtual chunk containers; for any of them, a new kind of virtual chunk container storage), and drop settings they do not recognize when they change the configuration. Icechunk 2.4.0 and later open such repositories, ignore the settings they cannot decode, and keep them.
- **Status changes with Icechunk older than 2.4.0 (not supported).** Icechunk 2.0 to 2.3 can change the status of a repository that lists writer features. The change removes the writer features and the read-only marking from the repository, and drops the real status from the history entries it rewrites (see below). On a repository in the [filesystem layout](./spec-filesystem-layout-v1.md) it forks the repository: older libraries then see an ordinary repository, can commit to it and collect garbage in it, and that garbage collection can delete files the filesystem-layout history needs. Icechunk 2.4.0 and later report the fork when they next publish the read-only copy or collect garbage, but the change can also be overwritten without a report. Icechunk 2.4.0 and later refuse to modify the forked repository with `local_filesystem_storage`. On a copy in other storage, the copy becomes an ordinary repository in the standard layout. Change the status of such repositories only with Icechunk 2.4.0 or later.
- **Status history and Icechunk before 2.4.0.** When Icechunk 2.0 to 2.3 modify a repository that once used writer features, its history shows the read-only marking instead of the real status for status changes recorded while it used them.
- **Large repositories and Icechunk before 2.2.1.** Icechunk 2.0.0 to 2.2.0 cannot open a repository whose repository info holds more than about one million internal records (roughly 300,000 to 1,000,000 snapshots).
- **Feature flags and Icechunk before 2.4.0.** Flags other than `move_node`, `create_tag` and `delete_tag` are not enforced by Icechunk older than 2.4.0. To stop every writer, set the repository status to read-only.

#### Storage layouts

A repository stored on a filesystem through `filesystem_storage` uses the [filesystem layout](./spec-filesystem-layout-v1.md), which lists the `filesystem-layout-v1` [writer feature](./spec-v2-2.md#writer-features). Only filesystem storage, from Icechunk 2.4.0, can modify such a repository. Every 2.x library can read it: other storage backends, and libraries older than 2.4.0, read it through a read-only copy of the repository info object that may lag behind the latest commit, especially after crashes or under heavy write contention. Setting the repository status to read-only makes the copy current once every earlier writer has finished; do it before serving a finished repository read-only or taking a copy of it, and check a copy as the [filesystem layout](./spec-filesystem-layout-v1.md#copying-a-repository) describes. Publishing the copy with `publish_mirror()` makes it current on a best-effort basis. A lagging copy can reference objects that garbage collection has deleted. Converting a repository between the filesystem layout and the standard layout is a metadata-only operation, supported in both directions. A copy of a filesystem-layout repository on other storage stays readable; to move one where it can be modified, set its status to read-only, convert it to the standard layout, copy it, optionally convert the original back to the filesystem layout, then set the statuses of the original and the copy to online. Conversions must run while nothing else uses the repository.
