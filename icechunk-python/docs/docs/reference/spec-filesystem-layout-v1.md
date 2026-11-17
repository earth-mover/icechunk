---
title: Filesystem layout, version 1
---
# Icechunk Filesystem Layout, version 1

!!! warning "Experimental"

    The filesystem layout is experimental. A later minor release may supersede `filesystem-layout-v1` with a new version of the layout, under a new writer feature name; this version doesn't change. See [experimental writer features](./version-policy.md#experimental-writer-features) for what the Icechunk libraries keep supporting.

!!! Note

    This page is part of the [Icechunk specification, version 2.2](./spec-v2-2.md). It defines the `filesystem-layout-v1` [writer feature](./spec-v2-2.md#writer-features).

    The key words "MUST", "MUST NOT", "REQUIRED", "SHALL", "SHALL NOT", "SHOULD", "SHOULD NOT", "RECOMMENDED", "MAY", and "OPTIONAL" in this document are to be interpreted as described in [RFC 2119](https://www.rfc-editor.org/rfc/rfc2119.html).

## Overview

The [object store layout](./spec-v2-2.md#storage-operations) keeps the repo info in a single `$ROOT/repo` file and requires a conditional update of it, which filesystems in general do not provide. In the filesystem layout, the repo info is not a single file. Each version is an immutable file in the `$ROOT/repo.d/` directory, and an empty *marker* file, whose name says which version is current, moves from one version to the next by `rename`. On this page, a *change* is any update of the repo info: a commit, a branch, tag, status or configuration change, expiration, garbage collection or a conversion. Every change except a conversion from the object store layout is a single `rename` of the marker, which the filesystem server decides.

Everything else is as in the [specification](./spec-v2-2.md): snapshots, manifests, chunks, transaction logs, the `overwritten/` backups, expiration and garbage collection.

A repository in this layout has no `$ROOT/repo`, except during a [conversion](#conversions), so implementations that do not implement this layout cannot open it. Every version in `repo.d/` lists the `filesystem-layout-v1` writer feature, except the last version written by a [conversion to the object store layout](#conversions).

Whether an implementation implements `filesystem-layout-v1` is decided per storage backend: a backend implements it only if it reads and writes the repo info as this document describes, on a filesystem. No implementation implements it on any other storage. Implementations MUST NOT write `$ROOT/repo` of a repository in this layout except to convert the repository.

This page describes the files of the layout and how the operations use them.

## Files

In `$ROOT/repo.d/`:

| Name | Meaning |
| --- | --- |
| `<N>.<h>` | Version `N` of the repo info, whose SHA-256 is `h` |
| `latest.<K>.<N>.<h>` | The marker: version `N`, with hash `h`, is current |
| `stage.<id>` | Staged bytes of a new version |

In `$ROOT`:

| Name | Meaning |
| --- | --- |
| `repo` | The repo info in the object store layout, only during a [conversion](#conversions) |
| `repo.d.tmp.<id>/` | A `repo.d` being built by a creation or a conversion from the object store layout, or being deleted by a conversion to the object store layout |

In every directory: `<name>.staging.<id>`, the staged bytes of the object `<name>`, such as `$ROOT/repo.staging.<id>`. Readers listing the object directories MUST ignore these names.

Where:

- `N` is a decimal integer below 10<sup>15</sup>, without leading zeros. The first version in a `repo.d` has `N` equal to the Unix time in milliseconds when it was created, and each change adds one.
- `K` is `999999999999999 - N` in exactly 15 digits, so that a lexicographic listing puts the marker with the highest `N` first.
- `h` is the SHA-256 of the version's bytes, as 64 lowercase hexadecimal characters.
- `id` is a random UUID in its hyphenated, lowercase form.

Implementations SHOULD NOT create other names in `repo.d`. Readers MUST ignore names that do not parse exactly as above, and garbage collection MUST NOT delete them. The rest of this page writes `latest.<N>.<h>` for `latest.<K>.<N>.<h>`.

Every version in `repo.d` is a repo info file in the [binary file format](./spec-v2-2.md#binary-file-format).

## Reading

To read the repo info, an implementation of this layout lists `repo.d`, whether or not `$ROOT/repo` exists. The marker with the highest `N` names the current version `<N>.<h>`. A concurrent change can delete that version after the listing, in which case the reader lists again. If the current version does not list `filesystem-layout-v1`, a conversion to the object store layout is in progress: implementations of this layout MUST NOT modify the repository then, by a change or by deleting objects, garbage collection included, other than by running that conversion again.

## Operations

**Change.** The writer writes the new version to `repo.d/stage.<id>`, hard-links it as `repo.d/<N+1>.<h'>`, then renames the marker `latest.<N>.<h>` to `latest.<N+1>.<h'>`. The rename fails if another change moved the marker first: that is a conflict. Exactly one marker exists at any time, and the version it names is complete and durable before the rename. Before writing the new version, the writer backs up the version `<N>.<h>` it read to `overwritten/`, as every update does (see the [specification](./spec-v2-2.md#updates-to-the-repo-info-file)). A writer that knows its attempt did not land, because another change moved the marker past `N` first, MAY delete that attempt's backup: no version names it, and the version it holds was backed up by the change that replaced it. It MUST NOT delete the backup of an attempt whose outcome is unknown. A failure to delete is ignored.

**Creation.** The writer builds the first version and its marker in `$ROOT/repo.d.tmp.<id>/`, then renames the directory to `$ROOT/repo.d`: that rename creates the repository. Creation fails if `repo` or `repo.d` exists, including when another creation's rename lands first. An interrupted creation leaves only a `repo.d.tmp.<id>/`, which garbage collection deletes.

**Deleting old versions.** Implementations MUST NOT delete a version file `<i>.<x>` in `repo.d` unless a listing of `repo.d` showed, or the implementation's own rename moved the marker to, a marker `(N, h)` with `i < N`, or with `i = N` and `x ≠ h`. Garbage collection also deletes the staged files in `$ROOT` and `repo.d/`, including `$ROOT/repo.staging.<id>`, and the `repo.d.tmp.<id>/` directories older than its cutoff. A conversion to the object store layout deletes the whole directory as described below.

**Other objects** are written either to a staged `<name>.staging.<id>` renamed to `<name>`, or directly under their final name when no reader can know the name before a change makes the object reachable. No reader sees a partially written object under a name it can know.

## Conversions

A repository can be converted between this layout and the object store layout. How an implementation converts is not part of this specification, but a conversion MUST change only the writer features and the ops log, with a `RepoStatusChangedUpdate` entry, and keep the repository's status, except that an `Offline` or undefined status is written as `ReadOnly`, with a reason that names the stored value.

A conversion to the object store layout ends with `$ROOT/repo` and no `repo.d`. It MUST NOT make its change while `$ROOT/repo` exists, as an interrupted conversion from the object store layout can leave it. It creates `$ROOT/repo` holding the converted version with a `link`, and MUST NOT replace an existing `$ROOT/repo`: if one appears before the link, the conversion fails and keeps `repo.d`. Running it again keeps an existing `$ROOT/repo` as it is only if it is a regular file holding a repo info that doesn't list `filesystem-layout-v1`; otherwise it MUST NOT delete `repo.d`, and recovery is manual. It creates `$ROOT/repo` if it is missing, then deletes `repo.d`.

A conversion from the object store layout ends with a `repo.d` whose first version is the repo info of `$ROOT/repo` with `filesystem-layout-v1` added, and no `$ROOT/repo`. It backs up the original repo info to `overwritten/`, and unlinks `$ROOT/repo` after it renames the directory to `repo.d`. Running it again unlinks a remaining `$ROOT/repo` only if it is the repo info that the conversion converted, as its backup shows.

An interrupted conversion SHOULD be completed by running it again with the implementation that started it, before anything else uses the repository.

## Requirements

The whole repository MUST be on a single filesystem, with no mount point inside `$ROOT`. Every `rename` within a directory of `$ROOT` MUST be atomic and decided by the server, and MUST fail if the source no longer exists. A `link` MUST fail if the new name exists, and the new name MUST show the complete content. Renaming a directory MUST be atomic, and MUST fail if the target exists, unless it is an empty directory. `unlink` MUST remove the name it is given. A listing MUST show only names that existed. `fsync` MUST be durable, and an `fsync` of a directory MUST make durable the entries in it that other clients created.

The first `N` of a new `repo.d`, made by a creation or a conversion from the object store layout, MUST exceed every `N` that an earlier `repo.d` in the same `$ROOT` reached, because a stale listing can still show the earlier directory's markers. Implementations rely on the clock of the host that creates the directory for this. If the requirement fails, for example because of clock skew between hosts, a stale listing of the earlier directory can lead garbage collection to delete the current version.
