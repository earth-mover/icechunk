---
title: Filesystem layout, version 1
---
# Icechunk Filesystem Layout, version 1

!!! Note

    This page is part of the [Icechunk specification, version 2.2](./spec-v2-2.md). It defines the `filesystem-layout-v1` [writer feature](./spec-v2-2.md#writer-features). The filesystem layout is experimental.

    The key words "MUST", "MUST NOT", "REQUIRED", "SHALL", "SHALL NOT", "SHOULD", "SHOULD NOT", "RECOMMENDED", "MAY", and "OPTIONAL" in this document are to be interpreted as described in [RFC 2119](https://www.rfc-editor.org/rfc/rfc2119.html).

## Overview

The Icechunk specification requires a conditional update of the repo info file, which filesystems in general do not provide. In the filesystem layout, the repo info is not a single file. Each version is an immutable file in the `$ROOT/repo.d/` directory, and an empty *marker* file, whose name says which version is current, moves from one version to the next by `rename`. A commit is a single `rename` of the marker, which the filesystem server decides.

Everything else is as in the [specification](./spec-v2-2.md): snapshots, manifests, chunks, transaction logs, the `overwritten/` backups, expiration and garbage collection.

`$ROOT/repo` holds a *mirror*: a byte-identical copy of a recent version in `repo.d/`, for implementations that do not implement this layout. Every version in `repo.d/`, and so the mirror, lists the `filesystem-layout-v1` writer feature, so those implementations see the repository as read-only.

An implementation implements `filesystem-layout-v1` when it follows this document on a filesystem. On any other storage, it implements it only to [convert a copy to the standard layout](#converting-a-copy-to-the-standard-layout), and MUST NOT otherwise modify the repository.

This page describes the files of the layout and how the operations use them. The complete protocol, including durability, recovery from interrupted operations and lost replies, is in the [filesystem storage design document](https://github.com/earth-mover/icechunk/blob/main/design-docs/020-filesystem-storage.md) and in the Icechunk implementation.

## Files

In `$ROOT/repo.d/`:

| Name | Meaning |
| --- | --- |
| `<N>.<h>` | Version `N` of the repo info, whose SHA-256 is `h` |
| `latest.<K>.<N>.<h>` | The marker: version `N`, with hash `h`, is current |
| `frozen.<K>.<N>.<h>` | The marker while the repository is frozen |
| `converted.<K>.<N>.<h>` | The marker of a repository converted to the standard layout from version `N` |
| `stage.<id>`, `mirror.<id>` | Staged bytes of a new version and of the mirror |

In `$ROOT`:

| Name | Meaning |
| --- | --- |
| `repo` | The mirror |
| `repo.d.tmp.<id>/` | A `repo.d` being built, by a creation or a conversion |
| `repo.d.converting.<id>/` | The `repo.d` of a repository being converted to the standard layout |
| `repo.legacy.<id>` | The repo info of a repository being converted from the standard layout |

In every directory: `<name>.staging.<id>`, the staged bytes of the object `<name>`. Readers listing the object directories MUST ignore these names.

Where:

- `N` is a decimal integer below 10<sup>15</sup>, without leading zeros. The first version of a repository has `N` equal to the Unix time in milliseconds when it was created, and each commit adds one.
- `K` is `999999999999999 - N` in exactly 15 digits, so that a lexicographic listing puts the marker with the highest `N` first.
- `h` is the SHA-256 of the version's bytes, as 64 lowercase hexadecimal characters.
- `id` is a random UUID in its hyphenated, lowercase form.

Implementations SHOULD NOT create other names in `repo.d`. Readers MUST ignore names that do not parse exactly as above, and garbage collection MUST NOT delete them. The rest of this page writes `latest.<N>.<h>` for `latest.<K>.<N>.<h>`, and similarly for the other markers.

Every version in `repo.d`, and the mirror, is a repo info file in the standard [binary file format](./spec-v2-2.md#binary-file-format) that lists `filesystem-layout-v1` in `Repo.writer_features`, and so carries the [status gate](./spec-v2-2.md#repostatus).

## Reading

To read the repo info, an implementation of this layout lists `repo.d`. The marker with the highest `N` names the current version `<N>.<h>`. A concurrent commit can delete that version after the listing, in which case the reader lists again. If the marker is `converted.*`, the repository has left this layout, and the repo info is `$ROOT/repo`.

Implementations that do not implement this layout read the mirror. The mirror can lag behind the current version, and can be missing until the next time it is published. A reader that needs the latest commit should be given its snapshot id.

## Operations

**Commit.** The writer writes the new version to `repo.d/stage.<id>`, hard-links it as `repo.d/<N+1>.<h'>`, then renames the marker `latest.<N>.<h>` to `latest.<N+1>.<h'>`. The rename fails if another commit moved the marker first: that is a conflict. Exactly one marker exists at any time, and the version it names is complete and durable before the rename.

**Freeze and unfreeze.** Freezing stops every change to the repo info until the repository is unfrozen. Unlike the `ReadOnly` status, it is a state of the layout, not of the repo info: it changes only the marker and writes no ops log entry. It exists to copy and to convert the repository at a fixed version. Freeze renames `latest.<N>.<h>` to `frozen.<N>.<h>` and publishes the mirror. Unfreeze hard-links `<N>.<h>` as `<N+1>.<h>` and renames the marker to `latest.<N+1>.<h>`. To copy a repository consistently, freeze it, check that the SHA-256 of `$ROOT/repo` is the `h` of the frozen marker, then copy every entry under `$ROOT`.

**Publishing the mirror.** After every commit, freeze and unfreeze, the implementation replaces `$ROOT/repo` with a copy of the current version, through a staged `repo.d/mirror.<id>` renamed to `$ROOT/repo`. Publishing SHOULD complete before the commit returns, and MAY run in the background if the user chooses.

**Creation.** The writer builds the first version in `$ROOT/repo.d.tmp.<id>/`, hard-links it as `$ROOT/repo`, adds the marker, and renames the directory to `$ROOT/repo.d`. Creation fails if `repo`, `repo.d`, a `repo.legacy.<id>` or a `repo.d.converting.<id>/` exists.

**Deleting old versions.** Implementations MUST NOT delete a version file `<i>.<x>` unless a listing of `repo.d` showed a marker `(N, h)` with `i < N`, or with `i = N` and `x ≠ h`. Garbage collection also deletes the staged files and `repo.d.tmp.<id>/` directories older than its cutoff. It MUST NOT delete `repo.legacy.<id>` or `repo.d.converting.<id>/`.

**Other objects** are written either to a staged `<name>.staging.<id>` renamed to `<name>`, or directly under their final name when no reader can know the name before a commit makes the object reachable. No reader sees a partially written object under a name it can know.

## Forks

An implementation that does not implement this layout can still change the status through the mirror, as Icechunk 2.0 to 2.3 do. That write removes `filesystem-layout-v1` from `$ROOT/repo`, and forks the repository: `repo` and `repo.d` hold diverging histories. Before every commit, before publishing the mirror, and before garbage collection deletes anything, implementations of this layout MUST read `$ROOT/repo`. If it does not list `filesystem-layout-v1` while `repo.d` has a `latest.*` or `frozen.*` marker, they MUST NOT commit, publish the mirror or delete anything, and MUST report the fork. Recovery is manual, and keeps one of the two histories.

## Conversions

A repository can be converted between this layout and the standard layout, in both directions. A conversion MUST run with no other writer of the repository running, including processes that publish the mirror in the background. It changes only the writer features, the status gate and the ops log, with a `RepoStatusChangedUpdate` entry, and keeps the repository's own status.

**From the standard layout.** The writer renames `$ROOT/repo` to `$ROOT/repo.legacy.<id>`, builds `repo.d` as a creation does, from that repo info with `filesystem-layout-v1` added, then deletes `repo.legacy.<id>`.

**To the standard layout.** The writer freezes the repository, renames the marker to `converted.<N>.<h>` so that no commit, freeze or unfreeze can move it again, renames `repo.d` to `repo.d.converting.<id>`, writes the current version without `filesystem-layout-v1` as `$ROOT/repo`, then deletes `repo.d.converting.<id>/`.

While a `repo.legacy.<id>` or a `repo.d.converting.<id>/` exists, a conversion is in progress. Implementations of this layout MUST NOT open the repository, other than to resume the conversion. A conversion to the standard layout that finds `$ROOT/repo` without `filesystem-layout-v1` resumes only if the ops log of `repo` names a backup in `overwritten/` whose SHA-256 is the `h` of the marker. Otherwise `repo` is a fork, and the conversion MUST stop and report it.

### Converting a copy to the standard layout

A copy of a repository in this layout on other storage, such as an object store, can be converted to the standard layout. The marker in the copy's `repo.d` MUST be `frozen.*` or `converted.*`; otherwise the copy may be inconsistent, and the conversion fails. The writer reads the version the marker names, removes `filesystem-layout-v1`, writes the result to `$ROOT/repo` with a conditional update, then deletes `repo.d/`. A conversion that finds `$ROOT/repo` without `filesystem-layout-v1` resumes only if the ops log of `repo` names a backup in `overwritten/` whose SHA-256 is the `h` of the marker. Otherwise `repo` is a fork, and the conversion MUST stop and report it.

To move a repository in this layout to other storage, convert it to the standard layout before copying it, or freeze it, copy it, and convert the copy.

## Filesystem requirements

The whole repository MUST be on a single filesystem, with no mount point inside `$ROOT`.
