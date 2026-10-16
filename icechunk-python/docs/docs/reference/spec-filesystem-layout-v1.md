---
title: Filesystem layout, version 1
---
# Icechunk Filesystem Layout, version 1

!!! Note

    This page is part of the [Icechunk specification, version 2.2](./spec-v2-2.md). It defines the `filesystem-layout-v1` [writer feature](./spec-v2-2.md#writer-features). The filesystem layout is experimental.

    The key words "MUST", "MUST NOT", "REQUIRED", "SHALL", "SHALL NOT", "SHOULD", "SHOULD NOT", "RECOMMENDED", "MAY", and "OPTIONAL" in this document are to be interpreted as described in [RFC 2119](https://www.rfc-editor.org/rfc/rfc2119.html).

## Overview

The Icechunk specification requires a conditional update of the repo info file, which filesystems in general do not provide. In the filesystem layout, the repo info is not a single file. Each version is an immutable file in the `$ROOT/repo.d/` directory, and an empty *marker* file, whose name says which version is current, moves from one version to the next by `rename`. On this page, a *change* is any update of the repo info: a commit, a branch, tag, status or configuration change, expiration, garbage collection or a conversion. Every change except a conversion from the standard layout is a single `rename` of the marker, which the filesystem server decides.

Everything else is as in the [specification](./spec-v2-2.md): snapshots, manifests, chunks, transaction logs, the `overwritten/` backups, expiration and garbage collection.

`$ROOT/repo` holds a *mirror*: a byte-identical copy of a version in `repo.d/`, normally one that was current (see [Reading](#reading)), for implementations that do not implement this layout. Every version in `repo.d/`, and so the mirror, lists the `filesystem-layout-v1` writer feature, so those implementations see the repository as read-only. The only exception is the last version written by a [conversion to the standard layout](#conversions).

Whether an implementation implements `filesystem-layout-v1` is decided per storage backend: a backend implements it only if it reads and writes the repo info as this document describes, on a filesystem. No implementation implements it on any other storage: there, implementations MUST NOT modify the repository, and read it through the mirror. Implementations MUST NOT write `$ROOT/repo` of a repository in this layout except to create the repository, to publish the mirror or to convert the repository.

This page describes the files of the layout and how the operations use them.

## Files

In `$ROOT/repo.d/`:

| Name | Meaning |
| --- | --- |
| `<N>.<h>` | Version `N` of the repo info, whose SHA-256 is `h` |
| `latest.<K>.<N>.<h>` | The marker: version `N`, with hash `h`, is current |
| `stage.<id>` | Staged bytes of a new version |
| `mirror.<id>` | Staged bytes of the mirror |

In `$ROOT`:

| Name | Meaning |
| --- | --- |
| `repo` | The mirror |
| `repo.d.tmp.<id>/` | A `repo.d` being built by a creation or a conversion from the standard layout, or being deleted by a conversion to the standard layout |

In every directory: `<name>.staging.<id>`, the staged bytes of the object `<name>`, such as `$ROOT/repo.staging.<id>`. Readers listing the object directories MUST ignore these names.

Where:

- `N` is a decimal integer below 10<sup>15</sup>, without leading zeros. The first version in a `repo.d` has `N` equal to the Unix time in milliseconds when it was created, and each change adds one.
- `K` is `999999999999999 - N` in exactly 15 digits, so that a lexicographic listing puts the marker with the highest `N` first.
- `h` is the SHA-256 of the version's bytes, as 64 lowercase hexadecimal characters.
- `id` is a random UUID in its hyphenated, lowercase form.

Implementations SHOULD NOT create other names in `repo.d`. Readers MUST ignore names that do not parse exactly as above, and garbage collection MUST NOT delete them. The rest of this page writes `latest.<N>.<h>` for `latest.<K>.<N>.<h>`.

Every version in `repo.d`, and the mirror, is a repo info file in the standard [binary file format](./spec-v2-2.md#binary-file-format). Those that list `filesystem-layout-v1` in `Repo.writer_features` carry the [status gate](./spec-v2-2.md#repostatus).

## Reading

To read the repo info, an implementation of this layout lists `repo.d`. The marker with the highest `N` names the current version `<N>.<h>`. A concurrent change can delete that version after the listing, in which case the reader lists again. If the current version does not list `filesystem-layout-v1`, a conversion to the standard layout is in progress: implementations of this layout MUST NOT change the repository then, other than by running that conversion again.

Implementations that do not implement this layout read the mirror. The mirror can lag behind the current version, can briefly hold a first version that never became current after concurrent creations, holds the standard repo info while a conversion from the standard layout runs, and can be missing until the next time it is published. A lagging mirror can reference objects that garbage collection has deleted, so reads through it can fail. A reader that needs the latest commit should be given its snapshot id.

## Operations

**Change.** The writer writes the new version to `repo.d/stage.<id>`, hard-links it as `repo.d/<N+1>.<h'>`, then renames the marker `latest.<N>.<h>` to `latest.<N+1>.<h'>`. The rename fails if another change moved the marker first: that is a conflict. Exactly one marker exists at any time, and the version it names is complete and durable before the rename. Before writing the new version, the writer backs up the version `<N>.<h>` it read to `overwritten/`, as every update does (see the [specification](./spec-v2-2.md#updates-to-the-repo-info-file)). It SHOULD skip the backup and treat the attempt as a conflict if a listing of `repo.d` shows a marker above `N`. A writer that knows its attempt did not land, because another change moved the marker past `N` first, MAY delete that attempt's backup: no version names it, and the version it holds was backed up by the change that replaced it. It MUST NOT delete the backup of an attempt whose outcome is unknown. A failure to delete is ignored.

**Publishing the mirror.** After every change, and before returning, the writer replaces `$ROOT/repo` with a byte-identical copy of the current version, through a staged `repo.d/mirror.<id>` renamed to `$ROOT/repo`. It publishes again, up to a fixed number of rounds, while a listing shows that the current version is no longer the one it published. A publication that fails or gives up can leave in `$ROOT/repo` an older version that was current, and does not undo the change. So the mirror can lag after crashes or under heavy write contention. Publishing the mirror explicitly (in Icechunk, `FilesystemStorage.publish_mirror()`) repairs this on a best-effort basis: it publishes the version it lists, so on filesystems with stale listings, such as NFS and FUSE, it can publish an older version that was current. A change made while nothing else changes the repository makes the mirror current when its publication succeeds, once every process that changed the repository earlier has finished publishing; the check in [Copying a repository](#copying-a-repository) catches the rest. Set the status to `ReadOnly`, which is such a change, before serving a finished repository read-only. Opening the repository never publishes.

**Garbage collection.** Before deleting anything, garbage collection makes a change, with a `GCRanUpdate` entry even if it removes no snapshot, and publishes the version that change made current. If that publication leaves `$ROOT/repo` holding neither that version nor a later one, garbage collection deletes nothing. After deleting, it publishes again. The mirror can still hold an older version for a while during and after the deletions.

**Creation.** The writer builds the first version and its marker in `$ROOT/repo.d.tmp.<id>/`, puts a byte-identical copy of the first version at `$ROOT/repo` with an exclusive create (a staged `$ROOT/repo.staging.<id>`, linked as `repo`), renames the directory to `$ROOT/repo.d`, then publishes the mirror. Creation fails if `repo` or `repo.d` exists, except that it replaces an interrupted creation: a `$ROOT/repo` that lists `filesystem-layout-v1` and has only `RepoInitializedUpdate` in its ops log, while `repo.d` does not exist.

**Deleting old versions.** Implementations MUST NOT delete a version file `<i>.<x>` in `repo.d` unless a listing of `repo.d` showed, or the implementation's own rename moved the marker to, a marker `(N, h)` with `i < N`, or with `i = N` and `x ≠ h`. Garbage collection also deletes the staged files in `$ROOT` and `repo.d/`, including `$ROOT/repo.staging.<id>`, and the `repo.d.tmp.<id>/` directories older than its cutoff. A conversion to the standard layout deletes the whole directory as described below.

**Other objects** are written either to a staged `<name>.staging.<id>` renamed to `<name>`, or directly under their final name when no reader can know the name before a change makes the object reachable. No reader sees a partially written object under a name it can know.

## Forks

Implementations that do not implement this layout, such as Icechunk 2.0 to 2.3, can still change the status through the mirror. Doing so is not supported. The write removes `filesystem-layout-v1` from `$ROOT/repo` and forks the repository: `repo` and `repo.d` hold diverging histories, and those implementations can then modify the `repo` side, including garbage collection that deletes files only `repo.d` references. Before publishing the mirror, before garbage collection deletes anything, and before the change of a conversion to the standard layout, implementations of this layout MUST read `$ROOT/repo`. If its SHA-256 is the `h` of any version file of `repo.d` that the implementation listed, read or wrote, it is a copy of that version, not a fork; otherwise they decode it. If it cannot be decoded, they MUST NOT overwrite it, garbage collection MUST NOT delete anything, a conversion to the standard layout MUST NOT make its change, and they MUST report it. If it does not list `filesystem-layout-v1`, and it is not the standard repo info that a conversion from the standard layout replaces, accepted by that conversion's provenance check (see [Conversions](#conversions)), it is a fork: they MUST NOT publish the mirror, garbage collection MUST NOT delete anything, a conversion to the standard layout MUST NOT make its change, and they MUST report the fork. A mirror that lags but lists `filesystem-layout-v1` is not a fork, and neither is a missing `$ROOT/repo`. This detection is best effort: a fork written between the check and the publication is overwritten without a report. Recovery is manual and keeps one of the two histories. On other storage, the same write turns a copy into an ordinary repository in the standard layout.

## Conversions

A repository can be converted between this layout and the standard layout, in both directions. Conversions are *administrative operations*: each MUST run with nothing else using the repository, with no writer in any process and no process still publishing the mirror. What happens otherwise is undefined. An interrupted conversion MUST be completed by running the same conversion again, under the same condition, before anything else uses the repository. Running the other conversion, or changing the repository, before that rerun is outside these rules and can require manual recovery. If only implementations of this layout ran in the meantime, no data is lost; if a standard-layout write also replaced `$ROOT/repo`, recovery keeps one of two diverging histories.

A conversion changes only the writer features, the status gate and the ops log, with a `RepoStatusChangedUpdate` entry, and keeps the repository's own status, except that an `Offline` or undefined status is written as `ReadOnly`, with a reason that names the stored value.

**To the standard layout.** The writer makes a change that removes `filesystem-layout-v1` from the current version, which removes the gate and keeps the repository's own status. It publishes that version as `$ROOT/repo`, then renames `repo.d` to `repo.d.tmp.<id>` and deletes that directory. A rerun skips the change if the current version no longer lists the feature. If `repo.d` no longer exists and `$ROOT/repo` is in the standard layout, a rerun only deletes leftover `repo.d.tmp.<id>/` directories. If a write in the standard layout replaced `$ROOT/repo` between the interruption and the rerun, the rerun stops and reports it, and recovery is manual.

**From the standard layout.** The writer backs up the bytes of `$ROOT/repo` to `overwritten/`. It builds in `$ROOT/repo.d.tmp.<id>/` a first version, which is that repo info with `filesystem-layout-v1` added and an ops log that names the backup, and its marker. It renames the directory to `$ROOT/repo.d`, failing if `repo.d` exists, then publishes the mirror. A rerun that finds `repo.d`, and `$ROOT/repo` without the feature, publishes the mirror only if `$ROOT/repo` has the bytes of a backup named or linked within the three newest files of the current version's ops log: a backup that an entry of one of those files names, or one of the files that the `repo_before_updates` chain links from the current version (the second and the third); otherwise it stops and reports it. A rerun that finds a current version without the feature stops: a conversion to the standard layout is in progress.

## Copying a repository

To copy a repository in this layout consistently, set its status to `ReadOnly`, which is a change and so publishes the mirror, and keep it until the copy is complete. Copy every entry under `$ROOT`, in any order. The copy is consistent if its `repo.d` has exactly one marker, `latest.<N>.<h>`, the copied `<N>.<h>` has the SHA-256 `h` and the status `ReadOnly`, and the SHA-256 of the copied `repo` is `h`. If the marker or its version fails the check, start over. If only the copied `repo` differs, publish the mirror, copy `repo` again and check again; if it still differs, set the status to `ReadOnly` again and start over. Set the status back to `Online` when the copy is complete.

A copy on other storage can be read through its mirror, but not modified. To move a repository in this layout to other storage where it can be modified, set its status to `ReadOnly`, convert it to the standard layout, copy it, optionally convert the original back to the filesystem layout, then set the statuses of the original and the copy to `Online`.

## Requirements

The whole repository MUST be on a single filesystem, with no mount point inside `$ROOT`. Every `rename` in `$ROOT`, within a directory or between directories, MUST be atomic and decided by the server, and MUST fail if the source no longer exists. A `link` MUST fail if the new name exists, and the new name MUST show the complete content. Renaming a directory MUST be atomic, and MUST fail if the target exists, unless it is an empty directory. `unlink` MUST remove the name it is given. A listing MUST show only names that existed. `fsync` MUST be durable, and an `fsync` of a directory MUST make durable the entries in it that other clients created.

The first `N` of a new `repo.d`, made by a creation or a conversion from the standard layout, MUST exceed every `N` that an earlier `repo.d` in the same `$ROOT` reached, because a stale listing can still show the earlier directory's markers. Implementations rely on the clock of the host that creates the directory for this. If the requirement fails, for example because of clock skew between hosts, a stale listing of the earlier directory can lead garbage collection to delete the current version.
