---
title: Filesystem layout, version 1
---
# Icechunk Filesystem Layout, version 1

!!! warning "Experimental"

    The filesystem layout is experimental. A later minor release may define a successor to this layout. The successor will get a new writer feature name, such as `filesystem-layout-v2`. The rules on this page for `filesystem-layout-v1` will stay as they are. See [experimental writer features](./version-policy.md#experimental-writer-features) for what the Icechunk libraries keep supporting.

!!! Note

    This page is part of the [Icechunk specification, version 2.2](./spec-v2-2.md). It defines the `filesystem-layout-v1` [writer feature](./spec-v2-2.md#writer-features).

    The key words "MUST", "MUST NOT", "REQUIRED", "SHALL", "SHALL NOT", "SHOULD", "SHOULD NOT", "RECOMMENDED", "MAY", and "OPTIONAL" in this document are to be interpreted as described in [RFC 2119](https://www.rfc-editor.org/rfc/rfc2119.html).

## Terms

- **Repo info version**, or **version**: one complete, immutable [repo info](./spec-v2-2.md#repo-info-file).
- **Current version**: the version named by the [marker](#files).
- **Change**: any update of the repo info. The changes are:
    - a commit
    - a branch change
    - a tag change
    - a status change
    - a configuration change
    - expiration
    - garbage collection
    - a conversion
- **Base**: the version that a change starts from. The change's new version is a copy of the base with the change applied.
- **Land**: a change lands when its new version becomes the current version.
- **Conflict**: the base was no longer the current version when the writer tried to change it, so the change did not land.
- **Backup**: the copy of the base that a writer makes in `overwritten/` before every change, as the [specification](./spec-v2-2.md#updates-to-the-repo-info-file) requires.
- **Mirror**: a byte-identical copy of a version at `$ROOT/repo`, normally one that was current. *Publishing the mirror* means replacing `$ROOT/repo` with a copy of the current version.

## Overview

This page defines the files that hold a repository's repo info on a filesystem, and the rules that implementations MUST follow to read and change those files. The rules ensure that a change lands only if its base is still the current version. This gives the filesystem layout the same [optimistic concurrency](./spec-v2-2.md#consistency-and-optimistic-concurrency) that a conditional update of `$ROOT/repo` gives the object store layout. Only the repo info is stored differently. Everything else is as in the [specification](./spec-v2-2.md): snapshots, manifests, chunks, transaction logs, the `overwritten/` backups, expiration and garbage collection.

The [object store layout](./spec-v2-2.md#object-store-layout) keeps the repo info in a single file, `$ROOT/repo`, and changes it with a conditional update of that file. Filesystems in general do not provide a conditional update of a file.

To make consistent changes without a conditional update of a file, the filesystem layout stores each version as a separate file in `$ROOT/repo.d/`. An empty [marker](#files) file names the current version. Every change except a conversion from the object store layout is a single `rename` of the marker, from the base to the new version, and the filesystem server decides that rename.

`$ROOT/repo` is the [mirror](#the-mirror-and-the-writer-feature), a copy of a version for implementations that do not implement this layout.

## Files

In `$ROOT`:

| Name | Meaning |
| --- | --- |
| `repo` | The mirror |
| `repo.d/` | Directory for repo info for filesystems |
| `repo.d.tmp.<id>/` | A `repo.d` being built by a creation or a conversion from the object store layout, or being deleted by a conversion to the object store layout |

In `$ROOT/repo.d/`, and in a `$ROOT/repo.d.tmp.<id>/` while it is built:

| Name | Meaning |
| --- | --- |
| `<N>.<h>` | Version `N` of the repo info, whose SHA-256 is `h` |
| `latest.<K>.<N>.<h>` | The marker: version `N`, with hash `h`, is current |
| `stage.<id>` | Staged bytes of a new version |
| `mirror.<id>` | Staged bytes of the mirror |

Where:

- `N` is a decimal integer below 10<sup>15</sup>, without leading zeros. The first version in a `repo.d` has `N` equal to the Unix time in milliseconds when it was created, and each change adds one.
- `K` is `999999999999999 - N` in exactly 15 digits, so that a lexicographic listing puts the marker with the highest `N` first.
- `h` is the SHA-256 of the version's bytes, as 64 lowercase hexadecimal characters.
- `id` is a random UUID in its hyphenated, lowercase form.

In any directory under `$ROOT`, a file whose name is `<name>.staging.<id>` holds the staged bytes of the file `<name>` in the same directory. For example, `$ROOT/repo.staging.<id>` holds the staged bytes of `$ROOT/repo`. Implementations that list a directory under `$ROOT` MUST ignore every name of the form `<name>.staging.<id>`.

Implementations SHOULD NOT create other names in `repo.d`. Readers MUST ignore names that do not parse exactly as above, and garbage collection MUST NOT delete them. The rest of this page writes `latest.<N>.<h>` for `latest.<K>.<N>.<h>`.

Every version in `repo.d` MUST be a repo info file in the [binary file format](./spec-v2-2.md#binary-file-format).

## The mirror and the writer feature

`$ROOT/repo` is the mirror. It is a byte-identical copy of a version in `repo.d/`, normally one that was current (see [Reading](#reading)).

Every version in `repo.d/` MUST list the `filesystem-layout-v1` writer feature, except the last version written by a [conversion to the object store layout](#conversions). Listing a writer feature turns on the [status gate](./spec-v2-2.md#repostatus): the version's `status` is `ReadOnly`, and the repository's own status is in `status.underlying`. Because the mirror is a copy of a version, it lists the feature and carries the gate as well. As a result:

- An implementation that knows about writer features, but does not implement `filesystem-layout-v1`, refuses to modify the repository, as [Modify a Repository](./spec-v2-2.md#modify-a-repository) requires.
- An implementation from before writer features existed sees the status `ReadOnly`. It refuses every change except a status change, and a status change creates a [fork](#forks).

To implement `filesystem-layout-v1`, an implementation MUST read and write the repo info as this page describes, on a filesystem that meets the [requirements](#requirements). Implementations that do not implement it MUST NOT modify the repository. They can read it through the mirror. Implementations MUST NOT write `$ROOT/repo` except to create the repository, to publish the mirror, or to convert the repository to the object store layout.

## Reading

To read the repo info, an implementation of this layout lists `repo.d`. The marker with the highest `N` names the current version `<N>.<h>`. A concurrent change can delete that version after the listing, in which case the reader lists again. If the current version does not list `filesystem-layout-v1`, a conversion to the object store layout is in progress: implementations of this layout MUST NOT change the repository then, other than by running that conversion again.

Implementations that do not implement this layout read the mirror. The mirror can lag behind the current version, can briefly hold a first version that never became current after concurrent creations, holds the original repo info while a conversion from the object store layout runs, and can be missing until the next time it is published. A lagging mirror can reference objects that garbage collection has deleted, so reads through it can fail. A reader that needs the latest commit should be given its snapshot id.

## Operations

### Change

The base is `repo.d/<N>.<h>`, and `B` is the bytes of the new version. To make a change, an implementation MUST make the checks that [Modify a Repository](./spec-v2-2.md#modify-a-repository) requires against the base, then take the steps below in order. Every step is REQUIRED except step 7, which is RECOMMENDED. When a step says that the change is a conflict, the implementation MUST stop and report a conflict. When a step says to stop with an error, the implementation MUST stop and report that error.

1. **Back up the base.**

    Copy the base's version file to a new backup in `overwritten/`, as the [specification](./spec-v2-2.md#updates-to-the-repo-info-file) requires.

    - If a listing of `repo.d` shows a marker with a number above `N`, do not write the backup: the change is a conflict.
    - If the base's version file cannot be read, do not write the backup: the change is a conflict.
    - If the SHA-256 of the bytes read is not `h`, stop with an error.

2. **Check the new bytes.**

    `B` MUST decode as a repo info, and MUST list `filesystem-layout-v1`. Otherwise, stop with an error.

3. **Make the names of data files durable.**

    Fsync `$ROOT`, and each of its directories `snapshots/`, `manifests/`, `transactions/`, `chunks/` and `overwritten/` that exists. This makes the names of the files that `B` references durable before any marker names `B`.

4. **Stage the new version.**

    Write `B` to `repo.d/stage.<id>`, with a new `id`, and fsync the file.

    - If `repo.d` does not exist, the change is a conflict.
    - On any other error, stop with an error.

5. **Link the new version.**

    Create `repo.d/<N+1>.<h'>`, where `h'` is the SHA-256 of `B`, as a hard link to `repo.d/stage.<id>`.

    - If `repo.d/<N+1>.<h'>` already exists, continue. Its name contains the hash of `B`, so it holds `B`.
    - If `repo.d/stage.<id>` no longer exists, the change is a conflict.
    - On any other error, stop with an error.

6. **Make the link durable.**

    Fsync `repo.d`. If the fsync fails, stop with an error.

7. **Delete the staged file.**

    Delete `repo.d/stage.<id>`. Also delete it when stopping at step 5 or 6. The implementation MAY ignore an error from the deletion.

8. **Rename the marker.**

    Rename `repo.d/latest.<N>.<h>` to `repo.d/latest.<N+1>.<h'>`. The source is the base's marker, whatever a listing of `repo.d` shows.

    - If the rename succeeds, the change landed. Fsync `repo.d`. If that fsync fails, stop with an error: the change landed, but may not be durable. After the fsync, the implementation MAY delete the version files that [Deleting old versions](#deleting-old-versions) allows.
    - If the rename fails because `repo.d/latest.<N>.<h>` does not exist, follow [Resolving a failed rename](#resolving-a-failed-rename). The change may have landed: if the reply to a rename that succeeded is lost, a retry of the rename fails this way.
    - On any other error, stop with an error. The rename may have landed, so `repo.d/<N+1>.<h'>` MUST NOT be deleted except as [Deleting old versions](#deleting-old-versions) allows.

9. **Publish the mirror**, as [Publishing the mirror](#publishing-the-mirror) describes.

### Resolving a failed rename

When the rename of [step 8](#change) fails because `repo.d/latest.<N>.<h>` does not exist, either another change moved the marker first, or the rename landed and its reply was lost. To find out which, an implementation MUST take the steps below in order. Steps 1 to 4 are REQUIRED, and step 5 is OPTIONAL. `N`, `h`, `h'`, `B`, *conflict* and *stop with an error* mean what they mean in [Change](#change). Unless they stop with an error, steps 1 to 3 find one of three outcomes: the change landed, the change is a conflict, or the outcome is unknown.

1. **List `repo.d`.**

    List `repo.d` until a listing shows a marker with a number above `N`. The implementation MUST bound the number of listings. A listing that finds no `repo.d` shows no marker. If a listing fails, stop with an error.

    Take the marker with the highest number in that listing:

    - If it is `latest.<N+1>.<h'>`, the change landed. Continue with step 8 of Change, as if the rename had succeeded.
    - If it is `latest.<N+1>.<x>`, with `x ≠ h'`, the change is a conflict. The implementation MAY delete `repo.d/<N+1>.<h'>`. Go to step 4.
    - If its number is `N+2` or higher, go to step 2. Whatever the later steps find, no marker will name `repo.d/<N+1>.<h'>`, so the implementation MAY delete it.

    If no listing within the bound shows a marker above `N`, go to step 2. `repo.d/<N+1>.<h'>` MUST NOT be deleted in this case, except as [Deleting old versions](#deleting-old-versions) allows.

2. **Read the current version.**

    Read the current version as [Reading](#reading) describes. While its number is not above `N`, read it again, up to a bound. Call the last version read `C`. `C` is *fresh* if its number is above `N`.

    - If `repo.d` does not exist, the outcome is unknown. Go to step 4.
    - If a read fails in any other way, stop with an error.
    - If `C` is `<N+1>.<h'>`, the change landed. Continue with step 8 of Change, as if the rename had succeeded.

3. **Search the ops log of `C`.**

    The search looks for the name of the change's backup in the [ops log](./spec-v2-2.md#update) of `C`. `B` records that name as the `backup_path` of its second-newest entry, or as its `repo_before_updates` when `B` has only one entry. If `B` records no such name, the outcome is unknown. Go to step 4.

    Search `C` for the name. Look at the `backup_path` of each entry, newest first, then at `repo_before_updates`. If the name is not found and the file has a `repo_before_updates`, read `overwritten/<repo_before_updates>` and search it the same way. The implementation MAY stop after any number of files, at least one.

    - If a file cannot be decoded, or a read fails for a reason other than a missing file, stop with an error.
    - If the name is found, the change landed. Continue with step 9 of Change.
    - If the name is not found, `C` is fresh, and the search either reached a file without `repo_before_updates` or passed the name of the base's backup, the change is a conflict. The name of the base's backup is the name that follows the change's backup name in `B`, in the order of the search.
    - Otherwise, the outcome is unknown. This includes a search that stopped at a missing file or at the implementation's limit.

4. **Report the outcome.**

    If the change is a conflict, or the outcome is unknown, report a conflict. The implementation MUST NOT report that the change landed unless step 1, 2 or 3 found that it landed.

5. **Delete the backup of a conflict.**

    A writer that knows its change did not land, because another change moved the marker past `N` first, MAY delete that change's backup: no version names it, and the version it holds was backed up by the change that replaced it. It MUST NOT delete the backup of a change whose outcome is unknown. A failure to delete is ignored.

### Publishing the mirror

After every change, and before returning, the writer replaces `$ROOT/repo` with a byte-identical copy of the current version, through a staged `repo.d/mirror.<id>` renamed to `$ROOT/repo`. It publishes again, up to a fixed number of rounds, while a listing shows that the current version is no longer the one it published. A publication that fails or gives up can leave in `$ROOT/repo` an older version that was current, and does not undo the change. So the mirror can lag after crashes or under heavy write contention. Publishing the mirror explicitly (in Icechunk, `FilesystemStorage.publish_mirror()`) repairs this on a best-effort basis.

**Garbage collection.** Before deleting anything, garbage collection makes a change, with a `GCRanUpdate` entry even if it removes no snapshot, and publishes the version that change made current. If that publication leaves `$ROOT/repo` holding neither that version nor a later one, garbage collection deletes nothing. After deleting, it publishes again. The mirror can still hold an older version for a while during and after the deletions.

**Creation.** The writer builds the first version and its marker in `$ROOT/repo.d.tmp.<id>/`, puts a byte-identical copy of the first version at `$ROOT/repo` with an exclusive create (a staged `$ROOT/repo.staging.<id>`, linked as `repo`), renames the directory to `$ROOT/repo.d`, then publishes the mirror. Creation fails if `repo` or `repo.d` exists, except that it replaces an interrupted creation: a `$ROOT/repo` that lists `filesystem-layout-v1` and has only `RepoInitializedUpdate` in its ops log, while `repo.d` does not exist.

### Deleting old versions

Implementations MUST NOT delete a version file `<i>.<x>` in `repo.d` unless they have seen the marker name a version `<N>.<h>` with either:

- `i < N`, or
- `i = N` and `x ≠ h`, a version that lost the race for `N`.

They see the marker in a listing of `repo.d`, or through their own rename of the marker to `<N>.<h>`.

Garbage collection also deletes the staged files in `$ROOT` and `repo.d/`, including `$ROOT/repo.staging.<id>`, and the `repo.d.tmp.<id>/` directories older than its cutoff. A conversion to the object store layout deletes the whole directory as described below.

**Other objects** are written either to a staged `<name>.staging.<id>` renamed to `<name>`, or directly under their final name when no reader can know the name before a change makes the object reachable. No reader sees a partially written object under a name it can know.

## Forks

Implementations that do not implement this layout, such as Icechunk 2.0 to 2.3, can still change the status through the mirror. The write removes `filesystem-layout-v1` from `$ROOT/repo` and forks the repository: `repo` and `repo.d` hold diverging histories, and those implementations can then modify the `repo` side, including garbage collection that deletes files only `repo.d` references. Before publishing the mirror, before garbage collection deletes anything, and before the change of a conversion to the object store layout, implementations of this layout MUST read `$ROOT/repo`. If its SHA-256 is the `h` of any version file of `repo.d` that the implementation listed, read or wrote, it is a copy of that version, not a fork; otherwise they decode it. If it cannot be decoded, they MUST NOT overwrite it, garbage collection MUST NOT delete anything, a conversion to the object store layout MUST NOT make its change, and they MUST report it. If it does not list `filesystem-layout-v1`, and it is not the original repo info that a conversion from the object store layout replaces, accepted by that conversion's provenance check (see [Conversions](#conversions)), it is a fork: they MUST NOT publish the mirror, garbage collection MUST NOT delete anything, a conversion to the object store layout MUST NOT make its change, and they MUST report the fork. A mirror that lags but lists `filesystem-layout-v1` is not a fork, and neither is a missing `$ROOT/repo`. This detection is best effort: a fork written between the check and the publication is overwritten without a report. Recovery is manual and keeps one of the two histories. On other storage, the same write turns a copy into an ordinary repository in the object store layout.

## Conversions

A repository can be converted between this layout and the object store layout. How an implementation converts is not part of this specification, but a conversion MUST change only the writer features, the status gate and the ops log, with a `RepoStatusChangedUpdate` entry, and keep the repository's own status, except that an `Offline` or undefined status is written as `ReadOnly`, with a reason that names the stored value.

A conversion to the object store layout ends with `$ROOT/repo` holding the converted version and no `repo.d`. A conversion from the object store layout ends with a `repo.d` whose first version is the repo info of `$ROOT/repo` with `filesystem-layout-v1` added, published as the mirror. An interrupted conversion SHOULD be completed by running it again with the implementation that started it, before anything else uses the repository.

## Requirements

The whole repository MUST be on a single filesystem, with no mount point inside `$ROOT`. Every `rename` in `$ROOT`, within a directory or between directories, MUST be atomic and decided by the server, and MUST fail if the source no longer exists. A `link` MUST fail if the new name exists, and the new name MUST show the complete content. Renaming a directory MUST be atomic, and MUST fail if the target exists, unless it is an empty directory. `unlink` MUST remove the name it is given. A listing MUST show only names that existed. `fsync` MUST be durable, and an `fsync` of a directory MUST make durable the entries in it that other clients created.

The first `N` of a new `repo.d`, made by a creation or a conversion from the object store layout, MUST exceed every `N` that an earlier `repo.d` in the same `$ROOT` reached, because a stale listing can still show the earlier directory's markers. Implementations rely on the clock of the host that creates the directory for this. If the requirement fails, for example because of clock skew between hosts, a stale listing of the earlier directory can lead garbage collection to delete the current version.
