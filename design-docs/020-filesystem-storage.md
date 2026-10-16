# Safe concurrent commits on local and network filesystems

## Problem

Icechunk's concurrency currently requires compare-and-swap: replacing the
repo info file only if it is still the version the writer read. On object
stores this is a conditional PUT (`If-Match`). `object_store`'s
`LocalFileSystem` does not implement `PutMode::Update`, so the current
filesystem backend overwrites unconditionally and concurrent commits can
silently lose updates.

Users want Icechunk on HPC and network filesystems: NFS, SMB, Lustre, GPFS,
CephFS, and others. This design adds a new experimental filesystem storage
backend with a safe conditional update.

Research done with Claude in October 2026, from published site data and the
2021 Hyperion Research industry survey, puts the share of HPC and
life-science sites using each shared filesystem at:

| Filesystem | Sites |
| --- | --- |
| NFS | 60% |
| Lustre | 26% |
| GPFS | 21% |
| BeeGFS | 11% |
| Isilon | 10% |
| Ceph | 10% |
| WEKA | 5% |

NFS is common, and has weak consistency, which is why the design relies mostly
on what every NFS client must execute on the server.

A design that keeps `repo` as a single file and replaces it with a lock-free
compare-and-swap built on hard links and `rename` loses updates on NFS: the read
it relies on to compare versions is not guaranteed to be fresh on any NFS
client. It is described at the end, under
[Rejected alternative](#rejected-alternative-compare-and-swap-on-a-single-repo-file).

## Goals and constraints

- No lost updates.
- Timeouts may affect liveness, not safety.
- Commit visibility across clients may lag: not the read-after-write of object
  stores. If users want read after commit consistency they need to use the
  snapshot id. See [Freshness and the nudge](#freshness-and-the-nudge).
- Ideally no changes to the on-disk format. Next best thing, minimize format
  differences for the filesystem, and a way to convert between the two formats
  in both directions.
- We only need to support Icechunk V2.

## Format

The repo info is no longer a single file, because we can't update in-place
safely. Each version is an immutable new file in the `$ROOT/repo.d/` directory,
where `$ROOT` is the repository's root directory. An empty marker file has a
name that says which version is current. The marker moves from one version to
the next by `rename`. `$ROOT/repo` holds the *mirror*, a byte-identical copy of
a recent version, for readers that don't implement the filesystem layout.
Everything else, chunks, manifests, snapshots, transaction logs, `overwritten/`
backups, is unchanged. On a filesystem, every other object is written to
`<name>.staging.<id>` and renamed into place, or written directly under its
final name when no reader can know that name before a change makes the object
reachable, so no reader sees a partial object. Converting a repository between
this layout and the standard layout rewrites only the repo info, in both
directions.

The new spec 2.2 allows for writer features. The repo info in this new layout
lists the `filesystem-layout-v1` writer feature. Because Icechunk 2.0 to 2.3
don't know writer features, a repo info that lists one also carries a status
*gate*: its status reads `ReadOnly`, with a reason naming the feature, and the
repository's own status moves to `status.underlying`. Icechunk 2.0 to 2.3 read
the repository through the mirror and refuse to modify it, except to change its
status, which forks the repository (unsupported, see [Forks](#forks)). Icechunk
2.4 and later decide per storage backend whether they implement the feature:
only `FilesystemStorage` does, so every other backend reads the mirror and
refuses to modify the repository.

## File names

| Path | Meaning |
| --- | --- |
| `repo` | The mirror: a byte-identical copy of a version that was current |
| `repo.d/<N>.<h>` | Bytes of version `N`, `h = sha256` |
| `repo.d/latest.<K>.<N>.<h>` | The marker: version `N` is current |
| `repo.d/stage.<id>` | Staged bytes of a change attempt |
| `repo.d/mirror.<id>` | A staged copy of the mirror |
| `repo.d.tmp.<id>/` | A `repo.d` being built (creation, conversion from the standard layout) or deleted (conversion to the standard layout) |
| `<name>.staging.<id>` | Staged bytes of the object `<name>`, in any directory |

`N` is a decimal integer below `10^15` without leading zeros, `K` is its nines'
complement in exactly 15 digits (`999999999999999 - N`, so version 1000 has
`K = 999999999998999`), `h` is 64 lowercase hex characters, and `id` a random
UUID in its hyphenated, lowercase form. A marker name parses only if `K` and `N`
agree. Version files are immutable, and markers are only renamed.

The first version of a `repo.d`, built by a creation or a conversion from the
standard layout, has `N` equal to the Unix time in milliseconds when it was
built. Each change adds one. The clock of the host that builds a `repo.d` must
not lag the highest `N` of an earlier `repo.d` at the same path, because a stale
listing can still show that directory's markers (see
[Accepted limits](#accepted-limits)).

`K` exists so that a lexicographic listing of the markers puts the newest
first. `N` is in the name for people and tools. The sections below write
`latest.<N>.<h>` for brevity.

## Algorithm overview

The marker is an empty file whose name says which version is current:
`latest.<N>.<h>` means the repo info is version `N`, with its bytes in
`repo.d/<N>.<h>`. It is one inode, never opened or written, only renamed,
so exactly one such name exists at any instant. `rename` is atomic for a
new file, so moving the marker is both the compare and the swap of a
change.

**Read.** List `repo.d` and take its marker, then read `<N>.<h>` and check
its hash.

**Change.** Write the new bytes to a staged file `repo.d/stage.<id>`, link it as
`repo.d/<N+1>.<h'>`, rename the marker in `repo.d` from `latest.<N>.<h>` to
`latest.<N+1>.<h'>`, then publish the mirror as **Publish** explains.

**Publish.** Copy the bytes of a version proved current into
`repo.d/mirror.<id>`, check `$ROOT/repo` for a fork (see [Forks](#forks)),
rename the copy over `$ROOT/repo`, and list `repo.d` again: if the marker moved
on, publish the newer version, up to four rounds.

**Status.** Setting the status `ReadOnly` is a change like any other: its
publication makes the mirror current once every earlier publication has
finished, and from then on Icechunk refuses every modification but a status
change or a conversion. It serves exports and consistent copies; setting the
status `Online` again resumes writers.

**Create.** Build `repo.d.tmp.<id>/` with `<N>.<h>` and `latest.<N>.<h>`,
place a copy of the version at `$ROOT/repo` with an exclusive link, then
rename the directory to `repo.d`. A directory rename fails if the target
exists and is not empty, so creation is exclusive.

**Convert to the standard layout.** Make a change that removes the
feature, publish that version as the mirror, rename `repo.d` to a
`repo.d.tmp.<id>` and delete it.

**Convert from the standard layout.** Back up `repo`, build `repo.d` from it as
a creation does, and publish.

## Invariants

1. Exactly one marker exists in a `repo.d` at any instant, and no marker
   name is ever used twice in it. The marker moves only by
   `rename(latest.<N>.<h>, latest.<N+1>.<h'>)`, a change.
2. `<N+1>.<h'>` is fully written, fsynced and durably linked before the
   marker refers to it.
3. A version file `<i>.<x>` in `repo.d` is deleted only if a listing of
   `repo.d` showed, or the implementation's own rename moved the marker
   to, a marker `(N, h)` with `i < N`, or `i = N` and `x ≠ h`.
4. No rename targets an existing protocol name. The only renames onto an
   existing name are the ones onto `$ROOT/repo`, and no process renames
   onto `$ROOT/repo` while it has `$ROOT/repo` open. No marker is ever
   opened. This avoids NFS silly-rename and cache issues.
5. Every version in `repo.d` lists `filesystem-layout-v1`, except the last one,
   made by a conversion to the standard layout: a change refuses bytes that
   don't, and only that conversion's change writes them.
6. Every version a publication copies is either one that a server-decided step
   of the same operation proved current, or one that a listing taken after that
   step shows with a higher `N` than every version the operation copied before.
   The exceptions: `publish_mirror()` and a creation that lost its race
   publish the version their listing shows, unproved.

## Operations

Listings and reads are preceded by a nudge of their directory (see
[Freshness and the nudge](#freshness-and-the-nudge)).

The conversions are *administrative operations*: the user is responsible for
running them with nothing else using the repository: no writer in any process,
no process still publishing the mirror and no other administrative operation.
What happens otherwise is undefined. If one is interrupted, running the same
operation again under the same condition completes it.

### Reading

A reader lists `repo.d`, takes the marker with the highest `N`, and reads the
version it names, checking its hash. A concurrent rename can tear a listing,
showing the marker under two names or none, and a change or the sweep can delete
a listed version file before it is read; in each case the reader lists again, a
bounded number of times. A missing `repo.d` sends the reader back to
[classifying the root](#opening). If the current version doesn't list
`filesystem-layout-v1`, a conversion to the standard layout was interrupted:
reads work, but Icechunk refuses every modification, GC and `publish_mirror()`
until that conversion is rerun.

### Opening

A storage classifies the root before it first uses `repo`. If `repo.d` exists,
the repository opens from `repo.d`, without reading `$ROOT/repo`: a fork or an
unreadable mirror is found by the operations that publish or delete, not by
opening. Opening never writes or publishes: readers may lack write permission.

Without `repo.d` there is no repository in this layout. Opening reports
"not found" if `$ROOT/repo` is missing or an interrupted creation, which the
next creation resumes, and an error naming what it found otherwise.

### Change

A change from version `(N, h)` with `new_bytes`:

1. Refuse bytes that don't list `filesystem-layout-v1`.

The update's backup of `<N>.<h>` (core's `copy_object` of `repo`) copies only if
a listing of `repo.d` shows no marker above `N`; otherwise it reports the base
stale, as a copy-if-match does on object storage. A stale listing just copies.

2. Write and fsync the bytes to `repo.d/stage.<id>`, link it as
   `<N+1>.<h'>`, where `h'` is their hash, and unlink the staging name. The
   name is new and names its bytes, so nothing is overwritten, and `EEXIST`
   is our own link after a lost reply.
3. Rename the marker from `latest.<N>.<h>` to `latest.<N+1>.<h'>`. Success
   proves our base was current: tidy up, then publish the mirror (see
   [Publishing the mirror](#publishing-the-mirror)). `ENOENT` means another
   change moved the marker first, or our rename landed and its reply was lost:
   list `repo.d` again, a bounded number of times, until the listing shows a
   marker above `N`. Ours means the change landed; another `N+1` is a conflict;
   `N+2` or higher, or listings that stay stale, go to the lost-reply check:
   each attempt writes a unique `backup_path` into the ops log, so the attempt
   landed if and only if the current version's ops log (or a few pages before
   it) holds that path; otherwise it's a conflict, never a false
   acknowledgement.

An attempt proven not to have landed (another `N+1`, or a lost-reply check that
read a version above `N` and searched its ops log back past our base's entry, or
to its start, without finding our `backup_path`) deletes its backup, ignoring
errors; an attempt whose outcome is unknown keeps it. With one update per repo
info file, the search reaches back only a few changes, so a conflict lost to a
burst of later changes keeps its backup. Deployments that can create but not delete in
`overwritten/` keep the backups, as before.

**Tidy up**, after a rename proved the marker is at `(N+1, h')`: delete
every version file at or below `N`, and every other `<N+1>.<x>`, so the
directory holds only the current version plus the attempts in flight.

### Publishing the mirror

A publication copies a version proved current into `repo.d/mirror.<id>`, checks
`$ROOT/repo` for a fork (see [Forks](#forks)), and renames the copy over
`$ROOT/repo`. It then lists `repo.d`, and if the marker has moved on it
publishes the newer version, for up to four rounds; after that it gives up with
a warning naming the version it left, the same as a crash at that point. Within
one publication the target never goes backwards: it never publishes a version
older than one it already published, and when its fork check finds a newer
version already in `$ROOT/repo` it adopts that version instead of replacing it.
Every change publishes before returning.

Readers of `$ROOT/repo` never see a torn file, and always see a version that was
current, except after a creation race (see [Accepted limits](#accepted-limits)).
It can lag after a crash, an exhausted loop or a lagging listing, briefly show
an older version than before when a slow publisher's rename lands after a newer
publication, briefly be missing when an NFS client splits the rename, and
reference objects GC has deleted.

### Repairing the mirror

`FilesystemStorage::publish_mirror()` repairs the mirror when a process died
between its change and its publication, or a publication ran out of rounds, on a
repository that may then stay idle. It publishes the version the current listing
of `repo.d` names, never changes the repo info, and is safe while writers run.
The repair is best effort: under a lagging listing it can install an older
version that was current, which the next publication fixes.

### Creating consistent copies

Set the status `ReadOnly`. Copy everything under `$ROOT`, in any order. Then
check the copy: `repo.d` has one marker, `latest.<N>.<h>`, and both
`repo.d/<N>.<h>` and the copied `repo` hash to `h`. Set the status back to
`Online` when done.

While the status is `ReadOnly` nothing moves the marker or deletes data, so
`repo.d` copies the same version whenever it is read. Only the mirror can
still change, when a publisher that started before the status change lands
late; the check catches it. If only `repo` fails the check, run
`publish_mirror()` and copy `repo` again; otherwise start over.

The copy keeps the status `ReadOnly`. On a filesystem, `FilesystemStorage`
can set it `Online` and write to it; on other storage it is read-only
through its mirror. To get a writable copy on other storage, convert the
source to the standard layout before copying it.

### GC

`FilesystemStorage` leaves temporary files behind when a process crashes or
loses a race: staged bytes (`repo.d/stage.<id>`, `repo.d/mirror.<id>`), staged
copies in `$ROOT` (`<name>.staging.<id>`, `repo.staging.<id>`), build
directories (`repo.d.tmp.<id>/`), and version files the marker has moved past.
GC deletes them (the *sweep*). Version files below the marker are dead for good
and always deleted; the other temporary files only once they are older than a
cutoff (the earlier of GC's cutoff and 24 hours ago), since a running process
may still be using them. GC never deletes the marker, version files above it or
names it doesn't recognize. The sweep visits only `$ROOT` and `repo.d/`: staging
files left in the object directories (`snapshots/`, `overwritten/`, …) are not
collected.

### Creation

A creator builds the first version, `<N>.<h>` with `N` the Unix time in
milliseconds, in a fresh `repo.d.tmp.<id>/`, places a copy of it at
`$ROOT/repo`, adds the marker `latest.<N>.<h>` to the directory, and renames
the directory to `repo.d`, then publishes. Placing `$ROOT/repo` before the
rename means the mirror exists as soon as `repo.d` does. Creation fails if
`repo.d` exists, or if `$ROOT/repo` holds anything but an interrupted
creation (a repo info that lists the feature with only
`RepoInitializedUpdate` in its ops log, and no `repo.d`), which it
replaces. Creators can race: the directory rename picks one winner, and a
loser that placed `$ROOT/repo` publishes the current version before
reporting that the repository exists.

### Conversion from the standard layout

`convert_to_filesystem_layout()` backs up the standard `repo` to `overwritten/`,
builds a `repo.d.tmp.<id>/` whose first version is that repo info with
`filesystem-layout-v1` added (a `RepoStatusChangedUpdate` naming the backup),
renames the directory to `repo.d`, and publishes the mirror over the standard
`repo`. Until that publication, readers that don't implement the layout still
find the standard repo info. A rerun after a crash between the rename and the
publication finds `repo.d` next to a standard `repo`; it publishes only if a
backup named or linked within the three newest files of `repo.d`'s ops log
holds the bytes of that `repo`, proving this conversion built `repo.d` from it,
and otherwise stops and reports. Its publication's fork check accepts that
standard repo info. A rerun that finds a current version without the feature
stops: a conversion to the standard layout is in progress.

### Conversion to the standard layout

`convert_to_standard_layout()` checks for a fork, then makes a change, like any
other except that its new version doesn't list the feature: it removes
`filesystem-layout-v1` and the gate, keeping the repository's own status. The
change's marker rename proves the version it converts is current, even under a
stale listing. It then publishes that version, so `$ROOT/repo` becomes the
standard repo info, renames `repo.d` to a `repo.d.tmp.<id>` and deletes it. From
the change until the rerun finishes, the current version lacks the feature, so
nothing else modifies the repository: a rerun finds that version and only
publishes and deletes.

### Fsyncs

The protocol's fsyncs always run: the version file and its directory entry
before the marker moves, the marker's directory, the mirror's stage and `$ROOT`,
and the conversions' backups and directories. A storage setting can turn off
fsync for every other object. When fsync is on, a change first fsyncs the data
directories (the *data-directory barrier*), so the names of the objects a new
version references are durable before the marker can name it.

## Freshness and the nudge

Changes are linearizable: each one is a single rename decided by the
server. Reads are not: a reader sees another client's change once its
directory listing refreshes, so this backend gives bounded staleness, not
the cross-client read-after-write of the object-store backends. Staleness
never loses an update, since a writer on a stale base gets a conflict and
retries, and within one process reads never go back. It can make a reader
on another host miss recent changes silently, so a handoff between hosts
should pass the snapshot id: opening a snapshot that a lagging reader
doesn't see yet fails with "not found" instead of returning old data.

`nudge(dir)` asks the client to refresh what it caches about one directory,
before every listing of `repo.d` and every listing or read in `$ROOT`. On Linux
it is `statx(dir, AT_STATX_FORCE_SYNC)`, which makes the NFS or FUSE client
revalidate the directory with the server; elsewhere it is a no-op, and staleness
is bounded by the client's attribute cache timeout. It never affects safety,
only how long a client lags.

## Filesystem requirements

What the algorithm needs from the filesystem, and where:

| # | Requirement | Needed by | Without it |
| --- | --- | --- | --- |
| 1 | Atomic `rename` by name, decided by the server, failing with `ENOENT` if the source name is gone | every change, GC's and the conversion to the standard layout's included | Lost update |
| 2 | The same for renames between directories of `$ROOT` | the mirror's rename from `repo.d/mirror.<id>` to `$ROOT/repo` | A torn or missing mirror |
| 3 | `link` into a new name, complete content behind it, failing with `EEXIST` if the name exists | publishing bytes, creation's `$ROOT/repo`, conditional creates of other objects | Changes fail; creation not exclusive |
| 4 | Exclusive directory rename | creation, the conversion from the standard layout | Concurrent creations clobber each other |
| 5 | Atomic directory rename | the conversion to the standard layout's rename of `repo.d` aside, the sweep's rename of build directories | A partly deleted `repo.d` |
| 6 | `unlink` by name | tidy-up, sweep, conversions | Garbage |
| 7 | Listings that show only names that existed | reads, sweep, publications | Lost update |
| 8 | Durable `fsync` | the change path, publications, conversions, every write | Power-loss damage |
| 9 | A directory fsync on one client persists entries other clients created | the data-directory barrier | A marker naming missing data after a crash |
| 10 | The whole repository on one filesystem, with no mount point inside `$ROOT` | every rename and link between directories | Those operations fail with `EXDEV` |

## Forks

A *fork* is a `$ROOT/repo` that decodes without `filesystem-layout-v1` while
`repo.d` exists, and isn't a version of `repo.d` or the standard repo info that
an interrupted conversion from the standard layout is about to replace. It comes
from something that doesn't implement the layout writing `$ROOT/repo`, all of it
unsupported: an Icechunk 2.0 to 2.3 status change through the mirror, a write
through another backend that skips the writer-feature check, or a person or tool
copying a file over it. `repo` and `repo.d` then hold diverging histories, and
to other implementations the `repo` side is an ordinary repository they can
write and garbage-collect, deleting files only `repo.d` references. Icechunk
2.4's `local_filesystem_storage` refuses GC and every write of `repo` while
`repo.d` exists, so 2.4 itself can't fork the repository through the local
backend.

Detection, the *fork check*, is best effort. Before every publication, and
before the change of the conversion to the standard layout, the operation checks
`$ROOT/repo`: missing, a copy of a version (by hash) or a repo info that lists
the feature is not a fork; one without the feature is. On a fork nothing is
published or deleted and the operation reports it; a change that already landed
reports that it is durable. An unreadable `$ROOT/repo` is reported and never
overwritten, but not called a fork. A fork written between a check and the
rename is overwritten without a report, and a stale listing on NFS or FUSE can
produce a false report that running the operation again clears.

Recovery is manual and keeps one of the two histories: keep `repo.d`, by moving
`$ROOT/repo` aside and republishing, which drops the changes made through the
fork; or keep `repo`, by archiving and deleting `repo.d`, which leaves a
repository in the standard layout and drops the changes made through the layout
after the fork. Either way, first check that the kept history's objects still
exist, since garbage collection on the other side may have deleted some. An
*orphan mirror*, a `$ROOT/repo` that lists the feature without a `repo.d`, means
`repo.d` was deleted or not copied: restore it, or build `repo.d` from the
mirror's bytes, losing the changes after it.

## Old writers

| Writer | On a repository in this layout |
| --- | --- |
| Icechunk 2.0 to 2.3, any backend | Reads through the mirror. Modifications are refused by the gate (status `ReadOnly`). A status change succeeds and forks the repository (unsupported) |
| Icechunk 2.4 and later, any backend but `FilesystemStorage` | Reads through the mirror. Every modification, status changes included, is refused with `MissingWriterFeatures`, since the backend implements no writer feature |
| Icechunk 2.4 `local_filesystem_storage` | As above, plus the guard of [Forks](#forks) while `repo.d` exists |
| `force_write_repo_info` through another backend | Writes `$ROOT/repo` unconditionally: a fork source |

Converting a repository once every other writer has stopped is safe.

## Accepted limits

| Limit | When | Effect | Until |
| --- | --- | --- | --- |
| Fork erased | An old status change lands between a publication's fork check and its rename | The fork is overwritten without a report | Permanent; forks are unsupported |
| Mirror lag | A publisher crashes, runs out of rounds, or lists stale | `$ROOT/repo` holds an older version that was current | The next change, GC or `publish_mirror()` |
| Mirror references deleted objects | A slow publisher's older version lands after GC's publications | Mirror readers can't read objects GC deleted | The next publication |
| Unproved publications | `publish_mirror()`, or a creation that lost its race, publishes the version its listing shows | An older version that was current may be installed | The next publication |
| Never-current mirror | A creator places `$ROOT/repo`, loses the race and crashes before republishing | `$ROOT/repo` holds a first version that was never current | The next publication |
| Administrative operations with other users | Anything else runs during a conversion | Undefined | Out of scope |
| Standard write during an interrupted conversion | A library writes the standard `$ROOT/repo` between a conversion's crash and its rerun, in either direction | A fork; the rerun stops. After a conversion to the standard layout nothing is lost | Manual recovery |
| Readers during a conversion | Mid-conversion | Readers of `repo.d` and of `$ROOT/repo` see different things | The end of the conversion |
| First `N` below an earlier `repo.d`'s | A new `repo.d` starts below an earlier one's highest `N` (clock lag, or more than one change per millisecond on average) | GC from a stale listing can delete the current version | Permanent if it happens |
| Writer before a conversion's rerun | A writer changes the repository after an interrupted conversion from the standard layout, before its rerun | A false fork report; the change is durable | The rerun, while the conversion's backup is within the 3 ops-log files it reads; after more changes, manual recovery |
| Wrong conversion instead of a rerun | The conversion to the standard layout runs while one from it is interrupted | It refuses and names the conversion to rerun | The rerun |
| Delayed NFSv3 duplicate request | A retransmitted rename of `repo.d` executes after a full round trip of conversions | `repo.d` moves aside | Manual recovery; theoretical |
| Old creation while the mirror is missing | Icechunk 2.0 to 2.3 creates a repository over a root whose mirror is missing | A fork | Manual recovery |
| False fork report | The fork check's own listing of `repo.d` is stale (NFS, FUSE) | A fork report without a fork; nothing changes | Running the operation again |
| Rerun misses `repo.d` | The rerun of an interrupted conversion to the standard layout lists `$ROOT` stale (NFS, FUSE) and misses `repo.d` | It reports nothing to convert and leaves `repo.d`, whose current version lacks the feature | Running it again once listings refresh |
| Spurious conflict | Many changes land, or listings stay stale, after a change's lost reply | A conflict although the change landed | Never a false acknowledgement |

## Rejected alternative: compare-and-swap on a single `repo` file

The alternative keeps the single `repo` file of the object-store format and
replaces it with a lock-free compare-and-swap. A writer stages its new bytes
in a ticket file, takes a lock for the version it builds on by hard-linking a
file named after that version's hash, deletes the tickets of earlier lock
holders so they can never complete, reads `repo` again to confirm its base
is still current, and renames its ticket over `repo`. A lock whose holder
looks dead can be stolen by taking the next generation.

It loses updates under stress on NFSv4.2. The confirming read of `repo` is not
fresh: Linux NFS opens a cached name by file handle, and a silly rename keeps
the replaced inode alive, so a writer can confirm a base that has already
been replaced and then overwrite a newer version. With `repo` open on the
renaming client, the rename over it also becomes two RPCs, leaving `repo`
missing in between, or for good if the client dies. What NFS does guarantee
is that a successful `link`, `rename`, `unlink`, `mkdir` or `rmdir` ran on the
server against the true directory state; errors, and reads of a name whose
binding can change, may come from client caches. The design above relies on
nothing else.
