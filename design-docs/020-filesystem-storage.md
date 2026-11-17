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
the next by `rename`. There is no `$ROOT/repo`, except during a conversion.
Everything else, chunks, manifests, snapshots, transaction logs, `overwritten/`
backups, is unchanged. On a filesystem, every other object is written to
`<name>.staging.<id>` and renamed into place, or written directly under its
final name when no reader can know that name before a change makes the object
reachable, so no reader sees a partial object. Converting a repository between
this layout and the *object store layout*, the single `$ROOT/repo` file that
every other backend updates by conditional writes, rewrites only the repo info,
in both directions.

The new spec 2.2 allows for writer features. The repo info in this new layout
lists the `filesystem-layout-v1` writer feature. Icechunk 2.4 and later decide
per storage backend whether they implement the feature: only
`FilesystemStorage` does, so every other backend refuses to modify a repo info
that lists it, wherever it finds one. Implementations that don't implement the
layout, Icechunk 2.0 to 2.3 and every other backend, look for `$ROOT/repo`, don't
find it, and fail with "the repository doesn't exist".

## File names

| Path | Meaning |
| --- | --- |
| `repo` | The repo info of the object store layout, only during a conversion |
| `repo.d/<N>.<h>` | Bytes of version `N`, `h = sha256` |
| `repo.d/latest.<K>.<N>.<h>` | The marker: version `N` is current |
| `repo.d/stage.<id>` | Staged bytes of a change attempt |
| `repo.d.tmp.<id>/` | A `repo.d` being built (creation, conversion from the object store layout) or deleted (conversion to the object store layout) |
| `<name>.staging.<id>` | Staged bytes of the object `<name>`, in any directory |

`N` is a decimal integer below `10^15` without leading zeros, `K` is its nines'
complement in exactly 15 digits (`999999999999999 - N`, so version 1000 has
`K = 999999999998999`), `h` is 64 lowercase hex characters, and `id` a random
UUID in its hyphenated, lowercase form. A marker name parses only if `K` and `N`
agree. Version files are immutable, and markers are only renamed.

The first version of a `repo.d`, built by a creation or a conversion from the
object store layout, has `N` equal to the Unix time in milliseconds when it was
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
`repo.d/<N+1>.<h'>`, and rename the marker in `repo.d` from `latest.<N>.<h>` to
`latest.<N+1>.<h'>`.

**Status.** Setting the status `ReadOnly` is a change like any other: from then
on Icechunk refuses every modification but a status change or a conversion. It
serves exports and consistent copies; setting the status `Online` again resumes
writers.

**Create.** Build `repo.d.tmp.<id>/` with `<N>.<h>` and `latest.<N>.<h>`, then
rename the directory to `repo.d`. A directory rename fails if the target
exists and is not empty, so creation is exclusive.

**Convert to the object store layout.** Make a change that removes the
feature, create `$ROOT/repo` with that version, rename `repo.d` to a
`repo.d.tmp.<id>` and delete it.

**Convert from the object store layout.** Back up `repo`, build `repo.d` from
it as a creation does, and unlink `repo`.

## Invariants

1. Exactly one marker exists in a `repo.d` at any instant, and no marker
   name is ever used twice in it. The marker moves only by
   `rename(latest.<N>.<h>, latest.<N+1>.<h'>)`, a change.
2. `<N+1>.<h'>` is fully written, fsynced and durably linked before the
   marker refers to it.
3. A version file `<i>.<x>` in `repo.d` is deleted only if a listing of
   `repo.d` showed, or the implementation's own rename moved the marker
   to, a marker `(N, h)` with `i < N`, or `i = N` and `x ≠ h`.
4. No rename or link replaces an existing protocol name: a name in `repo.d`,
   `repo.d` itself or `$ROOT/repo`. No marker is ever opened. This avoids NFS
   silly-rename and cache issues.
5. Every version in `repo.d` lists `filesystem-layout-v1`, except the last one,
   made by a conversion to the object store layout: a change refuses bytes that
   don't, and only that conversion's change writes them.

## Operations

Listings and reads are preceded by a nudge of their directory (see
[Freshness and the nudge](#freshness-and-the-nudge)).

The conversions are *administrative operations*: the user is responsible for
running them with nothing else using the repository: no writer in any process
and no other administrative operation. What happens otherwise is undefined. If
one is interrupted, running the same operation again under the same condition
completes it.

### Reading

A reader lists `repo.d`, takes the marker with the highest `N`, and reads the
version it names, checking its hash. A concurrent rename can tear a listing,
showing the marker under two names or none, and a change or the sweep can delete
a listed version file before it is read; in each case the reader lists again, a
bounded number of times. A missing `repo.d` sends the reader back to
[classifying the root](#opening). If the current version doesn't list
`filesystem-layout-v1`, a conversion to the object store layout was interrupted:
reads work, but Icechunk refuses every modification, GC included, until that
conversion is rerun.

### Opening

A storage classifies the root before it first uses the repo info. If `repo.d`
exists, the repository opens from `repo.d`, without reading `$ROOT/repo`.
Opening never writes: readers may lack write permission.

Without `repo.d` there is no repository in this layout. If `$ROOT/repo` exists,
the repository is in the object store layout, and opening reports an error
naming `convert_to_filesystem_layout()`; otherwise it reports "not found".

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
   proves our base was current: tidy up. `ENOENT` means another
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

### Creating consistent copies

Set the status `ReadOnly`. Copy everything under `$ROOT`, in any order. Then
check the copy: `repo.d` has one marker, `latest.<N>.<h>`, and
`repo.d/<N>.<h>` hashes to `h` and has the status `ReadOnly`; otherwise start
over. Set the status back to `Online` when done.

While the status is `ReadOnly` nothing moves the marker or deletes data, so
`repo.d` copies the same version whenever it is read.

The copy keeps the status `ReadOnly`. On a filesystem, `FilesystemStorage`
can set it `Online` and write to it. Other backends can't open it:
to put a copy on other storage, convert the source, or a copy of it on a
filesystem, to the object store layout first, then upload it.

### GC

`FilesystemStorage` leaves temporary files behind when a process crashes or
loses a race: staged bytes (`repo.d/stage.<id>`), staged copies in `$ROOT`
(`<name>.staging.<id>`, `repo.staging.<id>`), build directories
(`repo.d.tmp.<id>/`), and version files the marker has moved past.
GC deletes them (the *sweep*). Version files below the marker are dead for good
and always deleted; the other temporary files only once they are older than a
cutoff (the earlier of GC's cutoff and 24 hours ago), since a running process
may still be using them. GC never deletes the marker, version files above it or
names it doesn't recognize. The sweep visits only `$ROOT` and `repo.d/`: staging
files left in the object directories (`snapshots/`, `overwritten/`, …) are not
collected. The sweep changes no repo info: as on object stores, GC makes a
change whenever it is configured to delete snapshots, as a default GC is, even
if it drops none.

### Creation

A creator builds the first version, `<N>.<h>` with `N` the Unix time in
milliseconds, in a fresh `repo.d.tmp.<id>/`, adds the marker `latest.<N>.<h>`
to the directory, and renames the directory to `repo.d`: that rename creates the
repository. Creation fails if `repo.d` or `$ROOT/repo` exists. Creators can
race: the directory rename picks one winner, and a loser discards its directory
and reports that the repository exists. An interrupted creation leaves only its
`repo.d.tmp.<id>/`, which the sweep deletes.

### Conversion from the object store layout

`convert_to_filesystem_layout()` backs up `repo` to `overwritten/`, builds a
`repo.d.tmp.<id>/` whose first version is that repo info with
`filesystem-layout-v1` added (a `RepoStatusChangedUpdate` naming the backup),
renames the directory to `repo.d`, then unlinks `$ROOT/repo` and fsyncs
`$ROOT`. Until that unlink, readers that don't implement the layout still find
the original repo info. A rerun after a crash between the rename and the unlink
finds `repo.d` next to `$ROOT/repo`; it unlinks `repo` only if a backup named or
linked within the three newest files of `repo.d`'s ops log holds the bytes of
that `repo`, proving this conversion built `repo.d` from it, and otherwise stops
and reports, leaving recovery to the user. A rerun that finds only `repo.d`
reports that there is nothing to convert. A rerun that finds a current version
without the feature stops: a conversion to the object store layout is in
progress.

### Conversion to the object store layout

`convert_to_object_store_layout()` refuses to start while `$ROOT/repo` exists,
naming an interrupted conversion from the object store layout as the likely
cause: rerunning that one completes it. Otherwise it makes a change, like any
other except that its new version doesn't list the feature: it removes
`filesystem-layout-v1`, keeping the repository's status. The change's marker
rename proves the version it converts is current, even under a stale listing.
It then creates `$ROOT/repo` with that version, exclusively: it stages
`repo.staging.<id>`, links it as `$ROOT/repo` and unlinks the stage. It fsyncs
`$ROOT`, renames `repo.d` to a `repo.d.tmp.<id>` and deletes it. If
`$ROOT/repo` appears before the link, the run fails and keeps `repo.d`. From
the change on, the current version lacks the feature, so `FilesystemStorage`
modifies the repository only by rerunning the conversion. A rerun finds that
version and keeps an existing `$ROOT/repo` as it is only if it is a regular
file that decodes as a repo info without the feature: the conversion's own
write, an object-store-layout write made after it, or the write that made the
first run fail. Anything else stops the rerun with `repo.d` kept, for manual
recovery. A rerun creates `$ROOT/repo` if it is missing, then renames `repo.d`
aside and deletes it. No step replaces an existing `$ROOT/repo`.

### Fsyncs

The protocol's fsyncs always run: the version file and its directory entry
before the marker moves, the marker's directory, and the conversions' backups,
`$ROOT/repo` and directories. A storage setting can turn off fsync for every
other object. When fsync is on, a change first fsyncs the data
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
| 1 | Atomic `rename` by name, decided by the server, failing with `ENOENT` if the source name is gone | every change, including GC's and that of the conversion to the object store layout | Lost update |
| 2 | `link` into a new name, complete content behind it, failing with `EEXIST` if the name exists | a change's version file, the conversions' backup and `$ROOT/repo`, conditional creates of other objects | Changes fail; conditional creates not exclusive |
| 3 | Exclusive directory rename | creation, the conversion from the object store layout | Concurrent creations clobber each other |
| 4 | Atomic directory rename | the rename of `repo.d` aside by the conversion to the object store layout, the sweep's rename of build directories | A partly deleted `repo.d` |
| 5 | `unlink` by name | tidy-up, sweep, conversions | Garbage |
| 6 | Listings that show only names that existed | reads, sweep | Lost update |
| 7 | Durable `fsync` | the change path, conversions, every write | Power-loss damage |
| 8 | A directory fsync on one client persists entries other clients created | the data-directory barrier | A marker naming missing data after a crash |
| 9 | The whole repository on one filesystem, with no mount point inside `$ROOT` | the directory renames in `$ROOT` | Those renames fail |

## Old writers

| Writer | On a repository in this layout |
| --- | --- |
| Icechunk 2.0 to 2.3, any backend | No repository (`$ROOT/repo` is absent) |
| Icechunk 2.4 and later, any backend but `FilesystemStorage` | No repository (`$ROOT/repo` is absent) |
| `force_write_repo_info`, or `create(check_clean_root=False)`, through another backend or Icechunk 2.0 to 2.3 | Writes an unrelated object-store-layout repository; its GC can delete objects `repo.d` references (unsupported) |

Converting a repository once every other writer has stopped is safe.

## Accepted limits

| Limit | When | Effect | Until |
| --- | --- | --- | --- |
| Administrative operations with other users | Anything else runs during a conversion | Undefined | Out of scope |
| Object-store-layout write during an interrupted conversion from the object store layout | A library writes `$ROOT/repo` as in the object store layout between the directory rename and the rerun | The rerun's provenance check fails and it stops: `repo` and `repo.d` hold diverging histories | Manual recovery |
| Readers during a conversion | Mid-conversion | Readers of `repo.d` and of `$ROOT/repo` see different things | The end of the conversion |
| First `N` below an earlier `repo.d`'s | A new `repo.d` starts below an earlier one's highest `N` (clock lag, or more than one change per millisecond on average) | GC from a stale listing can delete the current version | Permanent if it happens |
| Writer before a conversion's rerun | A writer changes the repository after an interrupted conversion from the object store layout, before its rerun | `$ROOT/repo`, the original repo info, stays next to `repo.d`; the change is durable | The rerun, while the conversion's backup is within the 3 ops-log files it reads; after more changes, the rerun stops and recovery is manual |
| Wrong conversion instead of a rerun | The conversion to the object store layout runs while one from it is interrupted | It refuses, since `$ROOT/repo` exists, and names the conversion to rerun | The rerun |
| Delayed NFSv3 duplicate request | A retransmitted rename of `repo.d` executes after a full round trip of conversions | `repo.d` moves aside | Manual recovery; theoretical |
| Unrelated repository over the root | Icechunk 2.0 to 2.3, or any backend, uses `create(check_clean_root=False)` or `force_write_repo_info` over a root in this layout | An unrelated object-store-layout repository at `$ROOT/repo`; its GC can delete objects `repo.d` references | Permanent; unsupported |
| Rerun misses `repo.d` | The rerun of an interrupted conversion to the object store layout lists `$ROOT` stale (NFS, FUSE) and misses `repo.d` | It reports nothing to convert, or that the repository doesn't exist if the interrupted run was killed before writing `repo`, and leaves `repo.d`, whose current version lacks the feature | Running it again once listings refresh |
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
