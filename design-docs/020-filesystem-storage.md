# Safe concurrent commits on local and network filesystems

## Problem

Icechunk's concurrency currently requires compare-and-swap: replacing the
`repo` info file only if it is still the version the writer read. On object
stores this is a conditional PUT (`If-Match`). `object_store`'s
`LocalFileSystem` does not implement `PutMode::Update`, so the current
filesystem backend overwrites unconditionally and concurrent commits can
silently lose updates.

Users want Icechunk on HPC and network filesystems: NFS, SMB, Lustre, GPFS,
CephFS, and others. This design adds a new experimental filesystem storage backend with a
safe conditional update.

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

NFS is common, and has weak consistency, which is why the design
relies mostly on what every NFS client must execute on the server.

The first design kept `repo` as a single file and replaced it with a
lock-free compare-and-swap built on hard links and `rename`. It was
implemented and stress-tested, and it lost updates on NFS: the read it
relied on to compare versions is not guaranteed to be fresh on any NFS
client. It is kept at the end, under
[Previous attempt](#previous-attempt-compare-and-swap-on-a-single-repo-file).
This document describes its replacement that seems to work.

## Goals and constraints

- No lost updates.
- Timeouts may affect liveness, not safety.
- Commit visibility across clients may lag: not the read-after-write
  of object stores. If users want read after commit consistency they
  need to use the snapshot id. See [Freshness and the nudge](#freshness-and-the-nudge).
- Ideally no changes to the on-disk format
  (not possible because of NFS non-atomic renames). Next best thing,
  minimize format differences for the filesystem,  and a way to convert
  between the two formats in both directions.
- Keep it as minimal and simple as possible.
- We only need to support Icechunk V2.

## Format

The filesystem format replaces the `repo` object with a stub file and a
directory of immutable versions. The commit becomes a single
server-decided `rename`. Everything else, chunks, manifests, snapshots,
transaction logs, `overwritten/` backups, GC and expiration, is unchanged.
Old clients that open a repo in this format get a clear error when they
try to parse the `repo` stub.

## Names

| Path | Meaning |
| --- | --- |
| `repo` | Stub file, see below |
| `repo.d/<N>.<h>` | Bytes of version `N`, `h = sha256` |
| `repo.d/latest.<K>.<N>.<h>` | The marker: version `N` is current |
| `repo.d/frozen.<K>.<N>.<h>` | The marker while frozen |
| `repo.d/stage.<id>` | Staged bytes of an attempt |
| `repo.d.tmp.<id>/` | A repo being created or migrated |
| `repo.legacy.<id>` | A single-file repo being migrated |
| `repo.stub.<id>` | A staged stub |

`N` is a decimal integer below `10^15` without leading zeros, `K` is its
nines' complement in exactly 15 digits (`999999999999999 - N`, so version
1000 has `K = 999999999998999`), `h` is 64 lowercase hex characters, and
`id` a random UUID. A marker name parses only if `K` and `N`
agree. `VersionInfo` is `etag = h`, `generation = N`; `IfVersion` needs
both. Version files and the stub are immutable.

`K` exists so that a lexicographic listing of `latest.*` puts the newest
marker first. `N` is in the name for
people and tools. The sections below write `latest.<N>.<h>` and
`frozen.<N>.<h>` for brevity.

## Algorithm overview

The marker is an empty file whose name says which version is current:
`latest.<N>.<h>` means the RepoInfo is version `N`, with its bytes in
`repo.d/<N>.<h>`. It is one inode, never opened or written, only renamed,
so exactly one such name exists at any instant. `rename` is atomic for a new file,
so moving the marker is both the compare and the
swap of a commit. No files are read except for `<N>.<h>`, which are immutable.

**Read.** List `repo.d` and take its marker. A listing is served in several
`readdir` calls and a rename can land between them, so it can show the
marker under two names, the old and the new, or under none. *The marker of
a listing* is its `latest.*` or `frozen.*` entry with the highest `N`, and
at equal `N` the `frozen.*` one.
No marker means a torn listing, read again. Then read `<N>.<h>` and check its
hash. If the caller already holds `(N, h)`, or a newer `N`, report
`NotModified`.

**Commit**, replacing version `(N, h)` with `new_bytes`, `h' = sha256`:

1. **Stage.** Write and fsync `stage.<id>`.
2. **Publish the bytes.** `link` the staging file as `<N+1>.<h'>` and fsync
   the directory. The name is new, so nothing is overwritten. It is also the
   hash of the bytes, so whatever holds it has our bytes: on `EEXIST` it is
   our own link retried after a lost reply.
3. **Commit.** `rename(latest.<N>.<h>, latest.<N+1>.<h'>)`. Success means
   the marker was at our base on the server at that instant, and we moved
   it: the base was current and nobody else can have replaced it. `ENOENT`
   means the marker was elsewhere, or our rename landed and the reply was
   lost; a listing tells the two apart whenever it has caught up.
4. **Tidy up.** Fsync the directory, then delete `<N>.<h>` and, from one
   listing, every other version file at or below `N` and every `<N+1>.<x>`
   with `x != h'`. The successful rename proved the marker is at
   `(N+1, h')`, so all of them are dead for good. The directory therefore
   holds only two files plus the attempts in flight.
   Readers treat any failure as "deleted under me" and list again.

**Freeze** renames the marker to `frozen.<N>.<h>`, so no commit can
succeed. Version `N` stays readable. It serves exports and consistent copies
of the directory. **Unfreeze** links `<N>.<h>` as `<N+1>.<h>` and renames
the marker to `latest.<N+1>.<h>`. Writers based on `(N, h)` get one conflict.

**Create** builds `repo.d.tmp.<id>/` with `0.<h>` and `latest.0.<h>`, then
renames the directory to `repo.d`. A directory rename fails if the target
exists and is not empty, so creation is exclusive too. The stub is linked
as `repo` first.

**Sweep**, run by GC from one listing with marker `(N, h)`: delete every
`<i>.<x>` with `i < N`, and `<N>.<x>` with `x != h`.
Files above `N` belong to attempts in
flight. Staging files, including the root's staging files of other
objects, and abandoned creation directories go by age.

**Migrate** converts a single-file repo offline: rename `repo` aside to
`repo.legacy.<id>`, read it, build `repo.d` as in create, link the stub,
delete the legacy file. Every step is idempotent.

The stub is a V2 file header with the new file type `FilesystemStub`. Old
versions fail on the unknown type; current non-filesystem backends report
that the repository uses the filesystem format.

## Invariants

1. Exactly one marker exists at any instant, and no marker name is ever
   used twice. The marker moves only by
   `rename(latest.<N>.<h>, latest.<N+1>.<h'>)` (a commit),
   `rename(latest.<N>.<h>, frozen.<N>.<h>)` (a freeze), or
   `rename(frozen.<N>.<h>, latest.<N+1>.<h>)` (an unfreeze).
2. `<N+1>.<h'>` is fully written, fsynced and durably linked before the
   marker refers to it.
3. `<i>.<x>` is deleted only if a listing showed a marker `(N, h)` with
   `i < N`, or with `i == N` and `x != h`.
4. No rename targets an existing name, and no protocol file other than
   `<N>.<h>` is opened. This avoids NFS silly-rename and cache issues.
5. Clocks only decide the deletion of `stage.*`, `repo.stub.*` and `repo.d.tmp.*`.

## Algorithm

```rust
// Freshness only, never a safety input. See "Freshness and the nudge".
fn nudge(dir) {}

fn list_markers() -> Result<Vec<Marker>, NotFound> {
    match readdir("repo.d") {
        Ok(entries) => parse `latest.<N>.<h>` and `frozen.<N>.<h>`,
        // Possibly a cached negative entry: ask the server once.
        Err(ENOENT | ENOTDIR) => { nudge(root); readdir("repo.d") again, ENOENT => NotFound }
    }
}

// The marker of a listing: the highest N, on ties `frozen` over
// `latest`. None for a torn listing.
fn highest(markers) -> Option<Marker> {}

fn read_latest(previous: Option<(N, h)>) -> NotModified | Modified | NotFound | Error {
    for _ in 0..RETRIES (with backoff) {
        nudge("repo.d");
        let markers = list_markers()?;
        if markers.is_empty() { continue }             // torn listing, or creation in progress
        let (_, n, h) = highest(markers);
        if let Some((pn, ph)) = previous {
            if (n, h) == (pn, ph) || n < pn { return NotModified }  // equal, or a lagging listing
        }
        match read_all("repo.d/<n>.<h>") {
            Ok(bytes) if sha256(bytes) == h => return Modified { bytes, version: (n, h) },
            // Deleted under us by a commit or a sweep: ENOENT before the open,
            // ESTALE or a short read after it on NFSv3. The marker has moved
            // on; list again. A persistent error surfaces after RETRIES.
            _ => continue,
        }
    }
    // Kind Other, carrying why the last try failed; not NotFound: the repo exists.
    Error("no consistent view of repo.d", last cause)
}

fn conditional_update(new_bytes, base: (N, h)) -> Updated | NotOnLatestVersion | RepositoryFrozen | Io {
    let h2 = sha256(new_bytes);
    let stage = "repo.d/stage.<uuid>";
    write_fsync_close(stage, new_bytes)?;              // ENOENT | ENOTDIR => NotOnLatestVersion
    let content = "repo.d/<N+1>.<h2>";
    match link(stage, content) {
        Ok | Err(EEXIST) => {}                         // EEXIST: the name is the hash, so our bytes
        Err(ENOENT) => return NotOnLatestVersion,      // stage swept, or repo.d gone
        Err(e) => { unlink(stage); return Io(e) }
    }
    fsync_dir("repo.d");                               // the link is durable before the marker can reference it
    unlink(stage);
    match rename("repo.d/latest.<N>.<h>", "repo.d/latest.<N+1>.<h2>") {
        Ok => { fsync_dir("repo.d"); tidy_up(N+1, h2); Updated((N+1, h2)) }
        Err(ENOENT) => after_failed_rename(N, h, h2),
        Err(e) => Io(e),                               // the content file stays; the sweep owns it
    }
}

// After our rename proved the marker is at (n, x): delete every dead version
// file. Same rule as the sweep, with a stronger premise. Errors are ignored.
fn tidy_up(n, x) {
    unlink("repo.d/<n-1>.<base hash>");
    for <i>.<y> in readdir("repo.d") { if i < n || (i == n && y != x) { unlink } }
}

// The marker was not at our base, or our rename landed and the reply was lost.
fn after_failed_rename(N, h, h2) {
    nudge("repo.d");
    match highest(list_markers()) {
        (_, N+1, x) if x == h2 => { fsync_dir("repo.d"); tidy_up(N+1, h2); Updated((N+1, h2)) }
        (_, N+1, _)            => { unlink(content); NotOnLatestVersion }  // lost the race
        (_, m, _) if m >= N+2  => { unlink(content); NotOnLatestVersion }  // superseded either way
        // Marker names never recur, so our rename didn't land. The sweep
        // deletes the content once the unfreeze moves the marker past it.
        (Frozen, N, x) if x == h => RepositoryFrozen,
        // Ambiguous: leave the content to the sweep. Spurious only after a
        // lost reply on NFSv3/4.0, the 017 caveat.
        _                      => NotOnLatestVersion,
    }
}

fn write_stub() -> Ok | LegacyRepo | Io {
    stage "repo.stub.<uuid>", fsync;
    for _ in 0..2 {
        match link(it, "repo") {
            Ok => { unlink(staging); fsync_dir(root); return Ok }
            Err(EEXIST) => match read_all("repo") {
                the stub => { unlink(staging); fsync_dir(root); return Ok }
                missing => continue,                   // moved away since the link: link again
                _ => { unlink(staging); return LegacyRepo }
            },
            Err(e) => { unlink(staging); return Io(e) }
        }
    }
    unlink(staging); Io("another process is moving it")
}

// Never delete through `tmp`: a cached name may lead into the repo.d that
// it became. The rename to a fresh name is decided by the server.
fn discard(tmp) { let aside = "repo.d.tmp.<uuid>"; rename(tmp, aside)?; rm_rf(aside) }

fn build_repo_d(bytes) -> Created | Exists((N, h)) | Io {
    let h = sha256(bytes); let tmp = "repo.d.tmp.<uuid>";
    mkdir(tmp); write_fsync_close("tmp/0.<h>", bytes); create_fsync("tmp/latest.0.<h>"); fsync_dir(tmp);
    match rename(tmp, "repo.d") {                      // exclusive: fails if repo.d exists and isn't empty
        Ok => { fsync_dir(root); Created }
        Err(EEXIST | ENOTEMPTY) => { discard(tmp); Exists(highest(list_markers()?)) }
        Err(ENOENT) => match list_markers() {          // lost reply: tmp is gone
            Ok(m) if m == (0, h) => Created,
            Ok(m) => Exists(m),
            Err(NotFound) => Io("retry"),
        },
        Err(e) => { discard(tmp); Io(e) }
    }
}

fn create(bytes) -> Created((0, h)) | AlreadyExists | MigrationInterrupted | Io {   // put_object_conditional(repo, Create)
    create_dir_all(root); nudge(root);
    match read_all("repo") {
        Ok(the stub) | Err(ENOENT) => {}
        Ok(_) | Err(not a regular file) => return AlreadyExists,  // legacy repo: migrate first
        Err(e) => return Io(e),                        // unreadable is unknown, not "exists"
    }
    if open() == MigrationInterrupted { return MigrationInterrupted }  // creating would orphan the legacy file
    write_stub()?;                                     // LegacyRepo => AlreadyExists
    match build_repo_d(bytes) {
        Created => Created((0, sha256(bytes))),
        Exists((0, h)) if h == sha256(bytes) => Created((0, h)),   // ours, after a lost reply
        Exists(_) => AlreadyExists,
    }
}

fn copy_object(repo -> "overwritten/<id>", version: (N, h)) {  // the backup; asset manager unchanged
    let bytes = match read_all("repo.d/<N>.<h>") {
        Ok(bytes) if sha256(bytes) == h => bytes,
        Ok(_) => return Corrupt,                       // malformed data is an error
        Err(_) => return NotOnLatestVersion,           // missing or unreadable: the base is gone
    };
    put_object("overwritten/<id>", bytes)              // staging + rename into a new name
}

fn sweep(cutoff) {                                     // called by GC; one listing; no lock
    let entries = readdir("repo.d"); let marker = highest(markers of entries);
    for e in entries {
        // Dead forever (invariant 3). Without a marker no version file is deleted.
        <i>.<x> if marker is (_, N, h) && (i < N || (i == N && x != h)) => unlink,
        <i>.<x>                                => keep,    // in flight, or swept once the marker passes it
        stage.<id> older than cutoff           => unlink,  // liveness only
        latest.* | frozen.* | .nfs*            => keep,
    }
    // In the root, liveness only: `repo.legacy.*` is kept.
    discard "repo.d.tmp.*", unlink "repo.stub.*" and "<name>.staging.<id>", older than cutoff;
}

// AlreadyFrozen and NotFrozen come from a listing, which can lag: only
// Frozen and Unfrozen prove that the call acted.
fn freeze() -> Frozen((N, h)) | AlreadyFrozen | Io {
    retry: nudge("repo.d"); match highest(list_markers()) {
        Err(NotFound) => match open() { Ok => retry, status => its error },
        (Frozen, ..) => AlreadyFrozen,
        (Latest, n, h) => match rename("frozen.<n>.<h>" from "latest.<n>.<h>") {
            Ok => { fsync_dir("repo.d"); Frozen((n, h)) }
            Err(ENOENT) => retry,                      // a commit or another freeze got in first
            Err(e) => Io(e),
        },
        None => retry,
    }
}

// Advances N, so that no marker name recurs. Writers based on (N, h) get one conflict.
fn unfreeze() -> Unfrozen((N+1, h)) | NotFrozen | Io {
    retry: nudge("repo.d"); match highest(list_markers()) {
        Err(NotFound) => match open() { Ok => retry, status => its error },
        (Frozen, n, h) => {
            link("repo.d/<n>.<h>", "repo.d/<n+1>.<h>");    // Ok | EEXIST; ENOENT => retry
            fsync_dir("repo.d");
            match rename("repo.d/frozen.<n>.<h>", "repo.d/latest.<n+1>.<h>") {
                Ok => { fsync_dir("repo.d"); Unfrozen((n+1, h)) }
                Err(ENOENT) => retry,
                Err(e) => Io(e),
            }
        }
        (Latest, ..) => NotFrozen,
        None => retry,
    }
}

fn open() -> Ok | LegacyFormat | MigrationInterrupted | MixedFormats | NotFound {
    nudge(root);
    if "repo.d" exists {
        if "repo" exists and is not the stub { MixedFormats }  // a legacy writer overwrote the stub
        else { Ok }                                     // a missing stub is harmless; migrate restores it
    } else if "repo" exists {
        if it is the stub { NotFound }                  // an interrupted create; create resumes it
        else { LegacyFormat }                           // run migrate
    } else if any "repo.legacy.*" exists { MigrationInterrupted }
    else { NotFound }
}

// Offline: no legacy writer may run after this starts, and `repo` is missing
// between the rename and the stub. Every step is idempotent.
fn migrate() -> Migrated | AlreadyMigrated | NothingToMigrate | MixedFormats | Io {
    nudge(root);                                       // the client looks `repo` up again before the rename
    let legacy = if "repo" exists {
        if it is the stub {
            if "repo.d" exists { unlink any "repo.legacy.*"; return AlreadyMigrated }
            return NothingToMigrate                    // an interrupted create
        }
        // A legacy writer overwrote the stub: renaming it aside would hide
        // its update behind a repo that opens. Nothing is renamed.
        if "repo.d" exists { return MixedFormats }
        let name = "repo.legacy.<uuid>"; rename("repo", name)?; name   // use this name, don't list for it
    } else {
        match the "repo.legacy.*" in a listing of root {
            [] if "repo.d" exists => { write_stub()?; return AlreadyMigrated }  // restores a missing stub
            [] => return NothingToMigrate,
            [legacy] => legacy,                        // resuming
            _ => return Io("several legacy files: manual recovery"),
        }
    };
    let bytes = read_all(legacy)?;                     // fresh after the nudge; immutable from now on
    match build_repo_d(bytes) {
        Created => {}
        Exists((_, h)) if h == sha256(bytes) => {}     // a previous run got this far
        // Manual recovery. If an earlier run built repo.d (writers may have
        // committed since), the message says that deleting the legacy file
        // and rerunning finishes the migration.
        Exists(_) => return Io("repo.d holds other content: manual recovery"),
    }
    write_stub()?;                                     // LegacyRepo here means a legacy writer ran
    unlink(legacy); Migrated
}
```

The three fsyncs in the commit path, the content file and the two directory
fsyncs, always run, whatever the fsync setting. So do those in
`build_repo_d`, `write_stub`, `freeze` and `unfreeze`.

## Lost replies

| Operation | Spurious result | Resolution |
| --- | --- | --- |
| `link(stage, <N+1>.<h'>)` | `EEXIST` | The name is ours, by its bytes |
| `rename` of the marker | `ENOENT` | Re-list: `(N+1, h')` is a success; otherwise a conflict, spurious if a successor already landed |
| `rename(tmp, repo.d)` | `ENOENT` | A marker `(0, h)` means we created it; no marker is a retry |
| `link(<N>.<h>, <N+1>.<h>)` (unfreeze) | `EEXIST` | Irrelevant: the name is the hash, so the same bytes either way |
| `unlink`, `link(stub, repo)` | `ENOENT`, `EEXIST` | Irrelevant |

## Crash recovery

| Crash after | Left on disk | Resolution |
| --- | --- | --- |
| Staging | `stage.<id>` | Swept after the cutoff |
| Link | `<N+1>.<h'>` | Swept once a later commit moves the marker past `N` |
| Rename | `<N>.<h>` and dead losers | Deleted by the next committer's tidy-up, or by the sweep |
| Stub, in `create` | `repo` without `repo.d` | `open` reports NotFound; `create` resumes |
| `mkdir tmp` | `repo.d.tmp.<id>/` | Swept after the cutoff |
| Unfreeze link | `<N+1>.<h>` next to `<N>.<h>` | The next unfreeze gets `EEXIST` and continues |
| Migration | `repo.legacy.<id>`, maybe `repo.d` | Rerunning `migrate` |

## Freshness and the nudge

Commits are linearizable: each one is a single rename decided by the
server. Reads are not. A reader sees another client's commit once its
directory listing refreshes, so this backend gives bounded staleness, not
the cross-client read-after-write of the object-store backends.

Staleness never loses an update. A writer on a stale base gets a conflict
and the asset manager's retry lists again. The committer deletes its base
right after committing, so a lagging reader can fail on the content and list
again; the retry loop covers it. Within one process reads never go back: a
listing with a lower `N` returns `NotModified`.

Staleness can make a reader miss commits silently. A pipeline step started on
another host after a commit can open the branch and read the previous tip,
without an error. A handoff between hosts should pass the snapshot id, and
the reader should open that snapshot. Opening a snapshot id checks it
against the repo info, so a lagging reader gets a snapshot-not-found error
instead of old data, and can retry.

`nudge(dir)` asks the client to refresh what it has cached about one
directory, so that the next listing or lookup under it reflects the server.
It runs before every listing of `repo.d`, and on the root before `create`,
`migrate` and `open`. It is never a safety input: every safety argument in
this section holds without it. It only shortens how long a client can lag.

- **Linux:** `statx(dir, AT_STATX_FORCE_SYNC)`, defined by statx(2) as "do
  whatever is necessary to synchronize the attributes (e.g., for a network
  filesystem, contact the server)". The NFS client sends a GETATTR for the
  directory; if its change attribute moved, the client drops its cached
  listing and marks every cached name under the directory for a fresh
  lookup, so the next `readdir`, `open` or `rename` goes to the server.
  FUSE honours the flag too. Local and coherent filesystems have nothing
  to refresh. `ENOSYS` (kernels before 4.11) and every other error are
  ignored.
- **Other platforms:** no equivalent exists, so the nudge is a no-op and a
  reader sees another host's commit within the client's directory
  attribute cache timeout.
- **Rejected:** a creating name operation in the directory (`mkdir` plus
  `rmdir`) refreshes the Linux client as a side effect, but needs write
  permission, leaves garbage after a crash, and rests on undocumented
  behaviour.

## Filesystem requirements

What the algorithm needs from the filesystem, and where:

| # | Requirement | Needed by | Without it |
| --- | --- | --- | --- |
| 1 | Atomic `rename` by name, decided by the server | the commit, freeze, unfreeze | Lost update |
| 2 | `link` into a new name, complete content behind it | publishing bytes, the stub | Commits fail |
| 3 | Exclusive directory rename | creation | Concurrent creations clobber each other |
| 4 | `unlink` by name | tidy-up, sweep | Garbage |
| 5 | Listings that show only names that existed | reads, sweep | Lost update |
| 6 | Durable `fsync` | the commit path, every write | Power-loss damage |
| 7 | A directory fsync on one client persists entries other clients created | the data directory barrier | A marker naming missing data after a crash |

No requirement involves a fresh read, a server timestamp, or a lock.

**Expected to qualify:** local POSIX filesystems, NFSv3 and v4 with a Linux
client (verified on 3, 4.0 and 4.2), Lustre, GPFS, CephFS, JuiceFS.

## Backend

- **Crate and type.** A crate `icechunk-fs` with `FilesystemStorage`,
  implemented directly on `std::fs` with `spawn_blocking`. It does not use
  `object_store`.
- **Platforms.** Unix only. On other platforms the constructors return an
  error. Windows is future work.
- **The `repo` key** is the only one that goes through the protocol; V2
  updates nothing else conditionally. `get_object` and
  `get_object_conditional` on it are `read_latest`; `put_object_conditional`
  is `create` or `conditional_update`; `copy_object` from it reads the
  version file named by the version; unconditional `put_object` and
  `delete_objects` on it are unsupported.
- **Writes.** Every write ends with `fsync` and `close`, checking the error
  from `close` because on NFS, write errors such as out of space or over
  quota often only surface there.
  - **`put_object`, default**: staging file plus `rename`. Like a PUT on an
    object store, the write is all-or-nothing: no reader ever sees a partial
    object.
  - **`put_object` with the new-object hint**: written directly under its
    final name, opened with `O_CREAT|O_EXCL`. No staging file and no `rename`.
    The hint is a parameter of `put_object`, ignored by the other backends,
    whose PUTs are all-or-nothing anyway.
  - **`put_object_conditional(Create)`** on any key but `repo`: staging file
    plus `link`. `EEXIST` means the object exists: `NotOnLatestVersion`.
  - **`copy_object`**: the backup is staged and renamed into place.
- **`fsync` on by default.** A storage setting disables it.
- **IC1 repos are refused.** Opening or creating an IC1 repo with this
  backend fails with an error that points to migrating the repo to V2.
  The backend declares that it requires V2 through a `Storage` capability
  method, which defaults to "all versions" for the other backends.
- **Versions.** Only the conditional calls produce a version:

  | Call | `etag` | `generation` |
  | --- | --- | --- |
  | `get_object_conditional` | `h` of the marker | `N` of the marker |
  | `put_object_conditional` | `sha256(new_bytes)` | `N+1`, or `0` for a create |

  Both are the identity: `IfVersion` needs both, and `get_object_conditional`
  answers `NotModified` from the marker alone when the previous version
  matches.
- **A legacy single-file repo is refused** with a pointer to
  `migrate_legacy_repo` rather than migrated on first write, because the
  migration must run with no other writers.
- **`Storage` trait changes, for all backends.** A `VersionInfo` can only be
  obtained from a call that can serve a later conditional write. Every backend
  then has the same contract, and code can't pick up a version that works on
  object stores but fails with `NotOnLatestVersion` here.
  - **Reads.** `get_object` and the range-read methods (`get_object_range`,
    `get_object_range_read`, `get_object_concurrently`) stop returning a
    `VersionInfo`. Only `get_object_conditional` returns one.
  - **Writes.** `put_object` is split in two:
    - `put_object(…, hint) -> ()`: unconditional, with the new-object hint
      described above. It returns no version.
    - `put_object_conditional(…, condition) -> VersionedUpdateResult`, where
      `condition` is `Create` or `IfVersion(VersionInfo)`. The enum replaces
      `Option<&VersionInfo>` and the `VersionInfo::for_creation()` sentinel,
      so "create" and "update" are different types instead of a special
      version value.
  - **Capability.** A method declaring which spec versions the backend
    supports, used to refuse IC1 repos (see above). It defaults to all
    versions.

## Old writers

None of these use the protocol; they all overwrite `repo` unconditionally:

- older Icechunk versions;
- the `object_store` filesystem backend in the same version;
- `force_write_repo_info`.

On a repo in the filesystem format they overwrite the stub with a RepoInfo
that nobody reads. The backend then refuses to open the repo and reports
mixed formats, which needs manual recovery. The refusal happens at the
first use of `repo` by each storage instance (opening a repository creates
one), so an overwrite while an instance is in use goes unnoticed until the
next instance. Running any of them on a repo
at the same time as this backend is unsafe. Converting a repo once every
old writer has stopped is safe.

## Verification

- **Model checker.** A `stateright` model of the algorithm as specified, as a
  dev-dependency in the repo (`icechunk-fs/tests/protocol_model.rs`). It has
  writers doing a link and a rename, the sweep, freeze and unfreeze, crashes
  at every point, lost replies on the link and the rename, and a listing
  adversary that may return any marker that ever existed. Invariants: no
  violation (a false acknowledgement, a conflict reported for a landed
  commit without a lost reply, a frozen report for a landed commit, a marker
  name used twice), the marker always has readable bytes, versions form a
  chain with every acknowledgement in it, and progress in the fault-free
  configuration.

- **Storage tests.** The existing `Storage` tests run against
  `FilesystemStorage`, covering the reworked read and `put_object` APIs on
  every backend.
- **Repository tests.** The Rust garbage collection, stats, concurrency and
  distributed-writes tests run with `FilesystemStorage`. The Python suites
  that run on the `object_store` filesystem backend also run with
  `FilesystemStorage`, except for spec version 1 cases.
- **Stress test.** `fs-stress` (crate `icechunk-fs-stress`, not published)
  runs many worker processes against one repo on a real filesystem, then
  checks what they did against what they logged. Its README is the operator
  runbook.

## Follow-up: a read-only mirror at `repo`

Not part of the first implementation. Clients on other backends read
`repo`: HTTP storage serving the directory, an S3 gateway in front of the
filesystem, the `object_store` filesystem backend. With the stub they get
an error. With the mirror, `repo` holds a copy of the current version that
they can read but not write.

### Contents

The RepoInfo of version `(N, h)`, rendered by core with:

- availability at least `ReadOnly`: `Online` and `ReadOnly` become
  `ReadOnly`, `Offline` stays `Offline`;
- a reason saying that the repository uses the filesystem format and is
  written with `filesystem_storage`, followed by the repo's own reason, if
  any;
- the repo metadata key `icechunk.filesystem_mirror`, with value
  `{"version": N, "hash": "<h>"}`;
- no ops-log entry. Everything else is as in `<N>.<h>`.

Icechunk checks the status before updating a RepoInfo (since 2.0), except
in the calls listed under [Not defended against](#not-defended-against), so
other backends refuse to write through the mirror.

### Setting

`filesystem_mirror` in the repo config, ignored by every other backend:

| Value | `repo` |
| --- | --- |
| `Disabled` (default) | The stub |
| `Sync` | The mirror, published before the update returns |
| `Async` | The mirror, published by a background task after the update returns |

Async is best effort: a process that exits before the task runs leaves the
mirror behind. When the mirror is disabled, `repo` is the stub, never a
copy that nobody updates: an old version that opens without complaint is
worse than an error.

### Not atomic

No name operation changes two names at once, and the commit's compare lives
in the source name of the marker, which a fixed name like `repo` can't
carry. The mirror is replaced after the commit and is eventually consistent
with `repo.d`.

### Publishing

Core renders the mirror and runs the loop; the backend only puts bytes in
place:

```rust
// Core. After an update, errors are logged, never returned: the update has
// already landed. `Repository::publish_mirror` returns them.
fn publish_mirror() {
    for _ in 0..4 {
        let (info, version) = fetch_repo_info();       // from repo.d, bypassing the cache
        let bytes = match info.config().filesystem_mirror {
            Disabled => None,                          // the stub
            Sync | Async => Some(render_mirror(info, version)),
        };
        match storage.publish_repo(version, bytes) {
            Superseded => continue,
            Current | Unsupported => return,
        }
    }
}

// FilesystemStorage. Replaces `repo` with the stub (None) or a mirror of
// version `of`. Never reads `repo`: whatever is there is replaced. Never a
// safety input. Superseded tells core to render the newer version and call again.
fn publish_repo(of: (N, h), bytes: Option<Bytes>) -> Current | Superseded | Io {
    let stage = "repo.mirror.<uuid>";
    write_fsync_close(stage, bytes.unwrap_or(stub))?;
    match rename(stage, "repo") {                      // may silly-rename `repo`, see below
        Ok => fsync_dir(root),
        Err(e) => { unlink(stage); return Io(e) }
    }
    nudge("repo.d");
    match highest(list_markers()?) {
        Some((_, n, x)) if (n, x) == of => Current,    // latest or frozen: the bytes are the same
        _ => Superseded,                               // moved on, or a torn listing
    }
}
```

`publish_mirror` runs:

- after a successful repo-info update whose new or base config enables the
  mirror, so disabling it restores the stub;
- after creating a repo whose config enables it;
- in `Repository::publish_mirror`, the explicit repair.

Freeze and unfreeze don't publish: the bytes don't change. GC doesn't
either; its repo-info updates publish like any other. Opening a repo
doesn't, because readers may not have write permission.
`migrate_legacy_repo` ends with the stub; if the repo's config enables the
mirror, the next update or `publish_mirror` publishes it.

### Convergence

Take the last rename over `repo` to land. Its publisher listed afterwards
and saw the version it published, or it would have published again. A
listing never shows the future, so if that listing was fresh, `repo` copies
the current version, and any later update publishes again. The mirror stays
behind only when a writer dies between its update and that listing, when
that listing lags, or in async mode when the process exits first. The next
update or `publish_mirror` fixes all three.

### What readers of `repo` see

- Never a torn file: the copy is staged and renamed into place.
- Briefly, an older version than one they saw before, when a slow publisher
  lands after a newer one, until its listing sends it round again.
- Briefly, no `repo`: if a process on the publishing NFS client has `repo`
  open, the client splits the rename into a silly rename and a rename, and
  a kill between the two leaves no `repo` until the next publish.
- References to objects GC has deleted, like any reader holding an old
  RepoInfo.

### Not defended against

Calls that skip the status check (`Repository::set_status`,
`update_repo_info_unchecked`, `force_write_repo_info`) through another
backend write `repo`. This backend never reads it, and the next publish
replaces it: their updates are lost. The same holds for a repo created
through another backend while `repo` is missing.

### Cost

Per repo-info update: one RepoInfo render, one small write and fsync, a
rename, a fsync of the root and a listing. Every writer needs write
permission on the repo root.

### Changes to this design

- **Names.** `repo` is the stub or the mirror; new staging name
  `repo.mirror.<id>`, swept by age like `repo.stub.*`, hidden by
  `list_objects`, which shows `repo` with its own size and time.
- **Invariants.** Invariant 4 covers protocol names only: the rename of a
  staged copy over `repo` is outside the protocol. Invariant 5 adds
  `repo.mirror.*`. The fsyncs of `publish_repo` always run.
- **`open` and `migrate`.** Once `repo.d` exists, `repo` is never read: it
  may be the stub, the mirror or missing. `open` returns `Ok`, and `migrate`
  deletes any `repo.legacy.*` and returns `AlreadyMigrated`. Its final
  `write_stub` accepts `LegacyRepo`, since a writer of this format may
  already have published the mirror. `MixedFormats` goes away. `create`
  still reads `repo` before `repo.d` exists, and a mirror never exists
  without `repo.d`, so it is unaffected.
- **Old writers.** Older versions and the `object_store` backend read
  `repo` before every update, so they fail on the stub or refuse the
  mirror's status. What gets through is under
  [Not defended against](#not-defended-against).
- **Conversion.** With the mirror on, copying `repo` first and then every
  other object, with any tool, gives a consistent read-only repo unless GC
  deletes objects during the copy. Setting its status to `Online` and
  removing the metadata key makes it an ordinary repo.
- **API.** `Storage::publish_repo(settings, of: &VersionInfo, bytes:
  Option<Bytes>) -> StorageResult<Published>`, with `Published` one of
  `Current`, `Superseded` and `Unsupported`, the default implementation
  returning `Unsupported`. `RepositoryConfig.filesystem_mirror:
  FilesystemMirror` (`Disabled`, `Sync`, `Async`) and
  `Repository::publish_mirror`, in Rust, Python and JS.
- **Recovery.** A crash between an update and its publish, or a lost reply
  on the rename over `repo`, leaves the mirror of an older version; a crash
  after staging leaves `repo.mirror.<id>`, swept by age; the first half of a
  silly rename leaves no `repo` and a `.nfs*` file. The next publish fixes
  the first and the last.
- **Verification.** The mirror is not in the model, since no protocol step
  reads it. With the mirror on, the stress tool reads `repo` through the
  `object_store` backend during the run, which must always parse it or find
  it missing, never torn, and after a final `publish_mirror` the metadata of
  `repo` must name the current marker.

## Previous attempt: compare-and-swap on a single `repo` file

The first design, kept for the record. It keeps the single `repo` file of
the object-store format and replaces it with a lock-free compare-and-swap
built on hard links and `rename`. It was implemented through the stress
tool stage and lost updates on NFS; the last subsection explains why. The
`Storage` trait changes and the unconditional write paths under
[Backend](#backend) came from it and remain.

### Names of the lock protocol

Values:

| Name | Meaning |
| --- | --- |
| `base_bytes` | Bytes of `repo` the writer read and built on |
| `base_hash` | `sha256(base_bytes)`: the version to replace |
| `new_bytes` | Serialized RepoInfo to install |
| `attempt_id` | Random UUID, new for every attempt |
| `STALE_AFTER` | Lock age after which stealing is allowed |

Some new paths, all in the repo root next to `repo`:

| File | Meaning |
| --- | --- |
| `repo` | Repo info file, unchanged format |
| `repo.new_bytes.<attempt_id>` | Durable copy of `new_bytes`; the commit ticket |
| `repo.lockinfo.<attempt_id>` | Contains `attempt_id`; linked as the lock |
| `repo.lock.<h>.<gen>` | Lock for replacing hash `h`, generation `gen` |

The conditional-update protocol worked for any object: for an object `<name>`
the files were `<name>.new_bytes.<attempt_id>`, `<name>.lockinfo.<attempt_id>`
and `<name>.lock.<h>.<gen>` next to it (V2 only updates `repo`
conditionally).

### Overview

`repo` is only ever replaced by `rename`. A writer that wants to replace the
version `base_hash` with `new_bytes` goes through these steps:

1. **Stage.** Write `new_bytes` to `repo.new_bytes.<attempt_id>`. This file
   is the attempt's *commit ticket*. The commit is later just
   `rename(repo.new_bytes.<attempt_id>, repo)`, so the attempt can commit only
   while its ticket exists. Deleting another attempt's ticket (*fencing* it)
   permanently prevents that attempt from committing.
2. **Lock.** Write and fsync our `attempt_id` to `repo.lockinfo.<attempt_id>`,
   then `link` it to `repo.lock.<base_hash>.<gen>`, starting at `gen = 0`.
   - `link` creates the name only if it doesn't exist, so each generation has
     exactly one holder.
   - Because the lockinfo file is complete and durable before the link, a lock
     file appears atomically with its full content and never changes.
   - The lock name uses only values every writer knows, so other writers find
     it. All locks for the same hash form a *lock family*.
   - **Stealing.** If generation `gen` is taken by a holder that looks dead
     (its lock is older than `STALE_AFTER`), try `gen + 1`. Stealing is always
     safe; `STALE_AFTER` only decides *when* it happens.
     [Lock staleness](#lock-staleness) explains how its value is chosen.
3. **Fence.** For every lower generation, read the lock file to learn
   that holder's `attempt_id`, and delete its ticket. A
   holder that was only paused can then never commit.
4. **Compare.** Read `repo` fresh. If its hash isn't `base_hash`, someone else
   committed first: report a conflict.
5. **Commit.** Rename the ticket over `repo`. Overwriting is safe here:
   - we hold the lock for `base_hash`;
   - every earlier holder's ticket is gone;
   - step 4 confirmed `repo` is still `base_hash`.

   The only writer that could change `repo` before our rename is a later
   stealer, and it must delete our ticket first.
6. **Tidy up.** Delete the lock family of `base_hash`. That hash is no longer
   current, so its locks can never be used again.

### Algorithm detail

The algorithm is implemented by three `Storage` methods:
`put_object_conditional`, `copy_object`, and `get_object_conditional`. (The
split of `put_object` into an unconditional and a conditional method is
described under [Backend](#backend).)

#### Conditional update: `put_object_conditional(repo, new_bytes, IfVersion(base_hash))`

Only one caller reaches this algorithm: `write_repo_info`, inside
`AssetManager::update_repo_info`'s retry loop. Every operation that changes
repo info goes through it: commits, branch and tag changes, config updates, GC
and expiration. Chunks, manifests, snapshots and transaction logs are written
with the unconditional `put_object`, and repo creation with
`put_object_conditional(Create)`. Both take the simpler paths described under
[Backend](#backend).

```rust
fn conditional_update(base_hash: Hash, new_bytes: &[u8]) -> Result<Updated | NotOnLatestVersion> {
    // Writing identical bytes would make `base_hash` current again after it
    // was replaced. Deleting lock files is only safe because that never
    // happens (see "Version identity and content uniqueness").
    if sha256(new_bytes) == base_hash {
        return Err(RepeatedContent);
    }
    let attempt_id    = Uuid::new_v4();
    let ticket_path   = format!("repo.new_bytes.{attempt_id}");
    let lockinfo_path = format!("repo.lockinfo.{attempt_id}");
    // Before the commit point, any I/O error not matched below runs
    // cleanup() and is returned.

    // ── Step 1: stage the lock content and the new content ──────────────────
    match write_fsync_close(&lockinfo_path, attempt_id.to_string()) {
        Ok(()) => {}
        // The directory doesn't exist, so neither does `repo`.
        Err(ENOENT) => return NotOnLatestVersion,
    }

    // Early exit: if the gen-0 lock for our base exists and is not stale,
    // someone is committing on top of our base and we will lose anyway.
    let lock0 = lock_path(&base_hash, 0);
    if exists(&lock0) && !is_stale(&lock0) {
        cleanup();
        return NotOnLatestVersion;
    }

    // We create our commit ticket, critically, before our lock is visible.
    // If a stealer could see our lock before our ticket existed, its
    // unlink would hit ENOENT and we would still be able to commit later.
    write_fsync_close(&ticket_path, new_bytes);

    // ── Step 2: take the lock for replacing `base_hash` ─────────────────────
    let mut gen = 0;
    let my_gen = loop {
        let lock = lock_path(&base_hash, gen);
        match link(&lockinfo_path, &lock) {
            Ok(()) => break gen,
            Err(ENOENT) => { cleanup(); return NotOnLatestVersion; }  // our lockinfo was swept
            Err(EEXIST) => match read_lock_owner(&lock) {
                // A LINK retried after a lost reply reports EEXIST although the
                // first try succeeded. The lock content tells us it is ours.
                Ok(owner) if owner == attempt_id => break gen,

                // Deleted between our link and our read. Locks are only deleted
                // once their hash is no longer current, so our base is outdated.
                Err(ENOENT) => { cleanup(); return NotOnLatestVersion; }

                // Lock files are fsynced before being linked, so this is
                // corruption. Without the holder's attempt_id we cannot delete
                // its ticket, so we cannot steal. Needs manual recovery.
                Err(Unparsable) => { cleanup(); return Err(CorruptLock); }

                Err(e) => { cleanup(); return Err(e); }  // couldn't read the lock

                // Probably alive and about to change `repo`, which would outdate
                // our base anyway. Report a conflict; the caller's retry loop
                // re-reads `repo` with backoff.
                Ok(_) if !is_stale(&lock) => { cleanup(); return NotOnLatestVersion; }

                // Looks dead: steal by taking the next generation. Only one
                // stealer can create generation gen+1.
                Ok(_) => gen += 1,
            },
        }
    };

    // ── Step 3: fence every earlier holder in this lock family ──────────────
    // A lower-generation holder may be paused, not dead. Deleting its ticket
    // makes its future `rename(ticket, repo)` fail.
    for j in 0..my_gen {
        match read_lock_owner(&lock_path(&base_hash, j)) {
            Ok(owner) => match unlink(&format!("repo.new_bytes.{owner}")) {
                Ok(()) | Err(ENOENT) => {}  // ENOENT: already committed or deleted
                // The holder's ticket may still exist, and it could commit
                // alongside us.
                Err(e) => { cleanup(); return Err(e); }
            },
            Err(ENOENT) => {}        // deleted => base_hash no longer current => step 4 fails
            Err(Unparsable) => { cleanup(); return Err(CorruptLock); }  // needs manual recovery
            Err(e) => { cleanup(); return Err(e); }
        }
    }

    // ── Step 4: compare ─────────────────────────────────────────────────────
    match repo_hashes_to(&base_hash) {
        Ok(true) => {}
        Ok(false) => { cleanup(); return NotOnLatestVersion; }
        Err(e) => { cleanup(); return Err(e); }
    }

    // ── Step 5: commit point ────────────────────────────────────────────────
    match rename(&ticket_path, "repo") {
        Ok(()) => {}
        // Either our ticket was deleted before the rename reached the server, or the
        // rename succeeded and a retry after a lost reply saw ENOENT. Content
        // is unique, so reading `repo` tells us which.
        Err(ENOENT) => match repo_hashes_to(&sha256(new_bytes)) {
            Ok(true) => {}
            Ok(false) => { cleanup(); return NotOnLatestVersion; }
            Err(e) => { cleanup(); return Err(e); }
        },
    }
    // The rename is a change to the directory, not to any file, and fsyncing
    // the ticket doesn't make it durable. Without this, a power loss after we
    // report success can bring back the old `repo`, silently undoing a
    // commit the caller already saw succeed.
    fsync_dir(".");

    // ── Step 6: tidy up (a crash here is harmless) ─────────────
    unlink(&lockinfo_path);
    delete_all(lock_paths_for(&base_hash));
    Updated { new_version: VersionInfo { etag: sha256(new_bytes), .. } }
}

// Called on every failure path. Deleting our ticket guarantees this attempt
// can never commit later.
//
// It must NOT delete our lock file. A new writer could then take our freed
// generation. It only deletes the tickets of *lower* generations, so it would
// miss a stealer above us, and both could commit.
fn cleanup() { unlink(&ticket_path); unlink(&lockinfo_path); }

// A missing `repo` doesn't hash to anything.
fn repo_hashes_to(hash: &Hash) -> Result<bool> {
    match read_file_fresh("repo") {
        Ok(bytes) => Ok(sha256(&bytes) == hash),
        Err(ENOENT) => Ok(false),
        Err(e) => Err(e),
    }
}
```

##### Lock staleness

`is_stale(lock)` compares two server timestamps, so client clock skew doesn't
matter:

- the lock file's ctime, set by the server when the lock was linked;
- the mtime of our own lockinfo file, which we just wrote, so it reads as the
  server's current time.

The lock is stale if the difference exceeds `STALE_AFTER`.

A lock shares its inode with its holder's lockinfo, and unlinking any name
of an inode updates its ctime. So the lock's ctime is also refreshed when
the holder's lockinfo is deleted by its cleanup or its tidy-up, which can
delay a steal by up to another `STALE_AFTER`. This only affects liveness. A
GC sweep never unlinks a lockinfo still linked as a lock it keeps.

**Choosing `STALE_AFTER`.** it must be longer than a healthy
holder ever needs between taking the lock (step 2) and its commit `rename`
(step 5). Stealing after the rename does no harm: the stealer's compare
fails.

That window holds only:

- one `link`;
- up to one `unlink` per earlier generation;
- one fresh read of `repo`;
- one `rename`.

All the heavy work (staging the ticket, `fsync`) happens before the lock.

Consequences:

- **Too short.** Live holders get stolen from and fail with spurious
  conflicts. Under sustained stalls longer than `STALE_AFTER`, every holder
  can be stolen from before it finishes, and nobody makes progress.
- **Too long.** After a writer crashes while holding a lock, writers on top
  of that base get conflicts until the lock is stale. Commits and ref
  updates keep retrying with backoff (`repo_update_retries`), then steal the
  lock and commit about `STALE_AFTER` later, or up to twice that when a
  lockinfo unlink refreshed the lock's ctime. GC, expiration, and callers
  configured with few retries fail instead, with "too many attempts"
  (`RepoUpdateAttemptsLimit`).

Crashes are rare, and conflicts for up to a minute after one are acceptable,
so the value should err long. **Default: 60 s.** That is three or more orders of
magnitude above normal hold times, and above typical metadata stalls. It is
configurable for sites whose filesystems stall longer, and for tests.

#### Backup: `copy_object(repo, overwritten/<name>, version = base_hash)`

Called by `write_repo_info` just before `put_object_conditional`, to save
the version about to be replaced into `overwritten/`. The two calls are
separate, and the backup takes no lock:

1. Read `repo` with a fresh read.
2. If `sha256(bytes) != base_hash`, return `NotOnLatestVersion`.
3. Write *those same bytes* to the backup name: staged, fsynced, renamed into
   place.

The backup is the hashed version by construction, whatever happens
concurrently.

#### Reading `repo`: `get_object_conditional`

Called by `AssetManager::fetch_repo_info`.
This happens on every repo-info read, including the start of each attempt in
the update retry loop, where it produces `base_bytes` and `base_hash`. When
the repo-info cache holds a version, it is passed as `previous_version`.

- Open a new file descriptor on `repo`, then `fstat` it. Use `fstat` on an
  fd, not `stat`: `stat` can be answered from the NFS attribute cache, `open`
  cannot.
- Version: `etag = sha256(bytes)`, and
  `generation = (dev, ino, size, mtime_ns, ctime_ns)` from the `fstat`.
- If the caller's previous version has the same `generation` tuple, return
  `NotModified` without reading the data. This can be a false hit: file
  timestamps advance in kernel ticks or coarser (milliseconds on ext4, up to
  seconds on some filesystems and NFS exports), and inode numbers are reused,
  so two replacements of `repo` within one tick that restore its size on a
  reused inode look unchanged. A false hit never affects safety, because
  writers compare the hash of a fresh read in step 4. A reader may keep
  seeing its cached version until `repo` changes again. A writer that gets a
  conflict drops its cached version, so its next attempt reads in full.
- Otherwise read, hash, and return `Modified`.

### Algorithm correctness

- **Only an attempt whose ticket exists can change `repo`.** `repo` changes
  only through `rename(repo.new_bytes.<id>, repo)`, which needs the ticket.
- **At most one live ticket per lock family.** Each `(base_hash, gen)` is
  created by exclusive `link`, so it has one holder. A holder deletes the
  tickets of all lower generations (step 3) before comparing (step 4). Lock names are never freed
  while their hash is current, so a later writer can never take a lower
  generation than a live holder above it.
- **Attempts in other lock families can't commit.** Their base differs from
  the current hash, so they fail step 4. By content uniqueness (next section),
  a base that stopped being current never becomes current again.
- **The compare stays valid until the commit point.** After step 4 passes, the
  only way `repo` can change before step 5 is a later-generation stealer. That
  stealer deletes our ticket (step 3) before its own step 4. The server orders
  our `rename(ticket, repo)` and its `unlink(ticket)`:
  - If our rename wins, nothing changed `repo` between our compare and our
    commit, and the stealer then fails its own step 4.
  - If the unlink wins, our rename fails with `ENOENT`.
- **Timeouts only affect liveness.** Stealing from a live, slow holder makes it
  fail with a clean conflict.

### Version identity and content uniqueness

The version of `repo` is the hash of its content, `sha256(bytes)`. Inode,
mtime and size can't serve as identity: inode numbers are reused, mtime can be
coarse, and on NFS a `stat` may be answered from the client's cache. A content
hash can be checked by anyone who does a fresh read, and needs nothing else
from the filesystem.

The algorithm relies on one property of that content: **`repo` never holds
the same bytes twice.**

**Why it holds.** In V2, every RepoInfo write appends an `Update` entry with a
microsecond `updated_at` and a backup path containing a random id. Two writes
therefore produce different bytes even when they make the same logical
change.

**Enforcement.** The backend rejects a `put_object_conditional` whose new
hash equals the base hash. That catches the immediate A → A case.
Non-adjacent repeats can't be detected locally, so the guarantee rests on the
V2 format.

### Lost replies in the lock protocol

NFS clients retry a request whose reply was lost. A non-idempotent operation
can then report failure although the first try succeeded. Every case is resolved
by reading content, never by `stat`:

| Operation | Spurious result | Resolution |
| --- | --- | --- |
| `link(lockinfo, lock)` | `EEXIST` | The lock's `attempt_id` is ours: we hold the lock. |
| `link(staging, name)` (conditional create) | `EEXIST` | A fresh read of the target hashes to `sha256(new_bytes)`: the create succeeded. |
| `rename(ticket, repo)` | `ENOENT` | A fresh read of `repo` hashes to `sha256(new_bytes)`: we committed. |
| `unlink(other's ticket)` | `ENOENT` | Irrelevant: the ticket is gone either way. |

### Crash recovery in the lock protocol

| Crash after | Left on disk | Resolution |
| --- | --- | --- |
| Step 1 | ticket and lockinfo | Swept. Deleting a ticket is always safe; at worst a live attempt fails. |
| Steps 2–4 | `repo.lock.<base_hash>.<gen>` | After `STALE_AFTER` the next writer steals gen+1. No manual unlock. |
| Step 5 | `repo` updated, lock files remain | The lock family's hash is no longer current. Swept. |

### Sweeping the lock protocol's files

- **Step 6** handles the normal case.
- **GC** asks the storage backend to sweep its leftovers, through a `Storage`
  hook that the other backends don't implement. GC passes the metadata age
  cutoff the user gives it (`delete_object_older_than` in Python). The backend
  never uses a cutoff later than 24 hours ago, and sweeps only files whose
  mtime is older than it.
- The sweep reads only the repo root, in one listing. For any key `<name>`
  in the root:
  - `<name>.new_bytes.*` and `repo.probe.*`: deleted whatever their content.
  - `<name>.lockinfo.*`: deleted whatever their content, except a lockinfo
    still linked as a lock the sweep keeps (same device and inode). Unlinking
    it would refresh that lock's ctime and delay stealing it. A lockinfo
    linked to a lock the sweep deletes is deleted with it.
  - Staging files `<name>.staging.<uuid>`, written next to their target by
    unconditional writes and conditional creates: deleted whatever their
    content.
  - `<name>.lock.<h>.*`: the lock files are listed first, then `<name>` is
    read fresh. Only listed locks whose `h` differs from the hash of that read
    are deleted. A lock listed before the read was created on top of a version
    that was current at or before the listing. If the read shows a different
    hash, that version is gone for good, so its lock family can never be used
    again. A lock that appears after the listing is left for the next run.
  - A missing `<name>` has all its listed locks past the
    cutoff deleted.
  - A `<name>` that can't be read keeps its locks.

- `list_objects` returns neither protocol files nor staging files.
- The cutoff affects only liveness. Deleting a live attempt's ticket or
  lockinfo makes that attempt fail with a conflict, and a lock of a dead family
  is never used again. The 24-hour floor keeps GC from failing attempts that
  stalled for a long time but are still alive, such as a write on a suspended
  machine. It also absorbs any plausible skew between the clock of the GC
  client, which computes the cutoff, and the file server's clock, which sets
  the mtimes.
- **Never** delete a lock file whose hash is current.

### Fresh read

This analysis was wrong; [What went wrong](#what-went-wrong) explains how.

Steps 2, 3 and 5 are atomic name operations decided by the server. Step 4 is
a read, which a client could answer from its cache. A stale read there is a
lost update: the writer sees its outdated base, the hashes match, and it
overwrites a newer commit.

- **Coherent filesystems (Lustre, GPFS, CephFS, local disks):** reads are
  fresh.
- **Linux NFS client, NFSv3** (`fs/nfs/dir.c`, `nfs_lookup_verify_inode`):
  - Under close-to-open (`cto`, the default), every `open()` of a regular file
    forces a GETATTR on the cached file handle.
  - If that fails with `ESTALE`, or reports `nlink == 0`, the cached dentry is
    dropped and the name is looked up again on the server.
  - After `rename` over `repo`, the old inode has `nlink == 0` or no longer
    exists, so the open reaches the new file. Inode generations in file handles
    make a reused inode number return `ESTALE` too.
  - The only way to read the old file is if it still has `nlink > 0`, i.e. it
    is hard-linked somewhere. Hence byte-copy backups.
- **Linux NFS client, NFSv4** (`nfs4_lookup_revalidate`, `nfs4_file_open`):
  - `open()` sends an OPEN by name, which the server resolves. If the inode
    returned differs from the cached dentry's, the client drops the dentry and
    retries (`-EOPENSTALE`).
  - A client holding a read delegation on the old inode could open it locally.
    The server recalls conflicting delegations before `vfs_rename` replaces the
    target.
- **Second, unverified mechanism:** our own successful `link` in step 2
  returns post-op directory attributes. If the directory's change attribute
  moved because of someone else's change, `nfs_update_inode` calls
  `nfs_force_lookup_revalidate`, which invalidates cached dentries. This is
  defence in depth only, and depends on the server sending post-op attributes.
- **Implementation:** `read_file_fresh` opens a new file descriptor every
  time, reads through it, never reuses an fd and never mmaps.
- **Read-after-write across nodes:** every `get_object` is a fresh `open()`,
  so readers see a commit from another node on their next open.

Sources: [nfs(5)](https://man7.org/linux/man-pages/man5/nfs.5.html),
[open(2)](https://man7.org/linux/man-pages/man2/open.2.html),
[fs/nfs/dir.c](https://github.com/torvalds/linux/blob/master/fs/nfs/dir.c),
[fs/nfs/nfs4file.c](https://github.com/torvalds/linux/blob/master/fs/nfs/nfs4file.c),
[fs/nfs/inode.c](https://github.com/torvalds/linux/blob/master/fs/nfs/inode.c),
[RFC 1813](https://www.ietf.org/rfc/rfc1813).

### Filesystem requirements of the lock protocol

What the algorithm needs from the filesystem, and where:

| # | Requirement | Needed by | Without it |
| --- | --- | --- | --- |
| 1 | Exclusive hard links | Step 2, repo creation | Lost update |
| 2 | Atomic `rename` | Step 5, default writes | Partial files |
| 3 | `rename` by name | Step 5 vs step 3 | Lost update |
| 4 | `unlink` by name | Step 3 | Lost update |
| 5 | Fresh open | Steps 2–5, `copy_object`, GC | Lost update |
| 6 | Server timestamps | `is_stale` | Liveness only |
| 7 | Durable `fsync` | Steps 1 and 5, every write | Power-loss damage |

**Enforcement.** Exclusive hard links are checked; the other requirements are
documented.

- At repo creation and on first write, the backend creates
  `repo.probe.<uuid>` in the repo directory, hard-links it to
  `repo.probe.<uuid>.link`, links it again onto that existing name, which must
  fail with `EEXIST`, then deletes both. Failure means no exclusive hard links
  and the backend refuses to write. A crash mid-probe leaves files behind; GC
  sweeps them.
- NFS mounts with `nocto` are unsupported: the option breaks the fresh read
  on NFSv3. A check of `/proc/self/mountinfo` was considered; see the
  backend details below.

**Expected to qualify:** local POSIX filesystems, NFSv3/v4 with a Linux
client, Lustre, GPFS, CephFS, JuiceFS.

### Backend details specific to the lock protocol

- At repo creation and on first write, the backend created
  `repo.probe.<uuid>` in the repo directory, hard-linked it to
  `repo.probe.<uuid>.link`, linked it again onto that existing name, which
  had to fail with `EEXIST`, then deleted both. Failure meant no exclusive
  hard links and the backend refused to write.
- The lockinfo was always fsynced, even with fsync disabled: an empty lock
  file would have blocked its lock family until manual recovery.
- Versions carried an `fstat` tuple `(dev, ino, size, mtime_ns, ctime_ns)`
  as `generation`, used only to answer `NotModified` without reading. A
  false hit was possible when two replacements of `repo` within one
  timestamp tick restored its size on a reused inode number, leaving a
  reader on its cached version until `repo` changed again. The fix, git's
  "racy timestamp" rule, was never implemented.
- NFS mounts with `nocto` were documented as unsupported, because the
  option breaks the fresh read on NFSv3. A check of `/proc/self/mountinfo`
  was considered. With the replacement protocol `nocto` no longer matters:
  nothing reads a name whose binding changes.
- `STALE_AFTER` defaulted to 60 s, configurable.

### What went wrong

The protocol was implemented and stress-tested with `fs-stress`:
storage mode, two NFSv4.2 clients of one export, eight workers each, five
minutes. The run produced six lost updates, each a pair of acknowledged
updates from the same base, one per client. A deterministic repro shows the
cause.

**The fresh read in step 4 isn't fresh.** The [Fresh read](#fresh-read)
analysis assumed that `open()` resolves `repo` on the server. On Linux it
doesn't:

- NFSv4 opens a cached, positive dentry by file handle
  (`NFS4_OPEN_CLAIM_FH` in `fs/nfs/nfs4proc.c`), not by name. NFSv3 sends a
  GETATTR on the cached handle. Both succeed as long as the old inode still
  exists.
- The old inode survives a rename over `repo` whenever any process on the
  renaming client has `repo` open: the client first renames it to `.nfs…`
  (silly rename), then renames the ticket. With readers on the committing
  host that is common.
- The name is looked up again only when the client believes the directory
  changed. That depends on its attribute cache, on change information
  piggybacked on its own operations, and on delegations, which skip the
  directory check altogether (`nfs_lookup_revalidate_delegated`). None of
  it is a guarantee, and the stress run defeated it.

So a writer reads its base after it was replaced, the hashes match, and its
rename overwrites a newer commit. No mount option fixes the name binding.

**Two RPCs.** The silly rename makes the commit two RPCs. Between them
`repo` doesn't exist: other clients get `ENOENT` transiently, and if the
client dies between the RPCs, permanently.

**What NFS does guarantee.** A *successful* `link`, `rename`, `unlink`,
`mkdir` or `rmdir` was executed by the server against the true directory
state. Every error, `EEXIST` and `ENOENT` included, may come from a client
cache, and so may every read of a name whose binding can change. The
protocol that replaced this one relies on nothing else.
