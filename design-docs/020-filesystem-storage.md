# Safe concurrent commits on local and network filesystems

## Problem

Icechunk's concurrency currently requires compare-and-swap: replacing the
`repo` info file only if it is still the version the writer read. On object
stores this is a conditional PUT (`If-Match`). `object_store`'s
`LocalFileSystem` does not implement `PutMode::Update`, so the current
filesystem backend overwrites unconditionally and concurrent commits can
silently lose updates.

Users want Icechunk on HPC and network filesystems: NFS, SMB, Lustre, GPFS,
CephFS, and others. This design adds a new filesystem storage backend with a
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

Sites run several, so the shares overlap. NFS is the common case, and the
one with the weakest client-side consistency, which is why the design
relies only on what every NFS client must execute on the server.

The first design kept `repo` as a single file and replaced it with a
lock-free compare-and-swap built on hard links and `rename`. It was
implemented and stress-tested, and it lost updates on NFS: the read it
relied on to compare versions is not guaranteed to be fresh on any NFS
client. It is kept at the end, under
[Previous attempt](#previous-attempt-compare-and-swap-on-a-single-repo-file).
This document describes its replacement.

## Goals and constraints

- No lost updates, on local disks and on every distributed filesystem,
  relying only on operations that a client must execute on the server.
- Timeouts may affect liveness, not safety.
- The filesystem format may differ from the object-store format in the one
  object that is updated conditionally, `repo`. Everything else is
  byte-identical to V2, and a repo converts between the two formats in both
  directions.
- Keep it minimal and simple.
- We only need to support Icechunk V2.

## Format

The filesystem format replaces the `repo` object with a stub file and a
directory of immutable versions, and the commit becomes a single
server-decided `rename`. Everything else, chunks, manifests, snapshots,
transaction logs, `overwritten/` backups, GC and expiration, is unchanged.
Old clients that open a repo in this format get a clear error instead of a
stale or missing file.

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
`id` a random UUID, new for every attempt, creation or migration, so that
no two of them ever share a name. A marker name parses only if `K` and `N`
agree. `VersionInfo` is `etag = h`, `generation = N`; `IfVersion` needs
both. Version files and the stub are immutable.

`K` exists so that a lexicographic listing of `latest.*` puts the newest
marker first. No filesystem returns sorted listings, so nothing here
depends on it; it keeps the names usable by a future object-store variant,
whose listings are sorted and strongly consistent. `N` is in the name for
people and tools. The sections below write `latest.<N>.<h>` and
`frozen.<N>.<h>` for brevity.

## Algorithm overview

The marker is an empty file whose name says which version is current:
`latest.<N>.<h>` means the RepoInfo is version `N`, with its bytes in
`repo.d/<N>.<h>`. It is one inode, never opened or written, only renamed,
so exactly one such name exists at any instant. `rename` is atomic and
decided by the server, so moving the marker is both the compare and the
swap of a commit. Nothing else is ever renamed over, and no file but
`<N>.<h>` is ever read.

**Read.** List `repo.d` and take its marker. A listing is served in several
`readdir` calls and a rename can land between them, so it can show the
marker under two names, the old and the new, or under none. *The marker of
a listing* is its `latest.*` or `frozen.*` entry with the highest `N`, and
at equal `N` the `frozen.*` one, the later state: the newest state the
listing witnessed, and never ahead of the server. No marker means a torn
listing, read again. Then read `<N>.<h>` and check its
hash. If the caller already holds `(N, h)`, or a newer `N`, report
`NotModified`.

**Commit**, replacing version `(N, h)` with `new_bytes`, `h' = sha256`:

1. **Stage.** Write and fsync `stage.<id>`.
2. **Publish the bytes.** `link` the staging file as `<N+1>.<h'>` and fsync
   the directory. The name is new, so nothing is overwritten. It is also the
   hash of the bytes, so whatever holds it has our bytes: on `EEXIST` it is
   our own link retried after a lost reply, or another writer's identical
   bytes, and either is fine.
3. **Commit.** `rename(latest.<N>.<h>, latest.<N+1>.<h'>)`. Success means
   the marker was at our base on the server at that instant, and we moved
   it: the base was current and nobody else can have replaced it. `ENOENT`
   means the marker was elsewhere, or our rename landed and the reply was
   lost; a listing tells the two apart whenever it has caught up.
4. **Tidy up.** Fsync the directory, then delete `<N>.<h>` and, from one
   listing, every other version file at or below `N` and every `<N+1>.<x>`
   with `x != h'`. The successful rename proved the marker is at
   `(N+1, h')`, so all of them are dead for good. The directory therefore
   holds two files plus the attempts in flight, however many commits a day
   there are, and a listing stays one small `readdir`. A reader that
   already has the base's bytes keeps them. One that opened it from another
   client may get `ESTALE` on its next read, even on NFSv4.2 (observed:
   a holder that had not read yet got `ESTALE`, one mid-stream completed);
   one on the committing client is protected by the client's silly rename.
   Readers treat any failure as "deleted under me" and list again.

A conflict leaves at most `<N+1>.<h'>` behind, deleted by the loser when
the listing proves it lost, otherwise by the sweep.

**Freeze** renames the marker to `frozen.<N>.<h>`, a name no commit uses as
its source. Version `N` stays readable, no commit can succeed, and exactly
one operator can take the rename. It serves exports and consistent copies
of the directory. **Unfreeze** links `<N>.<h>` as `<N+1>.<h>` and renames
the marker to `latest.<N+1>.<h>`, never back to `latest.<N>.<h>`, so no
marker name is ever reused. Writers based on `(N, h)` get one conflict.
Only the rename proves that a freeze or an unfreeze acted: "already
frozen" and "not frozen" come from a listing, which can lag.

**Create** builds `repo.d.tmp.<id>/` with `0.<h>` and `latest.0.<h>`, then
renames the directory to `repo.d`. A directory rename fails if the target
exists and is not empty, so creation is exclusive too. The stub is linked
as `repo` first.

**Sweep**, run by GC from one listing with marker `(N, h)`: delete every
`<i>.<x>` with `i < N`, and `<N>.<x>` with `x != h`. Both are dead for
good, however old the listing. Files above `N` belong to attempts in
flight. Staging files, including the root's staging files of other
objects, and abandoned creation directories go by age.

**Migrate** converts a single-file repo offline: rename `repo` aside to
`repo.legacy.<id>`, read it, build `repo.d` as in create, link the stub,
delete the legacy file. Every step is idempotent.

The stub is a V2 file header with the new file type `FilesystemStub`. Old
versions fail on the unknown type; current non-filesystem backends report
that the repository uses the filesystem format.

## Rules

What the filesystem is trusted for, and nothing else:

1. A successful `link`, `rename`, `unlink`, `mkdir` or `rmdir` was executed
   by the server against the true directory state at that instant.
2. Any error may come from a client cache. An error never enables a commit.
   The one error stepped past is `EEXIST` on the content link; the
   server-decided rename still stands behind it.
3. A name created once and never rewritten returns the right bytes when it
   returns bytes.
4. A listing may lag, but shows only names that existed at some point. So
   its highest `N` is at most the true `N`, and version `N` has one hash
   forever.
5. Never read through a name obtained by renaming a name this client may
   have cached: the client moves its cached dentry onto the new name.

## Invariants

1. Exactly one marker exists at any instant, and no marker name is ever
   used twice. The marker moves only by
   `rename(latest.<N>.<h>, latest.<N+1>.<h'>)` (a commit),
   `rename(latest.<N>.<h>, frozen.<N>.<h>)` (a freeze), or
   `rename(frozen.<N>.<h>, latest.<N+1>.<h>)` (an unfreeze).
2. `<N+1>.<h'>` is fully written, fsynced and durably linked before the
   marker names it.
3. `<i>.<x>` is deleted only if a listing showed a marker `(N, h)` with
   `i < N`, or with `i == N` and `x != h`.
4. No rename targets an existing name, and no protocol file other than
   `<N>.<h>` is opened. So no silly rename ever happens, and no name is ever
   transiently missing.
5. No clock decides anything except the deletion of `stage.*`,
   `repo.stub.*` and `repo.d.tmp.*`.

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

// The marker of a listing: the highest N, and at equal N `frozen` over
// `latest`, which a freeze renamed it from. None for a torn listing.
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
    fsync_dir("repo.d");                               // the link is durable before the marker can name it
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
// it became (rule 5). The rename to a fresh name is decided by the server.
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

## Correctness

- **No lost update.** A commit is one successful `rename` whose source is
  `latest.<N>.<h>`, the writer's base. By rule 1 the marker was at the base
  at that instant, so the base was current. Two writers on one base race on
  the same source name and the server lets exactly one win. A writer on a
  stale base has a source that no longer exists; a cached `ENOENT` and a
  server `ENOENT` take the same conflict path.
- **The current version always has bytes.** Content is linked and made
  durable before the marker names it (invariant 2), and deleted only under
  invariant 3, which no lagging listing can violate (rule 4).
- **Acknowledged commits are never undone.** `N` only grows, and no marker
  name recurs, so no later rename can restore an earlier state.
- **Identical bytes are harmless.** Two writers committing identical bytes
  on one base can both be acknowledged, and a commit whose bytes equal its
  base's can coincide with an unfreeze's `latest.<N+1>.<h>`. Content
  addressing makes the end states identical either way.
- **`after_failed_rename` is safe.** Its two deletions are justified by the
  listed marker: `<N+1>.<h2>` is dead once version `N+1` has another hash,
  or once the marker is at `N+2` or beyond. The `Frozen` branch deletes
  nothing: a listing can show `frozen.<N>.<h>` from before a commit from
  `latest.<N>.<h>` only if that name was used again, which invariant 1
  forbids.
- **Deleting a version never corrupts a read.** Version files are
  immutable, so a reader gets either the right bytes or an error. A read in
  progress may complete or fail with `ESTALE`, on any NFS version, and
  `read_latest` lists again. The hash check catches anything else.
- **Tidy-up deletes only dead files.** The committer's own successful
  rename is proof that the marker is at `(N+1, h')`, at least as strong as
  a listing showing it, so invariant 3 covers every file it deletes.
- **Timeouts affect liveness only.** The cutoff deletes staging files and
  abandoned creation directories; a live attempt that loses its staging
  file gets a conflict.
- **Nothing is held across steps.** A crash anywhere blocks nobody.

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

A crashed attempt in a repo that never commits again leaves one content
file above the marker. That is deliberate: deleting it on age would let a
long-paused attempt commit a marker without bytes.

## Freshness and the nudge

Readers see another client's commit once their directory listing refreshes.
A writer on a stale base gets a conflict and the asset manager's retry lists
again. The committer deletes its base right after committing, so a lagging
reader can fail on the content and list again; the retry loop covers it.

`nudge(dir)` asks the client to refresh what it has cached about one
directory, so that the next listing or lookup under it reflects the server.
It runs before every listing of `repo.d`, and on the root before `create`,
`migrate` and `open`. It is never a safety input: every argument in this
section holds without it. It only shortens how long a client can lag.

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

## Conversion

- **Legacy single file on a filesystem to this format:** `migrate`, run
  offline. Its fresh read rests on the force-sync before the rename, Linux
  client behaviour, and the precondition that no legacy writer runs after
  it starts. The legacy format has no server-side version to check against.
- **This format to object store:** `freeze`, read `<N>.<h>`, copy every
  object listed by `list_objects` (which hides the protocol names), then
  put `repo` with a conditional create. The stub never travels. Unfreezing
  the source is the operator's explicit choice.
- **Object store to this format:** read `repo` and its etag, copy every
  object, read `repo` again with that etag and stop unless it is
  `NotModified`, then `create`. The re-read catches most violations of the
  no-writers precondition.

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

No requirement involves a fresh read, a server timestamp, or a lock. All
are documented, none is probed. Hard links are also used by the existing
unconditional and conditional writes, as before.

**Expected to qualify:** local POSIX filesystems, NFSv3 and v4 with a Linux
client (verified on 3, 4.0 and 4.2), Lustre, GPFS, CephFS, JuiceFS.

## Backend

- **Crate and type.** A crate `icechunk-fs` with `FilesystemStorage`,
  implemented directly on `std::fs` with `spawn_blocking`. It does not use
  `object_store`.
- **Platforms.** Unix only. On other platforms the constructors return an
  error. Windows is open question 4.
- **Rollout.** The new backend starts as an explicit opt-in. After real-world
  use, the existing names switch to it.

  | | Opt-in phase | After the switch |
  | --- | --- | --- |
  | Python function | `icechunk.filesystem_storage(path)` (new) | `local_filesystem_storage` becomes a deprecated alias of `filesystem_storage` |
  | Python class method | `Storage.new_filesystem(path)` (new) | `Storage.new_local_filesystem` becomes a deprecated alias |
  | Rust | `icechunk_fs::new_filesystem_storage(path)` (new) | `new_local_filesystem_storage` returns `FilesystemStorage` |
  | Old `object_store` backend | default for the existing names | `legacy_local_filesystem_storage` for one or two releases, then removed |

  - **Serde tags** follow the existing convention (plain `#[typetag::serde]`).
    The new backend serializes as `FilesystemStorage`. Payloads from the old
    backend keep `ObjectStorage` with the `local_file_system_object_store_provider`
    backend and keep deserializing to the old backend.
  - **Docstrings** for both constructors must state which one is safe for
    concurrent writers while both exist.
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
    - **The caller's promise.** With the hint, the caller declares two things:
      the name is new and written by nobody else, and nothing reads the object
      before `put_object`. The asset manager sets the hint for chunks,
      manifests, snapshots and transaction logs.
    - **Failure handling.** A crash mid-write leaves a truncated file that
      nothing references, and GC deletes it like any orphan. On a write error
      the partial file is deleted.
    - **Safety net.** If the exclusive open fails with `EEXIST`, the promise
      didn't hold (for example, a retry of the same write). The write falls
      back to the default staged path, before any data was written. This
      protects from retries.

    The hint is a parameter of `put_object`, ignored by the other backends,
    whose PUTs are all-or-nothing anyway.
  - **`put_object_conditional(Create)`** on any key but `repo`: staging file
    plus `link`. `EEXIST` means the object exists: `NotOnLatestVersion`.
  - **`copy_object`**: the backup is staged and renamed into place.

  The parent directory is fsynced after commit-critical renames and links.
  When directory entries of data files are fsynced is open question 2. A
  directory fsync failing with `EINVAL` or `ENOTSUP` is treated as
  unsupported by the filesystem and warned about once. Any other error after
  a successful rename or link is reported as an I/O error, and the write may
  have landed.
- **`fsync` on by default.** A storage setting disables it for data files,
  for users who accept that a power loss can leave chunks and metadata
  files empty. On NFS, `close()` already flushes and COMMITs, so the cost
  there is small. The fsyncs of the commit path, of creation (the stub
  included), of freeze and of unfreeze always run: a marker naming
  truncated bytes is the one state nothing recovers from.
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
- **`overwritten/` backups stay as they are:** no change to the asset
  manager, and byte parity with object-store repos. The version files make
  them redundant; dropping them is a later change.
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

  Callers:
  - Every V2 caller of the read methods, and every unconditional
    `put_object` caller, already discards the version.
  - Two IC1 callers keep a `get_object` version for a later conditional write:
    `refs::fetch_branch` (used by `refs::update_branch`) and
    `AssetManager::fetch_config`. Both switch to
    `get_object_conditional(previous_version = None)`.
  - `force_write_repo_info` returns the version from its unconditional put.
    It has no callers in the crate and changes to return `()`.
  - Conditional writers (repo info, repo creation, IC1 refs and config) move
    to `put_object_conditional`.

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

## Known limitation: spurious conflict after a lost commit reply

On NFSv3 and v4.0 a client retries a request whose reply was lost, and the
server may run it twice. If the commit `rename` lands but its reply is
lost, the retry returns `ENOENT`, and `after_failed_rename` lists the
directory. If the listing has caught up it shows our marker and we report
success. If another writer already committed on top of us, or the listing
lags, we report a conflict for a commit that landed.

**What the caller sees.** `update_repo_info_internal` treats the failure as
a conflict and reapplies the update on top of the newer version, so the
user gets a spurious error: a commit finds its branch tip is its own
snapshot and reports a conflict; a tag or branch creation reports "already
exists".

**How likely.** A lost reply, no session (NFSv4.1 and later have
exactly-once semantics), and either a lagging listing or another commit
within milliseconds. Local and coherent filesystems have no lost replies.

**Accepted.** This is the same as the "Overwrite race" caveat of design
doc 017 for object stores. It must be documented in the user-facing docs.

**Follow-up, for all backends.** Every RepoInfo update records a unique,
random backup path in its `Update` entry. When a conditional write fails
in a way that may hide a landed write, the repo-info layer could re-read
`repo` and look for its own backup path in the update history, which turns
the spurious conflict into a success. It requires backends to report an
*ambiguous* failure separately from a *definite* conflict, and enough
update history kept in `repo`. Tracked as a separate issue.

## Implementation notes

What an implementation of this section needs beyond the algorithm.

- **The stub.** The V2 file header (magic, implementation name, spec
  version, file type byte, compression) with a new
  `FileTypeBin::FilesystemStub = 7` and no body. Every shipped version
  fails on it with `UnknownFileType { got: 7 }`. The core RepoInfo reader,
  which already checks the file type after parsing, maps it to an error
  saying the repository uses the filesystem format and must be opened with
  `filesystem_storage`.
- **Name parsing.** `<N>.<h>`, `latest.<K>.<N>.<h>`, `frozen.<K>.<N>.<h>`,
  `stage.<id>`, with the exact widths and alphabets above; `K` must equal
  the complement of `N`. Anything else, `.nfs*` included, is ignored by
  reads and kept by the sweep.
- **Retries.** `read_latest` tries 8 times with backoff doubling from
  10 ms and capped at 500 ms. `list_markers` retries once after a nudge
  of the root on `ENOENT` or `ENOTDIR`. `freeze` and `unfreeze` use the
  same count.
- **Versions.** `IfVersion` without both `etag` and `generation` is an
  invalid-input error. `get_object_conditional` returns `NotModified` when
  the listed marker equals the previous version or has a lower `N`.
- **`open` classification** runs on every NotFound from `read_latest`, at
  the start of `create`, and at the first use of `repo` by each storage
  instance, where only success is cached; the `Storage` trait has no open
  call.
- **Errors.** `RepositoryFrozen` is a new `StorageErrorKind` variant. The
  repo-info update loop excludes it from the conflicts it retries and
  surfaces it unchanged, so Python and JS see an `IcechunkError` with that
  message. `LegacyFormat`, `MigrationInterrupted` and `MixedFormats` are
  errors whose messages name the remedy.
- **API.** `FilesystemStorage` gets `freeze_repo`, `unfreeze_repo` and
  `migrate_legacy_repo`. JS exposes all three on the `Storage` object that
  `Storage.newFilesystem` returns; Python exposes only
  `migrate_legacy_repo`, on the `FilesystemStorage` that
  `filesystem_storage` returns. Export and import are out of scope; the
  freeze is the backend's part of them.
- **Listings.** `list_objects` on the root shows the stub as `repo` with
  the stub's own size and time, and hides `repo.d`, `repo.d.tmp.*`,
  `repo.legacy.*` and `repo.stub.*`.
- **Sweep.** The `sweep_leftovers` hook keeps its signature. The cutoff GC
  passes applies only to `stage.*`, `repo.d.tmp.*`, `repo.stub.*` and the
  root's `<name>.staging.<id>` files of other objects; dead version files go
  whatever their age.
- **Settings.** `stale_after` is removed. `fsync` keeps its meaning for
  data files; the fsyncs in the commit path, in `build_repo_d`, in
  `write_stub`, in `freeze` and in `unfreeze` always run. No compatibility
  obligation: no release has shipped `FilesystemStorage`.
- **Removed from the crate:** the lock, ticket, lockinfo and probe code,
  lock sweeping, `STALE_AFTER`, the fresh-read generation fast path and
  the fault-injection points tied to the lock steps. Kept: staged and
  exclusive writes, directory fsync, listing filters, the sweep hook.
- **Fault injection** (`fault-injection` feature) keeps the hook trait,
  with points after the staging write, after the link, before the rename,
  after the rename and before tidy-up, so the stress tool can kill or
  pause there. The hooks also wrap the protocol's `link`, `rename` and
  `unlink`, for lost-reply tests, and `sabotaged(Sabotage)` turns on each
  sabotage of the self-test below.
- **Stress tool.** Storage and repository modes and the oracle are
  unchanged; the clean-root check expects `repo`, and `repo.d/` holding
  one marker and one version file, and ignores `.nfs*`. The self-test
  sabotages become one per way an implementation could break the
  protocol, each of which the oracle must report as a lost update, a
  false acknowledgement, or a repository it cannot read:
  1. commit without publishing the bytes (skip the link);
  2. rename from the listing's current marker instead of the base (no
     compare);
  3. acknowledge on `ENOENT` from the rename without listing again;
  4. acknowledge in every branch of `after_failed_rename` that the listing
     doesn't prove, `m >= N+2` included: the final `_` branch alone
     essentially never fires on a local filesystem;
  5. tidy-up deleting the current version;
  6. tidy-up or sweep deleting files above the marker, with concurrent
     writers so an in-flight attempt commits a marker without bytes;
  7. unlink the base before the rename;
  8. link the content after the rename, with in-window kills;
  9. `after_failed_rename` deleting the winner's file instead of its own;
  10. the `object_store` filesystem backend, which overwrites
      unconditionally, as the control.

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

  Configurations checked (2026-10-11, release build, 32 threads), all passing:

  | Writers × attempts | Faults | Unique states |
  | --- | --- | --- |
  | 2 × 2 | none | 816 |
  | 2 × 2 | lost replies | 3,466 |
  | 2 × 2 | stale listings | 1,216 |
  | 2 × 2 | stale listings, lost replies, one freeze | 182,175 |
  | 2 × 1 | everything, one freeze | 58,233 |
  | 2 × 2 | everything but stale listings, one freeze | 1,218,058 |
  | 2 × 2 | everything, one freeze (ignored) | 10,070,673 |
  | 3 × 1 | everything, one freeze | 1,157,560 |
  | 3 × 2 | everything but freezing (ignored) | 240,648,054 |

  `just test` runs the configurations not marked ignored, among them every
  fault with one attempt per writer, and every fault but stale listings
  with two. `just fs-model-check` runs the ignored ones, except three
  writers with two attempts and a freeze: about 5 minutes and 2.4 GB in
  release. That configuration is too large to finish; it is kept as an
  ignored test to document it.

- **NFS properties.** Every operation the protocol relies on was exercised
  on two NFS client mounts of one export, on NFSv4.2, 3 and 4.0: exclusive
  marker rename, content link and directory rename across clients, rename
  of a cached name another client renamed away, no silly rename from any
  protocol operation, listings during renames, the nudge, reads of a file
  deleted by the other client, the migration read, directory fsync. Script
  and findings: `icechunk-fs-nfs-verify.py` and `icechunk-fs-nfs-verify.md`
  (kept outside the repo). Lost replies need fault injection and are
  untested.
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
  - **Modes.** In storage mode every worker updates `repo` with
    `put_object_conditional`, backing up the base to
    `overwritten/<attempt id>` first. In repository mode workers commit to
    their own branch, commit to `main` with rebase, and create tags, each
    commit overwriting one chunk; optionally a GC worker runs throughout.
  - **Faults.** Through test-only protocol hooks of the backend
    (`fault-injection` feature), workers `SIGKILL` themselves or pause at
    chosen protocol steps. Each host's supervisor also `SIGKILL`s a random
    worker every few seconds and respawns it. A run fails unless enough
    faults of each kind fired.
  - **Oracle.** Workers log each intent before the attempt and its outcome
    after. Storage mode rebuilds the chain of versions from the backups;
    repository mode walks every branch's ancestry and checks the tags and
    each worker's last chunk. Every acknowledged update must be on the chain
    after its base, and nothing may appear that no intent explains. A
    conflict that landed fails a run on local filesystems; on network
    filesystems it is a warning, the lost-reply limitation above. An error
    that landed is always a warning.
  - **Liveness and recovery.** No gap between acknowledged updates may exceed
    30 s (a warning on network filesystems). After the run, a fresh writer
    must succeed within a few seconds. Then the storage root's files are
    aged past the sweep's 24-hour floor and swept (by the storage's sweep or
    by GC), after which only `repo`, and `repo.d/` holding one marker and
    one version file, may remain in the storage root; `.nfs*` files are
    ignored. Staging files in subdirectories are reported as a warning (open
    question 3).
  - **Multi-host runs.** Operators run it on any filesystem mounted on several
    hosts. The hosts coordinate only through the shared run directory:
    `control` is replaced by rename, and the participant list is decided once,
    by the first host to hard-link its list to `go`.
  - **Self-test.** `fs-stress selftest` runs each mode once per sabotage
    listed under [Implementation notes](#implementation-notes). Every case
    must be detected as a lost update, a false acknowledgement, or a repo
    the oracle can't read. It exercises only the tidy-up half of sabotage
    6, since no sweep runs while writers are live; a unit test covers the
    sweep half.
  - **Results.** Local runs on tmpfs and ZFS pass. The self-test detects all
    20 cases (9 sabotages plus the legacy control, in both modes). On two
    NFS client mounts of one loopback export, 5 minutes each:
    - NFSv4.2 storage mode passes with 4,415 acknowledged updates and no
      violation, and repository mode with 5,139;
    - NFSv3 storage and repository modes show no protocol violation (4,561
      and 3,673 acknowledged updates). Their cleanup failures are why the
      stress tool ages leftovers by path and skips names that are gone, and
      why protocol and staging files are created without `O_EXCL` (open
      question 8).

## Open questions

1. **SMB.** Not analysed: Linux cifs hard-link support, lease breaks on
   rename, Samba vs Windows servers.
2. **Durability of data files' directory entries.** A chunk, manifest,
   snapshot or transaction log is only durable once its *directory entry* is,
   and that entry must be durable before `repo` points at the file. Backups
   in `overwritten/` are data files in the same sense: their directory entry
   isn't fsynced either.
   - Fsyncing the parent directory after every write is simple and correct,
     but costs one journal commit per chunk on local disks. That caps write
     throughput at a few thousand chunks per second.
   - Fsyncing each data directory once before the commit is much cheaper, but
     chunks are often written by other processes (e.g. dask workers) than the
     one that commits. Each writer process would have to sync before handing
     its changes over, and the API has no hook for that today.

   On NFS this is free either way, because the server makes metadata
   operations durable before replying.
3. **Leftovers outside the repo root.** GC doesn't sweep staging files
   outside the repo root: of backups in `overwritten/`, of the initial
   snapshot and transaction log, and of an `EEXIST` fallback in any
   directory. How to sweep them without a second full listing of large
   directories such as `chunks/` is open.
4. **Windows.** NTFS has hard links and atomic rename. Replacing a file
   another process opened without `FILE_SHARE_DELETE` fails, which would
   surface as a failed unlink in tidy-up, never as corruption. Windows NFS
   clients would be unsupported. The backend is not implemented on Windows.
5. **Non-Linux NFS clients** (macOS, FreeBSD) are not analysed or tested.
   Safety does not depend on them, but the nudge has no equivalent there, so
   readers lag by the client's directory attribute cache timeout.
6. **Lost replies** on NFSv3 and v4.0 have not been exercised; provoking
   them needs network fault injection.
7. **Two physical clients.** All NFS verification ran on two mounts of one
   loopback export, which share one NFSv4 client id.
8. **Exclusive creates on NFSv3.** An `O_EXCL` create killed before the
   client's follow-up SETATTR leaves a mode-000 file whose mtime is the
   client's verifier, possibly far in the future. Protocol and staging files
   therefore don't use `O_EXCL`. Data files written under their final name
   still do, and an orphan left this way is never aged out by GC's mtime
   cutoff.
9. **Export, import and a continuous mirror** between the two formats are
   tools yet to be written; the freeze is the backend's part of them.
10. **An object-store variant** of this format would list `latest.*`
    newest first in one request, but its commit would be a conditional
    create of a marker object that is never deleted, a different protocol
    from the rename used here. The marker key keeps that door open.

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
