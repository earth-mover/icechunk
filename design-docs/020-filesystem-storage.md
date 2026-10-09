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

## Goals and constraints

- No lost updates.
- No change to the on-disk format. Existing repos are not modified.
  Extra files can only exist while a commit is in progress, plus garbage.
- Timeouts may affect liveness, not safety.
- We only need to support Icechunk V2.

## Names

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

The conditional-update protocol works for any object: for an object `<name>`
the files are `<name>.new_bytes.<attempt_id>`, `<name>.lockinfo.<attempt_id>`
and `<name>.lock.<h>.<gen>` next to it (V2 only updates `repo`
conditionally).

## Algorithm overview

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

## Algorithm detail

The algorithm is implemented by three `Storage` methods:
`put_object_conditional`, `copy_object`, and `get_object_conditional`. (The
split of `put_object` into an unconditional and a conditional method is
described under [Backend](#backend).)

### Conditional update: `put_object_conditional(repo, new_bytes, IfVersion(base_hash))`

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

#### Lock staleness

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

### Backup: `copy_object(repo, overwritten/<name>, version = base_hash)`

Called by `write_repo_info` just before `put_object_conditional`, to save
the version about to be replaced into `overwritten/`. The two calls are
separate, and the backup takes no lock:

1. Read `repo` with a fresh read.
2. If `sha256(bytes) != base_hash`, return `NotOnLatestVersion`.
3. Write *those same bytes* to the backup name: staged, fsynced, renamed into
   place.

The backup is the hashed version by construction, whatever happens
concurrently.

### Reading `repo`: `get_object_conditional`

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

## Algorithm correctness

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

## Version identity and content uniqueness

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

## Lost replies

NFS clients retry a request whose reply was lost. A non-idempotent operation
can then report failure although the first try succeeded. Every case is resolved
by reading content, never by `stat`:

| Operation | Spurious result | Resolution |
| --- | --- | --- |
| `link(lockinfo, lock)` | `EEXIST` | The lock's `attempt_id` is ours: we hold the lock. |
| `link(staging, name)` (conditional create) | `EEXIST` | A fresh read of the target hashes to `sha256(new_bytes)`: the create succeeded. |
| `rename(ticket, repo)` | `ENOENT` | A fresh read of `repo` hashes to `sha256(new_bytes)`: we committed. |
| `unlink(other's ticket)` | `ENOENT` | Irrelevant: the ticket is gone either way. |

## Crash recovery

| Crash after | Left on disk | Resolution |
| --- | --- | --- |
| Step 1 | ticket and lockinfo | Swept. Deleting a ticket is always safe; at worst a live attempt fails. |
| Steps 2–4 | `repo.lock.<base_hash>.<gen>` | After `STALE_AFTER` the next writer steals gen+1. No manual unlock. |
| Step 5 | `repo` updated, lock files remain | The lock family's hash is no longer current. Swept. |

## Sweeping

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

## Fresh read

This is Claude's analysis.

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

## Filesystem requirements

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
  on NFSv3. Open question 3 describes a possible check.

**Expected to qualify:** local POSIX filesystems, NFSv3/v4 with a Linux
client, Lustre, GPFS, CephFS, JuiceFS.

## Backend

- **Crate and type.** A new crate `icechunk-fs` with `FilesystemStorage`,
  implemented directly on `std::fs` with `spawn_blocking`. It does not use
  `object_store`.
- **Platforms.** Unix only. On other platforms the constructors return an
  error. Windows is open question 8.
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
- **Writes.** Every write ends with `fsync` and `close`, checking the error
  from `close` because on NFS, write errors such as out of space or over quota often
  only surface there.
  - **`put_object`, default**: staging file plus `rename`. Like a PUT
    on an object store, the write is all-or-nothing: no reader ever sees a
    partial object.
  - **`put_object` with the new-object hint**: written directly under its
    final name, opened with `O_CREAT|O_EXCL`. No staging file and no `rename`.
    - **The caller's promise.** With the hint, the caller declares two things:
      the name is new and written by nobody else, and nothing reads the object
      before `put_object`. The asset manager sets the hint for chunks, manifests,
      snapshots and transaction logs.
    - **Failure handling.** A crash mid-write leaves a truncated file that
      nothing references, and GC deletes it like any orphan. On a write error
      the partial file is deleted.
    - **Safety net.** If the exclusive open fails with `EEXIST`, the promise
      didn't hold (for example, a retry of the same write). The write falls
      back to the default staged path, before any data was written. This
      protects from retries.

    The hint is a parameter of `put_object`, ignored by the other backends,
    whose PUTs are all-or-nothing anyway.
  - **`put_object_conditional(Create)`**: staging file plus `link`. The
    `link` needs complete content behind it. `EEXIST` → fresh read of the
    target: the same hash is a success (see Lost replies), anything else
    `NotOnLatestVersion`.
  - **`put_object_conditional(IfVersion(hash))`**: the conditional update,
    staged in the ticket.
  - **`copy_object`**: the backup is staged and renamed into place.

  The parent directory is fsynced after commit-critical renames and links.
  When directory entries of data files are fsynced is open question 6. A
  directory fsync failing with `EINVAL` or `ENOTSUP` is treated as
  unsupported by the filesystem and warned about once. Any other error after
  a successful rename or link is reported as an I/O error, and the write may
  have landed.
- **`fsync` on by default.** A storage setting disables it, for users who
  accept that a power loss can leave `repo` pointing at empty files. On NFS,
  `close()` already flushes and COMMITs, so the cost there is small. The
  lockinfo is always fsynced, even with fsync disabled: an empty lock file
  would block its lock family until manual recovery.
- **IC1 repos are refused.** Opening or creating an IC1 repo with this
  backend fails with an error that points to migrating the repo to V2.
  The backend declares that it requires V2
  through a new `Storage` capability method, which defaults to "all versions"
  for the other backends.
- **Versions.** Only the conditional calls produce a version:

  | Call | `etag` | `generation` |
  | --- | --- | --- |
  | `get_object_conditional` | `sha256(content)` | `fstat` tuple `(dev, ino, size, mtime_ns, ctime_ns)` |
  | `put_object_conditional` | `sha256(new_bytes)` | `fstat` of the written file after its final rename or link (for a create: of the target, kept only if a fresh read hashes to `sha256(new_bytes)`) |

  If that `fstat` fails, the version carries only the etag.

  - **`etag` is the only identity used for compare-and-swap.** Conditional
    writes compare it against a fresh read of `repo`.
  - **`generation` only serves the read path.** It lets
    `get_object_conditional` answer `NotModified` without reading the file.
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

None of these bypass the protocol safely; they all overwrite `repo`
unconditionally:

- older Icechunk versions;
- the `object_store` filesystem backend in the same version;
- `force_write_repo_info`.

Running any of them on a repo at the same time as the new backend is unsafe.
Switching a repo to the new backend once every old writer has stopped is safe.

Open question 9 describes a possible warning.

## Verification

- **Model checker.** A `stateright` model of the algorithm as specified, as a
  dev-dependency in the repo. It covers:
  - steps 1–6, including the early exit, stealing, and `cleanup()`;
  - `copy_object` and GC sweeping of `repo`'s protocol files: tickets and
    lockinfos deleted at any age, and locks listed before a read of `repo`
    that shows another hash. It doesn't model the age cutoff, a missing
    target, other keys, or staging and probe files;
  - every interleaving of several writers, a crash at any point, and lost
    replies on `link` and `rename`.

  Invariants:
  - **No lost update.** Every successful commit replaces exactly the version
    it was based on.
  - **Acknowledged commits are never undone.**
  - **Progress.** Without crashes or GC sweeping, some writer eventually commits.
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
    (`fault-injection` feature), workers `SIGKILL` themselves or pause, past
    `STALE_AFTER`, at chosen protocol steps. Each host's supervisor also
    `SIGKILL`s a random worker every few seconds and respawns it. A run fails
    unless enough faults of each kind fired.
  - **Oracle.** Workers log each intent before the attempt and its outcome
    after. Storage mode rebuilds the chain of versions from the backups;
    repository mode walks every branch's ancestry and checks the tags and
    each worker's last chunk. Every acknowledged update must be on the chain
    after its base, and nothing may appear that no intent explains. A
    conflict that landed fails a run on local filesystems; on network
    filesystems it is a warning, the lost-reply limitation below. An error
    that landed is always a warning.
  - **Liveness and recovery.** No gap between acknowledged updates may exceed
    a threshold derived from `STALE_AFTER` and the pause length (a warning on
    network filesystems). After the run, a fresh writer must succeed within
    `2 × STALE_AFTER + 5 s`. Then the storage root's files are aged past the
    sweep's 24-hour floor and swept (by the storage's sweep or by GC), after
    which only `repo` may remain in the storage root. Staging files in
    subdirectories are reported as a warning (open question 7).
  - **Multi-host runs.** Operators run it on any filesystem mounted on several
    hosts. The hosts coordinate only through the shared run directory:
    `control` is replaced by rename, and the participant list is decided once,
    by the first host to hard-link its list to `go`.
  - **Self-test.** `fs-stress selftest` runs each mode once per sabotaged
    protocol step (skip the fence, skip the compare, trust `ENOENT` from the
    commit rename) and once on the `object_store` filesystem backend, which
    overwrites `repo` unconditionally. Every case must be detected as a lost
    update, or, for a sabotaged step, a repo the oracle can't read.

## Known limitation: spurious conflict after a lost commit reply

**How it happens:**

1. Writer A's step 5 sends `RENAME repo.new_bytes.A → repo`. The server performs
   it, so `repo` now holds A's version, but the reply is lost.
2. The NFS client resends the RENAME. On NFSv3/v4.0 the server's
   duplicate-request cache is best-effort (it can evict the entry or lose it
   on reboot), so the server runs the RENAME again and returns `ENOENT`,
   because `repo.new_bytes.A` no longer exists.
3. Before A's recovery read, writer B reads `repo` (A's version), takes the
   lock for A's hash, and commits. `repo` now holds B's version.
4. A's recovery read finds B's hash instead of `sha256(new_bytes)`. This looks
   exactly like having its ticket deleted, so A returns `NotOnLatestVersion`.

**Result.** A's update landed and B built on top of it, but A is told it
lost. The repo stays consistent and nothing is lost.

**What the caller sees.** `update_repo_info_internal` treats the failure as
a conflict and reapplies A's update closure on top of B, so the user gets a
spurious error:

- a commit finds its branch tip is its own snapshot rather than the expected
  parent and reports a conflict;
- tag or branch creation reports "already exists".

With automatic rebase, a commit could rebase onto its own landed snapshot
and commit the same changes a second time (not yet verified).

**How likely.** All three must coincide: a lost RENAME reply, a miss in the
duplicate-request cache, and another writer's commit within the few
milliseconds before A's recovery read. NFSv4.1+ sessions give exactly-once
semantics and rule out step 2. Local and coherent filesystems have no lost
replies.

**Accepted.** This is the same as the "Overwrite race" caveat of design
doc 017 for object stores, which is accepted there too. It must be
documented in the user-facing docs for the filesystem backend.

**Follow-up, for all backends.** Every RepoInfo update records a unique,
random backup path in its `Update` entry. When a conditional write fails in a
way that may hide a landed write, the repo-info layer could re-read `repo`
and look for its own backup path in the update history. Found means the write
landed, which turns the spurious conflict into a success. This would also
close 017's caveat on object stores. It requires:

- storage backends to report an *ambiguous* failure separately from a
  *definite* conflict;
- enough update history kept in `repo` that the next writer cannot have
  pushed the entry out.

Tracked as a separate issue.

## Open questions

1. **SMB.** Not analysed: Linux cifs hard-link support, lease breaks on
   rename, Samba vs Windows servers.
2. **Non-Linux NFS clients** (macOS, FreeBSD) are not analysed or tested. A
   2010 FreeBSD patch shows that client returned `ESTALE` from `open()`
   instead of retrying the lookup. That's safe (an error) but needs a retry in
   `read_file_fresh`.
3. **`nocto` mounts.** Whether they can still give fresh reads has not been
   investigated. If they can't, the backend could refuse them on Linux, at
   create and open:
   1. If `statfs` on the repo directory doesn't report NFS
      (`f_type == NFS_SUPER_MAGIC`), there is nothing to check.
   2. Find the mount by device number: the line in `/proc/self/mountinfo`
      whose `major:minor` field matches `st_dev` from `stat` on the repo
      directory. Matching paths instead would break with symlinks, bind
      mounts and filesystems mounted over other mounts.
   3. Look for `nocto` in that line's superblock options (the last field),
      not in the per-mount options.
   4. Fail closed: if the repo is on NFS and its options can't be read,
      refuse. A storage setting overrides the check.

   Reading `/proc/self/mountinfo` needs no privileges and shows mounts as our
   process sees them, which is right inside containers. The check would be
   Linux-only, wouldn't notice a remount after it ran, and would only reject
   the one option known to be dangerous. It wouldn't certify a mount as safe.
4. **Manual recovery for a corrupt lock file** in the current lock family: a
   CLI command, or documented steps.
5. **Validating the `STALE_AFTER` default** against metadata stall times
   measured on heavily loaded HPC filesystems.
6. **Durability of data files' directory entries.** A chunk, manifest,
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
7. **Leftovers outside the repo root.** GC doesn't sweep staging files and
   protocol files outside the repo root: staging files of backups in
   `overwritten/`, of the initial snapshot and transaction log, and of an
   `EEXIST` fallback in any directory; protocol files of keys in
   subdirectories, which V2 never updates conditionally. How to sweep them
   without a second full listing of large directories such as `chunks/` is
   open.
8. **Windows.** NTFS has hard links. Replacing a file another process opened
   without `FILE_SHARE_DELETE` fails, which would surface as a failed rename
   and a retry, never as corruption. Windows NFS clients would be unsupported.
   The backend is not implemented on Windows.
9. **Warning about old writers.** The backend could warn when `repo`'s
   `FileHeader.impl_name` shows it was last written by an Icechunk version
   older than the protocol.
