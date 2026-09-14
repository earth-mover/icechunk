# Conditional PUTs on the local filesystem

Icechunk protects every write to a ref and to the repo info object with a
compare-and-swap. On object stores the store enforces it: create-only
writes send `If-None-Match: *`, updates send `If-Match: <etag>`. The
local filesystem backend had only the create half. Updates were plain
overwrites, so concurrent commits on a local disk could lose one of them.

## The problem

The local backend builds on `object_store::LocalFileSystem`. That store
implements `PutMode::Create` with a staged file and an exclusive hard
link. It returns `NotImplemented` for `PutMode::Update`
(`object_store` 0.14.1, `src/local.rs:399`). Icechunk therefore set
`unsafe_use_conditional_update: Some(false)` in the local backend's
default settings and logged this warning on every open:

> The LocalFileSystem storage is not safe for concurrent commits.

Two writes depend on the missing update path:

* the branch update in `refs.rs`, which passes the branch's previous
  version to `put_object`;
* the repo info update in `asset_manager.rs`, which does the same for
  the `repo` object.

With the setting off, both degrade to overwrite. Two sessions that
commit at the same time both succeed. The second overwrite drops the
first commit from the branch, and the repo info loses the first
session's changes.

No POSIX primitive compares file content and replaces it atomically.
Any fix needs a lock, and neither storage library provides one.

## Design

`icechunk-arrow-object-store/src/local.rs` adds
`ConditionalLocalFileSystem`. It wraps `LocalFileSystem`, delegates
every `ObjectStore` method to it, and adds `PutMode::Update` in
`put_opts`:

1. If the mode is not `Update`, or the `Update` carries no etag,
   delegate unchanged. `LocalFileSystem` still answers `NotImplemented`
   for an etag-less update, the same as the S3 store.
2. Open the target file read-only. `NotFound` maps to
   `Error::Precondition`: the object is gone, so the caller's version
   is stale.
3. Take an exclusive advisory lock on that file with
   `std::fs::File::lock`. The open and the lock run in
   `spawn_blocking`.
4. `head` the object and compare its etag to the expected one. A
   mismatch returns `Error::Precondition`.
5. On a match, delegate the put with `PutMode::Overwrite`. The inner
   store writes a staged file and publishes it with `rename`.
6. Drop the file handle. That releases the lock.

The lock lives on the target file's own inode. There is no sidecar
lock file, so `list` output does not change and nothing needs cleanup.

### Why this is sound

`LocalFileSystem` computes the etag as `"inode-mtime-size"`
(`src/local.rs:1434`). Every publish is a `rename` of a staged file.
Each publish therefore installs a new inode and a new etag.

Consider writers that all hold the same expected etag, for inode X.
Any writer that opened the path while it pointed at X holds a handle on
X. The exclusive lock serializes those writers. The first one passes
the compare and publishes inode Y. A later writer is in one of two
states. Either it still holds a lock on the unlinked X and now reads
Y's etag in step 4. Or it opened the path after the rename and locks Y
directly. Both states fail the compare. Two writers cannot both pass
step 4 against the same etag.

### Backend wiring

* `LocalFileSystemObjectStoreBackend::mk_object_store` returns
  `ConditionalLocalFileSystem` on unix and the bare `LocalFileSystem`
  elsewhere.
* The local default becomes
  `unsafe_use_conditional_update: Some(cfg!(unix))`.
* The concurrency warning in `ObjectStorage::new_local_filesystem`
  fires only on non-unix platforms.
* The storage integration test
  `test_write_config_fails_on_bad_version_when_existing` drops its
  local-filesystem special case. Local storage now behaves like the
  object stores.

### Network filesystems

`flock` is advisory, and on a network filesystem it may guard one
client only. `local::network_filesystem` names the filesystem that
holds the repository path through `statfs(2)`. Linux reports a magic
number; the table covers nfs, smb, cifs and smb2. BSD and macOS report
a name; the table covers nfs, smbfs and webdav. The probe walks up to
the first existing ancestor, because a repository is often created
before its directory exists.

When the probe names a network filesystem, `default_settings` sets
`unsafe_use_conditional_update: Some(false)`. It logs a warning that
names the filesystem and the path. An explicit
`unsafe_use_conditional_update = true` in the user's settings still
overrides it.

A manual check under `icechunk-arrow-object-store/checks/nfs-lock/`
mounts one NFS-Ganesha export from two client containers and probes
the lock. Against Ganesha 6.5 over NFSv4.1, `flock` did reach the
server. A contended blocking lock failed with `EIO` instead of
waiting. A lock on NFS therefore either works or fails every contended
commit. Both outcomes argue for the default: keep conditional updates
off there. The check is not part of `just test`; its README lists the
prerequisites.

## Alternatives considered

* **Move the local backend to Apache OpenDAL.** OpenDAL 0.59.1's `fs`
  service does not solve the problem. Its capability block sets
  `write_with_if_not_exists` only. It has no `write_with_if_match`, and
  it reports no etag at all. With `atomic_write_dir` set, even
  `if_not_exists` is a check-then-act race: a `try_exists` check, then
  a `rename` that overwrites. Without `atomic_write_dir`, writes
  truncate the target in place, and readers can see partial files.
  `object_store`'s local store is stronger on every one of these
  points. OpenDAL's `s3`, `gcs` and `azblob` services do support
  conditional writes. Icechunk already has a native S3 backend and an
  `object_store` backend for the rest. A wider migration gains nothing
  for this goal.
* **A sidecar lock file per object.** Lock files under the repository
  root appear in every `list` over that prefix. Refs and GC would see
  them. Deleting a lock file after unlock reopens the classic unlink
  race: two writers lock two different inodes of the same path. Locking
  the target inode avoids both problems.
* **One lock for the whole repository.** Simple, but it serializes
  every ref and repo info write behind one lock.
* **Upstream `PutMode::Update` to `arrow-rs-object-store`.** The lock
  design is a policy choice the upstream may not accept. It would also
  tie the fix to their release cycle. Nothing in this design prevents a
  later upstream contribution.

## Caveats

* **Advisory locks.** `File::lock` is `flock` on unix. Only cooperating
  processes honor it. Every icechunk writer goes through this code, so
  that holds for icechunk. A process that edits the files by other
  means is outside the guarantee, as it always was.
* **Network filesystems.** Detection covers the filesystems in the
  `statfs` tables above. A network filesystem that reports another
  type, such as a FUSE mount, passes as local and gets the lock path.
  The doc comment on `new_local_filesystem` states this limit.
* **Windows.** `std::fs::File::lock` exists on Windows, but a `rename`
  over a file with an open handle may fail there. `object_store` has
  seen "Access is denied" on Windows for rapid operations on one path
  ([#714](https://github.com/apache/arrow-rs-object-store/issues/714)).
  Nobody has verified the lock path on Windows yet, so it is gated to
  `cfg(unix)`. Windows keeps the overwrite behavior and the warning.
* **No write-id recovery.** The local backend has
  `unsafe_use_metadata: Some(false)`, so conditional PUTs carry no
  write-id. The lost-response recovery from
  [017](017-conditional-put-lost-response.md) does not apply. A local
  filesystem has no lost responses, so nothing is lost.
* **Etag stability.** The soundness argument relies on every publish
  installing a new inode. That is how `LocalFileSystem` publishes
  today. An in-place write path in a future `object_store` release
  would weaken the argument and must be checked on upgrade.

## Testing

Unit tests in `local.rs` run through `ObjectStorage::put_object`, so
they cover the settings plumbing as well as the store:

* an update with the current etag lands and returns a different etag;
* an update with a stale etag returns `NotOnLatestVersion` and leaves
  the file unchanged;
* an update after the file was deleted returns `NotOnLatestVersion`;
* eight tasks, each with its own client, run fifty read-compare-write
  increments on one file and retry on `NotOnLatestVersion`. The final
  value must equal 400.

The last two tests were checked by mutation. With the lock removed, the
counter test fails. With the `NotFound` mapping removed, the delete
test fails.

Two more tests cover the filesystem probe. A temporary directory and a
child path that does not exist yet both report no network filesystem.
The `statfs` tables map the documented magic numbers and names.

`refs::tests::test_concurrent_branch_updates_keep_one_winner` races
eight writers through `update_branch` behind a barrier. Exactly one
writer must move the branch. The others must get a `Conflict` that
names the shared parent.

## Future work

* Enable the lock path on Windows once a CI run shows that `rename`
  over a locked handle works there. Otherwise find a Windows-specific
  lock design.
* Contribute `PutMode::Update` for `LocalFileSystem` upstream if the
  `arrow-rs-object-store` maintainers want it.
