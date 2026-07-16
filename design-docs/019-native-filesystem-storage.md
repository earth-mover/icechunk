# Native filesystem `Storage` for safe concurrent commits

## Problem

Local filesystem storage warns on every construction
(`icechunk-arrow-object-store/src/lib.rs:324`):

> The LocalFileSystem storage is not safe for concurrent commits.

Root cause: exactly one operation is unsafe — updating an existing branch tip.
`update_branch` (`icechunk/src/refs.rs:190`) does fetch → compare parent →
conditional put (compare-and-swap on the fetched `VersionInfo`) of
`branch.<name>/ref.json`. object_store's `LocalFileSystem` returns
`NotImplemented` for `PutMode::Update`, so the local backend sets
`unsafe_use_conditional_update: Some(false)`
(`icechunk-arrow-object-store/src/lib.rs:968`) and `get_put_mode`
(`lib.rs:489`) degrades the CAS to a blind `PutMode::Overwrite`. Two
concurrent committers that read the same parent both "win"; one commit is
silently lost.

Already safe today (do not change): branch/tag creation and the tag-delete
marker use `VersionInfo::for_creation()` → `PutMode::Create`, which
LocalFileSystem implements atomically (staged temp file + `hard_link`, EEXIST
on loss). Chunk/manifest/snapshot writes are content-addressed; races are
benign.

Upstream fix is not coming: apache/arrow-rs#6536 (add `PutMode::Update` to
LocalFileSystem) was closed unmerged — maintainers require cross-process
locking plus a design doc, and LocalFileSystem's etag (`inode-mtime-size`) is
inherently collidable (icechunk ref files are fixed-size, so two commits in
one mtime tick are indistinguishable).

## Decision

Replace the object_store-based local filesystem backend with a **native
`FilesystemStorage`** implementing the `Storage` trait directly over
`tokio::fs`, mirroring the structure of `icechunk-s3` (`icechunk-s3/src/lib.rs:105-126,781-785` —
direct impl, sealed, typetag). The on-disk repo layout is unchanged (same
`ref.json` scheme, spec-compatible; a local repo stays `cp`-able to S3).

Rejected alternatives:
- Delta-rs-style numbered ref files per commit: lock-free and sound, but
  changes the normative ref layout in the spec → format fragmentation.
- Lock-file patch inside the existing object_store backend: keeps collidable
  mtime etags, so the CAS compare is only probabilistically correct.

## Design

### Version tokens
ETags are **content hashes** of the object bytes (ref files are tiny; hash
whatever we serve/accept). Distinct ref contents (different snapshot IDs)
can never collide, unlike mtime-based etags. Etags are ephemeral —
fetched-then-used, never persisted — so no migration for existing repos.

### Conditional update (the CAS)
`put_object` with a non-create `previous_version`:
1. Acquire a lock file next to the target via **atomic exclusive create**
   (POSIX `O_EXCL`; the `link()` idiom where needed; Windows
   `CreateFile(CREATE_NEW)`). Lock contents: pid, hostname, timestamp.
2. Re-read the target, hash, compare with `previous_version`. Mismatch →
   release lock, return `VersionedUpdateResult::NotOnLatestVersion`
   (refs.rs already retries/raises Conflict on that).
3. Write temp file in the same directory, fsync, atomic rename over target,
   fsync parent dir, release (unlink) lock.

Stale locks: on contention, if the lock is older than a timeout, delete it
and retry. Residual unsoundness (a paused process resuming past expiry) is
bounded and standard.

Conditional create (`is_create()`): open temp + exclusive-create link/rename
into place; EEXIST → `NotOnLatestVersion`. No lock needed.

**Never rely on `flock`/`fcntl` for correctness** — on Lustre it is
node-local unless mounted `-o flock`; false safety on exactly the HPC
systems this targets.

### Platform notes
- NFSv2 `O_EXCL` is unreliable → use the `link()` create idiom.
- NFS rename may report spurious errors after success → verify-then-decide
  on ambiguous errors.
- `renameat2(RENAME_NOREPLACE)` only where kernel/fs support it; fall back
  to `link()`+`unlink()`.
- Windows: `CREATE_NEW` for exclusive create; `MoveFileEx(MOVEFILE_REPLACE_EXISTING)`
  for atomic replace.

### Surface to implement
Trait: `icechunk-storage/src/storage.rs:466-681` (`#[async_trait]`,
`#[typetag::serde]`, super-traits `Debug + Display + Sealed + Sync + Send`).
Required: `storage_info`, `can_write`, `get_object_range`, `put_object`,
`copy_object`, `list_objects`, `delete_batch`, `get_object_last_modified`,
`get_object_conditional`. Override `create_location_if_needed`
(`create_dir_all`) and `default_settings`. Note: refs listing must be sorted
(fs dir order is arbitrary; cf. `artificially_sort_refs_in_mem`,
`icechunk-arrow-object-store/src/lib.rs:954`).

Wiring: `new_local_filesystem_storage` (`icechunk-arrow-object-store/src/lib.rs:1709`,
re-export `icechunk/src/lib.rs:66`), `ObjectStoreConfig::LocalFileSystem`
(`icechunk/src/config.rs:66-81`, matched in `virtual_chunks.rs:189`),
Python `PyStorage.new_local_filesystem` (`icechunk-python/src/config.rs:3053`),
`icechunk-python/python/icechunk/storage.py:69,139`, JS
`icechunk-js/src/storage.rs`.

### Serde / typetag back-compat
`Arc<dyn Storage>` serializes via typetag through rmp_serde
(`repository.rs:631-635`; Python session `as_bytes`,
`icechunk-python/src/session.rs:144`) — this reaches pickles. A new type is
a new discriminant. Requirements:
- Old serialized blobs (object_store local backend typetag name) must still
  deserialize and work.
- Add a **third-wheel cross-version test**: `icechunk_v1` (renamed old
  package, `[tool.third-wheel]` in `icechunk-python/pyproject.toml`,
  pattern in `tests/test_stateful_compat.py`) pickles a session/storage on
  local filesystem storage; dev version unpickles and commits.

### Acceptance gates
1. Conformance: add to the `with_storage` matrix in
   `icechunk/tests/test_storage.rs:154-251` (local_filesystem entry ~:201).
2. Concurrency: enable `icechunk/tests/test_concurrency.rs`
   (`do_test_concurrency`, :71) for local fs — currently deliberately
   excluded (:33-60). Add a multi-**process** commit stress test (threads
   don't exercise cross-process exclusivity).
3. Third-wheel serde back-compat test (above).
4. Remove the construction warning; document NFS/Lustre caveats instead.
