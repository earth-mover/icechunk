# Icechunk 2 — Major Changes & New Features

## New Features

**Rectilinear Chunk Grids**: Support for Zarr 3 rectilinear (variable-sized) chunk grids, where each chunk along a dimension can have a different size. Chunk shapes can be specified inline (fully listed) or run-length encoded.

**Move**: `move_node(from, to)` moves or renames arrays and groups. Unlike vanilla Zarr, this is a cheap metadata-only operation that doesn't require copying any chunks. Requires a dedicated `rearrange_session` (cannot be mixed with data writes).

**Shift & Reindex**:

- `shift_array(path, offset)`: shift all chunks by a chunk offset
- `reindex_array(path, fn)`: arbitrary chunk coordinate transformation via a user-provided function

**HTTP Storage backend**: Read-only access to repositories served over HTTP/HTTPS, enabling public or CDN-hosted repos.

**Amend**: `amend()` replaces the previous commit on a branch instead of creating a new one.

**Experimental WASM Build**: Icechunk core compiles to WASM (single-threaded tokio runtime), enabling browser/Node.js usage.

**Repository Status**: Repos can be marked `Online`, `ReadOnly` with a reason. Status is checked before operations proceed.

**Feature Flags**: Per-repo toggleable flags (e.g., `move_node`, `create_tag`, `delete_tag`) stored in repo info. Allows disabling specific operations.

**Repository-Level Metadata**: Key-value metadata on the repo itself (not just on snapshots).

**Redirect Storage**: `RedirectStorage` follows HTTP 302 redirects to resolve the actual storage backend (S3, R2, Tigris, GCS). Useful for CDN/load-balancing scenarios.

**Anonymous Snapshots**: `session.flush(message)` creates a detached snapshot without advancing the branch. In IC2 these are first-class citizens tracked in the repo info and ops log.

**Better session fork**: `session.fork()` now creates an anonymous snapshot capturing the base session's state and returns a `ForkSession` that allows writes but disallows commits. Fork sessions are serializable (picklable) for distribution to workers, and are merged back via `session.merge(fork_session)`.

**Empty Commits**: `allow_empty(true)` on `CommitBuilder` permits commits with no data changes.

**Rebase attempt tracking**: Rebase attempts are recorded in snapshot metadata, giving visibility into conflict resolution history.

**Inspect/Debugging Tools**: `inspect_snapshot()`, `inspect_repo_info()`, `inspect_manifest()` return detailed JSON introspection of internal structures.

**IC1 → IC2 Migration**: `migrate_1_to_2()` with dry-run support.

---

## Consistency

In V1, refs (branches/tags) and repository config were independent objects in storage. Updates could race, and operations like GC or config changes had no way to get a consistent view of the full repository state.

In V2, all repository state is referenced from the unified repo info object. Every mutation, whether it's a commit, branch operation, config change, GC, or expiration, is applied to the freshest state using optimistic concurrency control with automatic retry and backoff on conflict.

---

## New On-Disk Format (Spec V2)

**Unified Repo Info file**: The biggest architectural change. V1 stored refs (branches/tags) as separate objects in the store. V2 consolidates everything into a single `repo` file containing:

- Full snapshot ancestry: parents, timestamps, commit messages. No more sequential snapshot reads.
- All branches, tags, and deleted tags.
- Repository status, metadata, config, and feature flags.
- An **operations log**, an immutable audit trail of every repo mutation. Every mutation is recorded: commits, amends, tag/branch create/delete, GC runs, config changes, migrations. Each entry has a timestamp and a backup pointer to the previous repo info file.

**FlatBuffers schemas**: All on-disk types (repo info, snapshots, manifests, transaction logs) are formally defined in `.fbs` schema files. Shared types (`ObjectId12`, `ObjectId8`, `MetadataItem`) extracted into `common.fbs`.

**Extensibility via `extra` fields**: Snapshots, nodes, manifests, chunk refs, and transaction logs all gained opaque `extra: [uint8]` fields for future innovation without format bumps.

**`DimensionShapeV2`**: V1 stored `(array_length, chunk_length)` per dimension, which only works for regular chunk grids. V2 stores `(array_length, num_chunks)`, the number of chunks along each axis, which is what Icechunk actually tracks internally. This decouples the on-disk format from the assumption of uniform chunk sizes, and is the foundational change enabling rectilinear chunk grids.

**`ManifestFileInfoV2`**: V2 uses a flatbuffers `table` with an optional `extra` field, allowing future extensibility without format bumps.

**Transaction logs now track move operations**:  A new `moved_nodes: [MoveOperation]` field records path renames.

---

## Virtual Chunk Improvements

**Relative virtual chunks (`vcc://` URLs)**: Virtual chunk locations can reference named Virtual Chunk Containers: `vcc://container-name/relative/path`. Repos can be relocated without breaking references.

**Zstd dictionary compression**: Virtual chunk URLs (often repetitive S3 paths) are automatically compressed using a trained zstd dictionary per manifest.

---

## Performance

**Parallel flush**: Array nodes are flushed in parallel during commit,  significantly speeding up commits with many arrays.

**Optimized manifest writes**: Manifest splitting without caching splits.

**NodeSnapshot cache**: cache on `Snapshot` for frequently accessed nodes, reducing deserialization.

**Faster ancestry/GC**: All snapshot info in the repo file enables O(1) ancestry lookups and fast expiration calculations (no sequential snapshot reads).

**Concurrent tx log fetching in rebase**: Transaction logs are now fetched concurrently during rebase, speeding up conflict detection.

---

## Reliability & Storage

**Expanded retries**: Retries on HTTP 408 (timeout), 429 (rate limit), 499 (client closed), plus connection reset and stalled stream detection. Configurable `max_tries`, backoff, and per-operation timeouts.

**Timeout settings**: `TimeoutSettings` wired through to S3 client: connect, read, operation, and attempt timeouts. Stalled stream protection with configurable grace period.

**User agent headers**: All requests include `icechunk-rust-<version>` for tracing.

**GC consistency**: Handles concurrent repo modifications during GC via retry with exponential backoff. Detects and re-processes when new snapshots appear during collection. Proper parent rewriting when deleting snapshot chains.

---

## Testing & Quality

**Stateful property tests**: Extended with move operations, two-version compat testing, GC/expiration invariants, and upgrade scenarios.

**Shuttle concurrency testing**: Probabilistic concurrency testing (PCT) with `shuttle` explores thread interleavings across commits, branch ops, and amends. Caught real race conditions.

**Criterion microbenchmarks**: Benchmarks for snapshot serialization, manifest writing, and asset manager operations.

**Parameterized V1/V2 tests**: `spec_version_cases!` macro runs tests against both spec versions automatically.

**Toxiproxy network fault testing**: Simulates connection resets, stalls, and other network failures to validate retry logic.

---

## Python API Changes

- Enums changed to **snake_case** (e.g., `session_type.writable` instead of `SessionType.Writable`)
- `manifest_files()` renamed to `list_manifest_files()`
