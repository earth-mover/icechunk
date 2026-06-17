# Ingest roadmap

## Design note: how ingest reaches `ObjectStoreBackend` from `Storage`

The ingest source API takes `icechunk.Storage` (same type as the destination
repo's storage). Inside Rust, the PyO3 binding needs to extract an
`Arc<dyn ObjectStoreBackend>` from the `Storage` trait object so it can
call `mk_object_store(&settings)` and produce an `Arc<dyn ObjectStore>`
for the ingest core.

We have two reasonable shapes for that extraction:

**Chosen: runtime downcast via `Storage::as_any`.**
`icechunk-storage::Storage` exposes `fn as_any(&self) -> &dyn Any;`. The
binding downcasts to `&ObjectStorage` (the only concrete `Storage` impl
that has a backend today) and reads `backend()` directly.

**Considered but rejected: move the `ObjectStoreBackend` trait into
`icechunk-storage` and add `fn object_store_backend(&self) ->
Option<Arc<dyn ObjectStoreBackend>>` to the `Storage` trait.**
This would give us typed-dispatch instead of runtime downcasting and
would put `ObjectStoreBackend` next to its sibling `Storage` trait at
the abstraction layer where it semantically belongs.

The reason we didn't take that path: `icechunk-storage` is deliberately
I/O-agnostic. Its `Cargo.toml` has no `object_store` dep, no I/O crate
deps at all — it's the workspace's "Storage trait and shared types,
nothing more." Adding `object_store` (even with `default-features = false`)
would make the crate aware of one specific I/O ecosystem at the
definition layer. The downcast hack is the smaller compromise — it
stays inside `icechunk-arrow-object-store` for the typed accessor and
only adds a one-method escape hatch to the otherwise-pure trait.

If a future refactor moves icechunk-storage away from "pure
abstraction" (e.g. when adding capabilities that genuinely couple
Storage to an I/O substrate — virtual chunk references, server-side
copy primitives), revisit this decision and consider F.2.



Tracks known gaps and follow-up work for `icechunk.from_zarr` /
`icechunk::ingest::ingest_zarr`. Not a release blocker; landing the
core feature first, then revisiting as users hit limits.

## Open items

### Destination-side path remap

Today the ingest writes every source key 1:1 into the destination
repo's root. Source-side selection works (`paths=...` plus a prefixed
obstore Store). There is no way to say "put the source's `/level_5`
under `/raw/level_5` in icechunk".

Workaround: target a separate branch or repo per dataset.

Possible API: a `dest_prefix: &str` field on `IngestOptions` that gets
prepended to every destination key. Must be threaded through the
cursor format (cursors reference destination keys), so worth careful
design before adding.

### Streaming enumeration for millions of arrays

Both `enumerate_arrays` and `copy_array_chunks` buffer their listings
into `Vec`s before processing:

- `enumerate_arrays` collects every array node path at the branch tip.
  ~100 MB at one million arrays.
- `copy_array_chunks` collects every chunk key for one array. Already
  has a `TODO(scale)` for billion-chunk arrays.

The fix is the same shape for both: stream discovery as we go, drop
each item as soon as it's processed. Practical limit today is roughly
10⁶ arrays per repo; we should revisit when a real workload pushes
past that.

### Same-store copy fast path

When source and destination resolve to the same physical store, we
can skip GET-into-memory + SET:

- Same S3 bucket / provider: `ObjectStore::copy` — server-side copy,
  no egress, single round trip.
- Same local filesystem: `std::fs::copy` or hardlink.
- Same icechunk repo: consider registering a virtual chunk reference
  rather than re-writing bytes at all.

Detection is now straightforward thanks to the `icechunk.Storage`
source API: ingest has typed access to the source's
`ObjectStoreBackend`, and the destination repo's storage is also
reachable via the same `Storage::as_any` → `ObjectStorage::backend`
path. The work is adding a backend-identity comparison method
(`fn same_physical_store_as(&self, other: &dyn ObjectStoreBackend) -> bool`)
plus the right copy primitive per backend.

This overlaps with `mode="virtual"` below.

### Partial / regional ingest

Today the unit of ingest is a whole array (cursor advances by chunk
key). No way to say "copy only chunks under `c/0/`" or "copy only
chunks whose Nth dimension index is in [a, b)". Not on any current
user's request list.

### `ProgressCallback` cancellation

The callback today is `Fn(IngestStats) -> ()`. If users need to abort
a long-running ingest mid-flight, the planned API is a separate
`CancellationToken` field on `IngestOptions` (tokio_util pattern) so
the callback shape stays binary-compatible.

### `mode` kwarg has only one valid value

The Python wrapper accepts `mode: Literal["copy"]` to leave room for
future modes like `"virtual"` (point icechunk's virtual references at
the source bytes without copying) or `"same-bucket-copy"` (S3-side
copy). Keeping the parameter documented today is cheap insurance
against a forward-incompat removal.

## Closed / decided

- **Source API takes `icechunk.Storage`** — same type as the destination
  repo's storage, constructed via `icechunk.s3_storage` /
  `local_filesystem_storage` / etc. The PyO3 binding is a thin wrapper;
  detection of backend sortedness and credentials happens in Rust via
  `Storage::as_any` → `ObjectStorage::backend()`. See the design note
  at the top of this file.
- **LocalFileSystem source backend** — handled via the existing
  `ObjectStoreBackend::artificially_sort_refs_in_mem()` flag, surfaced
  into `IngestOptions::source_listing_sorted` by the Storage-side
  entry point. Unsorted backends use the buffer+sort fallback; sorted
  backends use streaming + `list_with_offset`.
- **Byte-content verify on resume** — added as
  `verify_source_unchanged: bool = True` (default on).
- **Resume cursor scheme** — committed to lex-sorted batch boundaries
  with the cursor pointing at the last key copied. Documented in
  `plan-resumable-ingest-v2.md`.
