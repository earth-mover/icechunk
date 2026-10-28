# Chunk object layout

## Problem and scope

Icechunk currently stores native chunks at:

```text
$ROOT/chunks/{chunk_id}
```

This creates one very large filesystem directory or object-store prefix, which
can perform poorly on some local filesystems and S3-compatible stores. The
physical path is also used by external readers, so changing it is a format and
interoperability change even if chunk IDs, manifests, and chunk bytes remain
unchanged.

The design adds one repository-level layout choice. Existing repositories
without layout metadata continue to use the flat path. Migration of existing
objects is a separate follow-up.

## Design

### Repository invariant

Each repository has one `chunk_layout`, stored as a dedicated typed field in
the repository info file at `$ROOT/repo`. It is selected during `create`,
persisted before the repository is usable, and is not exposed as an `open`
argument. A missing value in an older repository means `flat`.

The chunk ID remains the logical identity stored in manifests. The
specification defines one rule for converting a chunk ID into a path relative
to `$ROOT`; all clients must use that rule.

### Layouts

The existing layout remains:

```text
chunks/{chunk_id}
```

The proposed `ID prefix` layout follows Git's loose-object fanout, using the
first two ID characters as a directory:

```text
chunks/{first-two-ID-characters}/{remaining-ID}
```

For each supported layout, the specification must define the required path
mapping. Clients use the mapping selected by the repository; flat remains
valid.

The layout values are `flat` and `id_prefix`. Whether new repositories default
to `flat` or `id_prefix` remains a rollout decision. Either way, the selected
value is persisted explicitly.

An alternative is to store each chunk's root-relative physical path alongside
its logical ID in manifests, similar to Iceberg's recorded data-file
locations. This supports arbitrary layouts and per-object migration, but
increases manifest size, changes the chunk-reference format, and permits mixed
layouts. This design instead keeps one repository-level resolver.

### Metadata and lifecycle

The layout is repository-level format metadata, not only a client
configuration value, arbitrary user metadata, or a separate object. Keeping
it in `$ROOT/repo` uses the existing repository-opening path and conditional
update mechanism; no additional metadata lookup is required.

Repository-level format metadata is canonical for storage interpretation. The
selected layout must be discoverable from `$ROOT/repo` during normal repository
opening. Clients must not need recursive listing or candidate-path probing to
determine it. Metadata required to locate native chunks is required metadata,
not an arbitrary optional configuration field.

`create` is the only public operation that accepts `chunk_layout`. It writes
the selected value into the typed field in `$ROOT/repo` before the repository
is usable. `open` and `reopen` read that value and never accept, infer, or
override a layout. Changing it later is a migration operation, not an
ordinary configuration change. Reads, writes, listing, deletion, garbage
collection, storage statistics, and diagnostics all use the same resolver;
none may fall back to a different layout or create a mixed repository.

## Compatibility and external readers

The affected ecosystem includes VirtualiZarr, `zarrs_icechunk`,
`icechunk-js`, Zarrs.jl, GDAL, and catalog or data-pipeline projects built on
those readers. They must either implement the repository metadata and resolver
or explicitly support flat repositories only.

Readers that do not understand the selected layout must reject the repository
during `open`, before any native chunk lookup. Because an older reader would
assume the flat path, a non-flat layout requires an incompatible spec major
version that older readers reject. Clients must reject unsupported versions
rather than guessing. An unrecognized layout field that older clients silently
ignore is insufficient, because they could assume the flat path and report a
misleading missing-chunk error.

| Repository | New client | Older flat-only client |
| --- | --- | --- |
| Existing repository without layout metadata | Reads as `flat` | Continues to work |
| New repository using `flat` | Reads as `flat` | Continues to work |
| New repository using `id_prefix` | Reads as `id_prefix` | Rejects it using the incompatible spec version |

Release documentation should identify the compatibility status of each
external reader. Unsupported layouts and missing native objects must fail
clearly rather than being interpreted as sparse or fill-value chunks.

## Migration

Migration is not required for the initial layout feature. A future migration
must cover native chunks reachable from all snapshots retained by branches,
tags, or other retention rules—not only the current tip:

1. Acquire an exclusive maintenance or write lock.
2. Read the source layout; a missing value means `flat`.
3. Write and verify new objects at their target paths without changing IDs or
   bytes; do not rely on rename.
4. Conditionally update `$ROOT/repo` to record the target layout only after
   copying succeeds.
5. Retain source objects until rollback and recovery are no longer needed;
   delete them only after the switch is durable.

The migration design must also define resumability, rollback, unreferenced
chunks, failed copies, concurrent readers, and older clients.

## Testing plan

The initial implementation should include:

- exact path-mapping tests for every supported layout, plus resolver-based
  enumeration for listing and garbage collection;
- creation tests for each layout proving `chunk_layout` is persisted in
  `$ROOT/repo`, and open/reopen tests proving it is read and cannot be
  overridden;
- native-object operation tests proving reads, writes, listing, deletion,
  garbage collection, statistics, and diagnostics all use the selected
  resolver and cannot create a mixed layout;
- a checked-in older flat-repository fixture proving current readers can still
  read it;
- tests proving older flat-only readers remain compatible with flat
  repositories and reject `id_prefix` before native chunk access;
- regression checks that inline and virtual chunk references are unaffected;
- integration coverage for a local filesystem and an affected S3-compatible
  store.

Migration tests belong with the migration feature.

## Rollout

1. Define the on-disk format and compatibility contract, including the
   `chunk_layout` field in `$ROOT/repo`, the exact `flat` and `id_prefix`
   mappings, and incompatible spec-major handling.
2. Document the compatibility implications for external readers and
   integrations.
3. Implement create-time layout selection and a shared resolver for all native
   chunk operations.
4. Choose the default for new repositories after compatibility review.

The remaining design decision is the default for new repositories.
