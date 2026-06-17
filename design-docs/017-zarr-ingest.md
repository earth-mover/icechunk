<!-- markdownlint-disable MD013 -->

# Zarr ingest

<!-- One-paragraph summary: what `from_zarr` does and why this doc exists. -->

## Motivation

There is a lot of zarr data out in the world. Many of the users who manage or read that data would like to benefit from the additional features and safety that Icechunk provides. Currently they have two ways to do this:

1. Use virtual chunks via `virtualizarr`
2. Write a script that copies the data.

However, both of these options have drawbacks. Virtual chunks have some fundamental instability because the source can be changed. So some users will never feel comfortable with them. Writing a script to do this is possibly error prone and would require the user to fully decode and encode every chunk.

Since Zarr is a special case of data in its relationship to Icechunk we have the ability to have a built in high performance zarr->icechunk native chunk ingest. With access to Icechunk internals this can live fully in rust, and crucially can avoid decoding and re-encoding the data.
There is a lot of zarr data out there. It should be as easy as possible as a user to get it into Icechunk.

## Goals

### Features

- Single Icechunk function to ingest Zarr v3 into Icechunk
- Ingest is resumable
- Additive (If the original zarr store grows then a re-ingest will import the new parts)
<!-- tom's thought about a native/virtual toggle-->

### Performance

- Scales to arbitrary sized zarr store (handles isssues raised in <https://github.com/zarr-developers/VirtualiZarr/issues/894>)
- Operate at bytes level - do not require decode/encode of chunk data
- Allow for optimization using storage level copy if the source and destination are on the same physical storage

## Non-goals

- Ingest of non zarr v3 data

## Design

Many of the technical issues in this design are not new, they have been considered by VirtualiZarr in  <https://github.com/zarr-developers/VirtualiZarr/issues/894>.

### Storage Permissions

- extact obstore? link issue by kyle barron
- icechunk.Storage

### Ingest

#### Stages

Skeleton
Chunks

#### Resumability

The skeleton phase of ingest will not be resumable. It is light enough

The heavy work phase is copying the chunk data. It must be resumable. The resume state will live on the commit

From a user perspective the cost of a long running ingest failing is the time cost. So a natural checkpointing trigger would be time based. I.e. checkpoint every 2 minutes.

Checkpointing triggers:

1. Time based (e.g. every 2 minutess)
2. Key based (e.g every 1000 keys)

The state required for resuming storing this will live in the Icechunk
Resume state will live in commit
<!-- Core idea to expand: resume state lives in the commit itself, not a sidecar.
     Each commit carries SnapshotProperties (string -> JSON); ingest writes its
     progress there, so resume just reads the branch tip. The commit log is the checkpoint.
     Why: properties commit atomically with the data; a sidecar needs its own
     atomicity/cleanup and can drift. -->

<!-- Two committed phases, then finalize:
     1. Skeleton: copy every zarr.json verbatim into one commit, stamp phase=skeleton.
        Why: a v3 group IS its zarr.json, so after this commit every group is done
        and the node tree knows which paths are arrays (enumerate via list_nodes,
        not by re-parsing source bytes).
     2. Chunks: per array, lex order. List keys after its cursor, copy a batch of
        checkpoint_ qqevery keys (default 1000, 32-way concurrent), commit with new cursor,
        write a DONE marker when exhausted.
        Why per-array sequential: simplest correct model; parallelism deferred.
        Why commit-per-batch: one commit is one checkpoint. Smaller batch, finer resume,
        more commits.
     3. Finalize: a commit stamps phase=complete; repeat calls short-circuit unless Overwrite.
     Copy is byte-for-byte (GET then SET); no decode/encode. -->

### Resume state

<!-- Two keys on every ingest commit (write up as prose around this table). -->

| key | value |
|-----|-------|
| `icechunk.ingest.phase` | `"skeleton"` \| `"chunks"` \| `"complete"` |
| `icechunk.ingest.cursors` | `{ "/arr": "/arr/c/3/7" \| "DONE", ... }` |

<!-- To expand:
     On entry, decode the tip into Fresh / AfterSkeleton / InChunks(cursors) / Complete.
     Complete errors (AlreadyComplete) unless Overwrite. On resume the skeleton step
     re-verifies the source still matches (same nodes; same bytes when
     verify_source_unchanged=True).
     Why verify: makes resume safe, not just fast (catches a source mutated between runs).
     Invariant: every commit is allow_empty(true), so DONE markers and empty Skip batches
     still advance state, otherwise resume loops. -->

### Cursor format

<!-- To expand:
     Cursor = the last chunk key copied, stored as a lex path ("/arr/c/3/7"); the next
     batch lists keys strictly after it.
     Why a raw lex string (not parsed indices): generalizes over any chunk-key encoding
     and axis count; resume is just "list after this string."
     Listing splits on backend sortedness (the one source signal needed):
       - Sorted (S3/GCS): stream list_with_offset(cursor), one batch in memory.
       - Unsorted (local FS): list, sort, slice after the cursor, O(chunks in that array).
     Why last-key-copied / strictly-after: re-copying is safe, skipping is data loss;
     the off-by-one is the sharpest edge, tested on both paths. -->

## Public API

<!-- A single function: icechunk.from_zarr(source, sink, ...). Expand each param:
     - source: Storage. The same icechunk.Storage type (s3_storage, local_filesystem_storage)
       used to open a repo.
       Why Storage, not a zarr/obstore Store: it already carries auth, endpoint/retry, the
       prefix (which IS the zarr root, no discovery), and the sortedness flag. One auth story.
     - sink: Repository. Target repo.
     - paths: logical paths to ingest (default: whole store).
     - branch, message, concurrency (32), on_progress (fires per commit).
     - on_collision: CollisionPolicy. Fail (default), Skip (skip existing, layering/resume),
       Overwrite (clobber; required over a complete tip).
     - checkpoint_every: int | None. Max keys per commit (default 1000).
     - verify_source_unchanged: bool = True. Byte-compare skeleton on resume.
     - mode: Literal["copy"] = "copy". Byte-faithful copy only.
       Why a single-valued param: reserves space for "virtual" / "same-bucket-copy" without
       a later signature break. -->

## Alternatives considered

<!-- To expand:
     - Sidecar state file: rejected; properties-in-commit can't drift, no separate cleanup.
     - Source as a zarr/obstore Store: rejected for icechunk.Storage (single auth/config;
       prefix is the root).
     - ObjectStoreBackend in icechunk-storage (typed dispatch): rejected for a runtime
       Storage::as_any downcast. Why: icechunk-storage is deliberately I/O-agnostic (no
       object_store dep); a one-method escape hatch beats coupling the trait layer to one
       I/O ecosystem. Revisit if Storage later couples to an I/O substrate. -->

## Open questions / deferred work

<!-- To expand:
     - Skeleton discovery scans all objects: flat recursive list() to pick out zarr.json.
       Fix: v3 hierarchy walk that stops at arrays, pruning by node_type.
     - Streaming enumeration: array list and unsorted chunk list are buffered (limit ~1e6
       arrays); stream discover-as-you-go.
     - Byte-budgeted concurrency: bounds in-flight count, not bytes (100 MB shards x 32 is
       ~3 GB resident).
     - Same-store fast path: same physical store means skip GET+SET via server-side copy /
       hardlink / virtual ref. Overlaps mode="virtual".
     - Destination path remap: no dest_prefix; cursors reference dest keys, so it needs care.
     - Progress-callback cancellation: planned CancellationToken, keeps callback shape stable. -->

<!-- markdownlint-disable MD013 -->
<!-- markdownlint-disable MD012 -->



## Optimizations

When the zarr source and the icechunk repository live on the same physical storage such as the same cloud bucket, or locally on a computer or HPC there is the opportunity to skip the transit of the data through Icechunk at all. Instead we can use the `copy/move` primitives provided by the storage in order to move the data chunk locations to under Icechunk. This would work by determining the correct ID for the chunk, then moving the chunk to that name. This skips the actual set in memory in Icechunk but still constructs a valid store.
