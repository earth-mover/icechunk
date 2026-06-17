# `from_zarr` ingest — implementation overview

A reading guide to the resumable zarr-v3 → icechunk ingest on `ian/ingest`.
Goal: let you understand the whole thing top-to-bottom and know where to look
hardest when reviewing.

## What it does

One call — `icechunk.from_zarr(source, sink)` — copies a zarr v3 store into an
icechunk repository, **byte-for-byte** and **resumably**. If the process dies
halfway, calling `from_zarr` again finishes from where it stopped. Source is any
`icechunk.Storage` (S3/GCS/Azure/HTTP/local); sink is a `Repository`.

## The one idea you need

Everything follows from a single design choice: **resume state lives in the
commit itself**, not in any sidecar file or external DB. Each commit on the
target branch carries `SnapshotProperties` (a string→JSON map). Ingest writes
three keys there:

| key | value |
|-----|-------|
| `icechunk.ingest.phase` | `"skeleton"` \| `"chunks"` \| `"complete"` |
| `icechunk.ingest.cursors` | `{ "/arr": "/arr/c/3/7" \| "DONE", ... }` |

To resume, a fresh process just reads the branch tip's properties. No locks, no
coordination. The commit log *is* the checkpoint.

Two phases, both committed:

1. **Skeleton** — copy every `zarr.json` (metadata) into one commit. In zarr v3
   a group is fully defined by its `zarr.json`, so after this commit every group
   is done and `Session::list_nodes` knows exactly which nodes are arrays.
2. **Chunks** — for each array in lex order, list chunk keys that sort *after*
   the array's cursor, copy a batch of `checkpoint_every` of them, commit, and
   record the new cursor. When an array is exhausted, write a `DONE` marker
   commit and move on. A final commit stamps `phase=complete`.

The cursor trick relies on **lexicographic ordering of chunk keys**. Sorted
object stores (S3 etc.) stream via `list_with_offset(cursor)` — O(batch) memory.
Unsorted backends (local FS) fall back to list-all-then-sort — O(chunks/array).

## Step by step: one ingest run

What actually happens, in order, when you call `from_zarr(source, sink)`.

### 1. Establish storage (source *and* sink are `icechunk.Storage`)

The source is described by the same `icechunk.Storage` type — and the same
constructors (`s3_storage`, `gcs_storage`, `local_filesystem_storage`, …) — you
use to open the destination repository. We chose this over taking a zarr/obstore
`Store` because one `Storage` already carries everything ingest needs:

- **credentials & auth** (anonymous, env, profiles, explicit keys, refreshable) —
  so there's a single auth story for source and sink, not two;
- **endpoint / region / retry config**, baked into the object store it builds;
- **the prefix**, which *is* the zarr root (there is no discovery step);
- **backend sortedness**, the one signal the resume algorithm needs (step 5).

The PyO3 binding downcasts the source `Storage` to the concrete `ObjectStorage`
(via the `Storage::as_any` escape hatch — kept a downcast so `icechunk-storage`
stays I/O-agnostic) and pulls out a configured `object_store` handle, the prefix,
and the sorted flag (`mod.rs:549`). Only `ObjectStorage` works as a source;
wrapper storages like `LatencyStorage` fail the downcast by design.

### 2. Read resume state from the branch tip

Before touching the source, read the target branch tip's `SnapshotProperties`
and decode `icechunk.ingest.phase` into Fresh / AfterSkeleton / InChunks(cursors)
/ Complete (`read_resume_phase`, `mod.rs:353`). A `Complete` tip short-circuits
with `AlreadyComplete` unless `CollisionPolicy::Overwrite` is set. This is why
resume needs no state file — the commit log is the checkpoint.

### 3. Skeleton step — find and copy every `zarr.json`

List the source under the prefix (scoped by `paths`) and keep the keys that end
in `zarr.json` — in zarr v3 that's *every* node, group or array. Copy those bytes
**verbatim** into a fresh writable session and commit with `phase=skeleton`. Two
things are true after this commit: every group is fully ingested (a v3 group is
nothing but its `zarr.json`), and icechunk's own node tree now knows which paths
are arrays. On a *resume*, this step instead **verifies** the source skeleton
still matches what was committed (same node set; same `zarr.json` bytes when
`verify_source_unchanged=True`) and errors with `SkeletonMismatch` if not.

> **Discovery cost (TODO).** Today this is a flat recursive `list()` that
> enumerates *every* object — including all chunk keys — and discards the
> non-`zarr.json` ones. That's an O(total-objects) scan up front. Because we
> support only v3, the fix is a hierarchy walk (`list_with_delimiter`) that reads
> each node's `node_type` and recurses into groups but stops at arrays — arrays
> are metadata leaves, so it never descends into a `c/` chunk subtree at all.
> Pruning by `node_type` rather than by the name `c/` stays correct even for a
> group legitimately named `c`. The verbatim byte copy is unchanged; we'd only
> peek at `node_type` to steer the walk.

### 4. Enumerate the arrays

Ask icechunk — not the source — for the arrays: `list_nodes` on the committed
skeleton, filter to `NodeType::Array`, sort lexicographically, dedup
(`enumerate_arrays`, `mod.rs:838`). The lex sort makes the per-array order
deterministic, which the cursor scheme in the next step relies on.

### 5. Copy chunks per array — lex-ordered, cursor-checkpointed

For each array in lex order (`copy_array_chunks`, `mod.rs:688`):

- Chunks are read under `<array>/c/` (the v3 default chunk-key layout).
- Keys are processed in **lexicographic order**, and the array's *cursor* is "the
  last chunk key copied." The next batch lists keys that sort **strictly after**
  the cursor — so a crash-and-resume continues exactly where it stopped without
  re-listing from the start.
  - **Sorted backends** (S3/GCS/…): stream `list_with_offset(cursor)` — peak
    memory is one batch.
  - **Unsorted backends** (local FS): list all of the array's keys, sort, then
    slice after the cursor — peak memory is O(chunks-in-that-array). This is the
    sole reason step 1 reads the sortedness flag.
- Copy each batch of at most `checkpoint_every` keys (**default 1000**)
  source→dest with bounded concurrency (`copy_keys`, **default 32-way**): each
  key is `GET` into memory then `SET` into the icechunk session.
- **Commit after every batch**, writing `phase=chunks` and the updated cursor
  into the commit properties. So one commit == one checkpoint == up to
  `checkpoint_every` chunks. Lower `checkpoint_every` ⇒ finer-grained resume but
  more commits; higher ⇒ fewer commits but more re-done work after a crash.
- When the array's keys are exhausted, write a distinct `DONE` marker commit so a
  resume sees the array is finished without re-listing it.

(Under `CollisionPolicy::Skip`, each batch is filtered against existing
destination keys first; the commit still lands — `allow_empty(true)` — so the
cursor advances even if every key in the batch was skipped.)

### 6. Finalize

After the last array, a final commit stamps `phase=complete`. From then on a
repeat call short-circuits (`AlreadyComplete`) unless you pass `Overwrite`. The
returned `IngestResult` carries the final snapshot id and the running counters.

## The diff in numbers — and why "1700 lines" is less than it looks

The headline `icechunk/src/ingest/mod.rs` is 1716 lines, but:

| region | lines | what |
|--------|------:|------|
| module doc + impl | ~965 | the actual logic (lines 1–965) |
| `#[cfg(test)]` tests | ~750 | 18 unit tests (lines 966–1716) |

So **the code to review is ~965 lines**, and ~44% of the file is tests you can
read as executable spec. The rest of the change:

- `icechunk-python/src/ingest.rs` (219) — PyO3 binding, almost all marshalling.
- `icechunk-python/python/icechunk/ingest.py` (161) — `from_zarr` wrapper +
  docstring + arg validation. ~40 lines are real code.
- One-line `as_any()` additions to each `Storage` wrapper (latency/logging/
  redirect/mod) + the trait method on `icechunk-storage` — plumbing for the
  source downcast.
- `_icechunk_python.pyi` stubs, `__init__` re-exports.

**Crucially additive.** It adds `pub mod ingest;` and a few escape-hatch methods;
it changes no existing icechunk read/write/commit logic. The blast radius on the
rest of the codebase is near zero — which makes it much safer to land than the
line count suggests.

## File-by-file tour

### `icechunk/src/ingest/mod.rs` — the core (read this first)

Flow, top to bottom:

- **Config/types** (lines 60–260): `IngestOptions` (private fields + builder),
  `CollisionPolicy` (Fail/Skip/Overwrite), `IngestStats`, `IngestError`.
- **Small pure helpers** (265–340): key classification, path normalization,
  `AtomicStats` counters. All unit-tested.
- **Resume decoding** (342–412): `ResumePhase` enum + `read_resume_phase` parse
  the tip's properties into Fresh / AfterSkeleton / InChunks(cursors) / Complete.
  `make_props` is the inverse (build properties for a commit).
- **`ingest_from_object_store`** (430–522): the driver. Reads resume phase →
  skeleton-or-verify → enumerate arrays → loop `copy_array_chunks` → final
  `complete` commit. This is the function to understand the control flow.
- **`ingest_zarr`** (549–561): the public Rust entry. Downcasts the source
  `ObjectStorage` to its raw `object_store` handle and calls the driver.
- **Phase implementations**: `write_skeleton` (568), `verify_skeleton_matches`
  (629, the resume-safety check), `copy_array_chunks` (688 — **the heart**;
  cursor logic, sorted/unsorted listing, per-batch Skip filter, commit), plus
  `commit_marker`, `enumerate_arrays`, `list_metadata_keys`, `copy_keys`.

### `icechunk-python/src/ingest.rs` — PyO3 binding

`PyIngestStats`/`PyIngestOutcome` classes, then `py_ingest_zarr` which unpacks
Python args, builds `IngestOptions`, downcasts the `PyStorage`, wraps the Python
`on_progress` callable as a Rust `ProgressCallback`, and calls `ingest_zarr`.
Thin; the only subtlety is the Storage downcast and the runtime bridge.

### `icechunk-python/python/icechunk/ingest.py` — public API

`from_zarr(...)` — validates `mode`/`source`/`sink` types, adapts the
`on_progress` callback shape, calls the binding, returns `IngestResult`. The bulk
is the docstring.

## Resume state machine (the part to scrutinize)

```
read tip properties
  ├─ Complete  ──(unless Overwrite)── error AlreadyComplete
  ├─ Fresh     ── write skeleton commit (phase=skeleton)
  └─ After/InChunks ── verify skeleton matches source, then continue
        │
   for each array (lex order):
        cursor==DONE? skip
        else: list keys after cursor → batch → copy → commit(cursor) → repeat
              → DONE marker commit
        │
   final commit (phase=complete)
```

Correctness leans on three invariants worth checking during review:

1. **Every commit is `allow_empty(true)`.** Cursor/phase-only progress commits
   (e.g. a `DONE` marker, or a Skip batch that filtered to zero writes) carry no
   data change but must still land to advance state. If this were ever dropped,
   resume would loop.
2. **Cursor is "last key copied", strictly-after semantics.** `copy_array_chunks`
   stores `/{last}` and the next run lists *after* it. Off-by-one here would
   either skip or re-copy a chunk (re-copy is safe; skip is data loss). The
   sorted path uses `list_with_offset`; the unsorted path uses
   `partition_point(|k| k <= cursor)`. Both are exercised by tests.
3. **Skeleton re-verification on resume.** `verify_skeleton_matches` guards
   against the source mutating between runs (`verify_source_unchanged=True` by
   default compares bytes, not just paths). This is what makes resume *safe*
   rather than just *fast*.

## Tests as spec (18 Rust + Python)

The `#[cfg(test)]` block reads as a behavior spec — start here to confirm intent:
`whole_store_copy_finishes_with_phase_complete`, `skeleton_then_resume_chunks`,
`multi_array_lex_order_progress`, `resume_after_partial_array`,
`resume_detects_source_metadata_mutation_by_default`, the three `CollisionPolicy`
tests, and `crash_then_resume_completes_byte_faithfully` (uses `FaultyStore` to
fail the Nth GET mid-copy). The Python side adds a real SIGKILL subprocess test
(`test_ingest_crash_subprocess.py`) plus smoke/hypothesis/resume/network suites.

## How hard is this to review?

**Moderate, and front-loaded.** The risk is concentrated in one ~150-line
function (`copy_array_chunks`) and the resume decoder — read those plus the state
machine above and you've seen 80% of the danger. Everything else is either
boilerplate (builder, PyO3 marshalling, passthrough wrappers) or test code. The
change touches no existing hot paths, so a reviewer doesn't need to re-reason
about icechunk's core. A focused reviewer can cover it in one sitting if they go
in this order: tests → `ingest_from_object_store` → `copy_array_chunks` →
`read_resume_phase`/`verify_skeleton_matches` → skim the rest.

## Known limitations (deferred, marked in code)

- **Skeleton discovery scans all objects**: step 3 lists the whole prefix
  recursively (chunks included) just to pick out `zarr.json` files — O(total
  objects). Fix: v3-only hierarchy walk that recurses into groups and stops at
  arrays, never descending into `c/` (prune by `node_type`, not by the name
  `c/`). See step 3.
- **Scale**: `enumerate_arrays` and the unsorted chunk-listing buffer hold their
  listings in memory — fine to ~10⁶ arrays / array-chunk-counts; streaming is the
  documented fix (`TODO(scale)`).
- **Large chunks**: `copy_keys` bounds in-flight *count* (32), not *bytes*; a
  store of 100 MB shards peaks ~3 GB resident (`TODO(perf)`).
- **Same-store fast path**: no server-side `ObjectStore::copy` / hardlink yet;
  every byte round-trips through the process (`TODO(perf)`).
- `mode="virtual"` / same-bucket copy reserved but unimplemented.

See `notes/ingest-roadmap.md` for the full deferred-work list and design
rationale (e.g. why the source API is `icechunk.Storage` + `as_any` downcast).
