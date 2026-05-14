# Plan: Re-architect `icechunk.from_zarr` as resumable per-batch ingest

> **For the next agent:** This is a fresh-context handoff. Read this whole
> document, then `notes/handoff-ian-ingest.md` for what's already on disk,
> then start at "Implementation order" near the bottom.
>
> Branch: `ian/ingest`. Nothing in this PR is committed yet — the V1
> ingest is uncommitted on disk and you'll be replacing parts of it.

## Context

The first iteration of `icechunk.from_zarr` (currently on branch
`ian/ingest`, all tests green) does a single mega-commit: list everything
→ copy everything → commit. That works for small-to-medium stores but is
non-resumable, burns memory on the full key list, and has no checkpoint
story for very large ingests.

Re-architecture goal: an MVP that is **resumable by design**. Skeleton
first, then per-array slab-by-slab copy with a commit between batches. The
cursor into each array's chunk space lives in `SnapshotProperties` of the
latest commit, so a fresh process picks up exactly where the previous one
stopped.

We are **replacing** the existing two-phase implementation entirely. Python
tests (`tests/test_ingest_smoke.py`, `tests/test_ingest_hypothesis.py`,
`tests/test_ingest_network.py`) **stay** — they test the public
`from_zarr(...)` API, which is unchanged. We may need to relax assertions
that check exact commit shape (e.g. tests that assume one-snapshot-per-call)
but the round-trip behaviour they assert remains correct.

Parallelism over arrays is **out of scope** for this MVP. Single array at a
time, sequential.

## Architecture

```
1. Open source as zarr Store + extract obstore-backed `Arc<dyn ObjectStore>`.
2. Walk source: list all `zarr.json` keys (group + array metadata).
3. Skeleton phase:
     - `Store::set` every `zarr.json` from step 2 into the writable session.
     - Commit with `commit_properties = {"icechunk.ingest.phase": "skeleton"}`.
4. Enumerate arrays: from the skeleton-committed session, walk
   `Session::list_nodes(/)` recursively, collecting node-paths whose
   `node_type == Array`.
5. For each array (sequentially, in lex order):
     a. Read latest commit's properties; extract this array's cursor (or `None`).
     b. Loop:
          - Open writable session.
          - `list_with_offset(prefix=<array_path>/, offset=cursor_or_array_root)`
            and skip the array's own `zarr.json` if listing returns it.
          - Take next `BATCH_KEYS` keys from the stream (default 1000).
          - `source.get` + `dest.set` each key.
          - Commit with `commit_properties` updated to record the new cursor:
              {
                "icechunk.ingest.cursors": {
                  "<array_path>": "<last_key_copied>",
                  ...
                },
                "icechunk.ingest.phase": "chunks",
              }
          - If the listing was exhausted before BATCH_KEYS, this array is
            done — record `<array_path>:DONE` and break.
6. Final close-out commit (only if needed): mark
   `icechunk.ingest.phase = "complete"`.
```

### Why this shape

- **Skeleton-first** lets us use `Session::list_nodes` to enumerate arrays
  authoritatively, instead of inferring array vs group by parsing
  `zarr.json` bytes ourselves.
- **Per-array sequential** is the simplest possible model. Adding
  parallelism later means scheduling concurrent sessions or using
  fork/merge — out of scope.
- **Lex-string cursor** generalizes over any chunk-key encoding (`/` or
  `.` separators, any number of axes). The cursor is just "the last raw
  key we copied"; resume = `list_with_offset(offset = cursor)`. No need
  to parse chunk indices into tuples.
- **Cursor in `commit_properties`** is the right place: it's atomic with
  the commit, survives process restarts, and
  `repo.lookup_snapshot(branch_tip)` fetches it in one round trip. No
  separate state file.

### Cursor format

Stored in `SnapshotProperties` (`BTreeMap<String, serde_json::Value>`):

```json
{
  "icechunk.ingest.phase": "chunks",
  "icechunk.ingest.cursors": {
    "/temperature": "/temperature/c/3/9",
    "/humidity":    "DONE",
    "/pressure":    null
  }
}
```

One entry per array, keyed by the array's NodePath (with leading `/`).
The value is the lex-string cursor or the sentinel `"DONE"`. Use `null`
or omit for "not started yet".

### Public API

`from_zarr(...)` signature is unchanged. The existing `paths`, `branch`,
`message`, `concurrency`, `on_progress`, `skip_existing`, `overwrite`,
`mode`, `checkpoint_every` kwargs all stay. New behaviour:

- `checkpoint_every: int | None`: now means "copy at most N chunk keys
  per commit". Previously raised `NotImplementedError`. Default `None` →
  `BATCH_KEYS = 1000`.
- `skip_existing` semantics still hold: pre-list dest, skip
  already-present keys. Now interacts cleanly with the cursor — if the
  cursor says `DONE` for an array, we skip listing entirely.
- `overwrite=True` interaction: still skips the pre-list collision
  check. Cursor still applies.

### Resume entry point

When `from_zarr` is called against a repo whose latest commit already
has `icechunk.ingest.phase` set, we **resume** instead of erroring on
collisions:

- `phase == "skeleton"`: skip step 3 (skeleton already committed).
  Verify the source's skeleton matches what's in the repo (same set of
  `zarr.json` paths). If they differ, error out — caller mixed sources.
- `phase == "chunks"`: skip steps 3+4, jump to step 5 with cursors from
  `commit_properties.cursors`.
- `phase == "complete"`: error or skip silently? Default: error — "this
  repo's latest commit is a completed ingest; pass `overwrite=True` to
  re-ingest."

## Files to change

### Replace entirely

- `icechunk/src/ingest/mod.rs` — current ~660 lines drops to ~400 of the
  new shape. Two-phase code (`copy_phase` etc.) goes; replaced by a
  skeleton walker + per-array slab loop.
- `icechunk-python/src/ingest.rs` — signature stays the same; internals
  thread `checkpoint_every`. The `py_ingest_zarr` pyfunction gains one
  int parameter.
- `icechunk-python/python/icechunk/ingest.py` — `checkpoint_every` no
  longer raises NotImplementedError; default `None` → 1000 server-side.

### Touch lightly

- `icechunk-python/python/icechunk/_icechunk_python.pyi` — update
  `py_ingest_zarr` stub for the new arg.
- `icechunk/src/ingest/BENCHMARKING.md` — note that the throughput
  numbers now come from a different shape (commit-per-batch), so they
  should be re-measured.

### Keep

- `icechunk-python/tests/test_ingest_smoke.py` — adjust if any test
  assumed a single commit per `from_zarr` call (one might — the
  `IngestResult.snapshot_id` is now the *final* commit after the
  per-batch series, not "the only" commit). Spot-check.
- `icechunk-python/tests/test_ingest_hypothesis.py` — pure round-trip
  assertions, unaffected.
- `icechunk-python/tests/test_ingest_network.py` — spot-check for the
  same one-commit assumption.

### Add

- `icechunk/src/ingest/mod.rs` mod tests:
  - `skeleton_then_resume_chunks` — manually do a skeleton commit, then
    resume from a fresh writable session with the chunk loop, verify
    the cursor advances correctly.
  - `resume_after_partial_array` — simulate crash mid-array (commit
    cursor `arr/c/0/3`), restart, verify only `arr/c/0/3+` are
    re-fetched.
  - `multi_array_lex_order_progress` — three arrays, the cursor map
    correctly tracks completion per-array.
  - `arbitrary_dimensionality` — an array with 4+ axes, confirm the
    lex-string cursor logic is unaffected by axis count.
- `icechunk-python/tests/test_ingest_resume.py` — Python end-to-end
  resume test: ingest, kill mid-way (mock the source's `get` to fail
  after N chunks), restart, verify completion. Use a fixture-stored
  sentinel object (e.g. tmp file the mock writes) to simulate "crashed
  here".

## Critical icechunk APIs (already verified)

- **Set commit properties**:
  `SessionCommitBuilder::properties(SnapshotProperties)` at
  `icechunk/src/session.rs:281`. `SnapshotProperties` is
  `BTreeMap<String, serde_json::Value>`
  (`icechunk-format/src/snapshot.rs:304`).
- **Read latest commit's properties**:
  ```rust
  let snap_id = repo.lookup_branch(branch).await?;
  let info = repo.lookup_snapshot(&snap_id).await?;
  let props = info.metadata; // BTreeMap<String, Value>
  ```
  `Repository::lookup_branch` at `icechunk/src/repository.rs:1258`,
  `lookup_snapshot` at `:1283`. Direct fetch — no ancestry walk.
- **Enumerate nodes**: `Session::list_nodes(&Path)` at
  `icechunk/src/session.rs:1327` returns a sync iterator of
  `SessionResult<NodeSnapshot>`. **Open question**: is it recursive?
  Earlier code in `store.rs:776` filters its output by prefix-match,
  suggesting it may already return descendants. **Action item for the
  implementing agent**: confirm by reading the function body; if it's
  direct-children only, write a 5-line recursive helper.
- **Filter for arrays**: `node.node_type() == NodeType::Array`
  (`icechunk-format/src/lib.rs:118`). Path on `node.path`. Raw
  `zarr.json` bytes in `node.user_data`.

## Source-side: `list_with_offset`

- Trait method on `object_store::ObjectStore`. Already in scope via our
  `use icechunk_arrow_object_store::object_store::ObjectStore;`.
- Signature:
  `fn list_with_offset(&self, prefix: Option<&Path>, offset: &Path) -> BoxStream<...>`.
- Offset is **exclusive** (lex order). Prefix is path-style — splits at
  `/`.
- **Important**: for chunk keys with `.` separator (`c/0.0.0`), prefix
  cannot slice axes. Use `prefix=<array_path>/c/` and let the cursor
  walk lex order. For `/` separator (the v3 default and the common
  case), same approach also works — we just don't get the per-axis
  prefix advantage. The MVP doesn't try to slice by axis; it just walks
  lex order with `list_with_offset`.

See `notes/ingest-research/list_with_offset_demo.py` for verified
behaviour, including the path-vs-string-prefix gotcha.

## Implementation order

1. Read `icechunk/src/ingest/mod.rs` carefully — understand current
   structure before deleting.
2. Confirm `Session::list_nodes` recursion semantics; write a recursive
   helper if needed.
3. Write the skeleton walk: list all `zarr.json` keys from source, set
   each one, commit with `phase = "skeleton"`. Add a Rust unit test.
4. Write the per-array chunk loop: read cursor, list_with_offset,
   batch-set, commit with updated cursor. Add unit tests.
5. Wire up the resume entry point: read latest commit's properties,
   decide skeleton vs chunks vs complete entry.
6. Update PyO3 binding for `checkpoint_every` parameter.
7. Update Python wrapper to pass it through (drop the
   NotImplementedError).
8. Update `.pyi` stub.
9. Run the full test suite. Fix any test that assumed
   single-commit-per-call.
10. Add `tests/test_ingest_resume.py` for end-to-end resume.

## Verification

```bash
# Rust unit tests (existing 10 + ~4 new = ~14)
cargo test -p icechunk --lib ingest:: --no-fail-fast > /tmp/ingest_rust.log 2>&1
tail -25 /tmp/ingest_rust.log

# Build
cd icechunk-python && uv run maturin develop --uv

# Python smoke + hypothesis (8 tests, possibly 9 with new resume test)
uv run pytest tests/test_ingest_smoke.py tests/test_ingest_hypothesis.py tests/test_ingest_resume.py -o addopts= -v

# Network test still passes against IDR (resumable shape; verify the
# IDR ingest now produces ~N+1 commits where N = number of chunk batches)
uv run pytest tests/test_ingest_network.py -m network -o addopts= -v
```

End-to-end resume verification idea:

```python
# Pseudocode for tests/test_ingest_resume.py
def test_resume_picks_up_after_simulated_crash(tmp_path_factory):
    # 1. Build a source with ~50 chunks across 2 arrays.
    # 2. Run from_zarr with checkpoint_every=10 against a wrapper source
    #    that raises after 25 successful gets.
    # 3. Confirm the partial commit's properties have a cursor mid-way.
    # 4. Run from_zarr again against the same source (no failing wrapper).
    # 5. Confirm: total bytes copied across both calls equals source bytes;
    #    final commit has phase="complete"; round-trip read matches source.
```

## Out of scope (deferred)

- **Parallelism across arrays.** MVP is sequential per array.
- **Skeleton incremental commits.** Skeleton is one commit; if skeleton
  write itself fails, retry from start (acceptable — skeletons are
  small).
- **Streaming `Store::set`.** Same gap as before — flagged in
  BENCHMARKING.md.
- **Subset-copy via `paths`** — keep the existing
  logical-path-with-ancestor behaviour, but exercise it through the new
  resumable shape. The cursor map only includes the arrays that fall
  within the requested subset.

## Open questions for the implementing agent

1. **`Session::list_nodes` recursion.** Read the body. If non-recursive,
   write the helper. Don't waste time guessing.
2. **What if two `from_zarr` calls run concurrently against the same
   branch?** Two sessions writing to `main` would conflict at commit
   time anyway. Out of scope — document that `from_zarr` is
   single-writer.
3. **Cursor-propagation for arrays NOT in the current call's `paths`.**
   If the user resumes with a smaller `paths` than the original call,
   what happens to old cursors for arrays no longer in scope?
   Recommend: carry them forward unchanged (preserve the cursor map
   across commits). Out-of-scope arrays are simply not advanced.

## References (for the next agent's first reads)

- Existing V1 code lives at `icechunk/src/ingest/mod.rs`,
  `icechunk-python/src/ingest.rs`,
  `icechunk-python/python/icechunk/ingest.py`. Review before deleting.
- V1 handoff: `notes/handoff-ian-ingest.md` (covers the V1 commit-split
  plan; superseded by this doc but useful for understanding what's on
  disk).
- Benchmarks + dataset list: `icechunk/src/ingest/BENCHMARKING.md`.
- `list_with_offset` behavioural demo:
  `notes/ingest-research/list_with_offset_demo.py`.
- Project memory: `~/.claude/projects/-Users-ian-Documents-dev-icechunk/memory/MEMORY.md`.
  Read it. Critical entries: `feedback_pipe_output.md`,
  `feedback_uv_python.md`, `feedback_subagents.md`, `feedback_no_cosign.md`,
  `feedback_maturin_uv.md`, `project_pyo3_object_store_version.md`.
