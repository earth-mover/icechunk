# `shift_array` / `reindex_array` on rectilinear chunk grids: reject for now

Issue: [#2151](https://github.com/earth-mover/icechunk/issues/2151)

This doc records the decision to reject these operations on non-regular chunk
grids as an immediate stopgap for a data-corruption bug, and sketches the
follow-up fix. The full design for the real fix — including a general API for
rewriting the chunk grid during `reindex_array`, the gap-ambiguity problem, and
the settled/open edge analysis for multidimensional grids — is in
[PR #2179](https://github.com/earth-mover/icechunk/pull/2179)
(`017-shift-array-rectilinear.md`); read that before starting the deeper fix.

`shift_array` on an array with a rectilinear chunk grid commits successfully
and produces an array that can no longer be read back:

```python
arr = root.create_array(
    name="a", shape=(3,), dtype="int32",
    chunks=RectilinearChunkGrid(chunk_shapes=((1, 2),)),
)
arr[:] = [10, 20, 30]
session.commit("write")

session = repo.writable_session("main")
session.shift_array("/a", [1])
session.commit("shift")  # no error

arr = zarr.open_array(session.store, path="a")
arr[:]  # ValueError: cannot reshape array of size 1 into shape (2,)
```

## Root cause

Icechunk's snapshot format does not know where rectilinear chunk boundaries
are. `ArrayShape` stores only `(dim_length, num_chunks)` per dimension; the
actual `chunk_shapes` list lives only inside the zarr.json bytes, which the
session carries as opaque `user_data`. `shift_array` is built on
`reindex_array`, which relabels chunk *indices* in the manifest without ever
looking at chunk *sizes*.

This is safe for regular grids because Zarr v3 stores every chunk — including
edge chunks — encoded at the full uniform chunk shape, so any payload fits any
slot. On a rectilinear grid, slot `i` expects exactly `chunk_shapes[i]`
elements. Shifting the `(1, 2)` grid above by +1 puts the 1-element payload in
the slot zarr decodes as 2 elements. Nothing on the write path validates
payload size against slot size, so the corruption is only discovered at read
time. The damage is confined to the new commit; earlier snapshots remain
readable via time travel.

Note that `reindex_array` has the same bug and is public API: it is exposed to
Python with arbitrary user-supplied `forward`/`backward` index mappings, so
any reindex of a rectilinear array can corrupt it the same way.

## Options considered

### Option A: reject the operation (implemented first)

Add a guard at the top of `reindex_array` (which `shift_array` goes through)
that inspects the node's `user_data`: if it parses as zarr array metadata and
the chunk grid is not `regular`, fail with a clear error before any chunk refs
are touched.

* The check is fail-open: `user_data` that doesn't parse as zarr metadata
  (e.g. arrays created through the Rust API with non-zarr metadata) is not
  rejected. Icechunk core otherwise treats `user_data` as opaque; the guard
  only interprets it when it can.
* No format change, no read-path change. The only behavior change is that an
  operation which previously corrupted data silently now errors.
* Cheap, low risk, and reversible: the error can later be replaced by the
  real fix (Option B) without leaving anything behind — the guard must remain
  on `reindex_array` permanently regardless (see below).

### Option B: rewrite the chunk grid (planned follow-up for `shift_array`)

Shifting a rectilinear array by `+k` chunks along a dimension is exactly a
**right-rotation of `chunk_shapes` by `k`** along that dimension:

* Slot `i+k` in the rotated grid has size `c_i`, so every surviving payload
  lands in a slot of its own size. No chunk data is decoded or rewritten —
  the fix is metadata-only.
* Every chunk moves by the same element offset: the total size of the `k`
  chunks that fall off the end (for negative shifts, off the start). For the
  example above, `(1, 2)` shifted by +1 becomes `(2, 1)` and the surviving
  data lands at element offset 2.
* `num_chunks` is invariant under rotation, so `ArrayShape`, manifest extents
  and the on-disk format are untouched. Snapshots remain forward and backward
  compatible.
* Composes with the documented resize-then-shift idiom for prepending: append
  chunk shapes to the grid via resize, then shift; the rotation moves the new
  slots to the front.

Work and risks that make this a separate change rather than the first fix:

* Icechunk core would author zarr metadata for the first time (today
  `user_data` is only ever written by the zarr store layer). Rewriting means
  parsing the full zarr.json, RLE-decoding `chunk_shapes` (both `[1, 2, 2]`
  and `[[2, 3]]` run-length forms), rotating, re-encoding (re-RLE to avoid
  blowing up `[[1, 1000000]]`-style grids), and updating the node via
  `update_array`.
* A shift becomes a metadata+chunks change, so it starts conflicting with
  concurrent metadata updates where before it only touched chunk refs.
  Arguably correct, but a behavior change that needs deliberate tests.
* The stateful-test model store must mirror the grid rotation.
* "Shift by k chunks" on a rectilinear grid means a data-dependent element
  offset; needs explicit documentation.

The rotation trick only covers `shift_array`. Arbitrary `reindex_array`
mappings admit no unique set of rewritten chunk boundaries (vacated slots can
be split in multiple valid ways — see the gap-ambiguity discussion in
[PR #2179](https://github.com/earth-mover/icechunk/pull/2179)), so a general
fix needs the user to supply or constrain the new grid. Until such an API
exists, `reindex_array` keeps Option A's rejection.

### Rejected: decode and re-slice chunk data

Issue option 3. Turns a metadata-only operation into reading, decoding,
re-chunking and re-writing potentially the entire array. Cost defeats the
purpose of `shift_array`, and it silently changes the operation's performance
class depending on the chunk grid.

## Decision

Ship Option A now (this change). Implement the real fix as a follow-up,
starting from the design in
[PR #2179](https://github.com/earth-mover/icechunk/pull/2179), keeping the
`reindex_array` guard until a grid-rewrite API covers it.
