<!-- markdownlint-disable MD013 -->
<!-- markdownlint-disable MD012 -->
# `shift_array` and `reindex_array` with rectilinear chunks

## Motivation

Zarr-python added support for rectilinear chunk grids behind a flag in version 3.2.0. These follow the [rectilinear zarr-extension](https://github.com/zarr-developers/zarr-extensions/tree/main/chunk-grids/rectilinear), not [ZEP 3](https://zarr.dev/zeps/draft/ZEP0003.html). The plan is to move this support from experimental to stable in the next minor version (<https://github.com/zarr-developers/zarr-python/issues/4025>).

These grids allow for variable chunk sizes along a given dimension. So you can have a 7-element 1-D array with chunk sizes `[1, 1, 2, 3]`.

Icechunk is agnostic to chunk size information except for the `shift_array` and `reindex_array` functions which modify the ordering of chunks. If you apply a shift to a rectilinear chunk array today then you can commit it without issue. But when anyone tries to actually read the chunk data they will get an error `cannot reshape array of size 1 into shape (2,)` because we change the mapping to array index to chunk, but do not update the chunk grid that zarr uses to understand what size chunk it should be looking for.

See <https://github.com/earth-mover/icechunk/issues/2151>


The core of the bug is that the declared chunk sizes in the zarr metadata end up out of sync with the on-disk data sizes. So to fix this we need to either refuse to do chunk reindexing on rectilinear arrays, or update the chunk grid as part of the operation.

There are two options.

1. Disallow `shfit_array` and `reindex_array` on rectilinear chunk grids.
2. Design an API to let the user indicate how to re-write the chunk grid.


`shift_array` is a useful feature, as are rectilinear chunks. They also play well in combination as an exemplar use case of rectilinear chunks is months of the year havign different shapes. So for something like weather predictions over a year we might expect rectilinear chunks. Predictions rolling over a year is a also a good use case of `shift_array`. Therefore simply disallowing `shift_array` on rectilinear chunks is not a good solution. We need to decide on the strategy how to correctly and consistently fill in the chunk sizes of the vacated slots.



### Handling Vacuumed chunks


`reindex_array` (and consequently `shift_array` which uses `reindex_array`) moves chunks between grid positions. After a `reindex` there every array index will be associated one of the two kinds of post reindex chunk types `vacuumed` and `filled`.

#### Gap ambiguity

Before determining the API we give to the user we need to determine how to handle disjoint gaps introduced by a reindex operation.

Take a 6-element array, chunks `[1, 2, 3]` holding data `A`, `B`, `C`:

```text
original:  [A][B B][C C C]      grid [1, 2, 3]
elements:   0  1 2  3 4 5
```

If the reindex send A and B out of the array and move C to chunk position 1 then there is not really an inherent size to the chunks in the 1st and 3rd positions anymore. So both of these might be valid.

Keep only `C` and place it at grid index 1, vacating indices 0 and 2 (`forward: 2 → 1`). `C` is 3 elements, so the new grid is `[g0, 3, g2]` with `g0 + g2 = 3`. Two results are both valid:

```text
g0=1, g2=2  ->  grid [1, 3, 2]
   [.][C C C][. .]
    0  1 2 3  4 5

g0=2, g2=1  ->  grid [2, 3, 1]
   [. .][C C C][.]
    0 1  2 3 4  5
```

In both, `C` is the same 3 elements of data, just at a different offset. The two gaps always sum to 3, but the split between them is free.

Two options:

1. We decide the element size of each gap, then ask the user only to chunk within each gap.
2. We ask the user to decide both, by returning a complete chunk grid.

Option 1 keeps the user's job small, but in the ambiguous case above (a survivor between two gaps) we have to pick the split, and the user cannot change it. Option 2 lets the user place everything, so the ambiguity goes away; the cost is they must return a complete grid that we then check tiles each axis and is consistent.

`shift_array` does not hit this either way, since its gap is always one block at an edge, so it keeps the simpler per-axis fill.




#### Multidimensional Arrays

In 2+ dimensions there are more constraints on chunk size than 1d. This is because in a rectilinear grid a chunk's edge length varies only **coaxially**: the edge length changes along the axis it measures and stays constant along every other axis. A chunk at `(i, j)` therefore always
has shape `(edge_0[i], edge_1[j])`, and the spec stores one edge-length vector per axis.

So this chunk grid is **not** legal

```text
          ┌──┬─────┐
   r0     │AA│ BBB │     columns [1, 2]
          ├──┼──┬──┤
   r1     │CC│DD│EE│     columns [1, 1, 1]
          └──┴──┴──┘
```


A consequence of this is that vacuumed chunks can have two types of edge length:

- `settled` - the edge length is determined by another chunk
- `open` - the edge length is undetermined

`open` edge lengths are only possible when the entire slice has been vacuumed. So in the diagram below the height of the chunks in the second row is fully determined by the filled chunk at `(1,1)`.

```text
           1  2   ?   ?      a ? marks a free edge length: this axis is undetermined
          ┌─┬──┬────┬────┐
   1      │■│□□│◌◌◌◌│◌◌◌◌│
          ├─┼──┼────┼────┤
          │□│■■│◌◌◌◌│◌◌◌◌│
   2      │□│■■│◌◌◌◌│◌◌◌◌│
          └─┴──┴────┴────┘

   ■  filled     a chunk with data; sets its row and column edge lengths
   □  vacuumed   a chunk whose edge lengths are all settled, so its shape is fixed
   ◌  open       an open edge length; the user chunks this run
```

So the user input here is simplified they only need to provide the chunk width for the 3rd and 4th columns.
This means that it is possilbe that even in a fairly aggresive vacuuming no user input is necessarily required to fully determine the chunk grid.


```text
            1  2  3        (6 elements wide)
          ┌─┬──┬───┐
   1      │□│□□│■■■│
          ├─┼──┼───┤
          │■│□□│□□□│
   2      │■│□□│□□□│
          ├─┼──┼───┤
          │□│■■│□□□│
   3      │□│■■│□□□│
          │□│■■│□□□│
          └─┴──┴───┘
       (6 elements tall)

   ■  filled     a chunk with data; sets its row and column edge lengths
   □  vacuumed   a chunk whose edge lengths are all settled, so its shape is fixed
```



## Chunk grid type

The spec stores the grid as `chunk_shapes`: one entry per axis, where an axis is a bare int (regular),
an explicit list, or run-length encoded.

```python
# One chunk's size along an axis (the spec's term).
EdgeLength = int

# Run-length form: (length, count) means `count` consecutive chunks of that edge length.
Run = tuple[EdgeLength, int]

# One axis. A bare int is a regular axis (uniform edge length, count from the axis length);
# a list is a mix of explicit edge lengths and runs.
AxisGrid = int | list[EdgeLength | Run]

# The whole grid, one entry per axis. This is exactly the spec's `chunk_shapes`.
ChunkGrid = list[AxisGrid]
```

For example `[4, [1, 2, 3], [[1, 3], 3]]` is a 3-D grid:

```python
4            # axis 0: regular, every chunk size 4
[1, 2, 3]    # axis 1: explicit       -> [1, 2, 3]
[[1, 3], 3]  # axis 2: run-length      -> [1, 1, 1, 3]
```

The fills never see `ChunkGrid`. We already have to normalize it to explicit edge lengths to validate
the grid and find the free slots, so we hand the fill that same `FlatGrid` at no extra cost. Passing
the raw form instead would only make every fill re-expand the runs itself. On write we emit explicit
lists; compacting back to regular or RLE is an optional optimization, not required for correctness.

```python
FlatGrid = list[list[EdgeLength]]   # per axis, explicit edge lengths
```

## `shift_array`

`shift_array` uses reindex internally but is more constrained to only allow shifts along an axis. This reduces the problem space to a simpler 1D problem. For multi d arrays with multiple shifts the 1d solution can just be applied independtly across the axes.


There are only two types of chunks. A **filled** position receives a chunk, its size follows that chunk, whether it carried data or was an empty chunk. Empty chunks still have a declared size so that information propagates. A **vacuumed** position has no chunk mapping to it, so its size may be partially undefined and must be chosen.

A diagram of what happens for shifts of magnitude 1 and 2 is shown is below.

Grid `[1, 1, 2, 3]` (7 array elements) with index 1 an empty chunk (`∅`), shifted right by 1 and by 2 chunks:

```text
before      pos:   0     1     2       3
            data: [A]   [∅]   [C C]   [D D D]
            size:  1     1     2       3

Shift +1    pos:   0     1     2       3
            data: [.]   [A]   [∅]     [C C]
            size:  ?     1     1       2
                   └┬┘   └──────┬──────┘
               vacuumed      filled

Shift +2    pos:   0     1     2     3
            data: [.]   [.]   [A]   [∅]
            size:  ?     ?     1     1
                   └──┬──┘     └──┬──┘
                  vacuumed      filled
```

The adjacent vacuumed space can be coalesced and then filled in in a few ways. For the above shift of +2 chunks the options are

1. Periodic boundary - `[2, 3, 1, 1]`
2. Arbitrary rechunks into multiple empty chunks. e.g. `[5, 1, 1]` or `[1,1,1,1,1,1,1]` or `[3, 2, 1, 1,]` or `[1,4,1,1]` etc.
3. Rechunk to include the empty part in the next filled chunk. So the first chunk would become (1+3+2) `[6, 1]`

Option 3 has the potential to be computationally intensive as it invovles rechunking a chunk with data. So option 3 can be rejected. In contrast both the periodic boundary and arbitraily rechunking are valid use cases. So we need to provide a way for the user express what they want, while also continuing to make shift_array convenient to use for simple cases such as periodic boundary or equally divided into chunks.


Building on the `reindex_array` API we can have the user provide a function to handle the per axis remapping of chunks. As a convenience we will provide pre-made functions for `periodic` (the default) and dividing into an aribtary set of subchunks.


```python
def shift_array(
    self,
    array_path: str,
    chunk_offset: Iterable[int],
    *,
    # called once with EVERY axis's rolled-off sizes (len == ndim, [] for unshifted axes);
    # returns each axis's vacuumed-slab sizes (same per-axis sum; [] stays []).
    fill: Callable[[list[list[int]]], list[list[int]]] = periodic,
) -> None: ...
```

Convenience strategies — `from icechunk import periodic, single_chunk, fixed_chunks` (new `icechunk/regrid.py`).

The input is a `list[list[int]]`, one entry per axis with only .

1-D, shift +2 on grid `[1, 1, 2, 3]` — one axis, still wrapped in a list:


```python
def fill(original_grid, shifts) -> new_grid:
```

```python
fill([[2, 3]])
#   periodic([[2, 3]])     -> [[2, 3]]   grid [2, 3, 1, 1]
#   single_chunk([[2, 3]]) -> [[5]]      grid [5, 1, 1]
```

2-D array with rows `[1, 1]` and cols `[1, 1, 2, 3]`, shift `(0, 2)`:

```python
fill([[], [2, 3]])
#      ^^ axis 0: shift 0  -> []
#          ^^^^^^ axis 1: shift +2 -> last 2 col sizes [2, 3]
```


`periodic` wraps the rolled-off sizes into the vacuumed slab — sizes move around the boundary, data does not:

```text
before  size:   1   1  [2   3]      [2, 3] roll off the right edge
after   size:  [2   3]  1   1       and become the new (vacuumed) left sizes
                └─┬─┘   └─┬─┘
              vacuumed   filled
```

Full implementation. Each fill maps the per-axis list-of-lists to a per-axis list-of-lists; `periodic` is just the identity:

```python
def periodic(rolled_off):      return rolled_off
def single_chunk(rolled_off):  return [[sum(a)] for a in rolled_off]   # [] -> []

def fixed_chunks(n):
    def fill(rolled_off):
        out = []
        for a in rolled_off:
            total = sum(a)
            if total % n:
                raise ValueError(f"slab of {total} not divisible by {n}")
            out.append([n] * (total // n))    # [] -> []
        return out
    return fill
```

`shift_array` rolls every axis, calls the fill once, splices each slab back in, then moves the data via `reindex_array`:

```python
def shift_array(self, array_path, chunk_offset, *, fill=periodic):
    grid   = self.chunk_sizes(array_path)               # list[list[int]], per axis
    rolled = [roll(s, o) for s, o in zip(grid, chunk_offset)]
    slabs  = fill(rolled)                                # one call, all axes
    new_grid = [splice(s, o, slab)
                for s, o, slab in zip(grid, chunk_offset, slabs)]
    self.reindex_array(array_path, forward=shift_map(chunk_offset))
    self.set_chunk_grid(array_path, new_grid)

def roll(sizes, offset):                  # sizes falling off this axis's far edge
    k = min(abs(offset), len(sizes))
    if k == 0: return []
    return sizes[-k:] if offset > 0 else sizes[:k]

def splice(sizes, offset, slab):          # survivors shifted, slab fills the vacuum
    k = min(abs(offset), len(sizes))
    if k == 0: return sizes
    assert sum(slab) == sum(roll(sizes, offset))
    return slab + sizes[:len(sizes) - k] if offset > 0 else sizes[k:] + slab
```

Because the fill sees all axes at once, a custom one can vary by axis (the list index is the axis):

```python
# periodic on axis 0, single chunk on axis 1; shift (+1, +2)
fill = lambda rolled: [rolled[0], [sum(rolled[1])]]
# fill([[3], [2, 3]]) -> [[3], [5]]
```

Built-ins are native markers, so the default round-trips nothing to Python; only a user callable crosses the boundary:

```python
periodic     = _Periodic()           # icechunk/regrid.py
single_chunk = _SingleChunk()
def fixed_chunks(n): return _FixedChunks(n)

ShiftFill = _Periodic | _SingleChunk | _FixedChunks | Callable[[list[list[int]]], list[list[int]]]
```

```rust
enum Fill { Periodic, SingleChunk, FixedChunks(usize), Custom(PyObject) }
// Periodic | SingleChunk | FixedChunks run in Rust; Custom calls back into Python
```

#### reindex_array

`reindex_array` is a lower-level, much more powerful tool than `shift_array`. It allows a user to arbitrarily reorder chunks in an array. Since a user can define both forward and optionally backward mappings, and not every chunk needs to be mapped somewhere new, there is no easy general definition of "periodic" boundaries. This is because, unlike in `shift_array` it is possible to have a vacuumed chunk disconnected from the edge.

##### Open question: a gap's size can be ambiguous

For `shift_array` a vacuumed region is always one contiguous slab at an edge, so its extent is fixed (`dim_length − sum(settled)`). `reindex_array` can break this: when a surviving chunk sits *between* two vacuumed regions, only the *total* vacated extent is fixed, and the split between the two gaps is not.


A vacuumed area is filled per dimension, not per cell: a filled chunk pins its whole row and column, so a size is free to choose only when an *entire* slice is vacuumed. 3x3 grid of chunks whose vacuumed cells (`.`) coalesce across both axes (top-left blob) with one isolated cell (bottom-right):

```text
            c0=1   c1=2   c2=3       (6 elements wide)
          ┌──────┬──────┬──────┐
   r0=1   │ .    │  .   │ FILL │
          ├──────┼──────┼──────┤
   r1=2   │ FILL │  .   │ FILL │
          ├──────┼──────┼──────┤
   r2=3   │ FILL │ FILL │  .   │
          └──────┴──────┴──────┘
        (6 elements tall)
```

Example inputs the fill sees (`None` = free slot), for this grid and two variants:

```python
# this grid (every slice pinned) — fill not called, nothing to choose
fill([[1, 2, 3], [1, 2, 3]])
# column 1 fully vacuumed — user fills the None (axis 1 must sum to 6)
fill([[1, 2, 3], [1, None, 3]])
# column 1 + row 2 fully vacuumed
fill([[1, 2, None], [1, None, 3]])
```

solutionto how to restructure the chunk grid shapes. So `reindex_array` needs to gain a way for the user to indicate how to transform the chunk grid. We also need to consider if we want any validation of the new chunk grid as the user can very easily construct a broken grid that will prevent data reads.

##### New Grid

The filled / vacuumed split from the [Motivation](#what-this-breaks-today) diagram, in terms of the `forward`/`backward` mappings:

1. **Has a source** — `backward(idx)` resolves to a source `S` (equivalently some `forward(S) == idx`). Shape is `old_grid[S]`, which Icechunk knows even when `S` is empty, since an empty chunk still has a declared size. Data is copied if `S` had it, otherwise the slot is cleared but keeps `S`'s size. No user input.
2. **Vacuumed** — `backward(idx)` is `None` (out of bounds). No source, so no size to inherit; the user must supply it.

Only case 2 needs a user-supplied shape, and it is the single situation `backward(idx) is None` — so one callback covers it.


Per-dimension chunk sizes are `[1, 2, 3]` on each axis, so e.g. the isolated `(2, 2)` cell spans a `3x3` element region. Even though the vacuumed cells coalesce into 2D shapes, every row and column still has a filled chunk, so all sizes stay pinned — `DimFill` gets no holes (axis 0 `[1, 2, 3]`, axis 1 `[1, 2, 3]`) and the user supplies nothing. The coalesced shape is not a fillable region; freedom is per-dimension and arises only when a whole row or column is vacuumed.

```python
# all axes at once (like ShiftFill): receives every axis's sizes with None at each
# free slot (a fully-vacuumed slice); returns the resolved per-axis sizes
# (pinned entries unchanged).
DimFill = Callable[[list[list[int | None]]], list[list[int]]]

def reindex_array(
    self,
    array_path: str,
    forward:  Callable[[Iterable[int]], Iterable[int] | None],
    backward: Callable[[Iterable[int]], Iterable[int] | None] | None = None,
    *,
    fill: DimFill | None = None,  # None = keep prior sizes
) -> None: ...
```

`single_chunk` / `fixed_chunks` from `icechunk.regrid` work here too (extent-only). `periodic` is shift-only.

Superseded alternatives:

- `forward`/`backward` returning a `(new_idx, chunk_shape)` tuple — breaks the signature for all callers.
- Paired `forward_chunk_shape` / `backward_chunk_shape` — doubles Rust/Python boundary crossings per chunk and duplicates logic.

The single additive callback is purely optional (no breaking change, sidesteps the variable-return-type concern from review). It declares a sub-grid per slot only; it does not guarantee a consistent rectilinear grid (see [Validation](#validation)).

##### Validation

Without any validation it would be possible for a `reindex_array` user to corrupt their array by writing invalid chunk sizes. Options

1. Accept this risk and warn in documentation.
   - Users of `reindex_array` are using an advanced tool and have both the power and responsibility that comes with that.
2. Perform basic validation
   - Per-axis rectilinear consistency (chunks sharing an axis index agree on that axis's size)
   - Per-axis sums equal dimension length

Option 2, doing validation, has a lot of potential for complexity and we may accidentally miss some edge cases. So an incomplete validation is likely worse than none at all as it would give false confidence. Given that users of `reindex_array` are doing an advanced operation it is acceptable for them to accept the risk of writing an incorrect chunk grid. I.e. they assume the responsibility for carefully validating their final chunk grid. Additionally this is safe because they can always roll back the operation using Icechunk time travel.


## Array Metadata implementation location

Array metadata is stored in two forms, with a parser translating between them:

1. **Verbatim `zarr.json`** — the raw bytes, stored opaquely. This is the only place the actual chunk sizes (e.g. rectilinear `[1, 1, 2, 3]`) live.
   - `user_data: Bytes` field on `NodeSnapshot` — `icechunk-format/src/snapshot.rs:148`
2. **Structured projection** — the typed subset icechunk operates on (shape, dimension names, chunk *count* per dimension). Lossy for rectilinear grids: it records the count, not the sizes. Part of the on-disk format.
   - `NodeData::Array { shape, dimension_names, manifests }` — `icechunk-format/src/snapshot.rs:135`
   - `ArrayShape` / `DimensionShape { dim_length, num_chunks }` — `icechunk-format/src/snapshot.rs:46` / `:28`

Translating between the two is a **parser** — code, not stored data. It reads the grid out of `zarr.json` and projects it down to the structured form. Read-only today, and lives in `store.rs`.

- `ArrayMetadata` (+ `ChunkGridSerializer`) — `icechunk/src/store.rs:1140` / `:1403`
- `num_chunks()`, `get_chunk_shapes()`, `shape()` — `icechunk/src/store.rs:1162`, `:1224`, `:1345`

The existing write path the Session can reuse to rewrite the verbatim form: `Session::update_array(path, shape, dimension_names, user_data)` — `icechunk/src/session.rs:754` (already called by the store's `set_array_meta`, `icechunk/src/store.rs:865`).

Any fix for the chunk grid necessarily will need access to `ArrayMetadata` and related structs which currently live in `store.rs`. However `shift_array` and `reindex_array` are in `session.rs`. So to fix the chunk grid issues we will need to import from store into session which inverts their current import relationship. While Rust can handle this, it's a sign that a new abstraction around metadata would be helpful.

Options:

1. Accept things as they are and just import both directions.
2. Create a new `array_metadata.rs` file with the metadata that lives in `zarr.json`
    - `store.rs` keeps store level metadata and anything that knows about a key like `array/c/0` while the new file knows about anything that goes inside of `zarr.json`

This appears to be a relatively clean extraction and may be helpful in the future. So the recommendation here is to extract to  `array_metadata.rs`
