<!-- markdownlint-disable MD013 -->
<!-- markdownlint-disable MD012 -->

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


`shift_array` is a useful feature, as are rectilinear chunks. They also play well in combination. An exemplar use case of rectilinear chunks is months of the year having different shapes. So for something like weather predictions over a year we might expect rectilinear chunks. Predictions rolling over a year is a also a good use case of `shift_array`. Therefore, simply disallowing `shift_array` on rectilinear chunks is not a good solution. We need to decide on the strategy how to correctly and consistently fill in the chunk sizes of the vacated slots.



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

Option 1 keeps the user's job small, but in the ambiguous case above (a survivor between two gaps) we have to pick the split, and the user cannot change it. Option 2 lets the user place everything, so the ambiguity goes away.


`shift_array` does not hit this ambiguity, since its gap is always one block at an edge



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

So the user input here is simplified because they only need to provide the chunk width for the 3rd and 4th columns.
This means that it is possible that even in a fairly aggresive vacuuming no user input is necessarily required to fully determine the chunk grid. So in the below exmaple the grid is fully determined without any user input.


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


```python
FlatGrid = list[list[EdgeLength]]   # per axis, explicit edge lengths
```

## `shift_array`

`shift_array` uses reindex internally but is more constrained to only allow shifts along an axis. This reduces the problem space to a simpler 1D problem. For multi d arrays with shifts along multiple axes we can break it up into a series of 1D problems because by their nature these shifts are indepdent. So in this section we only consider the 1d. (maybe we need multi dim for the API section?)

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

The adjacent vacuumed space can be coalesced and then filled in a few ways. For the above shift of +2 chunks the options are

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


TODO: exact API. do we have special objects or just list of list[int]?



`periodic` wraps the rolled-off sizes into the vacuumed slab — sizes move around the boundary, data does not:

```text
before  size:   1   1  [2   3]      [2, 3] roll off the right edge
after   size:  [2   3]  1   1       and become the new (vacuumed) left sizes
                └─┬─┘   └─┬─┘
              vacuumed   filled
```

Full implementation. Each fill maps the per-axis list-of-lists to a per-axis list-of-lists; `periodic` is just the identity:





##### Validation

Without any validation it would be possible for a `reindex_array` user to corrupt their array by writing invalid chunk sizes. Options

1. Accept this risk and warn in documentation.
   - Users of `reindex_array` are using an advanced tool and have both the power and responsibility that comes with that.
2. Perform basic validation
   - Per-axis rectilinear consistency (chunks sharing an axis index agree on that axis's size)
   - Per-axis sums equal dimension length

Option 2, doing validation, has a lot of potential for complexity and we may accidentally miss some edge cases. So an incomplete validation is likely worse than none at all as it would give false confidence. Given that users of `reindex_array` are doing an advanced operation it is acceptable for them to accept the risk of writing an incorrect chunk grid. I.e. they assume the responsibility for carefully validating their final chunk grid. Additionally this is safe because they can always roll back the operation using Icechunk time travel.


### Array Metadata implementation location

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
