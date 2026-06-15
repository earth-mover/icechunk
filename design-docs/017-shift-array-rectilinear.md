<!-- markdownlint-disable MD013 -->
<!-- markdownlint-disable MD012 -->
# `shift_array` and `reindex_array` with rectilinear chunks

## Motivation

Zarr-python added support for rectilinear chunk grids behind a flag in version 3.2.0. These follow the [rectilinear zarr-extension](https://github.com/zarr-developers/zarr-extensions/tree/main/chunk-grids/rectilinear), not [ZEP 3](https://zarr.dev/zeps/draft/ZEP0003.html). The plan is to move this support from experimental to stable in the next minor version (<https://github.com/zarr-developers/zarr-python/issues/4025>).

These grids allow for variable chunk sizes along a given dimension. So you can have a 7-element 1-D array with chunk sizes `[1, 1, 2, 3]`.

### What this breaks today

Icechunk is agnostic to chunk size information except for the `shift_array` and `reindex_array` functions which modify the ordering of chunks. If you apply a shift to a rectilinear chunk array today then you can commit it without issue. But when anyone tries to actually read the chunk data they will get an error `cannot reshape array of size 1 into shape (2,)` because we change the mapping to array index to chunk, but do not update the chunk grid that zarr uses to understand what size chunk it should be looking for.

See <https://github.com/earth-mover/icechunk/issues/2151>


## Design

### Chunk Size Cycling

The core of the bug is that the declared chunk sizes in the zarr metadata end up out of sync with the on-disk data sizes. So to fix this we need to either refuse to do chunk reindexing on rectilinear arrays, or update the chunk grid as part of the operation.


`shift_array` is a useful feature, as are rectilinear chunks. They also play well in combination as an exemplar use case of rectilinear chunks is months of the year havign different shapes. So for something like weather predictions over a year we might expect rectilinear chunks. Predictions rolling over a year is a also a good use case of `shift_array`. Therefore simply disallowing `shift_array` on rectilinear chunks is not a good solution. We need to decide on the strategy how to correctly and consistently fill in the chunk sizes of the vacated slots.

#### Shift Array

For a chunk grid of `[1, 1, 2, 3]` shifted right by 2 chunks, there are four options that preserve the array shape.

1. Periodic boundary - `[2, 3, 1, 1]`
2. One fill chunk that takes up all empty space `[5, 1, 1]`
3. Fill with size 1 - `[1,1,1,1,1,2]`
4. Rechunk to include the empty part in the existing chunk. So the first chunk would become (1+3+2) `[6, 1]`


Option 3 will not work because a chunk with size 1 is far too small for real-world applications. Option 4 has the potential to be computationally intensive as it might end up rechunking real data to combine it with empty chunks.

That leaves Options 1 and 2.  which are always equivalent for a shift (in chunk space) of magnitude 1. They differ with a shift of magnitude 2 or greater.

For any periodic data, like chunks of months, Option 1 is more sensible than creating a large empty chunk that may not fit well with writing workflows. Additionally there is current no mechanism for a user to rechunk the combined large empty chunk (`5`) into smaller chunks. Icechunk could provide this metadata only rechunking as a new feature, but that is out of scope here.

That leaves Option 1, which does have the downside that it may be slightly confusing that chunk sizes are shifted around the periodic boundary while values are **not** shifted to the other side of the boundary. However, it seems to be the only strategy that gives generally usable results.

**Decision:** periodic boundary for chunk sizes but not chunk values.

#### reindex_array

`reindex_array` is a lower-level, much more powerful tool than `shift_array`. It allows a user to arbitrarily reorder chunks in an array. Since a user can define both forward and optionally backward mappings, and not every chunk needs to be mapped somewhere new, there is no easy general solution to how to restructure the chunk grid shapes. So `reindex_array` needs to gain a way for the user to indicate how to transform the chunk grid. We also need to consider if we want any validation of the new chunk grid as the user can very easily construct a broken grid that will prevent data reads.

##### New Grid

The simplest approach here is to have the user provide a function that determines the chunk size of the vacated chunk.

1. Change signature of `forward`/`backward` to allow this.

   Today:

   ```python
   forward(idx: list[int])  -> list[int] | None
   backward(idx: list[int]) -> list[int] | None
   ```

   Proposed:

   ```python
   forward(idx: list[int])  -> tuple[list[int] | None, list[int] | None]
   backward(idx: list[int]) -> tuple[list[int] | None, list[int] | None]
   ```

   `forward(idx)` returns `(new_idx_or_None, chunk_shape_at_idx)` where `chunk_shape_at_idx` is the chunk shape at source position `idx`. `chunk_shape_at_idx` can also be `None` to indicate to leave the shape the same as a convenience for regular grids.
2.
3. Require two extra functions for chunk shapes (one per grid direction), leaving `forward`/`backward` unchanged.

   ```python
   forward(idx: list[int])              -> list[int] | None
   backward(idx: list[int])             -> list[int] | None
   forward_chunk_shape(idx: list[int])  -> list[int]
   backward_chunk_shape(idx: list[int]) -> list[int]
   ```

   `forward_chunk_shape(idx)` returns the chunk shape at `idx`.
   `backward_chunk_shape(idx)` returns the chunk shape at location that was mapped to `idx`.

Option 2 splits the computation of chunk grid and new location and would require 2 calls across the Rust/Python boundary for each chunk in the array.

So Option 1 is cleaner and easier for the user. However, it has the downside that it is an API change to `reindex_array`. This is an acceptable risk because there are likely very few users of `reindex_array` today.

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
