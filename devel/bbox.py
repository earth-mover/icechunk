# Prototype code for bbox merging

# Some notes:
# 1. We can treat a single chunk coordinate (0, 0, 0) as the Bbox (slice(0,1), slice(0,1), slice(0,1)).
#    With this in mind,
#    (a) computing the bbox for a group of chunks is the same as computing the
#        bbox for a group of bboxes.
#    (b) "a group of bboxes" might result from distributed writers, or an old bbox and new "update" bbox.
# 2. Optimizing the bounding box to be "tight" around populated chunks is hard to do without either
#    sorting the chunks, and/or processing a large number of them at once.
#    - Optimizing is especially hard when folding an iterator of BBoxes, or in `ChangeSet.set_chunk_ref`. For example,
#      consider the following chunks written in order: [(slice(0, 1),), (slice(1e5, 1e5+1),),  (slice(1, 2),)]
#      assuming that chunk 1e5 is so far away that we don't want to merge its Bbox with that of chunk 0.
#      Ideally we would like to merge to [(slice(0, 2),), slice(1e5, 1e5+1)]. For this we'd need to cluster or sort
#      so that nearby chunks are together and merge those.
#      In effect this is at least one full pass through all chunk coordinates in a ChangeSet.
# 3. One option would be to for
#     (a) ChangeSet to maintain a sorted list of chunk coordinates as chunks are written.
#         - we want to do this anyway to write sorted references to enable binary searching in the future, if needed.
#     (b) Call `ChangeSet.compute_bbox` when we are ready to flush; distributed writers can call this
#         before sending ChangeSets back to the coordinator.
#     (c) Merge old bboxes and new "update" bboxes together.
#     (d) Then iterate over bbox and generate the snapshot file, and manifest file entries.
# 4. Still need to derisk writing a manifest file with a given bbox and NULL entries.
# 5. Deleting chunks that are in a previous snapshot isn't handled yet.
#    - to do this we'd need to know which chunks are missing in the latest snapshot,
#    - combine with deleted chunks in this changeset
#    - then determine bounding box -- another pass through all chunk coordinates.

from typing import Generator, Iterable, NewType, cast, Iterator
import math
import operator
from itertools import accumulate, product
from hypothesis import strategies as st
import hypothesis.extra.numpy as npst
from hypothesis import given

Bbox = NewType("Bbox", tuple[slice, ...])
Coord = NewType("Coord", tuple[int, ...])

chunk_grid_shapes = npst.array_shapes(max_side=10)


# TODO: instead consider a present: list[Bbox] state
# and an update: Bbox
# that returns list[Bbox]
def merge_bboxes(
    bboxes: tuple[Bbox], chunk_grid_shape: tuple[int, ...], max_manifest_rows: int
) -> list[Bbox]:
    """
    Merges a sequence of bboxes together.

    Parameters
    ----------
    bboxes : Iterable[tuple[slice, ...]]
        Iterable of bounding boxes defined as a tuple of slice objects, one per axis
    chunk_grid_shape : tuple[int, ...]
        Shape of the chunk grid
    max_manifest_rows : int
        Maximum number of rows allowed in a single manifest file.

    Returns
    -------
    tuple[slice, ...]
    """
    # This algorithm runs from left to right. This decision follows from the decision
    # to ravel the coordinates in C-order.
    # It determines the "split_axis" threshold defined as
    #    - axes to the right of this axis are preserved whole.
    #    - axes to the left of this axis are split with size 1
    # TODO: A possibly more optimal approach is to sort the axes in order of sparsity,
    #       (sparsity defined as bbox-extent / num-chunks-along-axis)
    #       run this algorithm, and then permute back to the correct axes order.
    ndim = len(chunk_grid_shape)

    # Determine the unique indexes along each axis present in each bbox
    # This assumes the bbox is densely populated, which isn't great
    # but conveniently generalizes to treating a single coordinate
    # location as a single element bbox,
    uniques: list[set[int]] = list(set() for _ in range(ndim))
    # number of rows occupied by each bbox
    nrows: list[int] = []
    for bbox in bboxes:
        n = 1
        for axis, size in zip(range(ndim), chunk_grid_shape):
            indices = range(*bbox[axis].indices(size))
            n *= len(indices)
            uniques[axis].update(indices)
        nrows.append(n)

    # Figure out the min/max bounding box.
    # TODO: use the more efficient minmax function in rust,
    # and parse the bboxes directly
    mins = []
    maxs = []
    for uniq in uniques:
        mins.append(min(uniq))
        maxs.append(max(uniq))
    # Dense manifest case, number of elements along each axis is max-min + 1
    numel = tuple(max - min + 1 for min, max in zip(mins, maxs))
    nrows_all = math.prod(numel)

    # At this point, we basically have the "easy" min/max bounding box.
    # The rest is being clever about splitting this bounding box in to multiple bboxes
    # using max_manifest_rows, and potentially trimming empty bboxes.
    # -----------------------------------------------------------------

    # If the rows required to store coordinates using the min/max bounding box
    # is greater than (arbitrary factor) x rows requires to store each bounding box separately,
    # then we don't merge them.
    # FIXME: This is not robust to chunks coming in unsorted.
    # Example: for these bboxes [(slice(0, 1),), (slice(1e5, 1e5+1),), (slice(1, 2),)]
    # we'll never merge (slice(1,2),) with (slice(0,1),) without some kind of clustering,
    # and the merging
    # if sum(nrows) / nrows_all < 0.3:
    #    return list(bboxes)

    # First determine how many chunks are present in every axis to the right of a given axis
    cumprod = tuple(reversed(tuple(accumulate(reversed(numel), func=operator.mul))))
    print(f"{cumprod=}")

    # Now calculate the "split_axis"
    #   - every axis to the left, and this one, will be split to construct multiple bboxes
    #   - every axis to the right is preserved whole
    # `stride` is use to split `split_axis`,
    # axes to the left are split with stride=1
    # TODO: clean up this if/else
    if max_manifest_rows > cumprod[0]:
        split_axis = 0
        stride = numel[split_axis]
    else:
        for split_axis, n in enumerate(cumprod):
            if n < max_manifest_rows:
                split_axis -= 1
                break
        stride = max(max_manifest_rows // n, 1)

    print(f"{split_axis=}, {stride=}")
    print(f"{mins=}, {maxs=}")

    # Calculate breakpoints along each axis that divide the min/max bounding box appropriately
    strides = (
        # Use stride of `1` for axes to the left
        (1,) * split_axis
        # and `stride` for split_axis.
        + (stride,)
        # all axes to the right are fully included, no stride
        + (None,) * (ndim - split_axis - 1)
    )
    breaks = tuple(
        (
            # Add 1 to the last element +1 so that a slice object constructed later will include the last element.
            tuple(range(mins[axis], maxs[axis] + 1, stride or maxs[axis] + 1))
            # add the last element explicitly
            + (maxs[axis] + 1,)
        )
        for axis, stride in zip(range(ndim), strides)
    )
    print(f"{breaks=}")

    # slices that let us construct a bbox
    slicers: tuple[tuple[slice, ...], ...] = tuple(
        tuple(slice(start, stop) for start, stop in zip(inds[:-1], inds[1:]))
        for inds in breaks
    )
    print(f"{slicers=}")

    # TODO: consider deleting this, or implementing as a future optimization
    # Now we go through every possible bbox and ask if it is populated.
    out_bboxes: list[Bbox] = []
    for bbox_slicers in product(*slicers):
        is_empty = False
        for axis, slicer in enumerate(bbox_slicers):
            bbox_indices = tuple(range(*slicer.indices(chunk_grid_shape[axis])))
            present_indices = uniques[axis]
            if not (set(present_indices) & set(bbox_indices)):
                # empty bbox, skip
                is_empty = True
                continue
        if not is_empty:
            out_bboxes.append(cast("Bbox", bbox_slicers))
    return out_bboxes


def fold_bboxes(
    bbox_iter: Iterator[Bbox], chunk_grid_shape: tuple[int, ...], max_manifest_rows: int
) -> list[Bbox]:
    "Runs a fold right on an iterator of BBoxes."
    state = [next(bbox_iter)]
    for next_bbox in bbox_iter:
        print(state)
        state = merge_bboxes([*state, next_bbox], chunk_grid_shape, max_manifest_rows)
    return state


def coords_to_bboxes(coords: tuple[Coord, ...]) -> Generator[Bbox, None, None]:
    return (cast("Bbox", tuple(slice(i, i + 1) for i in coord)) for coord in coords)


def test_bbox():
    chunk_grid_shape = (5, 5, 5)
    # TODO: scramble the order on these
    chunkidxs = ((0, 1), (0, 4), (0, 1, 2))
    coords = tuple(product(*chunkidxs))
    bboxes = coords_to_bboxes(coords)
    expected = (
        (slice(0, 1), slice(0, 2), slice(0, 3)),
        (slice(0, 1), slice(4, 5), slice(0, 3)),
        (slice(1, 2), slice(0, 2), slice(0, 3)),
        (slice(1, 2), slice(4, 5), slice(0, 3)),
    )
    actual = tuple(fold_bboxes(bboxes, chunk_grid_shape, max_manifest_rows=6))
    assert actual == expected


@given(data=st.data(), chunk_grid_shape=chunk_grid_shapes)
def test_bbox_single(data, chunk_grid_shape):
    # for any delta, this creates a contiguous bbox,
    # if max_manifest_rows == numchunks + 1, then we should get one bbox back.
    delta = data.draw(st.integers(min_value=0, max_value=min(chunk_grid_shape) - 1))
    coords = tuple(product(*tuple(range(size - delta) for size in chunk_grid_shape)))
    max_manifest_rows = math.prod(chunk_grid_shape) + 1
    bboxes = coords_to_bboxes(coords)
    assert len(tuple(merge_bboxes(bboxes, chunk_grid_shape, max_manifest_rows))) == 1


@given(chunk_grid_shape=chunk_grid_shapes)
def test_bbox_single_chunks(chunk_grid_shape):
    # when max_manifest_rows == 1, should always get back 1 chunk bboxes
    coords = tuple(product(*tuple(range(size) for size in chunk_grid_shape)))
    bboxes = coords_to_bboxes(coords)
    assert (
        len(tuple(merge_bboxes(bboxes, chunk_grid_shape, max_manifest_rows=1)))
        == len(coords)
        == math.prod(chunk_grid_shape)
    )


@given(data=st.data(), shape=chunk_grid_shapes)
def test_bbox_merging(data, shape):
    # start with bbox == chunk_grid_shape
    # merge in a subset
    bbox_1 = tuple(slice(0, size) for size in shape)
    bbox_2 = []
    for size in shape:
        start, stop = sorted(
            data.draw(
                st.lists(
                    st.integers(min_value=0, max_value=size),
                    min_size=2,
                    max_size=2,
                    unique=True,
                )
            )
        )
        bbox_2.append(slice(start, stop))
    assert merge_bboxes(
        (bbox_1, bbox_2), shape, max_manifest_rows=math.prod(shape) + 1
    ) == [bbox_1]


@given(data=st.data(), shape=chunk_grid_shapes)
def test_bbox_splitting(data, shape):
    # start with bbox == chunk_grid_shape
    # set max_manifest_rows == 1, get a one bbox per coord
    bbox_1 = tuple(slice(0, size) for size in shape)
    coords = tuple(product(*tuple(range(size) for size in shape)))
    assert merge_bboxes((bbox_1,), shape, max_manifest_rows=1) == list(
        coords_to_bboxes(coords)
    )


@st.composite
def bbox_from_shape(draw, shapes=chunk_grid_shapes):
    shape = draw(shapes)
    bbox = []
    for size in shape:
        start, stop = sorted(
            draw(
                st.lists(
                    st.integers(min_value=0, max_value=size),
                    min_size=2,
                    max_size=2,
                    unique=True,
                )
            )
        )
        bbox.append(slice(start, stop))
    return tuple(bbox)


@given(data=st.data(), shape=chunk_grid_shapes)
def test_bbox_idempotent(data, shape):
    # a single bbox is unmodified
    bbox = data.draw(bbox_from_shape(shapes=st.just(shape)))
    assert merge_bboxes((bbox,), shape, max_manifest_rows=math.prod(shape) + 1) == [
        bbox
    ]
