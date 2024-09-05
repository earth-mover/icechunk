# Prototype code for bbox merging

from typing import Generator, Iterable, NewType, cast
import math
import operator
from itertools import accumulate, product
from hypothesis import strategies as st
import hypothesis.extra.numpy as npst
from hypothesis import given

Bbox = NewType("Bbox", tuple[slice, ...])
Coord = NewType("Coord", tuple[int, ...])

chunk_grid_shapes = npst.array_shapes(max_side=10)


def merge_bboxes(
    bboxes: Iterable[Bbox], chunk_grid_shape: tuple[int, ...], max_manifest_rows: int
) -> list[Bbox]:
    """
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
    # (sparsity defined as bbox-extent / num-chunks-along-axis)
    # run this algorithm, and then permute back to the correct axes order.
    ndim = len(chunk_grid_shape)

    # This assumes the bbox is densely populated, which isn't great
    # but conveniently generalizes to treating a single coordinate
    # location as a single element bbox
    uniques: list[set[int]] = list(set() for _ in range(ndim))
    for bbox in bboxes:
        for axis, size in zip(range(ndim), chunk_grid_shape):
            uniques[axis].update(range(*bbox[axis].indices(size)))

    # Dense manifest case, number of rows is max-min + 1
    # TODO: use a more efficient minmax function
    mins = []
    maxs = []
    for uniq in uniques:
        mins.append(min(uniq))
        maxs.append(max(uniq))
    nrows = tuple(max - min + 1 for min, max in zip(mins, maxs))

    cumprod = tuple(reversed(tuple(accumulate(reversed(nrows), func=operator.mul))))
    print(f"{cumprod=}")

    # This is the "split_axis" threshold, every axis to the left
    # and this one will be split to construct multiple bboxes
    # `stride` is use to split `split_axis`,
    # axes to the left are split with stride=1
    # TODO: clean up this if/else
    if max_manifest_rows > cumprod[0]:
        split_axis = 0
        stride = nrows[split_axis]
        # stride = cumprod[0]
    else:
        for split_axis, n in enumerate(cumprod):
            if n < max_manifest_rows:
                split_axis -= 1
                break
        stride = max(max_manifest_rows // n, 1)

    print(f"{split_axis=}, {stride=}")
    print(f"{mins=}, {maxs=}")

    # breakpoints along each axis, includes +1 to get the last element (yuck)
    breaks = (
        # stride of 1 for axes to the left
        tuple(tuple(range(mins[axis], maxs[axis] + 2)) for axis in range(split_axis))
        + (
            # use `stride` for split_axis
            tuple(range(mins[split_axis], maxs[split_axis] + 1, stride))
            # add the last element
            + (maxs[split_axis] + 1,),
        )
        # all axes to the right are fully included, no stride
        + tuple((mins[axis], maxs[axis] + 1) for axis in range(split_axis + 1, ndim))
    )
    print(f"{breaks=}")

    # slices that let us construct a bbox
    slicers: tuple[tuple[slice, ...], ...] = tuple(
        tuple(slice(start, stop) for start, stop in zip(inds[:-1], inds[1:]))
        for inds in breaks
    )
    print(f"{slicers=}")

    # TODO: consider deleting here.
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


def coords_to_bboxes(coords: tuple[Coord, ...]) -> Generator[Bbox, None, None]:
    return (cast("Bbox", tuple(slice(i, i + 1) for i in coord)) for coord in coords)


def test_bbox():
    chunk_grid_shape = (5, 5, 5)
    chunkidxs = ((0, 1), (0, 4), (0, 1, 2))
    coords = tuple(product(*chunkidxs))
    bboxes = coords_to_bboxes(coords)
    expected = (
        (slice(0, 1), slice(0, 2), slice(0, 3)),
        (slice(0, 1), slice(4, 5), slice(0, 3)),
        (slice(1, 2), slice(0, 2), slice(0, 3)),
        (slice(1, 2), slice(4, 5), slice(0, 3)),
    )
    actual = tuple(merge_bboxes(bboxes, chunk_grid_shape, max_manifest_rows=6))
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
