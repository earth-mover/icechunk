from collections.abc import Iterable
from typing import cast

import hypothesis.strategies as st

import icechunk as ic
import zarr
from zarr.core.metadata import ArrayV3Metadata


@st.composite
def splitting_configs(
    draw: st.DrawFn, *, arrays: Iterable[zarr.Array]
) -> ic.ManifestSplittingConfig:
    config_dict = {}
    for array in arrays:
        if draw(st.booleans()):
            array_condition = ic.ManifestSplitCondition.name_matches(
                array.path.split("/")[-1]
            )
        else:
            array_condition = ic.ManifestSplitCondition.path_matches(array.path)
        dimnames = (
            cast(ArrayV3Metadata, array.metadata).dimension_names or (None,) * array.ndim
        )
        dimsize_axis_names = draw(
            st.lists(
                st.sampled_from(
                    tuple(zip(array.shape, range(array.ndim), dimnames, strict=False))
                ),
                min_size=1,
                unique=True,
            )
        )
        for size, axis, dimname in dimsize_axis_names:
            if dimname is None or draw(st.booleans()):
                key = ic.ManifestSplitDimCondition.Axis(axis)
            else:
                key = ic.ManifestSplitDimCondition.DimensionName(dimname)  # type: ignore[assignment]
            config_dict[array_condition] = {
                key: draw(st.integers(min_value=1, max_value=size + 10))
            }
    return ic.ManifestSplittingConfig.from_dict(config_dict)  # type: ignore[attr-defined, no-any-return]


@st.composite
def chunk_coordinates(draw: st.DrawFn, numblocks: tuple[int, ...]) -> tuple[int, ...]:
    return draw(
        st.tuples(*tuple(st.integers(min_value=0, max_value=b - 1) for b in numblocks))
    )


@st.composite
def chunk_slicers(
    draw: st.DrawFn, numblocks: tuple[int, ...], chunk_shape: tuple[int, ...]
) -> tuple[slice, ...]:
    return tuple(
        (
            slice(coord * size, coord + 1 * size)
            for coord, size in zip(
                draw(chunk_coordinates(numblocks)), chunk_shape, strict=False
            )
        )
    )


@st.composite
def chunk_paths(draw: st.DrawFn, numblocks: tuple[int, ...]) -> str:
    blockidx = draw(chunk_coordinates(numblocks))
    return "/".join(map(str, blockidx))


@st.composite
def chunk_directories(draw: st.DrawFn, numblocks: tuple[int, ...]) -> str:
    ndim = len(numblocks)
    blockidx = draw(chunk_coordinates(numblocks))
    subset_slicer = (
        slice(draw(st.integers(min_value=0, max_value=ndim)))
        if draw(st.booleans())
        else slice(None)
    )
    return "/".join(map(str, blockidx[subset_slicer]))
