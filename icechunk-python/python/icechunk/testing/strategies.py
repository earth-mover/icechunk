from collections.abc import Iterable
from typing import TYPE_CHECKING, Any

import hypothesis.strategies as st

import icechunk as ic
import zarr

if TYPE_CHECKING:
    try:
        from zarr.core.metadata import ArrayV3Metadata
    except ImportError:
        ArrayV3Metadata = Any  # type: ignore[misc,assignment]


@st.composite
def splitting_configs(
    draw: st.DrawFn, *, arrays: "Iterable[zarr.Array[ArrayV3Metadata]]"
) -> ic.ManifestSplittingConfig:
    config_dict: dict[
        ic.ManifestSplitCondition,
        dict[
            ic.ManifestSplitDimCondition.Axis
            | ic.ManifestSplitDimCondition.DimensionName
            | ic.ManifestSplitDimCondition.Any,
            int,
        ],
    ] = {}
    for array in arrays:
        if draw(st.booleans()):
            array_condition = ic.ManifestSplitCondition.name_matches(
                array.path.split("/")[-1]
            )
        else:
            array_condition = ic.ManifestSplitCondition.path_matches(array.path)
        dimnames = (
            getattr(array.metadata, "dimension_names", None) or (None,) * array.ndim
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
    return ic.ManifestSplittingConfig.from_dict(config_dict)


@st.composite
def repository_configs(
    draw: st.DrawFn,
    num_updates_per_repo_info_file: st.SearchStrategy[int] = st.integers(  # noqa: B008
        min_value=1, max_value=50
    ),
    inline_chunk_threshold_bytes: st.SearchStrategy[int] | None = None,
    splitting: st.SearchStrategy[ic.ManifestSplittingConfig] | None = None,
) -> ic.RepositoryConfig:
    manifest = None
    if splitting is not None:
        manifest = ic.ManifestConfig(splitting=draw(splitting))
    return ic.RepositoryConfig(
        num_updates_per_repo_info_file=draw(num_updates_per_repo_info_file),
        inline_chunk_threshold_bytes=draw(inline_chunk_threshold_bytes)
        if inline_chunk_threshold_bytes is not None
        else None,
        manifest=manifest,
    )
