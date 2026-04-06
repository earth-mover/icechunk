from icechunk._icechunk_python import (
    CachingConfig,
    CompressionAlgorithm,
    CompressionConfig,
    FeatureFlag,
    ManifestConfig,
    ManifestPreloadCondition,
    ManifestPreloadConfig,
    ManifestSplitCondition,
    ManifestSplitDimCondition,
    ManifestSplittingConfig,
    ManifestVirtualChunkLocationCompressionConfig,
    ObjectStoreConfig,
    RepositoryConfig,
    initialize_logs,
    set_logs_filter,
)

__all__ = [
    "CachingConfig",
    "CompressionAlgorithm",
    "CompressionConfig",
    "FeatureFlag",
    "ManifestConfig",
    "ManifestPreloadCondition",
    "ManifestPreloadConfig",
    "ManifestSplitCondition",
    "ManifestSplitDimCondition",
    "ManifestSplittingConfig",
    "ManifestVirtualChunkLocationCompressionConfig",
    "ObjectStoreConfig",
    "RepositoryConfig",
    "initialize_logs",
    "set_logs_filter",
]


# --- ManifestSplittingConfig dict helpers ---
#
# Python dicts preserve insertion order, but this gets mapped to a Rust HashMap
# which does *not* preserve order. So on the python side, we accept a dict as a
# nicer API, and immediately convert it to tuples that preserve order, and pass
# those to Rust.

type ManifestSplitValues = dict[
    ManifestSplitDimCondition.Axis
    | ManifestSplitDimCondition.DimensionName
    | ManifestSplitDimCondition.Any,
    int,
]
type SplitSizesDict = dict[
    ManifestSplitCondition,
    ManifestSplitValues,
]


def from_dict(split_sizes: SplitSizesDict) -> ManifestSplittingConfig:
    unwrapped = tuple((k, tuple(v.items())) for k, v in split_sizes.items())
    return ManifestSplittingConfig(unwrapped)


def to_dict(config: ManifestSplittingConfig) -> SplitSizesDict:
    return {
        split_condition: dict(dim_conditions)
        for split_condition, dim_conditions in config.split_sizes
    }


ManifestSplittingConfig.from_dict = staticmethod(from_dict)  # type: ignore[method-assign]
ManifestSplittingConfig.to_dict = to_dict  # type: ignore[method-assign,assignment]
