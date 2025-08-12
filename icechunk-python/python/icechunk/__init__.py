# module

from typing import TypeAlias

from icechunk._icechunk_python import (
    AzureCredentials,
    AzureStaticCredentials,
    BasicConflictSolver,
    CachingConfig,
    CompressionAlgorithm,
    CompressionConfig,
    Conflict,
    ConflictDetector,
    ConflictError,
    ConflictSolver,
    ConflictType,
    Credentials,
    Diff,
    GcsBearerCredential,
    GcsCredentials,
    GcsStaticCredentials,
    GCSummary,
    IcechunkError,
    ManifestConfig,
    ManifestFileInfo,
    ManifestPreloadCondition,
    ManifestPreloadConfig,
    ManifestSplitCondition,
    ManifestSplitDimCondition,
    ManifestSplittingConfig,
    ObjectStoreConfig,
    RebaseFailedError,
    RepositoryConfig,
    S3Credentials,
    S3Options,
    S3StaticCredentials,
    SnapshotInfo,
    Storage,
    StorageConcurrencySettings,
    StorageRetriesSettings,
    StorageSettings,
    VersionSelection,
    VirtualChunkContainer,
    VirtualChunkSpec,
    __version__,
    initialize_logs,
    set_logs_filter,
    spec_version,
)
from icechunk.credentials import (
    AnyAzureCredential,
    AnyAzureStaticCredential,
    AnyCredential,
    AnyGcsCredential,
    AnyGcsStaticCredential,
    AnyS3Credential,
    azure_credentials,
    azure_from_env_credentials,
    azure_static_credentials,
    containers_credentials,
    gcs_credentials,
    gcs_from_env_credentials,
    gcs_refreshable_credentials,
    gcs_static_credentials,
    s3_anonymous_credentials,
    s3_credentials,
    s3_from_env_credentials,
    s3_refreshable_credentials,
    s3_static_credentials,
)
from icechunk.repository import Repository
from icechunk.session import ForkSession, Session
from icechunk.storage import (
    AnyObjectStoreConfig,
    azure_storage,
    gcs_storage,
    gcs_store,
    http_store,
    in_memory_storage,
    local_filesystem_storage,
    local_filesystem_store,
    r2_storage,
    s3_storage,
    s3_store,
    tigris_storage,
)
from icechunk.store import IcechunkStore
from icechunk.zarr_adapter import (
    IcechunkPathSpec,
    create_readonly_session_from_path,
    create_readonly_session_from_path_spec,
    parse_icechunk_path_spec,
)

__all__ = [
    "AnyAzureCredential",
    "AnyAzureStaticCredential",
    "AnyCredential",
    "AnyGcsCredential",
    "AnyGcsStaticCredential",
    "AnyObjectStoreConfig",
    "AnyS3Credential",
    "AzureCredentials",
    "AzureStaticCredentials",
    "BasicConflictSolver",
    "CachingConfig",
    "CompressionAlgorithm",
    "CompressionConfig",
    "Conflict",
    "ConflictDetector",
    "ConflictError",
    "ConflictSolver",
    "ConflictType",
    "Credentials",
    "Diff",
    "ForkSession",
    "GCSummary",
    "GcsBearerCredential",
    "GcsCredentials",
    "GcsStaticCredentials",
    "IcechunkError",
    "IcechunkPathSpec",
    "IcechunkStore",
    "ManifestConfig",
    "ManifestFileInfo",
    "ManifestPreloadCondition",
    "ManifestPreloadConfig",
    "ManifestSplitCondition",
    "ManifestSplitDimCondition",
    "ManifestSplittingConfig",
    "ObjectStoreConfig",
    "RebaseFailedError",
    "Repository",
    "RepositoryConfig",
    "S3Credentials",
    "S3Options",
    "S3StaticCredentials",
    "Session",
    "SnapshotInfo",
    "Storage",
    "StorageConcurrencySettings",
    "StorageRetriesSettings",
    "StorageSettings",
    "VersionSelection",
    "VirtualChunkContainer",
    "VirtualChunkSpec",
    "__version__",
    "azure_credentials",
    "azure_from_env_credentials",
    "azure_static_credentials",
    "azure_storage",
    "containers_credentials",
    "create_readonly_session_from_path",
    "create_readonly_session_from_path_spec",
    "gcs_credentials",
    "gcs_from_env_credentials",
    "gcs_refreshable_credentials",
    "gcs_static_credentials",
    "gcs_storage",
    "gcs_store",
    "http_store",
    "in_memory_storage",
    "initialize_logs",
    "local_filesystem_storage",
    "local_filesystem_store",
    "parse_icechunk_path_spec",
    "print_debug_info",
    "r2_storage",
    "s3_anonymous_credentials",
    "s3_credentials",
    "s3_from_env_credentials",
    "s3_refreshable_credentials",
    "s3_static_credentials",
    "s3_storage",
    "s3_store",
    "set_logs_filter",
    "spec_version",
    "tigris_storage",
]


def print_debug_info() -> None:
    import platform
    from importlib import import_module

    print(f"platform:  {platform.platform()}")
    print(f"python:  {platform.python_version()}")
    print(f"icechunk:  {__version__}")
    for package in ["zarr", "numcodecs", "xarray", "virtualizarr"]:
        try:
            print(f"{package}:  {import_module(package).__version__}")
        except ModuleNotFoundError:
            continue


# This monkey patch is a bit annoying. Python dicts preserve insertion order
# But this gets mapped to a Rust HashMap which does *not* preserve order
# So on the python side, we can accept a dict as a nicer API, and immediately
# convert it to tuples that preserve order, and pass those to Rust

SplitSizesDict: TypeAlias = dict[
    ManifestSplitCondition,
    dict[
        ManifestSplitDimCondition.Axis
        | ManifestSplitDimCondition.DimensionName
        | ManifestSplitDimCondition.Any,
        int,
    ],
]


def from_dict(split_sizes: SplitSizesDict) -> ManifestSplittingConfig:
    unwrapped = tuple((k, tuple(v.items())) for k, v in split_sizes.items())
    return ManifestSplittingConfig(unwrapped)


def to_dict(config: ManifestSplittingConfig) -> SplitSizesDict:
    return {
        split_condition: dict(dim_conditions)
        for split_condition, dim_conditions in config.split_sizes
    }


ManifestSplittingConfig.from_dict = from_dict  # type: ignore[method-assign]
ManifestSplittingConfig.to_dict = to_dict  # type: ignore[attr-defined]

initialize_logs()


# Auto-register ZEP 8 adapters for zarr URL support
def _register_zarr_adapters():
    """Auto-register icechunk adapters when the package is imported."""
    try:
        from icechunk.zarr_adapter import IcechunkStoreAdapter, ICStoreAdapter
        from zarr.registry import register_store_adapter

        register_store_adapter(IcechunkStoreAdapter)
        register_store_adapter(ICStoreAdapter)
    except ImportError:
        # zarr not available or version doesn't support ZEP 8
        pass


_register_zarr_adapters()
