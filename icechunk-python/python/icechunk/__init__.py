# module

from typing import TypeAlias

from icechunk._icechunk_python import (
    AzureCredentials,
    AzureStaticCredentials,
    BasicConflictSolver,
    BranchCreatedUpdate,
    BranchDeletedUpdate,
    BranchResetUpdate,
    CachingConfig,
    ChunkType,
    CommitAmendedUpdate,
    CompressionAlgorithm,
    CompressionConfig,
    ConfigChangedUpdate,
    Conflict,
    ConflictDetector,
    ConflictError,
    ConflictSolver,
    ConflictType,
    Credentials,
    Diff,
    ExpirationRanUpdate,
    GCRanUpdate,
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
    MetadataChangedUpdate,
    NewCommitUpdate,
    NewDetachedSnapshotUpdate,
    ObjectStoreConfig,
    RebaseFailedError,
    RepoInitializedUpdate,
    RepoMigratedUpdate,
    RepositoryConfig,
    RepoUpdateRetryConfig,
    S3Credentials,
    S3Options,
    S3StaticCredentials,
    SessionMode,
    SnapshotInfo,
    Storage,
    StorageConcurrencySettings,
    StorageRetriesSettings,
    StorageSettings,
    TagCreatedUpdate,
    TagDeletedUpdate,
    UpdateType,
    VersionSelection,
    VirtualChunkContainer,
    VirtualChunkSpec,
    __version__,
    _upgrade_icechunk_repository,
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
    http_storage,
    http_store,
    in_memory_storage,
    local_filesystem_storage,
    local_filesystem_store,
    r2_storage,
    redirect_storage,
    s3_storage,
    s3_store,
    tigris_storage,
)
from icechunk.store import IcechunkStore

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
    "BranchCreatedUpdate",
    "BranchDeletedUpdate",
    "BranchResetUpdate",
    "CachingConfig",
    "ChunkType",
    "CommitAmendedUpdate",
    "CompressionAlgorithm",
    "CompressionConfig",
    "ConfigChangedUpdate",
    "Conflict",
    "ConflictDetector",
    "ConflictError",
    "ConflictSolver",
    "ConflictType",
    "Credentials",
    "Diff",
    "ExpirationRanUpdate",
    "ForkSession",
    "GCRanUpdate",
    "GCSummary",
    "GcsBearerCredential",
    "GcsCredentials",
    "GcsStaticCredentials",
    "IcechunkError",
    "IcechunkStore",
    "ManifestConfig",
    "ManifestFileInfo",
    "ManifestPreloadCondition",
    "ManifestPreloadConfig",
    "ManifestSplitCondition",
    "ManifestSplitDimCondition",
    "ManifestSplittingConfig",
    "MetadataChangedUpdate",
    "NewCommitUpdate",
    "NewDetachedSnapshotUpdate",
    "ObjectStoreConfig",
    "RebaseFailedError",
    "RepoInitializedUpdate",
    "RepoMigratedUpdate",
    "RepoUpdateRetryConfig",
    "Repository",
    "RepositoryConfig",
    "S3Credentials",
    "S3Options",
    "S3StaticCredentials",
    "Session",
    "SessionMode",
    "SnapshotInfo",
    "Storage",
    "StorageConcurrencySettings",
    "StorageRetriesSettings",
    "StorageSettings",
    "TagCreatedUpdate",
    "TagDeletedUpdate",
    "UpdateType",
    "VersionSelection",
    "VirtualChunkContainer",
    "VirtualChunkSpec",
    "__version__",
    "_upgrade_icechunk_repository",
    "azure_credentials",
    "azure_from_env_credentials",
    "azure_static_credentials",
    "azure_storage",
    "containers_credentials",
    "gcs_credentials",
    "gcs_from_env_credentials",
    "gcs_refreshable_credentials",
    "gcs_static_credentials",
    "gcs_storage",
    "gcs_store",
    "http_storage",
    "http_store",
    "in_memory_storage",
    "initialize_logs",
    "local_filesystem_storage",
    "local_filesystem_store",
    "print_debug_info",
    "r2_storage",
    "redirect_storage",
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

ManifestSplitValues: TypeAlias = dict[
    ManifestSplitDimCondition.Axis
    | ManifestSplitDimCondition.DimensionName
    | ManifestSplitDimCondition.Any,
    int,
]
SplitSizesDict: TypeAlias = dict[
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


def upgrade_icechunk_repository(
    repo: Repository, *, dry_run: bool = True, delete_unused_v1_files: bool = True
) -> None:
    """
    Migrate a repository to the latest version of Icechunk.

    This is an administrative operation, and must be executed in isolation from
    other readers and writers. Other processes running concurrently on the same
    repo may see undefined behavior.

    At this time, this function supports only migration from Icechunk spec version 1
    to Icechunk spec version 2. This means Icechunk versions 1.x to 2.x.

    The operation is usually fast, but it can take several minutes if there is a very
    large version history (thousands of snapshots).
    """
    _upgrade_icechunk_repository(
        repo._repository, dry_run=dry_run, delete_unused_v1_files=delete_unused_v1_files
    )


ManifestSplittingConfig.from_dict = staticmethod(from_dict)  # type: ignore[method-assign]
ManifestSplittingConfig.to_dict = to_dict  # type: ignore[method-assign,assignment]

initialize_logs()
