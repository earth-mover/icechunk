# module

from icechunk._icechunk_python import (
    AzureCredentials,
    AzureStaticCredentials,
    BasicConflictSolver,
    CachingConfig,
    CompressionAlgorithm,
    CompressionConfig,
    Conflict,
    ConflictDetector,
    ConflictErrorData,
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
    ManifestPreloadCondition,
    ManifestPreloadConfig,
    ManifestShardCondition,
    ManifestShardingConfig,
    ObjectStoreConfig,
    RebaseFailedData,
    RepositoryConfig,
    S3Credentials,
    S3Options,
    S3StaticCredentials,
    ShardDimCondition,
    SnapshotInfo,
    Storage,
    StorageConcurrencySettings,
    StorageSettings,
    VersionSelection,
    VirtualChunkContainer,
    VirtualChunkSpec,
    __version__,
    initialize_logs,
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
from icechunk.session import ConflictError, RebaseFailedError, Session
from icechunk.storage import (
    AnyObjectStoreConfig,
    azure_storage,
    gcs_storage,
    in_memory_storage,
    local_filesystem_storage,
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
    "CachingConfig",
    "CompressionAlgorithm",
    "CompressionConfig",
    "Conflict",
    "ConflictDetector",
    "ConflictError",
    "ConflictErrorData",
    "ConflictSolver",
    "ConflictType",
    "Credentials",
    "Diff",
    "GCSummary",
    "GcsBearerCredential",
    "GcsCredentials",
    "GcsStaticCredentials",
    "IcechunkError",
    "IcechunkStore",
    "ManifestConfig",
    "ManifestPreloadCondition",
    "ManifestPreloadConfig",
    "ManifestShardCondition",
    "ManifestShardingConfig",
    "ObjectStoreConfig",
    "RebaseFailedData",
    "RebaseFailedError",
    "Repository",
    "RepositoryConfig",
    "S3Credentials",
    "S3Options",
    "S3StaticCredentials",
    "Session",
    "ShardDimCondition",
    "SnapshotInfo",
    "Storage",
    "StorageConcurrencySettings",
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
    "gcs_credentials",
    "gcs_from_env_credentials",
    "gcs_refreshable_credentials",
    "gcs_static_credentials",
    "gcs_storage",
    "in_memory_storage",
    "initialize_logs",
    "local_filesystem_storage",
    "print_debug_info",
    "s3_anonymous_credentials",
    "s3_credentials",
    "s3_from_env_credentials",
    "s3_refreshable_credentials",
    "s3_static_credentials",
    "s3_storage",
    "s3_store",
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


initialize_logs()
