import abc
import datetime
from collections.abc import AsyncGenerator
from enum import Enum

class S3Options:
    def __init__(
        self,
        region: str | None = None,
        endpoint_url: str | None = None,
        allow_http: bool = False,
        anonymous: bool = False,
    ) -> None: ...

class ObjectStoreConfig:
    class InMemory:
        def __init__(self) -> None: ...

    class LocalFileSystem:
        def __init__(self, path: str) -> None: ...

    class S3Compatible:
        def __init__(self, options: S3Options) -> None: ...

    class S3:
        def __init__(self, options: S3Options) -> None: ...

    class Gcs:
        def __init__(self) -> None: ...

    class Azure:
        def __init__(self) -> None: ...

    class Tigris:
        def __init__(self) -> None: ...

AnyObjectStoreConfig = (
    ObjectStoreConfig.InMemory
    | ObjectStoreConfig.LocalFileSystem
    | ObjectStoreConfig.S3
    | ObjectStoreConfig.S3Compatible
    | ObjectStoreConfig.Gcs
    | ObjectStoreConfig.Azure
    | ObjectStoreConfig.Tigris
)

class VirtualChunkContainer:
    name: str
    url_prefix: str
    store: ObjectStoreConfig

    def __init__(self, name: str, url_prefix: str, store: AnyObjectStoreConfig): ...

class CompressionAlgorithm(Enum):
    """Enum for selecting the compression algorithm used by Icechunk to write its metadata files"""

    Zstd = 0

    @staticmethod
    def default() -> CompressionAlgorithm: ...

class CompressionConfig:
    """Configuration for how Icechunk compresses its metadata files"""
    @property
    def algorithm(self) -> CompressionAlgorithm: ...
    @algorithm.setter
    def algorithm(self, value: CompressionAlgorithm) -> None: ...
    @property
    def level(self) -> int: ...
    @level.setter
    def level(self, value: int) -> None: ...
    @staticmethod
    def default() -> CompressionConfig: ...

class CachingConfig:
    """Configuration for how Icechunk caches its metadata files"""
    @property
    def snapshots_cache_size(self) -> int: ...
    @snapshots_cache_size.setter
    def snapshots_cache_size(self, value: int) -> None: ...
    @property
    def manifests_cache_size(self) -> int: ...
    @manifests_cache_size.setter
    def manifests_cache_size(self, value: int) -> None: ...
    @property
    def transactions_cache_size(self) -> int: ...
    @transactions_cache_size.setter
    def transactions_cache_size(self, value: int) -> None: ...
    @property
    def attributes_cache_size(self) -> int: ...
    @attributes_cache_size.setter
    def attributes_cache_size(self, value: int) -> None: ...
    @property
    def chunks_cache_size(self) -> int: ...
    @chunks_cache_size.setter
    def chunks_cache_size(self, value: int) -> None: ...
    @staticmethod
    def default() -> CachingConfig: ...

class StorageConcurrencySettings:
    """Configuration for how Icechunk uses its Storage instance"""

    @property
    def max_concurrent_requests_for_object(self) -> int: ...
    @max_concurrent_requests_for_object.setter
    def max_concurrent_requests_for_object(self, value: int) -> None: ...
    @property
    def ideal_concurrent_request_size(self) -> int: ...
    @ideal_concurrent_request_size.setter
    def ideal_concurrent_request_size(self, value: int) -> None: ...

class StorageSettings:
    """Configuration for how Icechunk uses its Storage instance"""

    @property
    def concurrency(self) -> StorageConcurrencySettings: ...
    @concurrency.setter
    def concurrency(self, value: StorageConcurrencySettings) -> None: ...

class RepositoryConfig:
    """Configuration for an Icechunk repository"""

    @staticmethod
    def default() -> RepositoryConfig: ...
    @property
    def inline_chunk_threshold_bytes(self) -> int: ...
    @inline_chunk_threshold_bytes.setter
    def inline_chunk_threshold_bytes(self, value: int) -> None: ...
    @property
    def unsafe_overwrite_refs(self) -> bool: ...
    @unsafe_overwrite_refs.setter
    def unsafe_overwrite_refs(self, value: bool) -> None: ...
    @property
    def get_partial_values_concurrency(self) -> int: ...
    @get_partial_values_concurrency.setter
    def get_partial_values_concurrency(self, value: int) -> None: ...
    @property
    def compression(self) -> CompressionConfig: ...
    @compression.setter
    def compression(self, value: CompressionConfig) -> None: ...
    @property
    def caching(self) -> CachingConfig: ...
    @caching.setter
    def caching(self, value: CachingConfig) -> None: ...
    @property
    def storage(self) -> Storage: ...
    @storage.setter
    def storage(self, value: Storage) -> None: ...
    @property
    def virtual_chunk_containers(self) -> dict[str, VirtualChunkContainer]: ...
    @virtual_chunk_containers.setter
    def virtual_chunk_containers(
        self, value: dict[str, VirtualChunkContainer]
    ) -> None: ...
    def set_virtual_chunk_container(self, cont: VirtualChunkContainer) -> None: ...
    def clear_virtual_chunk_containers(self) -> None: ...

class PyRepository:
    @classmethod
    def create(
        cls,
        storage: Storage,
        *,
        config: RepositoryConfig | None = None,
        virtual_chunk_credentials: dict[str, AnyCredential] | None = None,
    ) -> PyRepository: ...
    @classmethod
    def open(
        cls,
        storage: Storage,
        *,
        config: RepositoryConfig | None = None,
        virtual_chunk_credentials: dict[str, AnyCredential] | None = None,
    ) -> PyRepository: ...
    @classmethod
    def open_or_create(
        cls,
        storage: Storage,
        *,
        config: RepositoryConfig | None = None,
        virtual_chunk_credentials: dict[str, AnyCredential] | None = None,
    ) -> PyRepository: ...
    @staticmethod
    def exists(storage: Storage) -> bool: ...
    @staticmethod
    def fetch_config(storage: Storage) -> RepositoryConfig | None: ...
    def save_config(self) -> None: ...
    def ancestry(self, snapshot_id: str) -> list[SnapshotMetadata]: ...
    def create_branch(self, branch: str, snapshot_id: str) -> None: ...
    def list_branches(self) -> set[str]: ...
    def lookup_branch(self, branch: str) -> str: ...
    def reset_branch(self, branch: str, snapshot_id: str) -> None: ...
    def delete_branch(self, branch: str) -> None: ...
    def create_tag(self, tag: str, snapshot_id: str) -> None: ...
    def list_tags(self) -> set[str]: ...
    def lookup_tag(self, tag: str) -> str: ...
    def readonly_session(
        self,
        *,
        branch: str | None = None,
        tag: str | None = None,
        snapshot_id: str | None = None,
    ) -> PySession: ...
    def writable_session(self, branch: str) -> PySession: ...

class PySession:
    @classmethod
    def from_bytes(cls, data: bytes) -> PySession: ...
    def __eq__(self, value: object) -> bool: ...
    def as_bytes(self) -> bytes: ...
    @property
    def read_only(self) -> bool: ...
    @property
    def snapshot_id(self) -> str: ...
    @property
    def branch(self) -> str | None: ...
    @property
    def has_uncommitted_changes(self) -> bool: ...
    def discard_changes(self) -> None: ...
    def all_virtual_chunk_locations(self) -> list[str]: ...
    @property
    def store(self) -> PyStore: ...
    def merge(self, other: PySession) -> None: ...
    def commit(self, message: str) -> str: ...
    def rebase(self, solver: ConflictSolver) -> None: ...

class PyStore:
    @classmethod
    def from_bytes(cls, data: bytes) -> PyStore: ...
    def __eq__(self, value: object) -> bool: ...
    @property
    def read_only(self) -> bool: ...
    @property
    def session(self) -> PySession: ...
    def as_bytes(self) -> bytes: ...
    async def is_empty(self, prefix: str) -> bool: ...
    async def clear(self) -> None: ...
    def sync_clear(self) -> None: ...
    async def get(
        self, key: str, byte_range: tuple[int | None, int | None] | None = None
    ) -> bytes: ...
    async def get_partial_values(
        self, key_ranges: list[tuple[str, tuple[int | None, int | None]]]
    ) -> list[bytes]: ...
    async def exists(self, key: str) -> bool: ...
    @property
    def supports_writes(self) -> bool: ...
    @property
    def supports_deletes(self) -> bool: ...
    async def set(self, key: str, value: bytes) -> None: ...
    async def set_if_not_exists(self, key: str, value: bytes) -> None: ...
    def set_virtual_ref(
        self,
        key: str,
        location: str,
        offset: int,
        length: int,
        checksum: str | datetime.datetime | None = None,
    ) -> None: ...
    async def async_set_virtual_ref(
        self,
        key: str,
        location: str,
        offset: int,
        length: int,
        checksum: str | datetime.datetime | None = None,
    ) -> None: ...
    async def delete(self, key: str) -> None: ...
    async def delete_dir(self, prefix: str) -> None: ...
    @property
    def supports_partial_writes(self) -> bool: ...
    async def set_partial_values(
        self, key_start_values: list[tuple[str, int, bytes]]
    ) -> None: ...
    @property
    def supports_listing(self) -> bool: ...
    def list(self) -> PyAsyncStringGenerator: ...
    def list_prefix(self, prefix: str) -> PyAsyncStringGenerator: ...
    def list_dir(self, prefix: str) -> PyAsyncStringGenerator: ...

class PyAsyncStringGenerator(AsyncGenerator[str, None], metaclass=abc.ABCMeta):
    def __aiter__(self) -> PyAsyncStringGenerator: ...
    async def __anext__(self) -> str: ...

class SnapshotMetadata:
    """Metadata for a snapshot"""
    @property
    def id(self) -> str:
        """The snapshot ID"""
        ...
    @property
    def written_at(self) -> datetime.datetime:
        """
        The timestamp when the snapshot was written
        """
        ...
    @property
    def message(self) -> str:
        """
        The commit message of the snapshot
        """
        ...

class PyAsyncSnapshotGenerator(
    AsyncGenerator[SnapshotMetadata, None], metaclass=abc.ABCMeta
):
    def __aiter__(self) -> PyAsyncSnapshotGenerator: ...
    async def __anext__(self) -> SnapshotMetadata: ...

class S3StaticCredentials:
    access_key_id: str
    secret_access_key: str
    session_token: str | None
    expires_after: datetime.datetime | None

    def __init__(
        self,
        access_key_id: str,
        secret_access_key: str,
        session_token: str | None = None,
        expires_after: datetime.datetime | None = None,
    ): ...

class S3Credentials:
    class FromEnv:
        def __init__(self) -> None: ...

    class Anonymous:
        def __init__(self) -> None: ...

    class Static:
        def __init__(self, credentials: S3StaticCredentials) -> None: ...

    class Refreshable:
        def __init__(self, pickled_function: bytes) -> None: ...

AnyS3Credential = (
    S3Credentials.Static
    | S3Credentials.Anonymous
    | S3Credentials.FromEnv
    | S3Credentials.Refreshable
)

class GcsStaticCredentials:
    class ServiceAccount:
        def __init__(self, path: str) -> None: ...

    class ServiceAccountKey:
        def __init__(self, key: str) -> None: ...

    class ApplicationCredentials:
        def __init__(self, path: str) -> None: ...

AnyGcsStaticCredential = (
    GcsStaticCredentials.ServiceAccount
    | GcsStaticCredentials.ServiceAccountKey
    | GcsStaticCredentials.ApplicationCredentials
)

class GcsCredentials:
    class FromEnv:
        def __init__(self) -> None: ...

    class Static:
        def __init__(self, credentials: AnyGcsStaticCredential) -> None: ...

AnyGcsCredential = GcsCredentials.FromEnv | GcsCredentials.Static

class AzureStaticCredentials:
    class AccessKey:
        def __init__(self, key: str) -> None: ...

    class SasToken:
        def __init__(self, token: str) -> None: ...

    class BearerToken:
        def __init__(self, token: str) -> None: ...

AnyAzureStaticCredential = (
    AzureStaticCredentials.AccessKey
    | AzureStaticCredentials.SasToken
    | AzureStaticCredentials.BearerToken
)

class AzureCredentials:
    class FromEnv:
        def __init__(self) -> None: ...

    class Static:
        def __init__(self, credentials: AnyAzureStaticCredential) -> None: ...

AnyAzureCredential = AzureCredentials.FromEnv | AzureCredentials.Static

class Credentials:
    class S3:
        def __init__(self, credentials: AnyS3Credential) -> None: ...

    class Gcs:
        def __init__(self, credentials: GcsCredentials) -> None: ...

    class Azure:
        def __init__(self, credentials: AzureCredentials) -> None: ...

AnyCredential = Credentials.S3 | Credentials.Gcs | Credentials.Azure

class Storage:
    """Storage configuration for an IcechunkStore

    Currently supports memory, filesystem, and S3 storage backends.
    Use the class methods to create a StorageConfig object with the desired backend.

    Ex:
    ```
    storage_config = StorageConfig.memory("prefix")
    storage_config = StorageConfig.filesystem("/path/to/root")
    storage_config = StorageConfig.object_store("s3://bucket/prefix", vec!["my", "options"])
    storage_config = StorageConfig.s3_from_env("bucket", "prefix")
    storage_config = StorageConfig.s3_from_config("bucket", "prefix", ...)
    ```
    """

    @classmethod
    def new_s3(
        cls,
        config: S3Options,
        bucket: str,
        prefix: str | None,
        credentials: AnyS3Credential | None = None,
    ) -> Storage: ...
    @classmethod
    def new_in_memory(cls) -> Storage: ...
    @classmethod
    def new_local_filesystem(cls, path: str) -> Storage: ...
    @classmethod
    def new_gcs(
        cls,
        bucket: str,
        prefix: str | None,
        credentials: AnyGcsCredential | None = None,
        *,
        config: dict[str, str] | None = None,
    ) -> Storage: ...
    @classmethod
    def new_azure_blob(
        cls,
        container: str,
        prefix: str,
        credentials: AnyAzureCredential | None = None,
        *,
        config: dict[str, str] | None = None,
    ) -> Storage: ...

class VersionSelection(Enum):
    """Enum for selecting the which version of a conflict"""

    Fail = 0
    UseOurs = 1
    UseTheirs = 2

class ConflictSolver:
    """An abstract conflict solver that can be used to detect or resolve conflicts between two stores

    This should never be used directly, but should be subclassed to provide specific conflict resolution behavior
    """

    ...

class BasicConflictSolver(ConflictSolver):
    """A basic conflict solver that allows for simple configuration of resolution behavior

    This conflict solver allows for simple configuration of resolution behavior for conflicts that may occur during a rebase operation.
    It will attempt to resolve a limited set of conflicts based on the configuration options provided.

    - When a user attribute conflict is encountered, the behavior is determined by the `on_user_attributes_conflict` option
    - When a chunk conflict is encountered, the behavior is determined by the `on_chunk_conflict` option
    - When an array is deleted that has been updated, `fail_on_delete_of_updated_array` will determine whether to fail the rebase operation
    - When a group is deleted that has been updated, `fail_on_delete_of_updated_group` will determine whether to fail the rebase operation
    """

    def __init__(
        self,
        *,
        on_user_attributes_conflict: VersionSelection = VersionSelection.UseOurs,
        on_chunk_conflict: VersionSelection = VersionSelection.UseOurs,
        fail_on_delete_of_updated_array: bool = False,
        fail_on_delete_of_updated_group: bool = False,
    ) -> None:
        """Create a BasicConflictSolver object with the given configuration options
        Parameters:
        on_user_attributes_conflict: VersionSelection
            The behavior to use when a user attribute conflict is encountered, by default VersionSelection.use_ours()
        on_chunk_conflict: VersionSelection
            The behavior to use when a chunk conflict is encountered, by default VersionSelection.use_theirs()
        fail_on_delete_of_updated_array: bool
            Whether to fail when a chunk is deleted that has been updated, by default False
        fail_on_delete_of_updated_group: bool
            Whether to fail when a group is deleted that has been updated, by default False
        """
        ...

class ConflictDetector(ConflictSolver):
    """A conflict solver that can be used to detect conflicts between two stores, but does not resolve them

    Where the `BasicConflictSolver` will attempt to resolve conflicts, the `ConflictDetector` will only detect them. This means
    that during a rebase operation the `ConflictDetector` will raise a `RebaseFailed` error if any conflicts are detected, and
    allow the rebase operation to be retried with a different conflict resolution strategy. Otherwise, if no conflicts are detected
    the rebase operation will succeed.
    """

    def __init__(self) -> None: ...

class IcechunkError(Exception):
    """Base class for all Icechunk errors"""

    ...

class ConflictErrorData:
    """Data class for conflict errors. This describes the snapshot conflict detected when committing a session

    If this error is raised, it means the branch was modified and committed by another session after the session was created.
    """
    @property
    def expected_parent(self) -> str:
        """The expected parent snapshot ID.

        This is the snapshot ID that the session was based on when the
        commit operation was called.
        """
        ...
    @property
    def actual_parent(self) -> str:
        """
        The actual parent snapshot ID of the branch that the session attempted to commit to.

        When the session is based on a branch, this is the snapshot ID of the branch tip. If this
        error is raised, it means the branch was modified and committed by another session after
        the session was created.
        """
        ...

class PyConflictError(IcechunkError):
    """An error that occurs when a conflict is detected"""

    args: tuple[ConflictErrorData]
    ...

__version__: str

class ConflictType(Enum):
    """Type of conflict detected"""

    NewNodeConflictsWithExistingNode = 1
    NewNodeInInvalidGroup = 2
    ZarrMetadataDoubleUpdate = 3
    ZarrMetadataUpdateOfDeletedArray = 4
    UserAttributesDoubleUpdate = 5
    UserAttributesUpdateOfDeletedNode = 6
    ChunkDoubleUpdate = 7
    ChunksUpdatedInDeletedArray = 8
    ChunksUpdatedInUpdatedArray = 9
    DeleteOfUpdatedArray = 10
    DeleteOfUpdatedGroup = 11

class Conflict:
    """A conflict detected between snapshots"""

    @property
    def conflict_type(self) -> ConflictType:
        """The type of conflict detected"""
        ...

    @property
    def path(self) -> str:
        """The path of the node that caused the conflict"""
        ...

    @property
    def conflicted_chunks(self) -> list[list[int]] | None:
        """If the conflict is a chunk conflict, this will return the list of chunk indices that are in conflict"""
        ...

class RebaseFailedData:
    """Data class for rebase failed errors. This describes the error that occurred when rebasing a session"""

    @property
    def snapshot(self) -> str:
        """The snapshot ID that the session was rebased to"""
        ...

    @property
    def conflicts(self) -> list[Conflict]:
        """The conflicts that occurred during the rebase operation"""
        ...

class PyRebaseFailedError(IcechunkError):
    """An error that occurs when a rebase operation fails"""

    args: tuple[RebaseFailedData]
    ...
