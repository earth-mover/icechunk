import abc
import datetime
from collections.abc import AsyncGenerator, Iterable

class RepositoryConfig:
    def __init__(
        self,
        *,
        inline_chunk_threshold_bytes: int = 512,
        unsafe_overwrite_refs: bool = False,
    ) -> None: ...

class PyRepository:
    @classmethod
    def create(
        cls,
        storage: StorageConfig,
        *,
        config: RepositoryConfig | None = None,
    ) -> PyRepository: ...
    @classmethod
    def open(
        cls,
        storage: StorageConfig,
        *,
        config: RepositoryConfig | None = None,
    ) -> PyRepository: ...
    @classmethod
    def open_or_create(
        cls,
        storage: StorageConfig,
        *,
        config: RepositoryConfig | None = None,
    ) -> PyRepository: ...
    @staticmethod
    def exists(storage: StorageConfig) -> bool: ...
    def ancestry(self, snapshot_id: str) -> list[SnapshotMetadata]: ...
    def create_branch(self, branch: str, snapshot_id: str) -> None: ...
    def list_branches(self) -> list[str]: ...
    def branch_tip(self, branch: str) -> str: ...
    def reset_branch(self, branch: str, snapshot_id: str) -> None: ...
    def create_tag(self, tag: str, snapshot_id: str) -> None: ...
    def list_tags(self) -> list[str]: ...
    def tag(self, tag: str) -> str: ...
    def readonly_session(
        self,
        *,
        branch: str | None = None,
        tag: str | None = None,
        snapshot_id: str | None = None,
    ) -> PySession: ...
    def writeable_session(self, branch: str) -> PySession: ...

class PySession:
    @classmethod
    def from_bytes(cls, data: bytes) -> PySession: ...
    def __eq__(self, value: object) -> bool: ...
    def as_bytes(self) -> bytes: ...
    @property
    def id(self) -> str: ...
    @property
    def read_only(self) -> bool: ...
    @property
    def snapshot_id(self) -> str: ...
    @property
    def branch(self) -> str | None: ...
    @property
    def has_uncommitted_changes(self) -> bool: ...
    def discard_changes(self) -> None: ...
    def store(self, config: StoreConfig | None = None) -> PyStore: ...
    def merge(self, other: PySession) -> None: ...
    def commit(self, message: str) -> str: ...

class PyStore:
    @classmethod
    def from_bytes(cls, data: bytes, config: StoreConfig | None = None) -> PyStore: ...
    def __eq__(self, value: object) -> bool: ...
    @property
    def config(self) -> StoreConfig: ...
    @property
    def read_only(self) -> bool: ...
    @property
    def session_id(self) -> str: ...
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
        checksum: int | str | datetime.datetime | None = None,
    ) -> None: ...
    async def async_set_virtual_ref(
        self,
        key: str,
        location: str,
        offset: int,
        length: int,
        checksum: int | str | datetime.datetime | None = None,
    ) -> None: ...
    async def delete(self, key: str) -> None: ...
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
    @property
    def id(self) -> str: ...
    @property
    def written_at(self) -> datetime.datetime: ...
    @property
    def message(self) -> str: ...

class PyAsyncSnapshotGenerator(
    AsyncGenerator[SnapshotMetadata, None], metaclass=abc.ABCMeta
):
    def __aiter__(self) -> PyAsyncSnapshotGenerator: ...
    async def __anext__(self) -> SnapshotMetadata: ...

class StorageConfig:
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
    def memory(cls, prefix: str) -> StorageConfig:
        """Create a StorageConfig object for an in-memory storage backend with the given prefix"""
        ...

    @classmethod
    def filesystem(cls, root: str) -> StorageConfig:
        """Create a StorageConfig object for a local filesystem storage backend with the given root directory"""
        ...

    @classmethod
    def object_store(cls, url: str, options: Iterable[tuple[str, str]]) -> StorageConfig:
        """Create a StorageConfig object for an Object Storage compatible storage backend
        with the given URL and options"""
        ...

    @classmethod
    def s3_from_env(
        cls,
        bucket: str,
        prefix: str,
        endpoint_url: str | None,
        allow_http: bool = False,
        region: str | None = None,
    ) -> StorageConfig:
        """Create a StorageConfig object for an S3 Object Storage compatible storage backend
        with the given bucket and prefix

        This assumes that the necessary credentials are available in the environment:
            AWS_REGION
            AWS_ACCESS_KEY_ID,
            AWS_SECRET_ACCESS_KEY,
            AWS_SESSION_TOKEN (optional)
            AWS_ENDPOINT_URL (optional)
            AWS_ALLOW_HTTP (optional)
        """
        ...

    @classmethod
    def s3_from_config(
        cls,
        bucket: str,
        prefix: str,
        credentials: S3Credentials,
        endpoint_url: str | None,
        allow_http: bool = False,
        region: str | None = None,
    ) -> StorageConfig:
        """Create a StorageConfig object for an S3 Object Storage compatible storage
        backend with the given bucket, prefix, and configuration

        This method will directly use the provided credentials to authenticate with the S3 service,
        ignoring any environment variables.
        """
        ...

    @classmethod
    def s3_anonymous(
        cls,
        bucket: str,
        prefix: str,
        endpoint_url: str | None,
        allow_http: bool = False,
        region: str | None = None,
    ) -> StorageConfig:
        """Create a StorageConfig object for an S3 Object Storage compatible storage
        using anonymous access
        """
        ...

class S3Credentials:
    access_key_id: str
    secret_access_key: str
    session_token: str | None

    def __init__(
        self,
        access_key_id: str,
        secret_access_key: str,
        session_token: str | None = None,
    ): ...

class VirtualRefConfig:
    class S3:
        """Config for an S3 Object Storage compatible storage backend"""

        credentials: S3Credentials | None
        endpoint_url: str | None
        allow_http: bool | None
        region: str | None

    @classmethod
    def s3_from_env(cls) -> VirtualRefConfig:
        """Create a VirtualReferenceConfig object for an S3 Object Storage compatible storage backend
        with the given bucket and prefix

        This assumes that the necessary credentials are available in the environment:
            AWS_REGION or AWS_DEFAULT_REGION
            AWS_ACCESS_KEY_ID,
            AWS_SECRET_ACCESS_KEY,
            AWS_SESSION_TOKEN (optional)
            AWS_ENDPOINT_URL (optional)
            AWS_ALLOW_HTTP (optional)
        """
        ...

    @classmethod
    def s3_from_config(
        cls,
        credentials: S3Credentials,
        *,
        endpoint_url: str | None = None,
        allow_http: bool | None = None,
        region: str | None = None,
    ) -> VirtualRefConfig:
        """Create a VirtualReferenceConfig object for an S3 Object Storage compatible storage
        backend with the given bucket, prefix, and configuration

        This method will directly use the provided credentials to authenticate with the S3 service,
        ignoring any environment variables.
        """
        ...

    @classmethod
    def s3_anonymous(
        cls,
        *,
        endpoint_url: str | None = None,
        allow_http: bool | None = None,
        region: str | None = None,
    ) -> VirtualRefConfig:
        """Create a VirtualReferenceConfig object for an S3 Object Storage compatible storage
        using anonymous access
        """
        ...

class StoreConfig:
    """Configuration for an IcechunkStore"""

    def __init__(
        self,
        get_partial_values_concurrency: int | None = None,
    ):
        """Create a StoreConfig object with the given configuration options

        Parameters
        ----------
        get_partial_values_concurrency: int | None
            The number of concurrent requests to make when fetching partial values

        -------
        StoreConfig
            A StoreConfig object with the given configuration options
        """
        ...

    @classmethod
    def from_json(cls, json: str) -> StoreConfig:
        """Create a StoreConfig object from a JSON string"""
        ...

    def as_json(self) -> str:
        """Return the StoreConfig object as a JSON string"""
        ...

__version__: str
