import abc
import datetime
from typing import AsyncGenerator

class PyIcechunkStore:
    @property
    def snapshot_id(self) -> str: ...
    @property
    def branch(self) -> str | None: ...
    async def checkout_snapshot(self, snapshot_id: str) -> None: ...
    async def checkout_branch(self, branch: str) -> None: ...
    async def checkout_tag(self, tag: str) -> None: ...
    async def commit(self, message: str) -> str: ...
    @property
    def has_uncommitted_changes(self) -> bool: ...
    async def reset(self) -> None: ...
    async def new_branch(self, branch_name: str) -> str: ...
    async def tag(self, tag: str, snapshot_id: str) -> None: ...
    def ancestry(self) -> PyAsyncSnapshotGenerator: ...
    async def empty(self) -> bool: ...
    async def clear(self) -> None: ...
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
    async def set_virtual_ref(self, key: str, location: str, offset: int, length: int) -> None: ...
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
    def __eq__(self, other) -> bool: ...


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


class PyAsyncSnapshotGenerator(AsyncGenerator[SnapshotMetadata, None], metaclass=abc.ABCMeta):
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
    storage_config = StorageConfig.s3_from_env("bucket", "prefix")
    storage_config = StorageConfig.s3_from_credentials("bucket", "prefix",
    ```
    """
    class Memory:
        """Config for an in-memory storage backend"""
        prefix: str

    class Filesystem:
        """Config for a local filesystem storage backend"""
        root: str

    class S3:
        """Config for an S3 Object Storage compatible storage backend"""
        bucket: str
        prefix: str
        credentials: S3Credentials
        endpoint_url: str | None

    def __init__(self, storage: Memory | Filesystem | S3): ...

    @classmethod
    def memory(cls, prefix: str) -> StorageConfig:
        """Create a StorageConfig object for an in-memory storage backend with the given prefix"""
        ...

    @classmethod
    def filesystem(cls, root: str) -> StorageConfig:
        """Create a StorageConfig object for a local filesystem storage backend with the given root directory"""
        ...

    @classmethod
    def s3_from_env(cls, bucket: str, prefix: str, endpoint_url: str | None = None) -> StorageConfig:
        """Create a StorageConfig object for an S3 Object Storage compatible storage backend
        with the given bucket and prefix

        This assumes that the necessary credentials are available in the environment:
            AWS_ACCESS_KEY_ID,
            AWS_SECRET_ACCESS_KEY,
            AWS_SESSION_TOKEN (optional)
        """
        ...

    @classmethod
    def s3_from_credentials(cls, bucket: str, prefix: str, credentials: S3Credentials, endpoint_url: str | None) -> StorageConfig:
        """Create a StorageConfig object for an S3 Object Storage compatible storage
        backend with the given bucket, prefix, and credentials

        This method will directly use the provided credentials to authenticate with the S3 service,
        ignoring any environment variables.
        """
        ...


class S3Credentials:
    access_key_id: str
    secret_access_key: str
    session_token: str | None

    def __init__(self, access_key_id: str, secret_access_key: str, session_token: str | None = None): ...


class StoreConfig:
    # The number of concurrent requests to make when fetching partial values
    get_partial_values_concurrency: int | None
    # The threshold at which to inline chunks in the store in bytes. When set,
    # chunks smaller than this threshold will be inlined in the store. Default is
    # 512 bytes.
    inline_chunk_threshold_bytes: int | None
    # Whether to allow overwriting refs in the store. Default is False. Experimental.
    unsafe_overwrite_refs: bool | None

    def __init__(
        self,
        get_partial_values_concurrency: int | None = None,
        inline_chunk_threshold_bytes: int | None = None,
        unsafe_overwrite_refs: bool | None = None,
    ): ...


async def pyicechunk_store_exists(storage: StorageConfig) -> bool: ...
async def pyicechunk_store_create(storage: StorageConfig, config: StoreConfig) -> PyIcechunkStore: ...
async def pyicechunk_store_open_existing(storage: StorageConfig, read_only: bool, config: StoreConfig) -> PyIcechunkStore: ...
async def pyicechunk_store_from_json_config(config: str, read_only: bool) -> PyIcechunkStore: ...
