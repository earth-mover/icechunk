import abc
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
    def ancestry(self) -> PyAsyncStringGenerator: ...
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


class Storage:
    """Storage configuration for an IcechunkStore

    Currently supports memory, filesystem, and S3 storage backends.
    Use the class methods to create a Storage object with the desired backend.

    Ex:
    ```
    storage = Storage.memory("prefix")
    storage = Storage.filesystem("/path/to/root")
    storage = Storage.s3_from_env("bucket", "prefix")
    storage = Storage.s3_from_creds("bucket", "prefix",
    ```
    """
    class Memory:
        """An in-memory storage backend"""
        prefix: str

    class Filesystem:
        """A local filesystem storage backend"""
        root: str

    class S3:
        """An S3 Object Storage compatible storage backend"""
        bucket: str
        prefix: str
        credentials: S3Credentials
        endpoint_url: str | None

    def __init__(self, storage: Memory | Filesystem | S3): ...

    @classmethod
    def memory(cls, prefix: str) -> Storage: ...

    @classmethod
    def filesystem(cls, root: str) -> Storage: ...

    @classmethod
    def s3_from_env(cls, bucket: str, prefix: str, endpoint_url: str | None = None) -> Storage: ...

    @classmethod
    def s3_from_creds(cls, bucket: str, prefix: str, credentials: S3Credentials, endpoint_url: str | None) -> Storage: ...


class S3Credentials:
    access_key_id: str
    secret_access_key: str
    session_token: str | None

    def __init__(self, access_key_id: str, secret_access_key: str, session_token: str | None = None): ...


async def pyicechunk_store_exists(storage: Storage) -> bool: ...
async def pyicechunk_store_create(storage: Storage) -> PyIcechunkStore: ...
async def pyicechunk_store_open_existing(storage: Storage, read_only: bool) -> PyIcechunkStore: ...
async def pyicechunk_store_from_json_config(config: str, read_only: bool) -> PyIcechunkStore: ...
