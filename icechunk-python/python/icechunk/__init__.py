# module
import json
from typing import Any, AsyncGenerator, Self
from ._icechunk_python import (
    PyIcechunkStore,
    S3Credentials,
    pyicechunk_store_create,
    pyicechunk_store_from_json_config,
    SnapshotMetadata,
    Storage,
    StoreConfig,
    pyicechunk_store_open_existing,
    pyicechunk_store_exists,
)

from zarr.abc.store import AccessMode, Store
from zarr.core.buffer import Buffer, BufferPrototype
from zarr.core.common import AccessModeLiteral, BytesLike
from zarr.core.sync import SyncMixin

__all__ = ["IcechunkStore", "Storage", "S3Credentials", "StoreConfig"]


class IcechunkStore(Store, SyncMixin):
    _store: PyIcechunkStore

    @classmethod
    async def open(cls, *args: Any, **kwargs: Any) -> Self:
        """FIXME: Better handle the open method based on the access mode the user passed in along with the kwargs
        https://github.com/zarr-developers/zarr-python/blob/c878da2a900fc621ff23cc6d84d45cd3cb26cbed/src/zarr/abc/store.py#L24-L30
        """
        if "mode" in kwargs:
            mode = kwargs.pop("mode")
        else:
            mode = "r"

        access_mode = AccessMode.from_literal(mode)

        if "storage" in kwargs:
            storage = kwargs.pop("storage")
        else:
            raise ValueError(
                "Storage configuration is required. Pass a Storage object to construct an IcechunkStore"
            )

        store_exists = await pyicechunk_store_exists(storage)

        if access_mode.overwrite:
            if store_exists:
                raise ValueError(
                    "Store already exists and overwrite is not allowed for IcechunkStore"
                )
            store = await cls.create(storage, mode, *args, **kwargs)
        elif access_mode.create or access_mode.update:
            if store_exists:
                store = await cls.open_existing(storage, mode, *args, **kwargs)
            else:
                store = await cls.create(storage, mode, *args, **kwargs)
        else:
            store = await cls.open_existing(storage, mode, *args, **kwargs)

        # We dont want to call _open() becuase icechunk handles the opening, etc.
        # if we have gotten this far we can mark it as open
        store._is_open = True

        return store

    def __init__(
        self,
        store: PyIcechunkStore,
        mode: AccessModeLiteral = "r",
        *args: Any,
        **kwargs: Any,
    ):
        """Create a new IcechunkStore. This should not be called directly, instead use the create or open_existing class methods."""
        super().__init__(mode, *args, **kwargs)
        if store is None:
            raise ValueError(
                "An IcechunkStore should not be created with the default constructor, instead use either the create or open_existing class methods."
            )
        self._store = store

    @classmethod
    async def from_config(
        cls, config: dict, mode: AccessModeLiteral = "r", *args: Any, **kwargs: Any
    ) -> Self:
        """Create an IcechunkStore from a given configuration.

        NOTE: This is deprecated and will be removed in a future release. Use the open_existing or create methods instead.

        The configuration should be a dictionary in the following format:
        {
            "storage": {
                "type": "s3, // one of "in_memory", "local_filesystem", "s3", "cached"
                "...": "additional storage configuration"
            },
            "repository": {
                // Optional, only required if you want to open an existing repository
                "version": {
                    "branch": "main",
                },
                // The threshold at which chunks are stored inline and not written to chunk storage
                inline_chunk_threshold_bytes: 512,
            },
        }

        The following storage types are supported:
        - in_memory: store data in memory
        - local_filesystem: store data on the local filesystem
        - s3: store data on S3 compatible storage
        - cached: store data in memory with a backing storage

        The following additional configuration options are supported for each storage type:
        - in_memory: {}
        - local_filesystem: {"root": "path/to/root/directory"}
        - s3: {
            "bucket": "bucket-name",
            "prefix": "optional-prefix",
            "endpoint": "optional-end
            "access_key_id": "optional-access-key-id",
            "secret_access_key": "optional",
            "session_token": "optional",
            "endpoint": "optional"
        }
        - cached: {
            "approx_max_memory_bytes": 1_000_000,
            "backend": {
                "type": "s3",
                "...": "additional storage configuration"
            }
        }

        If opened with AccessModeLiteral "r", the store will be read-only. Otherwise the store will be writable.
        """
        config_str = json.dumps(config)
        read_only = mode == "r"
        store = await pyicechunk_store_from_json_config(config_str, read_only=read_only)
        return cls(store=store, mode=mode, args=args, kwargs=kwargs)

    @classmethod
    async def open_existing(
        cls,
        storage: Storage,
        mode: AccessModeLiteral = "r",
        *args: Any,
        **kwargs: Any,
    ) -> Self:
        """Open an existing IcechunkStore from the given storage.

        If there is not store at the given location, an error will be raised.

        It is recommended to use the cached storage option for better performance. If cached=True,
        this will be configured automatically with the provided storage_config as the underlying
        storage backend.

        If opened with AccessModeLiteral "r", the store will be read-only. Otherwise the store will be writable.
        """
        read_only = mode == "r"
        store = await pyicechunk_store_open_existing(storage, read_only=read_only)
        return cls(store=store, mode=mode, args=args, kwargs=kwargs)

    @classmethod
    async def create(
        cls,
        storage: Storage,
        mode: AccessModeLiteral = "w",
        *args: Any,
        **kwargs: Any,
    ) -> Self:
        """Create a new IcechunkStore with the given storage configuration.

        If a store already exists at the given location, an error will be raised.

        It is recommended to use the cached storage option for better performance. If cached=True,
        this will be configured automatically with the provided storage_config as the underlying
        storage backend.
        """
        store = await pyicechunk_store_create(storage)
        return cls(store=store, mode=mode, args=args, kwargs=kwargs)

    @property
    def snapshot_id(self) -> str:
        """Return the current snapshot id."""
        return self._store.snapshot_id

    @property
    def branch(self) -> str | None:
        """Return the current branch name."""
        return self._store.branch

    async def checkout(
        self,
        snapshot_id: str | None = None,
        branch: str | None = None,
        tag: str | None = None,
    ) -> None:
        """Checkout a branch, tag, or specific snapshot."""
        if snapshot_id is not None:
            if branch is not None or tag is not None:
                raise ValueError(
                    "only one of snapshot_id, branch, or tag may be specified"
                )
            return await self._store.checkout_snapshot(snapshot_id)
        if branch is not None:
            if tag is not None:
                raise ValueError(
                    "only one of snapshot_id, branch, or tag may be specified"
                )
            return await self._store.checkout_branch(branch)
        if tag is not None:
            return await self._store.checkout_tag(tag)

        raise ValueError("a snapshot_id, branch, or tag must be specified")

    async def commit(self, message: str) -> str:
        """Commit any uncommitted changes to the store.

        This will create a new snapshot on the current branch and return
        the snapshot id.
        """
        return await self._store.commit(message)

    @property
    def has_uncommitted_changes(self) -> bool:
        """Return True if there are uncommitted changes to the store"""
        return self._store.has_uncommitted_changes

    async def reset(self) -> None:
        """Discard any uncommitted changes and reset to the previous snapshot state."""
        return await self._store.reset()

    async def new_branch(self, branch_name: str) -> str:
        """Create a new branch from the current snapshot. This requires having no uncommitted changes."""
        return await self._store.new_branch(branch_name)

    async def tag(self, tag_name: str, snapshot_id: str) -> None:
        """Tag an existing snapshot with a given name."""
        return await self._store.tag(tag_name, snapshot_id=snapshot_id)

    def ancestry(self) -> AsyncGenerator[SnapshotMetadata, None]:
        """Get the list of parents of the current version.

        Returns
        -------
        AsyncGenerator[SnapshotMetadata, None]
        """
        return self._store.ancestry()

    async def empty(self) -> bool:
        """Check if the store is empty."""
        return await self._store.empty()

    async def clear(self) -> None:
        """Clear the store."""
        return await self._store.clear()

    async def get(
        self,
        key: str,
        prototype: BufferPrototype,
        byte_range: tuple[int | None, int | None] | None = None,
    ) -> Buffer | None:
        """Retrieve the value associated with a given key.

        Parameters
        ----------
        key : str
        byte_range : tuple[int, Optional[int]], optional

        Returns
        -------
        Buffer
        """
        try:
            result = await self._store.get(key, byte_range)
            if result is None:
                return None
        except ValueError as _e:
            # Zarr python expects None to be returned if the key does not exist
            # but an IcechunkStore returns an error if the key does not exist
            return None

        return prototype.buffer.from_bytes(result)

    async def get_partial_values(
        self,
        prototype: BufferPrototype,
        key_ranges: list[tuple[str, tuple[int | None, int | None]]],
    ) -> list[Buffer | None]:
        """Retrieve possibly partial values from given key_ranges.

        Parameters
        ----------
        key_ranges : list[tuple[str, tuple[int, int]]]
            Ordered set of key, range pairs, a key may occur multiple times with different ranges

        Returns
        -------
        list of values, in the order of the key_ranges, may contain null/none for missing keys
        """
        result = await self._store.get_partial_values(key_ranges)
        return [
            prototype.buffer.from_bytes(r) if r is not None else None for r in result
        ]

    async def exists(self, key: str) -> bool:
        """Check if a key exists in the store.

        Parameters
        ----------
        key : str

        Returns
        -------
        bool
        """
        return await self._store.exists(key)

    @property
    def supports_writes(self) -> bool:
        """Does the store support writes?"""
        return self._store.supports_writes

    async def set(self, key: str, value: Buffer) -> None:
        """Store a (key, value) pair.

        Parameters
        ----------
        key : str
        value : Buffer
        """
        return await self._store.set(key, value.to_bytes())

    async def set_virtual_ref(
        self, key: str, location: str, offset: int, length: int
    ) -> None:
        """Store a virtual reference to a chunk.

        Parameters
        ----------
        key : str
            The chunk to store the reference under. This is the fully qualified zarr key eg: 'array/c/0/0/0'
        location : str
            The location of the chunk in storage. This is absolute path to the chunk in storage eg: 's3://bucket/path/to/file.nc'
        offset : int
            The offset in bytes from the start of the file location in storage the chunk starts at
        length : int
            The length of the chunk in bytes, measured from the given offset
        """
        return await self._store.set_virtual_ref(key, location, offset, length)

    async def delete(self, key: str) -> None:
        """Remove a key from the store

        Parameters
        ----------
        key : strz
        """
        return await self._store.delete(key)

    @property
    def supports_partial_writes(self) -> bool:
        """Does the store support partial writes?"""
        return self._store.supports_partial_writes

    async def set_partial_values(
        self, key_start_values: list[tuple[str, int, BytesLike]]
    ) -> None:
        """Store values at a given key, starting at byte range_start.

        Parameters
        ----------
        key_start_values : list[tuple[str, int, BytesLike]]
            set of key, range_start, values triples, a key may occur multiple times with different
            range_starts, range_starts (considering the length of the respective values) must not
            specify overlapping ranges for the same key
        """
        return await self._store.set_partial_values(key_start_values)

    @property
    def supports_listing(self) -> bool:
        """Does the store support listing?"""
        return self._store.supports_listing

    @property
    def supports_deletes(self) -> bool:
        return self._store.supports_deletes

    def list(self) -> AsyncGenerator[str, None]:
        """Retrieve all keys in the store.

        Returns
        -------
        AsyncGenerator[str, None]
        """
        # The zarr spec specefies that that this and other
        # listing methods should not be async, so we need to
        # wrap the async method in a sync method.
        return self._store.list()

    def list_prefix(self, prefix: str) -> AsyncGenerator[str, None]:
        """Retrieve all keys in the store with a given prefix.

        Parameters
        ----------
        prefix : str

        Returns
        -------
        AsyncGenerator[str, None]
        """
        # The zarr spec specefies that that this and other
        # listing methods should not be async, so we need to
        # wrap the async method in a sync method.
        return self._store.list_prefix(prefix)

    def list_dir(self, prefix: str) -> AsyncGenerator[str, None]:
        """
        Retrieve all keys and prefixes with a given prefix and which do not contain the character
        “/” after the given prefix.

        Parameters
        ----------
        prefix : str

        Returns
        -------
        AsyncGenerator[str, None]
        """
        # The zarr spec specefies that that this and other
        # listing methods should not be async, so we need to
        # wrap the async method in a sync method.
        return self._store.list_dir(prefix)

    def __eq__(self, other) -> bool:
        if other is self:
            return True
        raise NotImplementedError
