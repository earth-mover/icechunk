# module
import json
from typing import Any, AsyncGenerator, Self
from ._icechunk_python import PyIcechunkStore, pyicechunk_store_from_json_config

from zarr.abc.store import Store
from zarr.core.buffer import Buffer, BufferPrototype
from zarr.core.common import AccessModeLiteral, BytesLike
from zarr.core.sync import SyncMixin


class IcechunkStore(Store, SyncMixin):
    _store: PyIcechunkStore

    @classmethod
    async def open(cls, *args: Any, **kwargs: Any) -> Self:
        store = await cls.from_json(*args, **kwargs)

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
        super().__init__(mode, *args, **kwargs)

        if store is None:
            raise ValueError("An IcechunkStore should not be created with the default constructor, instead use either the create or open_existing class methods.")
        self._store = store

    @staticmethod
    async def from_json(
        config: dict, mode: AccessModeLiteral = "r", *args: Any, **kwargs: Any
    ) -> Self:
        config_str = json.dumps(config)
        store = await pyicechunk_store_from_json_config(config_str)
        return IcechunkStore(store=store, mode=mode, *args, **kwargs)
    
    @staticmethod
    async def open_existing(
        storage_config: dict,
        cached: bool = True,
        approx_max_memory_bytes: int = 1_000_000,
        mode: AccessModeLiteral = "r",
        *args: Any,
        **kwargs: Any,
    ) -> Self:
        """Open an existing IcechunkStore from the given storage. 
        
        If there is not store at the given location, an error will be raised.

        It is recommended to use the cached storage option for better performance. If cached=True, 
        this will be configured automatically with the provided storage_config as the underlying
        storage backend.
        """
        config = {
            "dataset": {
                "version": {
                    "branch": "main",
                }
            }
        }

        if cached and storage_config.get("type", None) != "cached":
            config["storage"] = {
                "type": "cached",
                "approx_max_memory_bytes": approx_max_memory_bytes,
                "backend": storage_config,
            }
        else:
            config["storage"] = storage_config

        return await IcechunkStore.from_json(config, mode, *args, **kwargs)

    @staticmethod
    async def create(
        storage_config: dict,
        cached: bool = True,
        approx_max_memory_bytes: int = 1_000_000,
        inline_chunk_threshold=512,
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
        config = {
            "dataset": {
                "inline_chunk_threshold_bytes": inline_chunk_threshold,
            }
        }

        if cached and storage_config.get("type", None) != "cached":
            config["storage"] = {
                "type": "cached",
                "approx_max_memory_bytes": approx_max_memory_bytes,
                "backend": storage_config,
            }
        else:
            config["storage"] = storage_config
        
        return await IcechunkStore.from_json(config, mode, *args, **kwargs)

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
