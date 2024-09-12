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

    def __init__(
        self, store: PyIcechunkStore, mode: AccessModeLiteral = "r", *args: Any, **kwargs: Any
    ):
        super().__init__(mode, *args, **kwargs)
        self._store = store

    @staticmethod
    async def from_json(config: dict, mode: AccessModeLiteral = "r", *args: Any, **kwargs: Any) -> Self:
        config_str = json.dumps(config)
        store = await pyicechunk_store_from_json_config(config_str)
        return IcechunkStore(store=store, mode=mode, *args, **kwargs)
    
    async def checkout(self, snapshot_id: str) -> None:
        '''Checkout a snapshot by its id.
        
        TODO: Support branches and tags.
        '''
        return await self._store.checkout_snapshot(snapshot_id)

    async def commit(self, update_branch_name: str, message: str) -> str:
        return await self._store.commit(update_branch_name, message)

    async def empty(self) -> bool:
        return await self._store.empty()

    async def clear(self) -> None:
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
        return self._store.supports_writes()

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
        return self._store.supports_partial_writes()

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
        return self._store.supports_listing()

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
