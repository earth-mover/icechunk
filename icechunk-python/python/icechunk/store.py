from collections.abc import AsyncIterator, Iterable
from datetime import datetime
from typing import TYPE_CHECKING, Any

from icechunk._icechunk_python import PyStore, VirtualChunkSpec
from zarr.abc.store import (
    ByteRequest,
    OffsetByteRequest,
    RangeByteRequest,
    Store,
    SuffixByteRequest,
)
from zarr.core.buffer import Buffer, BufferPrototype
from zarr.core.common import BytesLike
from zarr.core.sync import SyncMixin

if TYPE_CHECKING:
    from icechunk import Session


def _byte_request_to_tuple(
    byte_request: ByteRequest | None,
) -> tuple[int | None, int | None]:
    match byte_request:
        case None:
            return (None, None)
        case RangeByteRequest(start, end):
            return (start, end)
        case OffsetByteRequest(offset):
            return (offset, None)
        case SuffixByteRequest(suffix):
            return (None, suffix)
        case _:
            raise ValueError(f"Unexpected byte_range, got {byte_request}")


class IcechunkStore(Store, SyncMixin):
    _store: PyStore
    _for_fork: bool

    def __init__(
        self,
        store: PyStore,
        for_fork: bool,
        read_only: bool | None = None,
        *args: Any,
        **kwargs: Any,
    ):
        """Create a new IcechunkStore.

        This should not be called directly, instead use the `create`, `open_existing` or `open_or_create` class methods.
        """
        read_only = read_only if read_only is not None else store.read_only
        super().__init__(read_only=read_only)
        if store is None:
            raise ValueError(
                "An IcechunkStore should not be created with the default constructor, instead use either the create or open_existing class methods."
            )
        self._store = store
        self._is_open = True
        self._for_fork = for_fork

    def __eq__(self, value: object) -> bool:
        if not isinstance(value, IcechunkStore):
            return False
        return self._store == value._store

    def __getstate__(self) -> object:
        # for read_only sessions we allow pickling, this allows distributed reads without forking
        writable = not self.session.read_only
        if writable and not self._for_fork:
            raise ValueError(
                "You must opt-in to pickle writable sessions in a distributed context "
                "using Session.fork(). "
                # link to docs
                "If you are using xarray's `Dataset.to_zarr` method to write dask arrays, "
                "please use `icechunk.xarray.to_icechunk` instead. "
            )
        d = self.__dict__.copy()
        # we serialize the Rust store as bytes
        d["_store"] = self._store.as_bytes()
        d["_for_fork"] = self._for_fork
        return d

    def __setstate__(self, state: Any) -> None:
        # we have to deserialize the bytes of the Rust store
        store_repr = state["_store"]
        state["_store"] = PyStore.from_bytes(store_repr)
        self.__dict__ = state

    def with_read_only(self, read_only: bool = False) -> Store:
        new_store = IcechunkStore(store=self._store, for_fork=False, read_only=read_only)
        new_store._is_open = False
        return new_store

    @property
    def session(self) -> "Session":
        from icechunk.session import ForkSession, Session

        if self._for_fork:
            return ForkSession(self._store.session)
        else:
            return Session(self._store.session)

    async def clear(self) -> None:
        """Clear the store.

        This will remove all contents from the current session,
        including all groups and all arrays. But it will not modify the repository history.
        """
        return await self._store.clear()

    def sync_clear(self) -> None:
        """Clear the store.

        This will remove all contents from the current session,
        including all groups and all arrays. But it will not modify the repository history.
        """
        return self._store.sync_clear()

    async def is_empty(self, prefix: str) -> bool:
        """
        Check if the directory is empty.

        Parameters
        ----------
        prefix : str
            Prefix of keys to check.

        Returns
        -------
        bool
            True if the store is empty, False otherwise.
        """
        return await self._store.is_empty(prefix)

    async def get(
        self,
        key: str,
        prototype: BufferPrototype,
        byte_range: ByteRequest | None = None,
    ) -> Buffer | None:
        """Retrieve the value associated with a given key.

        Parameters
        ----------
        key : str
        byte_range : ByteRequest, optional

            ByteRequest may be one of the following. If not provided, all data associated with the key is retrieved.

            - RangeByteRequest(int, int): Request a specific range of bytes in the form (start, end). The end is exclusive. If the given range is zero-length or starts after the end of the object, an error will be returned. Additionally, if the range ends after the end of the object, the entire remainder of the object will be returned. Otherwise, the exact requested range will be returned.
            - OffsetByteRequest(int): Request all bytes starting from a given byte offset. This is equivalent to bytes={int}- as an HTTP header.
            - SuffixByteRequest(int): Request the last int bytes. Note that here, int is the size of the request, not the byte offset. This is equivalent to bytes=-{int} as an HTTP header.

        Returns
        -------
        Buffer
        """

        try:
            result = await self._store.get(key, _byte_request_to_tuple(byte_range))
        except KeyError as _e:
            # Zarr python expects None to be returned if the key does not exist
            # but an IcechunkStore returns an error if the key does not exist
            return None

        return prototype.buffer.from_bytes(result)

    async def get_partial_values(
        self,
        prototype: BufferPrototype,
        key_ranges: Iterable[tuple[str, ByteRequest | None]],
    ) -> list[Buffer | None]:
        """Retrieve possibly partial values from given key_ranges.

        Parameters
        ----------
        key_ranges : Iterable[tuple[str, tuple[int | None, int | None]]]
            Ordered set of key, range pairs, a key may occur multiple times with different ranges

        Returns
        -------
        list of values, in the order of the key_ranges, may contain null/none for missing keys
        """
        # NOTE: pyo3 has not implicit conversion from an Iterable to a rust iterable. So we convert it
        # to a list here first. Possible opportunity for optimization.
        ranges = [(k[0], _byte_request_to_tuple(k[1])) for k in key_ranges]
        result = await self._store.get_partial_values(list(ranges))
        return [prototype.buffer.from_bytes(r) for r in result]

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
        if not isinstance(value, Buffer):
            raise TypeError(
                f"IcechunkStore.set(): `value` must be a Buffer instance. Got an instance of {type(value)} instead."
            )
        return await self._store.set(key, value.to_bytes())

    async def set_if_not_exists(self, key: str, value: Buffer) -> None:
        """
        Store a key to ``value`` if the key is not already present.

        Parameters
        -----------
        key : str
        value : Buffer
        """
        return await self._store.set_if_not_exists(key, value.to_bytes())

    def set_virtual_ref(
        self,
        key: str,
        location: str,
        *,
        offset: int,
        length: int,
        checksum: str | datetime | None = None,
        validate_container: bool = False,
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
        checksum : str | datetime | None
            The etag or last_medified_at field of the object
        validate_container: bool
            If set to true, fail for locations that don't match any existing virtual chunk container
        """
        return self._store.set_virtual_ref(
            key, location, offset, length, checksum, validate_container
        )

    def set_virtual_refs(
        self,
        array_path: str,
        chunks: list[VirtualChunkSpec],
        *,
        validate_containers: bool = False,
    ) -> list[tuple[int, ...]] | None:
        """Store multiple virtual references for the same array.

        Parameters
        ----------
        array_path : str
            The path to the array inside the Zarr store. Example: "/groupA/groupB/outputs/my-array"
        chunks : list[VirtualChunkSpec],
            The list of virtual chunks to add
        validate_containers: bool
            If set to true, ignore virtual references for locations that don't match any existing virtual chunk container


        Returns
        -------
        list[tuple[int, ...]] | None

            If all virtual references where successfully updated, it returns None.
            If there were validation errors, it returns the chunk indices of all failed references.
        """
        return self._store.set_virtual_refs(array_path, chunks, validate_containers)

    async def delete(self, key: str) -> None:
        """Remove a key from the store

        Parameters
        ----------
        key : str
        """
        return await self._store.delete(key)

    async def delete_dir(self, prefix: str) -> None:
        """Delete a prefix

        Parameters
        ----------
        prefix : str
        """
        return await self._store.delete_dir(prefix)

    @property
    def supports_partial_writes(self) -> bool:
        """Does the store support partial writes?"""
        return self._store.supports_partial_writes

    async def set_partial_values(
        self, key_start_values: Iterable[tuple[str, int, BytesLike]]
    ) -> None:
        """Store values at a given key, starting at byte range_start.

        Parameters
        ----------
        key_start_values : list[tuple[str, int, BytesLike]]
            set of key, range_start, values triples, a key may occur multiple times with different
            range_starts, range_starts (considering the length of the respective values) must not
            specify overlapping ranges for the same key
        """
        # NOTE: pyo3 does not implicit conversion from an Iterable to a rust iterable. So we convert it
        # to a list here first. Possible opportunity for optimization.
        # NOTE: currently we only implement the case where the values are bytes
        return await self._store.set_partial_values(list(key_start_values))  # type: ignore[arg-type]

    @property
    def supports_listing(self) -> bool:
        """Does the store support listing?"""
        return self._store.supports_listing

    @property
    def supports_consolidated_metadata(self) -> bool:
        return self._store.supports_consolidated_metadata

    @property
    def supports_deletes(self) -> bool:
        return self._store.supports_deletes

    def list(self) -> AsyncIterator[str]:
        """Retrieve all keys in the store.

        Returns
        -------
        AsyncIterator[str, None]
        """
        # This method should be async, like overridden methods in child classes.
        # However, that's not straightforward:
        # https://stackoverflow.com/questions/68905848

        # The zarr spec specefies that that this and other
        # listing methods should not be async, so we need to
        # wrap the async method in a sync method.
        return self._store.list()

    def list_prefix(self, prefix: str) -> AsyncIterator[str]:
        """Retrieve all keys in the store that begin with a given prefix. Keys are returned relative
        to the root of the store.

        Parameters
        ----------
        prefix : str

        Returns
        -------
        AsyncIterator[str, None]
        """
        # The zarr spec specefies that that this and other
        # listing methods should not be async, so we need to
        # wrap the async method in a sync method.
        return self._store.list_prefix(prefix)

    def list_dir(self, prefix: str) -> AsyncIterator[str]:
        """
        Retrieve all keys and prefixes with a given prefix and which do not contain the character
        “/” after the given prefix.

        Parameters
        ----------
        prefix : str

        Returns
        -------
        AsyncIterator[str, None]
        """
        # The zarr spec specefies that that this and other
        # listing methods should not be async, so we need to
        # wrap the async method in a sync method.
        return self._store.list_dir(prefix)

    async def getsize(self, key: str) -> int:
        return await self._store.getsize(key)

    async def getsize_prefix(self, prefix: str) -> int:
        return await self._store.getsize_prefix(prefix)
