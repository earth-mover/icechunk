# module
import contextlib
from collections.abc import AsyncGenerator, AsyncIterator, Generator, Iterable
from typing import Any, Self

from icechunk._icechunk_python import (
    PyIcechunkStore,
    S3Credentials,
    SnapshotMetadata,
    StorageConfig,
    StoreConfig,
    VirtualRefConfig,
    __version__,
    pyicechunk_store_create,
    pyicechunk_store_exists,
    pyicechunk_store_from_bytes,
    pyicechunk_store_open_existing,
)
from zarr.abc.store import ByteRangeRequest, Store
from zarr.core.buffer import Buffer, BufferPrototype
from zarr.core.common import BytesLike
from zarr.core.sync import SyncMixin

__all__ = [
    "IcechunkStore",
    "S3Credentials",
    "SnapshotMetadata",
    "StorageConfig",
    "StoreConfig",
    "VirtualRefConfig",
    "__version__",
]


class IcechunkStore(Store, SyncMixin):
    _store: PyIcechunkStore
    _pickle_preserves_read_only: bool

    @classmethod
    async def open(cls, *args: Any, **kwargs: Any) -> Self:
        """This method is called by zarr-python, it's not intended for users.

        Use one of `IcechunkStore.open_existing`, `IcechunkStore.create` or `IcechunkStore.open_or_create` instead.
        """
        return cls.open_or_create(*args, **kwargs)

    @classmethod
    def open_or_create(cls, *args: Any, **kwargs: Any) -> Self:
        if "read_only" in kwargs:
            read_only = kwargs.pop("read_only")
        else:
            read_only = False

        if "storage" in kwargs:
            storage = kwargs.pop("storage")
        else:
            raise ValueError(
                "Storage configuration is required. Pass a Storage object to construct an IcechunkStore"
            )

        store = None
        if read_only:
            store = cls.open_existing(storage, read_only, *args, **kwargs)
        else:
            if pyicechunk_store_exists(storage):
                store = cls.open_existing(storage, read_only, *args, **kwargs)
            else:
                store = cls.create(storage, read_only, *args, **kwargs)

        assert store
        # We dont want to call _open() because icechunk handles the opening, etc.
        # if we have gotten this far we can mark it as open
        store._is_open = True

        return store

    def __init__(
        self,
        store: PyIcechunkStore,
        read_only: bool = False,
        *args: Any,
        **kwargs: Any,
    ):
        """Create a new IcechunkStore.

        This should not be called directly, instead use the `create`, `open_existing` or `open_or_create` class methods.
        """
        super().__init__(read_only=read_only)
        if store is None:
            raise ValueError(
                "An IcechunkStore should not be created with the default constructor, instead use either the create or open_existing class methods."
            )
        self._store = store
        self._pickle_preserves_read_only = False

    @classmethod
    def open_existing(
        cls,
        storage: StorageConfig,
        read_only: bool = False,
        config: StoreConfig | None = None,
        *args: Any,
        **kwargs: Any,
    ) -> Self:
        """Open an existing IcechunkStore from the given storage.

        If there is not store at the given location, an error will be raised.

        It is recommended to use the cached storage option for better performance. If cached=True,
        this will be configured automatically with the provided storage_config as the underlying
        storage backend.
        """
        config = config or StoreConfig()
        # We have delayed checking if the repository exists, to avoid the delay in the happy case
        # So we need to check now if open fails, to provide a nice error message
        try:
            store = pyicechunk_store_open_existing(
                storage, read_only=read_only, config=config
            )
        # TODO: we should have an exception type to catch here, for the case of non-existing repo
        except Exception as e:
            if pyicechunk_store_exists(storage):
                # if the repo exists, this is an actual error we need to raise
                raise e
            else:
                # if the repo doesn't exists, we want to point users to that issue instead
                raise ValueError(
                    "No Icechunk repository at the provided location, try opening in create mode or changing the location"
                ) from None
        return cls(store=store, read_only=read_only, args=args, kwargs=kwargs)

    @classmethod
    def create(
        cls,
        storage: StorageConfig,
        read_only: bool = False,
        config: StoreConfig | None = None,
        *args: Any,
        **kwargs: Any,
    ) -> Self:
        """Create a new IcechunkStore with the given storage configuration.

        If a store already exists at the given location, an error will be raised.
        """
        config = config or StoreConfig()
        store = pyicechunk_store_create(storage, config=config)
        return cls(store=store, read_only=read_only, args=args, kwargs=kwargs)

    def __eq__(self, value: object) -> bool:
        if not isinstance(value, self.__class__):
            return False
        return self._store == value._store

    def __getstate__(self) -> object:
        # we serialize the Rust store as bytes
        d = self.__dict__.copy()
        d["_store"] = self._store.as_bytes()
        if not self._pickle_preserves_read_only:
            d["_read_only"] = True
        return d

    def __setstate__(self, state: Any) -> None:
        # we have to deserialize the bytes of the Rust store
        read_only = state["_read_only"]
        store_repr = state["_store"]
        state["_store"] = pyicechunk_store_from_bytes(store_repr, read_only)
        self.__dict__ = state

    @contextlib.contextmanager
    def preserve_read_only(self) -> Generator[None, None, None]:
        """
        Context manager to allow unpickling this store preserving `read_only` status.
        By default, stores are set to read-only after unpickling.
        """
        try:
            self._pickle_preserves_read_only = True
            yield
        finally:
            self._pickle_preserves_read_only = False

    def as_read_only(self) -> Self:
        """Return a read-only version of this store."""
        new_store = self._store.with_read_only(read_only=True)
        return self.__class__(store=new_store, read_only=True)

    def as_writeable(self) -> Self:
        """Return a writeable version of this store."""
        new_store = self._store.with_read_only(read_only=False)
        return self.__class__(store=new_store, read_only=False)

    def set_read_only(self) -> None:
        """Set the store to read-only mode."""
        self._store.set_read_only(read_only=True)
        self._read_only = True

    def set_writeable(self) -> None:
        """Set the store to writeable mode."""
        self._store.set_read_only(read_only=False)
        self._read_only = False

    @property
    def snapshot_id(self) -> str:
        """Return the current snapshot id."""
        return self._store.snapshot_id

    def change_set_bytes(self) -> bytes:
        """Get the complete list of changes applied in this session, serialized to bytes.

        This method is useful in combination with `IcechunkStore.distributed_commit`. When a
        write session is too large to execute in a single machine, it could be useful to
        distribute it across multiple workers. Each worker can write their changes independently
        (map) and then a single commit is executed by a coordinator (reduce).

        This methods provides a way to send back to gather a "description" of the
        changes applied by a worker. Resulting bytes, together with the `change_set_bytes` of
        other workers, can be fed to `distributed_commit`.

        This API is subject to change, it will be replaced by a merge operation at the Store level.
        """
        return self._store.change_set_bytes()

    @property
    def branch(self) -> str | None:
        """Return the current branch name."""
        return self._store.branch

    def checkout(
        self,
        snapshot_id: str | None = None,
        branch: str | None = None,
        tag: str | None = None,
    ) -> None:
        """Checkout a branch, tag, or specific snapshot.

        If a branch is checked out, any following `commit` attempts will update that branch
        reference if successful. If a tag or snapshot_id are checked out, the repository
        won't allow commits.
        """
        if snapshot_id is not None:
            if branch is not None or tag is not None:
                raise ValueError(
                    "only one of snapshot_id, branch, or tag may be specified"
                )
            self._store.checkout_snapshot(snapshot_id)
            self._read_only = True
            return
        if branch is not None:
            if tag is not None:
                raise ValueError(
                    "only one of snapshot_id, branch, or tag may be specified"
                )
            self._store.checkout_branch(branch)
            self._read_only = True
            return
        if tag is not None:
            self._store.checkout_tag(tag)
            self._read_only = True
            return

        raise ValueError("a snapshot_id, branch, or tag must be specified")

    async def async_checkout(
        self,
        snapshot_id: str | None = None,
        branch: str | None = None,
        tag: str | None = None,
    ) -> None:
        """Checkout a branch, tag, or specific snapshot.

        If a branch is checked out, any following `commit` attempts will update that branch
        reference if successful. If a tag or snapshot_id are checked out, the repository
        won't allow commits.
        """
        if snapshot_id is not None:
            if branch is not None or tag is not None:
                raise ValueError(
                    "only one of snapshot_id, branch, or tag may be specified"
                )
            await self._store.async_checkout_snapshot(snapshot_id)
            self._read_only = True
            return
        if branch is not None:
            if tag is not None:
                raise ValueError(
                    "only one of snapshot_id, branch, or tag may be specified"
                )
            await self._store.async_checkout_branch(branch)
            self._read_only = True
            return
        if tag is not None:
            await self._store.async_checkout_tag(tag)
            self._read_only = True
            return

        raise ValueError("a snapshot_id, branch, or tag must be specified")

    def commit(self, message: str) -> str:
        """Commit any uncommitted changes to the store.

        This will create a new snapshot on the current branch and return
        the new snapshot id.

        This method will fail if:

        * there is no currently checked out branch
        * some other writer updated the current branch since the repository was checked out
        """
        return self._store.commit(message)

    async def async_commit(self, message: str) -> str:
        """Commit any uncommitted changes to the store.

        This will create a new snapshot on the current branch and return
        the new snapshot id.

        This method will fail if:

        * there is no currently checked out branch
        * some other writer updated the current branch since the repository was checked out
        """
        return await self._store.async_commit(message)

    def merge(self, changes: bytes) -> None:
        """Merge the changes from another store into this store.

        This will create a new snapshot on the current branch and return
        the new snapshot id.

        This method will fail if:

        * there is no currently checked out branch
        * some other writer updated the current branch since the repository was checked out

        The behavior is undefined if the stores applied conflicting changes.
        """
        return self._store.merge(changes)

    async def async_merge(self, changes: bytes) -> None:
        """Merge the changes from another store into this store.

        This will create a new snapshot on the current branch and return
        the new snapshot id.

        This method will fail if:

        * there is no currently checked out branch
        * some other writer updated the current branch since the repository was checked out

        The behavior is undefined if the stores applied conflicting changes.
        """
        return await self._store.async_merge(changes)

    @property
    def has_uncommitted_changes(self) -> bool:
        """Return True if there are uncommitted changes to the store"""
        return self._store.has_uncommitted_changes

    async def async_reset(self) -> bytes:
        """Pop any uncommitted changes and reset to the previous snapshot state.

        Returns
        -------
        bytes : The changes that were taken from the working set
        """
        return await self._store.async_reset()

    def reset(self) -> bytes:
        """Pop any uncommitted changes and reset to the previous snapshot state.

        Returns
        -------
        bytes : The changes that were taken from the working set
        """
        return self._store.reset()

    async def async_new_branch(self, branch_name: str) -> str:
        """Create a new branch pointing to the current checked out snapshot.

        This requires having no uncommitted changes.
        """
        return await self._store.async_new_branch(branch_name)

    def new_branch(self, branch_name: str) -> str:
        """Create a new branch pointing to the current checked out snapshot.

        This requires having no uncommitted changes.
        """
        return self._store.new_branch(branch_name)

    async def async_reset_branch(self, to_snapshot: str) -> None:
        """Reset the currently checked out branch to point to a different snapshot.

        This requires having no uncommitted changes.

        The snapshot id can be obtained as the result of a commit operation, but, more probably,
        as the id of one of the SnapshotMetadata objects returned by `ancestry()`

        This operation edits the repository history; it must be executed carefully.
        In particular, the current snapshot may end up being inaccessible from any
        other branches or tags.
        """
        return await self._store.async_reset_branch(to_snapshot)

    def reset_branch(self, to_snapshot: str) -> None:
        """Reset the currently checked out branch to point to a different snapshot.

        This requires having no uncommitted changes.

        The snapshot id can be obtained as the result of a commit operation, but, more probably,
        as the id of one of the SnapshotMetadata objects returned by `ancestry()`

        This operation edits the repository history, it must be executed carefully.
        In particular, the current snapshot may end up being inaccessible from any
        other branches or tags.
        """
        return self._store.reset_branch(to_snapshot)

    def tag(self, tag_name: str, snapshot_id: str) -> None:
        """Create a tag pointing to the current checked out snapshot."""
        return self._store.tag(tag_name, snapshot_id=snapshot_id)

    async def async_tag(self, tag_name: str, snapshot_id: str) -> None:
        """Create a tag pointing to the current checked out snapshot."""
        return await self._store.async_tag(tag_name, snapshot_id=snapshot_id)

    def ancestry(self) -> list[SnapshotMetadata]:
        """Get the list of parents of the current version."""
        return self._store.ancestry()

    def async_ancestry(self) -> AsyncGenerator[SnapshotMetadata, None]:
        """Get the list of parents of the current version.

        Returns
        -------
        AsyncGenerator[SnapshotMetadata, None]
        """
        return self._store.async_ancestry()

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
        except KeyError as _e:
            # Zarr python expects None to be returned if the key does not exist
            # but an IcechunkStore returns an error if the key does not exist
            return None

        return prototype.buffer.from_bytes(result)

    async def get_partial_values(
        self,
        prototype: BufferPrototype,
        key_ranges: Iterable[tuple[str, ByteRangeRequest]],
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
        result = await self._store.get_partial_values(list(key_ranges))
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

    async def async_set_virtual_ref(
        self, key: str, location: str, *, offset: int, length: int
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
        return await self._store.async_set_virtual_ref(key, location, offset, length)

    def set_virtual_ref(
        self, key: str, location: str, *, offset: int, length: int
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
        return self._store.set_virtual_ref(key, location, offset, length)

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
        return await self._store.set_partial_values(list(key_start_values))

    @property
    def supports_listing(self) -> bool:
        """Does the store support listing?"""
        return self._store.supports_listing

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
