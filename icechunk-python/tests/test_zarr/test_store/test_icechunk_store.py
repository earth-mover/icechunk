from __future__ import annotations

import pickle
from pathlib import Path
from typing import Any, TypeVar

import pytest

from icechunk import IcechunkError, IcechunkStore, local_filesystem_storage
from icechunk.repository import Repository
from zarr.abc.store import OffsetByteRequest, RangeByteRequest, Store, SuffixByteRequest
from zarr.core.buffer import Buffer, cpu, default_buffer_prototype
from zarr.core.sync import _collect_aiterator, collect_aiterator
from zarr.testing.store import StoreTests
from zarr.testing.utils import assert_bytes_equal

DEFAULT_GROUP_METADATA = b'{"zarr_format":3,"node_type":"group"}'
ARRAY_METADATA = (
    b'{"zarr_format":3,"node_type":"array","attributes":{"foo":42},'
    b'"shape":[2,2,2],"data_type":"int32","chunk_grid":{"name":"regular","configuration":{"chunk_shape":[1,1,1]}},'
    b'"chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},"fill_value":0,'
    b'"codecs":[{"name":"mycodec","configuration":{"foo":42}}],"storage_transformers":[{"name":"mytransformer","configuration":{"bar":43}}],"dimension_names":["x","y","t"]}'
)

S = TypeVar("S", bound=Store)


class TestIcechunkStore(StoreTests[IcechunkStore, cpu.Buffer]):
    store_cls = IcechunkStore
    buffer_cls = cpu.Buffer

    async def set(self, store: IcechunkStore, key: str, value: Buffer) -> None:
        await store._store.set(key, value.to_bytes())

    async def get(self, store: IcechunkStore, key: str) -> Buffer | None:
        try:
            result = await store._store.get(key)
            if result is None:
                return None
        except ValueError as _e:
            # Zarr python expects None to be returned if the key does not exist
            # but an IcechunkStore returns an error if the key does not exist
            return None

        return self.buffer_cls.from_bytes(result)

    @pytest.fixture
    def store_kwargs(self, tmpdir: Path) -> dict[str, Any]:
        kwargs = {
            "storage": local_filesystem_storage(f"{tmpdir}/store_test"),
            "read_only": False,
        }
        return kwargs

    @pytest.fixture
    async def store(self, store_kwargs: dict[str, Any]) -> IcechunkStore:
        read_only = store_kwargs.pop("read_only")
        repo = Repository.open_or_create(**store_kwargs)
        if read_only:
            session = repo.readonly_session(branch="main")
        else:
            session = repo.writable_session("main")
        return session.store

    @pytest.fixture
    async def store_not_open(self, store_kwargs: dict[str, Any]) -> IcechunkStore:
        read_only = store_kwargs.pop("read_only")
        repo = Repository.open_or_create(**store_kwargs)
        if read_only:
            session = repo.readonly_session(branch="main")
        else:
            session = repo.writable_session("main")

        store = session.store
        store._is_open = False
        return store

    def test_store_eq(self, store: IcechunkStore, store_kwargs: dict[str, Any]) -> None:
        # check self equality
        assert store == store

        # check store equality with same inputs
        # asserting this is important for being able to compare (de)serialized stores
        repo = Repository.open(**store_kwargs)
        if store.read_only:
            session = repo.readonly_session(branch="main")
        else:
            session = repo.writable_session("main")
        store2 = session.store
        # stores dont point to the same session instance, so they are not equal
        assert store != store2

    @pytest.mark.skip(reason="Not implemented")
    def test_store_repr(self, store: IcechunkStore) -> None:
        pass

    @pytest.mark.parametrize("read_only", [True, False])
    async def test_store_open_read_only(
        self, store: IcechunkStore, store_kwargs: dict[str, Any], read_only: bool
    ) -> None:
        repo = Repository.open(**store_kwargs)
        if read_only:
            session = repo.readonly_session(branch="main")
        else:
            session = repo.writable_session("main")
        store = session.store
        assert store._is_open
        assert store.read_only == read_only

    async def test_read_only_store_raises(
        self, store: IcechunkStore, store_kwargs: dict[str, Any]
    ) -> None:
        kwargs = {**store_kwargs}
        repo = Repository.open(**kwargs)
        session = repo.readonly_session(branch="main")
        store = session.store

        assert store.read_only

        # set
        with pytest.raises(IcechunkError):
            await store.set("foo", self.buffer_cls.from_bytes(b"bar"))

        # delete
        with pytest.raises(IcechunkError):
            await store.delete("foo")

    @pytest.mark.skip(
        reason="icechunk requires opting-in to pickling at the session level"
    )
    def test_serializable_store(self, store: IcechunkStore) -> None:
        foo = pickle.dumps(store)
        loaded = pickle.loads(foo)
        # pickled stores dont point to the same session instance, so they are not equal
        assert loaded != store

    @pytest.mark.skip(
        reason="icechunk read-only follows the read-only flag of the session"
    )
    async def test_with_read_only_store(self, open_kwargs: dict[str, Any]) -> None:
        pass

    async def test_get_not_open(self, store_not_open: IcechunkStore) -> None:
        """
        Ensure that data can be read from the store that isn't yet open using the store.get method.
        """
        assert not store_not_open._is_open

        # create an array
        await store_not_open.set("zarr.json", self.buffer_cls.from_bytes(ARRAY_METADATA))

        data_buf = self.buffer_cls.from_bytes(b"\x01\x02\x03\x04")
        key = "c/0/0/0"
        await self.set(store_not_open, key, data_buf)
        observed = await store_not_open.get(key, prototype=default_buffer_prototype())
        assert_bytes_equal(observed, data_buf)

    async def test_get_raises(self, store: IcechunkStore) -> None:
        """
        Ensure that a ValueError is raise for invalid byte range syntax
        """
        # create an array
        await store.set("zarr.json", self.buffer_cls.from_bytes(ARRAY_METADATA))

        data_buf = self.buffer_cls.from_bytes(b"\x01\x02\x03\x04")
        await self.set(store, "c/0/0/0", data_buf)
        with pytest.raises(
            (ValueError, TypeError), match=r"Unexpected byte_range, got.*"
        ):
            await store.get(
                "c/0/0/0", prototype=default_buffer_prototype(), byte_range=(0, 2)
            )  # type: ignore[arg-type]

    @pytest.mark.xfail(reason="Not implemented")
    async def test_store_context_manager(self, open_kwargs: dict[str, Any]) -> None:
        # Test that the context manager closes the store
        with await self.store_cls.open(**open_kwargs) as store:
            assert store._is_open
            # Test trying to open an already open store
            with pytest.raises(ValueError, match="store is already open"):
                await store._open()
        assert not store._is_open

    async def test_set_many(self, store: IcechunkStore) -> None:
        """
        Test that a dict of key : value pairs can be inserted into the store via the
        `_set_many` method.
        """
        # This test won't work without initializing the array first
        await store.set("zarr.json", self.buffer_cls.from_bytes(ARRAY_METADATA))

        keys = [
            "zarr.json",
            "c/0/0/0",
            # icechunk does not allow v2 keys
            # "foo/c/0.0",
            # "foo/0/0"
        ]
        # icechunk strictly checks metadata?
        data_buf = [
            self.buffer_cls.from_bytes(k.encode() if k != "zarr.json" else ARRAY_METADATA)
            for k in keys
        ]
        store_dict = dict(zip(keys, data_buf, strict=True))
        await store._set_many(store_dict.items())
        for k, v in store_dict.items():
            assert (await self.get(store, k)).to_bytes() == v.to_bytes()

    async def test_set_not_open(self, store_not_open: IcechunkStore) -> None:
        """
        Ensure that data can be written to the store that's not yet open using the store.set method.
        """
        assert not store_not_open._is_open

        # create an array
        await store_not_open.set("zarr.json", self.buffer_cls.from_bytes(ARRAY_METADATA))

        data_buf = self.buffer_cls.from_bytes(b"\x01\x02\x03\x04")
        key = "c/0/0/0"
        await store_not_open.set(key, data_buf)
        observed = await self.get(store_not_open, key)
        assert_bytes_equal(observed, data_buf)

    async def test_set_invalid_buffer(self, store: S) -> None:
        """
        Ensure that set raises a Type or Value Error for invalid buffer arguments.
        """
        # create an array
        await store.set("zarr.json", self.buffer_cls.from_bytes(ARRAY_METADATA))

        with pytest.raises(
            (ValueError, TypeError),
            match=r"\S+\.set\(\): `value` must be a Buffer instance. Got an instance of <class 'int'> instead.",
        ):
            await store.set("c/0/0/0", 0)  # type: ignore[arg-type]

    def test_store_supports_deletes(self, store: IcechunkStore) -> None:
        assert store.supports_deletes

    def test_store_supports_writes(self, store: IcechunkStore) -> None:
        assert store.supports_writes

    def test_store_supports_listing(self, store: IcechunkStore) -> None:
        assert store.supports_listing

    def test_store_supports_partial_writes(self, store: IcechunkStore) -> None:
        assert not store.supports_partial_writes

    async def test_clear(self, store: IcechunkStore) -> None:
        await self.set(
            store,
            "foo/zarr.json",
            self.buffer_cls.from_bytes(DEFAULT_GROUP_METADATA),
        )
        await store.clear()

    async def test_exists(self, store: IcechunkStore) -> None:
        # Icechunk store does not support arbitrary keys
        with pytest.raises(IcechunkError):
            await store.exists("foo")
        assert not await store.exists("foo/zarr.json")

        # Icechunk store does not support arbitrary data either
        with pytest.raises(IcechunkError):
            await store.set("foo", self.buffer_cls.from_bytes(b"bar"))

        await store.set(
            "foo/zarr.json",
            self.buffer_cls.from_bytes(DEFAULT_GROUP_METADATA),
        )
        assert await store.exists("foo/zarr.json")

    async def test_list(self, store: IcechunkStore) -> None:
        assert [k async for k in store.list()] == []
        await store.set(
            "foo/zarr.json", self.buffer_cls.from_bytes(DEFAULT_GROUP_METADATA)
        )
        keys = [k async for k in store.list()]
        assert keys == ["foo/zarr.json"], keys

    async def test_list_prefix(self, store: S) -> None:
        """
        Test that the `list_prefix` method works as intended. Given a prefix, it should return
        all the keys in storage that start with this prefix.
        """
        prefixes = ("", "a/", "a/b/", "a/b/c/")
        data = self.buffer_cls.from_bytes(DEFAULT_GROUP_METADATA)
        fname = "zarr.json"
        store_dict = {p + fname: data for p in prefixes}

        await store._set_many(store_dict.items())

        for prefix in prefixes:
            observed = tuple(sorted([_ async for _ in store.list_prefix(prefix)]))
            expected: tuple[str, ...] = ()
            for key in store_dict:
                if key.startswith(prefix):
                    expected += (key,)
            expected = tuple(sorted(expected))
            assert observed == expected

    async def test_list_empty_path(self, store: S) -> None:
        """
        Verify that list and list_prefix work correctly when path is an empty string,
        i.e. no unwanted replacement occurs.
        """
        await store.set("foo/bar/zarr.json", self.buffer_cls.from_bytes(ARRAY_METADATA))
        data = self.buffer_cls.from_bytes(b"")
        store_dict = {
            "foo/bar/c/1/0/0": data,
            "foo/bar/c/0/0/0": data,
        }
        await store._set_many(store_dict.items())

        all_keys = sorted(list(store_dict.keys()) + ["foo/bar/zarr.json"])

        # Test list()
        observed_list = await _collect_aiterator(store.list())
        observed_list_sorted = sorted(observed_list)
        expected_list_sorted = all_keys
        assert observed_list_sorted == expected_list_sorted

        # Test list_prefix() with an empty prefix
        observed_prefix_empty = await _collect_aiterator(store.list_prefix(""))
        observed_prefix_empty_sorted = sorted(observed_prefix_empty)
        expected_prefix_empty_sorted = all_keys
        assert observed_prefix_empty_sorted == expected_prefix_empty_sorted

        # Test list_prefix() with a non-empty prefix
        observed_prefix = await _collect_aiterator(store.list_prefix("foo/bar/"))
        observed_prefix_sorted = sorted(observed_prefix)
        expected_prefix_sorted = sorted(k for k in all_keys if k.startswith("foo/bar/"))
        assert observed_prefix_sorted == expected_prefix_sorted

    async def test_list_dir(self, store: IcechunkStore) -> None:
        out = [k async for k in store.list_dir("")]
        assert out == []

        await store.set("zarr.json", self.buffer_cls.from_bytes(DEFAULT_GROUP_METADATA))

        await store.set(
            "foo/zarr.json", self.buffer_cls.from_bytes(DEFAULT_GROUP_METADATA)
        )
        await store.set(
            "goo/zarr.json", self.buffer_cls.from_bytes(DEFAULT_GROUP_METADATA)
        )

        keys_expected = ["foo", "goo", "zarr.json"]
        keys_observed = [k async for k in store.list_dir("")]
        assert set(keys_observed) == set(keys_expected)

        keys_expected = ["zarr.json"]
        keys_observed = [k async for k in store.list_dir("foo")]
        assert set(keys_observed) == set(keys_expected)

    async def test_delete(self, store: IcechunkStore) -> None:
        await store.set(
            "foo/zarr.json", self.buffer_cls.from_bytes(DEFAULT_GROUP_METADATA)
        )
        assert await store.exists("foo/zarr.json")
        await store.delete("foo/zarr.json")
        assert not await store.exists("foo/zarr.json")

    async def test_delete_nonexistent_key_does_not_raise(
        self, store: IcechunkStore
    ) -> None:
        if not store.supports_deletes:
            pytest.skip("store does not support deletes")
        await store.delete("zarr.json")

    async def test_get_partial_values(
        self,
        store: IcechunkStore,
    ) -> None:
        await self.set(
            store, "zarr.json", self.buffer_cls.from_bytes(DEFAULT_GROUP_METADATA)
        )
        # read back just part of it
        values = await store.get_partial_values(
            default_buffer_prototype(),
            [
                ("zarr.json", RangeByteRequest(0, 5)),
                ("zarr.json", SuffixByteRequest(5)),
                ("zarr.json", OffsetByteRequest(10)),
            ],
        )

        assert len(values) == 3
        data = values[0].to_bytes()
        assert len(data) == 5
        assert data == DEFAULT_GROUP_METADATA[:5]

        data = values[1].to_bytes()
        assert len(data) == len(DEFAULT_GROUP_METADATA) - 5
        assert data == DEFAULT_GROUP_METADATA[:-5]

        data = values[2].to_bytes()
        assert len(data) == len(DEFAULT_GROUP_METADATA) - 10
        assert data == DEFAULT_GROUP_METADATA[10:]

    async def test_set(self, store: IcechunkStore) -> None:
        await store.set("zarr.json", self.buffer_cls.from_bytes(DEFAULT_GROUP_METADATA))
        assert await store.exists("zarr.json")
        result = await self.get(store, "zarr.json")
        assert result.to_bytes() == DEFAULT_GROUP_METADATA

    async def test_get(self, store: IcechunkStore) -> None:
        await self.set(
            store, "zarr.json", self.buffer_cls.from_bytes(DEFAULT_GROUP_METADATA)
        )
        assert await store.exists("zarr.json")
        result = await store.get("zarr.json", default_buffer_prototype())
        assert result is not None
        assert result.to_bytes() == DEFAULT_GROUP_METADATA

    async def test_get_many(self, store: IcechunkStore) -> None:
        """
        Ensure that multiple keys can be retrieved at once with the _get_many method.
        """
        await store.set("zarr.json", self.buffer_cls.from_bytes(ARRAY_METADATA))

        keys = [
            "c/0/0/0",
            "c/0/0/1",
            "c/0/1/0",
            "c/0/1/1",
            "c/1/0/0",
            "c/1/0/1",
            "c/1/1/0",
            "c/1/1/1",
        ]
        values = [bytes(i) for i, _ in enumerate(keys)]
        for k, v in zip(keys, values, strict=False):
            await self.set(store, k, self.buffer_cls.from_bytes(v))
        observed_buffers = collect_aiterator(
            store._get_many(
                zip(
                    keys,
                    (default_buffer_prototype(),) * len(keys),
                    (None,) * len(keys),
                    strict=False,
                )
            )
        )
        observed_kvs = sorted(((k, b.to_bytes()) for k, b in observed_buffers))  # type: ignore[union-attr]
        expected_kvs = sorted(((k, b) for k, b in zip(keys, values, strict=False)))
        assert observed_kvs == expected_kvs

    async def test_set_if_not_exists(self, store: IcechunkStore) -> None:
        key = "zarr.json"
        data_buf = self.buffer_cls.from_bytes(ARRAY_METADATA)
        await self.set(store, key, data_buf)

        new = self.buffer_cls.from_bytes(b"1111")

        # no error even though the data is invalid and the metadata exists
        await store.set_if_not_exists(key, new)

        result = await store.get(key, default_buffer_prototype())
        assert result == data_buf

        await store.set_if_not_exists("c/0/0/0", new)  # no error

        result = await store.get("c/0/0/0", default_buffer_prototype())
        assert result == new

    async def test_is_empty(self, store: IcechunkStore) -> None:
        assert await store.is_empty("")
        await self.set(
            store, "zarr.json", self.buffer_cls.from_bytes(DEFAULT_GROUP_METADATA)
        )
        await self.set(
            store, "foo/zarr.json", self.buffer_cls.from_bytes(DEFAULT_GROUP_METADATA)
        )
        await self.set(
            store, "foo/bar/zarr.json", self.buffer_cls.from_bytes(DEFAULT_GROUP_METADATA)
        )
        assert not await store.is_empty("")
        assert await store.is_empty("fo")  # codespell:ignore
        assert not await store.is_empty("foo/")
        assert not await store.is_empty("foo")
        assert await store.is_empty("spam/")

    async def test_delete_dir(self, store: IcechunkStore) -> None:
        await store.set("zarr.json", self.buffer_cls.from_bytes(DEFAULT_GROUP_METADATA))
        await store.set(
            "foo-bar/zarr.json", self.buffer_cls.from_bytes(DEFAULT_GROUP_METADATA)
        )
        await store.set("foo/zarr.json", self.buffer_cls.from_bytes(ARRAY_METADATA))
        await store.set("foo/c/0/0/0", self.buffer_cls.from_bytes(b"chun"))
        await store.delete_dir("foo")
        assert await store.exists("zarr.json")
        assert await store.exists("foo-bar/zarr.json")
        assert not await store.exists("foo/zarr.json")
        assert not await store.exists("foo/c/0/0/0")

    async def test_getsize(self, store: IcechunkStore) -> None:
        key = "k/zarr.json"
        data = self.buffer_cls.from_bytes(DEFAULT_GROUP_METADATA)
        await self.set(store, key, data)

        result = await store.getsize(key)
        assert isinstance(result, int)
        assert result == len(DEFAULT_GROUP_METADATA)

    async def test_getsize_prefix(self, store: IcechunkStore) -> None:
        prefix = "array"
        await store.set(f"{prefix}/zarr.json", self.buffer_cls.from_bytes(ARRAY_METADATA))

        keys = [
            f"{prefix}/c/0/0/0",
            f"{prefix}/c/0/0/1",
            f"{prefix}/c/0/1/0",
            f"{prefix}/c/0/1/1",
            f"{prefix}/c/1/0/0",
            f"{prefix}/c/1/0/1",
            f"{prefix}/c/1/1/0",
            f"{prefix}/c/1/1/1",
        ]
        values = [bytes(i) for i, _ in enumerate(keys)]
        for k, v in zip(keys, values, strict=False):
            await self.set(store, k, self.buffer_cls.from_bytes(v))

        result = await store.getsize_prefix(prefix)
        assert isinstance(result, int)
        assert result == sum(len(v) for v in values) + len(ARRAY_METADATA)

    async def test_getsize_raises(self, store: IcechunkStore) -> None:
        # TODO: This maybe should be a FileNotFoundError instead of an IcechunkError
        with pytest.raises(IcechunkError):
            await store.getsize("not-a-real-key")
