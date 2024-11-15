from __future__ import annotations

from typing import Any

import pytest

from icechunk import IcechunkStore, StorageConfig
from zarr.core.buffer import Buffer, cpu, default_buffer_prototype
from zarr.core.sync import collect_aiterator
from zarr.testing.store import StoreTests

DEFAULT_GROUP_METADATA = b'{"zarr_format":3,"node_type":"group"}'
ARRAY_METADATA = (
    b'{"zarr_format":3,"node_type":"array","attributes":{"foo":42},'
    b'"shape":[2,2,2],"data_type":"int32","chunk_grid":{"name":"regular","configuration":{"chunk_shape":[1,1,1]}},'
    b'"chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},"fill_value":0,'
    b'"codecs":[{"name":"mycodec","configuration":{"foo":42}}],"storage_transformers":[{"name":"mytransformer","configuration":{"bar":43}}],"dimension_names":["x","y","t"]}'
)


class TestIcechunkStore(StoreTests[IcechunkStore, cpu.Buffer]):
    store_cls = IcechunkStore
    buffer_cls = cpu.Buffer

    async def set(self, store: IcechunkStore, key: str, value: Buffer) -> None:
        await store._store.set(key, value.to_bytes())

    async def get(self, store: IcechunkStore, key: str) -> Buffer:
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
    def store_kwargs(self, tmpdir) -> dict[str, Any]:
        kwargs = {
            "storage": StorageConfig.filesystem(f"{tmpdir}/store_test"),
            "read_only": False,
        }
        return kwargs

    @pytest.fixture
    async def store(self, store_kwargs: dict[str, Any]) -> IcechunkStore:
        return IcechunkStore.open_or_create(**store_kwargs)

    def test_store_eq(self, store: IcechunkStore, store_kwargs: dict[str, Any]) -> None:
        # check self equality
        assert store == store

        # check store equality with same inputs
        # asserting this is important for being able to compare (de)serialized stores
        store2 = self.store_cls.open_existing(**store_kwargs)
        assert store == store2

    @pytest.mark.xfail(reason="Not implemented")
    def test_store_repr(self, store: IcechunkStore) -> None:
        super().test_store_repr(store)

    @pytest.mark.parametrize("read_only", [True, False])
    async def test_store_open_read_only(
        self, store: IcechunkStore, store_kwargs: dict[str, Any], read_only: bool
    ) -> None:
        store_kwargs["read_only"] = read_only
        store = await self.store_cls.open(**store_kwargs)
        assert store._is_open
        assert store.read_only == read_only

    async def test_read_only_store_raises(
        self, store: IcechunkStore, store_kwargs: dict[str, Any]
    ) -> None:
        kwargs = {**store_kwargs, "read_only": True}
        store = await self.store_cls.open(**kwargs)
        assert store.read_only

        # set
        with pytest.raises(ValueError):
            await store.set("foo", self.buffer_cls.from_bytes(b"bar"))

        # delete
        with pytest.raises(ValueError):
            await store.delete("foo")

    async def test_set_many(self, store: IcechunkStore) -> None:
        """
        Test that a dict of key : value pairs can be inserted into the store via the
        `_set_many` method.
        """
        # This test won't work without initializing the array first
        await store.set("zarr.json", self.buffer_cls.from_bytes(ARRAY_METADATA))

        keys = [
            "zarr.json",
            "c/0",
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

    def test_store_supports_deletes(self, store: IcechunkStore) -> None:
        assert store.supports_deletes

    def test_store_supports_writes(self, store: IcechunkStore) -> None:
        assert store.supports_writes

    def test_store_supports_listing(self, store: IcechunkStore) -> None:
        assert store.supports_listing

    def test_store_supports_partial_writes(self, store: IcechunkStore) -> None:
        assert not store.supports_partial_writes

    async def test_list_prefix(self, store: IcechunkStore) -> None:
        assert True

    async def test_clear(self, store: IcechunkStore) -> None:
        await self.set(
            store,
            "foo/zarr.json",
            self.buffer_cls.from_bytes(DEFAULT_GROUP_METADATA),
        )
        await store.clear()

    async def test_exists(self, store: IcechunkStore) -> None:
        # Icechunk store does not support arbitrary keys
        with pytest.raises(ValueError):
            await store.exists("foo")
        assert not await store.exists("foo/zarr.json")

        # Icechunk store does not support arbitrary data either
        with pytest.raises(ValueError):
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

    async def test_list_dir(self, store: IcechunkStore) -> None:
        out = [k async for k in store.list_dir("")]
        assert out == []

        await store.set(
            "foo/zarr.json", self.buffer_cls.from_bytes(DEFAULT_GROUP_METADATA)
        )
        await store.set(
            "goo/zarr.json", self.buffer_cls.from_bytes(DEFAULT_GROUP_METADATA)
        )

        keys_expected = ["foo", "goo"]
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
                ("zarr.json", (0, 5)),
            ],
        )

        assert len(values) == 1
        data = values[0].to_bytes()
        assert len(data) == 5
        assert data == DEFAULT_GROUP_METADATA[:5]

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
            store, "foo/bar/zarr.json", self.buffer_cls.from_bytes(DEFAULT_GROUP_METADATA)
        )
        assert not await store.is_empty("")
        assert await store.is_empty("fo")  # codespell:ignore
        assert not await store.is_empty("foo/")
        assert not await store.is_empty("foo")
        assert await store.is_empty("spam/")

    async def test_delete_dir(self, store: IcechunkStore) -> None:
        if not store.supports_deletes:
            pytest.skip("store does not support deletes")
        await store.set("zarr.json", self.buffer_cls.from_bytes(DEFAULT_GROUP_METADATA))
        await store.set(
            "foo-bar/zarr.json", self.buffer_cls.from_bytes(DEFAULT_GROUP_METADATA)
        )
        await store.set("foo/zarr.json", self.buffer_cls.from_bytes(ARRAY_METADATA))
        await store.set("foo/c/0", self.buffer_cls.from_bytes(b"chun"))
        await store.delete_dir("foo")
        assert await store.exists("zarr.json")
        assert await store.exists("foo-bar/zarr.json")
        assert not await store.exists("foo/zarr.json")
        assert not await store.exists("foo/c/0")

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
        with pytest.raises(ValueError):
            await store.getsize("not-a-real-key")
