from __future__ import annotations

from typing import Any, cast

import pytest
from icechunk import IcechunkStore, StorageConfig
from zarr.abc.store import AccessMode
from zarr.core.buffer import Buffer, cpu, default_buffer_prototype
from zarr.core.common import AccessModeLiteral
from zarr.core.sync import collect_aiterator
from zarr.testing.store import StoreTests

DEFAULT_GROUP_METADATA = b'{"zarr_format":3,"node_type":"group","attributes":null}'
ARRAY_METADATA = (
    b'{"zarr_format":3,"node_type":"array","attributes":{"foo":42},'
    b'"shape":[2,2,2],"data_type":"int32","chunk_grid":{"name":"regular","configuration":{"chunk_shape":[1,1,1]}},'
    b'"chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},"fill_value":0,'
    b'"codecs":[{"name":"mycodec","configuration":{"foo":42}}],"storage_transformers":[{"name":"mytransformer","configuration":{"bar":43}}],"dimension_names":["x","y","t"]}'
)


class TestIcechunkStore(StoreTests[IcechunkStore, cpu.Buffer]):
    store_cls = IcechunkStore
    buffer_cls = cpu.Buffer

    @pytest.mark.xfail(reason="not implemented", strict=False)
    def test_store_eq(self, store: IcechunkStore, store_kwargs: dict[str, Any]) -> None:
        pass

    @pytest.mark.xfail(reason="not implemented", strict=False)
    async def test_serizalizable_store(self, store) -> None:
        pass

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

    @pytest.fixture(scope="function", params=[None, True])
    def store_kwargs(self) -> dict[str, Any]:
        kwargs = {
            "storage": StorageConfig.memory("store_test"),
            "mode": "w",
        }
        return kwargs

    @pytest.fixture(scope="function")
    async def store(self, store_kwargs: dict[str, Any]) -> IcechunkStore:
        return await IcechunkStore.open(**store_kwargs)

    @pytest.mark.xfail(reason="Not implemented")
    def test_store_repr(self, store: IcechunkStore) -> None:
        super().test_store_repr(store)

    @pytest.mark.xfail(reason="Not implemented")
    def test_serializable_store(self, store: IcechunkStore) -> None:
        super().test_serializable_store(store)

    def test_store_mode(self, store, store_kwargs: dict[str, Any]) -> None:
        assert store.mode == AccessMode.from_literal("w")
        assert not store.mode.readonly

    @pytest.mark.parametrize("mode", ["r", "r+", "a", "w", "w-"])
    async def test_store_open_mode(
        self, store_kwargs: dict[str, Any], mode: AccessModeLiteral
    ) -> None:
        store_kwargs["mode"] = mode
        try:
            store = await self.store_cls.open(**store_kwargs)
            assert store._is_open
            assert store.mode == AccessMode.from_literal(mode)
        except Exception:
            assert 'r' in mode

    async def test_not_writable_store_raises(self, store_kwargs: dict[str, Any]) -> None:
        create_kwargs = {**store_kwargs, "mode": "r"}
        with pytest.raises(ValueError):
            _store = await self.store_cls.open(**create_kwargs)

        # TODO
        # set
        # with pytest.raises(ValueError):
        #     await store.set("foo", self.buffer_cls.from_bytes(b"bar"))

        # # delete
        # with pytest.raises(ValueError):
        #     await store.delete("foo")

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

    async def test_empty(self, store: IcechunkStore) -> None:
        assert await store.empty()

        await store.set(
            "foo/zarr.json",
            self.buffer_cls.from_bytes(DEFAULT_GROUP_METADATA),
        )
        assert not await store.empty()

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

    async def test_with_mode(self, store: IcechunkStore) -> None:
        data = b"0000"
        await self.set(store, "zarr.json", self.buffer_cls.from_bytes(ARRAY_METADATA))
        await self.set(store, "c/0/0/0", self.buffer_cls.from_bytes(data))
        assert (await self.get(store, "c/0/0/0")).to_bytes() == data

        for mode in ["r", "a"]:
            mode = cast(AccessModeLiteral, mode)
            clone = store.with_mode(mode)
            # await store.close()
            await clone._ensure_open()
            assert clone.mode == AccessMode.from_literal(mode)
            assert isinstance(clone, type(store))

            # earlier writes are visible
            result = await clone.get("c/0/0/0", default_buffer_prototype())
            assert result is not None
            assert result.to_bytes() == data

            # writes to original after with_mode is visible
            await self.set(store, "c/0/0/1", self.buffer_cls.from_bytes(data))
            result = await clone.get("c/0/0/1", default_buffer_prototype())
            assert result is not None
            assert result.to_bytes() == data

            if mode == "a":
                # writes to clone is visible in the original
                await clone.set("c/0/1/0", self.buffer_cls.from_bytes(data))
                result = await clone.get("c/0/1/0", default_buffer_prototype())
                assert result is not None
                assert result.to_bytes() == data

            else:
                with pytest.raises(ValueError, match="store error: cannot write"):
                    await clone.set("c/0/1/0", self.buffer_cls.from_bytes(data))

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
