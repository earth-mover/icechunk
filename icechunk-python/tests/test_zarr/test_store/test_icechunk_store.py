from __future__ import annotations
import asyncio

import pytest

from zarr.core.buffer import Buffer, cpu, default_buffer_prototype
from zarr.testing.store import StoreTests

from icechunk import IcechunkStore


DEFAULT_GROUP_METADATA = b'{"zarr_format":3,"node_type":"group","attributes":null}'


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
            return None

        return self.buffer_cls.from_bytes(result)

    @pytest.fixture(scope="function", params=[None, True])
    def store_kwargs(
        self, request: pytest.FixtureRequest
    ) -> dict[str, str | None | dict[str, Buffer]]:
        kwargs = {
            "config": {"storage": {"type": "in_memory"}, "dataset": {}},
            "mode": "r+",
        }
        return kwargs

    @pytest.fixture(scope="function")
    def store(self, store_kwargs: str | None | dict[str, Buffer]) -> IcechunkStore:
        return asyncio.run(IcechunkStore.from_json(**store_kwargs))

    @pytest.mark.xfail(reason="Not implemented")
    def test_store_repr(self, store: IcechunkStore) -> None:
        super().test_store_repr(store)

    def test_store_supports_writes(self, store: IcechunkStore) -> None:
        assert store.supports_writes

    def test_store_supports_listing(self, store: IcechunkStore) -> None:
        assert store.supports_listing

    def test_store_supports_partial_writes(self, store: IcechunkStore) -> None:
        assert not store.supports_partial_writes

    def test_list_prefix(self, store: IcechunkStore) -> None:
        assert True

    @pytest.mark.xfail(reason="Not implemented")
    async def test_clear(self, store: IcechunkStore) -> None:
        await store.set(
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

    @pytest.mark.xfail(reason="Invalid usage pattern with Icechunk")
    async def test_get_partial_values(
        self,
        store: IcechunkStore,
        key_ranges: list[tuple[str, tuple[int | None, int | None]]],
    ) -> None:
        await super().test_get_partial_values(store, key_ranges)

    async def test_set(self, store: IcechunkStore) -> None:
        await store.set("zarr.json", self.buffer_cls.from_bytes(DEFAULT_GROUP_METADATA))
        assert await store.exists("zarr.json")
        result = await self.get(store, "zarr.json")
        assert result.to_bytes() == DEFAULT_GROUP_METADATA

    async def test_get(self, store: IcechunkStore) -> None:
        await self.set(store, "zarr.json", self.buffer_cls.from_bytes(DEFAULT_GROUP_METADATA))
        assert await store.exists("zarr.json")
        result = await store.get("zarr.json", default_buffer_prototype())
        assert result.to_bytes() == DEFAULT_GROUP_METADATA
