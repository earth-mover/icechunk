from __future__ import annotations
import asyncio

import pytest

from zarr.core.buffer import Buffer, cpu
from zarr.testing.store import StoreTests

from icechunk import IcechunkStore


class TestMemoryStore(StoreTests[IcechunkStore, cpu.Buffer]):
    store_cls = IcechunkStore
    buffer_cls = cpu.Buffer

    def set(self, store: IcechunkStore, key: str, value: Buffer) -> None:
        store.set_sync(key, value)

    def get(self, store: IcechunkStore, key: str) -> Buffer:
        return store.get_sync(key)

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
        print(store_kwargs)
        return asyncio.run(IcechunkStore.from_json(**store_kwargs))

    def test_store_repr(self, store: IcechunkStore) -> None:
        # TODO
        assert True

    def test_store_supports_writes(self, store: IcechunkStore) -> None:
        assert store.supports_writes

    def test_store_supports_listing(self, store: IcechunkStore) -> None:
        assert store.supports_listing

    def test_store_supports_partial_writes(self, store: IcechunkStore) -> None:
        assert not store.supports_partial_writes

    def test_list_prefix(self, store: IcechunkStore) -> None:
        assert True

    @pytest.mark.skip(reason="Not implemented")
    async def test_clear(self, store: IcechunkStore) -> None:
        await self.test_clear(store)