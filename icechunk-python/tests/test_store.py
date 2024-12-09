import icechunk
import zarr


async def test_store_clear() -> None:
    store = icechunk.IcechunkStore.create(
        storage=icechunk.StorageConfig.memory("test"),
        config=icechunk.StoreConfig(inline_chunk_threshold_bytes=1),
    )

    zarr.group(store=store)
    store.commit("created node /")
    await store.clear()
    zarr.group(store=store)
    assert len([_ async for _ in store.list_prefix("/")]) == 1
