import numpy as np

import icechunk
import zarr

rng = np.random.default_rng(seed=12345)


async def test_store_clear_metadata_list() -> None:
    store = icechunk.IcechunkStore.create(
        storage=icechunk.StorageConfig.memory("test"),
        config=icechunk.StoreConfig(inline_chunk_threshold_bytes=1),
    )

    zarr.group(store=store)
    store.commit("created node /")
    await store.clear()
    zarr.group(store=store)
    assert len([_ async for _ in store.list_prefix("/")]) == 1


async def test_store_clear_chunk_list() -> None:
    store = icechunk.IcechunkStore.create(
        storage=icechunk.StorageConfig.memory("test"),
        config=icechunk.StoreConfig(inline_chunk_threshold_bytes=1),
    )

    array_kwargs = dict(
        name="0", shape=(1, 3, 5, 1), chunks=(1, 3, 2, 1), fill_value=-1, dtype=np.int64
    )
    group = zarr.group(store=store)
    group.create_array(**array_kwargs)
    store.commit("created node /")

    await store.clear()
    zarr.group(store=store)
    array = group.create_array(**array_kwargs)
    assert len([_ async for _ in store.list_prefix("/")]) == 2
    array[:] = rng.integers(
        low=0, high=1234, size=array_kwargs["shape"], dtype=array_kwargs["dtype"]
    )
    keys = [_ async for _ in store.list_prefix("/")]
    assert len(keys) == 2 + 3, keys
