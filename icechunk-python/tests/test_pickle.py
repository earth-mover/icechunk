import pickle

import pytest
import zarr

import icechunk


@pytest.fixture(scope="function")
async def tmp_store(tmpdir):
    store_path = f"{tmpdir}"
    store = await icechunk.IcechunkStore.open(
        storage=icechunk.StorageConfig.filesystem(store_path),
        mode="w",
        config=icechunk.StoreConfig(inline_chunk_threshold_bytes=5),
    )

    yield store

    store.close()


async def test_pickle(tmp_store):
    root = zarr.group(store=tmp_store)
    array = root.ones(name="ones", shape=(10, 10), chunks=(5, 5), dtype="float32")
    array[:] = 20
    await tmp_store.commit("firsttt")

    pickled = pickle.dumps(tmp_store)

    store_loaded = pickle.loads(pickled)
    root_loaded = zarr.open_group(store_loaded)
    # array_loaded = root_loaded.open("ones")

    # assert array_loaded == array
