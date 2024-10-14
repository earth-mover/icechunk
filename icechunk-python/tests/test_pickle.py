import pickle

import icechunk
import pytest
import zarr
from zarr.storage import LocalStore


@pytest.fixture(scope="function")
async def tmp_store(tmpdir):
    store_path = f"{tmpdir}"
    store = icechunk.IcechunkStore.open(
        storage=icechunk.StorageConfig.filesystem(store_path),
        mode="w",
    )

    yield store

    store.close()


async def test_pickle(tmp_store):
    root = zarr.group(store=tmp_store)
    array = root.ones(name="ones", shape=(10, 10), chunks=(5, 5), dtype="float32")
    array[:] = 20
    tmp_store.commit("firsttt")

    pickled = pickle.dumps(tmp_store)

    store_loaded = pickle.loads(pickled)
    assert store_loaded == tmp_store

    root_loaded = zarr.open_group(store_loaded)
    array_loaded = root_loaded["ones"]

    assert type(array_loaded) is zarr.Array
    assert array_loaded == array
    assert array_loaded[0, 5] == 20


async def test_store_equality(tmpdir, tmp_store):
    assert tmp_store == tmp_store

    local_store = await LocalStore.open(f"{tmpdir}/zarr", mode="w")
    assert tmp_store != local_store

    store2 = icechunk.IcechunkStore.open(
        storage=icechunk.StorageConfig.memory(prefix="test"),
        mode="w",
    )
    assert tmp_store != store2

    store3 = icechunk.IcechunkStore.open(
        storage=icechunk.StorageConfig.filesystem(f"{tmpdir}/test"),
        mode="a",
    )
    assert tmp_store != store3

    store4 = icechunk.IcechunkStore.open(
        storage=icechunk.StorageConfig.filesystem(f"{tmpdir}/test"),
        mode="a",
    )
    assert store3 == store4
