import pickle

import zarr

import icechunk


async def test_pickle():
    store = await icechunk.IcechunkStore.open(
        mode="w",
        storage=icechunk.StorageConfig.memory(prefix='test'),
    )

    root = zarr.group(store=store)
    array = root.ones(name="ones", shape=(10, 10), chunks=(5, 5), dtype="f8")
    array[:] = 20

    pickled = pickle.dumps(store)

    store_loaded = pickle.loads(pickled)
    root_loaded = zarr.open_group(store_loaded, mode='r')
    array_loaded = root_loaded.require_array("ones")

    assert array_loaded == array
