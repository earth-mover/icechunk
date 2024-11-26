import pickle

import pytest

import icechunk
import zarr
from zarr.storage import LocalStore


@pytest.fixture(scope="function")
def tmp_store(tmpdir):
    store_path = f"{tmpdir}"
    store = icechunk.IcechunkStore.open_or_create(
        storage=icechunk.StorageConfig.filesystem(store_path),
        read_only=False,
    )

    yield store

    store.close()


def test_pickle_read_only(tmp_store):
    assert tmp_store._read_only is False

    roundtripped = pickle.loads(pickle.dumps(tmp_store))
    assert roundtripped._read_only is True

    with tmp_store.preserve_read_only():
        roundtripped = pickle.loads(pickle.dumps(tmp_store))
        assert roundtripped._read_only is False

    assert tmp_store._read_only is False


def test_pickle(tmp_store):
    root = zarr.group(store=tmp_store)
    array = root.ones(name="ones", shape=(10, 10), chunks=(5, 5), dtype="float32")
    array[:] = 20
    tmp_store.commit("firsttt")

    pickled = pickle.dumps(tmp_store)

    store_loaded = pickle.loads(pickled)
    assert store_loaded == tmp_store

    root_loaded = zarr.open_group(store_loaded, mode="r")
    array_loaded = root_loaded["ones"]

    assert type(array_loaded) is zarr.Array
    assert array_loaded == array
    assert array_loaded[0, 5] == 20


async def test_store_equality(tmpdir, tmp_store):
    assert tmp_store == tmp_store

    local_store = await LocalStore.open(f"{tmpdir}/zarr", read_only=False)
    assert tmp_store != local_store

    store2 = icechunk.IcechunkStore.open_or_create(
        storage=icechunk.StorageConfig.memory(prefix="test"),
        read_only=False,
    )
    assert tmp_store != store2

    store3 = icechunk.IcechunkStore.open_or_create(
        storage=icechunk.StorageConfig.filesystem(f"{tmpdir}/test"),
        read_only=False,
    )
    assert tmp_store != store3

    store4 = icechunk.IcechunkStore.open_or_create(
        storage=icechunk.StorageConfig.filesystem(f"{tmpdir}/test"),
        read_only=False,
    )
    assert store3 == store4
