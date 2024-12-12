import pickle

import pytest

import icechunk
import zarr
from zarr.storage import LocalStore


def create_local_repo(path: str) -> icechunk.Repository:
    return icechunk.Repository.create(
        storage=icechunk.StorageConfig.filesystem(path),
    )


@pytest.fixture(scope="function")
def tmp_store(tmpdir):
    store_path = f"{tmpdir}"
    repo = create_local_repo(store_path)

    session = repo.writeable_session("main")
    store = session.store()

    yield store


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

    memory_repo = icechunk.Repository.open_or_create(
        config=icechunk.RepositoryConfig(),
        storage=icechunk.StorageConfig.filesystem("store_path"),
    )

    memory_session = memory_repo.writeable_session(branch="main")
    memory_store = memory_session.store()
    assert tmp_store != memory_store

    repo3 = create_local_repo(f"{tmpdir}/test")
    session3 = repo3.writeable_session(branch="main")
    store3 = session3.store()
    assert tmp_store != store3
