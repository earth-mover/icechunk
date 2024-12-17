import pickle

import pytest

import icechunk
import zarr
from icechunk.repository import Repository


def create_local_repo(path: str) -> icechunk.Repository:
    return icechunk.Repository.create(
        storage=icechunk.StorageConfig.filesystem(path),
    )


@pytest.fixture(scope="function")
def tmp_repo(tmpdir) -> Repository:
    store_path = f"{tmpdir}"
    repo = create_local_repo(store_path)
    return repo


def test_pickle_read_only(tmp_repo: Repository):
    tmp_session = tmp_repo.writable_session(branch="main")
    tmp_store = tmp_session.store()

    assert tmp_store._read_only is False

    roundtripped = pickle.loads(pickle.dumps(tmp_store))
    assert roundtripped._read_only is False

    # with tmp_store.preserve_read_only():
    #     roundtripped = pickle.loads(pickle.dumps(tmp_store))
    #     assert roundtripped._read_only is False

    assert tmp_store._read_only is False


def test_pickle(tmp_repo: Repository):
    tmp_session = tmp_repo.writable_session(branch="main")
    tmp_store = tmp_session.store()

    root = zarr.group(store=tmp_store)
    array = root.ones(name="ones", shape=(10, 10), chunks=(5, 5), dtype="float32")
    array[:] = 20
    tmp_session.commit("firsttt")

    pickled_store = pickle.dumps(tmp_store)
    store_loaded = pickle.loads(pickled_store)

    root_loaded = zarr.open_group(store_loaded, mode="r")
    array_loaded = root_loaded["ones"]

    assert type(array_loaded) is zarr.Array
    # pickled stores dont point to the same session instance, so they are not equal
    assert array_loaded != array
    assert array_loaded[0, 5] == 20

    pickled_session = pickle.dumps(tmp_session)
    session_loaded = pickle.loads(pickled_session)
    assert tmp_session.snapshot_id == session_loaded.snapshot_id
    assert tmp_session.branch == session_loaded.branch
