import pickle
from pathlib import Path

import pytest

import zarr
from icechunk import Repository, local_filesystem_storage


def create_local_repo(path: str) -> Repository:
    return Repository.create(storage=local_filesystem_storage(path))


@pytest.fixture(scope="function")
def tmp_repo(tmpdir: Path) -> Repository:
    store_path = f"{tmpdir}"
    repo = create_local_repo(store_path)
    return repo


def test_pickle_read_only(tmp_repo: Repository) -> None:
    tmp_session = tmp_repo.writable_session(branch="main")
    tmp_store = tmp_session.store

    assert tmp_store._read_only is False

    roundtripped = pickle.loads(pickle.dumps(tmp_store))
    assert roundtripped._read_only is False

    # with tmp_store.preserve_read_only():
    #     roundtripped = pickle.loads(pickle.dumps(tmp_store))
    #     assert roundtripped._read_only is False

    assert tmp_store._read_only is False


def test_pickle(tmp_repo: Repository) -> None:
    tmp_session = tmp_repo.writable_session(branch="main")
    tmp_store = tmp_session.store

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
