import pickle
import time
from pathlib import Path

import pytest

import zarr
from icechunk import (
    IcechunkError,
    Repository,
    RepositoryConfig,
    S3StaticCredentials,
    local_filesystem_storage,
    s3_storage,
)


def create_local_repo(path: str) -> Repository:
    repo = Repository.create(storage=local_filesystem_storage(path))
    repo.set_default_commit_metadata({"author": "test"})
    return repo


@pytest.fixture(scope="function")
def tmp_repo(tmpdir: Path) -> Repository:
    store_path = f"{tmpdir}"
    repo = create_local_repo(store_path)
    return repo


def test_pickle_repository(tmpdir: Path, tmp_repo: Repository) -> None:
    pickled = pickle.dumps(tmp_repo)
    roundtripped = pickle.loads(pickled)
    assert tmp_repo.list_branches() == roundtripped.list_branches()
    assert tmp_repo.default_commit_metadata() == roundtripped.default_commit_metadata()
    assert tmp_repo.default_commit_metadata() == {"author": "test"}

    storage = tmp_repo.storage
    assert (
        repr(storage)
        == f"ObjectStorage(backend=LocalFileSystemObjectStoreBackend(path={tmpdir}))"
    )


def test_pickle_read_only(tmp_repo: Repository) -> None:
    tmp_session = tmp_repo.writable_session(branch="main")
    tmp_store = tmp_session.store

    assert tmp_store._read_only is False
    assert tmp_session.read_only is False

    with pytest.raises(ValueError):
        pickle.loads(pickle.dumps(tmp_store))

    roundtripped = pickle.loads(pickle.dumps(tmp_session.fork().store))
    assert roundtripped._read_only is False

    roundtripped = pickle.loads(pickle.dumps(tmp_session.fork()))
    assert roundtripped.store._read_only is False

    with pytest.raises(ValueError):
        pickle.loads(pickle.dumps(tmp_session))
    # assert roundtripped.store._read_only is False


def get_credentials() -> S3StaticCredentials:
    return S3StaticCredentials("minio123", "minio123")


def test_pickle() -> None:
    # we test with refreshable credentials because that gave us problems in the past

    def mk_repo() -> tuple[str, Repository]:
        prefix = "test-repo__" + str(time.time())
        repo = Repository.create(
            storage=s3_storage(
                endpoint_url="http://localhost:9000",
                allow_http=True,
                force_path_style=True,
                region="us-east-1",
                bucket="testbucket",
                prefix=prefix,
                get_credentials=get_credentials,
            ),
            config=RepositoryConfig(inline_chunk_threshold_bytes=0),
        )
        return (prefix, repo)

    (_, tmp_repo) = mk_repo()
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


def test_pickle_error() -> None:
    error = IcechunkError("test")
    pickled = pickle.dumps(error)
    roundtripped = pickle.loads(pickled)
    assert error.message == roundtripped.message
