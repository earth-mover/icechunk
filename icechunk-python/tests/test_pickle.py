import pickle
import time
from pathlib import Path

import pytest

import zarr
from icechunk import (
    Conflict,
    ConflictError,
    ConflictType,
    IcechunkError,
    RebaseFailedError,
    Repository,
    RepositoryConfig,
    S3StaticCredentials,
    local_filesystem_storage,
    s3_storage,
)


def create_local_repo(path: str, spec_version: int | None) -> Repository:
    repo = Repository.create(
        storage=local_filesystem_storage(path), spec_version=spec_version
    )
    repo.set_default_commit_metadata({"author": "test"})
    return repo


@pytest.fixture(scope="function")
def tmp_repo(tmpdir: Path, any_spec_version: int | None) -> Repository:
    store_path = f"{tmpdir}"
    repo = create_local_repo(
        store_path,
        spec_version=any_spec_version,
    )
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


def test_pickle(any_spec_version: int | None) -> None:
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
            spec_version=any_spec_version,
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


def test_pickle_conflict_types() -> None:
    """Test that Conflict types can be constructed and pickled correctly."""
    # Test Conflict with conflicted_chunks
    conflict = Conflict(
        ConflictType.ChunkDoubleUpdate,
        "/some/array",
        [[0, 0], [1, 1]],
    )
    assert conflict.conflict_type == ConflictType.ChunkDoubleUpdate
    assert conflict.path == "/some/array"
    assert conflict.conflicted_chunks == [[0, 0], [1, 1]]

    # Test pickle roundtrip
    roundtripped = pickle.loads(pickle.dumps(conflict))
    assert roundtripped.conflict_type == conflict.conflict_type
    assert roundtripped.path == conflict.path
    assert roundtripped.conflicted_chunks == conflict.conflicted_chunks

    # Test Conflict without conflicted_chunks
    conflict_no_chunks = Conflict(
        ConflictType.ZarrMetadataDoubleUpdate,
        "/another/array",
    )
    assert conflict_no_chunks.conflicted_chunks is None

    roundtripped_no_chunks = pickle.loads(pickle.dumps(conflict_no_chunks))
    assert roundtripped_no_chunks.conflict_type == conflict_no_chunks.conflict_type
    assert roundtripped_no_chunks.path == conflict_no_chunks.path
    assert roundtripped_no_chunks.conflicted_chunks is None

    # Test RebaseFailedError construction and pickle
    error = RebaseFailedError("snapshot_123", [conflict, conflict_no_chunks])
    assert error.snapshot == "snapshot_123"
    assert len(error.conflicts) == 2
    assert error.conflicts[0].path == "/some/array"
    assert error.conflicts[1].path == "/another/array"

    roundtripped_error = pickle.loads(pickle.dumps(error))
    assert roundtripped_error.snapshot == error.snapshot
    assert len(roundtripped_error.conflicts) == len(error.conflicts)
    assert roundtripped_error.conflicts[0].path == error.conflicts[0].path

    # Test ConflictError construction and pickle
    conflict_error = ConflictError("expected_snap", "actual_snap")
    assert conflict_error.expected_parent == "expected_snap"
    assert conflict_error.actual_parent == "actual_snap"

    roundtripped_conflict_error = pickle.loads(pickle.dumps(conflict_error))
    assert roundtripped_conflict_error.expected_parent == conflict_error.expected_parent
    assert roundtripped_conflict_error.actual_parent == conflict_error.actual_parent
