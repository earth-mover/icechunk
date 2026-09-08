import pickle
import tempfile
from concurrent.futures import ProcessPoolExecutor

import numpy as np
import pytest

import zarr
from icechunk import (
    ConflictType,
    ErrorKind,
    IcechunkError,
    InvalidInputError,
    MergeConflictError,
    Repository,
    Storage,
    in_memory_storage,
    local_filesystem_storage,
)
from icechunk.conflicts import MergeConflict


def create_repo(storage: Storage, spec_version: int | None = None) -> Repository:
    repo = Repository.create(storage, spec_version=spec_version)
    session = repo.writable_session("main")
    group = zarr.group(session.store)
    group.create_array("array", shape=(4,), chunks=(1,), dtype="int32", fill_value=0)
    session.commit("create array")
    return repo


def flush_value(repo: Repository, index: int, value: int) -> str:
    session = repo.writable_session("main")
    array = zarr.open_array(session.store, path="array", mode="r+")
    array[index] = value
    return session.flush(f"chunk {index}")


def read_all(repo: Repository) -> list[int]:
    session = repo.readonly_session("main")
    array = zarr.open_array(session.store, path="array", mode="r")
    return [int(v) for v in np.asarray(array[:])]


def flush_in_subprocess(path: str, index: int, value: int) -> str:
    repo = Repository.open(local_filesystem_storage(path))
    return flush_value(repo, index, value)


@pytest.mark.parametrize("use_async", [False, True])
async def test_merge_disjoint_writers(use_async: bool) -> None:
    repo = create_repo(in_memory_storage())
    a = flush_value(repo, 0, 1)
    b = flush_value(repo, 1, 2)

    if use_async:
        merged = await repo.merge_snapshots_async(
            "main", [a, b], "merge", metadata={"job": 7}
        )
    else:
        merged = repo.merge_snapshots("main", [a, b], "merge", metadata={"job": 7})

    assert read_all(repo) == [1, 2, 0, 0]
    head = next(iter(repo.ancestry(branch="main")))
    assert head.id == merged
    assert head.message == "merge"
    assert head.metadata == {"job": 7}


def test_merge_conflict_error() -> None:
    repo = create_repo(in_memory_storage())
    a = flush_value(repo, 0, 1)
    b = flush_value(repo, 0, 2)

    with pytest.raises(MergeConflictError) as excinfo:
        repo.merge_snapshots("main", [a, b], "merge")

    err = excinfo.value
    assert err.kind == ErrorKind.MERGE_CONFLICT
    assert len(err.conflicts) == 1
    conflict = err.conflicts[0]
    assert isinstance(conflict, MergeConflict)
    assert conflict.first_snapshot == a
    assert conflict.second_snapshot == b
    assert conflict.conflict.conflict_type == ConflictType.ChunkDoubleUpdate
    assert conflict.conflict.path == "/array"
    assert conflict.conflict.conflicted_chunks == [[0]]

    restored = pickle.loads(pickle.dumps(err))
    assert isinstance(restored, MergeConflictError)
    assert restored.conflicts[0].first_snapshot == a
    assert "1 conflicts found" in str(err)
    # the branch did not move
    assert read_all(repo) == [0, 0, 0, 0]


def test_merge_onto_moved_tip() -> None:
    repo = create_repo(in_memory_storage())
    a = flush_value(repo, 0, 1)
    session = repo.writable_session("main")
    zarr.open_array(session.store, path="array", mode="r+")[2] = 3
    session.commit("tip moved")

    repo.merge_snapshots("main", [a], "merge")

    assert read_all(repo) == [1, 0, 3, 0]


def test_merge_empty_list_is_invalid_input() -> None:
    repo = create_repo(in_memory_storage())

    with pytest.raises(InvalidInputError) as excinfo:
        repo.merge_snapshots("main", [], "merge")

    assert excinfo.value.kind == ErrorKind.NO_SNAPSHOTS_TO_MERGE


def test_merge_snapshot_outside_branch_history() -> None:
    repo = create_repo(in_memory_storage())
    tip = repo.lookup_branch("main")
    repo.create_branch("other", tip)
    session = repo.writable_session("other")
    zarr.open_array(session.store, path="array", mode="r+")[0] = 9
    session.commit("on other")
    session = repo.writable_session("other")
    zarr.open_array(session.store, path="array", mode="r+")[1] = 9
    source = session.flush("flush on other")

    with pytest.raises(InvalidInputError) as excinfo:
        repo.merge_snapshots("main", [source], "merge")

    assert excinfo.value.kind == ErrorKind.SNAPSHOT_NOT_IN_BRANCH_HISTORY


def test_merge_rejects_spec_v1() -> None:
    repo = create_repo(in_memory_storage(), spec_version=1)
    a = flush_value(repo, 0, 1)

    with pytest.raises(IcechunkError) as excinfo:
        repo.merge_snapshots("main", [a], "merge")

    assert excinfo.value.kind == ErrorKind.UNSUPPORTED_SPEC_VERSION


def test_merge_from_process_pool() -> None:
    # One worker: local filesystem repo_info updates aren't conditional, so
    # genuinely concurrent flushes race and can drop all but one
    # registration. This still exercises a fresh, unpicklable `Storage`
    # reopened in a worker process.
    with tempfile.TemporaryDirectory() as path:
        repo = create_repo(local_filesystem_storage(path))
        with ProcessPoolExecutor(max_workers=1) as executor:
            futures = [
                executor.submit(flush_in_subprocess, path, index, index + 10)
                for index in range(4)
            ]
            snapshots = [future.result() for future in futures]

        repo.merge_snapshots("main", snapshots, "merge from workers")

        assert read_all(repo) == [10, 11, 12, 13]
        assert np.array_equal(
            zarr.open_array(repo.readonly_session("main").store, path="array")[:],
            np.array([10, 11, 12, 13], dtype="int32"),
        )
