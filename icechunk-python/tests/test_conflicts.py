from pathlib import Path
from typing import cast

import numpy as np
import pytest

import icechunk
import zarr


@pytest.fixture
def repo(tmpdir: Path) -> icechunk.Repository:
    repo = icechunk.Repository.create(
        storage=icechunk.local_filesystem_storage(str(tmpdir))
    )

    session = repo.writable_session("main")
    store = session.store
    root = zarr.group(store=store)
    root.create_group("foo/bar")
    root.create_array("foo/bar/some-array", shape=(10, 10), chunks=(1, 1), dtype="i4")
    session.commit("commit 1")

    return repo


def test_detect_conflicts(repo: icechunk.Repository) -> None:
    session_a = repo.writable_session("main")
    session_b = repo.writable_session("main")
    store_a = session_a.store
    store_b = session_b.store

    root_a = zarr.group(store=store_a)
    array_a = cast(zarr.Array, root_a["foo/bar/some-array"])
    array_a[:, :] = 1
    array_a.attrs["repo"] = 1
    session_a.commit("update array")

    root_b = zarr.group(store=store_b)
    array_b = cast(zarr.Array, root_b["foo/bar/some-array"])
    array_b[:, :] = 2
    array_b.attrs["repo"] = 2

    with pytest.raises(icechunk.ConflictError):
        session_b.commit("update array")

    with pytest.raises(icechunk.RebaseFailedError):
        try:
            session_b.rebase(icechunk.ConflictDetector())
        except icechunk.RebaseFailedError as e:
            assert len(e.conflicts) == 3
            assert e.conflicts[0].path == "/foo/bar/some-array"
            assert e.conflicts[0].path == "/foo/bar/some-array"
            assert (
                e.conflicts[0].conflict_type
                == icechunk.ConflictType.ZarrMetadataDoubleUpdate
            )
            assert e.conflicts[1].path == "/foo/bar/some-array"
            assert (
                e.conflicts[1].conflict_type
                == icechunk.ConflictType.ChunksUpdatedInUpdatedArray
            )
            assert e.conflicts[2].path == "/foo/bar/some-array"
            assert e.conflicts[2].conflict_type == icechunk.ConflictType.ChunkDoubleUpdate
            assert e.conflicts[2].conflicted_chunks
            assert len(e.conflicts[2].conflicted_chunks) == 100

            raise e


def test_rebase_no_conflicts(repo: icechunk.Repository) -> None:
    session_a = repo.writable_session("main")
    session_b = repo.writable_session("main")
    store_a = session_a.store
    store_b = session_b.store

    root_a = zarr.group(store=store_a)
    array_a = cast(zarr.Array, root_a["foo/bar/some-array"])
    array_a[:] = 1
    session_a.commit("update array")

    root_b = zarr.group(store=store_b)
    array_b = cast(zarr.Array, root_b["foo/bar/some-array"])
    array_b.attrs["repo"] = 2

    session_b.commit("update array", rebase_with=icechunk.ConflictDetector())

    session_c = repo.readonly_session(branch="main")
    store_c = session_c.store
    root_c = zarr.open_group(store=store_c, mode="r")
    array_c = cast(zarr.Array, root_c["foo/bar/some-array"])
    np.testing.assert_array_equal(array_c[:], 1)
    assert array_c.attrs["repo"] == 2


def test_rebase_fails_on_user_atts_double_edit(repo: icechunk.Repository) -> None:
    session_a = repo.writable_session("main")
    session_b = repo.writable_session("main")
    store_a = session_a.store
    store_b = session_b.store

    root_a = zarr.group(store=store_a)
    array_a = cast(zarr.Array, root_a["foo/bar/some-array"])
    array_a.attrs["repo"] = 1
    session_a.commit("update array")

    root_b = zarr.group(store=store_b)
    array_b = cast(zarr.Array, root_b["foo/bar/some-array"])
    array_b.attrs["repo"] = 2

    with pytest.raises(icechunk.ConflictError):
        session_b.commit("update array")

    # Make sure it fails if the resolver is not set
    with pytest.raises(icechunk.RebaseFailedError):
        session_b.commit("update array", rebase_with=icechunk.BasicConflictSolver())


@pytest.mark.parametrize(
    "on_chunk_conflict",
    [icechunk.VersionSelection.UseOurs, icechunk.VersionSelection.UseTheirs],
)
def test_rebase_chunks_with_ours(
    repo: icechunk.Repository, on_chunk_conflict: icechunk.VersionSelection
) -> None:
    session_a = repo.writable_session("main")
    session_b = repo.writable_session("main")
    store_a = session_a.store
    store_b = session_b.store

    root_a = zarr.group(store=store_a)
    array_a = cast(zarr.Array, root_a["foo/bar/some-array"])
    array_a[:] = 1
    session_a.commit("update array")

    root_b = zarr.group(store=store_b)
    array_b = cast(zarr.Array, root_b["foo/bar/some-array"])
    array_b[:, 0] = 2

    with pytest.raises(icechunk.ConflictError):
        session_b.commit("update first column of array")

    # Make sure it fails if the resolver is not set
    with pytest.raises(icechunk.RebaseFailedError):
        try:
            session_b.commit(
                "update first column of array",
                rebase_with=icechunk.BasicConflictSolver(
                    on_chunk_conflict=icechunk.VersionSelection.Fail
                ),
            )
        except icechunk.RebaseFailedError as e:
            assert e.conflicts[0].path == "/foo/bar/some-array"
            assert e.conflicts[0].conflict_type == icechunk.ConflictType.ChunkDoubleUpdate
            assert len(e.conflicts) == 1

            np.testing.assert_array_equal(
                np.array(e.conflicts[0].conflicted_chunks),
                np.array(
                    [
                        [0, 0],
                        [1, 0],
                        [2, 0],
                        [3, 0],
                        [4, 0],
                        [5, 0],
                        [6, 0],
                        [7, 0],
                        [8, 0],
                        [9, 0],
                    ]
                ),
            )

            raise e

    solver = icechunk.BasicConflictSolver(
        on_chunk_conflict=on_chunk_conflict,
    )

    session_b.commit("after conflict", rebase_with=solver)

    session_c = repo.readonly_session(branch="main")
    store_c = session_c.store
    root_c = zarr.open_group(store=store_c, mode="r")
    array_c = cast(zarr.Array, root_c["foo/bar/some-array"])
    assert (
        array_c[0, 0] == 2
        if on_chunk_conflict == icechunk.VersionSelection.UseOurs
        else 1
    )
    assert array_c[0, 1] == 1


async def test_rebase_async() -> None:
    """Test async rebase functionality with conflict detection and resolution."""
    repo = await icechunk.Repository.create_async(
        storage=icechunk.in_memory_storage(),
    )

    # Set up initial state
    session_a = await repo.writable_session_async("main")
    root_a = zarr.group(store=session_a.store, overwrite=True)
    group_a = root_a.create_group("foo/bar")
    array_a = group_a.create_array(
        "some-array", shape=(100, 100), chunks=(10, 10), dtype="i4"
    )
    array_a[:, :] = 1
    array_a.attrs["repo"] = 1
    await session_a.commit_async("create array")

    # Create conflicting sessions
    session_b = await repo.writable_session_async("main")
    root_b = zarr.open_group(store=session_b.store)
    array_b = cast(zarr.Array, root_b["foo/bar/some-array"])

    session_c = await repo.writable_session_async("main")
    root_c = zarr.open_group(store=session_c.store)
    array_c = cast(zarr.Array, root_c["foo/bar/some-array"])

    # Make changes in session_c and commit first
    array_c[:, :] = 3
    await session_c.commit_async("update array from c")

    # Make conflicting changes in session_b
    array_b[:, :] = 2

    # This should fail due to conflicts
    with pytest.raises(icechunk.ConflictError):
        await session_b.commit_async("update array")

    # Test rebase_async with ConflictDetector (should fail)
    with pytest.raises(icechunk.RebaseFailedError):
        try:
            await session_b.rebase_async(icechunk.ConflictDetector())
        except icechunk.RebaseFailedError as e:
            assert len(e.conflicts) == 1
            assert e.conflicts[0].path == "/foo/bar/some-array"
            assert e.conflicts[0].conflict_type == icechunk.ConflictType.ChunkDoubleUpdate
            assert e.conflicts[0].conflicted_chunks
            assert len(e.conflicts[0].conflicted_chunks) == 100
            raise e

    # Test successful rebase_async with BasicConflictSolver
    solver = icechunk.BasicConflictSolver(
        on_chunk_conflict=icechunk.VersionSelection.UseOurs,
    )

    await session_b.rebase_async(solver)
    await session_b.commit_async("after conflict resolution")

    # Verify the result
    session_final = await repo.readonly_session_async(branch="main")
    store_final = session_final.store
    root_final = zarr.open_group(store=store_final, mode="r")
    array_final = cast(zarr.Array, root_final["foo/bar/some-array"])

    # Should have session_b's values due to UseOurs selection
    assert array_final[0, 0] == 2
    assert array_final.attrs["repo"] == 1
