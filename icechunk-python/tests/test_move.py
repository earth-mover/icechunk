import asyncio

import numpy.testing
import pytest

import icechunk as ic
import zarr


async def test_basic_move() -> None:
    repo = await ic.Repository.create_async(
        storage=ic.in_memory_storage(),
    )
    session = await repo.writable_session_async("main")
    store = session.store
    root = zarr.group(store=store, overwrite=True)
    group = root.create_group("my/old/path", overwrite=True)
    group.create_array("array", shape=(10, 10), chunks=(2, 2), dtype="i4", fill_value=42)
    all_keys = sorted([k async for k in store.list()])
    assert all_keys == sorted(
        [
            "zarr.json",
            "my/zarr.json",
            "my/old/zarr.json",
            "my/old/path/zarr.json",
            "my/old/path/array/zarr.json",
        ]
    )
    await session.commit_async("create array")

    session = await repo.rearrange_session_async("main")
    store = session.store
    await session.move_async("/my/old", "/my/new")
    all_keys = sorted([k async for k in store.list()])
    assert all_keys == sorted(
        [
            "zarr.json",
            "my/zarr.json",
            "my/new/zarr.json",
            "my/new/path/zarr.json",
            "my/new/path/array/zarr.json",
        ]
    )
    await session.commit_async("directory renamed")

    session = await repo.readonly_session_async("main")
    store = session.store
    group = zarr.open_group(store=store, mode="r")
    array = group["my/new/path/array"]
    numpy.testing.assert_array_equal(array, 42)

    a, b, *_ = await asyncio.to_thread(lambda: list(repo.ancestry(branch="main")))
    diff = await repo.diff_async(from_snapshot_id=b.id, to_snapshot_id=a.id)
    assert diff.moved_nodes == [("/my/old", "/my/new")]
    assert (
        repr(diff)
        == """\
Nodes moved/renamed:
    /my/old -> /my/new

"""
    )


def test_move_errors() -> None:
    repo = ic.Repository.create(
        storage=ic.in_memory_storage(),
    )
    session = repo.writable_session("main")
    store = session.store
    root = zarr.group(store=store, overwrite=True)
    group = root.create_group("my/old/path", overwrite=True)
    group.create_array("array", shape=(10, 10), chunks=(2, 2), dtype="i4", fill_value=42)
    session.commit("create array")

    session = repo.rearrange_session("main")
    store = session.store

    with pytest.raises(ic.IcechunkError, match="overwrite existing node"):
        session.move("/my/old/path", "/my/old/path")
    with pytest.raises(ic.IcechunkError, match="node not found"):
        session.move("/not-found", "/my/new/path")


def test_doesnt_rebase() -> None:
    repo = ic.Repository.create(
        storage=ic.in_memory_storage(),
    )
    session = repo.writable_session("main")
    root = zarr.group(store=session.store, overwrite=True)
    root.create_group("my/old/path", overwrite=True)
    session.commit("create group")

    session1 = repo.rearrange_session("main")
    session1.move("/my/old/path", "/my/new/path")

    session2 = repo.writable_session("main")
    root = zarr.group(store=session2.store, overwrite=True)
    root.create_group("no-conflict", overwrite=True)

    session1.commit("moved")

    try:
        session2.commit("rebase commit")
        raise AssertionError()
    except ic.ConflictError:
        try:
            session2.rebase(ic.ConflictDetector())
            raise AssertionError()
        except ic.RebaseFailedError as e:
            assert len(e.conflicts) == 1
            assert (
                e.conflicts[0].conflict_type
                == ic.ConflictType.MoveOperationCannotBeRebased
            )
