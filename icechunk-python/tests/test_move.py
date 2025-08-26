import numpy.testing
import pytest

import icechunk as ic
import zarr


async def test_basic_move():
    repo = ic.Repository.create(
        storage=ic.in_memory_storage(),
    )
    session = repo.writable_session("main")
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
    session.commit("create array")

    session = repo.rearrange_session("main")
    store = session.store
    session.move("/my/old", "/my/new")
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
    session.commit("directory renamed")

    session = repo.readonly_session("main")
    store = session.store
    group = zarr.open_group(store=store, mode="r")
    array = group["my/new/path/array"]
    numpy.testing.assert_array_equal(array, 42)


def test_move_errors():
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
