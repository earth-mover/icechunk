from typing import cast

import icechunk as ic
import zarr
import zarr.core
import zarr.core.array
import zarr.core.buffer


def test_timetravel() -> None:
    config = ic.RepositoryConfig.default()
    config.inline_chunk_threshold_bytes = 1
    repo = ic.Repository.create(
        storage=ic.in_memory_storage(),
        config=config,
    )
    session = repo.writable_session("main")
    store = session.store

    group = zarr.group(store=store, overwrite=True)
    air_temp = group.create_array(
        "air_temp", shape=(1000, 1000), chunks=(100, 100), dtype="i4"
    )

    air_temp[:, :] = 42
    assert air_temp[200, 6] == 42

    snapshot_id = session.commit("commit 1")
    assert session.read_only

    session = repo.writable_session("main")
    store = session.store
    group = zarr.open_group(store=store)
    air_temp = cast(zarr.core.array.Array, group["air_temp"])

    air_temp[:, :] = 54
    assert air_temp[200, 6] == 54

    new_snapshot_id = session.commit("commit 2")

    session = repo.readonly_session(snapshot_id=snapshot_id)
    store = session.store
    group = zarr.open_group(store=store, mode="r")
    air_temp = cast(zarr.core.array.Array, group["air_temp"])
    assert store.read_only
    assert air_temp[200, 6] == 42

    session = repo.readonly_session(snapshot_id=new_snapshot_id)
    store = session.store
    group = zarr.open_group(store=store, mode="r")
    air_temp = cast(zarr.core.array.Array, group["air_temp"])
    assert store.read_only
    assert air_temp[200, 6] == 54

    session = repo.writable_session("main")
    store = session.store
    group = zarr.open_group(store=store)
    air_temp = cast(zarr.core.array.Array, group["air_temp"])

    air_temp[:, :] = 76
    assert session.has_uncommitted_changes
    assert session.branch == "main"
    assert session.snapshot_id == new_snapshot_id

    session.discard_changes()
    assert not (session.has_uncommitted_changes)
    # I don't understand why I need to ignore here
    assert air_temp[200, 6] == 54  # type: ignore [unreachable]

    repo.create_branch("feature", new_snapshot_id)
    session = repo.writable_session("feature")
    store = session.store
    assert not store._read_only
    assert session.branch == "feature"

    group = zarr.open_group(store=store)
    air_temp = cast(zarr.core.array.Array, group["air_temp"])
    air_temp[:, :] = 90
    feature_snapshot_id = session.commit("commit 3")

    branches = repo.list_branches()
    assert branches == set(["main", "feature"])

    repo.delete_branch("feature")
    branches = repo.list_branches()
    assert branches == set(["main"])

    repo.create_tag("v1.0", feature_snapshot_id)
    session = repo.readonly_session(tag="v1.0")
    store = session.store
    assert store._read_only
    assert session.branch is None

    group = zarr.open_group(store=store, mode="r")
    air_temp = cast(zarr.core.array.Array, group["air_temp"])
    assert air_temp[200, 6] == 90

    parents = list(repo.ancestry(feature_snapshot_id))
    assert [snap.message for snap in parents] == [
        "commit 3",
        "commit 2",
        "commit 1",
        "Repository initialized",
    ]
    assert sorted(parents, key=lambda p: p.written_at) == list(reversed(parents))
    assert len(set([snap.id for snap in parents])) == 4

    tags = repo.list_tags()
    assert tags == set(["v1.0"])
    tag_snapshot_id = repo.lookup_tag("v1.0")
    assert tag_snapshot_id == feature_snapshot_id


async def test_branch_reset() -> None:
    config = ic.RepositoryConfig.default()
    config.inline_chunk_threshold_bytes = 1
    repo = ic.Repository.create(
        storage=ic.in_memory_storage(),
        config=config,
    )

    session = repo.writable_session("main")
    store = session.store

    group = zarr.group(store=store, overwrite=True)
    group.create_group("a")
    prev_snapshot_id = session.commit("group a")

    session = repo.writable_session("main")
    store = session.store

    group = zarr.open_group(store=store)
    group.create_group("b")
    session.commit("group b")

    keys = {k async for k in store.list()}
    assert "a/zarr.json" in keys
    assert "b/zarr.json" in keys

    repo.reset_branch("main", prev_snapshot_id)

    session = repo.readonly_session(branch="main")
    store = session.store

    keys = {k async for k in store.list()}
    assert "a/zarr.json" in keys
    assert "b/zarr.json" not in keys

    assert (
        await store.get("b/zarr.json", zarr.core.buffer.default_buffer_prototype())
    ) is None
