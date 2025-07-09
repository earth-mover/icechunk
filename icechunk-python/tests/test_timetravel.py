import asyncio
from datetime import timedelta
from typing import cast

import pytest

import icechunk as ic
import zarr
import zarr.core
import zarr.core.array
import zarr.core.buffer


async def async_ancestry(
    repo: ic.Repository, **kwargs: str | None
) -> list[ic.SnapshotInfo]:
    return [parent async for parent in repo.async_ancestry(**kwargs)]


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

    status = session.status()
    assert status.new_groups == {"/"}
    assert status.new_arrays == {"/air_temp"}
    assert list(status.updated_chunks.keys()) == ["/air_temp"]
    assert sorted(status.updated_chunks["/air_temp"]) == sorted(
        [[i, j] for i in range(10) for j in range(10)]
    )
    assert status.deleted_groups == set()
    assert status.deleted_arrays == set()
    assert status.updated_arrays == set()
    assert status.updated_groups == set()

    first_snapshot_id = session.commit("commit 1")
    assert session.read_only

    session = repo.writable_session("main")
    store = session.store
    group = zarr.open_group(store=store)
    air_temp = cast(zarr.core.array.Array, group["air_temp"])

    air_temp[:, :] = 54
    assert air_temp[200, 6] == 54

    new_snapshot_id = session.commit("commit 2")

    session = repo.readonly_session(snapshot_id=first_snapshot_id)
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
    repo.create_branch("feature-not-dead", feature_snapshot_id)
    session = repo.readonly_session(tag="v1.0")
    store = session.store
    assert store._read_only
    assert session.branch is None

    group = zarr.open_group(store=store, mode="r")
    air_temp = cast(zarr.core.array.Array, group["air_temp"])
    assert air_temp[200, 6] == 90

    parents = list(repo.ancestry(snapshot_id=feature_snapshot_id))
    assert [snap.message for snap in parents] == [
        "commit 3",
        "commit 2",
        "commit 1",
        "Repository initialized",
    ]
    assert parents[-1].id == "1CECHNKREP0F1RSTCMT0"
    assert [len(snap.manifests) for snap in parents] == [1, 1, 1, 0]
    assert sorted(parents, key=lambda p: p.written_at) == list(reversed(parents))
    assert len(set([snap.id for snap in parents])) == 4
    assert list(repo.ancestry(tag="v1.0")) == parents
    assert list(repo.ancestry(branch="feature-not-dead")) == parents

    diff = repo.diff(to_tag="v1.0", from_snapshot_id=parents[-1].id)
    assert diff.new_groups == {"/"}
    assert diff.new_arrays == {"/air_temp"}
    assert list(diff.updated_chunks.keys()) == ["/air_temp"]
    assert sorted(diff.updated_chunks["/air_temp"]) == sorted(
        [[i, j] for i in range(10) for j in range(10)]
    )
    assert diff.deleted_groups == set()
    assert diff.deleted_arrays == set()
    assert diff.updated_arrays == set()
    assert diff.updated_groups == set()
    assert (
        repr(diff)
        == """\
Groups created:
    /

Arrays created:
    /air_temp

Chunks updated:
    /air_temp:
        [0, 0]
        [0, 1]
        [0, 2]
        [0, 3]
        [0, 4]
        [0, 5]
        [0, 6]
        [0, 7]
        [0, 8]
        [0, 9]
        ... 90 more
"""
    )

    session = repo.writable_session("main")
    store = session.store

    group = zarr.open_group(store=store)
    air_temp = group.create_array(
        "air_temp", shape=(1000, 1000), chunks=(100, 100), dtype="i4", overwrite=True
    )
    assert (
        repr(session.status())
        == """\
Arrays created:
    /air_temp

Arrays deleted:
    /air_temp

"""
    )

    with pytest.raises(ic.IcechunkError, match="doesn't include"):
        # if we call diff in the wrong order it fails with a message
        repo.diff(from_tag="v1.0", to_snapshot_id=parents[-1].id)

    # check async ancestry works
    assert list(repo.ancestry(snapshot_id=feature_snapshot_id)) == asyncio.run(
        async_ancestry(repo, snapshot_id=feature_snapshot_id)
    )
    assert list(repo.ancestry(tag="v1.0")) == asyncio.run(
        async_ancestry(repo, tag="v1.0")
    )
    assert list(repo.ancestry(branch="feature-not-dead")) == asyncio.run(
        async_ancestry(repo, branch="feature-not-dead")
    )

    tags = repo.list_tags()
    assert tags == set(["v1.0"])
    tag_snapshot_id = repo.lookup_tag("v1.0")
    assert tag_snapshot_id == feature_snapshot_id

    actual = next(iter(repo.ancestry(tag="v1.0")))
    assert actual == repo.lookup_snapshot(actual.id)


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

    session = repo.readonly_session("main")
    store = session.store

    keys = {k async for k in store.list()}
    assert "a/zarr.json" in keys
    assert "b/zarr.json" not in keys

    assert (
        await store.get("b/zarr.json", zarr.core.buffer.default_buffer_prototype())
    ) is None


async def test_tag_delete() -> None:
    repo = ic.Repository.create(
        storage=ic.in_memory_storage(),
    )

    snap = repo.lookup_branch("main")
    repo.create_tag("tag", snap)
    repo.delete_tag("tag")

    with pytest.raises(ic.IcechunkError):
        repo.delete_tag("tag")

    with pytest.raises(ic.IcechunkError):
        repo.create_tag("tag", snap)


async def test_session_with_as_of() -> None:
    repo = ic.Repository.create(
        storage=ic.in_memory_storage(),
    )

    session = repo.writable_session("main")
    store = session.store

    times = []
    group = zarr.group(store=store, overwrite=True)
    sid = session.commit("root")
    times.append(next(repo.ancestry(snapshot_id=sid)).written_at)

    for i in range(5):
        session = repo.writable_session("main")
        store = session.store
        group = zarr.open_group(store=store)
        group.create_group(f"child {i}")
        sid = session.commit(f"child {i}")
        times.append(next(repo.ancestry(snapshot_id=sid)).written_at)

    ancestry = list(p for p in repo.ancestry(branch="main"))
    assert len(ancestry) == 7  # initial + root + 5 children

    store = repo.readonly_session("main", as_of=times[-1]).store
    group = zarr.open_group(store=store, mode="r")

    for i, time in enumerate(times):
        store = repo.readonly_session("main", as_of=time).store
        group = zarr.open_group(store=store, mode="r")
        expected_children = {f"child {j}" for j in range(i)}
        actual_children = {g[0] for g in group.members()}
        assert expected_children == actual_children


async def test_default_commit_metadata() -> None:
    repo = ic.Repository.create(
        storage=ic.in_memory_storage(),
    )

    repo.set_default_commit_metadata({"user": "test"})
    session = repo.writable_session("main")
    root = zarr.group(store=session.store, overwrite=True)
    root.create_group("child")
    sid = session.commit("root")
    snap = next(repo.ancestry(snapshot_id=sid))
    assert snap.metadata == {"user": "test"}


def test_tag_expiration() -> None:
    repo = ic.Repository.create(storage=ic.in_memory_storage())
    session = repo.writable_session("main")
    root = zarr.group(store=session.store, overwrite=True)
    a = session.commit("a")

    session = repo.writable_session("main")
    root = zarr.group(store=session.store, overwrite=True)
    root.create_group("child1")
    b = session.commit("b")

    session = repo.writable_session("main")
    root = zarr.group(store=session.store, overwrite=True)
    root.create_group("child2")
    session.commit("c")

    repo.create_tag("0", a)
    repo.create_tag("1", b)
    assert repo.list_tags() == {"0", "1"}

    expired = repo.expire_snapshots(
        repo.lookup_snapshot(b).written_at + timedelta(microseconds=1),
        delete_expired_tags=True,
    )
    assert expired == {a, b}
    assert repo.list_tags() == set()


def test_branch_expiration() -> None:
    repo = ic.Repository.create(storage=ic.in_memory_storage())
    session = repo.writable_session("main")
    root = zarr.group(store=session.store, overwrite=True)
    a = session.commit("a")

    repo.create_branch("branch", a)
    session = repo.writable_session("branch")
    root = zarr.group(store=session.store, overwrite=True)
    root.create_group("child1")
    b = session.commit("b")

    session = repo.writable_session("main")
    root = zarr.group(store=session.store, overwrite=True)
    root.create_group("child2")
    c = session.commit("c")

    expired = repo.expire_snapshots(
        repo.lookup_snapshot(b).written_at + timedelta(microseconds=1),
        delete_expired_branches=True,
    )
    assert expired == {a, b}
    assert "branch" not in repo.list_branches()

    for snap in (a, b):
        repo.lookup_snapshot(snap)

    # FIXME: this fails to delete snapshot `b` with microseconds=1
    repo.garbage_collect(repo.lookup_snapshot(b).written_at + timedelta(seconds=1))
    # make sure snapshot cannot be opened anymore
    for snap in (a, b):
        with pytest.raises(ic.IcechunkError):
            repo.readonly_session(snapshot_id=snap)

    # should succeed
    repo.lookup_snapshot(c)
