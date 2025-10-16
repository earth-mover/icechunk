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


@pytest.mark.parametrize(
    "using_flush",
    [False, True],
)
def test_timetravel(using_flush: bool) -> None:
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

    if using_flush:
        first_snapshot_id = session.flush("commit 1")
        repo.reset_branch("main", first_snapshot_id)
    else:
        first_snapshot_id = session.commit("commit 1")

    assert session.read_only

    session = repo.writable_session("main")
    store = session.store
    group = zarr.open_group(store=store)
    air_temp = cast(zarr.core.array.Array, group["air_temp"])

    air_temp[:, :] = 54
    assert air_temp[200, 6] == 54

    if using_flush:
        new_snapshot_id = session.flush("commit 2")
        repo.reset_branch("main", new_snapshot_id)
    else:
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

    if using_flush:
        feature_snapshot_id = session.flush("commit 3")
        repo.reset_branch("feature", feature_snapshot_id)
    else:
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
    last_commit = session.commit("group b")

    keys = {k async for k in store.list()}
    assert "a/zarr.json" in keys
    assert "b/zarr.json" in keys

    with pytest.raises(ic.IcechunkError, match="branch update conflict"):
        repo.reset_branch(
            "main", prev_snapshot_id, from_snapshot_id="1CECHNKREP0F1RSTCMT0"
        )

    assert last_commit == repo.lookup_branch("main")

    repo.reset_branch("main", prev_snapshot_id, from_snapshot_id=last_commit)
    assert prev_snapshot_id == repo.lookup_branch("main")

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


@pytest.mark.parametrize(
    "using_flush",
    [False, True],
)
async def test_timetravel_async(using_flush: bool) -> None:
    config = ic.RepositoryConfig.default()
    config.inline_chunk_threshold_bytes = 1
    repo = await ic.Repository.create_async(
        storage=ic.in_memory_storage(),
        config=config,
    )

    session = await repo.writable_session_async("main")
    group = zarr.group(store=session.store, overwrite=True)
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

    if using_flush:
        first_snapshot_id = await session.flush_async("commit 1")
        await repo.reset_branch_async("main", first_snapshot_id)
    else:
        first_snapshot_id = await session.commit_async("commit 1")
    assert session.read_only

    session = await repo.writable_session_async("main")
    group = zarr.open_group(store=session.store)
    air_temp = cast(zarr.core.array.Array, group["air_temp"])

    air_temp[:, :] = 54
    assert air_temp[200, 6] == 54

    if using_flush:
        new_snapshot_id = await session.flush_async("commit 2")
        await repo.reset_branch_async("main", new_snapshot_id)
    else:
        new_snapshot_id = await session.commit_async("commit 2")

    session = await repo.readonly_session_async(snapshot_id=first_snapshot_id)
    store = session.store
    group = zarr.open_group(store=store, mode="r")
    air_temp = cast(zarr.core.array.Array, group["air_temp"])
    assert store.read_only
    assert air_temp[200, 6] == 42

    session = await repo.readonly_session_async(snapshot_id=new_snapshot_id)
    store = session.store
    group = zarr.open_group(store=store, mode="r")
    air_temp = cast(zarr.core.array.Array, group["air_temp"])
    assert store.read_only
    assert air_temp[200, 6] == 54

    session = await repo.writable_session_async("main")
    group = zarr.open_group(store=session.store)
    air_temp = cast(zarr.core.array.Array, group["air_temp"])

    air_temp[:, :] = 76
    assert session.has_uncommitted_changes
    assert session.branch == "main"
    assert session.snapshot_id == new_snapshot_id

    session.discard_changes()
    assert not session.has_uncommitted_changes
    assert air_temp[200, 6] == 54

    await repo.create_branch_async("feature", new_snapshot_id)
    session = await repo.writable_session_async("feature")
    assert not session.store.read_only
    assert session.branch == "feature"

    group = zarr.open_group(store=session.store)
    air_temp = cast(zarr.core.array.Array, group["air_temp"])
    air_temp[:, :] = 90

    if using_flush:
        feature_snapshot_id = await session.flush_async("commit 3")
        await repo.reset_branch_async("feature", feature_snapshot_id)
    else:
        feature_snapshot_id = await session.commit_async("commit 3")

    branches = await repo.list_branches_async()
    assert branches == set(["main", "feature"])

    await repo.delete_branch_async("feature")
    branches = await repo.list_branches_async()
    assert branches == set(["main"])

    await repo.create_tag_async("v1.0", feature_snapshot_id)
    await repo.create_branch_async("feature-not-dead", feature_snapshot_id)
    session = await repo.readonly_session_async(tag="v1.0")
    assert session.store.read_only
    assert session.branch is None

    group = zarr.open_group(store=session.store, mode="r")
    air_temp = cast(zarr.core.array.Array, group["air_temp"])
    assert air_temp[200, 6] == 90

    parents = [
        parent async for parent in repo.async_ancestry(snapshot_id=feature_snapshot_id)
    ]
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
    assert [parent async for parent in repo.async_ancestry(tag="v1.0")] == parents
    assert [
        parent async for parent in repo.async_ancestry(branch="feature-not-dead")
    ] == parents

    diff = await repo.diff_async(to_tag="v1.0", from_snapshot_id=parents[-1].id)
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

    session = await repo.writable_session_async("main")
    group = zarr.open_group(store=session.store)
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
        await repo.diff_async(from_tag="v1.0", to_snapshot_id=parents[-1].id)

    tags = await repo.list_tags_async()
    assert tags == set(["v1.0"])
    tag_snapshot_id = await repo.lookup_tag_async("v1.0")
    assert tag_snapshot_id == feature_snapshot_id

    actual = next(iter([parent async for parent in repo.async_ancestry(tag="v1.0")]))
    assert actual == await repo.lookup_snapshot_async(actual.id)


async def test_branch_reset_async() -> None:
    config = ic.RepositoryConfig.default()
    config.inline_chunk_threshold_bytes = 1
    repo = await ic.Repository.create_async(
        storage=ic.in_memory_storage(),
        config=config,
    )

    session = await repo.writable_session_async("main")
    group = zarr.group(store=session.store, overwrite=True)
    group.create_group("a")
    prev_snapshot_id = await session.commit_async("group a")

    session = await repo.writable_session_async("main")
    store = session.store
    group = zarr.open_group(store=store)
    group.create_group("b")
    await session.commit_async("group b")

    keys = {k async for k in store.list()}
    assert "a/zarr.json" in keys
    assert "b/zarr.json" in keys

    await repo.reset_branch_async("main", prev_snapshot_id)

    session = await repo.readonly_session_async("main")
    store = session.store
    keys = {k async for k in store.list()}
    assert "a/zarr.json" in keys
    assert "b/zarr.json" not in keys

    assert (
        await store.get("b/zarr.json", zarr.core.buffer.default_buffer_prototype())
    ) is None


async def test_tag_delete_async() -> None:
    repo = await ic.Repository.create_async(
        storage=ic.in_memory_storage(),
    )

    snap = await repo.lookup_branch_async("main")
    await repo.create_tag_async("tag", snap)
    await repo.delete_tag_async("tag")

    with pytest.raises(ic.IcechunkError):
        await repo.delete_tag_async("tag")

    with pytest.raises(ic.IcechunkError):
        await repo.create_tag_async("tag", snap)


async def test_session_with_as_of_async() -> None:
    repo = await ic.Repository.create_async(
        storage=ic.in_memory_storage(),
    )

    session = await repo.writable_session_async("main")

    times = []
    group = zarr.group(store=session.store, overwrite=True)
    sid = await session.commit_async("root")
    to_append = await anext(repo.async_ancestry(snapshot_id=sid))
    times.append(to_append.written_at)

    for i in range(5):
        session = await repo.writable_session_async("main")
        group = zarr.open_group(store=session.store)
        group.create_group(f"child {i}")
        sid = await session.commit_async(f"child {i}")
        to_append = await anext(repo.async_ancestry(snapshot_id=sid))
        times.append(to_append.written_at)

    ancestry = [p async for p in repo.async_ancestry(branch="main")]
    assert len(ancestry) == 7  # initial + root + 5 children

    store = (await repo.readonly_session_async("main", as_of=times[-1])).store
    group = zarr.open_group(store=store, mode="r")

    for i, time in enumerate(times):
        store = (await repo.readonly_session_async("main", as_of=time)).store
        group = zarr.open_group(store=store, mode="r")
        expected_children = {f"child {j}" for j in range(i)}
        actual_children = {g[0] for g in group.members()}
        assert expected_children == actual_children


async def test_tag_expiration_async() -> None:
    repo = await ic.Repository.create_async(storage=ic.in_memory_storage())
    session = await repo.writable_session_async("main")
    root = zarr.group(store=session.store, overwrite=True)
    a = await session.commit_async("a")

    session = await repo.writable_session_async("main")
    root = zarr.group(store=session.store, overwrite=True)
    root.create_group("child1")
    b = await session.commit_async("b")

    session = await repo.writable_session_async("main")
    root = zarr.group(store=session.store, overwrite=True)
    root.create_group("child2")
    await session.commit_async("c")

    await repo.create_tag_async("0", a)
    await repo.create_tag_async("1", b)
    assert await repo.list_tags_async() == {"0", "1"}

    expired = await repo.expire_snapshots_async(
        (await repo.lookup_snapshot_async(b)).written_at + timedelta(microseconds=1),
        delete_expired_tags=True,
    )
    assert expired == {a, b}
    assert await repo.list_tags_async() == set()


async def test_branch_expiration_async() -> None:
    repo = await ic.Repository.open_or_create_async(storage=ic.in_memory_storage())
    session = await repo.writable_session_async("main")
    root = zarr.group(store=session.store, overwrite=True)
    a = await session.commit_async("a")

    await repo.create_branch_async("branch", a)
    session = await repo.writable_session_async("branch")
    root = zarr.group(store=session.store, overwrite=True)
    root.create_group("child1")
    b = await session.commit_async("b")

    session = await repo.writable_session_async("main")
    root = zarr.group(store=session.store, overwrite=True)
    root.create_group("child2")
    c = await session.commit_async("c")

    expired = await repo.expire_snapshots_async(
        (await repo.lookup_snapshot_async(b)).written_at + timedelta(microseconds=1),
        delete_expired_branches=True,
    )
    assert expired == {a, b}
    assert "branch" not in await repo.list_branches_async()

    for snap in (a, b):
        await repo.lookup_snapshot_async(snap)

    # FIXME: this fails to delete snapshot `b` with microseconds=1
    await repo.garbage_collect_async(
        (await repo.lookup_snapshot_async(b)).written_at + timedelta(seconds=1)
    )
    # make sure snapshot cannot be opened anymore
    for snap in (a, b):
        with pytest.raises(ic.IcechunkError):
            await repo.readonly_session_async(snapshot_id=snap)

    # should succeed
    await repo.lookup_snapshot_async(c)


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


async def test_repository_lifecycle_async() -> None:
    """Test Repository configuration and lifecycle async methods."""
    storage = ic.in_memory_storage()

    # Test exists_async with non-existent repo
    assert not await ic.Repository.exists_async(storage)

    # Test fetch_config_async with non-existent repo
    config = await ic.Repository.fetch_config_async(storage)
    assert config is None

    # Test create_async with custom config
    custom_config = ic.RepositoryConfig.default()
    custom_config.inline_chunk_threshold_bytes = 1024

    repo = await ic.Repository.create_async(
        storage=storage,
        config=custom_config,
    )

    # Test exists_async with existing repo
    assert await ic.Repository.exists_async(storage)

    # Test fetch_config_async with existing repo
    fetched_config = await ic.Repository.fetch_config_async(storage)
    assert fetched_config is not None
    assert fetched_config.inline_chunk_threshold_bytes == 1024

    # Test save_config_async - modify and save config
    await repo.save_config_async()

    # Verify config was saved
    saved_config = await ic.Repository.fetch_config_async(storage)
    assert saved_config is not None
    assert saved_config.inline_chunk_threshold_bytes == 1024

    # Test open_async
    opened_repo = await ic.Repository.open_async(storage=storage)
    assert opened_repo.config.inline_chunk_threshold_bytes == 1024

    # Test reopen_async with new config
    new_config = ic.RepositoryConfig.default()
    new_config.inline_chunk_threshold_bytes = 4096

    reopened_repo = await repo.reopen_async(config=new_config)
    assert reopened_repo.config.inline_chunk_threshold_bytes == 4096

    # Test reopen (sync) with new config
    new_config_sync = ic.RepositoryConfig.default()
    new_config_sync.inline_chunk_threshold_bytes = 2048

    reopened_repo_sync = repo.reopen(config=new_config_sync)
    assert reopened_repo_sync.config.inline_chunk_threshold_bytes == 2048

    # Test open_or_create_async with new storage (should create)
    new_storage = ic.in_memory_storage()
    assert not await ic.Repository.exists_async(new_storage)


async def test_rewrite_manifests_async() -> None:
    """Test Repository.rewrite_manifests_async method."""
    repo = await ic.Repository.create_async(
        storage=ic.in_memory_storage(),
        config=ic.RepositoryConfig(inline_chunk_threshold_bytes=0),
    )

    # Create some data to generate manifests
    session = await repo.writable_session_async("main")
    group = zarr.group(store=session.store, overwrite=True)
    array = group.create_array(
        "test_array",
        shape=(100, 50),
        chunks=(10, 10),
        dtype="i4",
        compressors=None,
    )

    # Write data to create chunks and manifests
    array[:50, :25] = 42
    _first_commit = await session.commit_async("initial data")

    # Add more data
    session = await repo.writable_session_async("main")
    group = zarr.open_group(store=session.store)
    array = group["test_array"]
    array[50:, 25:] = 99
    second_commit = await session.commit_async("more data")

    # Get initial ancestry to verify commits exist
    initial_ancestry = [snap async for snap in repo.async_ancestry(branch="main")]
    assert len(initial_ancestry) >= 3  # initial + first_commit + second_commit

    # Test rewrite_manifests_async
    rewrite_commit = await repo.rewrite_manifests_async(
        "rewritten manifests",
        branch="main",
    )

    # Verify the rewrite created a new commit
    assert rewrite_commit != second_commit

    # Verify ancestry after rewrite
    new_ancestry = [snap async for snap in repo.async_ancestry(branch="main")]
    assert len(new_ancestry) == len(initial_ancestry) + 1
    assert new_ancestry[0].message == "rewritten manifests"

    # Verify data is still accessible after manifest rewrite
    session = await repo.readonly_session_async("main")
    group = zarr.open_group(store=session.store, mode="r")
    array = group["test_array"]

    # Check that data is preserved
    assert array[0, 0] == 42  # from first commit
    assert array[99, 49] == 99  # from second commit
