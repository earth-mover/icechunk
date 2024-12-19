from typing import cast

import pytest

import icechunk
import zarr
import zarr.core
import zarr.core.array
import zarr.core.buffer


def test_timetravel():
    repo = icechunk.Repository.create(
        storage=icechunk.StorageConfig.memory("test"),
        config=icechunk.RepositoryConfig(inline_chunk_threshold_bytes=1),
    )
    session = repo.writable_session("main")
    store = session.store()

    group = zarr.group(store=store, overwrite=True)
    air_temp = group.create_array(
        "air_temp", shape=(1000, 1000), chunk_shape=(100, 100), dtype="i4"
    )

    air_temp[:, :] = 42
    assert air_temp[200, 6] == 42

    snapshot_id = session.commit("commit 1")
    assert session.read_only

    session = repo.writable_session("main")
    store = session.store()
    group = zarr.open_group(store=store)
    air_temp = cast(zarr.core.array.Array, group["air_temp"])

    air_temp[:, :] = 54
    assert air_temp[200, 6] == 54

    new_snapshot_id = session.commit("commit 2")

    session = repo.readonly_session(snapshot_id=snapshot_id)
    store = session.store()
    group = zarr.open_group(store=store, mode="r")
    air_temp = cast(zarr.core.array.Array, group["air_temp"])
    assert store.read_only
    assert air_temp[200, 6] == 42

    session = repo.readonly_session(snapshot_id=new_snapshot_id)
    store = session.store()
    group = zarr.open_group(store=store, mode="r")
    air_temp = cast(zarr.core.array.Array, group["air_temp"])
    assert store.read_only
    assert air_temp[200, 6] == 54

    session = repo.writable_session("main")
    store = session.store()
    group = zarr.open_group(store=store)
    air_temp = cast(zarr.core.array.Array, group["air_temp"])

    air_temp[:, :] = 76
    assert session.has_uncommitted_changes
    assert session.branch == "main"
    assert session.snapshot_id == new_snapshot_id

    session.discard_changes()
    assert not session.has_uncommitted_changes
    assert air_temp[200, 6] == 54

    repo.create_branch("feature", new_snapshot_id)
    session = repo.writable_session("feature")
    store = session.store()
    assert not store._read_only
    assert session.branch == "feature"

    group = zarr.open_group(store=store)
    air_temp = cast(zarr.core.array.Array, group["air_temp"])
    air_temp[:, :] = 90
    feature_snapshot_id = session.commit("commit 3")

    repo.create_tag("v1.0", feature_snapshot_id)
    session = repo.readonly_session(tag="v1.0")
    store = session.store()
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


async def test_branch_reset():
    repo = icechunk.Repository.create(
        storage=icechunk.StorageConfig.memory("test"),
        config=icechunk.RepositoryConfig(inline_chunk_threshold_bytes=1),
    )
    session = repo.writable_session("main")
    store = session.store()

    group = zarr.group(store=store, overwrite=True)
    group.create_group("a")
    prev_snapshot_id = session.commit("group a")

    session = repo.writable_session("main")
    store = session.store()

    group = zarr.open_group(store=store)
    group.create_group("b")
    session.commit("group b")

    keys = {k async for k in store.list()}
    assert "a/zarr.json" in keys
    assert "b/zarr.json" in keys

    repo.reset_branch("main", prev_snapshot_id)

    session = repo.readonly_session(branch="main")
    store = session.store()

    keys = {k async for k in store.list()}
    assert "a/zarr.json" in keys
    assert "b/zarr.json" not in keys

    assert (
        await store.get("b/zarr.json", zarr.core.buffer.default_buffer_prototype())
    ) is None


@pytest.fixture
def repo_for_rebase(tmpdir) -> icechunk.Repository:
    repo = icechunk.Repository.create(
        storage=icechunk.StorageConfig.filesystem(str(tmpdir)),
    )

    session = repo.writable_session("main")
    store = session.store()
    root = zarr.group(store=store)
    root.create_group("foo/bar")
    root.create_array("foo/bar/some-array", shape=(10, 10), dtype="i4")
    session.commit("commit 1")

    return repo


def test_rebase_user_attrs_edit_with_ours(repo_for_rebase):
    repo = repo_for_rebase

    session_a = repo.writable_session("main")
    session_b = repo.writable_session("main")
    store_a = session_a.store()
    store_b = session_b.store()

    root_a = zarr.group(store=store_a)
    array_a = cast(zarr.Array, root_a["foo/bar/some-array"])
    array_a.attrs["repo"] = 1
    session_a.commit("update array")

    root_b = zarr.group(store=store_b)
    array_b = cast(zarr.Array, root_b["foo/bar/some-array"])
    array_b.attrs["repo"] = 2

    with pytest.raises(icechunk.ConflictError):
        session_b.commit("update array")

    solver = icechunk.BasicConflictSolver(
        on_user_attributes_conflict=icechunk.VersionSelection.UseOurs,
    )

    try:
        session_b.rebase(solver)
        session_b.commit("after conflict")
    except icechunk.RebaseFailed as e:
        print(e)
        for conflict in e.conflicts:
            print(conflict)

    assert array_b.attrs["repo"] == 2
