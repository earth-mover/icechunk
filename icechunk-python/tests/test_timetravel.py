import icechunk
import zarr
import zarr.core
import zarr.core.buffer


def test_timetravel():
    store = icechunk.IcechunkStore.create(
        storage=icechunk.StorageConfig.memory("test"),
        config=icechunk.StoreConfig(inline_chunk_threshold_bytes=1),
        read_only=False,
    )

    group = zarr.group(store=store, overwrite=True)
    air_temp = group.create_array(
        "air_temp", shape=(1000, 1000), chunk_shape=(100, 100), dtype="i4"
    )

    air_temp[:, :] = 42
    assert air_temp[200, 6] == 42

    snapshot_id = store.commit("commit 1")
    assert not store._read_only

    air_temp[:, :] = 54
    assert air_temp[200, 6] == 54

    new_snapshot_id = store.commit("commit 2")

    store.checkout(snapshot_id=snapshot_id)
    assert store.read_only
    assert air_temp[200, 6] == 42

    store.checkout(snapshot_id=new_snapshot_id)
    assert store.read_only
    assert air_temp[200, 6] == 54

    store.checkout(branch="main")

    store.set_writeable()
    assert not store.read_only

    air_temp[:, :] = 76
    assert store.has_uncommitted_changes
    assert store.branch == "main"
    assert store.snapshot_id == new_snapshot_id

    store.reset()
    assert not store.has_uncommitted_changes
    assert air_temp[200, 6] == 54

    store.new_branch("feature")
    assert not store._read_only
    assert store.branch == "feature"
    air_temp[:, :] = 90
    feature_snapshot_id = store.commit("commit 3")
    store.tag("v1.0", feature_snapshot_id)

    store.checkout(tag="v1.0")
    assert store._read_only
    assert store.branch is None
    assert air_temp[200, 6] == 90

    parents = list(store.ancestry())
    assert [snap.message for snap in parents] == [
        "commit 3",
        "commit 2",
        "commit 1",
        "Repository initialized",
    ]
    assert sorted(parents, key=lambda p: p.written_at) == list(reversed(parents))
    assert len(set([snap.id for snap in parents])) == 4


async def test_branch_reset():
    store = icechunk.IcechunkStore.create(
        storage=icechunk.StorageConfig.memory("test"),
        config=icechunk.StoreConfig(inline_chunk_threshold_bytes=1),
    )

    group = zarr.group(store=store, overwrite=True)
    group.create_group("a")
    prev_snapshot_id = store.commit("group a")
    group.create_group("b")
    store.commit("group b")

    keys = {k async for k in store.list()}
    assert "a/zarr.json" in keys
    assert "b/zarr.json" in keys

    store.reset_branch(prev_snapshot_id)

    keys = {k async for k in store.list()}
    assert "a/zarr.json" in keys
    assert "b/zarr.json" not in keys

    assert (
        await store.get("b/zarr.json", zarr.core.buffer.default_buffer_prototype())
    ) is None
