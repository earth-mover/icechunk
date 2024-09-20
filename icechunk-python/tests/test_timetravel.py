import zarr

import icechunk


async def test_timetravel():
    store = await icechunk.IcechunkStore.from_config(
        config={"storage": {"type": "in_memory"}, "dataset": {}}, mode="w"
    )

    group = zarr.group(store=store, overwrite=True)
    air_temp = group.create_array("air_temp", shape=(1000, 1000), chunk_shape=(100, 100), dtype="i4")

    air_temp[:, :] = 42
    assert air_temp[200, 6] == 42

    snapshot_id = await store.commit("Initial commit")

    air_temp[:, :] = 54
    assert air_temp[200, 6] == 54

    new_snapshot_id = await store.commit("update air temp")

    await store.checkout(snapshot_id=snapshot_id)
    assert air_temp[200, 6] == 42

    await store.checkout(snapshot_id=new_snapshot_id)
    assert air_temp[200, 6] == 54

    await store.checkout(branch="main")
    air_temp[:, :] = 76
    assert store.has_uncommitted_changes
    assert store.branch == "main"
    assert store.snapshot_id == new_snapshot_id

    await store.reset()
    assert not store.has_uncommitted_changes
    assert air_temp[200, 6] == 54

    await store.new_branch("feature")
    assert store.branch == "feature"
    air_temp[:, :] = 90
    feature_snapshot_id = await store.commit("update air temp")
    await store.tag("v1.0", feature_snapshot_id)

    await store.checkout(tag="v1.0")
    assert store.branch == None
    assert air_temp[200, 6] == 90
