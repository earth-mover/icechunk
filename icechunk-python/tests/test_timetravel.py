import zarr

import icechunk


async def test_timetravel():
    store = await icechunk.IcechunkStore.from_json(
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

    await store.checkout(snapshot_id)
    assert air_temp[200, 6] == 42

    await store.checkout(new_snapshot_id)
    assert air_temp[200, 6] == 54
