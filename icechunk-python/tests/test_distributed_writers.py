import asyncio
import time
from dataclasses import dataclass
from typing import cast

import icechunk
import numpy as np
import zarr
from dask.distributed import Client


@dataclass
class Task:
    # fixme: useee StorageConfig and StoreConfig once those are pickable
    storage_config: dict
    store_config: dict
    area: tuple[slice, slice]
    seed: int


# We create a 2-d array with this many chunks along each direction
CHUNKS_PER_DIM = 10

# Each chunk is CHUNK_DIM_SIZE x CHUNK_DIM_SIZE floats
CHUNK_DIM_SIZE = 10

# We split the writes in tasks, each task does this many chunks
CHUNKS_PER_TASK = 2


def mk_store(mode: str, task: Task):
    storage_config = icechunk.StorageConfig.s3_from_config(
        **task.storage_config,
        credentials=icechunk.S3Credentials(
            access_key_id="minio123",
            secret_access_key="minio123",
        ),
    )
    store_config = icechunk.StoreConfig(**task.store_config)

    store = icechunk.IcechunkStore.open(
        storage=storage_config,
        mode="a",
        config=store_config,
    )

    return store


def generate_task_array(task: Task):
    np.random.seed(task.seed)
    nx = len(range(*task.area[0].indices(1000)))
    ny = len(range(*task.area[1].indices(1000)))
    return np.random.rand(nx, ny)


async def execute_task(task: Task):
    store = mk_store("w", task)

    group = zarr.group(store=store, overwrite=False)
    array = cast(zarr.Array, group["array"])
    array[task.area] = generate_task_array(task)
    return store.change_set_bytes()


def run_task(task: Task):
    return asyncio.run(execute_task(task))


async def test_distributed_writers():
    """Write to an array using uncoordinated writers, distributed via Dask.

    We create a big array, and then we split into workers, each worker gets
    an area, where it writes random data with a known seed. Each worker
    returns the bytes for its ChangeSet, then the coordinator (main thread)
    does a distributed commit. When done, we open the store again and verify
    we can write everything we have written.
    """

    client = Client(n_workers=8)
    storage_config = {
        "bucket": "testbucket",
        "prefix": "python-distributed-writers-test__" + str(time.time()),
        "endpoint_url": "http://localhost:9000",
        "region": "us-east-1",
        "allow_http": True,
    }
    store_config = {"inline_chunk_threshold_bytes": 5}

    ranges = [
        (
            slice(
                x,
                min(
                    x + CHUNKS_PER_TASK * CHUNK_DIM_SIZE,
                    CHUNK_DIM_SIZE * CHUNKS_PER_DIM,
                ),
            ),
            slice(
                y,
                min(
                    y + CHUNKS_PER_TASK * CHUNK_DIM_SIZE,
                    CHUNK_DIM_SIZE * CHUNKS_PER_DIM,
                ),
            ),
        )
        for x in range(
            0, CHUNK_DIM_SIZE * CHUNKS_PER_DIM, CHUNKS_PER_TASK * CHUNK_DIM_SIZE
        )
        for y in range(
            0, CHUNK_DIM_SIZE * CHUNKS_PER_DIM, CHUNKS_PER_TASK * CHUNK_DIM_SIZE
        )
    ]
    tasks = [
        Task(
            storage_config=storage_config,
            store_config=store_config,
            area=area,
            seed=idx,
        )
        for idx, area in enumerate(ranges)
    ]
    store = mk_store("r+", tasks[0])
    group = zarr.group(store=store, overwrite=True)

    n = CHUNKS_PER_DIM * CHUNK_DIM_SIZE
    array = group.create_array(
        "array",
        shape=(n, n),
        chunk_shape=(CHUNK_DIM_SIZE, CHUNK_DIM_SIZE),
        dtype="f8",
        fill_value=float("nan"),
    )
    _first_snap = store.commit("array created")

    map_result = client.map(run_task, tasks)
    change_sets_bytes = client.gather(map_result)

    # we can use the current store as the commit coordinator, because it doesn't have any pending changes,
    # all changes come from the tasks, Icechunk doesn't care about where the changes come from, the only
    # important thing is to not count changes twice
    commit_res = store.distributed_commit("distributed commit", change_sets_bytes)
    assert commit_res

    # Lets open a new store to verify the results
    store = mk_store("r", tasks[0])
    all_keys = [key async for key in store.list_prefix("/")]
    assert (
        len(all_keys) == 1 + 1 + CHUNKS_PER_DIM * CHUNKS_PER_DIM
    )  # group meta + array meta + each chunk

    group = zarr.group(store=store, overwrite=False)

    for task in tasks:
        actual = array[task.area]
        expected = generate_task_array(task)
        np.testing.assert_array_equal(actual, expected)
