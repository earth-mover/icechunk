"""
This scripts uses dask to write a big array to S3.

Example usage:

```
python ./examples/dask_write.py create --name test --t-chunks 100000
python ./examples/dask_write.py update --name test --t-from 0 --t-to 1500 --workers 16 --max-sleep 0.1 --min-sleep 0 --sleep-tasks 300
python ./examples/dask_write.py verify --name test --t-from 0 --t-to 1500 --workers 16
```
"""
import argparse
from dataclasses import dataclass
import time
import asyncio
import zarr
from dask.distributed import Client
import numpy as np

import icechunk


@dataclass
class Task:
    # fixme: useee StorageConfig and StoreConfig once those are pickable
    storage_config: dict
    store_config: dict
    time: int
    seed: int
    sleep: float


async def mk_store(mode: str, task: Task):
    storage_config = icechunk.StorageConfig.s3_from_env(**task.storage_config)
    store_config = icechunk.StoreConfig(**task.store_config)

    store = await icechunk.IcechunkStore.open(
        storage=storage_config,
        mode="a",
        config=store_config,
    )

    return store


def generate_task_array(task: Task, shape):
    np.random.seed(task.seed)
    return np.random.rand(*shape)


async def execute_write_task(task: Task):
    from zarr import config
    config.set({"async.concurrency": 10})

    store = await mk_store("w", task)

    group = zarr.group(store=store, overwrite=False)
    array = group["array"]
    print(f"Writing at t={task.time}")
    data = generate_task_array(task, array.shape[0:2])
    array[:,:,task.time] = data
    print(f"Writing at t={task.time} done")
    if task.sleep != 0:
        print(f"Sleeping for {task.sleep} secs")
        time.sleep(task.sleep)
    return store.change_set_bytes()


async def execute_read_task(task: Task):
    print(f"Reading t={task.time}")
    store = await mk_store("r", task)
    group = zarr.group(store=store, overwrite=False)
    array = group["array"]

    actual = array[:,:,task.time]
    expected = generate_task_array(task, array.shape[0:2])
    np.testing.assert_array_equal(actual, expected)

def run_write_task(task: Task):
    return asyncio.run(execute_write_task(task))

def run_read_task(task: Task):
    return asyncio.run(execute_read_task(task))

def storage_config(args):
    prefix = f"seba-tests/icechunk/{args.name}"
    return {
        "bucket":"arraylake-test",
        "prefix": prefix,
    }

def store_config(args):
    return {"inline_chunk_threshold_bytes": 1}

async def create(args):
    store = await icechunk.IcechunkStore.open(
        storage=icechunk.StorageConfig.s3_from_env(**storage_config(args)),
        mode="w",
        config=icechunk.StoreConfig(**store_config(args)),
    )

    group = zarr.group(store=store, overwrite=True)
    shape = (args.x_chunks * args.chunk_x_size, args.y_chunks * args.chunk_y_size, args.t_chunks * 1)
    chunk_shape = (args.chunk_x_size, args.chunk_y_size, 1)

    array = group.create_array(
        "array",
        shape=shape,
        chunk_shape=chunk_shape,
        dtype="f8",
        fill_value=float("nan"),
    )
    _first_snap = await store.commit("array created")
    print("Array initialized")

async def update(args):
    storage_conf = storage_config(args)
    store_conf = store_config(args)

    store = await icechunk.IcechunkStore.open(
        storage=icechunk.StorageConfig.s3_from_env(**storage_conf),
        mode="r+",
        config=icechunk.StoreConfig(**store_conf),
    )

    group = zarr.group(store=store, overwrite=False)
    array = group["array"]
    print(f"Found an array of shape: {array.shape}")

    tasks = [
        Task(
            storage_config=storage_conf,
            store_config=store_conf,
            time=time,
            seed=time,
            sleep=max(0, args.max_sleep - ((args.max_sleep - args.min_sleep)/args.sleep_tasks * time))
        )
        for time in range(args.t_from, args.t_to, 1)
    ]

    client = Client(n_workers=args.workers, threads_per_worker=1)

    map_result = client.map(run_write_task, tasks)
    change_sets_bytes = client.gather(map_result)

    print("Starting distributed commit")
    # we can use the current store as the commit coordinator, because it doesn't have any pending changes,
    # all changes come from the tasks, Icechunk doesn't care about where the changes come from, the only
    # important thing is to not count changes twice
    commit_res = await store.distributed_commit("distributed commit", change_sets_bytes)
    assert commit_res
    print("Distributed commit done")

async def verify(args):
    storage_conf = storage_config(args)
    store_conf = store_config(args)

    store = await icechunk.IcechunkStore.open(
        storage=icechunk.StorageConfig.s3_from_env(**storage_conf),
        mode="r+",
        config=icechunk.StoreConfig(**store_conf),
    )

    group = zarr.group(store=store, overwrite=False)
    array = group["array"]
    print(f"Found an array of shape: {array.shape}")

    tasks = [
        Task(
            storage_config=storage_conf,
            store_config=store_conf,
            time=time,
            seed=time,
            sleep=0
        )
        for time in range(args.t_from, args.t_to, 1)
    ]

    client = Client(n_workers=args.workers, threads_per_worker=1)

    map_result = client.map(run_read_task, tasks)
    client.gather(map_result)
    print(f"done, all good")



async def distributed_write():
    """Write to an array using uncoordinated writers, distributed via Dask.

    We create a big array, and then we split into workers, each worker gets
    an area, where it writes random data with a known seed. Each worker
    returns the bytes for its ChangeSet, then the coordinator (main thread)
    does a distributed commit. When done, we open the store again and verify
    we can write everything we have written.
    """

    global_parser = argparse.ArgumentParser(prog="dask_write")
    subparsers = global_parser.add_subparsers(title="subcommands", required=True)

    create_parser = subparsers.add_parser("create", help="create repo and array")
    create_parser.add_argument("--x-chunks", type=int, help="number of chunks in the x dimension", default=4)
    create_parser.add_argument("--y-chunks", type=int, help="number of chunks in the y dimension", default=4)
    create_parser.add_argument("--t-chunks", type=int, help="number of chunks in the t dimension", default=1000)
    create_parser.add_argument("--chunk-x-size", type=int, help="size of chunks in the x dimension", default=112)
    create_parser.add_argument("--chunk-y-size", type=int, help="size of chunks in the y dimension", default=112)
    create_parser.add_argument("--name", type=str, help="repository name", required=True)
    create_parser.set_defaults(command="create")


    update_parser = subparsers.add_parser("update", help="add chunks to the array")
    update_parser.add_argument("--t-from", type=int, help="time position where to start adding chunks (included)", required=True)
    update_parser.add_argument("--t-to", type=int, help="time position where to stop adding chunks (not included)", required=True)
    update_parser.add_argument("--workers", type=int, help="number of workers to use", required=True)
    update_parser.add_argument("--name", type=str, help="repository name", required=True)
    update_parser.add_argument("--max-sleep", type=float, help="initial tasks sleep by these many seconds", default=0.3)
    update_parser.add_argument("--min-sleep", type=float, help="last task that sleeps does it by these many seconds, a ramp from --max-sleep", default=0)
    update_parser.add_argument("--sleep-tasks", type=int, help="this many tasks sleep", default=0.3)
    update_parser.set_defaults(command="update")

    update_parser = subparsers.add_parser("verify", help="verify array chunks")
    update_parser.add_argument("--t-from", type=int, help="time position where to start adding chunks (included)", required=True)
    update_parser.add_argument("--t-to", type=int, help="time position where to stop adding chunks (not included)", required=True)
    update_parser.add_argument("--workers", type=int, help="number of workers to use", required=True)
    update_parser.add_argument("--name", type=str, help="repository name", required=True)
    update_parser.set_defaults(command="verify")

    args = global_parser.parse_args()
    match args.command:
        case "create":
            await create(args)
        case "update":
            await update(args)
        case "verify":
            await verify(args)

if __name__ == "__main__":
    asyncio.run(distributed_write())

