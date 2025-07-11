"""
This example uses Dask as a task orchestration framework
to write or update an array in an Icechunk repository.
To write an Xarray object with dask array use `icechunk.xarray.to_icechunk`

To understand all the available options run:
```
python ./examples/dask_write.py --help
python ./examples/dask_write.py create --help
python ./examples/dask_write.py update --help
python ./examples/dask_write.py verify --help
```

Example usage:

```
python ./examples/dask_write.py --url s3://my-bucket/my-icechunk-repo create --t-chunks 100000 --x-chunks 4 --y-chunks 4 --chunk-x-size 112 --chunk-y-size 112
python ./examples/dask_write.py --url s3://my-bucket/my-icechunk-repo update --t-from 0 --t-to 1500 --workers 16
python ./examples/dask_write.py --url s3://my-bucket/my-icechunk-repo verify --t-from 0 --t-to 1500 --workers 16
```

The work is split into three different commands.
* `create` initializes the repository and the array, without writing any chunks. For this example
   we chose a 3D array that simulates a dataset that needs backfilling across its time dimension.
* `update` can be called multiple times to write a number of "pancakes" to the array.
  It does so by distributing the work among Dask workers, in small tasks, one pancake per task.
  The example invocation above, will write 1,500 pancakes using 16 Dask workers.
* `verify` can read a part of the array and check that it contains the required data.

Dask can be used to read and write to Icechunk from multiple processes and machines, we just need to use a lower level
Dask API based, for example, in `map/gather`. This mechanism is what we show in this example.
"""

import argparse
from dataclasses import dataclass
from typing import Any, cast
from urllib.parse import urlparse

import numpy as np

import icechunk
import zarr
from dask.distributed import Client
from dask.distributed import print as dprint


@dataclass
class Task:
    """A task distributed to Dask workers"""

    session: (
        icechunk.Session
    )  # The worker will use this Icechunk session to read/write to the dataset
    time: (
        int  # The position in the coordinate dimension where the read/write should happen
    )
    seed: int  # An RNG seed used to generate or recreate random data for the array


def generate_task_array(task: Task, shape: tuple[int, ...]) -> np.typing.ArrayLike:
    """Generates a randm array with the given shape and using the seed in the Task"""
    np.random.seed(task.seed)
    return np.random.rand(*shape)


def execute_write_task(task: Task) -> icechunk.Session:
    """Execute task as a write task.

    This will read the time coordinade from `task` and write a "pancake" in that position,
    using random data. Random data is generated using the task seed.

    Returns the Icechunk session after the write is done.

    As you can see Icechunk session can be passed to remote workers, and returned from them.
    The reason to return the session is that we'll need all the remote session, when they are
    done, to be able to do a single, global commit to Icechunk.
    """

    session = task.session
    store = session.store

    group = zarr.group(store=store, overwrite=False)
    array = cast(zarr.Array, group["array"])
    dprint(f"Writing at t={task.time}")
    data = generate_task_array(task, array.shape[0:2])
    array[:, :, task.time] = data
    dprint(f"Writing at t={task.time} done")
    return session


def execute_read_task(task: Task) -> None:
    """Execute task as a read task.

    This will read the time coordinade from `task` and read a "pancake" in that position.
    Then it will assert the data is valid by re-generating the random data from the passed seed.

    As you can see Icechunk sessions can be passed to remote workers.
    """

    session = task.session
    store = session.store
    group = zarr.group(store=store, overwrite=False)
    array = cast(zarr.Array, group["array"])

    actual = array[:, :, task.time]
    expected = generate_task_array(task, array.shape[0:2])
    np.testing.assert_array_equal(actual, expected)
    dprint(f"t={task.time} verified")


def storage_config(args: argparse.Namespace) -> dict[str, Any]:
    """Return the Icechunk S3 configuration map"""
    bucket = args.url.netloc
    prefix = args.url.path[1:]
    return {
        "bucket": bucket,
        "prefix": prefix,
        "region": "us-east-1",
    }


def repository_config(args: argparse.Namespace) -> icechunk.RepositoryConfig:
    """Return the Icechunk repo configuration.

    We lower the default to make sure we write chunks and not inline them.
    """
    config = icechunk.RepositoryConfig.default()
    config.inline_chunk_threshold_bytes = 1
    return config


def create(args: argparse.Namespace) -> None:
    """Execute the create subcommand.

    Creates an Icechunk repo, a root group and an array named "array"
    with the shape passed as arguments.

    Commits the Icechunk repository when done.
    """
    repo = icechunk.Repository.create(
        storage=icechunk.s3_storage(**storage_config(args)),
        config=repository_config(args),
    )

    session = repo.writable_session("main")
    store = session.store

    group = zarr.group(store=store, overwrite=True)
    shape = (
        args.x_chunks * args.chunk_x_size,
        args.y_chunks * args.chunk_y_size,
        args.t_chunks * 1,
    )
    chunk_shape = (args.chunk_x_size, args.chunk_y_size, 1)

    group.create_array(
        "array",
        shape=shape,
        chunks=chunk_shape,
        dtype="f8",
        fill_value=float("nan"),
    )
    first_snapshot = session.commit("array created")
    print(f"Array initialized, snapshot {first_snapshot}")


def update(args: argparse.Namespace) -> None:
    """Execute the update subcommand.

    Uses Dask to write chunks to the Icechunk repository. Currently Icechunk cannot
    use the Dask array API (see https://github.com/earth-mover/icechunk/issues/185) but we
    can still use a lower level API to do the writes:
    * We split the work into small `Task`s, one 'pancake' per task, at a given t coordinate.
    * We use Dask's `map` to ship the `Task` to a worker
    * The `Task` includes a copy of the Icechunk Session, so workers can do the writes
    * When workers are done, they send their Session back
    * When all workers are done (Dask's `gather`), we take all Sessions and do a distributed commit in Icechunk
    """

    repo = icechunk.Repository.open(
        storage=icechunk.s3_storage(**storage_config(args)),
        config=repository_config(args),
    )

    session = repo.writable_session("main")
    fork = session.fork()

    tasks = [
        Task(
            session=fork,
            time=time,
            seed=time,
        )
        for time in range(args.t_from, args.t_to, 1)
    ]

    client = Client(n_workers=args.workers, threads_per_worker=1)

    map_result = client.map(execute_write_task, tasks)
    worker_sessions = client.gather(map_result)

    print("Starting distributed commit")
    # we can use the current session as the commit coordinator, because it doesn't have any pending changes,
    # all changes come from the tasks, Icechunk doesn't care about where the changes come from, the only
    # important thing is to not count changes twice
    session.merge(*worker_sessions)
    commit_res = session.commit("distributed commit")
    assert commit_res
    print("Distributed commit done")


def verify(args: argparse.Namespace) -> None:
    """Execute the verify subcommand.

    Uses Dask to read and verify chunks from the Icechunk repository. Currently Icechunk cannot
    use the Dask array API (see https://github.com/earth-mover/icechunk/issues/185) but we
    can still use a lower level API to do the verification:
    * We split the work into small `Task`s, one 'pancake' per task, at a given t coordinate.
    * We use Dask's `map` to ship the `Task` to a worker
    * The `Task` includes a copy of the Icechunk Store, so workers can do the Icechunk reads
    """
    repo = icechunk.Repository.open(
        storage=icechunk.s3_storage(**storage_config(args)),
        config=repository_config(args),
    )

    session = repo.writable_session("main")
    store = session.store

    group = zarr.group(store=store, overwrite=False)
    array = cast(zarr.Array, group["array"])
    print(f"Found an array with shape: {array.shape}")

    tasks = [
        Task(
            session=session,
            time=time,
            seed=time,
        )
        for time in range(args.t_from, args.t_to, 1)
    ]

    client = Client(n_workers=args.workers, threads_per_worker=1)

    map_result = client.map(execute_read_task, tasks)
    client.gather(map_result)
    print("done, all good")


def main() -> None:
    """Main entry point for the script.

    Parses arguments and delegates to a subcommand.
    """

    global_parser = argparse.ArgumentParser(prog="dask_write")
    global_parser.add_argument(
        "--url",
        type=str,
        help="url for the repository: s3://bucket/optional-prefix/repository-name",
        required=True,
    )
    subparsers = global_parser.add_subparsers(title="subcommands", required=True)

    create_parser = subparsers.add_parser("create", help="create repo and array")
    create_parser.add_argument(
        "--x-chunks", type=int, help="number of chunks in the x dimension", default=4
    )
    create_parser.add_argument(
        "--y-chunks", type=int, help="number of chunks in the y dimension", default=4
    )
    create_parser.add_argument(
        "--t-chunks", type=int, help="number of chunks in the t dimension", default=1000
    )
    create_parser.add_argument(
        "--chunk-x-size",
        type=int,
        help="size of chunks in the x dimension",
        default=112,
    )
    create_parser.add_argument(
        "--chunk-y-size",
        type=int,
        help="size of chunks in the y dimension",
        default=112,
    )
    create_parser.set_defaults(command="create")

    update_parser = subparsers.add_parser("update", help="add chunks to the array")
    update_parser.add_argument(
        "--t-from",
        type=int,
        help="time position where to start adding chunks (included)",
        required=True,
    )
    update_parser.add_argument(
        "--t-to",
        type=int,
        help="time position where to stop adding chunks (not included)",
        required=True,
    )
    update_parser.add_argument(
        "--workers", type=int, help="number of workers to use", required=True
    )
    update_parser.set_defaults(command="update")

    verify_parser = subparsers.add_parser("verify", help="verify array chunks")
    verify_parser.add_argument(
        "--t-from",
        type=int,
        help="time position where to start adding chunks (included)",
        required=True,
    )
    verify_parser.add_argument(
        "--t-to",
        type=int,
        help="time position where to stop adding chunks (not included)",
        required=True,
    )
    verify_parser.add_argument(
        "--workers", type=int, help="number of workers to use", required=True
    )
    verify_parser.set_defaults(command="verify")

    args = global_parser.parse_args()
    url = urlparse(args.url, "s3")
    if (
        url.scheme != "s3"
        or url.netloc == ""
        or url.path == ""
        or url.params != ""
        or url.query != ""
        or url.fragment != ""
    ):
        raise ValueError(f"Invalid url {args.url}")

    args.url = url

    match args.command:
        case "create":
            create(args)
        case "update":
            update(args)
        case "verify":
            verify(args)


if __name__ == "__main__":
    main()
