"""
This example shows Icechunk can execute very high concurrency tasks, using
multiple machines to read and write from the same repository.

To understand all the available options run:
```
python ./high_concurrency.py --help
python ./high_concurrency.py create-repo --help
python ./high_concurrency.py read --help
```

This script uses Coiled to spawn tasks in multiple machines. You'll need
a Coiled account and login. Alternatively you can pass --local-cluster
to use only the current machine, but that will significantly limit concurrency.

We are trying to prove Icechunk repositories are "sharded" correctly. We
don't care much about the efficiency or speed of writing and reading to
the repo. We want to prove we can scale the number of concurrent reads/writes.
To that end, we use tiny, 8 byte chunks. Each element in the array is a
different chunk. This forces one request per element read. Terrible for
real world performance, but ideal to maximize number of concurrent requests.

Example usage:

Create a repository with 1M tiny chunks in AWS:

```shell
# AWS
python ./high_concurrency.py \
    --cloud s3 \
    --bucket my-bucket \
    --prefix my-bucket-prefix \
    --bucket-region us-east-1 \
    --coiled-region us-east-1 \
    --cluster-name high-concurrency-tests \
    --workers 2 \
    create-repo --chunks 1000000

# R2
python ./high_concurrency.py \
    --cloud r2 \
    --bucket my-bucket \
    --prefix my-bucket-prefix \
    --account-id abcdef0123456789 \
    --bucket-region us-east-1 \
    --coiled-region us-east-1 \
    --cluster-name high-concurrency-tests \
    --workers 2 \
    --access-key-id "$AWS_ACCESS_KEY_ID" \
    --secret-access-key "$AWS_SECRET_ACCESS_KEY" \
    create-repo --chunks 1000000

# Tigris
python ./high_concurrency.py \
    --cloud tigris \
    --bucket my-bucket \
    --prefix my-bucket-prefix \
    --bucket-region iad \
    --coiled-region us-east-1 \
    --cluster-name high-concurrency-tests \
    --workers 2 \
    --access-key-id "$AWS_ACCESS_KEY_ID" \
    --secret-access-key "$AWS_SECRET_ACCESS_KEY" \
    create-repo --chunks 1000000
```

Then you can use high concurrency to read from
those repositories for 30 seconds.

```shell
# AWS
python ./high_concurrency.py \
    --cloud s3 \
    --bucket my-bucket \
    --prefix my-bucket-prefix \
    --bucket-region us-east-1 \
    --coiled-region us-east-1 \
    --cluster-name high-concurrency-tests \
    --workers 20 \
    read --duration 30

# R2
python ./high_concurrency.py \
    --cloud r2 \
    --bucket my-bucket \
    --prefix my-bucket-prefix \
    --account-id abcdef0123456789 \
    --bucket-region us-east-1 \
    --coiled-region us-east-1 \
    --cluster-name high-concurrency-tests \
    --workers 20 \
    --access-key-id "$AWS_ACCESS_KEY_ID" \
    --secret-access-key "$AWS_SECRET_ACCESS_KEY" \
    read --duration 30

# Tigris
python ./high_concurrency.py \
    --cloud tigris \
    --bucket my-bucket \
    --prefix my-bucket-prefix \
    --bucket-region iad \
    --coiled-region us-east-1 \
    --cluster-name high-concurrency-tests \
    --workers 20 \
    --access-key-id "$AWS_ACCESS_KEY_ID" \
    --secret-access-key "$AWS_SECRET_ACCESS_KEY" \
    read --duration 30
```
"""

import argparse
import random
import time
from dataclasses import dataclass
from typing import Any, cast

import numpy as np

import dask.array as da
import icechunk
import icechunk.dask
import zarr
from dask.distributed import Client, get_client
from dask.distributed import print as dprint


@dataclass
class Task:
    """A read task.

    Will execute reads of read_size elements, notifying every notify_every
    in a pub/sub topic. It will run for duration_sec and then finish.
    """

    session: icechunk.Session
    duration_sec: float
    zarr_concurrency: int
    read_size: int
    notify_every: float = 1


topic_name = "progress"
"The topic used for communication between the read workers and the coordinator"


def total_tasks(client: Client) -> int:
    """
    Calculate the total number of available threads.

    We will launch a read task per thread
    """
    return sum(threads for (_, threads) in client.ncores().items())


def repository_config() -> icechunk.RepositoryConfig:
    """Return the Icechunk repo configuration.

    We lower the default to make sure we write chunks and not inline them.
    """
    config = icechunk.RepositoryConfig.default()
    config.inline_chunk_threshold_bytes = 1
    return config


def make_s3_repo(bucket: str, prefix: str, region: str) -> icechunk.Repository:
    repo = icechunk.Repository.open_or_create(
        storage=icechunk.s3_storage(bucket=bucket, prefix=prefix, region=region),
        config=repository_config(),
    )
    return repo


def make_gcs_repo(bucket: str, prefix: str, region: str) -> icechunk.Repository:
    from coiled.credentials.google import CoiledShippedCredentials

    repo = icechunk.Repository.open_or_create(
        storage=icechunk.gcs_storage(
            bucket=bucket, prefix=prefix, bearer_token=CoiledShippedCredentials().token
        ),
        config=repository_config(),
    )
    return repo


def make_r2_repo(
    bucket: str, prefix: str, account_id: str, access_key_id: str, secret_access_key: str
) -> icechunk.Repository:
    repo = icechunk.Repository.open_or_create(
        storage=icechunk.r2_storage(
            bucket=bucket,
            prefix=prefix,
            account_id=account_id,
            access_key_id=access_key_id,
            secret_access_key=secret_access_key,
        ),
        config=repository_config(),
    )
    return repo


def make_tigris_repo(
    bucket: str, prefix: str, region: str, access_key_id: str, secret_access_key: str
) -> icechunk.Repository:
    repo = icechunk.Repository.open_or_create(
        storage=icechunk.tigris_storage(
            bucket=bucket,
            prefix=prefix,
            region=region,
            access_key_id=access_key_id,
            secret_access_key=secret_access_key,
        ),
        config=repository_config(),
    )
    return repo


def make_repo(args: argparse.Namespace) -> icechunk.Repository:
    match args.cloud:
        case "s3":
            return make_s3_repo(args.bucket, args.prefix, args.bucket_region)
        case "gcs":
            return make_gcs_repo(args.bucket, args.prefix, args.bucket_region)
        case "r2":
            return make_r2_repo(
                args.bucket,
                args.prefix,
                args.account_id,
                args.access_key_id,
                args.secret_access_key,
            )
        case "tigris":
            return make_tigris_repo(
                args.bucket,
                args.prefix,
                args.bucket_region,
                args.access_key_id,
                args.secret_access_key,
            )
        case _:
            raise ValueError(f"Invalid cloud: {args.cloud}")


def generate_repo_data(args: argparse.Namespace) -> str:
    """Use Dask to write random data to /array, a 1D-array."""
    repo = make_repo(args)
    chunks = args.chunks
    dask_chunk_factor = 100

    shape = (args.chunk_size * chunks,)
    dask_chunks = (args.chunk_size * dask_chunk_factor,)
    dask_array = da.random.random(shape, chunks=dask_chunks)

    session = repo.writable_session("main")
    with session.allow_pickling():
        store = session.store
        group = zarr.group(store=store, overwrite=True)
        chunk_shape = (args.chunk_size,)

        zarray = group.create_array(
            "array",
            shape=shape,
            chunks=chunk_shape,
            dtype="f8",
            fill_value=0,
            compressors=None,
        )
        start = time.monotonic()
        icechunk.dask.store_dask(session, sources=[dask_array], targets=[zarray])
    dprint(f"Array written in {time.monotonic() - start} seconds")
    return session.commit("wrote the array")


def read_task(task: Task) -> int:
    """Repeateadly read random parts of /array.

    It runs for the period of time indicated in the task, notifying
    periodically via a pub/sub topic the number of reads executed.
    """
    zarr.config.set({"async.concurrency": task.zarr_concurrency})
    client = get_client()
    store = task.session.store
    group = zarr.open_group(store=store, mode="r")
    array = cast(zarr.Array, group["array"])
    chunk_size = array.chunks[0]
    max_index = array.shape[0] - 1
    read_size = min(task.read_size, max_index + 1)
    total_reads = 0

    start = time.monotonic()
    while time.monotonic() - start < task.duration_sec:
        last_notified = time.monotonic()
        reads_done = 0
        while time.monotonic() - last_notified < task.notify_every:
            offset = random.randint(0, max_index - read_size + 1)
            data = array[offset : offset + read_size][:]
            assert np.max(data) <= 1
            assert len(data) == read_size

            # we add the total number of chunks read
            first_chunk = offset // chunk_size
            last_chunk = (offset + read_size - 1) // chunk_size
            chunks_read = last_chunk - first_chunk + 1

            reads_done += chunks_read
            total_reads += chunks_read
        client.log_event(topic_name, reads_done)
    return total_reads


class ProgressTracker:
    def __init__(self) -> None:
        self.executed = 0
        self.started_at = time.monotonic()
        self.last_log = self.started_at

    def __call__(self, event: tuple[Any, int]) -> None:
        (_timestamp, reads_done) = event
        self.executed += reads_done
        now = time.monotonic()
        if now - self.last_log > 1:
            self.report()
            self.last_log = now

    def report(self) -> None:
        dprint(
            f"Average speed: {self.executed / (time.monotonic() - self.started_at)} req/sec"
        )


def mk_client(
    local: bool,
    cluster_name: str,
    workspace: str | None,
    n_workers: int,
    n_threads: int | None,
    region: str | None,
    access_key_id: str | None,
    secret_access_key: str | None,
    shutdown_on_close: bool,
) -> Client:
    if local:
        client = Client(n_workers=n_workers, threads_per_worker=n_threads)
    else:
        from coiled import Cluster

        credentials = None if access_key_id and secret_access_key else "local"
        cluster = Cluster(
            n_workers=n_workers,
            worker_options={"nthreads": n_threads},
            workspace=workspace,
            region=region,
            name=cluster_name,
            spot_policy="spot_with_fallback",
            credentials=credentials,
            environ={"ICECHUNK_LOG": "trace", "NO_COLOR": "true"},
            shutdown_on_close=shutdown_on_close,
        )
        client = cluster.get_client()
    return client


def do_reads(args: argparse.Namespace) -> None:
    client = mk_client(
        local=args.local_cluster,
        cluster_name=args.cluster_name,
        workspace=args.coiled_workspace,
        n_workers=args.workers,
        n_threads=args.threads,
        region=args.coiled_region,
        access_key_id=args.access_key_id,
        secret_access_key=args.secret_access_key,
        shutdown_on_close=args.shutdown_cluster,
    )
    dprint(f"Total workers: {total_tasks(client)}")
    repo = make_repo(args)
    session = repo.readonly_session("main")
    tasks = [
        Task(
            duration_sec=args.duration,
            session=session,
            zarr_concurrency=args.zarr_concurrency,
            read_size=args.read_size,
        )
        for _ in range(total_tasks(client))
    ]

    tracker = ProgressTracker()
    client.subscribe_topic(topic_name, tracker)
    futures = client.map(read_task, tasks, pure=False)

    client.gather(futures)
    tracker.report()


def do_writes(args: argparse.Namespace) -> None:
    client = mk_client(
        local=args.local_cluster,
        cluster_name=args.cluster_name,
        workspace=args.coiled_workspace,
        n_workers=args.workers,
        n_threads=args.threads,
        region=args.coiled_region,
        access_key_id=args.access_key_id,
        secret_access_key=args.secret_access_key,
        shutdown_on_close=args.shutdown_cluster,
    )
    dprint(f"Total workers: {total_tasks(client)}")
    start = time.monotonic()
    generate_repo_data(args)
    dprint(f"Total time for writes and commit: {time.monotonic() - start} seconds")


def main() -> None:
    global_parser = argparse.ArgumentParser(prog="high-read-concurrency")
    global_parser.add_argument(
        "--cloud",
        choices=["s3", "gcs", "r2", "tigris"],
        help="Object store where to the repository is placed",
        required=True,
    )
    global_parser.add_argument(
        "--bucket",
        type=str,
        # help="Object store where to the repository is placed",
        required=True,
    )
    global_parser.add_argument(
        "--prefix",
        type=str,
        # help="Object store where to the repository is placed",
        required=True,
    )
    global_parser.add_argument(
        "--bucket-region",
        type=str,
        # help="Object store where to the repository is placed",
        required=False,
    )
    global_parser.add_argument(
        "--access-key-id",
        type=str,
        # help="Object store where to the repository is placed",
        required=False,
    )
    global_parser.add_argument(
        "--secret-access-key",
        type=str,
        # help="Object store where to the repository is placed",
        required=False,
    )
    global_parser.add_argument(
        "--coiled-region",
        type=str,
        # help="Object store where to the repository is placed",
        required=True,
    )
    global_parser.add_argument(
        "--coiled-workspace",
        type=str,
        required=False,
    )
    global_parser.add_argument(
        "--account-id",
        type=str,
        # help="Object store where to the repository is placed",
        required=False,
    )
    global_parser.add_argument(
        "--cluster-name",
        type=str,
        default="high-read-concurrency-tests",
        # help="Object store where to the repository is placed",
        required=False,
    )
    global_parser.add_argument(
        "--shutdown-cluster",
        action=argparse.BooleanOptionalAction,
        default=True,
        # help="Object store where to the repository is placed",
        required=False,
    )
    global_parser.add_argument(
        "--local-cluster",
        action=argparse.BooleanOptionalAction,
        default=False,
        # help="Object store where to the repository is placed",
        required=False,
    )
    global_parser.add_argument(
        "--workers",
        type=int,
        # help="Object store where to the repository is placed",
        required=True,
    )
    global_parser.add_argument(
        "--threads",
        type=int,
        # help="Object store where to the repository is placed",
        required=False,
    )
    global_parser.add_argument(
        "--zarr-concurrency",
        type=int,
        default=32,
        # help="Object store where to the repository is placed",
        required=False,
    )

    subparsers = global_parser.add_subparsers(title="subcommands", required=True)

    create_parser = subparsers.add_parser("create-repo", help="create repo and array")
    create_parser.add_argument(
        "--chunks", type=int, help="number of chunks in the array", default=1_000_000
    )
    create_parser.add_argument(
        "--chunk-size", type=int, help="number of elements per chunk", default=1
    )
    create_parser.set_defaults(command="create-repo")

    read_parser = subparsers.add_parser("read", help="execute reads")
    read_parser.add_argument(
        "--duration", type=float, help="number of seconds to execute", default=30.0
    )
    read_parser.add_argument("--read-size", type=int, default=1_000)
    read_parser.set_defaults(command="read")

    args = global_parser.parse_args()

    match args.command:
        case "create-repo":
            do_writes(args)
        case "read":
            do_reads(args)


if __name__ == "__main__":
    main()
