#!/usr/bin/env python3

# This app records performance measurements for icechunk reads/writes orchestrated using plain tasks.
# See array.py that orchestrates similar operations using an array library.

# Orchestration
# --executor=threads, --executor=processes, with --num-workers
# --executor=dask-threads, --executor=dask-processes, --executor=dask-distributed --num-workers --threads-per-worker
# --framework=dask-array or --framework=tasks

#
import math
import os
import time
from concurrent import futures
from dataclasses import dataclass
from datetime import timedelta
from enum import StrEnum, auto
from typing import cast
from urllib.parse import urlparse

import numpy as np
import tqdm

# import typer
import zarr
from benchmarks import lib
from icechunk import ForkSession, Repository, S3Options, Storage

BRANCH_NAME = "main"


@dataclass
class Task:
    """A read/write task"""

    # The worker will use this Icechunk store to read/write to the dataset
    session: ForkSession
    # Region of the array to write to
    region: tuple[slice, ...]


@dataclass
class TaskResult:
    session: ForkSession
    time: timedelta


def generate_task_array(task: Task, shape: tuple[int, ...]) -> np.typing.ArrayLike:
    """Generates a random array with the given shape and using the seed in the Task"""
    seed = math.prod(slicer.stop for slicer in task.region)
    np.random.seed(seed)
    return np.random.rand(*shape)


def execute_write_task(task: Task) -> TaskResult:
    """Execute task as a write task.

    This will read the time coordinade from `task` and write a "pancake" in that position,
    using random data. Random data is generated using the task seed.

    Returns the Icechunk store after the write is done.

    As you can see Icechunk stores can be passed to remote workers, and returned from them.
    The reason to return the store is that we'll need all the remote stores, when they are
    done, to be able to do a single, global commit to Icechunk.
    """

    store = task.session.store
    group = zarr.group(store=store, overwrite=False)
    array = cast(zarr.Array, group["array"])
    data = generate_task_array(task, array[task.region].shape)
    assert len(task.region) == array.ndim
    tic = time.perf_counter()
    array[task.region] = data
    toc = time.perf_counter()
    return TaskResult(task.session, time=toc - tic)


def get_storage(url: str) -> Storage:
    url = urlparse(url)
    if url.netloc == "" or url.params != "" or url.query != "" or url.fragment != "":
        raise ValueError(f"Invalid {url=!r}")

    if url.scheme == "s3":
        if url.path == "":
            raise ValueError(f"Invalid {url=!r}")
        storage = Storage.new_s3(bucket=url.netloc, prefix=url.path, config=S3Options())

    elif url.scheme == "local":
        storage = Storage.new_local_filesystem(
            os.path.expanduser(f"{url.netloc}/{url.path}")
        )

    return storage


class Executor(StrEnum):
    threads = auto()
    processes = auto()
    # dask_threads = auto()
    # dask_processes = auto()
    dask_distributed = auto()


# app = typer.Typer()


# @app.command()
def read(
    # data
    url,
    # execution
    executor: Executor = Executor.threads,
    workers: int | None = None,
    threads_per_worker: int | None = None,
) -> None:
    pass


def validate_data(*, num_arrays, shape, chunks) -> None:
    if len(chunks) != len(shape):
        raise ValueError(f"{shape=!r} does not match {chunks=!r}")


def get_executor(
    *,
    executor: Executor,
    workers: int | None = None,
    threads_per_worker: int | None = None,
) -> futures.Executor:
    if executor is Executor.threads:
        return futures.ThreadPoolExecutor(workers)
    elif executor is Executor.processes:
        return futures.ProcessPoolExecutor(workers)
    elif executor is Executor.dask_distributed:
        from distributed import Client

        return Client(n_workers=workers, threads_per_worker=threads_per_worker)
    else:
        raise NotImplementedError


def initialize_store(repo: Repository, *, shape, chunks):
    session = repo.writable_session(BRANCH_NAME)
    store = session.store
    group = zarr.group(store, overwrite=True)
    group.create_array(
        "array", shape=shape, dtype=np.float32, chunks=chunks, overwrite=True
    )
    session.commit("initialized")


# @app.command()
def write(
    # data
    url: str | None = None,
    storage: Storage | None = None,
    # num_arrays: Annotated[int, typer.Option(min=1, max=1)] = 1,
    num_arrays: int = 1,
    # scale:   # TODO: [small, medium, large]
    shape: list[int] = [3, 720, 1440],  # noqa: B006
    chunks: list[int] = [1, -1, -1],  # noqa: B006
    task_nchunks: int = 1,
    # execution
    executor: Executor = Executor.threads,
    workers: int | None = None,
    threads_per_worker: int | None = None,
    record: bool = False,
) -> None:
    timer = lib.Timer()
    validate_data(num_arrays=num_arrays, shape=shape, chunks=chunks)
    chunks = lib.normalize_chunks(shape=shape, chunks=chunks)
    executor = get_executor(
        executor=executor, workers=workers, threads_per_worker=threads_per_worker
    )
    if storage is None:
        storage = get_storage(url)
    repo = Repository.open_or_create(storage)
    initialize_store(repo, shape=shape, chunks=chunks)

    session = repo.writable_session("main")

    task_chunk_shape = lib.get_task_chunk_shape(
        task_nchunks=task_nchunks, shape=shape, chunks=chunks
    )

    # TODO: record nchunks, chunk size in bytes for all timers?
    # TODO: record commit hash of benchmarked commit too.
    timer_context = {}
    with timer.time(op="total_write_time", **timer_context):
        # tasks =
        # print(f"submitted {len(tasks)} tasks")
        fork = session.fork()
        results = [
            f.result(timeout=5)
            for f in tqdm.tqdm(
                futures.as_completed(
                    [
                        executor.submit(
                            execute_write_task, Task(session=fork, region=slicer)
                        )
                        for slicer in lib.slices_from_chunks(shape, task_chunk_shape)
                    ]
                )
            )
        ]

    timer.diagnostics.append(
        {"op": "write_task", "runtime": tuple(r.time for r in results)}
    )

    # TODO: record time & byte size?
    with timer.time(op="merge_changeset", **timer_context):
        if not isinstance(executor, futures.ThreadPoolExecutor):
            session.merge(*(res.session for res in results))
    assert session.has_uncommitted_changes

    with timer.time(op="commit", **timer_context):
        commit_res = session.commit("committed.")
    assert commit_res

    print("Written and committed!")
    return timer


# @app.command()
def verify(
    # data
    # execution
    executor: Executor = Executor.threads,
    workers: int | None = None,
    threads_per_worker: int | None = None,
) -> None:
    pass


# @app.command()
def create(
    # data
    # execution
    executor: Executor = Executor.threads,
    workers: int | None = None,
    threads_per_worker: int | None = None,
) -> None:
    pass


# if __name__ == "__main__":
#     app()
