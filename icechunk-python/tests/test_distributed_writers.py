import time
import warnings
from concurrent.futures import ThreadPoolExecutor
from typing import cast

import pytest

import dask.array
import icechunk
import zarr
from dask.array.utils import assert_eq
from icechunk.dask import store_dask

# We create a 2-d array with this many chunks along each direction
CHUNKS_PER_DIM = 10

# Each chunk is CHUNK_DIM_SIZE x CHUNK_DIM_SIZE floats
CHUNK_DIM_SIZE = 10

# We split the writes in tasks, each task does this many chunks
CHUNKS_PER_TASK = 2


def mk_repo() -> icechunk.Repository:
    storage = icechunk.s3_storage(
        endpoint_url="http://localhost:9000",
        allow_http=True,
        region="us-east-1",
        bucket="testbucket",
        prefix="python-distributed-writers-test__" + str(time.time()),
        access_key_id="minio123",
        secret_access_key="minio123",
    )
    repo_config = icechunk.RepositoryConfig.default()
    repo_config.inline_chunk_threshold_bytes = 5
    repo = icechunk.Repository.open_or_create(
        storage=storage,
        config=repo_config,
    )

    return repo


async def test_distributed_writers() -> None:
    """Write to an array using uncoordinated writers, distributed via Dask.

    We create a big array, and then we split into workers, each worker gets
    an area, where it writes random data with a known seed. Each worker
    returns the bytes for its ChangeSet, then the coordinator (main thread)
    does a distributed commit. When done, we open the store again and verify
    we can write everything we have written.
    """
    pytest.skip(reason="deadlock")

    repo = mk_repo()
    session = repo.writable_session(branch="main")
    store = session.store

    shape = (CHUNKS_PER_DIM * CHUNK_DIM_SIZE,) * 2
    dask_chunks = (CHUNK_DIM_SIZE * CHUNKS_PER_TASK,) * 2
    dask_array = dask.array.random.random(shape, chunks=dask_chunks)
    group = zarr.group(store=store, overwrite=True)

    zarray = group.create_array(
        "array",
        shape=shape,
        chunks=(CHUNK_DIM_SIZE, CHUNK_DIM_SIZE),
        dtype="f8",
        fill_value=float("nan"),
    )
    _first_snap = session.commit("array created")

    with dask.config.set(scheduler="threads"):  # type: ignore[no-untyped-call]
        session = repo.writable_session(branch="main")
        store = session.store
        group = zarr.open_group(store=store)
        zarray = cast(zarr.Array, group["array"])
        # with store.preserve_read_only():
        store_dask(session, sources=[dask_array], targets=[zarray])
        commit_res = session.commit("distributed commit")
        assert commit_res

        # Lets open a new store to verify the results
        readonly_session = repo.readonly_session(branch="main")
        store = readonly_session.store
        all_keys = [key async for key in store.list_prefix("/")]
        assert (
            len(all_keys) == 1 + 1 + CHUNKS_PER_DIM * CHUNKS_PER_DIM
        )  # group meta + array meta + each chunk

        group = zarr.open_group(store=store, mode="r")

        roundtripped = dask.array.from_array(group["array"], chunks=dask_chunks)  # type: ignore [no-untyped-call, attr-defined]
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=UserWarning)
            assert_eq(roundtripped, dask_array)  # type: ignore [no-untyped-call]


import math
from dataclasses import dataclass

import numpy as np

from icechunk import Repository, Session, Storage


@dataclass
class Task:
    """A read/write task"""

    # The worker will use this Icechunk store to read/write to the dataset
    session: Session
    # Region of the array to write to
    region: tuple[slice, ...]


def generate_task_array(task: Task, shape: tuple[int, ...]) -> np.typing.ArrayLike:
    """Generates a random array with the given shape and using the seed in the Task"""
    seed = math.prod(slicer.stop for slicer in task.region)
    np.random.seed(seed)
    return np.random.rand(*shape)


def execute_write_task(task: Task) -> None:
    store = task.session.store
    group = zarr.group(store=store, overwrite=False)
    array = cast(zarr.Array, group["array"])
    data = generate_task_array(task, array[task.region].shape)
    assert len(task.region) == array.ndim
    print(f"started writing {task.region}")
    tic = time.perf_counter()
    array[task.region] = data
    toc = time.perf_counter()
    print(f"finished writing {task.region} in {toc-tic} seconds")


def slices_from_chunks(shape: tuple[int, ...], chunks: tuple[int, ...]):
    """slightly modified from dask.array.core.slices_from_chunks to be lazy"""
    import operator
    from itertools import product, repeat

    import toolz as tlz

    extras = ((s % c,) if s % c > 0 else () for s, c in zip(shape, chunks, strict=True))
    # need this twice
    chunks = tuple(
        tuple(tlz.concatv(repeat(c, s // c), e))
        for s, c, e in zip(shape, chunks, extras, strict=True)
    )
    cumdims = (tlz.accumulate(operator.add, bds[:-1], 0) for bds in chunks)
    slices = (
        (slice(s, s + dim) for s, dim in zip(starts, shapes, strict=True))
        for starts, shapes in zip(cumdims, chunks, strict=True)
    )
    return product(*slices)


def test_multi_threading():
    # pytest.skip("deadlock")
    executor = ThreadPoolExecutor(max_workers=4)
    shape = (120, 20, 20)
    chunk_shape = (1, -1, -1)

    storage = Storage.new_local_filesystem("/tmp/icechunk-multithreading-test")
    repo = Repository.open_or_create(storage=storage)
    session = repo.writable_session("main")

    assert len(shape) == len(chunk_shape)
    chunks = tuple(s if c == -1 else c for s, c in zip(shape, chunk_shape, strict=True))

    store = session.store
    group = zarr.group(store, overwrite=True)
    group.create_array(
        "array", shape=shape, dtype=np.float32, chunks=chunks, overwrite=True
    )
    session.commit("initialized")

    session = repo.writable_session("main")
    [
        f.result(timeout=5)
        for f in (
            [
                executor.submit(execute_write_task, Task(session=session, region=slicer))
                for slicer in slices_from_chunks(shape, chunks)
            ]
        )
    ]
