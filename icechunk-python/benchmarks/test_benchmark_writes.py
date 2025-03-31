#!/usr/bin/env python3
# pytest -s test_benchmark_writes.py::test_write_chunks
import numpy as np
import pytest

import zarr
from benchmarks import lib
from benchmarks.tasks import Executor, write
from icechunk import Repository, RepositoryConfig, local_filesystem_storage

NUM_CHUNK_REFS = 10_000
NUM_VIRTUAL_CHUNK_REFS = 100_000


# FIXME: figure out a reasonable default
@pytest.mark.write_benchmark
@pytest.mark.parametrize(
    "executor",
    [
        Executor.threads,
        pytest.param(
            Executor.processes, marks=pytest.mark.skipif(True, reason="hangs on Coiled")
        ),
    ],
)
def test_write_chunks_with_tasks(synth_write_dataset, benchmark, executor):
    """
    Writes chunks orchestrated using 'bare' tasks and executed using
    either a ThreadPoolExecutor or ProcessPoolExecutor.

    Importantly this benchmarks captures timings PER write task, summarizes them,
    and then records them in the .json file.
    """
    timer = benchmark.pedantic(
        # TODO: parametrize over some of these
        write,
        kwargs=dict(
            storage=synth_write_dataset.storage,
            num_arrays=synth_write_dataset.num_arrays,
            shape=synth_write_dataset.shape,
            chunks=synth_write_dataset.chunks,
            task_nchunks=1,
            executor=executor,
            workers=4,
            threads_per_worker=None,
        ),
        iterations=1,
        rounds=1,
    )
    diags = timer.as_dict()
    diags["write_task_raw"] = diags["write_task"]
    diags["write_task"] = lib.stats(np.array(diags["write_task"]))
    benchmark.extra_info["data"] = diags


@pytest.mark.write_benchmark
def test_write_simple_1d(benchmark, simple_write_dataset):
    dataset = simple_write_dataset
    repo = dataset.create()
    session = repo.writable_session(branch="main")
    store = session.store
    group = zarr.open_group(store, zarr_format=3)
    group.create_array(
        "array",
        shape=dataset.shape,
        chunks=dataset.chunks,
        dtype="int8",
        fill_value=0,
        compressors=None,
    )
    session.commit("initialize")

    def write_task():
        session = repo.writable_session(branch="main")
        array = zarr.open_array(session.store, path="array")
        array[:] = 42
        session.commit("foo")

    benchmark(write_task)


def repo_config_with(
    *, inline_chunk_threshold_bytes: int | None = None
) -> RepositoryConfig:
    config = RepositoryConfig.default()
    if inline_chunk_threshold_bytes is not None:
        config.inline_chunk_threshold_bytes = inline_chunk_threshold_bytes


@pytest.mark.write_benchmark
@pytest.mark.benchmark(group="refs-write")
@pytest.mark.parametrize("commit", [True, False])
@pytest.mark.parametrize(
    "repo_config",
    [
        pytest.param(repo_config_with(), id="default-inlined"),
        pytest.param(repo_config_with(inline_chunk_threshold_bytes=0), id="not-inlined"),
    ],
)
def test_write_many_chunk_refs(
    commit: bool, benchmark, repo_config: RepositoryConfig, tmpdir
) -> None:
    """Benchmarking the writing of many chunk refs, inlined and not; committed and not;"""

    # 1. benchmark only the writes with no commit
    # 2. one with commit
    def write_chunk_refs(repo) -> None:
        session = repo.writable_session("main")
        array = zarr.open_array(path="array", store=session.store)
        # TODO: configurable?
        with zarr.config.set({"async.concurrency": 64}):
            array[:] = -1
        if commit:
            session.commit("written!")

    repo = Repository.create(storage=local_filesystem_storage(tmpdir), config=repo_config)
    session = repo.writable_session("main")
    group = zarr.group(session.store)
    kwargs = dict(
        name="array",
        shape=(NUM_CHUNK_REFS,),
        chunks=(1,),
        dtype=np.int8,
        dimension_names=("t",),
    )
    try:
        group.create_array(**kwargs)
    except AttributeError:
        group.array(**kwargs)
    session.commit("initialized")

    benchmark(write_chunk_refs, repo)


@pytest.mark.write_benchmark
@pytest.mark.benchmark(group="refs-write")
def test_write_many_virtual_chunk_refs(benchmark, repo) -> None:
    """Benchmark the setting of many virtual chunk refs."""
    session = repo.writable_session("main")
    store = session.store
    group = zarr.group(store)
    kwargs = dict(
        name="array",
        shape=(NUM_VIRTUAL_CHUNK_REFS,),
        chunks=(1,),
        dtype=np.int8,
        dimension_names=("t",),
        overwrite=True,
    )
    try:
        group.create_array(**kwargs)
    except AttributeError:
        group.array(**kwargs)
    session.commit("initialized")

    @benchmark
    def write():
        # always create a new session so we have a clean changelog
        session = repo.writable_session("main")
        store = session.store
        for i in range(NUM_VIRTUAL_CHUNK_REFS):
            store.set_virtual_ref(
                f"array/c/{i}", location=f"s3://foo/bar/{i}.nc", offset=0, length=1
            )
