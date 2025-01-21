#!/usr/bin/env python3
# pytest -s test_benchmark_writes.py::test_write_chunks
import numpy as np
import pytest

import zarr
from icechunk import Repository, RepositoryConfig, local_filesystem_storage
from tests.benchmarks import lib
from tests.benchmarks.tasks import Executor, write

NUM_CHUNK_REFS = 20_000
NUM_VIRTUAL_CHUNK_REFS = 100_000


# FIXME: figure out a reasonable default
@pytest.mark.parametrize("executor", [Executor.threads, Executor.processes])
@pytest.mark.parametrize(
    "url",
    [
        pytest.param("local://tmp/icechunk-test/perf-writes/foo", id="local"),
        # pytest.param("s3://icechunk-test/perf-writes/foo", id="s3"),
    ],
)
def test_write_chunks(url, benchmark, executor):
    """
    Writes chunks locally orchestrated using 'bare' tasks and executed using
    either a ThreadPoolExecutor or ProcessPoolExecutor.

    Importantly this benchmarks captures timings PER write task, summarizes them,
    and then records them in the .json file.
    """
    pytest.skip("too slow for now. FIXME!")
    timer = benchmark.pedantic(
        # TODO: parametrize over some of these
        write,
        kwargs=dict(
            url=url,
            num_arrays=1,
            shape=[320, 720, 1441],
            chunks=[1, -1, -1],
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


def repo_config_with(
    *, inline_chunk_threshold_bytes: int | None = None
) -> RepositoryConfig:
    config = RepositoryConfig.default()
    if inline_chunk_threshold_bytes is not None:
        config.inline_chunk_threshold_bytes = inline_chunk_threshold_bytes


@pytest.mark.benchmark(group="refs-write")
@pytest.mark.parametrize(
    "repo_config",
    [
        pytest.param(repo_config_with(), id="default-inlined"),
        pytest.param(repo_config_with(inline_chunk_threshold_bytes=0), id="not-inlined"),
    ],
)
def test_write_many_chunk_refs(benchmark, repo_config: RepositoryConfig, tmpdir) -> None:
    def write_chunk_refs(repo) -> None:
        session = repo.writable_session("main")
        group = zarr.group(session.store)
        array = group.create_array(
            name="array",
            shape=(NUM_CHUNK_REFS,),
            chunks=(1,),
            dtype=np.int8,
            dimension_names=("t",),
        )
        # TODO: configurable?
        with zarr.config.set({"async.concurrency": 64}):
            array[:] = -1
        session.commit("written!")

    repo = Repository.create(storage=local_filesystem_storage(tmpdir), config=repo_config)
    benchmark(write_chunk_refs, repo)


@pytest.mark.benchmark(group="refs-write")
def test_write_many_virtual_chunk_refs(benchmark, repo) -> None:
    @benchmark
    def write():
        session = repo.writable_session("main")
        store = session.store
        group = zarr.group(store)
        group.create_array(
            name="array",
            shape=(NUM_VIRTUAL_CHUNK_REFS,),
            chunks=(1,),
            dtype=np.int8,
            dimension_names=("t",),
        )
        for i in range(NUM_VIRTUAL_CHUNK_REFS):
            store.set_virtual_ref(
                f"array/c/{i}", location=f"s3://foo/bar/{i}.nc", offset=0, length=1
            )
        session.commit("written!")