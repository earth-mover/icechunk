#!/usr/bin/env python3
# pytest -s test_benchmark_writes.py::test_write_chunks
import numpy as np
import pytest

from tests.benchmarks import lib
from tests.benchmarks.tasks import Executor, write


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
    pytest.skip()
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


# TODO: write a large number of virtual chunk refs
def write_million_chunk_refs(repo):
    pass
