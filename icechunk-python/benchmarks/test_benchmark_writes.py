#!/usr/bin/env python3
# pytest -s test_benchmark_writes.py::test_write_chunks
import numpy as np
import pytest

from benchmarks import lib
from benchmarks.tasks import Executor, write


@pytest.mark.parametrize("executor", [Executor.processes])
def test_write_chunks(benchmark, executor):
    timer = benchmark.pedantic(
        # TODO: parametrize over some of these
        write,
        kwargs=dict(
            url="local://tmp/icechunk-test/perf-writes/foo",
            num_arrays=1,
            shape=[3200, 720, 1441],
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
