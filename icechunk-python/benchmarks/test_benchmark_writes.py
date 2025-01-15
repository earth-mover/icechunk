#!/usr/bin/env python3
# pytest -s test_benchmark_writes.py::test_write_chunks

from benchmarks.tasks import Executor, write


def test_write_chunks(benchmark):
    timer = benchmark.pedantic(
        # TODO: parametrize over some of these
        write,
        kwargs=dict(
            url="local://tmp/icechunk-test/perf-writes/foo",
            num_arrays=1,
            shape=[120, 20, 20],
            chunks=[1, -1, -1],
            task_nchunks=1,
            executor=Executor.threads,
            workers=4,
            threads_per_worker=None,
        ),
        iterations=1,
        rounds=1,
    )
    benchmark.extra_info = timer.dataframe().to_dict()
