#!/usr/bin/env python3
# pytest -s test_benchmark_writes.py::test_write_chunks
import numpy as np
import pytest

import zarr
from benchmarks import lib
from benchmarks.helpers import get_splitting_config, repo_config_with
from benchmarks.tasks import Executor, write
from icechunk import (
    Repository,
    RepositoryConfig,
    Session,
    VirtualChunkSpec,
    local_filesystem_storage,
)

NUM_CHUNK_REFS = 10_000
NUM_VIRTUAL_CHUNK_REFS = 100_000


pytestmark = pytest.mark.write_benchmark


@pytest.fixture(
    params=[
        pytest.param(None, id="no-splitting"),
        pytest.param(10000, id="split-size-10_000"),
    ]
)
def splitting(request):
    if request.param is None:
        return None
    else:
        try:
            return get_splitting_config(split_size=request.param)
        except ImportError:
            pytest.skip("splitting not supported on this version")


# FIXME: figure out a reasonable default
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


def test_write_simple_1d(benchmark, simple_write_dataset):
    """Simple write benchmarks. Shows 2-3X slower on GCS compared to S3"""
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

    assert repo_config is not None
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


@pytest.mark.benchmark(group="refs-write")
def test_set_many_virtual_chunk_refs(benchmark, repo) -> None:
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


@pytest.mark.benchmark(group="refs-write")
def test_write_split_manifest_refs_full_rewrite(
    benchmark, splitting, large_write_dataset
) -> None:
    dataset = large_write_dataset
    config = repo_config_with(splitting=splitting)
    assert config is not None
    if hasattr(config.manifest, "splitting"):
        assert config.manifest.splitting == splitting
    repo = dataset.create(config=config)
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

    def write_refs() -> Session:
        session = repo.writable_session(branch="main")
        num_chunks = dataset.shape[0] // dataset.chunks[0]
        chunks = [
            VirtualChunkSpec(
                index=[i], location=f"s3://foo/bar/{i}.nc", offset=0, length=1
            )
            for i in range(num_chunks)
        ]

        session.store.set_virtual_refs("array", chunks)
        # (args, kwargs)
        return ((session,), {})

    def commit(session_from_setup):
        session_from_setup.commit("wrote refs")

    benchmark.pedantic(commit, setup=write_refs, iterations=1, rounds=10)


@pytest.mark.benchmark(group="refs-write")
def test_write_split_manifest_refs_append(
    benchmark, splitting, large_write_dataset
) -> None:
    dataset = large_write_dataset
    config = repo_config_with(splitting=splitting)
    assert config is not None
    if hasattr(config.manifest, "splitting"):
        assert config.manifest.splitting == splitting
    repo = dataset.create(config=config)
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

    # yuck, but I'm abusing `rounds` to do a loop and time _only_ the commit.
    global counter
    counter = 0
    rounds = 10
    num_chunks = dataset.shape[0] // dataset.chunks[0]
    batch_size = num_chunks // rounds

    def write_refs() -> Session:
        global counter
        session = repo.writable_session(branch="main")
        chunks = [
            VirtualChunkSpec(
                index=[i], location=f"s3://foo/bar/{i}.nc", offset=0, length=1
            )
            for i in range(counter * batch_size, counter * batch_size + batch_size)
        ]
        counter += 1
        session.store.set_virtual_refs("array", chunks)
        # (args, kwargs)
        return ((session,), {})

    def commit(session_from_setup):
        session_from_setup.commit("wrote refs")

    benchmark.pedantic(commit, setup=write_refs, iterations=1, rounds=rounds)
