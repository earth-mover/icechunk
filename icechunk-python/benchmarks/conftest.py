import copy
from typing import cast

import pytest

from benchmarks import helpers
from benchmarks.datasets import (
    ERA5,
    ERA5_ARCO,
    ERA5_SINGLE,
    GB_8MB_CHUNKS,
    GB_128MB_CHUNKS,
    PANCAKE_WRITES,
    SIMPLE_1D,
    TEST_BUCKETS,
    BenchmarkReadDataset,
    BenchmarkWriteDataset,
    Dataset,
)
from icechunk import Repository, local_filesystem_storage


def request_to_dataset(request, moar_prefix: str = "") -> Dataset:
    extra_prefix = request.config.getoption("--icechunk-prefix") + moar_prefix
    where = request.config.getoption("--where")
    ds = copy.deepcopy(request.param)
    if where == "local" and ds.skip_local:
        pytest.skip()
    # this gets run multiple times because the fixture scope is 'function'
    # so we need this `force_idempotent` ugliness
    ds.storage_config = ds.storage_config.with_overwrite(
        **TEST_BUCKETS[where]
    ).with_extra(prefix=extra_prefix, force_idempotent=True)
    return ds


@pytest.fixture(scope="function")
def repo(tmpdir: str) -> Repository:
    return Repository.create(storage=local_filesystem_storage(tmpdir))


@pytest.fixture(params=[pytest.param(PANCAKE_WRITES, id="pancake-writes")])
def synth_write_dataset(request) -> BenchmarkWriteDataset:
    ds = request_to_dataset(request, moar_prefix=helpers.rdms())
    return cast(BenchmarkWriteDataset, ds)


@pytest.fixture(params=[pytest.param(SIMPLE_1D, id="simple-1d")])
def simple_write_dataset(request) -> BenchmarkWriteDataset:
    ds = request_to_dataset(request, moar_prefix=helpers.rdms())
    return cast(BenchmarkWriteDataset, ds)


@pytest.fixture(
    params=[
        pytest.param(GB_8MB_CHUNKS, id="gb-8mb"),
        pytest.param(GB_128MB_CHUNKS, id="gb-128mb"),
        pytest.param(ERA5_SINGLE, id="era5-single"),
        pytest.param(ERA5, id="era5-weatherbench"),
        pytest.param(ERA5_ARCO, id="era5-arco"),
    ],
)
def synth_dataset(request) -> BenchmarkReadDataset:
    """For now, these are synthetic datasets stored in the cloud."""
    ds = request_to_dataset(request)
    if ds.setupfn is None:
        # these datasets aren't automatically set up
        # so skip if the data haven't been written yet.
        try:
            ds.store()
        except ValueError as e:
            pytest.skip(reason=str(e))
    return cast(BenchmarkReadDataset, ds)


# This hook is used instead of `pyproject.toml` so that we can run the benchmark infra
# on versions older than alpha-13
# TODO: Migrate to pyproject.toml after 1.0 has been released.
def pytest_configure(config):
    config.addinivalue_line(
        "markers", "setup_benchmarks: Run synthetic dataset setup code for benchmarks."
    )
    config.addinivalue_line("markers", "read_benchmark: Read benchmark.")
    config.addinivalue_line("markers", "write_benchmark: Write benchmark.")


def pytest_addoption(parser):
    parser.addoption(
        "--icechunk-prefix",
        action="store",
        default="",
        help="""
        extra prefix that is prefix-ed to the configured prefix or
        path in icechunk benchmark datasets.
        """,
    )

    parser.addoption(
        "--force-setup",
        action="store",
        default=True,
        help="""
        Force running the setup_benchmarks code even if there is a valid repo
        for this icechunk version at that URI. True by default.
        """,
    )

    parser.addoption(
        "--where",
        action="store",
        help="Where to run icechunk benchmarks? [local|s3|gcs].",
        default="local",
    )
