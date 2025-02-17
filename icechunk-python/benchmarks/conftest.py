import pytest

from benchmarks.datasets import (
    ERA5,
    ERA5_ARCO,
    ERA5_SINGLE,
    GB_8MB_CHUNKS,
    GB_128MB_CHUNKS,
    TEST_BUCKETS,
)
from icechunk import Repository, local_filesystem_storage
from zarr.abc.store import Store


@pytest.fixture(scope="function")
def repo(tmpdir: str) -> Repository:
    return Repository.create(storage=local_filesystem_storage(tmpdir))


@pytest.fixture(
    params=[
        pytest.param(GB_8MB_CHUNKS, id="gb-8mb"),
        pytest.param(GB_128MB_CHUNKS, id="gb-128mb"),
        pytest.param(ERA5_SINGLE, id="era5-single"),
        pytest.param(ERA5, id="era5-weatherbench"),
        pytest.param(ERA5_ARCO, id="era5-arco"),
    ],
)
def synth_dataset(request) -> Store:
    """For now, these are synthetic datasets stored in the cloud."""
    extra_prefix = request.config.getoption("--icechunk-prefix")
    where = request.config.getoption("--where")
    ds = request.param
    if where == "local" and ds.skip_local:
        pytest.skip()
    # for some reason, this gets run multiple times so we apply the prefix repeatedly
    # if we don't catch that :(
    ds.storage_config = ds.storage_config.with_overwrite(
        **TEST_BUCKETS[where]
    ).with_extra(prefix=extra_prefix, force_idempotent=True)
    if ds.setupfn is None:
        # these datasets aren't automatically set up
        # so skip if the data haven't been written yet.
        try:
            ds.store()
        except ValueError as e:
            pytest.skip(reason=str(e))
    return ds


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
