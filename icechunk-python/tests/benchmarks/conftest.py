import pytest

from tests.benchmarks.datasets import ERA5_SINGLE, GB_8MB_CHUNKS, GB_128MB_CHUNKS
from zarr.abc.store import Store


@pytest.fixture(
    params=[
        pytest.param(ERA5_SINGLE, id="era5-single"),
        pytest.param(GB_128MB_CHUNKS, id="gb-128mb"),
        pytest.param(GB_8MB_CHUNKS, id="gb-8mb"),
    ]
)
def synth_dataset(request) -> Store:
    """For now, these are synthetic datasets stored in the cloud."""
    return request.param


# This hook is used instead of `pyproject.toml` so that we can run the benchmark infra
# on versions older than alpha-13
# TODO: Migrate to pyproject.toml after 1.0 has been released.
def pytest_configure(config):
    config.addinivalue_line(
        "markers", "setup_benchmarks: Run synthetic dataset setup code for benchmarks."
    )


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
