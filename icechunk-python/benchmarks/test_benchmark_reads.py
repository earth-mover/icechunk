import operator

import pytest

import icechunk as ic
import xarray as xr
import zarr
from benchmarks.datasets import Dataset

# TODO: configurable?
zarr.config.set({"async.concurrency": 64})

pytestmark = pytest.mark.read_benchmark


@pytest.fixture(
    params=[
        pytest.param(None, id="default"),
        pytest.param(
            ic.ManifestPreloadConfig(preload_if=ic.ManifestPreloadCondition.false()),
            id="off",
        ),
    ],
)
def preload(request):
    return request.param


@pytest.mark.setup_benchmarks
@pytest.mark.filterwarnings("ignore:datetime.datetime.utcnow")  # bah, boto!
def test_recreate_datasets(synth_dataset, request):
    """
    Helper to (re)create Icechunk repos when format or Dataset changes.
    Use `pytest -m setup_benchmarks` to run it
    `--force-setup=True` will always recreate.
    `--force-setup=False` will recreate if icechunk cannot create a Repository (perhaps due to format change).
    """
    # Check if the mark is passed via the command line
    if "setup_benchmarks" not in request.config.getoption("-m", default=""):
        pytest.skip(
            "Skipping re-creating benchmarks because 'setup_benchmarks' is not set in the command line."
        )

    if synth_dataset.setupfn is not None:
        force = request.config.getoption("--force-setup") == "True"
        synth_dataset.setup(force=force)


def test_time_create_store(synth_dataset: Dataset, benchmark) -> None:
    """time to create the icechunk create the Repo, session, and store objects."""
    benchmark(operator.attrgetter("store"), synth_dataset)


@pytest.mark.benchmark(warmup=1)
def test_time_getsize_key(synth_dataset: Dataset, benchmark) -> None:
    from zarr.core.sync import sync

    if synth_dataset.load_variables is None:
        pytest.skip()

    store = synth_dataset.store

    @benchmark
    def fn():
        for array in synth_dataset.load_variables:
            if (group := synth_dataset.group) is not None:
                prefix = f"{group}/"
            else:
                prefix = ""
            key = f"{prefix}{array}/zarr.json"
            sync(store.getsize(key))


def test_time_getsize_prefix(synth_dataset: Dataset, benchmark) -> None:
    if synth_dataset.first_byte_variable is None:
        pytest.skip()
    store = synth_dataset.store
    group = zarr.open_group(store, path=synth_dataset.group, mode="r")
    # Using a relatively small variable.
    array = group[synth_dataset.first_byte_variable]
    benchmark(operator.methodcaller("nbytes_stored"), array)


@pytest.mark.benchmark(group="zarr-read")
def test_time_zarr_open(synth_dataset: Dataset, benchmark) -> None:
    """
    We repeatedly create the store inside the benchmarked function to
    test the "cold open" time, and downloading of the snapshot file.
    """

    @benchmark
    def fn():
        zarr.open_group(synth_dataset.store, path=synth_dataset.group, mode="r")


@pytest.mark.benchmark(group="zarr-read")
def test_time_zarr_members(synth_dataset: Dataset, benchmark) -> None:
    # list_dir, maybe warmup=1
    group = zarr.open_group(synth_dataset.store, path=synth_dataset.group, mode="r")
    benchmark(operator.methodcaller("members"), group)


@pytest.mark.benchmark(group="xarray-read", min_rounds=10)
def test_time_xarray_open(synth_dataset: Dataset, benchmark) -> None:
    @benchmark
    def fn():
        xr.open_zarr(
            synth_dataset.store,
            group=synth_dataset.group,
            chunks=None,
            consolidated=False,
        )


@pytest.mark.parametrize("selector", ["single-chunk", "full-read"])
@pytest.mark.benchmark(group="xarray-read", min_rounds=2)
def test_time_xarray_read_chunks_cold_cache(
    synth_dataset: Dataset, selector, preload, benchmark
) -> None:
    """128MB vs 8MB chunks. should see a difference."""
    if synth_dataset.load_variables is None:
        pytest.skip()

    if selector == "single-chunk":
        subsetter = synth_dataset.chunk_selector
    elif selector == "full-read":
        subsetter = synth_dataset.full_load_selector
    else:
        raise ValueError(f"bad selector: {selector}")

    if subsetter is None:
        pytest.skip(f"no selector specified for {selector}")

    # TODO: switch out concurrency "ideal_request_size"
    @benchmark
    def fn():
        repo = ic.Repository.open(
            storage=synth_dataset.storage,
            config=ic.RepositoryConfig(manifest=ic.ManifestConfig(preload=preload)),
        )
        ds = xr.open_zarr(
            repo.readonly_session("main").store,
            group=synth_dataset.group,
            chunks=None,
            consolidated=False,
        )
        subset = ds.isel(subsetter)
        subset[synth_dataset.load_variables].compute()


@pytest.mark.benchmark(group="xarray-read", min_rounds=2)
def test_time_xarray_read_chunks_hot_cache(
    synth_dataset: Dataset, preload, benchmark
) -> None:
    """128MB vs 8MB chunks. should see a difference."""
    if synth_dataset.load_variables is None:
        pytest.skip()
    # TODO: switch out concurrency "ideal_request_size"
    repo = ic.Repository.open(
        storage=synth_dataset.storage,
        config=ic.RepositoryConfig(manifest=ic.ManifestConfig(preload=preload)),
    )
    ds = xr.open_zarr(
        repo.readonly_session("main").store,
        group=synth_dataset.group,
        chunks=None,
        consolidated=False,
    )
    subset = ds.isel(synth_dataset.chunk_selector)
    # important this cannot be `load`
    benchmark(operator.methodcaller("compute"), subset[synth_dataset.load_variables])


@pytest.mark.benchmark(group="bytes-read")
def test_time_first_bytes(synth_dataset: Dataset, benchmark) -> None:
    """TODO: this should be sensitive to manifest splitting"""
    if synth_dataset.first_byte_variable is None:
        pytest.skip("first_byte_variable not set!")

    @benchmark
    def open_and_read():
        # by opening the group repeatedly inside the benchmarked function
        # we force re-download of manifest so that we actually measure what we want.
        group = zarr.open_group(synth_dataset.store, path=synth_dataset.group, mode="r")
        group[synth_dataset.first_byte_variable][:]


# TODO: synthetic dataset with very deep and large hierarchies for e.g. tree & members
