# Consolidate some "big" dataset creation
# My workflow is:
#   1. just create-deepak-env v0.1.0a12
#   2. conda activate icechunk-v0.1.0a12
#   3. python benchmarks/create-era5.py
import argparse
import datetime
import logging
import warnings

import helpers
from datasets import ERA5, Dataset
from packaging.version import Version

import icechunk as ic
import xarray as xr
from icechunk.xarray import to_icechunk

logger = logging.getLogger("icechunk-bench")
logger.setLevel(logging.INFO)
console_handler = logging.StreamHandler()
logger.addHandler(console_handler)


# @coiled.function
def write_era5(dataset: Dataset, *, ref, arrays_to_write):
    """
    1. We write all the metadata and coordinate arrays to make a "big" snapshot.
    2. We only write a few arrays to save time.
    """
    import coiled

    import distributed

    SELECTOR = {"time": slice(5 * 365 * 24)}
    chunk_shape = {"time": 1, "level": 1, "latitude": 721, "longitude": 1440}
    zarr_kwargs = dict(group=dataset.group, zarr_format=3, consolidated=False)

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=UserWarning)
        ds = xr.open_zarr(
            "gs://weatherbench2/datasets/era5/1959-2023_01_10-full_37-1h-0p25deg-chunk-1.zarr",
            chunks={"time": 24 * 3, "level": 1},
        ).drop_encoding()
    for v in ds:
        ds[v].encoding["chunks"] = tuple(chunk_shape[dim] for dim in ds[v].dims)

    towrite = ds[arrays_to_write].isel(SELECTOR)
    towrite.attrs["written_arrays"] = " ".join(towrite.data_vars)
    towrite.attrs["icechunk_commit"] = helpers.get_commit(ref)
    towrite.attrs["icechunk_ref"] = ref
    towrite.attrs["selector"] = str(SELECTOR)

    repo = ic.Repository.open(dataset.storage)

    logger.info("Initializing dataset.")
    session = repo.writable_session("main")
    ds.to_zarr(session.store, compute=False, **zarr_kwargs)
    session.commit("initialized dataset")
    logger.info("Finished initializing dataset.")

    session = repo.writable_session("main")
    # FIXME: use name
    #   # name=f"earthmover/{ref}",
    with coiled.Cluster(n_workers=(4, 200), worker_cpu=2) as cluster:
        client = distributed.Client(cluster)
        print(client)
        with distributed.performance_report(
            f"reports/era5-ingest-{ref}-{datetime.datetime.now()}.html"
        ):
            logger.info(f"Started writing {arrays_to_write=}.")
            # FIXME: switch to region="auto" when GH606 is closed
            to_icechunk(
                towrite, session.store, region=SELECTOR, **zarr_kwargs, split_every=32
            )
    session.commit("ingest!")
    logger.info(f"Finished writing {arrays_to_write=}.")


def setup_era5_weatherbench2(
    dataset: Dataset, *, ref: str, arrays_to_write: list[str]
) -> None:
    commit = helpers.get_commit(ref)
    logger.info(f"Writing ERA5 for {ref=}, {commit=}, {arrays_to_write=}")
    prefix = f"benchmarks/{ref}_{commit}/"
    dataset.storage_config = dataset.storage_config.with_extra(prefix=prefix)
    dataset.create()
    write_era5(dataset, ref=ref, arrays_to_write=arrays_to_write)


def get_version() -> str:
    version = Version(ic.__version__)
    if "a" in version.pre:
        return f"icechunk-v{version.base_version}-alpha.{version.pre[1]}"
    else:
        raise NotImplementedError


if __name__ == "__main__":
    helpers.assert_cwd_is_icechunk_python()

    parser = argparse.ArgumentParser()
    # parser.add_argument("ref", help="ref to run ingest for")
    parser.add_argument(
        "--arrays",
        help="arrays to write",
        nargs="+",
        default=[
            "2m_temperature",
            "10m_u_component_of_wind",
            "10m_v_component_of_wind",
            "boundary_layer_height",
        ],
    )
    args = parser.parse_args()
    setup_era5_weatherbench2(ERA5, ref=get_version(), arrays_to_write=args.arrays)
