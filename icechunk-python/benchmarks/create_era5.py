# Consolidate some "big" dataset creation
# My workflow is:
#   1. just create-deepak-env v0.1.0a12
#   2. conda activate icechunk-v0.1.0a12
#   3. python benchmarks/create-era5.py
import argparse
import datetime
import logging
import math
import warnings

import helpers
import humanize
from datasets import Dataset, StorageConfig, Store
from packaging.version import Version

import icechunk as ic
import xarray as xr
import zarr
from icechunk.xarray import to_icechunk

logger = logging.getLogger("icechunk-bench")
logger.setLevel(logging.INFO)
console_handler = logging.StreamHandler()
logger.addHandler(console_handler)

PUBLIC_DATA_BUCKET = "icechunk-public-data"
ICECHUNK_FORMAT = "v01"  # FIXME: get this from icechunk python

# Note: this region is really a "compute" region, so `us-east-1` for AWS/Tigris
BUCKETS = {
    "s3": dict(store="s3", bucket=PUBLIC_DATA_BUCKET, region="us-east-1"),
    "gcs": dict(store="gcs", bucket=PUBLIC_DATA_BUCKET, region="us-east1"),
    "tigris": dict(store="tigris", bucket=PUBLIC_DATA_BUCKET, region="us-east-1"),
}


def make_dataset(*, store: Store, prefix: str, group: str | None = None) -> Dataset:
    storage_config = StorageConfig(prefix=prefix, **BUCKETS[store])
    return Dataset(storage_config=storage_config, group=group)


# @coiled.function
def write_era5(
    dataset: Dataset,
    *,
    nyears: int | None = None,
    arrays_to_write: list[str],
    extra_attrs: dict[str, str] | None = None,
    initialize: bool = True,
    initialize_all_vars: bool = False,
    dry_run: bool = False,
):
    """
    dataset: Dataset t write
    arrays_to_write: list[str],
    extra_attrs: any attributes to add
    initialize_all_vars: whether to write all coordinate arrays, and metadata for ALL data_vars.

    Usually, initialize_all_vars=True for benchmarks, but not for the "public dataset".
    For benchmarks,
      1. We write all the metadata and coordinate arrays to make a "big" snapshot.
      2. We only write a few arrays to save time.
    """
    import coiled

    import distributed

    SELECTOR = {"time": slice(nyears * 365 * 24) if nyears is not None else slice(None)}
    chunk_shape = {"time": 1, "level": 1, "latitude": 721, "longitude": 1440}
    ic_kwargs = dict(group=dataset.group)
    zarr_kwargs = dict(**ic_kwargs, zarr_format=3, consolidated=False)

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
    towrite.attrs["selector"] = str(SELECTOR)
    towrite.attrs.update(extra_attrs or {})

    repo = ic.Repository.open(dataset.storage)

    nchunks = tuple(
        math.prod((var.sizes[dim] // chunk_shape[dim] + 1) for dim in var.dims)
        for _, var in towrite.data_vars.items()
    )
    logger.info(
        f"Size: {humanize.naturalsize(towrite.nbytes)}, "
        f"Total nchunks= {humanize.intcomma(sum(nchunks))}, "
        f"per array: {[humanize.intcomma(i) for i in nchunks]}"
    )

    if dry_run:
        print("Dry run. Exiting")
        return

    if initialize:
        logger.info("Initializing dataset.")
        session = repo.writable_session("main")
        init_dataset = ds if initialize_all_vars else towrite
        init_dataset.to_zarr(session.store, compute=False, **zarr_kwargs)
        session.commit("initialized dataset")
        logger.info("Finished initializing dataset.")

    session = repo.writable_session("main")
    # FIXME: use name
    #   # name=f"earthmover/{ref}",
    with coiled.Cluster(
        n_workers=(4, 200),
        worker_cpu=2,
        # backend=dataset.storage_config.get_coiled_backend(),
        region=dataset.storage_config.region,
    ) as cluster:
        client = distributed.Client(cluster)
        print(client)
        with distributed.performance_report(
            f"reports/era5-ingest-{dataset.storage_config.store}-{ICECHUNK_FORMAT}-{datetime.datetime.now()}.html"
        ):
            logger.info(f"Started writing {arrays_to_write=}.")
            with zarr.config.set({"async.concurrency": 24}):
                to_icechunk(
                    towrite, session=session, region="auto", **ic_kwargs, split_every=32
                )
    session.commit("ingest!")
    logger.info(f"Finished writing {arrays_to_write=}.")


def setup_for_benchmarks(
    dataset: Dataset, *, ref: str, arrays_to_write: list[str]
) -> None:
    commit = helpers.get_commit(ref)
    logger.info(f"Writing ERA5 for {ref=}, {commit=}, {arrays_to_write=}")
    prefix = f"benchmarks/{ref}_{commit}/"
    dataset.storage_config = dataset.storage_config.with_extra(prefix=prefix)
    dataset.create()
    attrs = {"icechunk_commit": helpers.get_commit(ref), "icechunk_ref": ref}
    write_era5(
        dataset,
        extra_attrs=attrs,
        arrays_to_write=arrays_to_write,
        initialize_all_vars=False,
    )


def setup_dataset(
    dataset: Dataset, *, create: bool = False, dry_run: bool = False, **kwargs
) -> None:
    # commit = helpers.get_commit(ref)
    format = ICECHUNK_FORMAT
    logger.info(f"Writing ERA5 for {format}, {kwargs=}")
    prefix = f"{format}/"
    dataset.storage_config = dataset.storage_config.with_extra(prefix=prefix)
    if create and not dry_run:
        dataset.create(clear=True)
    write_era5(dataset, initialize_all_vars=False, dry_run=dry_run, **kwargs)


def get_version() -> str:
    version = Version(ic.__version__)
    if version.pre is not None and "a" in version.pre:
        return f"icechunk-v{version.base_version}-alpha.{version.pre[1]}"
    else:
        return f"icechunk-v{version.base_version}"


if __name__ == "__main__":
    helpers.assert_cwd_is_icechunk_python()

    parser = argparse.ArgumentParser()
    parser.add_argument("store", help="object store to write to")
    parser.add_argument(
        "--create",
        help="whether to create the store",
        action="store_true",
        default=False,
    )
    parser.add_argument(
        "--arrays",
        help="arrays to write",
        nargs="+",
        default=[
            "2m_temperature",
            "10m_u_component_of_wind",
            "10m_v_component_of_wind",
        ],
    )
    parser.add_argument(
        "--nyears", help="number of years to write (from start)", default=None, type=int
    )
    parser.add_argument("--dry-run", action="store_true", help="dry run/?", default=False)
    args = parser.parse_args()
    # setup_for_benchmarks(ERA5_TIGRIS, ref=get_version(), arrays_to_write=args.arrays)
    dataset = make_dataset(
        store=args.store, prefix="era5_weatherbench2", group="1x721x1440"
    )
    print(dataset)
    setup_dataset(
        dataset,
        nyears=args.nyears,
        create=args.create,
        arrays_to_write=args.arrays,
        dry_run=args.dry_run,
    )


# always same prefix; different constructor, bucket name
# create_era5 --object_store tigris --nyears 20 --arrays ...
# create_era5 --object_store s3 --nyears 20 --arrays ...
