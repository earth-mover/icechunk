# Consolidate some "big" dataset creation
# My workflow is:
#   1. just create-deepak-env v0.1.0a12
#   2. conda activate icechunk-v0.1.0a12
#   3. python benchmarks/create-era5.py

import argparse
import datetime
import math
import random
import warnings
from dataclasses import dataclass
from enum import StrEnum, auto
from typing import Any

import helpers
import humanize
import pandas as pd
from datasets import Dataset, StorageConfig
from packaging.version import Version

import dask
import icechunk as ic
import xarray as xr
import zarr
from dask.diagnostics import ProgressBar
from icechunk.xarray import to_icechunk

logger = helpers.setup_loger()

PUBLIC_DATA_BUCKET = "icechunk-public-data"
ICECHUNK_FORMAT = f"v{ic.spec_version():02d}"
ZARR_KWARGS = dict(zarr_format=3, consolidated=False)

DEBUG_BUCKETS = {
    "s3": dict(store="s3", bucket="icechunk-test", region="us-east-1"),
    "gcs": dict(store="gcs", bucket="icechunk-test-gcp", region="us-east1"),
    "tigris": dict(
        store="tigris", bucket="icechunk-test" + "-tigris", region="us-east-1"
    ),
}

BUCKETS = {
    "s3": dict(store="s3", bucket=PUBLIC_DATA_BUCKET, region="us-east-1"),
    "gcs": dict(store="gcs", bucket=PUBLIC_DATA_BUCKET + "-gcs", region="us-east1"),
    "tigris": dict(store="tigris", bucket=PUBLIC_DATA_BUCKET + "-tigris", region="iad"),
}


@dataclass(kw_only=True)
class IngestDataset:
    name: str
    uri: str
    group: str
    prefix: str
    write_chunks: dict[str, int]
    arrays: list[str]
    engine: str | None = None
    read_chunks: dict[str, int] | None = None

    def open_dataset(self, chunks=None, **kwargs: Any) -> xr.Dataset:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=UserWarning)
            return xr.open_dataset(
                self.uri, chunks=chunks or self.read_chunks, engine=self.engine, **kwargs
            ).drop_encoding()

    def make_dataset(self, *, store: str, debug: bool) -> Dataset:
        buckets = BUCKETS if not debug else DEBUG_BUCKETS
        storage_config = StorageConfig(prefix=self.prefix, **buckets[store])
        return Dataset(storage_config=storage_config, group=self.group)


ERA5 = IngestDataset(
    name="ERA5",
    prefix="era5_weatherbench2",
    uri="gs://weatherbench2/datasets/era5/1959-2023_01_10-full_37-1h-0p25deg-chunk-1.zarr",
    engine="zarr",
    read_chunks={"time": 24 * 3, "level": 1},
    write_chunks={"time": 1, "level": 1, "latitude": 721, "longitude": 1440},
    group="1x721x1440",
    arrays=["2m_temperature", "10m_u_component_of_wind", "10m_v_component_of_wind"],
)


class Mode(StrEnum):
    APPEND = auto()
    CREATE = auto()
    OVERWRITE = auto()
    VERIFY = auto()


def verify(dataset: Dataset, *, ingest: IngestDataset, seed: int | None = None):
    random.seed(seed)

    format = ICECHUNK_FORMAT
    prefix = f"{format}/"
    dataset.storage_config = dataset.storage_config.with_extra(prefix=prefix)
    repo = ic.Repository.open(dataset.storage)
    session = repo.readonly_session(branch="main")
    instore = xr.open_dataset(
        session.store, group=dataset.group, engine="zarr", chunks=None, consolidated=False
    )
    time = pd.Timestamp(random.choice(instore.time.data.tolist()))
    logger.info(f"Verifying {ingest.name} for {seed=!r}, {time=!r}")
    actual = instore.sel(time=time)

    ds = ingest.open_dataset(chunks=None)
    expected = ds[list(instore.data_vars)].sel(time=time)

    # I add global attrs, don't compare those
    expected.attrs.clear()
    actual.attrs.clear()

    with ProgressBar():
        actual, expected = dask.compute(actual, expected)
        # assert (expected == actual).all().to_array().all()
    xr.testing.assert_identical(expected, actual)
    logger.info("Successfully verified!")


def write(
    dataset: Dataset,
    *,
    ingest: IngestDataset,
    mode: Mode,
    nyears: int | None = None,
    arrays_to_write: list[str] | None = None,
    extra_attrs: dict[str, str] | None = None,
    initialize_all_vars: bool = False,
    dry_run: bool = False,
) -> None:
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
    if mode in [Mode.CREATE, Mode.OVERWRITE]:
        write_mode = "w"
    elif mode is Mode.APPEND:
        write_mode = "a"

    ic_kwargs = dict(group=dataset.group, mode=write_mode)

    ds = ingest.open_dataset()
    if arrays_to_write is not None:
        towrite = ds[arrays_to_write].isel(SELECTOR)
    else:
        towrite = ds.isel(SELECTOR)
    towrite.attrs["selector"] = str(SELECTOR)
    towrite.attrs.update(extra_attrs or {})
    for v in towrite:
        towrite[v].encoding["chunks"] = tuple(
            ingest.write_chunks[dim] for dim in ds[v].dims
        )

    nchunks = tuple(
        math.prod((var.sizes[dim] // ingest.write_chunks[dim] + 1) for dim in var.dims)
        for _, var in towrite.data_vars.items()
    )
    logger.info(
        f"Size: {humanize.naturalsize(towrite.nbytes)}, "
        f"Total nchunks= {humanize.intcomma(sum(nchunks))}, "
        f"per array: {[humanize.intcomma(i) for i in nchunks]}"
    )

    repo = ic.Repository.open(dataset.storage)

    if dry_run:
        print("Dry run. Exiting")
        return

    # FIXME: use name
    session = repo.writable_session("main")
    with coiled.Cluster(
        name="deepak-era5",
        shutdown_on_close=False,
        n_workers=(4, 200),
        worker_cpu=2,
        **dataset.storage_config.get_coiled_kwargs(),
    ) as cluster:
        # https://docs.coiled.io/user_guide/clusters/environ.html
        cluster.send_private_envs(dataset.storage_config.env_vars)
        client = distributed.Client(cluster)  # type: ignore[no-untyped-call]
        print(client)
        with distributed.performance_report(  # type: ignore[no-untyped-call]
            f"reports/{ingest.name}-ingest-{dataset.storage_config.store}-{ICECHUNK_FORMAT}-{datetime.datetime.now()}.html"
        ):
            logger.info(f"Started writing {tuple(towrite.data_vars)}.")
            with zarr.config.set({"async.concurrency": 24}):
                to_icechunk(towrite, session=session, **ic_kwargs, split_every=32)
    session.commit("ingest!")
    logger.info(f"Finished writing {tuple(towrite.data_vars)}.")


def setup_for_benchmarks(
    dataset: Dataset,
    *,
    ingest: IngestDataset,
    mode: Mode,
    ref: str,
    arrays_to_write: list[str],
) -> None:
    """
    FIXME: set the right bucket.
    For benchmarks, we
    1. add a specific prefix.
    2. always write the metadata for the WHOLE dataset
    3. then append a small subset of data for a few arrays
    """
    ref = helpers.get_version()

    commit = helpers.get_commit(ref)
    logger.info(
        f"Benchmarks: Writing {ingest.name} for {ref=}, {commit=}, {arrays_to_write=}"
    )
    prefix = f"benchmarks/{ref}_{commit}/"
    dataset.storage_config = dataset.storage_config.with_extra(prefix=prefix)
    dataset.create()
    attrs = {
        "icechunk_commit": helpers.get_commit(ref),
        "icechunk_ref": ref,
        "written_arrays": " ".join(arrays_to_write),
    }

    repo = ic.Repository.open(dataset.storage)
    ds = ingest.open_dataset()

    if mode is Mode.CREATE:
        logger.info("Initializing dataset for benchmarks..")
        session = repo.writable_session("main")
        ds.to_zarr(
            session.store, compute=False, mode="w-", group=dataset.group, **ZARR_KWARGS
        )
        session.commit("initialized dataset")
        logger.info("Finished initializing dataset.")

    write(
        dataset,
        ingest=ingest,
        mode=Mode.APPEND,
        extra_attrs=attrs,
        arrays_to_write=arrays_to_write,
        initialize_all_vars=False,
    )


def setup_dataset(
    dataset: Dataset,
    *,
    ingest: IngestDataset,
    mode: Mode,
    dry_run: bool = False,
    **kwargs: Any,
) -> None:
    # commit = helpers.get_commit(ref)
    format = ICECHUNK_FORMAT
    logger.info(f"Writing {ingest.name} for {format}, {kwargs=}")
    prefix = f"{format}/"
    dataset.storage_config = dataset.storage_config.with_extra(prefix=prefix)
    if mode is Mode.CREATE:
        logger.info("Creating new repository")
        repo = dataset.create(clear=True)
        logger.info("Initializing root group")
        session = repo.writable_session("main")
        zarr.open_group(session.store, mode="w-")
        session.commit("initialized root group")
        logger.info("Initialized root group")

    write(
        dataset,
        ingest=ingest,
        mode=mode,
        initialize_all_vars=False,
        dry_run=dry_run,
        **kwargs,
    )


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
        "--mode", help="'create'/'overwrite'/'append'/'verify'", default="append"
    )
    parser.add_argument(
        "--nyears", help="number of years to write (from start)", default=None, type=int
    )
    parser.add_argument("--dry-run", action="store_true", help="dry run/?", default=False)
    parser.add_argument(
        "--append", action="store_true", help="append or create?", default=False
    )
    parser.add_argument("--arrays", help="arrays to write", nargs="+", default=[])
    parser.add_argument("--seed", help="random seed for verify", default=None, type=int)
    parser.add_argument(
        "--debug", help="write to debug bucket?", default=False, action="store_true"
    )

    args = parser.parse_args()
    if args.mode == "create":
        mode = Mode.CREATE
    elif args.mode == "append":
        mode = Mode.APPEND
    elif args.mode == "overwrite":
        mode = Mode.OVERWRITE
    elif args.mode == "verify":
        mode = Mode.VERIFY
    else:
        raise ValueError(
            f"mode must be one of ['create', 'overwrite', 'append', 'verify']. Received {args.mode=!r}"
        )

    ingest = ERA5
    dataset = ingest.make_dataset(store=args.store, debug=args.debug)
    logger.info(ingest)
    logger.info(dataset)
    logger.info(args)
    ds = ingest.open_dataset()
    if mode is Mode.VERIFY:
        verify(dataset, ingest=ingest, seed=args.seed)
    else:
        setup_dataset(
            dataset,
            ingest=ingest,
            nyears=args.nyears,
            mode=mode,
            arrays_to_write=args.arrays or ingest.arrays,
            dry_run=args.dry_run,
        )


# always same prefix; different constructor, bucket name
# create_era5 --object_store tigris --nyears 20 --arrays ...
# create_era5 --object_store s3 --nyears 20 --arrays ...
