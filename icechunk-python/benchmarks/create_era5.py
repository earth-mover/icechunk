# Consolidate some "big" dataset creation
# My workflow is:
#   1. just create-deepak-env v0.1.0a12
#   2. conda activate icechunk-v0.1.0a12
#   3. python benchmarks/create-era5.py

import argparse
import datetime
import math
import random
from enum import StrEnum, auto
from typing import Any

import humanize
import pandas as pd
from packaging.version import Version

import dask
import icechunk as ic
import xarray as xr
import zarr
from benchmarks import helpers
from benchmarks.datasets import Dataset, IngestDataset
from dask.diagnostics import ProgressBar
from icechunk.xarray import to_icechunk

logger = helpers.setup_logger()

ICECHUNK_FORMAT = f"v{ic.spec_version():d}"
ZARR_KWARGS = dict(zarr_format=3, consolidated=False)


class Mode(StrEnum):
    APPEND = auto()
    CREATE = auto()
    OVERWRITE = auto()
    VERIFY = auto()


ERA5_WB = IngestDataset(
    name="ERA5-WB",
    prefix="era5_weatherbench2",
    source_uri="gs://weatherbench2/datasets/era5/1959-2023_01_10-full_37-1h-0p25deg-chunk-1.zarr",
    engine="zarr",
    read_chunks={"time": 24 * 3, "level": 1},
    write_chunks={"time": 1, "level": 1, "latitude": 721, "longitude": 1440},
    group="1x721x1440",
    arrays=["2m_temperature", "10m_u_component_of_wind", "10m_v_component_of_wind"],
)


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

    # TODO: Parallelize the compare in `assert_identical` upstream
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

    ckwargs = dataset.storage_config.get_coiled_kwargs()
    session = repo.writable_session("main")
    with coiled.Cluster(
        name=f"icechunk-ingest-{ICECHUNK_FORMAT}-{ingest.name}",
        shutdown_on_close=False,
        n_workers=(4, 200),
        worker_cpu=2,
        workspace=ckwargs["workspace"],
        region=ckwargs["region"],
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
    parser.add_argument("--dry-run", action="store_true", help="dry run?", default=False)
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

    ingest = ERA5_WB
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
