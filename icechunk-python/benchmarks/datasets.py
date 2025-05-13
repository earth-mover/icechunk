import datetime
import random
import time
import warnings
from collections.abc import Callable
from dataclasses import dataclass, field
from functools import partial
from typing import Any, Literal, Self, TypeAlias

import fsspec
import numpy as np
import platformdirs
import pytest

import icechunk as ic
import icechunk.xarray
import xarray as xr
import zarr
from benchmarks.helpers import (
    get_coiled_kwargs,
    get_splitting_config,
    rdms,
    repo_config_with,
    setup_logger,
)

rng = np.random.default_rng(seed=123)

Store: TypeAlias = Literal["s3", "gcs", "az", "tigris"]
PUBLIC_DATA_BUCKET = "icechunk-public-data"
ZARR_KWARGS = dict(zarr_format=3, consolidated=False)

CONSTRUCTORS = {
    "s3": ic.s3_storage,
    "s3_ob": ic.storage.s3_object_store_storage,
    "gcs": ic.gcs_storage,
    "tigris": ic.tigris_storage,
    "local": ic.local_filesystem_storage,
    "r2": ic.s3_storage,
}
TEST_BUCKETS = {
    "s3": dict(store="s3", bucket="icechunk-test", region="us-east-1"),
    "gcs": dict(store="gcs", bucket="icechunk-test-gcp", region="us-east1"),
    # "gcs": dict(store="gcs", bucket="arraylake-scratch", region="us-east1"),
    # not using region="auto", because for now we pass this directly to coiled.
    "r2": dict(store="r2", bucket="icechunk-test-r2", region="us-east-1"),
    # "tigris": dict(
    #     store="tigris", bucket="deepak-private-bucket" + "-test", region="iad"
    # ),
    "tigris": dict(store="tigris", bucket="icechunk-test", region="iad"),
    "local": dict(store="local", bucket=platformdirs.site_cache_dir()),
}
TEST_BUCKETS["s3_ob"] = TEST_BUCKETS["s3"]
BUCKETS = {
    "s3": dict(store="s3", bucket=PUBLIC_DATA_BUCKET, region="us-east-1"),
    "gcs": dict(store="gcs", bucket=PUBLIC_DATA_BUCKET + "-gcs", region="us-east1"),
    "tigris": dict(store="tigris", bucket=PUBLIC_DATA_BUCKET + "-tigris", region="iad"),
    "r2": dict(store="r2", bucket=PUBLIC_DATA_BUCKET + "-r2", region="us-east-1"),
}

logger = setup_logger()


def set_env_credentials() -> tuple[str, str]:
    import boto3

    session = boto3.Session()
    creds = session.get_credentials()
    client = boto3.client("s3")
    return {
        "access_key_id": creds.access_key,
        "secret_access_key": creds.secret_key,
        "endpoint_url": client.meta.endpoint_url,
    }


@dataclass
class StorageConfig:
    """wrapper that allows us to config the prefix for a ref."""

    store: str | None = None
    config: dict[str, Any] = field(default_factory=dict)
    bucket: str | None = None
    prefix: str | None = None
    region: str | None = None

    @property
    def path(self) -> str:
        if self.store != "local":
            raise ValueError(f"can't grab path for {self.store=!r}")
        return f"{self.bucket}/{self.prefix}"

    def create(self) -> ic.Storage:
        if self.store is None:
            raise ValueError("StorageConfig.store is None!")
        kwargs = {}
        if self.store == "local":
            kwargs["path"] = self.path
        else:
            if self.bucket is not None:
                kwargs["bucket"] = self.bucket
            if self.prefix is not None:
                kwargs["prefix"] = self.prefix
            if self.region is not None and self.store not in ["gcs"]:
                kwargs["region"] = self.region
            if self.store in ["tigris", "r2"]:
                # {"from_env": True} fails on coiled because the credentials aren't shipped
                kwargs.update(set_env_credentials())
        return CONSTRUCTORS[self.store](**self.config, **kwargs)

    def with_overwrite(
        self,
        *,
        store: str | None = None,
        bucket: str | None = None,
        region: str | None = None,
    ) -> Self:
        return type(self)(
            store=store if store is not None else self.store,
            bucket=bucket if bucket is not None else self.bucket,
            region=region if region is not None else self.region,
            prefix=self.prefix,
            config=self.config,
        )

    def with_extra(
        self,
        *,
        prefix: str | None = None,
        force_idempotent: bool = False,
    ) -> Self:
        if self.prefix is not None:
            if force_idempotent and self.prefix.startswith(prefix):
                return self
            new_prefix = (prefix or "") + self.prefix
        else:
            new_prefix = None

        return type(self)(
            store=self.store,
            bucket=self.bucket,
            prefix=new_prefix,
            region=self.region,
            config=self.config,
        )

    @property
    def env_vars(self) -> dict[str, str]:
        # if self.store == "tigris":
        #     # https://www.tigrisdata.com/docs/iam/#create-an-access-key
        #     return {"AWS_ENDPOINT_URL_IAM": "https://fly.iam.storage.tigris.dev"}
        return {}

    @property
    def protocol(self) -> str:
        if self.store in ("s3", "tigris"):
            protocol = "s3"
        elif self.store == "gcs":
            protocol = "gcs"
        else:
            protocol = "file"
        return protocol

    def clear_uri(self) -> str:
        """URI to clear when re-creating data from scratch."""
        if self.store == "local":
            return f"{self.protocol}://{self.path}"
        else:
            return f"{self.protocol}://{self.bucket}/{self.prefix}"

    def get_coiled_kwargs(self) -> str:
        return get_coiled_kwargs(store=self.store, region=self.region)


@dataclass
class Dataset:
    """
    Helper class that defines a Dataset used for a benchmark.
    """

    storage_config: StorageConfig
    # core useful group path used to open an Xarray Dataset
    group: str | None = None
    _storage: ic.Storage | None = field(default=None, init=False, repr=False)

    @property
    def storage(self) -> ic.Storage:
        if self._storage is None:
            self._storage = self.storage_config.create()
        return self._storage

    def create(
        self, clear: bool = False, config: ic.RepositoryConfig | None = None
    ) -> ic.Repository:
        if clear:
            clear_uri = self.storage_config.clear_uri()
            if clear_uri is None:
                raise NotImplementedError
            if self.storage_config.protocol not in ["file", "s3", "gcs"]:
                warnings.warn(
                    f"Only clearing of GCS, S3-compatible URIs supported at the moment. Received {clear_uri!r}",
                    RuntimeWarning,
                    stacklevel=2,
                )
            else:
                fs = fsspec.filesystem(self.storage_config.protocol)
                try:
                    logger.info(f"Clearing prefix: {clear_uri!r}")
                    fs.rm(clear_uri, recursive=True)
                except FileNotFoundError:
                    pass
        logger.info(repr(self.storage))
        return ic.Repository.create(self.storage, config=config)

    @property
    def store(self) -> ic.IcechunkStore:
        repo = ic.Repository.open(self.storage)
        return repo.readonly_session(branch="main").store


@dataclass(kw_only=True)
class BenchmarkWriteDataset(Dataset):
    num_arrays: int
    shape: tuple[int, ...]
    chunks: tuple[int, ...]
    # whether to skip this one on local runs
    skip_local: bool = False


@dataclass(kw_only=True)
class BenchmarkReadDataset(Dataset):
    # data variable to load in `time_xarray_read_chunks`
    load_variables: list[str] | None = None
    # Passed to .isel for `time_xarray_read_chunks`, reads a single chunk
    chunk_selector: dict[str, Any] | None = None
    # Passed to .isel for `time_xarray_read_chunks`, reads a large fraction of the data
    full_load_selector: dict[str, Any] | None = None
    # name of (coordinate) variable used for testing "time to first byte"
    first_byte_variable: str | None
    # function used to construct the dataset prior to read benchmarks
    setupfn: Callable | None = None
    # whether to skip this one on local runs
    skip_local: bool = False

    def create(self, clear: bool = True, config: ic.RepositoryConfig | None = None):
        if clear is not True:
            raise ValueError("clear *must* be true for benchmark datasets.")
        return super().create(clear=True, config=config)

    def setup(self, force: bool = False) -> None:
        """
        force: if True, recreate from scratch. If False, try opening the store,
        if it succeeds, do nothing.
        """
        if self.setupfn is None:
            raise NotImplementedError("setupfn has not been provided.")

        if force:
            logger.info("forced re-creating")
            self.setupfn(self)
            return

        try:
            _ = self.store
        except ic.IcechunkError as e:
            logger.info("Read of existing store failed. Re-creating")
            logger.info(e)
            self.setupfn(self)


@dataclass(kw_only=True)
class IngestDataset:
    name: str
    source_uri: str
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
                self.source_uri,
                chunks=chunks or self.read_chunks,
                engine=self.engine,
                **kwargs,
            ).drop_encoding()

    def make_dataset(self, *, store: str, debug: bool) -> Dataset:
        buckets = BUCKETS if not debug else TEST_BUCKETS
        extra_prefix = f"_{rdms()}" if debug else ""
        storage_config = StorageConfig(
            prefix=self.prefix + extra_prefix, **buckets[store]
        )
        return Dataset(storage_config=storage_config, group=self.group)


def setup_synthetic_gb_dataset(
    dataset: Dataset,
    chunk_shape: tuple[int, ...],
    shape: tuple[int, ...] = (512, 512, 512),
):
    repo = dataset.create()
    session = repo.writable_session("main")
    store = session.store
    group = zarr.group(store)
    array = group.create_array(
        name="array",
        shape=shape,
        chunks=chunk_shape,
        dtype=np.int64,
        dimension_names=("t", "y", "x"),
        compressors=None,
        filters=None,
    )
    array[:] = rng.integers(-1000, high=1000, size=shape, dtype=np.int64)
    session.commit("initialized")


def setup_era5_single(dataset: Dataset):
    import pooch

    # FIXME: move to earthmover-sample-data
    url = "https://nsf-ncar-era5.s3.amazonaws.com/e5.oper.an.pl/194106/e5.oper.an.pl.128_060_pv.ll025sc.1941060100_1941060123.nc"
    logger.info(f"Reading {url}")
    tic = time.time()
    ds = xr.open_dataset(
        # using pooch means we download only once on a local machine
        pooch.retrieve(
            url,
            known_hash="2322a4baaabd105cdc68356bdf2447b59fbbec559ee1f9a9a91d3c94f242701a",
        ),
        engine="h5netcdf",
    )
    ds = ds.drop_encoding().load()
    logger.info(f"Loaded data in {time.time() - tic} seconds")

    repo = dataset.create()
    session = repo.writable_session("main")
    tic = time.time()
    encoding = {
        "PV": {"compressors": [zarr.codecs.ZstdCodec()], "chunks": (1, 1, 721, 1440)}
    }
    logger.info("Writing data...")
    ds.to_zarr(
        session.store, mode="w", zarr_format=3, consolidated=False, encoding=encoding
    )
    logger.info(f"Wrote data in {time.time() - tic} seconds")
    session.commit(f"wrote data at {datetime.datetime.now(datetime.UTC)}")


def setup_ingest_for_benchmarks(dataset: Dataset, *, ingest: IngestDataset) -> None:
    """
    For benchmarks, we
    1. add a specific prefix.
    2. always write the metadata for the WHOLE dataset
    3. then append a small subset of data for a few arrays
    """
    from benchmarks.create_era5 import Mode, write

    repo = dataset.create()
    ds = ingest.open_dataset()
    logger.info("Initializing dataset for benchmarks..")
    session = repo.writable_session("main")
    ds.to_zarr(
        session.store, compute=False, mode="w-", group=dataset.group, **ZARR_KWARGS
    )
    session.commit("initialized dataset")
    logger.info("Finished initializing dataset.")

    if ingest.arrays:
        attrs = {
            "written_arrays": " ".join(ingest.arrays),
        }
        write(
            dataset,
            ingest=ingest,
            mode=Mode.APPEND,
            extra_attrs=attrs,
            arrays_to_write=ingest.arrays,
            initialize_all_vars=False,
        )


def setup_era5(*args, **kwargs):
    from benchmarks.create_era5 import setup_for_benchmarks

    return setup_for_benchmarks(*args, **kwargs, arrays_to_write=[])


def setup_split_manifest_refs(dataset: Dataset, *, split_size: int | None):
    shape = (500_000 * 1000,)
    chunks = (1000,)

    if split_size is not None:
        try:
            splitting = get_splitting_config(split_size=split_size)
        except ImportError:
            logger.info("splitting not supported")
            pytest.skip("splitting not supported on this version")
    else:
        splitting = None
    config = repo_config_with(splitting=splitting)
    assert config is not None
    if hasattr(config.manifest, "splitting"):
        assert config.manifest.splitting == splitting
    repo = dataset.create(config=config)
    logger.info(repo.config)
    session = repo.writable_session(branch="main")
    ds = xr.Dataset({"array": ("x", np.ones(shape=shape))})
    with zarr.config.set({"async.concurrency": 64}):
        ic.xarray.to_icechunk(ds, session, encoding={"array": {"chunks": chunks}})
    session.commit("wrote data")


ERA5_ARCO_INGEST = IngestDataset(
    name="ERA5-ARCO",
    prefix="era5_arco",
    source_uri="gs://gcp-public-data-arco-era5/ar/full_37-1h-0p25deg-chunk-1.zarr-v3",
    engine="zarr",
    read_chunks={"time": 72 * 24, "level": 1},
    write_chunks={"time": 1, "level": 1, "latitude": 721, "longitude": 1440},
    group="1x721x1440",
    arrays=[],
)

ERA5 = BenchmarkReadDataset(
    # weatherbench2 data - 5 years
    skip_local=False,
    storage_config=StorageConfig(prefix="era5-weatherbench"),
    load_variables=["2m_temperature"],
    chunk_selector={"time": 1},
    first_byte_variable="latitude",
    group="1x721x1440",
    # don't set setupfn here so we don't run a really expensive job
    # by mistake
    # setupfn=partial(setup_ingest_for_benchmarks, ingest=ERA5_WB),
)

ERA5_ARCO = BenchmarkReadDataset(
    skip_local=False,
    storage_config=StorageConfig(prefix="era5-arco"),
    first_byte_variable="latitude",
    group="1x721x1440",
    setupfn=partial(setup_ingest_for_benchmarks, ingest=ERA5_ARCO_INGEST),
)

# ERA5_LARGE = BenchmarkReadDataset(
#     skip_local=True,
#     storage_config=StorageConfig(
#         bucket="icechunk-public-data", prefix="era5-weatherbench2"
#     ),
#     load_variables=["2m_temperature"],
#     chunk_selector={"time": 1},
#     first_byte_variable="latitude",
#     group="1x721x1440",
#     # don't set setupfn here so we don't run a really expensive job
#     # by mistake
# )

ERA5_SINGLE = BenchmarkReadDataset(
    # Single NCAR AWS PDS ERA5 netCDF
    storage_config=StorageConfig(prefix="perf-era5-single"),
    load_variables=["PV"],
    chunk_selector={"time": 1},
    first_byte_variable="latitude",
    setupfn=setup_era5_single,
)

GB_128MB_CHUNKS = BenchmarkReadDataset(
    storage_config=StorageConfig(prefix="gb-128mb-chunks"),
    load_variables=["array"],
    chunk_selector={},
    first_byte_variable=None,
    setupfn=partial(setup_synthetic_gb_dataset, chunk_shape=(64, 512, 512)),
)

GB_8MB_CHUNKS = BenchmarkReadDataset(
    storage_config=StorageConfig(prefix="gb-8mb-chunks"),
    load_variables=["array"],
    chunk_selector={},
    first_byte_variable=None,
    setupfn=partial(setup_synthetic_gb_dataset, chunk_shape=(4, 512, 512)),
)

random_selector = sorted(random.choices(range(500_000 * 1000), k=50_000))

# large manifest sharded, unsharded
LARGE_MANIFEST_UNSHARDED = BenchmarkReadDataset(
    storage_config=StorageConfig(prefix="large_manifest_no_split"),
    chunk_selector={"x": 1},
    full_load_selector={"x": random_selector},
    load_variables=["array"],
    first_byte_variable=None,
    setupfn=partial(setup_split_manifest_refs, split_size=None),
)
LARGE_MANIFEST_SHARDED = BenchmarkReadDataset(
    storage_config=StorageConfig(prefix="large_manifest_split"),
    chunk_selector={"x": 1},
    full_load_selector={"x": random_selector},
    load_variables=["array"],
    first_byte_variable=None,
    setupfn=partial(setup_split_manifest_refs, split_size=100_000),
)

# TODO
GPM_IMERG_VIRTUAL = BenchmarkReadDataset(
    storage_config=StorageConfig(
        store="s3",
        bucket="earthmover-icechunk-us-west-2",
        prefix="nasa-impact/GPM_3IMERGHH.07-virtual-1998",
        region="us-west-2",
        # access_key_id=access_key_id,
        # secret_access_key=secret,
        # session_token=session_token,
    ),
    load_variables=["foo"],
    chunk_selector={"time": 1},
    first_byte_variable="lat",
)


PANCAKE_WRITES = BenchmarkWriteDataset(
    storage_config=StorageConfig(prefix="pancake_writes"),
    num_arrays=1,
    shape=(320, 720, 1441),
    chunks=(1, -1, -1),
)
SIMPLE_1D = BenchmarkWriteDataset(
    storage_config=StorageConfig(prefix="simple_1d_writes"),
    num_arrays=1,
    shape=(2000 * 1000,),
    chunks=(1000,),
)
LARGE_1D = BenchmarkWriteDataset(
    storage_config=StorageConfig(prefix="large_1d_writes"),
    num_arrays=1,
    shape=(500_000 * 1000,),
    chunks=(1000,),
)
