import datetime
import time
import warnings
from collections.abc import Callable
from dataclasses import dataclass, field
from functools import partial
from typing import Any, Literal, Self, TypeAlias

import fsspec
import numpy as np

import icechunk as ic
import xarray as xr
import zarr

rng = np.random.default_rng(seed=123)

Store: TypeAlias = Literal["s3", "gcs", "az", "tigris"]

CONSTRUCTORS = {
    "s3": ic.s3_storage,
    "gcs": ic.gcs_storage,
    "tigris": ic.tigris_storage,
}


@dataclass
class StorageConfig:
    """wrapper that allows us to config the prefix for a ref."""

    store: str
    config: dict[str, Any] = field(default_factory=dict)
    bucket: str | None = None
    prefix: str | None = None
    region: str | None = None
    path: str | None = None

    def create(self) -> ic.Storage:
        kwargs = {}
        if self.bucket is not None:
            kwargs["bucket"] = self.bucket
        if self.prefix is not None:
            kwargs["prefix"] = self.prefix
        if self.path is not None:
            kwargs["path"] = self.path
        if self.region is not None and self.store != "gcs":
            kwargs["region"] = self.region
        return CONSTRUCTORS[self.store](**self.config, **kwargs)

    def with_extra(
        self, *, prefix: str | None = None, force_idempotent: bool = False
    ) -> Self:
        if self.prefix is not None:
            if force_idempotent and self.prefix.startswith(prefix):
                return self
            new_prefix = (prefix or "") + self.prefix
        else:
            new_prefix = None

        if self.path is not None:
            if force_idempotent and self.path.startswith(prefix):
                return self
            new_path = (prefix or "") + self.path
        else:
            new_path = None
        return type(self)(
            store=self.store,
            bucket=self.bucket,
            prefix=new_prefix,
            path=new_path,
            region=self.region,
            config=self.config,
        )

    def clear_uri(self) -> str:
        """URI to clear when re-creating data from scratch."""
        if self.store in ("s3", "tigtis"):
            protocol = "s3://"
        elif self.store == "gcs":
            protocol = "gcs://"
        else:
            protocol = ""

        if self.bucket is not None:
            return f"{protocol}{self.bucket}/{self.prefix}"
        elif self.path is not None:
            return self.path
        else:
            raise NotImplementedError("I don't know what to do here.")

    def get_coiled_backend(self) -> str:
        BACKENDS = {"s3": "aws", "gcs": "gcp", "tigris": "aws"}
        return BACKENDS[self.store]


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

    def create(self, clear: bool = False) -> ic.Repository:
        if clear:
            clear_uri = self.storage_config.clear_uri()
            if clear_uri is None:
                raise NotImplementedError
            if self.storage_config.store == "tigris":
                warnings.warn(
                    f"Only clearing of GCS, S3 URIs supported at the moment. Received {clear_uri!r}",
                    RuntimeWarning,
                    stacklevel=2,
                )
            else:
                fs = fsspec.filesystem(self.storage_config.store)
                try:
                    fs.rm(clear_uri, recursive=True)
                except FileNotFoundError:
                    pass
        return ic.Repository.create(self.storage)

    @property
    def store(self) -> ic.IcechunkStore:
        repo = ic.Repository.open(self.storage)
        return repo.readonly_session(branch="main").store

    def setup(self, force: bool = False) -> None:
        """
        force: if True, recreate from scratch. If False, try opening the store,
        if it succeeds, do nothing.
        """
        if self.setupfn is None:
            raise NotImplementedError("setupfn has not been provided.")

        if force:
            print("forced re-creating")
            self.setupfn(self)
            return

        try:
            _ = self.store
        except ic.IcechunkError as e:
            print("Read of existing store failed. Re-creating")
            print(e)
            self.setupfn(self)


@dataclass(kw_only=True)
class BenchmarkDataset(Dataset):
    # data variable to load in `time_xarray_read_chunks`
    load_variables: list[str]
    # Passed to .isel for `time_xarray_read_chunks`
    chunk_selector: dict[str, Any]
    # name of (coordinate) variable used for testing "time to first byte"
    first_byte_variable: str | None
    # function used to construct the dataset prior to read benchmarks
    setupfn: Callable | None = None

    def create(self, clear: bool = True):
        if clear is not True:
            raise ValueError("clear *must* be true for benchmark datasets.")
        super().create(clear=True)


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
    print(f"Reading {url}")
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
    print(f"Loaded data in {time.time() - tic} seconds")

    repo = dataset.create()
    session = repo.writable_session("main")
    tic = time.time()
    encoding = {
        "PV": {"compressors": [zarr.codecs.ZstdCodec()], "chunks": (1, 1, 721, 1440)}
    }
    print("Writing data...")
    ds.to_zarr(
        session.store, mode="w", zarr_format=3, consolidated=False, encoding=encoding
    )
    print(f"Wrote data in {time.time() - tic} seconds")
    session.commit(f"wrote data at {datetime.datetime.now(datetime.UTC)}")


# TODO: passing Storage directly is nice, but doesn't let us add an extra prefix.
ERA5 = BenchmarkDataset(
    storage_config=StorageConfig(
        store="s3",
        bucket="icechunk-test",
        prefix="era5-weatherbench",
    ),
    load_variables=["2m_temperature"],
    chunk_selector={"time": 1},
    first_byte_variable="latitude",
    group="1x721x1440",
    # don't set setupfn here so we don't run a really expensive job
    # by mistake
)

ERA5_LARGE = BenchmarkDataset(
    storage_config=StorageConfig(
        store="s3",
        bucket="icechunk-public-data",
        prefix="era5-weatherbench2",
    ),
    load_variables=["2m_temperature"],
    chunk_selector={"time": 1},
    first_byte_variable="latitude",
    group="1x721x1440",
    # don't set setupfn here so we don't run a really expensive job
    # by mistake
)

ERA5_SINGLE = BenchmarkDataset(
    storage_config=StorageConfig(
        store="s3",
        bucket="icechunk-test",
        prefix="perf-era5-single",
    ),
    load_variables=["PV"],
    chunk_selector={"time": 1},
    first_byte_variable="latitude",
    setupfn=setup_era5_single,
)

GB_128MB_CHUNKS = BenchmarkDataset(
    storage_config=StorageConfig(
        store="s3",
        bucket="icechunk-test",
        prefix="gb-128mb-chunks",
    ),
    load_variables=["array"],
    chunk_selector={},
    first_byte_variable=None,
    setupfn=partial(setup_synthetic_gb_dataset, chunk_shape=(64, 512, 512)),
)

GB_8MB_CHUNKS = BenchmarkDataset(
    storage_config=StorageConfig(
        store="s3",
        bucket="icechunk-test",
        prefix="gb-8mb-chunks",
    ),
    load_variables=["array"],
    chunk_selector={},
    first_byte_variable=None,
    setupfn=partial(setup_synthetic_gb_dataset, chunk_shape=(4, 512, 512)),
)

# TODO
GPM_IMERG_VIRTUAL = BenchmarkDataset(
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
