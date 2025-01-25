import datetime
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from functools import partial
from typing import Any, Self

import fsspec
import numpy as np

import icechunk as ic
import xarray as xr
import zarr

rng = np.random.default_rng(seed=123)


@dataclass
class StorageConfig:
    """wrapper that allows us to config the prefix for a ref."""

    constructor: Callable
    config: Any
    bucket: str | None = None
    prefix: str | None = None
    path: str | None = None

    def create(self) -> ic.Storage:
        kwargs = {}
        if self.bucket is not None:
            kwargs["bucket"] = self.bucket
        if self.prefix is not None:
            kwargs["prefix"] = self.prefix
        if self.path is not None:
            kwargs["path"] = self.path
        return self.constructor(config=self.config, **kwargs)

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
            constructor=self.constructor,
            bucket=self.bucket,
            prefix=new_prefix,
            path=new_path,
            config=self.config,
        )

    def clear_uri(self) -> str:
        """URI to clear when re-creating data from scratch."""
        if self.constructor == ic.Storage.new_s3:
            protocol = "s3://"
        else:
            protocol = ""

        if self.bucket is not None:
            return f"{protocol}{self.bucket}/{self.prefix}"
        elif self.path is not None:
            return self.path
        else:
            raise NotImplementedError("I don't know what to do here.")


@dataclass
class Dataset:
    """
    Helper class that defines a Dataset used for a benchmark.
    """

    storage_config: StorageConfig
    # data variable to load in `time_xarray_read_chunks`
    load_variables: list[str]
    # Passed to .isel for `time_xarray_read_chunks`
    chunk_selector: dict[str, Any]
    # name of (coordinate) variable used for testing "time to first byte"
    first_byte_variable: str | None
    # core useful group path used to open an Xarray Dataset
    group: str | None = None
    # function used to construct the dataset prior to read benchmarks
    setupfn: Callable | None = None
    _storage: ic.Storage | None = field(default=None, init=False)

    @property
    def storage(self) -> ic.Storage:
        if self._storage is None:
            self._storage = self.storage_config.create()
        return self._storage

    def create(self) -> ic.Repository:
        clear_uri = self.storage_config.clear_uri()
        if clear_uri is None:
            raise NotImplementedError
        if not clear_uri.startswith("s3://"):
            raise NotImplementedError(
                f"Only S3 URIs supported at the moment. Received {clear_uri}"
            )
        fs = fsspec.filesystem("s3")
        try:
            fs.rm(f"{clear_uri}", recursive=True)
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
ERA5 = Dataset(
    storage_config=StorageConfig(
        constructor=ic.Storage.new_s3,
        bucket="icechunk-test",
        prefix="era5-weatherbench",
        config=ic.S3Options(),
    ),
    load_variables=["2m_temperature"],
    chunk_selector={"time": 1},
    first_byte_variable="latitude",
    group="1x721x1440",
    # don't set setupfn here so we don't run a really expensive job
    # by mistake
)

ERA5_SINGLE = Dataset(
    storage_config=StorageConfig(
        constructor=ic.Storage.new_s3,
        bucket="icechunk-test",
        prefix="perf-era5-single",
        config=ic.S3Options(),
    ),
    load_variables=["PV"],
    chunk_selector={"time": 1},
    first_byte_variable="latitude",
    setupfn=setup_era5_single,
)

GB_128MB_CHUNKS = Dataset(
    storage_config=StorageConfig(
        constructor=ic.Storage.new_s3,
        bucket="icechunk-test",
        prefix="gb-128mb-chunks",
        config=ic.S3Options(),
    ),
    load_variables=["array"],
    chunk_selector={},
    first_byte_variable=None,
    setupfn=partial(setup_synthetic_gb_dataset, chunk_shape=(64, 512, 512)),
)

GB_8MB_CHUNKS = Dataset(
    storage_config=StorageConfig(
        constructor=ic.Storage.new_s3,
        bucket="icechunk-test",
        prefix="gb-8mb-chunks",
        config=ic.S3Options(),
    ),
    load_variables=["array"],
    chunk_selector={},
    first_byte_variable=None,
    setupfn=partial(setup_synthetic_gb_dataset, chunk_shape=(4, 512, 512)),
)

# TODO
GPM_IMERG_VIRTUAL = Dataset(
    storage_config=StorageConfig(
        constructor=ic.Storage.new_s3,
        bucket="earthmover-icechunk-us-west-2",
        prefix="nasa-impact/GPM_3IMERGHH.07-virtual-1998",
        config=ic.S3Options(),
        # access_key_id=access_key_id,
        # secret_access_key=secret,
        # session_token=session_token,
    ),
    load_variables=["foo"],
    chunk_selector={"time": 1},
    first_byte_variable="lat",
)
