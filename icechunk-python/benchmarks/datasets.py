import datetime
import time
from collections.abc import Callable
from dataclasses import dataclass
from functools import partial
from typing import Any

import fsspec
import numpy as np
import pooch

import icechunk as ic
import xarray as xr
import zarr

rng = np.random.default_rng(seed=123)


@dataclass
class Dataset:
    """
    Helper class that defines a Dataset used for a benchmark.
    """

    storage: ic.Storage
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
    # URI to clear when re-creating data from scratch
    clear_uri: str | None = None

    def create(self) -> ic.Repository:
        if self.clear_uri is None:
            raise NotImplementedError
        if not self.clear_uri.startswith("s3://"):
            raise NotImplementedError("Only S3 URIs supported at the moment.")
        fs = fsspec.filesystem("s3")
        try:
            fs.rm(f"{self.clear_uri}", recursive=True)
        except FileNotFoundError:
            pass
        return ic.Repository.create(self.storage)

    @property
    def store(self) -> ic.IcechunkStore:
        repo = ic.Repository.open(self.storage)
        return repo.readonly_session(branch="main").store

    def setup(self) -> None:
        if self.setupfn is None:
            raise NotImplementedError("setupfn has not been provided.")

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
    )
    array[:] = rng.integers(-1000, high=1000, size=shape, dtype=np.int64)
    session.commit("initialized")


def setup_era5_single(dataset: Dataset):
    # FIXME: move to earthmover-sample-data
    url = "https://nsf-ncar-era5.s3.amazonaws.com/e5.oper.an.pl/194106/e5.oper.an.pl.128_060_pv.ll025sc.1941060100_1941060123.nc"
    print(f"Reading {url}")
    tic = time.time()
    ds = xr.open_dataset(
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


ERA5 = Dataset(
    # TODO: this duplication is not great.
    # make these accessible from storage
    storage=ic.Storage.new_s3(
        bucket="icechunk-test",
        prefix="era5-demo-repository-a10",
        config=ic.S3Options(),
    ),
    load_variables=["2m_temperature"],
    chunk_selector={"time": 1},
    first_byte_variable="latitude",
)

ERA5_SINGLE = Dataset(
    clear_uri="s3://icechunk-test/perf-era5-single",
    storage=ic.Storage.new_s3(
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
    clear_uri="s3://icechunk-test/gb-128mb-chunks",
    storage=ic.Storage.new_s3(
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
    clear_uri="s3://icechunk-test/gb-8mb-chunks",
    storage=ic.Storage.new_s3(
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
    storage=ic.s3_storage(
        bucket="earthmover-icechunk-us-west-2",
        prefix="nasa-impact/GPM_3IMERGHH.07-virtual-1998",
        # access_key_id=access_key_id,
        # secret_access_key=secret,
        # session_token=session_token,
    ),
    load_variables=["foo"],
    chunk_selector={"time": 1},
    first_byte_variable="lat",
)
