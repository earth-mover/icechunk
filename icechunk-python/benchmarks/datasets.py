from dataclasses import dataclass
from typing import Any

import fsspec

import icechunk as ic


@dataclass
class Dataset:
    bucket: str
    prefix: str
    storage: ic.Storage
    # data variable to load in `time_xarray_read_chunks`
    load_variables: list[str]
    # Passed to .isel for `time_xarray_read_chunks`
    chunk_selector: dict[str, Any]
    # name of (coordinate) variable used for testing "time to first byte"
    first_byte_variable: str | None
    # core useful group
    group: str | None = None

    def create(self) -> ic.Repository:
        fs = fsspec.filesystem("s3")
        try:
            fs.rm(f"{self.bucket}/{self.prefix}", recursive=True)
        except FileNotFoundError:
            pass
        return ic.Repository.create(self.storage)

    @property
    def store(self) -> ic.IcechunkStore:
        repo = ic.Repository.open(self.storage)
        return repo.readonly_session(branch="main").store


ERA5 = Dataset(
    # FIXME: make these accessible from storage
    bucket="icechunk-test",
    prefix="era5-demo-repository-a10",
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
    bucket="icechunk-test",
    prefix="perf-era5-single",
    storage=ic.Storage.new_s3(
        bucket="icechunk-test",
        prefix="perf-era5-single",
        config=ic.S3Options(),
    ),
    load_variables=["PV"],
    chunk_selector={"time": 1},
    first_byte_variable="latitude",
)

GB_128MB_CHUNKS = Dataset(
    bucket="icechunk-test",
    prefix="gb-128mb-chunks",
    storage=ic.Storage.new_s3(
        bucket="icechunk-test",
        prefix="gb-128mb-chunks",
        config=ic.S3Options(),
    ),
    load_variables=["array"],
    chunk_selector={},
    first_byte_variable=None,
)

GB_8MB_CHUNKS = Dataset(
    bucket="icechunk-test",
    prefix="gb-8mb-chunks",
    storage=ic.Storage.new_s3(
        bucket="icechunk-test",
        prefix="gb-8mb-chunks",
        config=ic.S3Options(),
    ),
    load_variables=["array"],
    chunk_selector={},
    first_byte_variable=None,
)

GPM_IMERG_VIRTUAL = Dataset(
    bucket="earthmover-icechunk-us-west-2",
    prefix="nasa-impact/GPM_3IMERGHH.07-virtual-1998",
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
