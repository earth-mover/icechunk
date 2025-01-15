#!/usr/bin/env python3
import datetime
import time

import numpy as np
import tqdm
from lib import ERA5_SINGLE, GB_8MB_CHUNKS, GB_128MB_CHUNKS

import xarray as xr
import zarr

rng = np.random.default_rng(seed=123)


def setup_synthetic_gb_dataset():
    shape = (512, 512, 512)
    for dataset, chunksize in tqdm.tqdm(
        (
            (GB_128MB_CHUNKS, (64, 512, 512)),
            (GB_8MB_CHUNKS, (4, 512, 512)),
        )
    ):
        repo = dataset.create()
        session = repo.writable_session("main")
        store = session.store
        group = zarr.group(store)
        array = group.create_array(
            name="array",
            shape=shape,
            chunks=chunksize,
            dtype=np.int64,
            dimension_names=("t", "y", "x"),
        )
        array[:] = rng.integers(-1000, high=1000, size=shape, dtype=np.int64)
        session.commit("initialized")


def setup_era5_single():
    import pooch

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

    repo = ERA5_SINGLE.create()
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


# FIXME: Take a prefix
if __name__ == "__main__":
    setup_era5_single()
    setup_synthetic_gb_dataset()
