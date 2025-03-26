#!/usr/bin/env python3

import math
import os
import tempfile

import numpy as np

import icechunk as ic
import xarray as xr
import zarr
from icechunk import ManifestShardCondition, ShardDimCondition
from icechunk.xarray import to_icechunk

SHAPE = (3, 4, 17)
CHUNKS = (1, 1, 1)
DIMS = ("time", "latitude", "longitude")


def test_manifest_sharding():
    # sconfig = ic.ManifestShardingConfig(
    #     [
    #         (
    #             ManifestShardCondition.name_matches("temperature"),
    #             [(ShardDimCondition.DimensionName("longitude"), 3)],
    #         )
    #     ]
    # )
    sconfig = ic.ManifestShardingConfig.from_dict(
        {
            ManifestShardCondition.name_matches("temperature"): {
                ShardDimCondition.DimensionName("longitude"): 3
            }
        }
    )
    config = ic.RepositoryConfig(
        inline_chunk_threshold_bytes=0, manifest=ic.ManifestConfig(sharding=sconfig)
    )
    with tempfile.TemporaryDirectory() as tmpdir:
        repo = ic.Repository.create(ic.local_filesystem_storage(tmpdir), config=config)
        assert repo.config.manifest.sharding is not None
        ds = xr.Dataset(
            {"temperature": (DIMS, np.arange(math.prod(SHAPE)).reshape(SHAPE))}
        )
        session = repo.writable_session("main")
        with zarr.config.set({"array.write_empty_chunks": True}):
            to_icechunk(
                ds, session, encoding={"temperature": {"chunks": CHUNKS}}, mode="w"
            )
        session.commit("initialize")

        assert len(os.listdir(f"{tmpdir}/chunks")) == math.prod(SHAPE)
        assert len(os.listdir(f"{tmpdir}/manifests")) == 6
