#!/usr/bin/env python3

import math
import os
import tempfile

import numpy as np
import pytest

import icechunk as ic
import xarray as xr
import zarr
from icechunk import ManifestSplitCondition, ManifestSplitDimCondition
from icechunk.xarray import to_icechunk

SHAPE = (3, 4, 17)
CHUNKS = (1, 1, 1)
DIMS = ("time", "latitude", "longitude")


def test_manifest_splitting_appends():
    sconfig = ic.ManifestSplittingConfig.from_dict(
        {
            ManifestSplitCondition.name_matches("temperature"): {
                ManifestSplitDimCondition.DimensionName("longitude"): 3
            }
        }
    )
    config = ic.RepositoryConfig(
        inline_chunk_threshold_bytes=0, manifest=ic.ManifestConfig(splitting=sconfig)
    )
    with tempfile.TemporaryDirectory() as tmpdir:
        ### simple create repo with manifest splitting
        storage = ic.local_filesystem_storage(tmpdir)
        repo = ic.Repository.create(storage, config=config)
        assert repo.config.manifest.splitting is not None

        ds = xr.Dataset(
            {"temperature": (DIMS, np.arange(math.prod(SHAPE)).reshape(SHAPE))}
        )
        session = repo.writable_session("main")
        with zarr.config.set({"array.write_empty_chunks": True}):
            to_icechunk(
                ds, session, encoding={"temperature": {"chunks": CHUNKS}}, mode="w"
            )
        session.commit("initialize")
        roundtripped = xr.open_dataset(session.store, engine="zarr", consolidated=False)
        xr.testing.assert_identical(roundtripped, ds)

        nchunks = math.prod(SHAPE)
        nmanifests = 6
        assert len(os.listdir(f"{tmpdir}/chunks")) == nchunks
        assert len(os.listdir(f"{tmpdir}/manifests")) == nmanifests

        #### check that config is persisted and used when writing after re-open
        ### append along time - no splitting specified along this dimension
        repo = ic.Repository.open(storage)
        assert repo.config.manifest.splitting is not None
        session = repo.writable_session("main")
        with zarr.config.set({"array.write_empty_chunks": True}):
            to_icechunk(ds, session, mode="a", append_dim="time")
        session.commit("appended")
        roundtripped = xr.open_dataset(session.store, engine="zarr", consolidated=False)
        xr.testing.assert_identical(roundtripped, xr.concat([ds, ds], dim="time"))
        nchunks += math.prod(SHAPE)
        nmanifests += 6

        assert len(os.listdir(f"{tmpdir}/chunks")) == nchunks
        assert len(os.listdir(f"{tmpdir}/manifests")) == nmanifests

        #### check that config is persisted and used when writing after re-open
        ### append along longitude - splitting specified
        NEWSHAPE = (2 * SHAPE[0], *SHAPE[1:-1], 2)
        newds = xr.Dataset(
            {"temperature": (DIMS, np.arange(math.prod(NEWSHAPE)).reshape(NEWSHAPE))}
        )
        repo = ic.Repository.open(storage)
        assert repo.config.manifest.splitting is not None
        session = repo.writable_session("main")
        with zarr.config.set({"array.write_empty_chunks": True}):
            to_icechunk(newds, session, mode="a", append_dim="longitude")
        session.commit("appended")
        roundtripped = xr.open_dataset(session.store, engine="zarr", consolidated=False)
        xr.testing.assert_identical(
            roundtripped,
            xr.concat([xr.concat([ds, ds], dim="time"), newds], dim="longitude"),
        )
        nchunks += math.prod(NEWSHAPE)
        # the lon size goes from 17 -> 19 so one extra manifest,
        # compared to previous writes
        nmanifests += 7

        assert len(os.listdir(f"{tmpdir}/chunks")) == nchunks
        assert len(os.listdir(f"{tmpdir}/manifests")) == nmanifests


def test_manifest_overwrite_splitting_config_on_read():
    sconfig = ic.ManifestSplittingConfig.from_dict(
        {
            ManifestSplitCondition.name_matches("temperature"): {
                ManifestSplitDimCondition.DimensionName("longitude"): 3
            }
        }
    )

    new_sconfig = ic.ManifestSplittingConfig.from_dict(
        {
            ManifestSplitCondition.name_matches("temperature"): {
                ManifestSplitDimCondition.DimensionName("longitude"): 10
            }
        }
    )

    config = ic.RepositoryConfig(
        inline_chunk_threshold_bytes=0, manifest=ic.ManifestConfig(splitting=sconfig)
    )
    new_config = ic.RepositoryConfig(
        inline_chunk_threshold_bytes=0, manifest=ic.ManifestConfig(splitting=new_sconfig)
    )
    with tempfile.TemporaryDirectory() as tmpdir:
        ### simple create repo with manifest splitting
        storage = ic.local_filesystem_storage(tmpdir)
        repo = ic.Repository.create(storage, config=config)
        assert repo.config.manifest.splitting is not None

        ds = xr.Dataset(
            {"temperature": (DIMS, np.arange(math.prod(SHAPE)).reshape(SHAPE))}
        )
        session = repo.writable_session("main")
        with zarr.config.set({"array.write_empty_chunks": True}):
            to_icechunk(
                ds, session, encoding={"temperature": {"chunks": CHUNKS}}, mode="w"
            )
        session.commit("initialize")
        roundtripped = xr.open_dataset(session.store, engine="zarr", consolidated=False)
        xr.testing.assert_identical(roundtripped, ds)

        nchunks = math.prod(SHAPE)
        nmanifests = 6
        assert len(os.listdir(f"{tmpdir}/chunks")) == nchunks
        assert len(os.listdir(f"{tmpdir}/manifests")) == nmanifests

        #### check that config is overwritten on read
        ### append along time - no splitting specified along this dimension
        repo = ic.Repository.open(storage, config=new_config)
        assert repo.config.manifest.splitting is not None
        session = repo.writable_session("main")
        with zarr.config.set({"array.write_empty_chunks": True}):
            to_icechunk(ds, session, mode="a", append_dim="time")
        session.commit("appended")
        roundtripped = xr.open_dataset(session.store, engine="zarr", consolidated=False)
        xr.testing.assert_identical(roundtripped, xr.concat([ds, ds], dim="time"))
        nchunks = 2 * math.prod(SHAPE)
        nmanifests += 2

        assert len(os.listdir(f"{tmpdir}/chunks")) == nchunks
        assert len(os.listdir(f"{tmpdir}/manifests")) == nmanifests


def test_manifest_splitting_sparse_regions():
    sconfig = ic.ManifestSplittingConfig.from_dict(
        {
            ManifestSplitCondition.name_matches("temperature"): {
                ManifestSplitDimCondition.DimensionName("longitude"): 3
            }
        }
    )
    config = ic.RepositoryConfig(
        inline_chunk_threshold_bytes=0, manifest=ic.ManifestConfig(splitting=sconfig)
    )
    with tempfile.TemporaryDirectory() as tmpdir:
        ### simple create repo with manifest splitting
        storage = ic.local_filesystem_storage(tmpdir)
        repo = ic.Repository.create(storage, config=config)
        assert repo.config.manifest.splitting is not None

        ds = xr.Dataset(
            {"temperature": (DIMS, np.arange(math.prod(SHAPE)).reshape(SHAPE))},
            coords=dict(zip(DIMS, map(np.arange, SHAPE), strict=False)),
        )
        session = repo.writable_session("main")
        ds.chunk(-1).to_zarr(
            session.store,
            encoding={"temperature": {"chunks": CHUNKS}},
            mode="w",
            compute=False,
            consolidated=False,
        )
        session.commit("initialize")

        session = repo.writable_session("main")
        with zarr.config.set({"array.write_empty_chunks": False}):
            to_icechunk(ds.isel(longitude=slice(1, 4)), session, region="auto")
        session.commit("write region 1")
        roundtripped = xr.open_dataset(session.store, engine="zarr", consolidated=False)
        expected = xr.zeros_like(ds)
        expected.loc[{"longitude": slice(1, 3)}] = ds.isel(longitude=slice(1, 4))
        xr.testing.assert_identical(roundtripped, expected)

        nchunks = math.prod(ds.isel(longitude=slice(1, 4)).sizes.values()) + len(
            ds.coords
        )
        nmanifests = 2 + len(ds.coords)
        assert len(os.listdir(f"{tmpdir}/chunks")) == nchunks
        assert len(os.listdir(f"{tmpdir}/manifests")) == nmanifests


@pytest.mark.parametrize(
    "config, expected_shard_sizes",
    [
        (
            {
                ManifestSplitDimCondition.DimensionName("longitude"): 3,
                ManifestSplitDimCondition.Axis(1): 2,
                ManifestSplitDimCondition.Rest(): 1,
            },
            (1, 2, 3),
        ),
        (
            {
                ManifestSplitDimCondition.DimensionName("longitude"): 3,
                ManifestSplitDimCondition.Rest(): 1,
                # this next one gets overwritten by Rest
                ManifestSplitDimCondition.Axis(1): 2,
            },
            (1, 1, 3),
        ),
        (
            {
                ManifestSplitDimCondition.DimensionName("longitude"): 3,
                ManifestSplitDimCondition.DimensionName("latitude"): 3,
                # this next one gets overwritten by DimensionName above.
                ManifestSplitDimCondition.Axis(1): 13,
                ManifestSplitDimCondition.Rest(): 1,
            },
            (1, 3, 3),
        ),
    ],
)
def test_manifest_splitting_complex_config(config, expected_shard_sizes):
    sconfig = ic.ManifestSplittingConfig.from_dict(
        {ManifestSplitCondition.name_matches("temperature"): config}
    )
    config = ic.RepositoryConfig(
        inline_chunk_threshold_bytes=0, manifest=ic.ManifestConfig(splitting=sconfig)
    )
    with tempfile.TemporaryDirectory() as tmpdir:
        ### simple create repo with manifest splitting
        storage = ic.local_filesystem_storage(tmpdir)
        repo = ic.Repository.create(storage, config=config)
        assert repo.config.manifest.splitting is not None

        ds = xr.Dataset(
            {"temperature": (DIMS, np.arange(math.prod(SHAPE)).reshape(SHAPE))},
        )
        session = repo.writable_session("main")
        with zarr.config.set({"array.write_empty_chunks": True}):
            to_icechunk(
                ds, session, mode="w", encoding={"temperature": {"chunks": CHUNKS}}
            )
        session.commit("write")

        nmanifests = math.prod(
            math.ceil(nchunks / shardsize)
            for nchunks, shardsize in zip(SHAPE, expected_shard_sizes, strict=False)
        )
        assert len(os.listdir(f"{tmpdir}/manifests")) == nmanifests
