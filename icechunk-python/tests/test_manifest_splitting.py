#!/usr/bin/env python3

import math
import os
import tempfile

import numpy as np
import pytest
from hypothesis import given
from hypothesis import strategies as st

import icechunk as ic
import xarray as xr
import zarr
from icechunk import ManifestSplitCondition, ManifestSplitDimCondition
from icechunk.testing.strategies import splitting_configs
from icechunk.xarray import to_icechunk
from zarr.testing.strategies import arrays as zarr_arrays

SHAPE = (3, 4, 17)
CHUNKS = (1, 1, 1)
DIMS = ("time", "latitude", "longitude")


@given(data=st.data())
def test_splitting_config_dict_roundtrip(data) -> None:
    arrays = data.draw(
        st.lists(
            zarr_arrays(compressors=st.none(), attrs=st.none(), zarr_formats=st.just(3))
        )
    )
    config = data.draw(splitting_configs(arrays=arrays))
    assert ic.ManifestSplittingConfig.from_dict(config.to_dict()) == config


def test_manifest_splitting_appends() -> None:
    array_condition = ManifestSplitCondition.or_conditions(
        [
            ManifestSplitCondition.name_matches("temperature"),
            ManifestSplitCondition.name_matches("salinity"),
        ]
    )
    sconfig = ic.ManifestSplittingConfig.from_dict(
        {array_condition: {ManifestSplitDimCondition.DimensionName("longitude"): 3}}
    )
    config = ic.RepositoryConfig(
        inline_chunk_threshold_bytes=0, manifest=ic.ManifestConfig(splitting=sconfig)
    )
    with tempfile.TemporaryDirectory() as tmpdir:
        ### simple create repo with manifest splitting
        storage = ic.local_filesystem_storage(tmpdir)
        repo = ic.Repository.create(storage, config=config)
        assert repo.config.manifest
        assert repo.config.manifest.splitting is not None

        ds = xr.Dataset(
            {
                "temperature": (DIMS, np.arange(math.prod(SHAPE)).reshape(SHAPE)),
                "salinity": (DIMS, np.arange(math.prod(SHAPE)).reshape(SHAPE)),
            }
        )
        session = repo.writable_session("main")
        with zarr.config.set({"array.write_empty_chunks": True}):
            to_icechunk(
                ds,
                session,
                encoding={
                    "temperature": {"chunks": CHUNKS},
                    "salinity": {"chunks": CHUNKS},
                },
                mode="w",
            )
        session.commit("initialize")
        roundtripped = xr.open_dataset(session.store, engine="zarr", consolidated=False)
        xr.testing.assert_identical(roundtripped, ds)

        nchunks = math.prod(SHAPE) * 2
        nmanifests = 6 * 2
        assert len(os.listdir(f"{tmpdir}/chunks")) == nchunks
        assert len(os.listdir(f"{tmpdir}/manifests")) == nmanifests

        #### check that config is persisted and used when writing after re-open
        ### append along time - no splitting specified along this dimension
        repo = ic.Repository.open(storage)
        assert repo.config.manifest
        assert repo.config.manifest.splitting is not None
        session = repo.writable_session("main")
        with zarr.config.set({"array.write_empty_chunks": True}):
            to_icechunk(ds, session, mode="a", append_dim="time")
        session.commit("appended")
        roundtripped = xr.open_dataset(session.store, engine="zarr", consolidated=False)
        xr.testing.assert_identical(roundtripped, xr.concat([ds, ds], dim="time"))
        nchunks += math.prod(SHAPE) * 2
        nmanifests += 6 * 2

        assert len(os.listdir(f"{tmpdir}/chunks")) == nchunks
        assert len(os.listdir(f"{tmpdir}/manifests")) == nmanifests

        #### check that config is persisted and used when writing after re-open
        ### append along longitude - splitting specified
        NEWSHAPE = (2 * SHAPE[0], *SHAPE[1:-1], 2)
        newds = xr.Dataset(
            {
                "temperature": (DIMS, np.arange(math.prod(NEWSHAPE)).reshape(NEWSHAPE)),
                "salinity": (DIMS, np.arange(math.prod(NEWSHAPE)).reshape(NEWSHAPE)),
            }
        )
        repo = ic.Repository.open(storage)
        assert repo.config.manifest
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
        nchunks += math.prod(NEWSHAPE) * 2
        # the lon size goes from 17 -> 19 so one extra manifest,
        # compared to previous writes
        nmanifests += 2 * 2

        assert len(os.listdir(f"{tmpdir}/chunks")) == nchunks
        assert len(os.listdir(f"{tmpdir}/manifests")) == nmanifests


def test_manifest_overwrite_splitting_config_on_read() -> None:
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


def test_manifest_splitting_sparse_regions() -> None:
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
        assert repo.config.manifest
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
    "config, expected_split_sizes",
    [
        (
            {
                ManifestSplitDimCondition.DimensionName("longitude"): 3,
                ManifestSplitDimCondition.Axis(1): 2,
                ManifestSplitDimCondition.Any(): 1,
            },
            (1, 2, 3),
        ),
        (
            {
                ManifestSplitDimCondition.DimensionName("longitude"): 3,
                ManifestSplitDimCondition.Any(): 1,
                # this next one gets overwritten by Any
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
                ManifestSplitDimCondition.Any(): 1,
            },
            (1, 3, 3),
        ),
    ],
)
def test_manifest_splitting_complex_config(config, expected_split_sizes) -> None:
    sconfig = ic.ManifestSplittingConfig.from_dict(
        {ManifestSplitCondition.AnyArray(): config}
    )
    config = ic.RepositoryConfig(
        inline_chunk_threshold_bytes=0, manifest=ic.ManifestConfig(splitting=sconfig)
    )
    with tempfile.TemporaryDirectory() as tmpdir:
        ### simple create repo with manifest splitting
        storage = ic.local_filesystem_storage(tmpdir)
        repo = ic.Repository.create(storage, config=config)
        assert repo.config.manifest
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
            math.ceil(nchunks / splitsize)
            for nchunks, splitsize in zip(SHAPE, expected_split_sizes, strict=False)
        )
        assert len(os.listdir(f"{tmpdir}/manifests")) == nmanifests


def test_manifest_splitting_magic_methods() -> None:
    assert ManifestSplitCondition.and_conditions(
        [
            ManifestSplitCondition.name_matches("temperature"),
            ManifestSplitCondition.name_matches("salinity"),
        ]
    ) == ManifestSplitCondition.name_matches(
        "temperature"
    ) & ManifestSplitCondition.name_matches("salinity")
    assert ManifestSplitCondition.or_conditions(
        [
            ManifestSplitCondition.name_matches("temperature"),
            ManifestSplitCondition.name_matches("salinity"),
        ]
    ) == ManifestSplitCondition.name_matches(
        "temperature"
    ) | ManifestSplitCondition.name_matches("salinity")
