# Deepak runs this with
# pip install ../../xarray && ICECHUNK_XARRAY_BACKENDS_TESTS=1 pytest -s --pdb -c=../../xarray/pyproject.toml -W ignore tests/run_xarray_backends_tests.py
import contextlib
import os
import tempfile
import time
from collections.abc import Generator
from typing import Any

import numpy as np
import pytest

import xarray as xr
import zarr
from icechunk import (
    IcechunkStore,
    Repository,
    in_memory_storage,
    local_filesystem_storage,
    s3_storage,
)
from icechunk.xarray import to_icechunk
from xarray.tests.test_backends import (
    TestZarrRegionAuto as ZarrRegionAutoTests,
)
from xarray.tests.test_backends import (
    ZarrBase,
    # needed otherwise not discovered
    default_zarr_format,  # noqa: F401, RUF100
)


@pytest.mark.skipif(
    not os.environ.get("ICECHUNK_XARRAY_BACKENDS_TESTS", None),
    reason="skipping Xarray backends tests",
)
class IcechunkStoreBase(ZarrBase):
    @pytest.mark.parametrize("consolidated", [False, True, None])
    @pytest.mark.parametrize("compute", [False, True])
    @pytest.mark.parametrize("use_dask", [False, True])
    @pytest.mark.parametrize("write_empty", [False, True, None])
    def test_write_region(
        self, consolidated: Any, compute: Any, use_dask: Any, write_empty: Any
    ) -> None:
        if consolidated is not False:
            pytest.skip("consolidated not supported.")
        super().test_write_region(consolidated, compute, use_dask, write_empty)

    @pytest.mark.parametrize("consolidated", [False, True, None])
    def test_roundtrip_consolidated(self, consolidated: Any) -> None:
        if consolidated is not False:
            pytest.skip("consolidated not supported.")
        super().test_roundtrip_consolidated(consolidated)

    def test_pickle(self) -> None:
        pytest.skip(reason="pickling of icechunk stores is complicated.")

    def test_pickle_dataarray(self) -> None:
        pytest.skip(reason="pickling of icechunk stores is complicated.")


class TestIcechunkStoreFilesystem(IcechunkStoreBase):
    @contextlib.contextmanager
    def create_zarr_target(self) -> Generator[IcechunkStore]:
        if zarr.config.config["default_zarr_format"] == 2:
            pytest.skip("v2 not supported")
        with tempfile.TemporaryDirectory() as tmpdir:
            repo = Repository.create(local_filesystem_storage(tmpdir))
            session = repo.writable_session("main")
            yield session.store


class TestIcechunkStoreMemory(IcechunkStoreBase):
    @contextlib.contextmanager
    def create_zarr_target(self) -> Generator[IcechunkStore]:
        if zarr.config.config["default_zarr_format"] == 2:
            pytest.skip("v2 not supported")
        repo = Repository.create(in_memory_storage())
        session = repo.writable_session("main")
        yield session.store


class TestIcechunkStoreMinio(IcechunkStoreBase):
    @contextlib.contextmanager
    def create_zarr_target(self) -> Generator[IcechunkStore]:
        if zarr.config.config["default_zarr_format"] == 2:
            pytest.skip("v2 not supported")
        repo = Repository.create(
            s3_storage(
                endpoint_url="http://localhost:9000",
                allow_http=True,
                force_path_style=True,
                region="us-east-1",
                bucket="testbucket",
                prefix="python-xarray-test__" + str(time.time()),
                access_key_id="minio123",
                secret_access_key="minio123",
            )
        )
        session = repo.writable_session("main")
        yield session.store


@pytest.mark.filterwarnings("ignore:Failed to open:RuntimeWarning")
class TestIcechunkRegionAuto(ZarrRegionAutoTests):
    @contextlib.contextmanager
    def create_zarr_target(self) -> Generator[IcechunkStore]:
        if zarr.config.config["default_zarr_format"] == 2:
            pytest.skip("v2 not supported")
        repo = Repository.create(in_memory_storage())
        session = repo.writable_session("main")
        yield session.store

    @contextlib.contextmanager
    def create(self):
        x = np.arange(0, 50, 10)
        y = np.arange(0, 20, 2)
        data = np.ones((5, 10))
        ds = xr.Dataset(
            {"test": xr.DataArray(data, dims=("x", "y"), coords={"x": x, "y": y})}
        )
        repo = Repository.create(in_memory_storage())
        session = repo.writable_session("main")
        self.save(session.store, ds)
        session.commit("initial commit")
        yield repo.writable_session("main").store, ds

    def save(self, target, ds, **kwargs):
        # not really important here
        kwargs.pop("compute", None)
        to_icechunk(ds, session=target.session, **kwargs)

    def test_zarr_region_index_write(self, tmp_path):
        pytest.skip("runs multiple saves, doesn't work")

    def test_zarr_region(self):
        """duplicated because it uses multiple saves"""
        pytest.skip("runs multiple saves, doesn't work")
