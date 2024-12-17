import contextlib
import os
import tempfile
import time

import pytest

import zarr
from icechunk import S3Credentials, StorageConfig
from icechunk.repository import Repository
from xarray.tests.test_backends import (
    ZarrBase,
    default_zarr_version,  # noqa: F401; needed otherwise not discovered
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
    def test_write_region(self, consolidated, compute, use_dask, write_empty) -> None:
        if consolidated is not False:
            pytest.skip("consolidated not supported.")
        super().test_write_region(consolidated, compute, use_dask, write_empty)

    @pytest.mark.parametrize("consolidated", [False, True, None])
    def test_roundtrip_consolidated(self, consolidated) -> None:
        if consolidated is not False:
            pytest.skip("consolidated not supported.")
        super().test_roundtrip_consolidated(consolidated)


class TestIcechunkStoreFilesystem(IcechunkStoreBase):
    @contextlib.contextmanager
    def create_zarr_target(self):
        if zarr.config.config["default_zarr_version"] == 2:
            pytest.skip("v2 not supported")
        with tempfile.TemporaryDirectory() as tmpdir:
            repo = Repository.create(StorageConfig.filesystem(tmpdir))
            session = repo.writable_session("main")
            yield session.store()


class TestIcechunkStoreMemory(IcechunkStoreBase):
    @contextlib.contextmanager
    def create_zarr_target(self):
        if zarr.config.config["default_zarr_version"] == 2:
            pytest.skip("v2 not supported")
        repo = Repository.create(StorageConfig.memory(""))
        session = repo.writable_session("main")
        yield session.store()

    def test_pickle(self):
        pytest.skip(reason="memory icechunk stores cannot be pickled.")

    def test_pickle_dataarray(self):
        pytest.skip(reason="memory icechunk stores cannot be pickled.")


class TestIcechunkStoreMinio(IcechunkStoreBase):
    @contextlib.contextmanager
    def create_zarr_target(self):
        if zarr.config.config["default_zarr_version"] == 2:
            pytest.skip("v2 not supported")
        storage_config = {
            "bucket": "testbucket",
            "prefix": "python-xarray-test__" + str(time.time()),
            "endpoint_url": "http://localhost:9000",
            "region": "us-east-1",
            "allow_http": True,
        }
        storage_config = StorageConfig.s3_from_config(
            **storage_config,
            credentials=S3Credentials(
                access_key_id="minio123", secret_access_key="minio123"
            ),
        )
        repo = Repository.create(storage=storage_config)
        session = repo.writable_session("main")
        yield session.store()
