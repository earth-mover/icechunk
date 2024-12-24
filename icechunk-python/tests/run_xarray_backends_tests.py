import contextlib
import os
import tempfile
import time

import pytest

import zarr
from icechunk import (
<<<<<<< HEAD
    S3Credentials,
    S3Options,
    S3StaticCredentials,
=======
    Credentials,
    ObjectStoreConfig,
    S3CompatibleOptions,
    S3Credentials,
>>>>>>> 801dbdd (Rename static credentials to s3 credentials)
    Storage,
)
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
            repo = Repository.create(Storage.local_filesystem(tmpdir))
            session = repo.writable_session("main")
            yield session.store


class TestIcechunkStoreMemory(IcechunkStoreBase):
    @contextlib.contextmanager
    def create_zarr_target(self):
        if zarr.config.config["default_zarr_version"] == 2:
            pytest.skip("v2 not supported")
        repo = Repository.create(Storage.in_memory())
        session = repo.writable_session("main")
        yield session.store

    def test_pickle(self):
        pytest.skip(reason="memory icechunk stores cannot be pickled.")

    def test_pickle_dataarray(self):
        pytest.skip(reason="memory icechunk stores cannot be pickled.")


class TestIcechunkStoreMinio(IcechunkStoreBase):
    @contextlib.contextmanager
    def create_zarr_target(self):
        if zarr.config.config["default_zarr_version"] == 2:
            pytest.skip("v2 not supported")
        opts = S3Options(
            endpoint_url="http://localhost:9000", allow_http=True, region="us-east-1"
        )
<<<<<<< HEAD
        credentials = S3Credentials.Static(
            S3StaticCredentials(access_key_id="minio123", secret_access_key="minio123")
=======
        credentials = Credentials.S3(
            S3Credentials(access_key_id="minio123", secret_access_key="minio123")
>>>>>>> 801dbdd (Rename static credentials to s3 credentials)
        )
        repo = Repository.create(
            Storage.s3(
                opts,
                bucket="testbucket",
                prefix="python-xarray-test__" + str(time.time()),
                credentials=credentials,
            )
        )
        session = repo.writable_session("main")
        yield session.store
