import contextlib
import os
import tempfile
import time
from collections.abc import Generator
from typing import Any

import pytest

import zarr
from icechunk import (
    IcechunkStore,
    Repository,
    in_memory_storage,
    local_filesystem_storage,
    s3_storage,
)
from xarray.tests.test_backends import (
    ZarrBase,
    default_zarr_format,  # noqa: F401; needed otherwise not discovered
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

    def test_pickle(self) -> None:
        pytest.skip(reason="memory icechunk stores cannot be pickled.")

    def test_pickle_dataarray(self) -> None:
        pytest.skip(reason="memory icechunk stores cannot be pickled.")


class TestIcechunkStoreMinio(IcechunkStoreBase):
    @contextlib.contextmanager
    def create_zarr_target(self) -> Generator[IcechunkStore]:
        if zarr.config.config["default_zarr_format"] == 2:
            pytest.skip("v2 not supported")
        repo = Repository.create(
            s3_storage(
                endpoint_url="http://localhost:9000",
                allow_http=True,
                region="us-east-1",
                bucket="testbucket",
                prefix="python-xarray-test__" + str(time.time()),
                access_key_id="minio123",
                secret_access_key="minio123",
            )
        )
        session = repo.writable_session("main")
        yield session.store
