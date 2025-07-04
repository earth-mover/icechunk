# Deepak runs this with
# pip install ../../xarray && ICECHUNK_XARRAY_BACKENDS_TESTS=1 pytest -s --pdb -c=../../xarray/pyproject.toml -W ignore tests/run_xarray_backends_tests.py
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
from icechunk.xarray import to_icechunk

# needed otherwise not discovered
from xarray.tests.conftest import time_unit  # noqa: F401, RUF100
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

    def test_append_string_length_mismatch_works(self, *args, **kwargs):
        pytest.skip("test will be deleted soon")


class TestIcechunkStoreFilesystem(IcechunkStoreBase):
    @contextlib.contextmanager
    def create_zarr_target(self) -> Generator[IcechunkStore]:
        if zarr.config.config["default_zarr_format"] == 2:
            pytest.skip("v2 not supported")
        with tempfile.TemporaryDirectory() as tmpdir:
            repo = Repository.create(local_filesystem_storage(tmpdir))
            session = repo.writable_session("main")
            with session.allow_pickling():
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
                force_path_style=True,
                region="us-east-1",
                bucket="testbucket",
                prefix="python-xarray-test__" + str(time.time()),
                access_key_id="minio123",
                secret_access_key="minio123",
            )
        )
        session = repo.writable_session("main")
        with session.allow_pickling():
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

    def save(self, target, ds, **kwargs):
        # not really important here
        kwargs.pop("compute", None)
        to_icechunk(ds, session=target.session, **kwargs)
