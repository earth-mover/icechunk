import contextlib
import os
import tempfile

import pytest

import zarr
from icechunk import IcechunkStore, StorageConfig
from xarray.tests.test_backends import (
    ZarrBase,
    default_zarr_version,  # noqa: F401; needed otherwise not discovered
)


@pytest.mark.skipif(
    not os.environ.get("ICECHUNK_XARRAY_BACKENDS_TESTS", None),
    reason="skipping Xarray backends tests",
)
class TestIcechunkStore(ZarrBase):
    @contextlib.contextmanager
    def create_zarr_target(self):
        if zarr.config.config["default_zarr_version"] == 2:
            pytest.skip("v2 not supported")
        with tempfile.TemporaryDirectory() as tmpdir:
            yield IcechunkStore.create(StorageConfig.filesystem(tmpdir))

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
