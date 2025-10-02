# Deepak runs this with
# pip install ../../xarray && ICECHUNK_XARRAY_BACKENDS_TESTS=1 pytest -s --pdb -c=../../xarray/pyproject.toml -W ignore tests/run_xarray_backends_tests.py
import contextlib
import os
import pickle
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
    local_filesystem_storage,
    s3_storage,
)
from icechunk.xarray import is_dask_collection, to_icechunk
from xarray.testing import assert_identical

# needed otherwise not discovered
from xarray.tests.conftest import time_unit  # noqa: F401, RUF100
from xarray.tests.test_backends import (
    TestZarrRegionAuto as ZarrRegionAutoTests,
)
from xarray.tests.test_backends import (
    ZarrBase,
    # needed otherwise not discovered
    default_zarr_format,  # noqa: F401, RUF100
    tmp_store,  # noqa: F401, RUF100
)


@pytest.mark.skipif(
    not os.environ.get("ICECHUNK_XARRAY_BACKENDS_TESTS", None),
    reason="skipping Xarray backends tests",
)
class IcechunkStoreBase(ZarrBase):
    @contextlib.contextmanager
    def create_repo(self) -> Generator[Repository]:
        raise NotImplementedError

    @contextlib.contextmanager
    def create_zarr_target(self) -> Generator[IcechunkStore]:
        if zarr.config.config["default_zarr_format"] == 2:
            pytest.skip("v2 not supported")
        with self.create_repo() as repo:
            session = repo.writable_session("main")
            yield session.store

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

    @contextlib.contextmanager
    def roundtrip_with_commit(self, ds: xr.Dataset) -> Generator[xr.Dataset]:
        with self.create_repo() as repo:
            session = repo.writable_session("main")
            to_icechunk(ds, session)
            session.commit("initial commit")
            yield xr.open_zarr(session.store)

    def test_pickle(self) -> None:
        expected = xr.Dataset({"foo": ("x", [42])})
        with self.roundtrip_with_commit(expected) as roundtripped:
            raw_pickle = pickle.dumps(roundtripped)
            with pickle.loads(raw_pickle) as unpickled_ds:
                assert_identical(expected, unpickled_ds)

    @pytest.mark.filterwarnings("ignore:deallocating CachingFileManager")
    def test_pickle_dataarray(self) -> None:
        expected = xr.Dataset({"foo": ("x", [42])})
        with self.roundtrip_with_commit(expected) as roundtripped:
            raw_pickle = pickle.dumps(roundtripped["foo"])
            # TODO: figure out how to explicitly close the file for the
            # unpickled DataArray?
            unpickled = pickle.loads(raw_pickle)
            assert_identical(expected["foo"], unpickled)


class TestIcechunkStoreFilesystem(IcechunkStoreBase):
    @contextlib.contextmanager
    def create_repo(self) -> Generator[Repository]:
        with tempfile.TemporaryDirectory() as tmpdir:
            yield Repository.create(local_filesystem_storage(tmpdir))


class TestIcechunkStoreMemory(IcechunkStoreBase):
    @contextlib.contextmanager
    def create_repo(self) -> Generator[Repository]:
        with tempfile.TemporaryDirectory() as tmpdir:
            yield Repository.create(local_filesystem_storage(tmpdir))

    def test_pickle(self) -> None:
        pytest.skip("pickling memory store is not supported")

    def test_pickle_dataarray(self) -> None:
        pytest.skip("pickling memory store is not supported")


class TestIcechunkStoreMinio(IcechunkStoreBase):
    @contextlib.contextmanager
    def create_repo(self) -> Generator[Repository]:
        yield Repository.create(
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


@pytest.mark.filterwarnings("ignore:Failed to open:RuntimeWarning")
class TestIcechunkRegionAuto(ZarrRegionAutoTests):
    @contextlib.contextmanager
    def create_zarr_target(self) -> Generator[IcechunkStore]:
        if zarr.config.config["default_zarr_format"] == 2:
            pytest.skip("v2 not supported")

        with tempfile.TemporaryDirectory() as tmpdir:
            storage = local_filesystem_storage(tmpdir)
            self._repo = Repository.create(storage)
            session = self._repo.writable_session("main")
            yield session.store

    @contextlib.contextmanager
    def create(self):
        x = np.arange(0, 50, 10)
        y = np.arange(0, 20, 2)
        data = np.ones((5, 10))
        ds = xr.Dataset(
            {"test": xr.DataArray(data, dims=("x", "y"), coords={"x": x, "y": y})}
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            storage = local_filesystem_storage(tmpdir)
            self._repo = Repository.create(storage)
            session = self._repo.writable_session("main")
            self.save(session.store, ds)
            session.commit("initial commit")
            yield self._repo.writable_session("main").store, ds

    def save(self, target, ds, **kwargs):
        # not really important here
        kwargs.pop("compute", None)

        # Check if we have dask arrays
        has_dask = any(is_dask_collection(var.data) for var in ds.variables.values())

        # Special handling for dask arrays to support multiple writes to the same store.
        #
        # Context: Some xarray tests (e.g., test_dataset_to_zarr_align_chunks_true) call
        # save() multiple times on the same store object to test append/region writes.
        #
        # Background: to_icechunk() handles dask vs non-dask arrays differently:
        #   - Non-dask: Writes directly to the session (allows uncommitted changes)
        #   - Dask: Uses fork/merge for parallel workers, which:
        #       1. session.fork() creates a child session
        #       2. Workers write chunks, changes are merged back to parent session
        #       3. After merge, the parent session has uncommitted changes
        #
        # Problem: to_icechunk() only allows dask writes on clean sessions (safety check
        # to avoid consistency issues during fork/merge). A second dask write fails with:
        #   "Calling `to_icechunk` is not allowed on a Session with uncommitted changes"
        #
        # Solution: Before each dask write, if there are uncommitted changes from a previous
        # write, commit them and refresh the session for the next fork/merge cycle.
        if has_dask and hasattr(self, "_repo"):
            session = target.session
            if session.has_uncommitted_changes:
                session.commit("intermediate commit")
                # After commit, session is read-only. Get a fresh writable session and
                # update the store so subsequent operations see the committed data.
                new_store = self._repo.writable_session("main").store
                target._store = new_store._store

        to_icechunk(ds, session=target.session, **kwargs)

    def test_zarr_append_chunk_partial(self):
        pytest.skip(
            "this test requires multiple saves, and is meant to exercise Xarray logic."
        )

    def test_zarr_safe_chunk_region(self, *args, **kwargs):
        pytest.skip(
            "this test requires multiple saves, and is meant to exercise Xarray logic."
        )

    def test_zarr_safe_chunk_append_dim(self, *args, **kwargs):
        pytest.skip(
            "this test requires multiple saves, and is meant to exercise Xarray logic."
        )

    def test_zarr_region_chunk_partial_offset(self):
        pytest.skip(
            "this test requires multiple saves, and is meant to exercise Xarray logic."
        )
