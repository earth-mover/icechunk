from collections.abc import Generator

import pytest

pytest.importorskip("xarray")

import contextlib
import string
import tempfile

import numpy as np
import pandas as pd

import xarray as xr
from icechunk import Repository, in_memory_storage, local_filesystem_storage
from icechunk.xarray import to_icechunk
from xarray.testing import assert_identical


def create_test_data(
    seed: int | None = None,
    add_attrs: bool = True,
    dim_sizes: tuple[int, int, int] = (8, 9, 10),
) -> xr.Dataset:
    rs = np.random.RandomState(seed)
    _vars = {
        "var1": ["dim1", "dim2"],
        "var2": ["dim1", "dim2"],
        "var3": ["dim3", "dim1"],
    }
    _dims = {"dim1": dim_sizes[0], "dim2": dim_sizes[1], "dim3": dim_sizes[2]}

    obj = xr.Dataset()
    obj["dim2"] = ("dim2", 0.5 * np.arange(_dims["dim2"]))
    if _dims["dim3"] > 26:
        raise RuntimeError(
            f"Not enough letters for filling this dimension size ({_dims['dim3']})"
        )
    obj["dim3"] = ("dim3", list(string.ascii_lowercase[0 : _dims["dim3"]]))
    obj["time"] = ("time", pd.date_range("2000-01-01", periods=20))
    for v, dims in sorted(_vars.items()):
        data = rs.normal(size=tuple(_dims[d] for d in dims))
        obj[v] = (dims, data)
        if add_attrs:
            obj[v].attrs = {"foo": "variable"}
    numbers_values = rs.randint(0, 3, _dims["dim3"], dtype="int64")
    obj.coords["numbers"] = ("dim3", numbers_values)
    obj.encoding = {"foo": "bar"}
    return obj


@contextlib.contextmanager
def roundtrip(
    data: xr.Dataset, *, commit: bool = False
) -> Generator[xr.Dataset, None, None]:
    with tempfile.TemporaryDirectory() as tmpdir:
        repo = Repository.create(local_filesystem_storage(tmpdir))
        session = repo.writable_session("main")
        to_icechunk(data, session=session, mode="w")
        session.commit("write")
        with xr.open_zarr(session.store, consolidated=False) as ds:
            yield ds


def test_xarray_to_icechunk() -> None:
    ds = create_test_data()
    with roundtrip(ds) as actual:
        assert_identical(actual, ds)


def test_repeated_to_icechunk_serial() -> None:
    ds = create_test_data()
    repo = Repository.create(in_memory_storage())
    session = repo.writable_session("main")
    to_icechunk(ds, session)
    to_icechunk(ds, session, mode="w")
