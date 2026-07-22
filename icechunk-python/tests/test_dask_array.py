import pytest

pytest.importorskip("dask")
# xarray must be imported first: dask_array only pins its "dask" chunkmanager
# eagerly when xarray is already in sys.modules; otherwise the first chunked
# op races xarray's entry-point discovery and may get the builtin DaskManager.
pytest.importorskip("xarray")
dask_array = pytest.importorskip("dask_array")

from tests.test_xarray import create_test_data, roundtrip
from xarray.namedarray.parallelcompat import guess_chunkmanager
from xarray.testing import assert_identical


def test_chunkmanager_is_replaced() -> None:
    # dask-array's "dask" entry point wins over xarray's builtin DaskManager,
    # so .chunk() produces dask_array.Array collections; the tests below rely on it.
    manager = guess_chunkmanager("dask")
    assert type(manager).__module__.startswith("dask_array")


def test_to_icechunk_roundtrip(any_spec_version: int | None) -> None:
    ds = create_test_data().chunk(dim1=3, dim2=4)
    assert isinstance(ds["var1"].data, dask_array.Array)
    with roundtrip(ds, commit=True, spec_version=any_spec_version) as actual:
        assert_identical(actual, ds)
