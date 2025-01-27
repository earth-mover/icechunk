#### Delete this when Xarray 2025.02.0 is out
import contextlib
from collections.abc import Mapping
from typing import Literal
from unittest.mock import patch

import numpy as np
import pytest

import xarray as xr
from xarray.backends.zarr import ZarrStore
from xarray.testing import assert_identical


class ZarrRegionAutoTests:
    """These are separated out since we should not need to test this logic with every store."""

    @contextlib.contextmanager
    def create(self):
        x = np.arange(0, 50, 10)
        y = np.arange(0, 20, 2)
        data = np.ones((5, 10))
        ds = xr.Dataset(
            {"test": xr.DataArray(data, dims=("x", "y"), coords={"x": x, "y": y})}
        )
        with self.create_zarr_target() as target:
            self.save(target, ds)
            yield target, ds

    def save(self, target, ds, **kwargs):
        ds.to_zarr(target, **kwargs)

    @pytest.mark.parametrize(
        "region",
        [
            pytest.param("auto", id="full-auto"),
            pytest.param({"x": "auto", "y": slice(6, 8)}, id="mixed-auto"),
        ],
    )
    def test_zarr_region_auto(self, region):
        with self.create() as (target, ds):
            ds_region = 1 + ds.isel(x=slice(2, 4), y=slice(6, 8))
            self.save(target, ds_region, region=region)
            ds_updated = xr.open_zarr(target)

            expected = ds.copy()
            expected["test"][2:4, 6:8] += 1
            assert_identical(ds_updated, expected)

    def test_zarr_region_auto_noncontiguous(self):
        with self.create() as (target, ds):
            with pytest.raises(ValueError):
                self.save(target, ds.isel(x=[0, 2, 3], y=[5, 6]), region="auto")

            dsnew = ds.copy()
            dsnew["x"] = dsnew.x + 5
            with pytest.raises(KeyError):
                self.save(target, dsnew, region="auto")

    def test_zarr_region_index_write(self, tmp_path):
        region: Mapping[str, slice] | Literal["auto"]
        region_slice = dict(x=slice(2, 4), y=slice(6, 8))

        with self.create() as (target, ds):
            ds_region = 1 + ds.isel(region_slice)
            for region in [region_slice, "auto"]:  # type: ignore[assignment]
                with patch.object(
                    ZarrStore,
                    "set_variables",
                    side_effect=ZarrStore.set_variables,
                    autospec=True,
                ) as mock:
                    self.save(target, ds_region, region=region, mode="r+")

                    # should write the data vars but never the index vars with auto mode
                    for call in mock.call_args_list:
                        written_variables = call.args[1].keys()
                        assert "test" in written_variables
                        assert "x" not in written_variables
                        assert "y" not in written_variables

    def test_zarr_region_append(self):
        with self.create() as (target, ds):
            x_new = np.arange(40, 70, 10)
            data_new = np.ones((3, 10))
            ds_new = xr.Dataset(
                {
                    "test": xr.DataArray(
                        data_new,
                        dims=("x", "y"),
                        coords={"x": x_new, "y": ds.y},
                    )
                }
            )

            # Now it is valid to use auto region detection with the append mode,
            # but it is still unsafe to modify dimensions or metadata using the region
            # parameter.
            with pytest.raises(KeyError):
                self.save(target, ds_new, mode="a", append_dim="x", region="auto")

    def test_zarr_region(self):
        with self.create() as (target, ds):
            ds_transposed = ds.transpose("y", "x")
            ds_region = 1 + ds_transposed.isel(x=[0], y=[0])
            self.save(target, ds_region, region={"x": slice(0, 1), "y": slice(0, 1)})

            # Write without region
            self.save(target, ds_transposed, mode="r+")

    def test_zarr_region_chunk_partial(self):
        """
        Check that writing to partial chunks with `region` fails, assuming `safe_chunks=False`.
        """
        ds = (
            xr.DataArray(np.arange(120).reshape(4, 3, -1), dims=list("abc"))
            .rename("var1")
            .to_dataset()
        )

        with self.create_zarr_target() as target:
            self.save(target, ds.chunk(5), compute=False, mode="w")
            with pytest.raises(ValueError):
                for r in range(ds.sizes["a"]):
                    self.save(
                        target, ds.chunk(3).isel(a=[r]), region=dict(a=slice(r, r + 1))
                    )

    def test_zarr_append_chunk_partial(self):
        t_coords = np.array([np.datetime64("2020-01-01").astype("datetime64[ns]")])
        data = np.ones((10, 10))

        da = xr.DataArray(
            data.reshape((-1, 10, 10)),
            dims=["time", "x", "y"],
            coords={"time": t_coords},
            name="foo",
        )
        new_time = np.array([np.datetime64("2021-01-01").astype("datetime64[ns]")])
        da2 = xr.DataArray(
            data.reshape((-1, 10, 10)),
            dims=["time", "x", "y"],
            coords={"time": new_time},
            name="foo",
        )

        with self.create_zarr_target() as target:
            self.save(target, da, mode="w", encoding={"foo": {"chunks": (5, 5, 1)}})

            with pytest.raises(ValueError, match="encoding was provided"):
                self.save(
                    target,
                    da2,
                    append_dim="time",
                    mode="a",
                    encoding={"foo": {"chunks": (1, 1, 1)}},
                )

            # chunking with dask sidesteps the encoding check, so we need a different check
            with pytest.raises(ValueError, match="Specified zarr chunks"):
                self.save(
                    target,
                    da2.chunk({"x": 1, "y": 1, "time": 1}),
                    append_dim="time",
                    mode="a",
                )

    def test_zarr_region_chunk_partial_offset(self):
        # https://github.com/pydata/xarray/pull/8459#issuecomment-1819417545
        with self.create_zarr_target() as store:
            data = np.ones((30,))
            da = xr.DataArray(
                data, dims=["x"], coords={"x": range(30)}, name="foo"
            ).chunk(x=10)
            self.save(store, da, compute=False)

            self.save(store, da.isel(x=slice(10)).chunk(x=(10,)), region="auto")

            self.save(
                store,
                da.isel(x=slice(5, 25)).chunk(x=(10, 10)),
                safe_chunks=False,
                region="auto",
            )

            with pytest.raises(ValueError):
                self.save(store, da.isel(x=slice(5, 25)).chunk(x=(10, 10)), region="auto")

    def test_zarr_safe_chunk_append_dim(self):
        with self.create_zarr_target() as store:
            data = np.ones((20,))
            da = xr.DataArray(
                data, dims=["x"], coords={"x": range(20)}, name="foo"
            ).chunk(x=5)

            self.save(store, da.isel(x=slice(0, 7)), safe_chunks=True, mode="w")
            with pytest.raises(ValueError):
                # If the first chunk is smaller than the border size then raise an error
                self.save(
                    store,
                    da.isel(x=slice(7, 11)).chunk(x=(2, 2)),
                    append_dim="x",
                    safe_chunks=True,
                )

            self.save(store, da.isel(x=slice(0, 7)), safe_chunks=True, mode="w")
            # If the first chunk is of the size of the border size then it is valid
            self.save(
                store,
                da.isel(x=slice(7, 11)).chunk(x=(3, 1)),
                safe_chunks=True,
                append_dim="x",
            )
            assert xr.open_zarr(store)["foo"].equals(da.isel(x=slice(0, 11)))

            self.save(store, da.isel(x=slice(0, 7)), safe_chunks=True, mode="w")
            # If the first chunk is of the size of the border size + N * zchunk then it is valid
            self.save(
                store,
                da.isel(x=slice(7, 17)).chunk(x=(8, 2)),
                safe_chunks=True,
                append_dim="x",
            )
            assert xr.open_zarr(store)["foo"].equals(da.isel(x=slice(0, 17)))

            self.save(store, da.isel(x=slice(0, 7)), safe_chunks=True, mode="w")
            with pytest.raises(ValueError):
                # If the first chunk is valid but the other are not then raise an error
                self.save(
                    store,
                    da.isel(x=slice(7, 14)).chunk(x=(3, 3, 1)),
                    append_dim="x",
                    safe_chunks=True,
                )

            self.save(store, da.isel(x=slice(0, 7)), safe_chunks=True, mode="w")
            with pytest.raises(ValueError):
                # If the first chunk have a size bigger than the border size but not enough
                # to complete the size of the next chunk then an error must be raised
                self.save(
                    store,
                    da.isel(x=slice(7, 14)).chunk(x=(4, 3)),
                    append_dim="x",
                    safe_chunks=True,
                )

            self.save(store, da.isel(x=slice(0, 7)), safe_chunks=True, mode="w")
            # Append with a single chunk it's totally valid,
            # and it does not matter the size of the chunk
            self.save(
                store,
                da.isel(x=slice(7, 19)).chunk(x=-1),
                append_dim="x",
                safe_chunks=True,
            )
            assert xr.open_zarr(store)["foo"].equals(da.isel(x=slice(0, 19)))

    @pytest.mark.parametrize("mode", ["r+", "a"])
    def test_zarr_safe_chunk_region(self, mode: Literal["r+", "a"]):
        with self.create_zarr_target() as store:
            arr = xr.DataArray(
                list(range(11)), dims=["a"], coords={"a": list(range(11))}, name="foo"
            ).chunk(a=3)
            self.save(store, arr, mode="w")

            with pytest.raises(ValueError):
                # There are two Dask chunks on the same Zarr chunk,
                # which means that it is unsafe in any mode
                self.save(
                    store,
                    arr.isel(a=slice(0, 3)).chunk(a=(2, 1)),
                    region="auto",
                    mode=mode,
                )

            with pytest.raises(ValueError):
                # the first chunk is covering the border size, but it is not
                # completely covering the second chunk, which means that it is
                # unsafe in any mode
                self.save(
                    store,
                    arr.isel(a=slice(1, 5)).chunk(a=(3, 1)),
                    region="auto",
                    mode=mode,
                )

            with pytest.raises(ValueError):
                # The first chunk is safe but the other two chunks are overlapping with
                # the same Zarr chunk
                self.save(
                    store,
                    arr.isel(a=slice(0, 5)).chunk(a=(3, 1, 1)),
                    region="auto",
                    mode=mode,
                )

            # Fully update two contiguous chunks is safe in any mode
            self.save(store, arr.isel(a=slice(3, 9)), region="auto", mode=mode)

            # The last chunk is considered full based on their current size (2)
            self.save(store, arr.isel(a=slice(9, 11)), region="auto", mode=mode)
            self.save(
                store, arr.isel(a=slice(6, None)).chunk(a=-1), region="auto", mode=mode
            )

            # Write the last chunk of a region partially is safe in "a" mode
            self.save(store, arr.isel(a=slice(3, 8)), region="auto", mode="a")
            with pytest.raises(ValueError):
                # with "r+" mode it is invalid to write partial chunk
                self.save(arr.isel(a=slice(3, 8)), region="auto", mode="r+")

            # This is safe with mode "a", the border size is covered by the first chunk of Dask
            self.save(
                store, arr.isel(a=slice(1, 4)).chunk(a=(2, 1)), region="auto", mode="a"
            )
            with pytest.raises(ValueError):
                # This is considered unsafe in mode "r+" because it is writing in a partial chunk
                self.save(
                    store,
                    arr.isel(a=slice(1, 4)).chunk(a=(2, 1)),
                    region="auto",
                    mode="r+",
                )

            # This is safe on mode "a" because there is a single dask chunk
            self.save(
                store, arr.isel(a=slice(1, 5)).chunk(a=(4,)), region="auto", mode="a"
            )
            with pytest.raises(ValueError):
                # This is unsafe on mode "r+", because the Dask chunk is partially writing
                # in the first chunk of Zarr
                self.save(
                    store, arr.isel(a=slice(1, 5)).chunk(a=(4,)), region="auto", mode="r+"
                )

            # The first chunk is completely covering the first Zarr chunk
            # and the last chunk is a partial one
            self.save(
                store, arr.isel(a=slice(0, 5)).chunk(a=(3, 2)), region="auto", mode="a"
            )

            with pytest.raises(ValueError):
                # The last chunk is partial, so it is considered unsafe on mode "r+"
                self.save(
                    store,
                    arr.isel(a=slice(0, 5)).chunk(a=(3, 2)),
                    region="auto",
                    mode="r+",
                )

            # The first chunk is covering the border size (2 elements)
            # and also the second chunk (3 elements), so it is valid
            self.save(
                store, arr.isel(a=slice(1, 8)).chunk(a=(5, 2)), region="auto", mode="a"
            )

            with pytest.raises(ValueError):
                # The first chunk is not fully covering the first zarr chunk
                self.save(
                    store,
                    arr.isel(a=slice(1, 8)).chunk(a=(5, 2)),
                    region="auto",
                    mode="r+",
                )

            with pytest.raises(ValueError):
                # Validate that the border condition is not affecting the "r+" mode
                self.save(store, arr.isel(a=slice(1, 9)), region="auto", mode="r+")

            self.save(store, arr.isel(a=slice(10, 11)), region="auto", mode="a")
            with pytest.raises(ValueError):
                # Validate that even if we write with a single Dask chunk on the last Zarr
                # chunk it is still unsafe if it is not fully covering it
                # (the last Zarr chunk has size 2)
                self.save(store, arr.isel(a=slice(10, 11)), region="auto", mode="r+")

            # Validate the same as the above test but in the beginning of the last chunk
            self.save(store, arr.isel(a=slice(9, 10)), region="auto", mode="a")
            with pytest.raises(ValueError):
                self.save(store, arr.isel(a=slice(9, 10)), region="auto", mode="r+")

            self.save(
                store, arr.isel(a=slice(7, None)).chunk(a=-1), region="auto", mode="a"
            )
            with pytest.raises(ValueError):
                # Test that even a Dask chunk that covers the last Zarr chunk can be unsafe
                # if it is partial covering other Zarr chunks
                self.save(
                    store,
                    arr.isel(a=slice(7, None)).chunk(a=-1),
                    region="auto",
                    mode="r+",
                )

            with pytest.raises(ValueError):
                # If the chunk is of size equal to the one in the Zarr encoding, but
                # it is partially writing in the first chunk then raise an error
                self.save(
                    store, arr.isel(a=slice(8, None)).chunk(a=3), region="auto", mode="r+"
                )

            with pytest.raises(ValueError):
                self.save(
                    store, arr.isel(a=slice(5, -1)).chunk(a=5), region="auto", mode="r+"
                )

            # Test if the code is detecting the last chunk correctly
            data = np.random.default_rng(0).random((2920, 25, 53))
            ds = xr.Dataset({"temperature": (("time", "lat", "lon"), data)})
            chunks = {"time": 1000, "lat": 25, "lon": 53}
            self.save(store, ds.chunk(chunks), compute=False, mode="w")
            region = {"time": slice(1000, 2000, 1)}
            chunk = ds.isel(region)
            chunk = chunk.chunk()
            self.save(store, chunk.chunk(), region=region)
