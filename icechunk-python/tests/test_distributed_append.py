# Regression test for GH1006
# https://github.com/earth-mover/icechunk/issues/1006

import datetime

import pytest

import dask
import icechunk as ic
import xarray as xr
import zarr
from icechunk.xarray import to_icechunk

p = zarr.core.buffer.default_buffer_prototype()

PLOT = False
SPLIT_EVERY = 128
DIMS = ("x", "y")
IC_STORAGE = ic.local_filesystem_storage(
    f"/tmp/test/icechunk_data_corrupted/{str(datetime.datetime.now()).split(' ')[-1]}",
)


def do_test(scheduler) -> None:
    # Writing the initial dataset
    if scheduler in ["processes", "sync"]:
        CHUNKX = 3
        CHUNKY = 6
        X = CHUNKX * 1 + 1
        Y = CHUNKY * 1 + 1
        dX = 1
        dY = 1
    else:
        CHUNKX = 300
        CHUNKY = 600
        X = CHUNKX * 1 + 1
        Y = CHUNKY * 25 + 1
        dX = 1
        dY = 1

    def plot() -> None:
        if not PLOT:
            return
        import matplotlib.pyplot as plt

        # kwargs = dict(ylim=(X - CHUNKX, X + dX + 1), xlim=(Y - CHUNKY // 10, Y + dY + 1))
        plt.pcolor(
            xr.open_zarr(session.store, consolidated=False)["a"].isel(
                x=slice(X - CHUNKX, None), y=slice(Y - CHUNKY, None)
            ),
            vmin=0,
            vmax=4,
        )
        # plt.gca().set_yticks(np.arange(0, X + dX + 1, CHUNKX))
        # plt.gca().set_xticks(np.arange(0, Y + dY + 1, CHUNKY))
        plt.xlim(0, Y + 1)
        plt.ylim(0, X + 1)
        plt.axhline(CHUNKX - 1, color="g")
        plt.axvline(CHUNKY - 1, color="g")
        plt.axhline(X - 1, color="r")
        plt.axvline(Y - 1, color="r")
        plt.grid(True)

    initial = xr.Dataset(
        {
            "a": xr.DataArray(
                1.1, dims=DIMS, coords={"x": list(range(X)), "y": list(range(Y))}
            )
        }
    ).chunk(x=CHUNKX, y=CHUNKY)

    repo = ic.Repository.open_or_create(IC_STORAGE)
    session = repo.writable_session("main")
    with dask.config.set(scheduler=scheduler):
        to_icechunk(initial, session=session, mode="w", split_every=SPLIT_EVERY)
    plot()
    session.commit("initial write")
    assert (xr.open_zarr(session.store, consolidated=False)["a"] >= 1).all()

    print("finished initial write")
    print("-======================")
    print("appending along x")

    # Appending data on the X axis
    x_append_data = xr.Dataset(
        {
            "a": xr.DataArray(
                2.2,
                dims=DIMS,
                coords={"x": list(range(X, X + dX)), "y": list(range(Y))},
            )
        }
    ).chunk(x=dX, y=CHUNKY)
    session = repo.writable_session("main")
    with dask.config.set(scheduler=scheduler):
        to_icechunk(
            x_append_data, session=session, append_dim="x", split_every=SPLIT_EVERY
        )
    plot()
    session.commit("append along x")
    assert (xr.open_zarr(session.store, consolidated=False)["a"] >= 1).all()

    print("-======================")
    print("appending along y")

    # Appending data on the Y axis, this part is the one that corrupts the data
    y_append_data = xr.Dataset(
        {
            "a": xr.DataArray(
                3.3,
                dims=DIMS,
                coords={"x": list(range(X + dX)), "y": list(range(Y, Y + dY))},
            ),
        }
    ).chunk(x=CHUNKX, y=-1)
    session = repo.writable_session("main")
    with dask.config.set(scheduler=scheduler):
        to_icechunk(
            y_append_data, session=session, append_dim="y", split_every=SPLIT_EVERY
        )
    plot()
    session.commit("append along y")
    assert (xr.open_zarr(session.store, consolidated=False)["a"] >= 1).all()


@pytest.mark.parametrize("scheduler", ["threads", "processes"])
def test_dask_distributed_appends(scheduler) -> None:
    do_test(scheduler)


if __name__ == "__main__":
    do_test("threads")
    do_test("processes")
