# An example of using multiprocessing to write to an Icechunk dataset

import tempfile
from concurrent.futures import ProcessPoolExecutor

import numpy as np
import pandas as pd

import xarray as xr
from icechunk import Repository, Session, local_filesystem_storage

TIME = pd.date_range("2000-01-01", periods=24, freq="MS")
NY, NX = 205, 275


def make_dataset():
    """Generate a fake dataset with a 'Tair' variable on a (time, y, x) grid."""
    rng = np.random.default_rng(42)
    tair = rng.standard_normal((len(TIME), NY, NX)).astype(np.float32)
    return xr.Dataset(
        {"Tair": (("time", "y", "x"), tair)},
        coords={"time": TIME},
    )


def write_timestamp(*, itime: int, session: Session) -> Session:
    # In a real workflow, you might use an `xarray.open_dataset` call here
    # and index out the right time value
    ds = make_dataset().isel(time=[itime])
    # region="auto" tells Xarray to infer which "region" of the output arrays to write to.
    ds.to_zarr(session.store, region="auto", consolidated=False)
    return session


if __name__ == "__main__":
    ds = make_dataset()

    repo = Repository.create(local_filesystem_storage(tempfile.mkdtemp()))
    session = repo.writable_session("main")

    chunks = {1 if dim == "time" else ds.sizes[dim] for dim in ds.Tair.dims}
    ds.to_zarr(
        session.store,
        compute=False,
        encoding={"Tair": {"chunks": chunks}},
        mode="w",
        consolidated=False,
    )
    # this commit is optional, but may be useful in your workflow
    session.commit("initialize store")

    session = repo.writable_session("main")
    # opt-in to successful pickling of a writable session
    fork = session.fork()
    with ProcessPoolExecutor() as executor:
        # submit the writes
        futures = [
            executor.submit(write_timestamp, itime=i, session=fork)
            for i in range(ds.sizes["time"])
        ]
        # grab the Session objects from each individual write task
        fork_sessions = [f.result() for f in futures]

    # manually merge the remote sessions in to the local session
    session.merge(*fork_sessions)
    session.commit("finished writes")

    ondisk = xr.open_zarr(repo.readonly_session("main").store, consolidated=False)
    xr.testing.assert_identical(ds, ondisk)
