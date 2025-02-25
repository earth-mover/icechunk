# An example of using multiprocessing to write to an Icechunk dataset

import tempfile
from concurrent.futures import ProcessPoolExecutor

import xarray as xr
from icechunk import Repository, Session, local_filesystem_storage
from icechunk.distributed import merge_sessions


def write_timestamp(*, itime: int, session: Session) -> Session:
    # pass a list to isel to preserve the time dimension
    ds = xr.tutorial.open_dataset("rasm").isel(time=[itime])
    # region="auto" tells Xarray to infer which "region" of the output arrays to write to.
    ds.to_zarr(session.store, region="auto", consolidated=False)
    return session


if __name__ == "__main__":
    ds = xr.tutorial.open_dataset("rasm").isel(time=slice(24))
    repo = Repository.create(local_filesystem_storage(tempfile.mkdtemp()))
    session = repo.writable_session("main")

    chunks = {1 if dim == "time" else ds.sizes[dim] for dim in ds.Tair.dims}
    ds.to_zarr(
        session.store, compute=False, encoding={"Tair": {"chunks": chunks}}, mode="w"
    )
    # this commit is optional, but may be useful in your workflow
    session.commit("initialize store")

    session = repo.writable_session("main")
    with ProcessPoolExecutor() as executor:
        # opt-in to successful pickling of a writable session
        with session.allow_pickling():
            # submit the writes
            futures = [
                executor.submit(write_timestamp, itime=i, session=session)
                for i in range(ds.sizes["time"])
            ]
            # grab the Session objects from each individual write task
            sessions = [f.result() for f in futures]

    # manually merge the remote sessions in to the local session
    session = merge_sessions(session, *sessions)
    session.commit("finished writes")

    ondisk = xr.open_zarr(repo.readonly_session("main").store, consolidated=False)
    xr.testing.assert_identical(ds, ondisk)
