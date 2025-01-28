# Distributed writes

A common pattern with large distributed write jobs is to first initialize the dataset on a disk
with all appropriate metadata, and any coordinate variables. Following this a large write job
is kicked off in a distributed setting, where each worker is responsible for an independent
"region" of the output.

Here is how you can execute such writes with Icechunk, illustrate with a `ThreadPoolExecutor`.
First read some example data, and create an Icechunk Repository.
```python
import xarray as xr
import tempfile
from icechunk import Repository, local_filesystem_storage

ds = xr.tutorial.open_dataset("rasm").isel(time=slice(24))
repo = Repository.create(local_filesystem_storage(tempfile.mkdtemp()))
session = repo.writable_session("main")
```
We will orchestrate so that each task writes one timestep.
This is an arbitrary choice but determines what we set for the Zarr chunk size.
```python
chunks = {1 if dim == "time" else ds.sizes[dim] for dim in ds.Tair.dims}
```

Initialize the dataset using to_zarr and compute=False, this will NOT write any chunked array data,
but will write all array metadata, and any in-memory arrays (only `time` in this case).
```python
ds.to_zarr(session.store, compute=False, encoding={"Tair": {"chunks": chunks}}, mode="w")
# this commit is optional, but may be useful in your workflow
session.commit("initialize store")
```

Define a function that constitutes one "write task".
It is important to return the Session here. It contains a record
of the changes executed by this task.
Later the changes from individual tasks will be merged in order to create a meaningful commit.
```python
from icechunk import Session

def write_timestamp(*, itime: int, session: Session) -> Session:
    # pass a list to isel to preserve the time dimension
    ds = xr.tutorial.open_dataset("rasm").isel(time=[itime])
    # region="auto" tells Xarray to infer which "region" of the output arrays to write to.
    ds.to_zarr(session.store, region="auto", consolidated=False)
    return session
```

Now execute the writes. We use a `ThreadPoolExecutor` for ease of demonstration but any task
execution framework (e.g. `ProcessPoolExecutor`, joblib, lithops, dask, ray, etc.)
```python
from concurrent.futures import ThreadPoolExecutor
from icechunk.distributed import merge_sessions

session = repo.writable_session("main")
with ThreadPoolExecutor() as executor:
    # submit the writes
    futures = [executor.submit(write_timestamp, itime=i, session=session) for i in range(ds.sizes["time"])]
    # grab the Session objects from each individual write task
    sessions = [f.result() for f in futures]

# merge the remote sessions in to the local session
session = merge_sessions(session, *sessions)
session.commit("finished writes")
```

Verify that the writes worked as expected:
```python
ondisk = xr.open_zarr(repo.readonly_session(branch="main").store, consolidated=False)
xr.testing.assert_identical(ds, ondisk)
```
