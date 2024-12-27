import time
import warnings
from typing import cast

import pytest

import dask.array
import icechunk
import zarr
from dask.array.utils import assert_eq
from dask.distributed import Client
from icechunk.dask import store_dask

# We create a 2-d array with this many chunks along each direction
CHUNKS_PER_DIM = 10

# Each chunk is CHUNK_DIM_SIZE x CHUNK_DIM_SIZE floats
CHUNK_DIM_SIZE = 10

# We split the writes in tasks, each task does this many chunks
CHUNKS_PER_TASK = 2


def mk_repo() -> icechunk.Repository:
    opts = icechunk.S3CompatibleOptions(
        endpoint_url="http://localhost:9000", allow_http=True, region="us-east-1"
    )
    credentials = icechunk.Credentials.Static(
        icechunk.StaticCredentials(access_key_id="minio123", secret_access_key="minio123")
    )
    storage = icechunk.Storage.create(
        icechunk.ObjectStoreConfig.S3Compatible(opts),
        bucket="testbucket",
        prefix="python-distributed-writers-test__" + str(time.time()),
        credentials=credentials,
    )
    repo_config = icechunk.RepositoryConfig(
        inline_chunk_threshold_bytes=5,
    )
    repo = icechunk.Repository.open_or_create(
        storage=storage,
        config=repo_config,
    )

    return repo


@pytest.mark.skip(reason="Distributed writes are not yet fully implemented")
async def test_distributed_writers():
    """Write to an array using uncoordinated writers, distributed via Dask.

    We create a big array, and then we split into workers, each worker gets
    an area, where it writes random data with a known seed. Each worker
    returns the bytes for its ChangeSet, then the coordinator (main thread)
    does a distributed commit. When done, we open the store again and verify
    we can write everything we have written.
    """
    repo = mk_repo()
    session = repo.writable_session(branch="main")
    store = session.store

    shape = (CHUNKS_PER_DIM * CHUNK_DIM_SIZE,) * 2
    dask_chunks = (CHUNK_DIM_SIZE * CHUNKS_PER_TASK,) * 2
    dask_array = dask.array.random.random(shape, chunks=dask_chunks)
    group = zarr.group(store=store, overwrite=True)

    zarray = group.create_array(
        "array",
        shape=shape,
        chunk_shape=(CHUNK_DIM_SIZE, CHUNK_DIM_SIZE),
        dtype="f8",
        fill_value=float("nan"),
    )
    _first_snap = session.commit("array created")

    with Client(n_workers=8):
        session = repo.writable_session(branch="main")
        store = session.store
        group = zarr.open_group(store=store)
        zarray = cast(zarr.Array, group["array"])
        # with store.preserve_read_only():
        store_dask(session, sources=[dask_array], targets=[zarray])
        commit_res = session.commit("distributed commit")
        assert commit_res

        # Lets open a new store to verify the results
        readonly_session = repo.readonly_session(branch="main")
        store = readonly_session.store
        all_keys = [key async for key in store.list_prefix("/")]
        assert (
            len(all_keys) == 1 + 1 + CHUNKS_PER_DIM * CHUNKS_PER_DIM
        )  # group meta + array meta + each chunk

        group = zarr.open_group(store=store, mode="r")

        roundtripped = dask.array.from_array(group["array"], chunks=dask_chunks)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=UserWarning)
            assert_eq(roundtripped, dask_array)
