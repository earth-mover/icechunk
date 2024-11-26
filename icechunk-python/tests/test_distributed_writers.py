import time
import warnings
from typing import Any

import dask.array
import icechunk
import zarr
from dask.array.utils import assert_eq
from dask.distributed import Client
from icechunk import IcechunkStore
from icechunk.dask import store_dask

# We create a 2-d array with this many chunks along each direction
CHUNKS_PER_DIM = 10

# Each chunk is CHUNK_DIM_SIZE x CHUNK_DIM_SIZE floats
CHUNK_DIM_SIZE = 10

# We split the writes in tasks, each task does this many chunks
CHUNKS_PER_TASK = 2


def mk_store(
    read_only: bool, storage_config: dict[str, Any], store_config: dict[str, Any]
) -> IcechunkStore:
    storage_config = icechunk.StorageConfig.s3_from_config(
        **storage_config,
        credentials=icechunk.S3Credentials(
            access_key_id="minio123",
            secret_access_key="minio123",
        ),
    )
    store_config = icechunk.StoreConfig(**store_config)

    store = icechunk.IcechunkStore.open_or_create(
        storage=storage_config,
        read_only=read_only,
        config=store_config,
    )

    return store


async def test_distributed_writers():
    """Write to an array using uncoordinated writers, distributed via Dask.

    We create a big array, and then we split into workers, each worker gets
    an area, where it writes random data with a known seed. Each worker
    returns the bytes for its ChangeSet, then the coordinator (main thread)
    does a distributed commit. When done, we open the store again and verify
    we can write everything we have written.
    """

    storage_config = {
        "bucket": "testbucket",
        "prefix": "python-distributed-writers-test__" + str(time.time()),
        "endpoint_url": "http://localhost:9000",
        "region": "us-east-1",
        "allow_http": True,
    }
    store_config = {"inline_chunk_threshold_bytes": 5}
    store = mk_store(
        read_only=False, storage_config=storage_config, store_config=store_config
    )

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
    _first_snap = store.commit("array created")

    with Client(n_workers=8):
        with store.preserve_read_only():
            store_dask(store, sources=[dask_array], targets=[zarray])
        commit_res = store.commit("distributed commit")
        assert commit_res

        # Lets open a new store to verify the results
        store = mk_store(
            read_only=True, storage_config=storage_config, store_config=store_config
        )
        all_keys = [key async for key in store.list_prefix("/")]
        assert (
            len(all_keys) == 1 + 1 + CHUNKS_PER_DIM * CHUNKS_PER_DIM
        )  # group meta + array meta + each chunk

        group = zarr.open_group(store=store, mode="r")

        roundtripped = dask.array.from_array(group["array"], chunks=dask_chunks)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=UserWarning)
            assert_eq(roundtripped, dask_array)
