import os
import shutil

import icechunk
import pytest
import zarr

STORE_PATH = "/tmp/icechunk_config_test"

@pytest.fixture
async def store():
    store = await icechunk.IcechunkStore.open(
        storage=icechunk.StorageConfig.filesystem(STORE_PATH),
        mode="a",
        config=icechunk.StoreConfig(inline_chunk_threshold_bytes=5),
    )

    yield store

    store.close()
    shutil.rmtree(STORE_PATH)


async def test_no_inline_chunks(store):
    array = zarr.open_array(
        store=store,
        mode="a",
        shape=(10),
        dtype="int64",
        zarr_format=3,
        chunk_shape=(1),
        fill_value=-1,
    )
    array[:] = 42

    # Check that the chunks directory was created, since each chunk is 4 bytes and the
    # inline_chunk_threshold is 1, we should have 10 chunks in the chunks directory
    assert os.path.isdir(f"{STORE_PATH}/chunks")
    assert len(os.listdir(f"{STORE_PATH}/chunks")) == 10


async def test_inline_chunks(store):
    inline_array = zarr.open_array(
        store=store,
        mode="a",
        path="inline",
        shape=(10),
        dtype="int32",
        zarr_format=3,
        chunk_shape=(1),
        fill_value=-1,
    )

    inline_array[:] = 9

    # Check that the chunks directory was not created, since each chunk is 4 bytes and the
    # inline_chunk_threshold is 40, we should have no chunks directory
    assert not os.path.isdir(f"{STORE_PATH}/chunks")

    written_array = zarr.open_array(
        store=store,
        mode="a",
        path="not_inline",
        shape=(10),
        dtype="int64",
        zarr_format=3,
        chunk_shape=(1),
        fill_value=-1,
    )

    written_array[:] = 3

    # Check that the chunks directory was not created, since each chunk is 8 bytes and the
    # inline_chunk_threshold is 40, we should have 10 chunks in the chunks directory
    assert os.path.isdir(f"{STORE_PATH}/chunks")
    assert len(os.listdir(f"/{STORE_PATH}/chunks")) == 10
