import os

import pytest

import icechunk
import zarr


@pytest.fixture(scope="function")
def tmp_store(tmpdir):
    repo_path = f"{tmpdir}"
    repo = icechunk.Repository.open_or_create(
        storage=icechunk.StorageConfig.filesystem(repo_path),
        config=icechunk.RepositoryConfig(inline_chunk_threshold_bytes=5),
    )

    session = repo.writable_session("main")
    store = session.store()

    yield store, repo_path


def test_no_inline_chunks(tmp_store):
    store = tmp_store[0]
    store_path = tmp_store[1]
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
    assert os.path.isdir(f"{store_path}/chunks")
    assert len(os.listdir(f"{store_path}/chunks")) == 10


def test_inline_chunks(tmp_store):
    store = tmp_store[0]
    store_path = tmp_store[1]

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
    assert not os.path.isdir(f"{store_path}/chunks")

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
    assert os.path.isdir(f"{store_path}/chunks")
    assert len(os.listdir(f"/{store_path}/chunks")) == 10
