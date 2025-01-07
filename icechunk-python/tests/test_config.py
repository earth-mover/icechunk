import os
from collections.abc import Generator
from pathlib import Path

import pytest

import icechunk
import zarr


@pytest.fixture(scope="function")
def tmp_store(tmpdir: Path) -> Generator[tuple[icechunk.IcechunkStore, str]]:
    repo_path = f"{tmpdir}"
    config = icechunk.RepositoryConfig.default()
    config.inline_chunk_threshold_bytes = 5
    repo = icechunk.Repository.open_or_create(
        storage=icechunk.local_filesystem_storage(repo_path),
        config=config,
    )

    session = repo.writable_session("main")
    store = session.store

    yield store, repo_path


def test_config_fetch() -> None:
    config = icechunk.RepositoryConfig.default()
    config.inline_chunk_threshold_bytes = 5
    storage = icechunk.in_memory_storage()
    icechunk.Repository.create(
        storage=storage,
        config=config,
    )

    assert icechunk.Repository.fetch_config(storage) == config


def test_config_save() -> None:
    config = icechunk.RepositoryConfig.default()
    storage = icechunk.in_memory_storage()
    repo = icechunk.Repository.create(
        storage=storage,
    )

    config.inline_chunk_threshold_bytes = 5
    repo = icechunk.Repository.open(
        storage=storage,
        config=config,
    )

    assert icechunk.Repository.fetch_config(storage) != config
    repo.save_config()
    assert icechunk.Repository.fetch_config(storage) == config


def test_no_inline_chunks(tmp_store: tuple[icechunk.IcechunkStore, str]) -> None:
    store = tmp_store[0]
    store_path = tmp_store[1]
    array = zarr.create_array(
        store=store,
        shape=(10),
        dtype="int64",
        zarr_format=3,
        chunks=(1,),
        fill_value=-1,
    )
    array[:] = 42

    # Check that the chunks directory was created, since each chunk is 4 bytes and the
    # inline_chunk_threshold is 1, we should have 10 chunks in the chunks directory
    assert os.path.isdir(f"{store_path}/chunks")
    assert len(os.listdir(f"{store_path}/chunks")) == 10


def test_inline_chunks(tmp_store: tuple[icechunk.IcechunkStore, str]) -> None:
    store = tmp_store[0]
    store_path = tmp_store[1]

    inline_array = zarr.create_array(
        store=store,
        name="inline",
        shape=(10),
        dtype="int32",
        zarr_format=3,
        chunks=(1,),
        fill_value=-1,
        compressors=None,
    )

    inline_array[:] = 9

    # Check that the chunks directory was not created, since each chunk is 4 bytes and the
    # inline_chunk_threshold is 40, we should have no chunks directory
    assert not os.path.isdir(f"{store_path}/chunks")

    written_array = zarr.create_array(
        store=store,
        name="not_inline",
        shape=(10),
        dtype="int64",
        zarr_format=3,
        chunks=(1,),
        fill_value=-1,
        compressors=None,
    )

    written_array[:] = 3

    # Check that the chunks directory was not created, since each chunk is 8 bytes and the
    # inline_chunk_threshold is 40, we should have 10 chunks in the chunks directory
    assert os.path.isdir(f"{store_path}/chunks")
    assert len(os.listdir(f"/{store_path}/chunks")) == 10


def test_virtual_chunk_containers() -> None:
    config = icechunk.RepositoryConfig.default()

    store_config = icechunk.s3_store(
        region="us-east-1",
        endpoint_url="http://localhost:9000",
        allow_http=True,
        s3_compatible=True,
    )
    container = icechunk.VirtualChunkContainer("custom", "s3://", store_config)
    config.set_virtual_chunk_container(container)
    assert len(config.virtual_chunk_containers) > 1
    found_cont = [
        cont
        for (name, cont) in config.virtual_chunk_containers.items()
        if name == "custom"
    ]
    assert found_cont[0] == container

    config.clear_virtual_chunk_containers()
    assert {} == config.virtual_chunk_containers

    config.set_virtual_chunk_container(container)
    found_cont = [
        cont
        for (name, cont) in config.virtual_chunk_containers.items()
        if name == "custom"
    ]
    assert found_cont == [container]
