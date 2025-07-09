import os
import re
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
    repo = icechunk.Repository.create(
        storage=storage,
        config=config,
    )

    assert repo.config == config
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
    container = icechunk.VirtualChunkContainer("s3://testbucket", store_config)
    config.set_virtual_chunk_container(container)
    assert re.match(
        r"RepositoryConfig\(inline_chunk_threshold_bytes=None, get_partial_values_concurrency=None, compression=None, caching=None, storage=None, manifest=.*\)",
        repr(config),
    )
    assert config.virtual_chunk_containers
    assert len(config.virtual_chunk_containers) == 1
    assert config.virtual_chunk_containers["s3://testbucket"] == container

    config.clear_virtual_chunk_containers()
    assert {} == config.virtual_chunk_containers

    config.set_virtual_chunk_container(container)
    assert len(config.virtual_chunk_containers) == 1
    assert config.virtual_chunk_containers["s3://testbucket"] == container


def test_can_change_deep_config_values() -> None:
    storage = icechunk.in_memory_storage()
    repo = icechunk.Repository.create(
        storage=storage,
    )
    config = icechunk.RepositoryConfig(
        inline_chunk_threshold_bytes=11,
        compression=icechunk.CompressionConfig(level=0),
    )
    config.inline_chunk_threshold_bytes = 5
    config.get_partial_values_concurrency = 42
    config.compression = icechunk.CompressionConfig(level=8)
    config.compression.level = 2
    config.caching = icechunk.CachingConfig(num_chunk_refs=8)
    config.storage = storage.default_settings()
    assert config.storage.concurrency
    config.storage.concurrency.ideal_concurrent_request_size = 1_000_000
    assert config.storage.retries
    config.storage.retries.max_tries = 42
    config.storage.retries.initial_backoff_ms = 500
    config.storage.retries.max_backoff_ms = 600
    config.storage.storage_class = "STANDARD_IA"
    config.manifest = icechunk.ManifestConfig()
    config.manifest.preload = icechunk.ManifestPreloadConfig(max_total_refs=42)
    config.manifest.preload.preload_if = icechunk.ManifestPreloadCondition.and_conditions(
        [
            icechunk.ManifestPreloadCondition.true(),
            icechunk.ManifestPreloadCondition.name_matches("foo"),
        ]
    )

    assert re.match(
        r"RepositoryConfig\(inline_chunk_threshold_bytes=5, get_partial_values_concurrency=42, compression=CompressionConfig\(algorithm=None, level=2\), caching=CachingConfig\(num_snapshot_nodes=None, num_chunk_refs=8, num_transaction_changes=None, num_bytes_attributes=None, num_bytes_chunks=None\), storage=StorageSettings\(concurrency=StorageConcurrencySettings\(max_concurrent_requests_for_object=5, ideal_concurrent_request_size=1000000\), retries=StorageRetriesSettings\(max_tries=42, initial_backoff_ms=500, max_backoff_ms=600\), unsafe_use_conditional_create=None, unsafe_use_conditional_update=None, unsafe_use_metadata=None, storage_class=\"STANDARD_IA\", metadata_storage_class=None, chunks_storage_class=None\), manifest=.*\)",
        repr(config),
    )
    repo = icechunk.Repository.open(
        storage=storage,
        config=config,
    )
    repo.save_config()

    stored_config = icechunk.Repository.fetch_config(storage)
    assert stored_config
    assert stored_config.inline_chunk_threshold_bytes == 5
    assert stored_config.compression
    assert stored_config.compression.level == 2
    assert stored_config.caching
    assert stored_config.caching.num_chunk_refs == 8
    assert stored_config.storage
    assert stored_config.storage.concurrency
    assert stored_config.storage.concurrency.ideal_concurrent_request_size == 1_000_000
    assert stored_config.storage.retries
    assert stored_config.storage.retries.max_tries == 42
    assert stored_config.storage.retries.initial_backoff_ms == 500
    assert stored_config.storage.retries.max_backoff_ms == 600
    assert stored_config.manifest
    assert stored_config.manifest.preload
    assert config.manifest.preload.max_total_refs == 42
    assert (
        config.manifest.preload.preload_if
        == icechunk.ManifestPreloadCondition.and_conditions(
            [
                icechunk.ManifestPreloadCondition.true(),
                icechunk.ManifestPreloadCondition.name_matches("foo"),
            ]
        )
    )


def test_manifest_preload_magic_methods() -> None:
    assert (
        icechunk.ManifestPreloadCondition.and_conditions(
            [
                icechunk.ManifestPreloadCondition.true(),
                icechunk.ManifestPreloadCondition.name_matches("foo"),
            ]
        )
        == icechunk.ManifestPreloadCondition.true()
        & icechunk.ManifestPreloadCondition.name_matches("foo")
    )
    assert (
        icechunk.ManifestPreloadCondition.or_conditions(
            [
                icechunk.ManifestPreloadCondition.true(),
                icechunk.ManifestPreloadCondition.name_matches("foo"),
            ]
        )
        == icechunk.ManifestPreloadCondition.true()
        | icechunk.ManifestPreloadCondition.name_matches("foo")
    )


def test_spec_version() -> None:
    assert icechunk.spec_version() >= 1
