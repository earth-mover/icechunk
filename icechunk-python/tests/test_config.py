import os
from collections.abc import Generator
from pathlib import Path

import pytest

import icechunk
import zarr
from icechunk._icechunk_python import RepoUpdateRetryConfig


@pytest.fixture(scope="function")
def tmp_store(
    tmpdir: Path, any_spec_version: int | None
) -> Generator[tuple[icechunk.IcechunkStore, str]]:
    repo_path = f"{tmpdir}"
    config = icechunk.RepositoryConfig.default()
    config.inline_chunk_threshold_bytes = 5
    repo = icechunk.Repository.open_or_create(
        storage=icechunk.local_filesystem_storage(repo_path),
        config=config,
        create_version=any_spec_version,
    )

    session = repo.writable_session("main")
    store = session.store

    yield store, repo_path


def test_config_fetch(any_spec_version: int | None) -> None:
    storage = icechunk.in_memory_storage()
    config = icechunk.RepositoryConfig.default()
    config.inline_chunk_threshold_bytes = 5
    config.storage = storage.default_settings()
    repo = icechunk.Repository.create(
        storage=storage,
        config=config,
        spec_version=any_spec_version,
    )

    assert repo.config == config
    assert icechunk.Repository.fetch_config(storage) == config


def test_config_save(any_spec_version: int | None) -> None:
    storage = icechunk.in_memory_storage()
    config = icechunk.RepositoryConfig.default()
    repo = icechunk.Repository.create(
        storage=storage,
        spec_version=any_spec_version,
    )

    config.inline_chunk_threshold_bytes = 5
    config.storage = storage.default_settings()
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
        endpoint_url="http://localhost:4200",
        allow_http=True,
        s3_compatible=True,
    )
    container = icechunk.VirtualChunkContainer("s3://testbucket/", store_config)
    config.set_virtual_chunk_container(container)
    # TODO: check all fields once compression, storage, manifest are added to the repr
    repr_str = repr(config)
    assert "icechunk.RepositoryConfig(" in repr_str
    assert "inline_chunk_threshold_bytes=None" in repr_str
    assert "caching=None" in repr_str
    assert config.virtual_chunk_containers
    assert len(config.virtual_chunk_containers) == 1
    assert config.virtual_chunk_containers["s3://testbucket/"] == container

    config.clear_virtual_chunk_containers()
    assert {} == config.virtual_chunk_containers

    config.set_virtual_chunk_container(container)
    assert len(config.virtual_chunk_containers) == 1
    assert config.virtual_chunk_containers["s3://testbucket/"] == container


def test_can_change_deep_config_values(any_spec_version: int | None) -> None:
    storage = icechunk.in_memory_storage()
    repo = icechunk.Repository.create(
        storage=storage,
        spec_version=any_spec_version,
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
    config.max_concurrent_requests = 10
    config.num_updates_per_repo_info_file = 50
    config.repo_update_retries = RepoUpdateRetryConfig(
        default=icechunk.StorageRetriesSettings(
            max_tries=200, initial_backoff_ms=100, max_backoff_ms=60_000
        )
    )
    config.manifest.preload.preload_if = icechunk.ManifestPreloadCondition.and_conditions(
        [
            icechunk.ManifestPreloadCondition.true(),
            icechunk.ManifestPreloadCondition.name_matches("foo"),
        ]
    )
    config.manifest.virtual_chunk_location_compression = (
        icechunk.ManifestVirtualChunkLocationCompressionConfig(
            min_num_chunks=500,
            dictionary_max_training_samples=200,
            dictionary_max_size_bytes=4096,
            compression_level=5,
        )
    )

    # TODO: check all fields once compression, storage, manifest are added to the repr
    repr_str = repr(config)
    assert "icechunk.RepositoryConfig(" in repr_str
    assert "inline_chunk_threshold_bytes=5" in repr_str
    assert "get_partial_values_concurrency=42" in repr_str
    assert "icechunk.CachingConfig(" in repr_str
    assert "num_chunk_refs=8" in repr_str
    repo = icechunk.Repository.open(
        storage=storage,
        config=config,
    )
    repo.save_config()

    stored_config = icechunk.Repository.fetch_config(storage)
    assert stored_config
    assert stored_config.inline_chunk_threshold_bytes == 5
    assert stored_config.get_partial_values_concurrency == 42
    assert stored_config.compression
    assert stored_config.compression.level == 2
    assert stored_config.max_concurrent_requests == 10
    assert stored_config.num_updates_per_repo_info_file == 50
    assert stored_config.caching
    assert stored_config.caching.num_chunk_refs == 8
    assert stored_config.storage
    assert stored_config.storage.concurrency
    assert stored_config.storage.concurrency.ideal_concurrent_request_size == 1_000_000
    assert stored_config.storage.retries
    assert stored_config.storage.retries.max_tries == 42
    assert stored_config.storage.retries.initial_backoff_ms == 500
    assert stored_config.storage.retries.max_backoff_ms == 600
    assert stored_config.storage.storage_class == "STANDARD_IA"
    assert stored_config.repo_update_retries
    assert stored_config.repo_update_retries.default
    assert stored_config.repo_update_retries.default.max_tries == 200
    assert stored_config.repo_update_retries.default.initial_backoff_ms == 100
    assert stored_config.repo_update_retries.default.max_backoff_ms == 60_000
    assert stored_config.manifest
    assert stored_config.manifest.preload
    assert stored_config.manifest.preload.max_total_refs == 42
    assert (
        stored_config.manifest.preload.preload_if
        == icechunk.ManifestPreloadCondition.and_conditions(
            [
                icechunk.ManifestPreloadCondition.true(),
                icechunk.ManifestPreloadCondition.name_matches("foo"),
            ]
        )
    )
    assert stored_config.manifest.virtual_chunk_location_compression
    vlc = stored_config.manifest.virtual_chunk_location_compression
    assert vlc.min_num_chunks == 500
    assert vlc.dictionary_max_training_samples == 200
    assert vlc.dictionary_max_size_bytes == 4096
    assert vlc.compression_level == 5


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


def test_config_from_store(any_spec_version: int | None) -> None:
    storage = icechunk.in_memory_storage()
    config = icechunk.RepositoryConfig.default()
    config.inline_chunk_threshold_bytes = 5
    config.storage = storage.default_settings()

    store_config = icechunk.s3_store(region="us-east-1")
    container = icechunk.VirtualChunkContainer("s3://example/", store_config)
    config.set_virtual_chunk_container(container)

    repo = icechunk.Repository.create(
        storage=storage,
        config=config,
        spec_version=any_spec_version,
    )
    session = repo.writable_session("main")
    store = session.store

    assert store.session.config == config
    assert store.session.config.virtual_chunk_containers is not None
    assert store.session.config.virtual_chunk_containers.keys() == {"s3://example/"}


def test_s3_storage_options() -> None:
    _storage = icechunk.s3_storage(
        region="us-east-1",
        endpoint_url="http://localhost:4200",
        allow_http=True,
        force_path_style=True,
        bucket="testbucket",
        prefix="this-repo-does-not-exist",
        scatter_initial_credentials=True,
        network_stream_timeout_seconds=50,
    )
    # TODO: add accessors and verify values
    # currently this is only testing we can construct


def test_clear_virtual_chunk_containers_persists_through_reopen() -> None:
    """Test that clearing VCCs is respected by reopen().

    Regression test for: https://github.com/earth-mover/icechunk/issues/XXX
    """
    storage = icechunk.in_memory_storage()

    # Create repo and add a VCC
    repo = icechunk.Repository.create(storage=storage)
    config = repo.config
    store_config = icechunk.s3_store(region="us-east-1")
    container = icechunk.VirtualChunkContainer("s3://testbucket/", store_config)
    config.set_virtual_chunk_container(container)
    repo = repo.reopen(config=config)
    repo.save_config()

    vccs = repo.config.virtual_chunk_containers or {}
    assert "s3://testbucket/" in vccs

    # Clear VCCs and reopen
    config = repo.config
    config.clear_virtual_chunk_containers()
    assert config.virtual_chunk_containers == {}

    repo = repo.reopen(config=config)

    # VCCs should be cleared after reopen
    reopened_vccs = repo.config.virtual_chunk_containers or {}
    assert reopened_vccs == {}, (
        f"Expected no VCCs after reopen, got: {list(reopened_vccs.keys())}"
    )
