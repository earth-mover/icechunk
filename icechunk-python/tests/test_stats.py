from pathlib import Path

import pytest

import icechunk as ic
import zarr


@pytest.mark.filterwarnings("ignore:datetime.datetime.utcnow")
def test_total_chunks_storage(any_spec_version: int | None) -> None:
    """We only test the interface, more detailed test is done in Rust"""

    repo = ic.Repository.create(
        storage=ic.in_memory_storage(),
        config=ic.RepositoryConfig(inline_chunk_threshold_bytes=0),
        spec_version=any_spec_version,
    )
    session = repo.writable_session("main")
    store = session.store

    group = zarr.group(store=store, overwrite=True)
    array = group.create_array(
        "array",
        shape=(100),
        chunks=(1,),
        dtype="i4",
        compressors=None,
    )

    array[:] = 42
    session.commit("commit 1")

    with pytest.warns(DeprecationWarning, match="total_chunks_storage"):
        assert repo.total_chunks_storage() == 100 * 4


@pytest.mark.filterwarnings("ignore:datetime.datetime.utcnow")
async def test_total_chunks_storage_async(any_spec_version: int | None) -> None:
    """Test the async version of total_chunks_storage"""

    repo = await ic.Repository.create_async(
        storage=ic.in_memory_storage(),
        config=ic.RepositoryConfig(inline_chunk_threshold_bytes=0),
        spec_version=any_spec_version,
    )
    session = await repo.writable_session_async("main")
    store = session.store

    group = zarr.group(store=store, overwrite=True)
    array = group.create_array(
        "array",
        shape=(100),
        chunks=(1,),
        dtype="i4",
        compressors=None,
    )

    array[:] = 42
    await session.commit_async("commit 1")

    with pytest.warns(DeprecationWarning, match="total_chunks_storage_async"):
        assert await repo.total_chunks_storage_async() == 100 * 4


@pytest.mark.parametrize(
    "dir", ["./tests/data/test-repo-v2", "./tests/data/test-repo-v1"]
)
def test_chunk_storage_on_filesystem(dir: str) -> None:
    repo = ic.Repository.open(
        storage=ic.local_filesystem_storage(dir),
    )
    with pytest.warns(DeprecationWarning, match="total_chunks_storage"):
        actual = repo.total_chunks_storage()
    expected = sum(
        f.stat().st_size for f in (Path(dir) / "chunks").glob("*") if f.is_file()
    )
    assert actual == expected


@pytest.mark.filterwarnings("ignore:datetime.datetime.utcnow")
async def test_chunk_storage_by_prefix_async(any_spec_version: int | None) -> None:
    """Test the memory-efficient by_prefix async version"""

    repo = await ic.Repository.create_async(
        storage=ic.in_memory_storage(),
        config=ic.RepositoryConfig(inline_chunk_threshold_bytes=0),
        spec_version=any_spec_version,
    )
    session = await repo.writable_session_async("main")
    store = session.store

    group = zarr.group(store=store, overwrite=True)
    array = group.create_array(
        "array",
        shape=(100),
        chunks=(1,),
        dtype="i4",
        compressors=None,
    )

    array[:] = 42
    await session.commit_async("commit 1")

    # Test the by_prefix method
    stats = await repo.chunk_storage_stats_by_prefix_async()
    assert stats.native_bytes == 100 * 4
    # Virtual and inline should be 0 since we're only listing storage
    assert stats.virtual_bytes == 0
    assert stats.inlined_bytes == 0


@pytest.mark.parametrize(
    "dir", ["./tests/data/test-repo-v2", "./tests/data/test-repo-v1"]
)
async def test_chunk_storage_by_prefix_on_filesystem(dir: str) -> None:
    """Test that by_prefix method gives same results as listing filesystem"""
    repo = await ic.Repository.open_async(
        storage=ic.local_filesystem_storage(dir),
    )
    stats = await repo.chunk_storage_stats_by_prefix_async()
    expected = sum(
        f.stat().st_size for f in (Path(dir) / "chunks").glob("*") if f.is_file()
    )
    assert stats.native_bytes == expected
    assert stats.virtual_bytes == 0
    assert stats.inlined_bytes == 0
