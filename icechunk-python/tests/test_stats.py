from pathlib import Path

import pytest

import icechunk as ic
import zarr


@pytest.mark.filterwarnings("ignore:datetime.datetime.utcnow")
def test_total_chunks_storage() -> None:
    """We only test the interface, more detailed test is done in Rust"""

    repo = ic.Repository.create(
        storage=ic.in_memory_storage(),
        config=ic.RepositoryConfig(inline_chunk_threshold_bytes=0),
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
async def test_total_chunks_storage_async() -> None:
    """Test the async version of total_chunks_storage"""

    repo = await ic.Repository.create_async(
        storage=ic.in_memory_storage(),
        config=ic.RepositoryConfig(inline_chunk_threshold_bytes=0),
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


@pytest.mark.parametrize("dir", ["./tests/data/test-repo"])
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
