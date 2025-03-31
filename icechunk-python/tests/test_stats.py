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

    assert repo.total_chunks_storage() == 100 * 4
