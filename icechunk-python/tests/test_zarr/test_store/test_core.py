from icechunk import IcechunkStore
from tests.conftest import parse_repo_async
from zarr.storage._common import make_store_path


async def test_make_store_path(any_spec_version: int | None) -> None:
    # Memory store
    repo = await parse_repo_async(
        "memory",
        path="",
        spec_version=any_spec_version,
    )
    session = await repo.writable_session_async("main")
    store = session.store
    store_path = await make_store_path(store)
    assert isinstance(store_path.store, IcechunkStore)
