from icechunk import IcechunkStore
from tests.conftest import parse_repo
from zarr.storage._common import make_store_path


async def test_make_store_path(any_spec_version: int | None) -> None:
    # Memory store
    repo = parse_repo(
        "memory",
        path="",
        spec_version=any_spec_version,
    )
    session = repo.writable_session("main")
    store = session.store
    store_path = await make_store_path(store)
    assert isinstance(store_path.store, IcechunkStore)
