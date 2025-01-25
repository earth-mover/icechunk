from icechunk import IcechunkStore
from tests.conftest import parse_repo
from zarr.storage._common import make_store_path


async def test_make_store_path() -> None:
    # Memory store
    repo = parse_repo("memory", path="")
    session = repo.writable_session("main")
    store = session.store
    store_path = await make_store_path(store)
    assert isinstance(store_path.store, IcechunkStore)
