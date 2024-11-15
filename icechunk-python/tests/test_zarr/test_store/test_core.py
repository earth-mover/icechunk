from icechunk import IcechunkStore
from tests.conftest import parse_store
from zarr.storage import make_store_path


async def test_make_store_path() -> None:
    # Memory store
    store = parse_store("memory", path="")
    store_path = await make_store_path(store)
    assert isinstance(store_path.store, IcechunkStore)
