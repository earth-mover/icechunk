from icechunk import IcechunkStore

from zarr.store.common import make_store_path

from ...conftest import parse_store


async def test_make_store_path() -> None:
    # Memory store
    store = await parse_store("memory", path="")
    store_path = await make_store_path(store)
    assert isinstance(store_path.store, IcechunkStore)
