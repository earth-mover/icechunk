import zarr
from tests.conftest import parse_repo


async def test_store_clear() -> None:
    repo = parse_repo("memory", "test")
    session = repo.writable_session("main")
    store = session.store()

    zarr.group(store=store)
    session.commit("created node /")

    session = repo.writable_session("main")
    store = session.store()
    await store.clear()
    zarr.group(store=store)
    assert len([_ async for _ in store.list_prefix("/")]) == 1
