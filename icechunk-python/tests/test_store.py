import json

import numpy as np

import zarr
from tests.conftest import parse_repo
from zarr.core.buffer import default_buffer_prototype

rng = np.random.default_rng(seed=12345)


async def test_store_clear_metadata_list() -> None:
    repo = parse_repo("memory", "test")
    session = repo.writable_session("main")
    store = session.store

    zarr.group(store=store)
    session.commit("created node /")

    session = repo.writable_session("main")
    store = session.store
    await store.clear()
    zarr.group(store=store)
    assert len([_ async for _ in store.list_prefix("/")]) == 1


async def test_store_clear_chunk_list() -> None:
    repo = parse_repo("memory", "test")
    session = repo.writable_session("main")
    store = session.store

    array_kwargs = dict(
        name="0", shape=(1, 3, 5, 1), chunks=(1, 3, 2, 1), fill_value=-1, dtype=np.int64
    )
    group = zarr.group(store=store)
    group.create_array(**array_kwargs)
    session.commit("created node /")

    session = repo.writable_session("main")
    store = session.store
    await store.clear()

    group = zarr.group(store=store)
    array = group.create_array(**array_kwargs, overwrite=True)
    assert len([_ async for _ in store.list_prefix("/")]) == 2
    array[:] = rng.integers(
        low=0,
        high=1234,
        size=array_kwargs["shape"],
        dtype=array_kwargs["dtype"],  # type: ignore [call-overload]
    )
    keys = [_ async for _ in store.list_prefix("/")]
    assert len(keys) == 2 + 3, keys


async def test_support_dimension_names_null() -> None:
    repo = parse_repo("memory", "test")
    session = repo.writable_session("main")
    store = session.store

    root = zarr.group(store=store)
    # no dimensions names!
    root.create_array(
        name="0", shape=(1, 3, 5, 1), chunks=(1, 3, 2, 1), fill_value=-1, dtype=np.int64
    )
    meta = json.loads(
        (await store.get("0/zarr.json", prototype=default_buffer_prototype())).to_bytes()
    )
    assert "dimension_names" not in meta


def test_doesnt_support_consolidated_metadata() -> None:
    repo = parse_repo("memory", "test")
    session = repo.writable_session("main")
    store = session.store
    assert not store.supports_consolidated_metadata
