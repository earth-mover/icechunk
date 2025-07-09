import json

import numpy as np

import zarr
from tests.conftest import parse_repo
from zarr.core.buffer import cpu, default_buffer_prototype

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
    value = await store.get("0/zarr.json", prototype=default_buffer_prototype())
    assert value
    meta = json.loads(value.to_bytes())
    assert "dimension_names" not in meta


def test_doesnt_support_consolidated_metadata() -> None:
    repo = parse_repo("memory", "test")
    session = repo.writable_session("main")
    store = session.store
    assert not store.supports_consolidated_metadata


async def test_with_readonly() -> None:
    repo = parse_repo("memory", "test")
    session = repo.readonly_session("main")
    store = session.store
    assert store.read_only

    session = repo.writable_session("main")
    store = session.store
    writer = store.with_read_only(read_only=False)
    assert not writer._is_open
    assert not writer.read_only

    root = zarr.group(store=store)
    root.create_array(name="foo", shape=(1,), chunks=(1,), dtype=np.int64)
    await writer.set("foo/c/0", cpu.Buffer.from_bytes(b"bar"))
    await writer.delete("foo/c/0")

    reader = store.with_read_only(read_only=True)
    assert reader.read_only


async def test_transaction() -> None:
    repo = parse_repo("memory", "test")
    cid1 = repo.lookup_branch("main")
    # TODO: test metadata, rebase_with, and rebase_tries kwargs
    with repo.transaction("main", message="initialize group") as store:
        assert not store.read_only
        root = zarr.group(store=store)
        root.attrs["foo"] = "bar"
    cid2 = repo.lookup_branch("main")
    assert cid1 != cid2, "Transaction did not commit changes"


async def test_transaction_failed_no_commit() -> None:
    repo = parse_repo("memory", "test")
    cid1 = repo.lookup_branch("main")
    try:
        with repo.transaction("main", message="initialize group") as store:
            assert not store.read_only
            root = zarr.group(store=store)
            root.attrs["foo"] = "bar"
            raise RuntimeError("Simulating an error to prevent commit")
    except RuntimeError:
        pass
    cid2 = repo.lookup_branch("main")
    assert cid1 == cid2, "Transaction committed changes despite error"
