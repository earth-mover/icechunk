import pytest

import icechunk as ic
import zarr


def test_create_repo_with_spec_version_2() -> None:
    storage = ic.in_memory_storage()
    ic.Repository.create(storage, spec_version=2)
    assert ic.Repository.open(storage).spec_version == 2


def test_create_repo_with_spec_version_1() -> None:
    storage = ic.in_memory_storage()
    ic.Repository.create(storage, spec_version=1)
    assert ic.Repository.open(storage).spec_version == 1


def test_cannot_amend_with_spec_version_1() -> None:
    storage = ic.in_memory_storage()
    repo = ic.Repository.create(storage, spec_version=1)
    session = repo.writable_session("main")
    group = zarr.group(store=session.store, overwrite=True)
    group.create_group("foo")
    session.commit("make group")

    session = repo.writable_session("main")
    group = zarr.group(store=session.store, overwrite=False)
    group.create_group("bar")
    with pytest.raises(ic.IcechunkError, match="upgrade"):
        session.amend("amend attempt")


def test_cannot_get_metadata_with_version_1() -> None:
    storage = ic.in_memory_storage()
    repo = ic.Repository.create(storage, spec_version=1)
    with pytest.raises(ic.IcechunkError, match="upgrade"):
        repo.get_metadata()


def test_cannot_set_metadata_with_version_1() -> None:
    storage = ic.in_memory_storage()
    repo = ic.Repository.create(storage, spec_version=1)
    with pytest.raises(ic.IcechunkError, match="upgrade"):
        repo.set_metadata({"foo": "bar"})


def test_cannot_update_metadata_with_version_1() -> None:
    storage = ic.in_memory_storage()
    repo = ic.Repository.create(storage, spec_version=1)
    with pytest.raises(ic.IcechunkError, match="upgrade"):
        repo.update_metadata({"foo": "bar"})


def test_fetch_spec_version_v1() -> None:
    storage = ic.in_memory_storage()
    ic.Repository.create(storage, spec_version=1)
    assert ic.Repository.fetch_spec_version(storage) == 1


def test_fetch_spec_version_v2() -> None:
    storage = ic.in_memory_storage()
    ic.Repository.create(storage, spec_version=2)
    assert ic.Repository.fetch_spec_version(storage) == 2


def test_fetch_spec_version_no_repo() -> None:
    storage = ic.in_memory_storage()
    assert ic.Repository.fetch_spec_version(storage) is None


@pytest.mark.asyncio
async def test_fetch_spec_version_async_v1() -> None:
    storage = ic.in_memory_storage()
    await ic.Repository.create_async(storage, spec_version=1)
    assert await ic.Repository.fetch_spec_version_async(storage) == 1


@pytest.mark.asyncio
async def test_fetch_spec_version_async_v2() -> None:
    storage = ic.in_memory_storage()
    await ic.Repository.create_async(storage, spec_version=2)
    assert await ic.Repository.fetch_spec_version_async(storage) == 2


@pytest.mark.asyncio
async def test_fetch_spec_version_async_no_repo() -> None:
    storage = ic.in_memory_storage()
    assert await ic.Repository.fetch_spec_version_async(storage) is None
