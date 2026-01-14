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
