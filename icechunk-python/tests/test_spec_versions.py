import pytest

import icechunk as ic
import zarr


def test_create_repo_with_spec_version_2():
    storage = ic.in_memory_storage()
    ic.Repository.create(storage, spec_version=2)
    assert ic.Repository.open(storage).spec_version() == 2


def test_create_repo_with_spec_version_1():
    storage = ic.in_memory_storage()
    ic.Repository.create(storage, spec_version=1)
    assert ic.Repository.open(storage).spec_version() == 1


async def test_can_commit_with_spec_version_1():
    storage = ic.in_memory_storage()
    repo = ic.Repository.create(storage, spec_version=1)
    session = repo.writable_session("main")
    group = zarr.group(store=session.store, overwrite=True)
    group.create_group("foo")
    session.commit("make group")

    repo = ic.Repository.open(storage)
    assert len(list(repo.ancestry(branch="main"))) == 2
    session = repo.readonly_session("main")
    assert {k async for k in session.store.list()} == {"zarr.json", "foo/zarr.json"}


def test_cannot_amend_with_spec_version_1():
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


def test_can_create_branch_on_v1():
    storage = ic.in_memory_storage()
    repo = ic.Repository.create(storage, spec_version=1)
    repo.create_branch("foo", "1CECHNKREP0F1RSTCMT0")
    assert repo.list_branches() == {"main", "foo"}


def test_can_create_tag_on_v1():
    storage = ic.in_memory_storage()
    repo = ic.Repository.create(storage, spec_version=1)
    repo.create_tag("foo", "1CECHNKREP0F1RSTCMT0")
    assert repo.list_tags() == {"foo"}


def test_can_reset_branch_on_v1():
    storage = ic.in_memory_storage()
    repo = ic.Repository.create(storage, spec_version=1)
    session = repo.writable_session("main")
    group = zarr.group(store=session.store, overwrite=True)
    group.create_group("foo")
    commit_id = session.commit("make group")

    repo.reset_branch("main", "1CECHNKREP0F1RSTCMT0", from_snapshot_id=commit_id)
    assert repo.lookup_branch("main") == "1CECHNKREP0F1RSTCMT0"


def test_can_delete_branch_on_v1():
    storage = ic.in_memory_storage()
    repo = ic.Repository.create(storage, spec_version=1)
    repo.create_branch("foo", "1CECHNKREP0F1RSTCMT0")
    assert repo.list_branches() == {"main", "foo"}
    repo.delete_branch("foo")
    assert repo.list_branches() == {"main"}


def test_can_delete_tag_on_v1():
    storage = ic.in_memory_storage()
    repo = ic.Repository.create(storage, spec_version=1)
    repo.create_tag("foo", "1CECHNKREP0F1RSTCMT0")
    assert repo.list_tags() == {"foo"}
    repo.delete_tag("foo")
    assert repo.list_tags() == set()
