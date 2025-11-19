import icechunk as ic


def test_create_repo_with_spec_version_2():
    storage = ic.in_memory_storage()
    ic.Repository.create(storage, spec_version=2)
    assert ic.Repository.open(storage).spec_version() == 2


def test_create_repo_with_spec_version_1():
    storage = ic.in_memory_storage()
    ic.Repository.create(storage, spec_version=1)
    assert ic.Repository.open(storage).spec_version() == 1
