import shutil
from pathlib import Path

import icechunk as ic


def test_migration_1_to_2_dry_run(tmpdir: Path) -> None:
    shutil.copytree("tests/data/test-repo", tmpdir, dirs_exist_ok=True)
    storage = ic.local_filesystem_storage(str(tmpdir))
    repo = ic.Repository.open(storage)
    assert repo.spec_version() == 1

    # running by default with dry_run=True
    ic.upgrade_icechunk_repository(repo)

    repo = ic.Repository.open(storage)
    assert repo.spec_version() == 1

    # running with explicit dry_run=True
    ic.upgrade_icechunk_repository(repo, dry_run=True)
    repo = ic.Repository.open(storage)
    assert repo.spec_version() == 1


def test_migration_1_to_2(tmpdir: Path) -> None:
    shutil.copytree("tests/data/test-repo", tmpdir, dirs_exist_ok=True)
    storage = ic.local_filesystem_storage(str(tmpdir))
    repo = ic.Repository.open(storage)
    assert repo.spec_version() == 1

    ic.upgrade_icechunk_repository(repo, dry_run=False)
    repo = ic.Repository.open(storage)
    assert repo.spec_version() == 2
