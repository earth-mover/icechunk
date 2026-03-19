import shutil
from pathlib import Path

import pytest

import icechunk as ic
from icechunk import SpecVersion


def test_migration_1_to_2_dry_run(tmpdir: Path) -> None:
    shutil.copytree("tests/data/test-repo-v1", tmpdir, dirs_exist_ok=True)
    storage = ic.local_filesystem_storage(str(tmpdir))
    repo = ic.Repository.open(storage)
    assert repo.spec_version == SpecVersion.v1dot0

    # running with explicit dry_run=True
    repo = ic.upgrade_icechunk_repository(repo, dry_run=True)
    assert repo.spec_version == SpecVersion.v1dot0


def test_migration_1_to_2(tmpdir: Path) -> None:
    shutil.copytree("tests/data/test-repo-v1", tmpdir, dirs_exist_ok=True)
    storage = ic.local_filesystem_storage(str(tmpdir))
    repo = ic.Repository.open(storage)
    assert repo.spec_version == SpecVersion.v1dot0

    repo = ic.upgrade_icechunk_repository(repo, dry_run=False)
    assert repo.spec_version == SpecVersion.v2dot0


def test_migration_invalidates_old_repo(tmpdir: Path) -> None:
    """After a non-dry-run migration, the old repo object should be invalidated (issue #1521)."""
    shutil.copytree("tests/data/test-repo-v1", tmpdir, dirs_exist_ok=True)
    storage = ic.local_filesystem_storage(str(tmpdir))
    old_repo = ic.Repository.open(storage)

    new_repo = ic.upgrade_icechunk_repository(old_repo, dry_run=False)
    assert new_repo.spec_version == SpecVersion.v2dot0

    with pytest.raises(RuntimeError, match="invalidated by upgrade_icechunk_repository"):
        old_repo.writable_session("main")
