import os
import pickle
import sys
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path

import numpy as np
import pytest

import zarr
from icechunk import (
    ErrorKind,
    FilesystemStorage,
    FormatError,
    Repository,
    Storage,
    StorageError,
    filesystem_storage,
    in_memory_storage,
    local_filesystem_storage,
)

pytestmark = pytest.mark.skipif(
    sys.platform == "win32", reason="FilesystemStorage is Unix-only"
)


def write_array(repo: Repository) -> None:
    session = repo.writable_session("main")
    group = zarr.group(session.store)
    array = group.create_array("arr", shape=(4,), chunks=(2,), dtype="i4")
    array[:] = np.arange(4)
    session.commit("write array")


def read_array(repo: Repository) -> np.ndarray:
    session = repo.readonly_session("main")
    group = zarr.open_group(session.store, mode="r")
    array = group["arr"]
    assert isinstance(array, zarr.Array)
    return np.asarray(array[:])


def test_create_commit_reopen(tmp_path: Path) -> None:
    repo = Repository.create(filesystem_storage(str(tmp_path)))
    write_array(repo)

    reopened = Repository.open(filesystem_storage(str(tmp_path)))
    np.testing.assert_array_equal(read_array(reopened), np.arange(4))


def test_settings(tmp_path: Path) -> None:
    storage = Storage.new_filesystem(str(tmp_path), fsync=False)
    assert isinstance(storage, FilesystemStorage)
    repo = Repository.create(storage)
    write_array(repo)
    np.testing.assert_array_equal(read_array(repo), np.arange(4))


def test_returns_filesystem_storage(tmp_path: Path) -> None:
    storage = filesystem_storage(str(tmp_path))
    assert isinstance(storage, FilesystemStorage)
    assert isinstance(storage, Storage)


def test_repr(tmp_path: Path) -> None:
    text = repr(filesystem_storage(str(tmp_path)))
    assert "type: filesystem" in text
    assert str(tmp_path) in text


def test_refuses_ic1(tmp_path: Path) -> None:
    with pytest.raises(FormatError, match="local_filesystem_storage") as exc_info:
        Repository.create(filesystem_storage(str(tmp_path)), spec_version=1)
    assert exc_info.value.kind == ErrorKind.UNSUPPORTED_SPEC_VERSION


def test_gc_deletes_leftovers(tmp_path: Path) -> None:
    repo = Repository.create(filesystem_storage(str(tmp_path)))
    write_array(repo)

    leftover = tmp_path / "repo.d" / f"stage.{uuid.uuid4()}"
    leftover.write_bytes(b"abandoned")
    two_days_ago = time.time() - 2 * 24 * 3600
    os.utime(leftover, (two_days_ago, two_days_ago))

    summary = repo.garbage_collect(datetime.now(UTC))
    assert summary.storage_leftovers_deleted == 1
    assert not leftover.exists()


def test_migrate_legacy_repo(tmp_path: Path) -> None:
    write_array(Repository.create(local_filesystem_storage(str(tmp_path))))

    storage = filesystem_storage(str(tmp_path))
    with pytest.raises(StorageError, match="migrate_legacy_repo"):
        Repository.open(storage)

    assert storage.migrate_legacy_repo()
    np.testing.assert_array_equal(read_array(Repository.open(storage)), np.arange(4))
    assert not storage.migrate_legacy_repo()


async def test_migrate_legacy_repo_async(tmp_path: Path) -> None:
    write_array(Repository.create(local_filesystem_storage(str(tmp_path))))

    storage = filesystem_storage(str(tmp_path))
    with pytest.raises(StorageError, match="migrate_legacy_repo"):
        await Repository.open_async(storage)

    assert await storage.migrate_legacy_repo_async()
    repo = await Repository.open_async(storage)
    np.testing.assert_array_equal(read_array(repo), np.arange(4))
    assert not await storage.migrate_legacy_repo_async()


def test_migrate_filesystem_repo_changes_nothing(tmp_path: Path) -> None:
    storage = filesystem_storage(str(tmp_path))
    assert not storage.migrate_legacy_repo()
    write_array(Repository.create(storage))
    assert not storage.migrate_legacy_repo()
    np.testing.assert_array_equal(read_array(Repository.open(storage)), np.arange(4))


def test_freeze_repo(tmp_path: Path) -> None:
    storage = filesystem_storage(str(tmp_path))
    repo = Repository.create(storage)

    assert storage.freeze_repo()
    assert not storage.freeze_repo()
    with pytest.raises(StorageError, match="frozen"):
        write_array(repo)

    assert storage.unfreeze_repo()
    assert not storage.unfreeze_repo()
    repo = Repository.open(storage)
    write_array(repo)
    np.testing.assert_array_equal(read_array(repo), np.arange(4))


async def test_freeze_repo_async(tmp_path: Path) -> None:
    storage = filesystem_storage(str(tmp_path))
    repo = await Repository.create_async(storage)

    assert await storage.freeze_repo_async()
    assert not await storage.freeze_repo_async()
    with pytest.raises(StorageError, match="frozen"):
        write_array(repo)

    assert await storage.unfreeze_repo_async()
    assert not await storage.unfreeze_repo_async()
    repo = await Repository.open_async(storage)
    write_array(repo)
    np.testing.assert_array_equal(read_array(repo), np.arange(4))


def test_local_filesystem_storage_refuses_filesystem_format(tmp_path: Path) -> None:
    write_array(Repository.create(filesystem_storage(str(tmp_path))))

    with pytest.raises(FormatError, match="filesystem_storage") as exc_info:
        Repository.open(local_filesystem_storage(str(tmp_path)))
    assert exc_info.value.kind == ErrorKind.INVALID_FILE_FORMAT


def test_gc_in_memory_has_no_leftovers() -> None:
    repo = Repository.create(in_memory_storage())
    write_array(repo)
    summary = repo.garbage_collect(datetime.now(UTC))
    assert summary.storage_leftovers_deleted == 0


def test_pickle_repository(tmp_path: Path) -> None:
    repo = Repository.create(filesystem_storage(str(tmp_path)))
    write_array(repo)

    roundtripped = pickle.loads(pickle.dumps(repo))
    assert roundtripped.list_branches() == repo.list_branches()
    np.testing.assert_array_equal(read_array(roundtripped), np.arange(4))
