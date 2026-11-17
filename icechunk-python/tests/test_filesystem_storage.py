import hashlib
import os
import pickle
import shutil
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
    ReadOnlyError,
    RepoAvailability,
    Repository,
    RepositoryNotFoundError,
    RepoStatus,
    SessionStateError,
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


def overwrite_array(repo: Repository, value: int) -> None:
    session = repo.writable_session("main")
    array = zarr.open_array(session.store, path="arr", mode="a")
    array[:] = value
    session.commit(f"write {value}")


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


def test_convert_to_filesystem_layout(tmp_path: Path) -> None:
    write_array(Repository.create(local_filesystem_storage(str(tmp_path))))

    storage = filesystem_storage(str(tmp_path))
    with pytest.raises(StorageError, match="convert_to_filesystem_layout"):
        Repository.open(storage)

    assert storage.convert_to_filesystem_layout()
    np.testing.assert_array_equal(read_array(Repository.open(storage)), np.arange(4))
    assert not storage.convert_to_filesystem_layout()


async def test_convert_to_filesystem_layout_async(tmp_path: Path) -> None:
    write_array(Repository.create(local_filesystem_storage(str(tmp_path))))

    storage = filesystem_storage(str(tmp_path))
    with pytest.raises(StorageError, match="convert_to_filesystem_layout"):
        await Repository.open_async(storage)

    assert await storage.convert_to_filesystem_layout_async()
    repo = await Repository.open_async(storage)
    np.testing.assert_array_equal(read_array(repo), np.arange(4))
    assert not await storage.convert_to_filesystem_layout_async()


def test_convert_filesystem_repo_changes_nothing(tmp_path: Path) -> None:
    storage = filesystem_storage(str(tmp_path))
    with pytest.raises(StorageError) as exc_info:
        storage.convert_to_filesystem_layout()
    assert exc_info.value.kind == ErrorKind.OBJECT_NOT_FOUND
    write_array(Repository.create(storage))
    assert not storage.convert_to_filesystem_layout()
    np.testing.assert_array_equal(read_array(Repository.open(storage)), np.arange(4))


def test_convert_to_object_store_layout(tmp_path: Path) -> None:
    storage = filesystem_storage(str(tmp_path))
    write_array(Repository.create(storage))

    assert storage.convert_to_object_store_layout()
    assert not (tmp_path / "repo.d").exists()
    assert not storage.convert_to_object_store_layout()

    object_store_repo = Repository.open(local_filesystem_storage(str(tmp_path)))
    np.testing.assert_array_equal(read_array(object_store_repo), np.arange(4))
    object_store_repo.create_branch("dev", object_store_repo.lookup_branch("main"))

    assert storage.convert_to_filesystem_layout()
    repo = Repository.open(storage)
    assert repo.list_branches() == {"main", "dev"}


async def test_convert_to_object_store_layout_async(tmp_path: Path) -> None:
    storage = filesystem_storage(str(tmp_path))
    write_array(await Repository.create_async(storage))

    assert await storage.convert_to_object_store_layout_async()
    assert not await storage.convert_to_object_store_layout_async()
    repo = await Repository.open_async(local_filesystem_storage(str(tmp_path)))
    np.testing.assert_array_equal(read_array(repo), np.arange(4))


def current_version(root: Path) -> Path:
    """The version file the marker of `repo.d` names."""
    (marker,) = [
        p.name for p in (root / "repo.d").iterdir() if p.name.startswith("latest.")
    ]
    _, _, n, h = marker.split(".")
    return root / "repo.d" / f"{n}.{h}"


def plant_current_version(root: Path, repo_info: bytes) -> None:
    """Makes `repo_info` the current version of `repo.d` with plain file
    operations, as a conversion to the object store layout interrupted after its
    change leaves it."""
    version = current_version(root)
    n = int(version.name.split(".")[0]) + 1
    h = hashlib.sha256(repo_info).hexdigest()
    (root / "repo.d" / f"{n}.{h}").write_bytes(repo_info)
    (marker,) = [p for p in (root / "repo.d").iterdir() if p.name.startswith("latest.")]
    marker.rename(root / "repo.d" / f"latest.{999_999_999_999_999 - n:015d}.{n}.{h}")


def object_store_repo_info(root: Path, tmp_path: Path) -> bytes:
    """The repo info `convert_to_object_store_layout` makes from the repository
    at `root`, converting a copy."""
    copy = tmp_path / "object-store-copy"
    shutil.copytree(root, copy)
    assert filesystem_storage(str(copy)).convert_to_object_store_layout()
    return (copy / "repo").read_bytes()


def test_an_interrupted_conversion_to_the_object_store_layout_refuses_writes_until_rerun(
    tmp_path: Path,
) -> None:
    root = tmp_path / "repo-root"
    storage = filesystem_storage(str(root))
    write_array(Repository.create(storage))
    converted = object_store_repo_info(root, tmp_path)
    plant_current_version(root, converted)

    repo = Repository.open(filesystem_storage(str(root)))
    np.testing.assert_array_equal(read_array(repo), np.arange(4))
    with pytest.raises(SessionStateError) as exc_info:
        repo.create_branch("dev", repo.lookup_branch("main"))
    assert exc_info.value.kind == ErrorKind.LAYOUT_CONVERSION_IN_PROGRESS
    assert "convert_to_object_store_layout" in str(exc_info.value)
    with pytest.raises(SessionStateError) as exc_info:
        repo.garbage_collect(datetime.now(UTC))
    assert exc_info.value.kind == ErrorKind.LAYOUT_CONVERSION_IN_PROGRESS

    # the rerun finishes the conversion from the change that landed
    assert storage.convert_to_object_store_layout()
    assert not (root / "repo.d").exists()
    assert (root / "repo").read_bytes() == converted
    assert not storage.convert_to_object_store_layout()
    object_store_repo = Repository.open(local_filesystem_storage(str(root)))
    np.testing.assert_array_equal(read_array(object_store_repo), np.arange(4))
    object_store_repo.create_branch("dev", object_store_repo.lookup_branch("main"))
    assert object_store_repo.list_branches() == {"main", "dev"}


@pytest.mark.parametrize("copy", ["in_place", "copy", "copy_without_repo_d"])
def test_local_filesystem_storage_cant_open(tmp_path: Path, copy: str) -> None:
    """Other backends can't open a repository in the filesystem layout, a copy
    of it, or a copy without `repo.d` (as on other storage)."""
    root = tmp_path / "repo-root"
    write_array(Repository.create(filesystem_storage(str(root))))
    if copy != "in_place":
        target = tmp_path / "copy"
        ignore = (
            shutil.ignore_patterns("repo.d") if copy == "copy_without_repo_d" else None
        )
        shutil.copytree(root, target, ignore=ignore)
        root = target

    assert not (root / "repo").exists()
    storage = local_filesystem_storage(str(root))
    assert not Repository.exists(storage)
    with pytest.raises(RepositoryNotFoundError) as exc_info:
        Repository.open(storage)
    assert exc_info.value.kind == ErrorKind.REPOSITORY_NOT_FOUND


def set_status(repo: Repository, availability: RepoAvailability) -> None:
    repo.set_status(RepoStatus(availability))


def test_a_consistent_copy(tmp_path: Path) -> None:
    """`ReadOnly`, copy every entry, check the copy, then `Online` again."""
    root = tmp_path / "repo-root"
    repo = Repository.create(filesystem_storage(str(root)))
    write_array(repo)
    set_status(repo, RepoAvailability.read_only)
    with pytest.raises(ReadOnlyError) as exc_info:
        repo.writable_session("main")
    assert exc_info.value.kind == ErrorKind.READ_ONLY_REPOSITORY

    copy = tmp_path / "copy"
    shutil.copytree(root, copy)
    (marker,) = [
        p.name for p in (copy / "repo.d").iterdir() if p.name.startswith("latest.")
    ]
    _, _, n, h = marker.split(".")
    assert hashlib.sha256((copy / "repo.d" / f"{n}.{h}").read_bytes()).hexdigest() == h
    assert not (copy / "repo").exists()
    copied = Repository.open(filesystem_storage(str(copy)))
    assert copied.get_status().availability == RepoAvailability.read_only
    np.testing.assert_array_equal(read_array(copied), np.arange(4))

    set_status(repo, RepoAvailability.online)
    repo.create_branch("dev", repo.lookup_branch("main"))
    set_status(copied, RepoAvailability.online)
    overwrite_array(copied, 7)
    np.testing.assert_array_equal(read_array(copied), np.full(4, 7))
    np.testing.assert_array_equal(read_array(repo), np.arange(4))


def test_make_a_writable_copy_on_other_storage(tmp_path: Path) -> None:
    """`ReadOnly`, convert to the object store layout, copy, convert the source
    back, then `Online` on both: the copy takes commits through
    `local_filesystem_storage`."""
    root = tmp_path / "repo-root"
    storage = filesystem_storage(str(root))
    repo = Repository.create(storage)
    write_array(repo)
    set_status(repo, RepoAvailability.read_only)
    assert storage.convert_to_object_store_layout()
    copy = tmp_path / "copy"
    shutil.copytree(root, copy)
    assert storage.convert_to_filesystem_layout()

    source = Repository.open(storage)
    assert source.get_status().availability == RepoAvailability.read_only
    set_status(source, RepoAvailability.online)
    source.create_branch("dev", source.lookup_branch("main"))

    copied = Repository.open(local_filesystem_storage(str(copy)))
    assert copied.get_status().availability == RepoAvailability.read_only
    set_status(copied, RepoAvailability.online)
    overwrite_array(copied, 7)
    np.testing.assert_array_equal(read_array(copied), np.full(4, 7))
    np.testing.assert_array_equal(read_array(source), np.arange(4))
    assert copied.list_branches() == {"main"}


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
