import hashlib
import os
import pickle
import shutil
import sys
import time
import uuid
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path

import numpy as np
import pytest

import zarr
from icechunk import (
    CommitDurableButRepositoryForkedError,
    ErrorKind,
    FilesystemStorage,
    FormatError,
    PublishOutcome,
    ReadOnlyError,
    RepoAvailability,
    Repository,
    RepositoryForkedError,
    RepoStatus,
    SessionStateError,
    Storage,
    StorageError,
    UpdateDurableButRepositoryForkedError,
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


def test_publish_mirror_repairs_a_lagging_or_missing_mirror(tmp_path: Path) -> None:
    storage = filesystem_storage(str(tmp_path))
    repo = Repository.create(storage)
    mirror = tmp_path / "repo"
    lagging = mirror.read_bytes()
    write_array(repo)

    mirror.write_bytes(lagging)
    outcome = storage.publish_mirror()
    assert isinstance(outcome, PublishOutcome)
    assert outcome.current
    assert outcome.left is None
    assert mirror.read_bytes() == current_version(tmp_path).read_bytes()

    mirror.unlink()
    assert storage.publish_mirror().current
    assert mirror.read_bytes() == current_version(tmp_path).read_bytes()


async def test_publish_mirror_async(tmp_path: Path) -> None:
    storage = filesystem_storage(str(tmp_path))
    await Repository.create_async(storage)
    (tmp_path / "repo").unlink()
    outcome = await storage.publish_mirror_async()
    assert outcome.current
    assert outcome.left is None
    assert (tmp_path / "repo").read_bytes() == current_version(tmp_path).read_bytes()


def fork(root: Path, other: Path) -> None:
    """Writes over the mirror the repo info of another repository, in the object
    store layout."""
    Repository.create(local_filesystem_storage(str(other)))
    (root / "repo").write_bytes((other / "repo").read_bytes())


def test_publish_mirror_refuses_a_fork(tmp_path: Path) -> None:
    root = tmp_path / "repo-root"
    storage = filesystem_storage(str(root))
    Repository.create(storage)
    fork(root, tmp_path / "other")

    with pytest.raises(RepositoryForkedError) as exc_info:
        storage.publish_mirror()
    assert type(exc_info.value) is RepositoryForkedError
    assert exc_info.value.kind == ErrorKind.REPOSITORY_FORKED


def test_publish_mirror_refuses_an_unreadable_mirror(tmp_path: Path) -> None:
    storage = filesystem_storage(str(tmp_path))
    Repository.create(storage)
    (tmp_path / "repo").write_bytes(b"not a repo info")

    with pytest.raises(StorageError) as exc_info:
        storage.publish_mirror()
    assert exc_info.value.kind == ErrorKind.MIRROR_UNREADABLE
    assert (tmp_path / "repo").read_bytes() == b"not a repo info"


def test_a_change_that_finds_a_fork_is_durable(tmp_path: Path) -> None:
    root = tmp_path / "repo-root"
    repo = Repository.create(filesystem_storage(str(root)))
    fork(root, tmp_path / "other")

    session = repo.writable_session("main")
    zarr.group(session.store)
    with pytest.raises(UpdateDurableButRepositoryForkedError) as commit_info:
        session.commit("lands, then finds the fork")
    err = commit_info.value
    assert isinstance(err, CommitDurableButRepositoryForkedError)
    assert isinstance(err, RepositoryForkedError)
    assert err.kind == ErrorKind.COMMIT_DURABLE_BUT_REPOSITORY_FORKED
    assert err.snapshot_id == repo.lookup_branch("main")
    assert err.snapshot_id in str(err)
    assert "is durable" in str(err)
    assert "must not be retried" in str(err)
    assert "forked" in str(err)
    roundtripped = pickle.loads(pickle.dumps(err))
    assert type(roundtripped) is CommitDurableButRepositoryForkedError
    assert roundtripped.snapshot_id == err.snapshot_id
    assert roundtripped.kind == err.kind
    assert roundtripped.message == err.message
    # the commit is durable: the session can't commit it again
    assert session.read_only
    with pytest.raises(SessionStateError) as retry_info:
        session.commit("retry")
    assert retry_info.value.kind == ErrorKind.COMMIT_NOT_ALLOWED

    with pytest.raises(UpdateDurableButRepositoryForkedError) as branch_info:
        repo.create_branch("dev", err.snapshot_id)
    branch_err = branch_info.value
    assert type(branch_err) is UpdateDurableButRepositoryForkedError
    assert branch_err.kind == ErrorKind.UPDATE_DURABLE_BUT_REPOSITORY_FORKED
    assert "durable" in str(branch_err)
    assert type(pickle.loads(pickle.dumps(branch_err))) is type(branch_err)
    assert repo.list_branches() == {"main", "dev"}


def test_a_flush_that_finds_a_fork_is_durable(tmp_path: Path) -> None:
    root = tmp_path / "repo-root"
    repo = Repository.create(filesystem_storage(str(root)))
    tip = repo.lookup_branch("main")
    fork(root, tmp_path / "other")

    session = repo.writable_session("main")
    zarr.group(session.store)
    with pytest.raises(CommitDurableButRepositoryForkedError) as flush_info:
        session.flush("lands, then finds the fork")
    err = flush_info.value
    assert err.kind == ErrorKind.COMMIT_DURABLE_BUT_REPOSITORY_FORKED
    assert f"snapshot {err.snapshot_id} is durable" in str(err)
    assert [snap.id for snap in repo.ancestry(snapshot_id=err.snapshot_id)] == [
        err.snapshot_id,
        tip,
    ]
    assert repo.lookup_branch("main") == tip
    # the snapshot is durable: the session is done, as after a flush
    assert session.read_only
    assert session.snapshot_id == err.snapshot_id
    assert not session.has_uncommitted_changes
    with pytest.raises(SessionStateError) as retry_info:
        session.commit("retry")
    assert retry_info.value.kind == ErrorKind.COMMIT_NOT_ALLOWED


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
        storage.publish_mirror()
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


def file_digests(root: Path) -> dict[str, str]:
    return {
        str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
        for p in root.rglob("*")
        if p.is_file()
    }


@pytest.mark.parametrize("copy", ["in_place", "copy", "copy_without_repo_d"])
def test_local_filesystem_storage_reads_the_mirror_and_cannot_write(
    tmp_path: Path, copy: str
) -> None:
    """Other backends read a repository in the filesystem layout, a copy of
    it, or a copy without `repo.d` (as on other storage) through the mirror,
    and refuse every modification."""
    root = tmp_path / "repo-root"
    created = Repository.create(filesystem_storage(str(root)))
    write_array(created)
    created.create_tag("v1", created.lookup_branch("main"))
    if copy != "in_place":
        target = tmp_path / "copy"
        ignore = (
            shutil.ignore_patterns("repo.d") if copy == "copy_without_repo_d" else None
        )
        shutil.copytree(root, target, ignore=ignore)
        root = target

    repo = Repository.open(local_filesystem_storage(str(root)))
    assert repo.list_branches() == created.list_branches()
    assert repo.list_tags() == {"v1"}
    assert [s.id for s in repo.ancestry(branch="main")] == [
        s.id for s in created.ancestry(branch="main")
    ]
    np.testing.assert_array_equal(read_array(repo), np.arange(4))

    main = repo.lookup_branch("main")
    modifications: dict[str, Callable[[], object]] = {
        "writable_session": lambda: repo.writable_session("main"),
        "create_branch": lambda: repo.create_branch("dev", main),
        "delete_tag": lambda: repo.delete_tag("v1"),
        "set_status": lambda: repo.set_status(RepoStatus(RepoAvailability.read_only)),
        "garbage_collect": lambda: repo.garbage_collect(datetime.now(UTC)),
    }
    before = file_digests(root)
    for name, modify in modifications.items():
        with pytest.raises(
            ReadOnlyError, match=r"filesystem-layout-v1.*Icechunk 2\.4\.0"
        ) as exc_info:
            modify()
        assert exc_info.value.kind == ErrorKind.MISSING_WRITER_FEATURES, name
    assert file_digests(root) == before


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
    assert hashlib.sha256((copy / "repo").read_bytes()).hexdigest() == h
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


def test_gc_refuses_a_fork(tmp_path: Path) -> None:
    """GC's own change lands, then its publication finds the fork: nothing is
    deleted and the fork is left alone."""
    root = tmp_path / "repo-root"
    repo = Repository.create(filesystem_storage(str(root)))
    write_array(repo)
    overwrite_array(repo, 7)
    repo.reset_branch("main", list(repo.ancestry(branch="main"))[1].id)
    fork_bytes = object_store_repo_info(root, tmp_path)
    (root / "repo").write_bytes(fork_bytes)
    # The chunks are inline: GC would delete from these directories.
    collected = ("manifests", "snapshots", "transactions")
    objects = {name: file_digests(root / name) for name in collected}
    assert all(objects.values())

    with pytest.raises(UpdateDurableButRepositoryForkedError) as exc_info:
        repo.garbage_collect(datetime.now(UTC))
    assert type(exc_info.value) is UpdateDurableButRepositoryForkedError
    assert {name: file_digests(root / name) for name in collected} == objects
    assert (root / "repo").read_bytes() == fork_bytes


def test_local_filesystem_storage_refuses_to_write_a_fork(tmp_path: Path) -> None:
    """While `repo.d` exists, `local_filesystem_storage` refuses every write of
    `repo` and GC, even when `repo` doesn't list the writer feature."""
    root = tmp_path / "repo-root"
    write_array(Repository.create(filesystem_storage(str(root))))
    fork_bytes = object_store_repo_info(root, tmp_path)
    (root / "repo").write_bytes(fork_bytes)

    repo = Repository.open(local_filesystem_storage(str(root)))
    np.testing.assert_array_equal(read_array(repo), np.arange(4))

    main = repo.lookup_branch("main")
    modifications: dict[str, Callable[[], object]] = {
        "commit": lambda: overwrite_array(repo, 7),
        "create_branch": lambda: repo.create_branch("dev", main),
        "set_status": lambda: set_status(repo, RepoAvailability.read_only),
        "garbage_collect": lambda: repo.garbage_collect(datetime.now(UTC)),
    }
    for name, modify in modifications.items():
        with pytest.raises(StorageError, match="local_filesystem_storage refuses"):
            modify()
        assert (root / "repo").read_bytes() == fork_bytes, name


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
