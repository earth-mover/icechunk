"""Spec conformance tests.

Dedicated tests for spec-v2-1.md requirements that no other test in the
python suite verifies directly. Each test carries a tracey `ic[verify ...]`
annotation tying it to the requirement it checks.
"""

import hashlib
import re
from datetime import UTC, datetime
from pathlib import Path
from threading import Barrier, Thread
from typing import Any, cast

import pytest

import icechunk as ic
import zarr

# Crockford base 32: uppercase, no I, L, O, U. 12 bytes encode to 20 chars.
CROCKFORD_20 = re.compile(r"^[0-9A-HJKMNP-TV-Z]{20}$")
# Backup names: the `repo.` literal, a 14-digit reversed timestamp, 12 random
# bytes in Crockford base 32.
BACKUP_NAME = re.compile(r"^repo\.\d{14}\.[0-9A-HJKMNP-TV-Z]{20}$")


def make_repo(path: Path) -> ic.Repository:
    config = ic.RepositoryConfig.default()
    # force chunk files so commits write to every metadata prefix
    config.inline_chunk_threshold_bytes = 1
    return ic.Repository.create(
        storage=ic.local_filesystem_storage(str(path)), config=config
    )


def commit_value(repo: ic.Repository, value: int) -> str:
    """One commit writing `value` into a (creating-if-needed) array."""
    session = repo.writable_session("main")
    group = zarr.group(store=session.store, overwrite=False)
    if "data" not in group:
        group.create_array("data", shape=(4, 4), chunks=(2, 2), dtype="i4")
    array = cast("zarr.core.array.Array[Any]", group["data"])
    array[:, :] = value
    return session.commit(f"write {value}")


# ic[verify algo.init.atomic]
def test_second_initialization_fails(tmp_path: Path) -> None:
    storage = ic.local_filesystem_storage(str(tmp_path / "repo"))
    ic.Repository.create(storage=storage)
    with pytest.raises(ic.IcechunkError):
        ic.Repository.create(storage=storage)


# ic[verify algo.tag.create] a tag can be created from any snapshot
def test_tag_from_any_snapshot(tmp_path: Path) -> None:
    repo = make_repo(tmp_path / "repo")
    old_snapshot = commit_value(repo, 1)
    tip = commit_value(repo, 2)
    assert tip != old_snapshot

    # tag a non-tip snapshot and read back through the tag
    repo.create_tag("v1", old_snapshot)
    assert repo.lookup_tag("v1") == old_snapshot
    session = repo.readonly_session(tag="v1")
    array = cast(
        "zarr.core.array.Array[Any]", zarr.open_group(session.store, mode="r")["data"]
    )
    assert array[0, 0] == 1


# ic[verify algo.tag.conditional-update]
# ic[verify algo.tag.retry]
def test_concurrent_tag_creation() -> None:
    """Racing tag creations from independent repo handles: each conditional
    update either lands or detects the other session's update and retries, so
    every tag must be created exactly as requested.

    Uses in-memory storage: the local filesystem backend does not implement
    an atomic conditional update, so it cannot honor this requirement under
    real concurrency (icechunk warns about exactly this).
    """
    storage = ic.in_memory_storage()
    repo = ic.Repository.create(storage=storage)
    snapshot = commit_value(repo, 1)

    num_threads = 8
    barrier = Barrier(num_threads)
    errors: list[Exception] = []

    def create_tag(i: int) -> None:
        try:
            handle = ic.Repository.open(storage=storage)
            barrier.wait()
            handle.create_tag(f"tag-{i}", snapshot)
        except Exception as e:  # pragma: no cover - failure reporting only
            errors.append(e)

    threads = [Thread(target=create_tag, args=(i,)) for i in range(num_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert errors == []
    fresh = ic.Repository.open(storage=storage)
    for i in range(num_threads):
        assert fresh.lookup_tag(f"tag-{i}") == snapshot


# ic[verify algo.write.commit-retry-other]
def test_commit_succeeds_after_unrelated_repo_update(tmp_path: Path) -> None:
    """A commit whose conditional update fails because of an unrelated repo
    info update (a tag creation) must re-read and retry, not fail."""
    path = tmp_path / "repo"
    repo = make_repo(path)
    base = commit_value(repo, 1)

    session = repo.writable_session("main")
    group = zarr.group(store=session.store, overwrite=False)
    cast("zarr.core.array.Array[Any]", group["data"])[:, :] = 2

    # unrelated update to the repo info file while the session is open
    other = ic.Repository.open(storage=ic.local_filesystem_storage(str(path)))
    other.create_tag("concurrent", base)

    new_snapshot = session.commit("after unrelated update")

    fresh = ic.Repository.open(storage=ic.local_filesystem_storage(str(path)))
    assert fresh.lookup_branch("main") == new_snapshot
    assert fresh.lookup_tag("concurrent") == base
    parents = [s.id for s in fresh.ancestry(branch="main")]
    assert parents[0] == new_snapshot
    assert parents[1] == base


# ic[verify repo.update.backup-path]
def test_backup_names_match_spec(tmp_path: Path) -> None:
    path = tmp_path / "repo"
    repo = make_repo(path)
    overwritten = path / "overwritten"

    commit_value(repo, 1)
    first_backups = {p.name for p in overwritten.iterdir()}
    assert first_backups, "overwriting `repo` must leave a backup"

    commit_value(repo, 2)
    second_backups = {p.name for p in overwritten.iterdir()}
    new_backups = second_backups - first_backups
    assert new_backups, "every repo info overwrite must add a backup"

    for name in second_backups:
        assert BACKUP_NAME.match(name), f"backup name {name} not spec-shaped"

    # reversed timestamps: a later backup sorts before every earlier one
    for name in new_backups:
        assert all(name < old for old in first_backups), (
            f"later backup {name} does not sort before {sorted(first_backups)}"
        )


# ic[verify storage.immutability]
# ic[verify repo.update.only-mutable]
# ic[verify storage.ops.write] files are never moved or altered once written
def test_files_immutable_and_only_repo_overwritten(tmp_path: Path) -> None:
    path = tmp_path / "repo"
    repo = make_repo(path)

    def tree(root: Path) -> dict[str, str]:
        return {
            str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in root.rglob("*")
            if p.is_file()
        }

    seen = tree(path)
    repo_info_changed = False
    for value in (1, 2, 3):
        snapshot = commit_value(repo, value)
        if value == 2:
            repo.create_tag(f"v{value}", snapshot)
        now = tree(path)
        for name, digest in seen.items():
            assert name in now, f"{name} was moved or deleted"
            if name == "repo":
                repo_info_changed = repo_info_changed or now[name] != digest
            else:
                assert now[name] == digest, f"immutable file {name} was altered"
        seen = now
    assert repo_info_changed, "commits must rewrite the repo info file"


# ic[verify types.object-id.encoding]
def test_object_ids_are_uppercase_crockford(tmp_path: Path) -> None:
    path = tmp_path / "repo"
    repo = make_repo(path)
    commit_value(repo, 1)
    commit_value(repo, 2)

    ids = [s.id for s in repo.ancestry(branch="main")]
    # the spec's worked example: 0b1cc8d6787580f0e33a6534 -> 1CECHNKREP0F1RSTCMT0
    assert ids[-1] == "1CECHNKREP0F1RSTCMT0"
    for object_id in ids:
        assert CROCKFORD_20.match(object_id), (
            f"{object_id} is not 20-char uppercase Crockford base 32"
        )
    # ids are used verbatim as storage file names
    assert {p.name for p in (path / "snapshots").iterdir()} == set(ids)


# ic[verify snapshot.describes-state]
def test_snapshot_fully_describes_state(tmp_path: Path) -> None:
    """An old snapshot alone reproduces the full hierarchy as of its commit,
    unaffected by later commits that mutate and delete nodes."""
    repo = make_repo(tmp_path / "repo")

    session = repo.writable_session("main")
    group = zarr.group(store=session.store, overwrite=True)
    group.attrs["title"] = "v1"
    group.create_array("temp", shape=(2, 2), chunks=(1, 1), dtype="i4")[:, :] = 7
    sub = group.create_group("sub")
    sub.create_array("pressure", shape=(2,), chunks=(2,), dtype="i8")[:] = 11
    first = session.commit("rich hierarchy")

    session = repo.writable_session("main")
    group = zarr.open_group(session.store)
    del group["temp"]
    group.attrs["title"] = "v2"
    group.create_array("humidity", shape=(2,), chunks=(2,), dtype="i4")
    session.commit("mutate and delete")

    old = zarr.open_group(repo.readonly_session(snapshot_id=first).store, mode="r")
    assert old.attrs["title"] == "v1"
    assert sorted(k for k, _ in old.members(max_depth=None)) == [
        "sub",
        "sub/pressure",
        "temp",
    ]
    assert cast("zarr.core.array.Array[Any]", old["temp"])[1, 1] == 7
    assert cast("zarr.core.array.Array[Any]", old["sub/pressure"])[0] == 11


# ic[verify repo.pruned-tx-logs.full-history]
def test_expired_history_remains_complete(tmp_path: Path) -> None:
    """After expiration prunes intermediate snapshots, the surviving tip must
    still expose the full transaction history (via pruned_ancestor_tx_logs):
    a diff across the expired range reports every change."""
    repo = make_repo(tmp_path / "repo")
    initial = [s.id for s in repo.ancestry(branch="main")][-1]

    session = repo.writable_session("main")
    group = zarr.group(store=session.store, overwrite=True)
    expected_arrays = set()
    for i in range(3):
        group = zarr.open_group(session.store)
        group.create_array(f"array-{i}", shape=(2,), chunks=(2,), dtype="i4")
        expected_arrays.add(f"/array-{i}")
        session.commit(f"add array-{i}")
        session = repo.writable_session("main")

    expired = repo.expire_snapshots(datetime.now(UTC))
    assert len(expired) == 2, "intermediate snapshots should be pruned"

    # ancestry collapsed to tip + initial, but the diff still covers the
    # changes made by the expired commits
    assert len(list(repo.ancestry(branch="main"))) == 2
    diff = repo.diff(from_snapshot_id=initial, to_branch="main")
    assert expected_arrays <= diff.new_arrays
