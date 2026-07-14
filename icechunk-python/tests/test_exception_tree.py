"""Tests for the Icechunk exception tree.

Covers the class hierarchy, the classification of Rust errors into it, the
ErrorKind sync contract, and pickling (tblib compatibility).
"""

import asyncio
import pickle
from typing import Any

import pytest

import icechunk as ic
import zarr
from icechunk._icechunk_python import _all_error_kinds


def test_tree_shape() -> None:
    # everything derives from IcechunkError
    for cls in (
        ic.ConflictError,
        ic.RebaseFailedError,
        ic.NotFoundError,
        ic.NodeNotFoundError,
        ic.SnapshotNotFoundError,
        ic.RefNotFoundError,
        ic.RepositoryNotFoundError,
        ic.AlreadyExistsError,
        ic.ReadOnlyError,
        ic.InvalidInputError,
        ic.SessionStateError,
        ic.StorageError,
        ic.FormatError,
        ic.InternalError,
    ):
        assert issubclass(cls, ic.IcechunkError)

    # a rebase failure is a kind of conflict
    assert issubclass(ic.RebaseFailedError, ic.ConflictError)

    # builtin compatibility contracts
    assert issubclass(ic.NotFoundError, KeyError)
    assert issubclass(ic.NodeNotFoundError, KeyError)
    assert issubclass(ic.SnapshotNotFoundError, ic.NotFoundError)
    assert issubclass(ic.RefNotFoundError, ic.NotFoundError)
    assert issubclass(ic.RepositoryNotFoundError, ic.NotFoundError)
    assert issubclass(ic.InvalidInputError, ValueError)


def test_error_kind_enum_in_sync_with_rust() -> None:
    rust_kinds = set(_all_error_kinds())
    python_kinds = {kind.value for kind in ic.ErrorKind}
    assert rust_kinds == python_kinds


@pytest.fixture
def repo() -> ic.Repository:
    repo = ic.Repository.create(storage=ic.in_memory_storage())
    session = repo.writable_session("main")
    root = zarr.group(store=session.store)
    root.create_array("some-array", shape=(10, 10), chunks=(1, 1), dtype="i4")
    session.commit("commit 1")
    return repo


@pytest.fixture
def conflicted_session(repo: ic.Repository) -> ic.Session:
    """A session whose commit will conflict: main advanced after it forked."""
    session_a = repo.writable_session("main")
    session_b = repo.writable_session("main")
    zarr.group(store=session_a.store).attrs["who"] = "a"
    zarr.group(store=session_b.store).attrs["who"] = "b"
    session_a.commit("a")
    return session_b


def test_commit_conflict(conflicted_session: ic.Session) -> None:
    with pytest.raises(ic.IcechunkError) as excinfo:
        conflicted_session.commit("b")
    err = excinfo.value
    assert isinstance(err, ic.ConflictError)
    assert err.kind == ic.ErrorKind.COMMIT_CONFLICT
    assert err.expected_parent is not None
    assert err.actual_parent is not None


def test_rebase_failed(conflicted_session: ic.Session) -> None:
    with pytest.raises(ic.ConflictError) as excinfo:
        conflicted_session.commit("b", rebase_with=ic.ConflictDetector())
    err = excinfo.value
    assert isinstance(err, ic.RebaseFailedError)
    assert err.kind == ic.ErrorKind.REBASE_FAILED
    assert err.snapshot
    assert len(err.conflicts) > 0


def test_branch_update_conflict(repo: ic.Repository) -> None:
    snapshot = repo.lookup_branch("main")
    with pytest.raises(ic.ConflictError) as excinfo:
        repo.reset_branch("main", snapshot, from_snapshot_id="1CECHNKREP0F1RSTCMT0")
    err = excinfo.value
    assert err.kind == ic.ErrorKind.BRANCH_UPDATE_CONFLICT
    assert "branch update conflict" in str(err)


def test_node_not_found(repo: ic.Repository) -> None:
    session = repo.rearrange_session("main")
    with pytest.raises(KeyError) as excinfo:
        session.move("/definitely/not/here", "/elsewhere")
    err = excinfo.value
    assert isinstance(err, ic.NotFoundError)
    assert isinstance(err, ic.IcechunkError)
    assert err.kind in (
        ic.ErrorKind.NODE_NOT_FOUND,
        ic.ErrorKind.ANCESTOR_NODE_NOT_FOUND,
    )


def test_store_get_missing_key_still_returns_none(repo: ic.Repository) -> None:
    # zarr's store contract: missing keys surface as None, implemented by an
    # `except KeyError` in IcechunkStore.get, which NotFoundError must satisfy
    from zarr.core.buffer import default_buffer_prototype

    store = repo.readonly_session("main").store
    result = asyncio.run(store.get("nope/zarr.json", default_buffer_prototype()))
    assert result is None


def test_read_only(repo: ic.Repository) -> None:
    # write through the Rust store directly: zarr's own read_only pre-check
    # would otherwise raise before Icechunk ever sees the write
    store = repo.readonly_session("main").store

    async def write() -> None:
        await store._store.set("zarr.json", b"{}")

    with pytest.raises(ic.ReadOnlyError) as excinfo:
        asyncio.run(write())
    assert excinfo.value.kind in (
        ic.ErrorKind.READ_ONLY_SESSION,
        ic.ErrorKind.READ_ONLY_STORE,
    )


def test_tag_already_exists(repo: ic.Repository) -> None:
    snapshot = repo.lookup_branch("main")
    repo.create_tag("v1", snapshot)
    with pytest.raises(ic.AlreadyExistsError) as excinfo:
        repo.create_tag("v1", snapshot)
    assert excinfo.value.kind == ic.ErrorKind.TAG_ALREADY_EXISTS


def test_ref_not_found(repo: ic.Repository) -> None:
    with pytest.raises(ic.NotFoundError) as excinfo:
        repo.lookup_branch("does-not-exist")
    err = excinfo.value
    assert isinstance(err, ic.RefNotFoundError | ic.NotFoundError)
    assert isinstance(err, KeyError)


@pytest.mark.parametrize("spec_version", [1, ic.SpecVersion.v2], ids=["v1", "v2"])
def test_readonly_session_ref_not_found(spec_version: ic.SpecVersion | int) -> None:
    # v1 and v2 resolve refs through different Rust paths; both must classify
    repo = ic.Repository.create(storage=ic.in_memory_storage(), spec_version=spec_version)

    with pytest.raises(ic.RefNotFoundError) as excinfo:
        repo.readonly_session(branch="does-not-exist")
    err = excinfo.value
    assert isinstance(err, KeyError)
    assert err.kind in (ic.ErrorKind.REF_NOT_FOUND, ic.ErrorKind.BRANCH_NOT_FOUND)

    with pytest.raises(ic.RefNotFoundError) as excinfo:
        repo.readonly_session(tag="does-not-exist")
    err = excinfo.value
    assert err.kind in (ic.ErrorKind.REF_NOT_FOUND, ic.ErrorKind.TAG_NOT_FOUND)


def test_repository_not_found(tmpdir: Any) -> None:
    with pytest.raises(ic.RepositoryNotFoundError) as excinfo:
        ic.Repository.open(storage=ic.local_filesystem_storage(str(tmpdir / "nope")))
    err = excinfo.value
    assert err.kind == ic.ErrorKind.REPOSITORY_NOT_FOUND
    assert isinstance(err, KeyError)
    # the full diagnostic report rides along as a PEP 678 note
    assert getattr(err, "__notes__", None)


def test_no_changes_to_commit(repo: ic.Repository) -> None:
    session = repo.writable_session("main")
    with pytest.raises(ic.SessionStateError) as excinfo:
        session.commit("nothing")
    assert excinfo.value.kind == ic.ErrorKind.NO_CHANGES_TO_COMMIT


def test_invalid_snapshot_id(repo: ic.Repository) -> None:
    with pytest.raises(ic.InvalidInputError) as excinfo:
        repo.readonly_session(snapshot_id="not a snapshot id")
    err = excinfo.value
    assert isinstance(err, ValueError)
    assert err.kind == ic.ErrorKind.INVALID_SNAPSHOT_ID


@pytest.mark.parametrize(
    "err",
    [
        ic.IcechunkError("some message"),
        ic.IcechunkError("some message", "io"),
        ic.ConflictError("abc", "def"),
        ic.ConflictError(None, None, "custom message", "branch-update-conflict"),
        ic.RebaseFailedError("SNAP1", []),
        ic.NotFoundError("missing", "key-not-found"),
        ic.NodeNotFoundError("missing node", "node-not-found"),
        ic.SnapshotNotFoundError("missing snapshot", "snapshot-not-found"),
        ic.RefNotFoundError("missing ref", "ref-not-found"),
        ic.RepositoryNotFoundError("missing repo", "repository-not-found"),
        ic.AlreadyExistsError("already there", "tag-already-exists"),
        ic.ReadOnlyError("read only", "read-only-session"),
        ic.InvalidInputError("bad input", "invalid-key"),
        ic.SessionStateError("bad state", "no-changes-to-commit"),
        ic.StorageError("storage failed", "object-store"),
        ic.FormatError("bad bytes", "deserialization"),
        ic.InternalError("bug", "unknown"),
    ],
    ids=lambda err: f"{type(err).__name__}-{err.kind}",
)
def test_pickle_roundtrip(err: ic.IcechunkError) -> None:
    unpickled = pickle.loads(pickle.dumps(err))
    assert type(unpickled) is type(err)
    assert unpickled.message == err.message
    assert unpickled.kind == err.kind
    if isinstance(err, ic.ConflictError):
        assert isinstance(unpickled, ic.ConflictError)
        assert unpickled.expected_parent == err.expected_parent
        assert unpickled.actual_parent == err.actual_parent
    if isinstance(err, ic.RebaseFailedError):
        assert isinstance(unpickled, ic.RebaseFailedError)
        assert unpickled.snapshot == err.snapshot
        assert len(unpickled.conflicts) == len(err.conflicts)


def test_pickle_roundtrip_of_raised_error(conflicted_session: ic.Session) -> None:
    with pytest.raises(ic.ConflictError) as excinfo:
        conflicted_session.commit("b")
    err = excinfo.value
    unpickled = pickle.loads(pickle.dumps(err))
    assert type(unpickled) is ic.ConflictError
    assert unpickled.expected_parent == err.expected_parent
    assert unpickled.actual_parent == err.actual_parent
    assert unpickled.kind == err.kind


def test_default_messages_preserved() -> None:
    c = ic.ConflictError("abc", "def")
    assert str(c) == (
        'Failed to commit, expected parent: Some("abc"), actual parent: Some("def")'
    )
    r = ic.RebaseFailedError("SNAP1", [])
    assert str(r) == "Rebase failed on snapshot SNAP1: 0 conflicts found"
