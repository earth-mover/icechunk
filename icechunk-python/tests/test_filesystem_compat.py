"""Cross-version compatibility tests for the native filesystem `Storage`.

`local_filesystem_storage()` used to be backed by object_store's
`LocalFileSystem`; it is now backed by a native `FilesystemStorage`
(typetag ``native_local_filesystem``) with a different serde discriminant.
These tests verify that the on-disk repository layout stays interoperable
across the icechunk 1.x and dev (2.x) packages in both directions, which is
the guarantee users actually rely on.

Requires:
  - icechunk     (dev build)
  - icechunk_v1  (latest 1.x from PyPI, renamed via third-wheel)

Skipped entirely if icechunk_v1 is not installed.

Note on pickling: a `Storage` object is not picklable in either version, and
an in-memory `Repository`/`Session` blob does not deserialize across the
1.x/2.x major boundary (the `Repository`/`TimeoutSettings` serde structs
changed, independently of the storage typetag). Cross-version compatibility
therefore travels through the shared on-disk format, exercised below.
"""

from pathlib import Path
from typing import Any, cast

import numpy as np
import pytest

import icechunk as ic
import zarr

ic_v1 = pytest.importorskip("icechunk_v1")


log_filter = "warn,icechunk::storage::object_store=error"
ic.set_logs_filter(log_filter)
ic_v1.set_logs_filter(log_filter)


def commit_msgs(repo: object) -> list[str]:
    return [snap.message for snap in repo.ancestry(branch="main")]  # type: ignore[attr-defined]


def read_array(store: Any) -> list[Any]:
    group = zarr.open_group(store, mode="r")
    values: list[Any] = np.asarray(cast("zarr.Array[Any]", group["data"])[:]).tolist()
    return values


def write_array(store: Any, values: list[int]) -> None:
    group = zarr.open_group(store, mode="a")
    cast("zarr.Array[Any]", group["data"])[:] = values


def test_dev_native_reads_and_extends_v1_repo(tmp_path: Path) -> None:
    """A repo written by icechunk 1.x opens under the native backend, and the
    dev version can read it and commit on top of its history."""
    path = str(tmp_path)

    v1_repo = ic_v1.Repository.create(storage=ic_v1.local_filesystem_storage(path))
    session = v1_repo.writable_session("main")
    root = zarr.group(store=session.store, overwrite=True)
    array = root.create_array("data", shape=(4,), chunks=(2,), dtype="i4")
    array[:] = [1, 2, 3, 4]
    session.commit("v1 commit")

    dev_repo = ic.Repository.open(ic.local_filesystem_storage(path))
    assert repr(dev_repo.storage).splitlines()[1] == "type: local filesystem (native)"

    assert read_array(dev_repo.readonly_session("main").store) == [1, 2, 3, 4]

    session = dev_repo.writable_session("main")
    write_array(session.store, [5, 6, 7, 8])
    session.commit("dev on top of v1")

    assert commit_msgs(dev_repo) == [
        "dev on top of v1",
        "v1 commit",
        "Repository initialized",
    ]


def test_v1_reads_and_extends_dev_native_repo(tmp_path: Path) -> None:
    """A repo written by the dev native backend opens under icechunk 1.x, which
    can read it and commit on top; the dev version then reads 1.x's commit.

    This proves the native backend writes a spec-compatible on-disk layout.
    """
    path = str(tmp_path)

    dev_repo = ic.Repository.create(
        storage=ic.local_filesystem_storage(path), spec_version=1
    )
    session = dev_repo.writable_session("main")
    root = zarr.group(store=session.store, overwrite=True)
    array = root.create_array("data", shape=(4,), chunks=(2,), dtype="i4")
    array[:] = [10, 20, 30, 40]
    session.commit("dev native commit")

    v1_repo = ic_v1.Repository.open(ic_v1.local_filesystem_storage(path))
    assert read_array(v1_repo.readonly_session("main").store) == [10, 20, 30, 40]

    session = v1_repo.writable_session("main")
    write_array(session.store, [11, 22, 33, 44])
    session.commit("v1 on top of dev")

    reopened = ic.Repository.open(ic.local_filesystem_storage(path))
    assert read_array(reopened.readonly_session("main").store) == [11, 22, 33, 44]
    assert commit_msgs(reopened) == [
        "v1 on top of dev",
        "dev native commit",
        "Repository initialized",
    ]


def test_backend_discriminant_changed(tmp_path: Path) -> None:
    """Document the serde discriminant change: dev reports the native backend
    while icechunk 1.x reports the object_store backend for the same path."""
    path = str(tmp_path)
    assert "native" in repr(ic.local_filesystem_storage(path))
    assert "native" not in repr(ic_v1.local_filesystem_storage(path))
