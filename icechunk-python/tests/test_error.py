import glob
import re
from pathlib import Path
from shutil import rmtree
from typing import cast

import pytest

import icechunk as ic
import zarr


def test_error_message_when_snapshot_deleted(tmpdir: Path) -> None:
    tmpdir = Path(tmpdir)
    storage = ic.local_filesystem_storage(str(tmpdir))
    repo = ic.Repository.create(storage=storage)

    rmtree(tmpdir / "snapshots")

    repo = ic.Repository.open(storage=storage)
    # we check error includes the spans for ancestry and fetch_snapshot
    with pytest.raises(
        ic.IcechunkError, match=re.compile("fetch_snapshot.*ancestry", re.DOTALL)
    ):
        repo.ancestry(branch="main")


def test_error_message_when_manifest_file_altered(tmpdir: Path) -> None:
    tmpdir = Path(tmpdir)
    storage = ic.local_filesystem_storage(str(tmpdir))
    repo = ic.Repository.create(storage=storage)

    session = repo.writable_session("main")
    store = session.store

    group = zarr.group(store=store, overwrite=True)
    array = group.create_array("array", shape=(100, 100), chunks=(10, 10), dtype="i4")

    array[:, :] = 42
    session.commit("commit 1")

    manifest_path = glob.glob(f"{tmpdir}/manifests/*")[0]
    with open(manifest_path, "w") as file:
        file.write("invalid msgpack")

    repo = ic.Repository.open(storage=storage)

    session = repo.readonly_session(branch="main")
    store = session.store
    group = zarr.Group.open(store=store)
    fetched_array = cast(zarr.Array, group["array"])

    ## we check error includes the spans for ancestry and fetch_snapshot
    with pytest.raises(
        ic.IcechunkError, match=re.compile("fetch_manifest.*get", re.DOTALL)
    ):
        fetched_array[0]
