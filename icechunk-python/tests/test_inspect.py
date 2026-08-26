from typing import Any

import icechunk as ic
import zarr


def check_header(header: dict[str, Any], file_type: str) -> None:
    assert header["file_type"] == file_type
    assert header["app_name"] == "ic"
    assert header["written_by"] == f"ic-{header['app_version']}"
    assert header["spec_version"] == "2.0"
    assert header["compression"] in ("none", "zstd")


async def test_inspect_snapshot() -> None:
    repo = await ic.Repository.open_async(
        storage=ic.local_filesystem_storage("./tests/data/split-repo-v2")
    )
    snap = next(repo.ancestry(branch="main")).id
    sync_result = repo.inspect_snapshot(snap)
    async_result = await repo.inspect_snapshot_async(snap)

    assert sync_result == async_result
    assert sync_result["id"] == snap
    check_header(sync_result["header"], "snapshot")


async def test_inspect_repo_info() -> None:
    repo = await ic.Repository.open_async(
        storage=ic.local_filesystem_storage("./tests/data/split-repo-v2")
    )
    sync_result = repo.inspect_repo_info()
    async_result = await repo.inspect_repo_info_async()

    assert sync_result == async_result
    assert "main" in sync_result["branches"]
    assert len(sync_result["snapshots"]) > 0
    check_header(sync_result["header"], "repo-info")


async def test_inspect_manifest() -> None:
    repo = await ic.Repository.open_async(
        storage=ic.local_filesystem_storage("./tests/data/split-repo-v2")
    )
    snap = next(repo.ancestry(branch="main")).id
    manifests = repo.list_manifest_files(snap)
    assert len(manifests) > 0

    manifest_id = manifests[0].id
    sync_result = repo.inspect_manifest(manifest_id)
    async_result = await repo.inspect_manifest_async(manifest_id)

    assert sync_result == async_result
    assert sync_result["id"] == manifest_id
    assert sync_result["num_arrays"] > 0
    assert sync_result["total_chunk_refs"] > 0
    assert (
        sync_result["total_chunk_refs"]
        == sync_result["total_inline"]
        + sync_result["total_native"]
        + sync_result["total_virtual"]
    )
    assert len(sync_result["arrays"]) == sync_result["num_arrays"]
    for arr in sync_result["arrays"]:
        assert (
            arr["num_chunk_refs"]
            == arr["num_inline"] + arr["num_native"] + arr["num_virtual"]
        )
    check_header(sync_result["header"], "manifest")


async def test_inspect_transaction_log() -> None:
    repo = await ic.Repository.open_async(
        storage=ic.local_filesystem_storage("./tests/data/split-repo-v2")
    )
    snap = next(repo.ancestry(branch="main")).id
    sync_result = repo.inspect_transaction_log(snap)
    async_result = await repo.inspect_transaction_log_async(snap)

    assert sync_result == async_result
    assert "new_groups" in sync_result
    assert "new_arrays" in sync_result
    assert "moved_nodes" in sync_result
    check_header(sync_result["header"], "transaction-log")


async def test_inspect_reports_writing_library_version() -> None:
    """Files this build writes are stamped with this build's version."""
    repo = ic.Repository.create(storage=ic.in_memory_storage())
    session = repo.writable_session("main")
    zarr.group(store=session.store, overwrite=True)
    snap = session.commit("commit")

    version = f"ic-{ic.__version__}"
    assert repo.inspect_repo_info()["header"]["written_by"] == version
    assert repo.inspect_snapshot(snap)["header"]["written_by"] == version
    assert repo.inspect_transaction_log(snap)["header"]["written_by"] == version
