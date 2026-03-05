import json

import icechunk as ic


async def test_inspect_snapshot() -> None:
    repo = await ic.Repository.open_async(
        storage=ic.local_filesystem_storage("./tests/data/split-repo-v2")
    )
    snap = next(repo.ancestry(branch="main")).id
    sync_str = repo.inspect_snapshot(snap, pretty=True)
    async_str = await repo.inspect_snapshot_async(snap, pretty=True)
    non_pretty_str = repo.inspect_snapshot(snap, pretty=False)

    sync_result = json.loads(sync_str)
    async_result = json.loads(async_str)
    non_pretty_result = json.loads(non_pretty_str)

    assert sync_result == async_result
    assert sync_result == non_pretty_result
    assert sync_str != non_pretty_str
    assert sync_result["id"] == snap


async def test_inspect_repo_info() -> None:
    repo = await ic.Repository.open_async(
        storage=ic.local_filesystem_storage("./tests/data/split-repo-v2")
    )
    sync_result = repo.inspect_repo_info()
    async_result = await repo.inspect_repo_info_async()

    assert sync_result == async_result
    assert "main" in sync_result["branches"]
    assert len(sync_result["snapshots"]) > 0


async def test_inspect_manifest() -> None:
    repo = await ic.Repository.open_async(
        storage=ic.local_filesystem_storage("./tests/data/split-repo-v2")
    )
    snap = next(repo.ancestry(branch="main")).id
    manifests = repo.list_manifest_files(snap)
    assert len(manifests) > 0

    manifest_id = manifests[0].id
    sync_str = repo.inspect_manifest(manifest_id, pretty=True)
    async_str = await repo.inspect_manifest_async(manifest_id, pretty=True)
    non_pretty_str = repo.inspect_manifest(manifest_id, pretty=False)

    sync_result = json.loads(sync_str)
    async_result = json.loads(async_str)
    non_pretty_result = json.loads(non_pretty_str)

    assert sync_result == async_result
    assert sync_result == non_pretty_result
    assert sync_str != non_pretty_str

    assert sync_result["id"] == manifest_id
    assert sync_result["num_arrays"] > 0
    assert sync_result["total_chunk_refs"] > 0
    assert sync_result["total_chunk_refs"] == sync_result["total_inline"] + sync_result["total_native"] + sync_result["total_virtual"]
    assert len(sync_result["arrays"]) == sync_result["num_arrays"]
    for arr in sync_result["arrays"]:
        assert arr["num_chunk_refs"] == arr["num_inline"] + arr["num_native"] + arr["num_virtual"]
