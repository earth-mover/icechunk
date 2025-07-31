import json

import icechunk as ic


def test_inspect_snapshot() -> None:
    repo = ic.Repository.open(
        storage=ic.local_filesystem_storage("./tests/data/split-repo")
    )
    snap = next(repo.ancestry(branch="main")).id
    pretty_str = repo.inspect_snapshot(snap, pretty=True)
    non_pretty_str = repo.inspect_snapshot(snap, pretty=False)

    pretty = json.loads(pretty_str)
    non_pretty = json.loads(non_pretty_str)

    assert pretty["id"] == snap
    assert pretty_str != non_pretty_str
    assert pretty == non_pretty


async def test_inspect_snapshot_async() -> None:
    repo = await ic.Repository.open_async(
        storage=ic.local_filesystem_storage("./tests/data/split-repo")
    )
    snap = next(repo.ancestry(branch="main")).id
    pretty_str = await repo.inspect_snapshot_async(snap, pretty=True)
    non_pretty_str = await repo.inspect_snapshot_async(snap, pretty=False)

    pretty = json.loads(pretty_str)
    non_pretty = json.loads(non_pretty_str)

    assert pretty["id"] == snap
    assert pretty_str != non_pretty_str
    assert pretty == non_pretty
