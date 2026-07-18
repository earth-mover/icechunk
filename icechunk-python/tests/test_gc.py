import time
from datetime import UTC, datetime
from typing import Any, cast

import pytest

import icechunk as ic
import zarr
from tests.conftest import Permission, get_minio_client


def mk_repo(spec_version: int | None) -> tuple[str, ic.Repository]:
    prefix = "test-repo__" + str(time.time())
    access_key_id, secret_access_key = Permission.MODIFY.keys()
    repo = ic.Repository.create(
        storage=ic.s3_storage(
            endpoint_url="http://localhost:4200",
            allow_http=True,
            force_path_style=True,
            region="us-east-1",
            bucket="testbucket",
            prefix=prefix,
            access_key_id=access_key_id,
            secret_access_key=secret_access_key,
        ),
        config=ic.RepositoryConfig(inline_chunk_threshold_bytes=0),
        spec_version=spec_version,
    )
    return (prefix, repo)


# ic[verify layout.root]
# ic[verify layout.paths]
# ic[verify algo.write.steps]
# ic[verify storage.ops.deletes]
@pytest.mark.filterwarnings("ignore:datetime.datetime.utcnow")
@pytest.mark.parametrize("use_async", [True, False])
async def test_expire_and_gc(use_async: bool, any_spec_version: int | None) -> None:
    prefix, repo = mk_repo(any_spec_version)

    session = repo.writable_session("main")
    store = session.store

    group = zarr.group(store=store, overwrite=True)
    array = group.create_array(
        "array",
        shape=(1000,),
        chunks=(10,),
        dtype="i4",
        fill_value=-1,
    )
    session.commit("array created")

    for i in range(20):
        session = repo.writable_session("main")
        store = session.store
        group = zarr.open_group(store=store)
        array = cast("zarr.core.array.Array[Any]", group["array"])
        array[i] = i
        session.commit(f"written coord {i}")

    old = datetime.now(UTC)

    session = repo.writable_session("main")
    store = session.store
    group = zarr.open_group(store=store)
    array = cast("zarr.core.array.Array[Any]", group["array"])
    array[999] = 0
    session.commit("written coord 999")

    client = get_minio_client()

    # repo initial snap + array creation + 20 old version + 1 new version
    assert (
        len(
            client.list_objects(Bucket="testbucket", Prefix=f"{prefix}/snapshots")[
                "Contents"
            ]
        )
        == 23
    )
    # 20 old chunks + 1 new
    assert (
        len(
            client.list_objects(Bucket="testbucket", Prefix=f"{prefix}/chunks")[
                "Contents"
            ]
        )
        == 21
    )
    # 21 commits that modify chunks
    assert (
        len(
            client.list_objects(Bucket="testbucket", Prefix=f"{prefix}/manifests")[
                "Contents"
            ]
        )
        == 21
    )
    # V2 repos have a transaction log for the initial snapshot
    expected_tx_logs = 23 if any_spec_version != 1 else 22
    assert (
        len(
            client.list_objects(Bucket="testbucket", Prefix=f"{prefix}/transactions")[
                "Contents"
            ]
        )
        == expected_tx_logs
    )

    if use_async:
        expired_snapshots = await repo.expire_snapshots_async(old)
    else:
        expired_snapshots = repo.expire_snapshots(old)
    # empty array + 20 old versions
    assert len(expired_snapshots) == 21

    def space_used() -> int:
        space = 0
        for obj in client.list_objects(Bucket="testbucket", Prefix=f"{prefix}/snapshots")[
            "Contents"
        ]:
            space += obj["Size"]
        for obj in client.list_objects(Bucket="testbucket", Prefix=f"{prefix}/chunks")[
            "Contents"
        ]:
            space += obj["Size"]
        for obj in client.list_objects(Bucket="testbucket", Prefix=f"{prefix}/manifests")[
            "Contents"
        ]:
            space += obj["Size"]
        for obj in client.list_objects(
            Bucket="testbucket", Prefix=f"{prefix}/transactions"
        )["Contents"]:
            space += obj["Size"]
        return space

    space_before = space_used()

    # On V2 repos the surviving tip carries their tx-log ids in pruned_ancestor_tx_logs,
    # so GC retains those logs. V1 has no such mechanism and deletes them
    expected_tx_logs_deleted = 21 if any_spec_version == 1 else 0

    # let's run GC using dry_run = True
    if use_async:
        gc_result = await repo.garbage_collect_async(old, dry_run=True)
    else:
        gc_result = repo.garbage_collect(old, dry_run=True)
    space_after = space_used()

    assert space_before == space_after
    # there were 21 chunks, and we need 3 alive (for indexes 0..20 and 999)
    assert gc_result.chunks_deleted == 18
    # there were 23 snapshots, we need the initial one and the latest version only
    assert gc_result.snapshots_deleted == 21
    # there were 21 manifests, we need only 1
    assert gc_result.manifests_deleted == 20
    # not implemented yet
    assert gc_result.attributes_deleted == 0
    assert gc_result.transaction_logs_deleted == expected_tx_logs_deleted

    # now let's run real GC, no dry_run.
    if use_async:
        gc_result = await repo.garbage_collect_async(old)
    else:
        gc_result = repo.garbage_collect(old)

    space_after = space_used()

    assert space_before - gc_result.bytes_deleted == space_after
    # there were 21 chunks, and we need 3 alive (for indexes 0..20 and 999)
    assert gc_result.chunks_deleted == 18
    # there were 23 snapshots, we need the initial one and the latest version only
    assert gc_result.snapshots_deleted == 21
    # there were 21 manifests, we need only 1
    assert gc_result.manifests_deleted == 20
    # not implemented yet
    assert gc_result.attributes_deleted == 0
    assert gc_result.transaction_logs_deleted == expected_tx_logs_deleted

    assert (
        len(
            client.list_objects(Bucket="testbucket", Prefix=f"{prefix}/snapshots")[
                "Contents"
            ]
        )
        == 2
    )
    assert (
        len(
            client.list_objects(Bucket="testbucket", Prefix=f"{prefix}/chunks")[
                "Contents"
            ]
        )
        == 3
    )
    assert (
        len(
            client.list_objects(Bucket="testbucket", Prefix=f"{prefix}/manifests")[
                "Contents"
            ]
        )
        == 1
    )
    # On V2 repos the surviving tip carries their tx-log ids in pruned_ancestor_tx_logs:
    # initial + tip + 21 expired ancestors = 23.
    # V1 has no such mechanism and deletes them
    expected_remaining_tx_logs = 1 if any_spec_version == 1 else 23
    assert (
        len(
            client.list_objects(Bucket="testbucket", Prefix=f"{prefix}/transactions")[
                "Contents"
            ]
        )
        == expected_remaining_tx_logs
    )

    # we can still read the array
    session = repo.readonly_session(branch="main")
    store = session.store
    group = zarr.open_group(store=store, mode="r")
    array = cast("zarr.core.array.Array[Any]", group["array"])
    assert array[999] == 0
    for i in range(20):
        assert array[i] == i


@pytest.mark.filterwarnings("ignore:datetime.datetime.utcnow")
@pytest.mark.parametrize("use_async", [True, False])
async def test_gc_deletes_only_unreferenced_expired_tx_logs(use_async: bool) -> None:
    """GC must discriminate among expired transaction logs: a released snapshot
    whose log is referenced by a survivor's pruned_ancestor_tx_logs is retained,
    while a released snapshot with no surviving descendant has its log deleted.
    test_expire_and_gc only covers the "everything referenced" case (0 deleted);
    here a doomed side branch forces the mixed case. V2-only: V1 has no
    pruned-ancestor mechanism.
    """
    prefix, repo = mk_repo(ic.SpecVersion.v2)
    client = get_minio_client()

    def commit_group(branch: str, name: str) -> str:
        session = repo.writable_session(branch)
        zarr.open_group(store=session.store).create_group(name)
        return session.commit(f"add {name} on {branch}")

    def transaction_count() -> int:
        resp = client.list_objects(Bucket="testbucket", Prefix=f"{prefix}/transactions")
        return len(resp.get("Contents", []))

    # main: root group -> /a -> /b -> /c, threshold just before /c. /a and /b are
    # released but referenced by the re-parented /c, so their logs survive.
    session = repo.writable_session("main")
    zarr.group(store=session.store, overwrite=True)
    session.commit("root group")
    commit_group("main", "a")
    snap_b = commit_group("main", "b")

    # A doomed branch off /b, fully pre-threshold, deleted at expiration. Its
    # unique commits have no surviving descendant, so nobody references their
    # logs and GC deletes them.
    repo.create_branch("doomed", snap_b)
    commit_group("doomed", "d")
    commit_group("doomed", "e")

    # Bracket the threshold with gaps so prior commits land strictly before it
    # and /c strictly after, clear of created_at (ms) vs flushed_at truncation.
    time.sleep(0.05)
    threshold = datetime.now(UTC)
    time.sleep(0.05)

    commit_group("main", "c")  # survives, re-parented to root

    # initial snapshot + root group + /a + /b + /d + /e + /c
    assert transaction_count() == 7

    if use_async:
        expired = await repo.expire_snapshots_async(
            threshold, delete_expired_branches=True
        )
    else:
        expired = repo.expire_snapshots(threshold, delete_expired_branches=True)
    # root group, /a, /b on main + /d, /e on doomed
    assert len(expired) == 5
    assert "doomed" not in repo.list_branches()

    if use_async:
        gc_result = await repo.garbage_collect_async(threshold)
    else:
        gc_result = repo.garbage_collect(threshold)

    # All 5 released snapshot files are deleted...
    assert gc_result.snapshots_deleted == 5
    # ...but only the two unreferenced logs (doomed's /d, /e). /a and /b are
    # retained via /c's pruned_ancestor_tx_logs.
    assert gc_result.transaction_logs_deleted == 2

    # initial + root group + /a + /b + /c remain
    assert transaction_count() == 5
