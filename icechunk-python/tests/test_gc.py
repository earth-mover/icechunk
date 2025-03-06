import time
from datetime import UTC, datetime
from typing import cast

import pytest

import icechunk as ic
import zarr
from tests.conftest import get_minio_client


def mk_repo() -> tuple[str, ic.Repository]:
    prefix = "test-repo__" + str(time.time())
    repo = ic.Repository.create(
        storage=ic.s3_storage(
            endpoint_url="http://localhost:9000",
            allow_http=True,
            force_path_style=True,
            region="us-east-1",
            bucket="testbucket",
            prefix=prefix,
            access_key_id="minio123",
            secret_access_key="minio123",
        ),
        config=ic.RepositoryConfig(inline_chunk_threshold_bytes=0),
    )
    return (prefix, repo)


@pytest.mark.filterwarnings("ignore:datetime.datetime.utcnow")
def test_expire_and_gc() -> None:
    prefix, repo = mk_repo()

    session = repo.writable_session("main")
    store = session.store

    group = zarr.group(store=store, overwrite=True)
    array = group.create_array(
        "array",
        shape=(1000),
        chunks=(10),
        dtype="i4",
        fill_value=-1,
    )
    session.commit("array created")

    for i in range(20):
        session = repo.writable_session("main")
        store = session.store
        group = zarr.open_group(store=store)
        array = cast(zarr.core.array.Array, group["array"])
        array[i] = i
        session.commit(f"written coord {i}")

    old = datetime.now(UTC)

    session = repo.writable_session("main")
    store = session.store
    group = zarr.open_group(store=store)
    array = cast(zarr.core.array.Array, group["array"])
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
    # 22 commits total
    assert (
        len(
            client.list_objects(Bucket="testbucket", Prefix=f"{prefix}/transactions")[
                "Contents"
            ]
        )
        == 22
    )

    expired_snapshots = repo.expire_snapshots(old)
    # empty array + 20 old versions
    assert len(expired_snapshots) == 21

    space_before = 0
    for obj in client.list_objects(Bucket="testbucket", Prefix=f"{prefix}")["Contents"]:
        space_before += obj["Size"]

    gc_result = repo.garbage_collect(old)

    space_after = 0
    for obj in client.list_objects(Bucket="testbucket", Prefix=f"{prefix}")["Contents"]:
        space_after += obj["Size"]

    assert space_before - gc_result.bytes_deleted == space_after
    # there were 21 chunks, and we need 3 alive (for indexes 0..20 and 999)
    assert gc_result.chunks_deleted == 18
    # there were 23 snapshots, we need the initial one and the latest version only
    assert gc_result.snapshots_deleted == 21
    # there were 21 manifests, we need only 1
    assert gc_result.manifests_deleted == 20
    # not implemented yet
    assert gc_result.attributes_deleted == 0
    # same number as snapshots
    assert gc_result.transaction_logs_deleted == 21

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
    assert (
        len(
            client.list_objects(Bucket="testbucket", Prefix=f"{prefix}/transactions")[
                "Contents"
            ]
        )
        == 1
    )

    # we can still read the array
    session = repo.readonly_session(branch="main")
    store = session.store
    group = zarr.open_group(store=store, mode="r")
    array = cast(zarr.core.array.Array, group["array"])
    assert array[999] == 0
    for i in range(20):
        assert array[i] == i
