"""This test reads a repository generated with an older version of icechunk.

In this way, we check we maintain read compatibility. The repository lives
in the git repository, as a filesystem store, in the directory icechunk-python/tests/data/test-repo

If something changes in the on disk format, we probably won't be able to read the repo, the
test will fail and we can avoid breaking user data.

When new features that impact the stored info are added, or when the on-disk format is
intentionally changed, the repository files must be regenerated. For that, run the current
file as a python script: `python ./tests/test_can_read_old.py`.
"""

from typing import cast

import pytest
from numpy.testing import assert_array_equal
from object_store import ClientOptions, ObjectStore

import icechunk as ic
import zarr


def write_chunks_to_minio(chunks: list[tuple[str, bytes]]):
    client_options = ClientOptions(
        allow_http=True,  # type: ignore
    )
    store = ObjectStore(
        "s3://testbucket",
        {
            "access_key_id": "minio123",
            "secret_access_key": "minio123",
            "aws_region": "us-east-1",
            "aws_endpoint": "http://localhost:9000",
        },
        client_options=client_options,
    )

    for key, data in chunks:
        store.put(key, data)


def mk_repo():
    """Create a store that can access virtual chunks in localhost MinIO"""
    store_path = "./tests/data/test-repo"
    store = ic.Repository.open_or_create(
        storage=ic.StorageConfig.filesystem(store_path),
        config=ic.RepositoryConfig(
            inline_chunk_threshold_bytes=10,
        ),
    )
    return store


async def write_a_test_repo():
    """Write the test repository.

    This function tries to explore as many icechunk features as possible, to generate
    an richer repository on disk. For example, it does several commits, it has a hierarchy,
    it has virtual, inline and materialized chunks, branches and tags, etc.

    PLEASE: keep addign more actions to this function as we add more features to Icechunk.
    """

    print("Writing repository to ./tests/data/test-repo")
    repo = mk_repo()
    session = repo.writable_session("main")
    store = session.store()

    root = zarr.group(store=store)
    group1 = root.create_group(
        "group1", attributes={"this": "is a nice group", "icechunk": 1, "size": 42.0}
    )

    # these chunks will be materialized
    big_chunks = group1.create_array(
        "big_chunks",
        shape=(10, 10),
        chunk_shape=(5, 5),
        dtype="float32",
        fill_value=float("nan"),
        attributes={"this": "is a nice array", "icechunk": 1, "size": 42.0},
    )

    # these chunks will be inline
    small_chunks = group1.create_array(
        "small_chunks",
        shape=(5),
        chunk_shape=(1),
        dtype="int8",
        fill_value=8,
        attributes={"this": "is a nice array", "icechunk": 1, "size": 42.0},
    )
    session.commit("empty structure")

    session = repo.writable_session("main")
    store = session.store()
    root = zarr.group(store=store)
    big_chunks = cast(zarr.Array, root["group1/big_chunks"])
    small_chunks = cast(zarr.Array, root["group1/small_chunks"])

    big_chunks[:] = 42.0
    small_chunks[:] = 84
    snapshot = session.commit("fill data")

    # store.set_virtual_ref(
    #     "group1/big_chunks/c/0/0",
    #     "s3://testbucket/path/to/python/chunk-1",
    #     offset=0,
    #     length=5 * 5 * 4,
    # )
    # store.commit("set virtual chunk")

    repo.create_branch("my-branch", snapshot_id=snapshot)
    session = repo.writable_session("my-branch")
    store = session.store()

    await store.delete("group1/small_chunks/c/4")
    snap4 = session.commit("delete a chunk")

    repo.create_tag("it works!", snapshot_id=snap4)

    session = repo.writable_session("my-branch")
    store = session.store()
    root = zarr.open_group(store=store)

    group2 = root.create_group(
        "group2", attributes={"this": "is a nice group", "icechunk": 1, "size": 42.0}
    )
    group3 = group2.create_group(
        "group3", attributes={"this": "is a nice group", "icechunk": 1, "size": 42.0}
    )
    group4 = group3.create_group(
        "group4", attributes={"this": "is a nice group", "icechunk": 1, "size": 42.0}
    )
    group5 = group4.create_group(
        "group5", attributes={"this": "is a nice group", "icechunk": 1, "size": 42.0}
    )
    group5.create_array(
        "inner",
        shape=(10, 10),
        chunk_shape=(5, 5),
        dtype="float32",
        fill_value=float("nan"),
        attributes={"this": "is a nice array", "icechunk": 1, "size": 42.0},
    )
    snap5 = session.commit("some more structure")

    repo.create_tag("it also works!", snapshot_id=snap5)

    store.close()


async def test_icechunk_can_read_old_repo():
    # FIXME
    pytest.xfail(
        "Temporary flagged as failing while we implement new virtual chunk mechanism"
    )

    repo = mk_repo()
    main_snapshot = repo.branch_tip("main")

    expected_main_history = [
        "set virtual chunk",
        "fill data",
        "empty structure",
        "Repository initialized",
    ]
    assert [p.message for p in repo.ancestry(main_snapshot)] == expected_main_history

    my_branch_snapshot = repo.branch_tip("my-branch")
    expected_branch_history = [
        "some more structure",
        "delete a chunk",
    ] + expected_main_history
    assert [
        p.message for p in repo.ancestry(my_branch_snapshot)
    ] == expected_branch_history

    tag_snapshot = repo.tag("it also works!")
    assert [p.message for p in repo.ancestry(tag_snapshot)] == expected_branch_history

    tag_snapshot = repo.tag("it works!")
    assert [p.message for p in repo.ancestry(tag_snapshot)] == expected_branch_history[1:]

    session = repo.writable_session("my-branch")
    store = session.store()
    assert sorted([p async for p in store.list_dir("")]) == [
        "group1",
        "group2",
        "zarr.json",
    ]
    assert sorted([p async for p in store.list_dir("group1")]) == [
        "big_chunks",
        "small_chunks",
        "zarr.json",
    ]
    assert sorted([p async for p in store.list_dir("group2")]) == ["group3", "zarr.json"]
    assert sorted([p async for p in store.list_dir("group2/group3")]) == [
        "group4",
        "zarr.json",
    ]
    assert sorted([p async for p in store.list_dir("group2/group3/group4")]) == [
        "group5",
        "zarr.json",
    ]
    assert sorted([p async for p in store.list_dir("group2/group3/group4/group5")]) == [
        "inner",
        "zarr.json",
    ]
    assert sorted(
        [p async for p in store.list_dir("group2/group3/group4/group5/inner")]
    ) == ["c", "zarr.json"]

    root = zarr.group(store=store.as_writable())
    # inner is not initialized, so it's all fill values
    inner = root["group2/group3/group4/group5/inner"]
    assert_array_equal(inner[:], float("nan"))

    small_chunks = root["group1/small_chunks"]
    # has 5 elements, we deleted the last chunk (of size 1), and the fill value is 8
    assert_array_equal(small_chunks[:], [84, 84, 84, 84, 8])

    # big_chunks array has a virtual chunk, so we need to write it to local MinIO
    # we get the bytes from one of the materialized chunks
    buffer_prototype = zarr.core.buffer.default_buffer_prototype()
    chunk_data = (
        await store.get("group1/big_chunks/c/0/1", prototype=buffer_prototype)
    ).to_bytes()

    # big chunks array has a virtual chunk pointing here
    write_chunks_to_minio(
        [
            ("path/to/python/chunk-1", chunk_data),
        ]
    )

    big_chunks = root["group1/big_chunks"]
    assert_array_equal(big_chunks[:], 42.0)


if __name__ == "__main__":
    import asyncio

    asyncio.run(write_a_test_repo())
