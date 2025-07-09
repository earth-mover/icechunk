"""This test reads a repository generated with an older version of icechunk.

In this way, we check we maintain read compatibility. The repository lives
in the git repository, as a filesystem store, in the directory icechunk-python/tests/data/test-repo

If something changes in the on disk format, we probably won't be able to read the repo, the
test will fail and we can avoid breaking user data.

When new features that impact the stored info are added, or when the on-disk format is
intentionally changed, the repository files must be regenerated. For that, run the current
file as a python script: `python ./tests/test_can_read_old.py`.
"""

from datetime import UTC, datetime
from typing import cast

import pytest
from numpy.testing import assert_array_equal

import icechunk as ic
import zarr

UPDATED_SPLITTING_CONFIG = ic.ManifestSplittingConfig.from_dict(
    {
        ic.ManifestSplitCondition.name_matches("split_*"): {
            ic.ManifestSplitDimCondition.Axis(0): 1,
            ic.ManifestSplitDimCondition.DimensionName("longitude"): 1,
            ic.ManifestSplitDimCondition.Any(): 3,
        }
    }
)


def mk_repo(
    *, create: bool, store_path: str, config: ic.RepositoryConfig | None = None
) -> ic.Repository:
    """Create a store that can access virtual chunks in localhost MinIO"""
    if create and config is None:
        config = ic.RepositoryConfig.default()
        config.inline_chunk_threshold_bytes = 12

        virtual_store_config = ic.s3_store(
            region="us-east-1",
            endpoint_url="http://localhost:9000",
            allow_http=True,
            s3_compatible=True,
            force_path_style=True,
        )
        container = ic.VirtualChunkContainer("s3://testbucket", virtual_store_config)
        config.set_virtual_chunk_container(container)
    credentials = ic.containers_credentials(
        {
            "s3://testbucket": ic.s3_credentials(
                access_key_id="minio123", secret_access_key="minio123"
            )
        }
    )

    operation = ic.Repository.create if create else ic.Repository.open
    repo = operation(
        storage=ic.local_filesystem_storage(store_path),
        config=config,
        authorize_virtual_chunk_access=credentials,
    )

    return repo


async def write_a_split_repo() -> None:
    """Write the test repository with manifest splitting."""

    store_path = "./tests/data/split-repo"
    config = ic.RepositoryConfig.default()
    config.inline_chunk_threshold_bytes = 12
    config.manifest = ic.ManifestConfig(
        splitting=ic.ManifestSplittingConfig.from_dict(
            {
                ic.ManifestSplitCondition.name_matches("split_*"): {
                    ic.ManifestSplitDimCondition.Axis(0): 1,
                    ic.ManifestSplitDimCondition.DimensionName("latitude"): 1,
                    ic.ManifestSplitDimCondition.Any(): 3,
                }
            }
        )
    )

    print(f"Writing repository to {store_path}")
    repo = mk_repo(create=True, store_path=store_path, config=config)
    session = repo.writable_session("main")
    store = session.store

    root = zarr.group(store=store)
    group1 = root.create_group(
        "group1", attributes={"this": "is a nice group", "icechunk": 1, "size": 42.0}
    )

    # these chunks will be materialized
    big_chunks = group1.create_array(
        "split",
        shape=(10, 10),
        chunks=(3, 3),
        dimension_names=(None, "longitude"),
        dtype="float32",
        fill_value=float("nan"),
        attributes={"this": "is a nice array", "icechunk": 1, "size": 42.0},
    )

    # these chunks will be inline
    small_chunks = group1.create_array(
        "small_chunks",
        shape=(5,),
        chunks=(1,),
        dtype="int8",
        fill_value=8,
        attributes={"this": "is a nice array", "icechunk": 1, "size": 42.0},
    )
    session.commit("empty structure")

    session = repo.writable_session("main")
    big_chunks = zarr.open_array(session.store, path="/group1/split", mode="a")
    small_chunks = zarr.open_array(session.store, path="/group1/small_chunks", mode="a")
    big_chunks[:] = 120
    small_chunks[:] = 0
    session.commit("write data")

    session = repo.writable_session("main")
    big_chunks = zarr.open_array(session.store, path="group1/split", mode="a")
    small_chunks = zarr.open_array(session.store, path="group1/small_chunks", mode="a")
    big_chunks[:] = 12
    small_chunks[:] = 1
    session.commit("write data again")

    ### new config
    config = ic.RepositoryConfig.default()
    config.inline_chunk_threshold_bytes = 12
    config.manifest = ic.ManifestConfig(splitting=UPDATED_SPLITTING_CONFIG)
    repo = mk_repo(create=False, store_path=store_path, config=config)
    repo.save_config()
    session = repo.writable_session("main")
    big_chunks = zarr.open_array(session.store, path="group1/split", mode="a")
    small_chunks = zarr.open_array(session.store, path="group1/small_chunks", mode="a")
    big_chunks[:] = 14
    small_chunks[:] = 3
    session.commit("write data again with more splits")


async def test_icechunk_can_read_old_repo_with_manifest_splitting() -> None:
    repo = mk_repo(create=False, store_path="./tests/data/split-repo")
    ancestry = list(repo.ancestry(branch="main"))[::-1]

    init_snapshot = ancestry[1]
    assert init_snapshot.message == "empty structure"
    assert len(init_snapshot.manifests) == 0

    snapshot = ancestry[2]
    assert snapshot.message == "write data"
    assert len(snapshot.manifests) == 9

    snapshot = ancestry[3]
    assert snapshot.message == "write data again"
    assert len(snapshot.manifests) == 9

    snapshot = ancestry[4]
    assert snapshot.message == "write data again with more splits"
    assert len(snapshot.manifests) == 17

    assert repo.config.manifest
    assert repo.config.manifest.splitting == UPDATED_SPLITTING_CONFIG


async def write_a_test_repo() -> None:
    """Write the test repository.

    This function tries to explore as many icechunk features as possible, to generate
    an richer repository on disk. For example, it does several commits, it has a hierarchy,
    it has virtual, inline and materialized chunks, branches and tags, etc.

    PLEASE: keep addign more actions to this function as we add more features to Icechunk.
    """

    print("Writing repository to ./tests/data/test-repo")
    repo = mk_repo(create=True, store_path="./tests/data/test-repo")
    session = repo.writable_session("main")
    store = session.store

    root = zarr.group(store=store)
    group1 = root.create_group(
        "group1", attributes={"this": "is a nice group", "icechunk": 1, "size": 42.0}
    )

    # these chunks will be materialized
    big_chunks = group1.create_array(
        "big_chunks",
        shape=(10, 10),
        chunks=(5, 5),
        dtype="float32",
        fill_value=float("nan"),
        attributes={"this": "is a nice array", "icechunk": 1, "size": 42.0},
    )

    # these chunks will be inline
    small_chunks = group1.create_array(
        "small_chunks",
        shape=(5,),
        chunks=(1,),
        dtype="int8",
        fill_value=8,
        attributes={"this": "is a nice array", "icechunk": 1, "size": 42.0},
    )
    session.commit("empty structure")

    session = repo.writable_session("main")
    store = session.store
    root = zarr.group(store=store)
    big_chunks = cast(zarr.Array, root["group1/big_chunks"])
    small_chunks = cast(zarr.Array, root["group1/small_chunks"])

    big_chunks[:] = 42.0
    small_chunks[:] = 84
    snapshot = session.commit("fill data")
    session = repo.writable_session("main")
    store = session.store

    # We are going to write this chunk to storage as a virtual chunk
    # We need to know its size
    buffer_prototype = zarr.core.buffer.default_buffer_prototype()
    chunk = await store.get("group1/big_chunks/c/0/1", prototype=buffer_prototype)
    assert chunk
    virtual_chunk_data_size = len(chunk.to_bytes())

    store.set_virtual_ref(
        "group1/big_chunks/c/0/0",
        "s3://testbucket/can_read_old/chunk-1",
        offset=0,
        length=virtual_chunk_data_size,
        checksum=datetime(9999, 12, 31, tzinfo=UTC),
    )
    snapshot = session.commit("set virtual chunk")
    session = repo.writable_session("main")
    store = session.store

    repo.create_branch("my-branch", snapshot_id=snapshot)
    session = repo.writable_session("my-branch")
    store = session.store

    await store.delete("group1/small_chunks/c/4")
    snap4 = session.commit("delete a chunk")

    repo.create_tag("it works!", snapshot_id=snap4)
    repo.create_tag("deleted", snapshot_id=snap4)
    repo.delete_tag("deleted")

    session = repo.writable_session("my-branch")
    store = session.store
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
        chunks=(5, 5),
        dtype="float32",
        fill_value=float("nan"),
        attributes={"this": "is a nice array", "icechunk": 1, "size": 42.0},
    )
    snap5 = session.commit("some more structure")

    repo.create_tag("it also works!", snapshot_id=snap5)

    store.close()


@pytest.mark.filterwarnings("ignore:datetime.datetime.utcnow")
async def test_icechunk_can_read_old_repo() -> None:
    # we import here so it works when the script is ran by pytest
    from tests.conftest import write_chunks_to_minio

    repo = mk_repo(create=False, store_path="./tests/data/test-repo")
    main_snapshot = repo.lookup_branch("main")

    expected_main_history = [
        "set virtual chunk",
        "fill data",
        "empty structure",
        "Repository initialized",
    ]
    assert [
        p.message for p in repo.ancestry(snapshot_id=main_snapshot)
    ] == expected_main_history

    expected_branch_history = [
        "some more structure",
        "delete a chunk",
    ] + expected_main_history

    assert [
        p.message for p in repo.ancestry(branch="my-branch")
    ] == expected_branch_history

    assert [
        p.message for p in repo.ancestry(tag="it also works!")
    ] == expected_branch_history

    assert [p.message for p in repo.ancestry(tag="it works!")] == expected_branch_history[
        1:
    ]

    with pytest.raises(ic.IcechunkError, match="ref not found"):
        repo.readonly_session(tag="deleted")

    session = repo.writable_session("my-branch")
    store = session.store
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

    root = zarr.group(store=store)
    # inner is not initialized, so it's all fill values
    inner = root["group2/group3/group4/group5/inner"]
    assert_array_equal(inner[:], float("nan"))

    small_chunks = root["group1/small_chunks"]
    # has 5 elements, we deleted the last chunk (of size 1), and the fill value is 8
    assert_array_equal(small_chunks[:], [84, 84, 84, 84, 8])

    # big_chunks array has a virtual chunk, so we need to write it to local MinIO
    # we get the bytes from one of the materialized chunks
    buffer_prototype = zarr.core.buffer.default_buffer_prototype()
    chunk_data = await store.get("group1/big_chunks/c/0/1", prototype=buffer_prototype)
    assert chunk_data
    chunk_data_bytes = chunk_data.to_bytes()

    # big chunks array has a virtual chunk pointing here
    write_chunks_to_minio(
        [
            ("can_read_old/chunk-1", chunk_data_bytes),
        ]
    )

    big_chunks = root["group1/big_chunks"]
    assert_array_equal(big_chunks[:], 42.0)

    parents = list(repo.ancestry(branch="main"))
    diff = repo.diff(to_branch="main", from_snapshot_id=parents[-2].id)
    assert diff.new_groups == set()
    assert diff.new_arrays == set()
    assert set(diff.updated_chunks.keys()) == {
        "/group1/big_chunks",
        "/group1/small_chunks",
    }
    assert sorted(diff.updated_chunks["/group1/big_chunks"]) == sorted(
        [[i, j] for i in range(2) for j in range(2)]
    )
    assert sorted(diff.updated_chunks["/group1/small_chunks"]) == sorted(
        [[i] for i in range(5)]
    )
    assert diff.deleted_groups == set()
    assert diff.deleted_arrays == set()
    assert diff.updated_groups == set()
    assert diff.updated_arrays == set()


if __name__ == "__main__":
    import asyncio

    # we import here so it works when the script is ran by pytest

    # asyncio.run(write_a_test_repo())
    asyncio.run(write_a_split_repo())
