"""This test reads a repository generated with an older version of icechunk.

In this way, we check we maintain read compatibility. The repository lives
in the git repository, as a filesystem store, in the directory icechunk-python/tests/data/test-repo

If something changes in the on disk format, we probably won't be able to read the repo, the
test will fail and we can avoid breaking user data.

When new features that impact the stored info are added, or when the on-disk format is
intentionally changed, the repository files must be regenerated. For that, run the current
file as a python script: `python ./tests/test_can_read_old.py`.

Note: backward compatibility for the empty-`prefix` S3 key layout (#2239) is *not*
covered here. These fixtures are local-filesystem/object_store repos, whose backend
normalizes away leading slashes and so cannot represent a native-S3 `/chunks/...`
("rooted") repository. That layout is S3-only and is covered by synthesizing it at
test time: see `tests/test_key_layout.py` and `icechunk/tests/test_key_layout.rs`.
"""

import shutil
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

import pytest
from numpy.testing import assert_array_equal

import icechunk as ic
import zarr

# This file can be run as a script to regenerate test data (see module docstring).
# When run as a script, `tests` is not a package, so we fall back to a direct import.
try:
    from tests.conftest import Permission
except ImportError:
    from conftest import Permission  # type: ignore[import-not-found,no-redef]

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
            endpoint_url="http://localhost:4200",
            allow_http=True,
            s3_compatible=True,
            force_path_style=True,
        )
        container = ic.VirtualChunkContainer("s3://testbucket/", virtual_store_config)
        config.set_virtual_chunk_container(container)
    access_key_id, secret_access_key = Permission.READONLY.keys()
    credentials = ic.containers_credentials(
        {
            "s3://testbucket": ic.s3_credentials(
                access_key_id=access_key_id,
                secret_access_key=secret_access_key,
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


async def write_a_split_repo(path: str) -> None:
    """Write the test repository with manifest splitting."""

    store_path = path
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


async def do_icechunk_can_read_old_repo_with_manifest_splitting(path: str) -> None:
    repo = mk_repo(create=False, store_path=path)
    ancestry = list(repo.ancestry(branch="main"))[::-1]

    init_snapshot = ancestry[1]
    assert init_snapshot.message == "empty structure"
    assert len(repo.list_manifest_files(init_snapshot.id)) == 0

    snapshot = ancestry[2]
    assert snapshot.message == "write data"
    assert len(repo.list_manifest_files(snapshot.id)) == 9

    snapshot = ancestry[3]
    assert snapshot.message == "write data again"
    assert len(await repo.list_manifest_files_async(snapshot.id)) == 9

    snapshot = ancestry[4]
    assert snapshot.message == "write data again with more splits"
    assert len(await repo.list_manifest_files_async(snapshot.id)) == 17

    assert repo.config.manifest
    assert repo.config.manifest.splitting == UPDATED_SPLITTING_CONFIG


async def write_a_test_repo(path: str) -> None:
    """Write the test repository.

    This function tries to explore as many icechunk features as possible, to generate
    an richer repository on disk. For example, it does several commits, it has a hierarchy,
    it has virtual, inline and materialized chunks, branches and tags, etc.

    PLEASE: keep addign more actions to this function as we add more features to Icechunk.
    """

    print(f"Writing repository to {path}")
    repo = mk_repo(create=True, store_path=path)
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
    big_chunks = cast("zarr.Array[Any]", root["group1/big_chunks"])
    small_chunks = cast("zarr.Array[Any]", root["group1/small_chunks"])

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


async def do_icechunk_can_read_old_repo(path: str) -> None:
    try:
        from tests.conftest import write_chunks_to_minio
    except ImportError:
        from conftest import write_chunks_to_minio  # type: ignore[no-redef]

    repo = mk_repo(create=False, store_path=path)
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
    inner = cast("zarr.Array[Any]", root["group2/group3/group4/group5/inner"])
    assert_array_equal(inner[:], float("nan"))

    small_chunks = cast("zarr.Array[Any]", root["group1/small_chunks"])
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

    big_chunks = cast("zarr.Array[Any]", root["group1/big_chunks"])
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


def migrate_repo_v1_to_v2(from_dir: str, to_dir: str) -> None:
    shutil.copytree(from_dir, to_dir, dirs_exist_ok=True)
    repo = mk_repo(create=False, store_path=to_dir)
    ic.upgrade_icechunk_repository(repo, dry_run=False)


@pytest.mark.filterwarnings("ignore:datetime.datetime.utcnow")
async def test_icechunk_can_read_old_repo_v1() -> None:
    await do_icechunk_can_read_old_repo("./tests/data/test-repo-v1")


@pytest.mark.filterwarnings("ignore:datetime.datetime.utcnow")
async def test_icechunk_can_read_old_repo_v2() -> None:
    await do_icechunk_can_read_old_repo("./tests/data/test-repo-v2")


@pytest.mark.filterwarnings("ignore:datetime.datetime.utcnow")
async def test_icechunk_can_read_old_repo_v2_migrated() -> None:
    await do_icechunk_can_read_old_repo("./tests/data/test-repo-v2-migrated")


async def test_icechunk_can_read_old_repo_with_manifest_splitting_v1() -> None:
    await do_icechunk_can_read_old_repo_with_manifest_splitting(
        "./tests/data/split-repo-v1"
    )


async def test_icechunk_can_read_old_repo_with_manifest_splitting_v2() -> None:
    await do_icechunk_can_read_old_repo_with_manifest_splitting(
        "./tests/data/split-repo-v2"
    )


async def test_icechunk_can_read_old_repo_with_manifest_splitting_v2_migrated() -> None:
    await do_icechunk_can_read_old_repo_with_manifest_splitting(
        "./tests/data/split-repo-v2-migrated"
    )


# ---------------------------------------------------------------------------
# Post-expiration fixtures (design-doc 016, pruned_ancestor_tx_logs).
#
# These repos are NOT generated by this file; they are produced by
# tests/data_generation/generate_expired_repos.py, which drives several library
# versions (1.1.21, 2.0.5, working copy) through the same DAG of two expiration
# rounds + GC. Regenerate with `just gen-expired-fixtures`. The histories below
# must match that script's commit messages and the post-expiration ancestries it
# prints. See do_read_expired_repo for what each fixture pins.
# ---------------------------------------------------------------------------

# After two expiration rounds every branch/tag tip is re-parented straight onto
# the initial commit, so each surviving ref's ancestry is just [tip, initial].
EXPIRED_MAIN_HISTORY = ["write j", "Repository initialized"]
EXPIRED_FEATURE_HISTORY = ["feature post-t2", "Repository initialized"]
EXPIRED_PROTECT_B_HISTORY = ["write b", "Repository initialized"]


async def do_read_expired_repo(path: str, *, tracks_tx_logs: bool) -> None:
    """Read a repo that went through two rounds of expiration + GC.

    ``tracks_tx_logs`` selects the expectation: the v2 fixture written by a
    library that populates ``pruned_ancestor_tx_logs`` keeps its expired-ancestor
    logs (so the boundary commit's transaction log inspects as a complete
    synthetic composite and a root-to-tip diff is complete); every other fixture
    lost those logs to GC (degraded but non-crashing).
    """
    repo = ic.Repository.open(storage=ic.local_filesystem_storage(path))

    # ----- common: surviving structure is identical across all fixtures -----
    branches = repo.list_branches()
    assert "main" in branches
    assert "feature" in branches
    assert "doomed1" not in branches
    assert "doomed2" not in branches
    assert "protect-b" in repo.list_tags()

    assert [s.message for s in repo.ancestry(branch="main")] == EXPIRED_MAIN_HISTORY
    assert [s.message for s in repo.ancestry(branch="feature")] == EXPIRED_FEATURE_HISTORY
    assert [
        s.message for s in repo.ancestry(tag="protect-b")
    ] == EXPIRED_PROTECT_B_HISTORY

    # ----- common: array data on the main tip reads back correctly -----
    session = repo.readonly_session(branch="main")
    root = zarr.open_group(store=session.store, mode="r")
    data = cast("zarr.Array[Any]", root["group1/data"])
    small = cast("zarr.Array[Any]", root["group1/small"])
    # j churned row 0 to 99; a..i churned rows 1..8 to their index; 9..11 stay 0.
    assert data[0, 0] == 99
    assert data[5, 0] == 5
    assert data[8, 0] == 8
    assert data[11, 0] == 0
    assert_array_equal(small[:], 0)

    # ----- discriminating: the pruned-ancestor-tx-log feature -----
    main_tip = repo.lookup_branch("main")
    root_snap = list(repo.ancestry(branch="main"))[-1].id
    tx_log = repo.inspect_transaction_log(main_tip)
    diff = repo.diff(from_snapshot_id=root_snap, to_snapshot_id=main_tip)

    tx_dir = Path(path) / "transactions"
    n_tx = sum(1 for _ in tx_dir.iterdir())
    n_snap = sum(1 for _ in (Path(path) / "snapshots").iterdir())

    if tracks_tx_logs:
        composite = tx_log["synthetic_composite"]
        assert composite is not None
        merged = composite["merged_pruned_ancestor_tx_logs"]
        assert merged  # non-empty: the boundary absorbed pruned ancestors
        assert composite["missing_tx_logs"] == []

        # The complete diff sees the original array creation and every churned
        # chunk folded in from the pruned-ancestor logs.
        assert "/group1/data" in diff.new_arrays
        assert "/group1/small" in diff.new_arrays
        assert set(diff.updated_chunks.keys()) == {"/group1/data", "/group1/small"}
        assert len(diff.updated_chunks["/group1/data"]) == 12
        assert len(diff.updated_chunks["/group1/small"]) == 4

        # The referenced pruned-ancestor logs physically survived GC...
        on_disk = {p.name for p in tx_dir.iterdir()}
        assert set(merged) <= on_disk
        # ...so GC retained transaction logs beyond the live snapshots.
        assert n_tx > n_snap
    else:
        # No tracking: the boundary's tx log is a plain log, and the root-to-tip
        # diff only sees the boundary commit's own delta (degraded but no crash).
        assert tx_log.get("synthetic_composite") is None
        assert diff.new_arrays == set()
        # GC kept no transaction logs beyond the live snapshots.
        assert n_tx <= n_snap


@pytest.mark.filterwarnings("ignore:datetime.datetime.utcnow")
async def test_read_expired_repo_v1_by_1_1_21() -> None:
    await do_read_expired_repo(
        "./tests/data/expire-repo-v1-by-1.1.21", tracks_tx_logs=False
    )


@pytest.mark.filterwarnings("ignore:datetime.datetime.utcnow")
async def test_read_expired_repo_v1_by_2_0_5() -> None:
    await do_read_expired_repo(
        "./tests/data/expire-repo-v1-by-2.0.5", tracks_tx_logs=False
    )


@pytest.mark.filterwarnings("ignore:datetime.datetime.utcnow")
async def test_read_expired_repo_v2_by_2_0_5() -> None:
    await do_read_expired_repo(
        "./tests/data/expire-repo-v2-by-2.0.5", tracks_tx_logs=False
    )


@pytest.mark.filterwarnings("ignore:datetime.datetime.utcnow")
async def test_read_expired_repo_v1_by_working_copy() -> None:
    await do_read_expired_repo(
        "./tests/data/expire-repo-v1-by-working-copy",
        tracks_tx_logs=False,
    )


@pytest.mark.filterwarnings("ignore:datetime.datetime.utcnow")
async def test_read_expired_repo_v2_by_working_copy() -> None:
    await do_read_expired_repo(
        "./tests/data/expire-repo-v2-by-working-copy",
        tracks_tx_logs=True,
    )


if __name__ == "__main__":
    import asyncio

    # we import here so it works when the script is ran by pytest

    asyncio.run(write_a_test_repo("./tests/data/test-repo-v2"))
    asyncio.run(write_a_split_repo("./tests/data/split-repo-v2"))

    migrate_repo_v1_to_v2(
        "./tests/data/test-repo-v1", "./tests/data/test-repo-v2-migrated"
    )
    migrate_repo_v1_to_v2(
        "./tests/data/split-repo-v1", "./tests/data/split-repo-v2-migrated"
    )
