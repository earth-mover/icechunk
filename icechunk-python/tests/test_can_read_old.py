"""This test reads a repository generated with an older version of icechunk.

In this way, we check we maintain read compatibility. The repository lives
in the git repository, as a filesystem store, in the directory icechunk-python/tests/data/test-repo

If something changes in the on disk format, we probably won't be able to read the repo, the
test will fail and we can avoid breaking user data.

When new features that impact the stored info are added, or when the on-disk format is
intentionally changed, the repository files must be regenerated. For that, delete the
fixture directories and run `just gen-can-read-old-fixtures <set>...`, see FIXTURE_SETS.
"""

import hashlib
import os
import shutil
import sys
import tempfile
import time
import uuid
from collections.abc import Callable, Coroutine
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
    from tests.conftest import Permission, fs_storage, readable_path
except ImportError:
    from conftest import (  # type: ignore[import-not-found,no-redef]
        Permission,
        fs_storage,
        readable_path,
    )


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
    *,
    create: bool,
    store_path: str,
    backend: str,
    config: ic.RepositoryConfig | None = None,
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
        storage=fs_storage(backend, store_path),
        config=config,
        authorize_virtual_chunk_access=credentials,
    )

    return repo


async def write_a_split_repo(path: str, *, backend: str) -> None:
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
    repo = mk_repo(create=True, store_path=store_path, config=config, backend=backend)
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
    repo = mk_repo(create=False, store_path=store_path, config=config, backend=backend)
    repo.save_config()
    session = repo.writable_session("main")
    big_chunks = zarr.open_array(session.store, path="group1/split", mode="a")
    small_chunks = zarr.open_array(session.store, path="group1/small_chunks", mode="a")
    big_chunks[:] = 14
    small_chunks[:] = 3
    session.commit("write data again with more splits")


async def do_icechunk_can_read_old_repo_with_manifest_splitting(
    path: str, *, backend: str
) -> None:
    repo = mk_repo(create=False, store_path=path, backend=backend)
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


async def write_a_test_repo(path: str, *, backend: str) -> None:
    """Write the test repository.

    This function tries to explore as many icechunk features as possible, to generate
    an richer repository on disk. For example, it does several commits, it has a hierarchy,
    it has virtual, inline and materialized chunks, branches and tags, etc.

    PLEASE: keep addign more actions to this function as we add more features to Icechunk.
    """

    print(f"Writing repository to {path}")
    repo = mk_repo(create=True, store_path=path, backend=backend)
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


async def write_a_frozen_repo(path: str, *, unfreeze: bool) -> None:
    """Write the test repository in the filesystem format and freeze it.

    With ``unfreeze`` it is unfrozen right after, and nothing commits after that,
    so the current version is the one unfreeze linked from the frozen one.
    """
    await write_a_test_repo(path, backend="filesystem")
    storage = ic.filesystem_storage(path)
    assert storage.freeze_repo()
    if unfreeze:
        assert storage.unfreeze_repo()


# What a GC sweeps from leftovers-repo-v2-fs: the stage, the version below the
# marker, the creation directory and the stub staging file.
LEFTOVERS_SWEPT = 4


def repo_d_versions(path: str | Path) -> tuple[int, set[int]]:
    """The current version of `repo`, and the versions of every file in repo.d."""
    names = [p.name for p in (Path(path) / "repo.d").iterdir()]
    (marker,) = [n for n in names if n.startswith(("latest.", "frozen."))]
    current = int(marker.split(".")[2])
    versions = {int(n.split(".")[0]) for n in names if n.split(".")[0].isdigit()}
    return current, versions


def leftover_counts(path: str | Path) -> dict[str, int]:
    """How many of each leftover planted by write_a_leftovers_repo are in ``path``."""
    patterns = {
        "stage": "repo.d/stage.*",
        "tmp": "repo.d.tmp.*",
        "stub": "repo.stub.*",
        "staging": "overwritten/*.staging.*",
    }
    return {kind: len(list(Path(path).glob(p))) for kind, p in patterns.items()}


def age_tree(path: str | Path) -> None:
    """Make everything under ``path`` older than GC's minimum age for leftovers."""
    two_days_ago = time.time() - 2 * 24 * 3600
    for dirpath, _, files in os.walk(path):
        for name in [*files, "."]:
            os.utime(Path(dirpath, name), (two_days_ago, two_days_ago))


def sweep_count(path: str | Path, *, dry_run: bool) -> int:
    repo = ic.Repository.open(ic.filesystem_storage(str(path)))
    return repo.garbage_collect(
        datetime.now(UTC), dry_run=dry_run
    ).storage_leftovers_deleted


async def write_a_leftovers_repo(path: str) -> None:
    """Write the test repository in the filesystem format, then add one of each
    leftover that a crash of `filesystem_storage` can leave behind.

    The leftovers are planted by name, so the generator checks that the current
    code's GC recognizes the ones it must sweep.
    """
    await write_a_test_repo(path, backend="filesystem")
    root = Path(path)
    repo_d = root / "repo.d"
    current, _ = repo_d_versions(path)
    # Earlier versions of `repo`, as the content of the leftover versions.
    infos = [p.read_bytes() for p in sorted((root / "overwritten").iterdir())]

    def plant_version(directory: Path, n: int, data: bytes) -> str:
        h = hashlib.sha256(data).hexdigest()
        (directory / f"{n}.{h}").write_bytes(data)
        return h

    # A commit that crashed before linking its stage.
    (repo_d / f"stage.{uuid.uuid4()}").write_bytes(infos[0])
    # A commit that crashed between its link and its marker rename.
    plant_version(repo_d, current + 1, infos[1])
    # A commit that crashed between its marker rename and its tidy-up.
    plant_version(repo_d, current - 1, infos[2])
    # A creation that crashed before renaming its directory to repo.d.
    tmp = root / f"repo.d.tmp.{uuid.uuid4()}"
    tmp.mkdir()
    h = plant_version(tmp, 0, infos[3])
    (tmp / f"latest.{999_999_999_999_999:015d}.0.{h}").touch()
    # A creation that crashed while staging the stub.
    (root / f"repo.stub.{uuid.uuid4()}").write_bytes((root / "repo").read_bytes())
    # A write of a backup of `repo` that crashed before its rename.
    backup = next((root / "overwritten").iterdir())
    backup.with_name(f"{backup.name}.staging.{uuid.uuid4()}").write_bytes(
        backup.read_bytes()
    )

    with tempfile.TemporaryDirectory() as tmp_dir:
        copy = Path(tmp_dir) / "repo"
        shutil.copytree(path, copy)
        age_tree(copy)
        assert sweep_count(copy, dry_run=True) == LEFTOVERS_SWEPT


async def do_icechunk_can_read_old_repo(path: str, *, backend: str) -> None:
    try:
        from tests.conftest import write_chunks_to_minio
    except ImportError:
        from conftest import write_chunks_to_minio  # type: ignore[no-redef]

    repo = mk_repo(create=False, store_path=path, backend=backend)
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
    repo = mk_repo(create=False, store_path=to_dir, backend="local_filesystem")
    ic.upgrade_icechunk_repository(repo, dry_run=False)


@pytest.mark.filterwarnings("ignore:datetime.datetime.utcnow")
async def test_icechunk_can_read_old_repo_v1() -> None:
    await do_icechunk_can_read_old_repo(
        "./tests/data/test-repo-v1", backend="local_filesystem"
    )


@pytest.mark.filterwarnings("ignore:datetime.datetime.utcnow")
async def test_icechunk_can_read_old_repo_v2(fs_backend: str, tmp_path: Path) -> None:
    path = readable_path("./tests/data/test-repo-v2", fs_backend, tmp_path)
    await do_icechunk_can_read_old_repo(path, backend=fs_backend)


@pytest.mark.filterwarnings("ignore:datetime.datetime.utcnow")
async def test_icechunk_can_read_old_repo_v2_migrated(
    fs_backend: str, tmp_path: Path
) -> None:
    path = readable_path("./tests/data/test-repo-v2-migrated", fs_backend, tmp_path)
    await do_icechunk_can_read_old_repo(path, backend=fs_backend)


async def test_icechunk_can_read_old_repo_with_manifest_splitting_v1() -> None:
    await do_icechunk_can_read_old_repo_with_manifest_splitting(
        "./tests/data/split-repo-v1", backend="local_filesystem"
    )


async def test_icechunk_can_read_old_repo_with_manifest_splitting_v2(
    fs_backend: str, tmp_path: Path
) -> None:
    path = readable_path("./tests/data/split-repo-v2", fs_backend, tmp_path)
    await do_icechunk_can_read_old_repo_with_manifest_splitting(path, backend=fs_backend)


async def test_icechunk_can_read_old_repo_with_manifest_splitting_v2_migrated(
    fs_backend: str, tmp_path: Path
) -> None:
    path = readable_path("./tests/data/split-repo-v2-migrated", fs_backend, tmp_path)
    await do_icechunk_can_read_old_repo_with_manifest_splitting(path, backend=fs_backend)


# Fixtures written natively by `filesystem_storage`, pinning its repo.d format.
unix_only = pytest.mark.skipif(
    sys.platform == "win32", reason="FilesystemStorage is Unix-only"
)


@unix_only
@pytest.mark.filterwarnings("ignore:datetime.datetime.utcnow")
async def test_icechunk_can_read_old_repo_v2_fs() -> None:
    await do_icechunk_can_read_old_repo(
        "./tests/data/test-repo-v2-fs", backend="filesystem"
    )


@unix_only
async def test_icechunk_can_read_old_repo_with_manifest_splitting_v2_fs() -> None:
    await do_icechunk_can_read_old_repo_with_manifest_splitting(
        "./tests/data/split-repo-v2-fs", backend="filesystem"
    )


@pytest.mark.parametrize(
    "path", ["./tests/data/test-repo-v2-fs", "./tests/data/split-repo-v2-fs"]
)
def test_local_filesystem_refuses_v2_fs_repo(path: str) -> None:
    with pytest.raises(ic.IcechunkError, match="uses the filesystem format"):
        ic.Repository.open(storage=ic.local_filesystem_storage(path))


def commit_to_copy(path: str, tmp_path: Path) -> None:
    """Commit to a copy of the fixture at ``path``, which stays untouched."""
    copy = tmp_path / "repo"
    shutil.copytree(path, copy)
    session = mk_repo(
        create=False, store_path=str(copy), backend="filesystem"
    ).writable_session("main")
    small_chunks = zarr.open_array(session.store, path="group1/small_chunks", mode="a")
    small_chunks[:] = 7
    session.commit("commit to a copy")


@unix_only
@pytest.mark.filterwarnings("ignore:datetime.datetime.utcnow")
async def test_icechunk_can_read_old_frozen_repo_v2_fs(tmp_path: Path) -> None:
    path = "./tests/data/frozen-repo-v2-fs"
    await do_icechunk_can_read_old_repo(path, backend="filesystem")

    with pytest.raises(ic.IcechunkError, match="frozen"):
        commit_to_copy(path, tmp_path / "frozen")

    copy = tmp_path / "unfrozen"
    shutil.copytree(path, copy)
    assert ic.filesystem_storage(str(copy)).unfreeze_repo()
    commit_to_copy(str(copy), tmp_path)


@unix_only
@pytest.mark.filterwarnings("ignore:datetime.datetime.utcnow")
async def test_icechunk_can_read_old_unfrozen_repo_v2_fs(tmp_path: Path) -> None:
    path = "./tests/data/unfrozen-repo-v2-fs"
    await do_icechunk_can_read_old_repo(path, backend="filesystem")
    commit_to_copy(path, tmp_path)


@unix_only
@pytest.mark.filterwarnings("ignore:datetime.datetime.utcnow")
async def test_icechunk_can_read_old_leftovers_repo_v2_fs(tmp_path: Path) -> None:
    path = "./tests/data/leftovers-repo-v2-fs"
    current, versions = repo_d_versions(path)
    assert versions == {current - 1, current, current + 1}
    assert leftover_counts(path) == {"stage": 1, "tmp": 1, "stub": 1, "staging": 1}
    await do_icechunk_can_read_old_repo(path, backend="filesystem")

    # A checkout sets every mtime to now, younger than GC sweeps.
    copy = tmp_path / "repo"
    shutil.copytree(path, copy)
    age_tree(copy)
    # Not the version above the marker, which may be a commit in flight.
    assert sweep_count(copy, dry_run=True) == LEFTOVERS_SWEPT
    assert sweep_count(copy, dry_run=False) == LEFTOVERS_SWEPT
    # GC commits too, so the version above the marker ends up dead or current.
    new_current, versions = repo_d_versions(copy)
    assert versions == {new_current}
    # Nested staging files are never swept.
    assert leftover_counts(copy) == {"stage": 0, "tmp": 0, "stub": 0, "staging": 1}
    await do_icechunk_can_read_old_repo(str(copy), backend="filesystem")
    commit_to_copy(str(copy), tmp_path / "commit")


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


async def do_read_expired_repo(path: str, *, tracks_tx_logs: bool, backend: str) -> None:
    """Read a repo that went through two rounds of expiration + GC.

    ``tracks_tx_logs`` selects the expectation: the v2 fixture written by a
    library that populates ``pruned_ancestor_tx_logs`` keeps its expired-ancestor
    logs (so the boundary commit's transaction log inspects as a complete
    synthetic composite and a root-to-tip diff is complete); every other fixture
    lost those logs to GC (degraded but non-crashing).
    """
    repo = ic.Repository.open(storage=fs_storage(backend, path))

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
        "./tests/data/expire-repo-v1-by-1.1.21",
        tracks_tx_logs=False,
        backend="local_filesystem",
    )


@pytest.mark.filterwarnings("ignore:datetime.datetime.utcnow")
async def test_read_expired_repo_v1_by_2_0_5() -> None:
    await do_read_expired_repo(
        "./tests/data/expire-repo-v1-by-2.0.5",
        tracks_tx_logs=False,
        backend="local_filesystem",
    )


@pytest.mark.filterwarnings("ignore:datetime.datetime.utcnow")
async def test_read_expired_repo_v2_by_2_0_5(fs_backend: str, tmp_path: Path) -> None:
    path = readable_path("./tests/data/expire-repo-v2-by-2.0.5", fs_backend, tmp_path)
    await do_read_expired_repo(path, tracks_tx_logs=False, backend=fs_backend)


@pytest.mark.filterwarnings("ignore:datetime.datetime.utcnow")
async def test_read_expired_repo_v1_by_working_copy() -> None:
    await do_read_expired_repo(
        "./tests/data/expire-repo-v1-by-working-copy",
        tracks_tx_logs=False,
        backend="local_filesystem",
    )


@pytest.mark.filterwarnings("ignore:datetime.datetime.utcnow")
async def test_read_expired_repo_v2_by_working_copy(
    fs_backend: str, tmp_path: Path
) -> None:
    path = readable_path(
        "./tests/data/expire-repo-v2-by-working-copy", fs_backend, tmp_path
    )
    await do_read_expired_repo(path, tracks_tx_logs=True, backend=fs_backend)


async def migrate_v1_fixture(from_dir: str, to_dir: str) -> None:
    migrate_repo_v1_to_v2(from_dir, to_dir)


Writer = Callable[[str], Coroutine[Any, Any, None]]

# Each set maps the directories under tests/data it holds to their writers.
FIXTURE_SETS: dict[str, dict[str, Writer]] = {
    "v2": {
        "test-repo-v2": lambda path: write_a_test_repo(path, backend="local_filesystem"),
        "split-repo-v2": lambda path: write_a_split_repo(
            path, backend="local_filesystem"
        ),
    },
    "v2-migrated": {
        "test-repo-v2-migrated": lambda path: migrate_v1_fixture(
            "./tests/data/test-repo-v1", path
        ),
        "split-repo-v2-migrated": lambda path: migrate_v1_fixture(
            "./tests/data/split-repo-v1", path
        ),
    },
    "v2-fs": {
        "test-repo-v2-fs": lambda path: write_a_test_repo(path, backend="filesystem"),
        "split-repo-v2-fs": lambda path: write_a_split_repo(path, backend="filesystem"),
        "frozen-repo-v2-fs": lambda path: write_a_frozen_repo(path, unfreeze=False),
        "unfrozen-repo-v2-fs": lambda path: write_a_frozen_repo(path, unfreeze=True),
        "leftovers-repo-v2-fs": write_a_leftovers_repo,
    },
}


if __name__ == "__main__":
    import asyncio

    sets = sys.argv[1:]
    if not sets or not set(sets) <= FIXTURE_SETS.keys():
        sys.exit(f"usage: {sys.argv[0]} {{{','.join(FIXTURE_SETS)}}}...")
    for name in sets:
        for directory, writer in FIXTURE_SETS[name].items():
            path = f"./tests/data/{directory}"
            if Path(path).exists():
                print(f"Skipping {path}: it exists, delete it to regenerate")
            else:
                asyncio.run(writer(path))
