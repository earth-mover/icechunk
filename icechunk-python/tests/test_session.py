import pickle
import tempfile
import time
from typing import Any, cast

import pytest

import zarr
import zarr.core.array
from icechunk import (
    ChunkType,
    ForkSession,
    IcechunkError,
    RepoAvailability,
    Repository,
    RepositoryConfig,
    RepoStatus,
    SessionMode,
    VirtualChunkContainer,
    VirtualChunkSpec,
    in_memory_storage,
    local_filesystem_storage,
    local_filesystem_store,
    s3_storage,
)
from icechunk.distributed import merge_sessions
from tests.conftest import Permission


@pytest.mark.parametrize("use_async", [True, False])
async def test_session_fork(use_async: bool, any_spec_version: int | None) -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        repo = Repository.create(
            local_filesystem_storage(tmpdir),
            spec_version=any_spec_version,
        )
        session = repo.writable_session("main")
        zarr.group(session.store)
        assert session.has_uncommitted_changes

        with pytest.raises(ValueError):
            session.fork()

        if use_async:
            await session.commit_async("init")
        else:
            session.commit("init")

        # forking a read-only session
        with pytest.raises(ValueError):
            session.fork()
        # readonly sessions can be pickled directly
        with pytest.raises(ValueError, match="can be pickled"):
            pickle.loads(pickle.dumps(session.fork()))
        pickle.loads(pickle.dumps(session))

        session = repo.writable_session("main")
        fork = pickle.loads(pickle.dumps(session.fork()))
        zarr.create_group(fork.store, path="/foo")
        assert not session.has_uncommitted_changes
        assert fork.has_uncommitted_changes
        with pytest.warns(UserWarning):
            with pytest.raises(IcechunkError, match="cannot commit"):
                session.commit("foo")
        if use_async:
            await session.merge_async(fork)
            await session.commit_async("foo")
        else:
            session.merge(fork)
            session.commit("foo")

        session = repo.writable_session("main")
        fork1 = pickle.loads(pickle.dumps(session.fork()))
        fork2 = pickle.loads(pickle.dumps(session.fork()))
        zarr.create_group(fork1.store, path="/foo1")
        zarr.create_group(fork2.store, path="/foo2")

        with pytest.raises(TypeError, match="Cannot commit a fork"):
            fork1.commit("foo")

        fork1 = pickle.loads(pickle.dumps(fork1))
        fork2 = pickle.loads(pickle.dumps(fork2))

        # this is wrong
        with pytest.raises(TypeError, match="Received 'Session'"):
            merge_sessions(cast(ForkSession, session), fork1, fork2)
        # this is right
        if use_async:
            await session.merge_async(fork1, fork2)
            await session.commit_async("all done")
        else:
            session.merge(fork1, fork2)
            session.commit("all done")

        groups = set(
            name for name, _ in zarr.open_group(session.store, mode="r").groups()
        )
        assert groups == {"foo", "foo1", "foo2"}

        # forking a forked session may be useful
        session = repo.writable_session("main")
        fork1 = pickle.loads(pickle.dumps(session.fork()))
        fork2 = pickle.loads(pickle.dumps(fork1.fork()))
        zarr.create_group(fork1.store, path="/foo3")
        with pytest.raises(ValueError):
            fork1.fork()
        zarr.create_group(fork2.store, path="/foo4")

        fork1 = pickle.loads(pickle.dumps(fork1))
        fork2 = pickle.loads(pickle.dumps(fork2))
        session.merge(fork1, fork2)

        groups = set(
            name for name, _ in zarr.open_group(session.store, mode="r").groups()
        )
        assert groups == {"foo", "foo1", "foo2", "foo3", "foo4"}


@pytest.mark.parametrize(
    "inline_threshold,chunk_type",
    [(10_000, ChunkType.inline), (1, ChunkType.native)],
    ids=["inline", "native"],
)
async def test_chunk_type(
    inline_threshold: int, chunk_type: ChunkType, any_spec_version: int | None
) -> None:
    config = RepositoryConfig.default()
    config.inline_chunk_threshold_bytes = inline_threshold
    store_config = local_filesystem_store("/foo")
    container = VirtualChunkContainer("file:///foo/", store_config)
    config.set_virtual_chunk_container(container)

    repo = Repository.create(
        storage=in_memory_storage(),
        config=config,
        spec_version=any_spec_version,
    )

    session = repo.writable_session("main")
    store = session.store
    group = zarr.group(store=store, overwrite=True)
    air_temp = group.create_array("air_temp", shape=(1, 4), chunks=(1, 1), dtype="i4")

    # set index [0, 0] to be a virtual chunk
    # note: we can't ACCESS it, since the file `foo` is never instantiated
    store.set_virtual_refs(
        array_path="/air_temp",
        validate_containers=False,
        chunks=[
            VirtualChunkSpec(
                index=[0, 0],
                location="file:///foo",
                offset=0,
                length=1_000,
            ),
        ],
    )

    # This forces the chunk to be initialized, either to inline or native chunks
    air_temp[0, 2] = 42
    assert air_temp[0, 2] == 42

    assert session.chunk_type("/air_temp", [0, 0]) == ChunkType.virtual
    assert session.chunk_type("/air_temp", [0, 2]) == chunk_type
    assert session.chunk_type("/air_temp", [0, 3]) == ChunkType.uninitialized

    assert await session.chunk_type_async("/air_temp", [0, 0]) == ChunkType.virtual
    assert await session.chunk_type_async("/air_temp", [0, 2]) == chunk_type
    assert await session.chunk_type_async("/air_temp", [0, 3]) == ChunkType.uninitialized


def test_session_mode() -> None:
    repo = Repository.create(storage=in_memory_storage())

    # writable session
    writable = repo.writable_session("main")
    assert writable.mode == SessionMode.writable
    assert not writable.read_only

    # readonly session from branch
    readonly = repo.readonly_session(branch="main")
    assert readonly.mode == SessionMode.readonly
    assert readonly.read_only

    # readonly session from snapshot
    readonly_snap = repo.readonly_session(snapshot_id=writable.snapshot_id)
    assert readonly_snap.mode == SessionMode.readonly
    assert readonly_snap.read_only

    # rearrange session (requires spec_version >= 2)
    repo_v2 = Repository.create(storage=in_memory_storage(), spec_version=2)
    rearrange = repo_v2.rearrange_session("main")
    assert rearrange.mode == SessionMode.rearrange
    assert not rearrange.read_only

    # after commit, session becomes readonly
    writable = repo.writable_session("main")
    assert writable.mode == SessionMode.writable
    writable.commit("test", allow_empty=True)
    assert writable.mode == SessionMode.readonly  # type: ignore[comparison-overlap]


def test_repository_open_no_list_bucket(any_spec_version: int | None) -> None:
    prefix = "test-repo__" + str(time.time())
    (access_key_id, secret_access_key) = Permission.MODIFY.keys()
    write_storage = s3_storage(
        endpoint_url="http://localhost:9000",
        allow_http=True,
        force_path_style=True,
        region="us-east-1",
        bucket="testbucket",
        prefix=prefix,
        access_key_id=access_key_id,
        secret_access_key=secret_access_key,
    )
    (access_key_id, secret_access_key) = Permission.READONLY.keys()
    readonly_storage = s3_storage(
        endpoint_url="http://localhost:9000",
        allow_http=True,
        force_path_style=True,
        region="us-east-1",
        bucket="testbucket",
        prefix=prefix,
        access_key_id=access_key_id,
        secret_access_key=secret_access_key,
    )

    config = RepositoryConfig.default()

    # Create a repo with one array, with modify permissions
    repo = Repository.create(
        storage=write_storage, config=config, spec_version=any_spec_version
    )
    session = repo.writable_session("main")
    store = session.store
    group = zarr.group(store=store, overwrite=True)
    air_temp = group.create_array("air_temp", shape=(1, 4), chunks=(1, 1), dtype="i4")
    air_temp[0, 2] = 42
    snapshot_id = session.commit("init")
    repo.create_tag("new_tag", snapshot_id)
    repo.create_branch("new_branch", snapshot_id)

    session = repo.writable_session("main")
    store = session.store
    group = zarr.open_group(store=store, mode="a")
    air_temp = cast("zarr.core.array.Array[Any]", group["air_temp"])
    air_temp[0, 3] = 41
    session.commit("tag and branch")

    # Opening the repo with a storage without ListBucket permissions
    repo = Repository.open(storage=readonly_storage, config=config)
    if any_spec_version:
        assert repo.spec_version == any_spec_version
    readonly = repo.readonly_session(branch="main")
    group = zarr.open_group(store=readonly.store, mode="r")
    air_temp = cast("zarr.core.array.Array[Any]", group["air_temp"])
    assert air_temp[0, 2] == 42
    assert air_temp[0, 3] == 41
    assert air_temp.chunks == (1, 1)
    assert air_temp.size == 4

    assert len(list(repo.ancestry(branch="main"))) == 3
    assert len(list(repo.ancestry(branch="new_branch"))) == 2
    if repo.spec_version == 1:
        # This should fail for v1 spec, since listing branches and tags
        # try to list from the object store instead of reading from
        # repo_info like in a v2 repo
        with pytest.raises(IcechunkError) as e:
            assert repo.list_branches() == set(["main"])
            assert "error listing objects" in e.value.message
        with pytest.raises(IcechunkError) as e:
            assert len(list(repo.list_tags())) == 1
            assert "error listing objects" in e.value.message
        # skip ops log check, need repo v2
    else:
        assert repo.list_branches() == set(["main", "new_branch"])
        assert list(repo.list_tags()) == ["new_tag"]
        assert len(list(repo.ops_log())) == 5


def test_readonly_repo_status_blocks_writable_session() -> None:
    repo = Repository.create(
        storage=in_memory_storage(),
        spec_version=2,
    )

    session = repo.writable_session("main")
    session.commit("initial commit", allow_empty=True)

    # Writable session works before setting ReadOnly
    session = repo.writable_session("main")
    assert not session.read_only

    repo.set_status(RepoStatus(availability=RepoAvailability.ReadOnly))
    assert repo.status.availability == RepoAvailability.ReadOnly

    # Writable session should fail when repo is ReadOnly
    with pytest.raises(IcechunkError):
        repo.writable_session("main")

    # Read-only sessions should still work
    session = repo.readonly_session("main")
    assert session.read_only

    # Set back to Online and verify writable session works again
    repo.set_status(RepoStatus(availability=RepoAvailability.Online))
    assert repo.status.availability == RepoAvailability.Online

    session = repo.writable_session("main")
    assert not session.read_only


def test_readonly_repo_status_blocks_rearrange_session() -> None:
    repo = Repository.create(
        storage=in_memory_storage(),
        spec_version=2,
    )

    session = repo.writable_session("main")
    session.commit("initial commit", allow_empty=True)

    # Rearrange session works before setting ReadOnly
    session = repo.rearrange_session("main")
    assert not session.read_only

    repo.set_status(RepoStatus(availability=RepoAvailability.ReadOnly))
    assert repo.status.availability == RepoAvailability.ReadOnly

    # Rearrange session should fail when repo is ReadOnly
    with pytest.raises(IcechunkError):
        repo.rearrange_session("main")

    # Read-only sessions should still work
    session = repo.readonly_session("main")
    assert session.read_only

    # Set back to Online and verify rearrange session works again
    repo.set_status(RepoStatus(availability=RepoAvailability.Online))
    assert repo.status.availability == RepoAvailability.Online

    session = repo.rearrange_session("main")
    assert not session.read_only


async def test_readonly_repo_status_blocks_rearrange_session_async() -> None:
    repo = await Repository.create_async(
        storage=in_memory_storage(),
        spec_version=2,
    )

    session = await repo.writable_session_async("main")
    await session.commit_async("initial commit", allow_empty=True)

    await repo.set_status_async(RepoStatus(availability=RepoAvailability.ReadOnly))
    status = await repo.get_status_async()
    assert status.availability == RepoAvailability.ReadOnly

    with pytest.raises(IcechunkError):
        await repo.rearrange_session_async("main")

    session = await repo.readonly_session_async("main")
    assert session.read_only

    await repo.set_status_async(RepoStatus(availability=RepoAvailability.Online))
    status = await repo.get_status_async()
    assert status.availability == RepoAvailability.Online

    session = await repo.rearrange_session_async("main")
    assert not session.read_only


async def test_readonly_repo_status_blocks_writable_session_async() -> None:
    repo = await Repository.create_async(
        storage=in_memory_storage(),
        spec_version=2,
    )

    session = await repo.writable_session_async("main")
    await session.commit_async("initial commit", allow_empty=True)

    await repo.set_status_async(RepoStatus(availability=RepoAvailability.ReadOnly))
    status = await repo.get_status_async()
    assert status.availability == RepoAvailability.ReadOnly

    with pytest.raises(IcechunkError):
        await repo.writable_session_async("main")
