import pickle
import tempfile
from typing import cast

import pytest

import zarr
from icechunk import (
    ChunkType,
    ForkSession,
    IcechunkError,
    Repository,
    RepositoryConfig,
    SessionMode,
    VirtualChunkContainer,
    VirtualChunkSpec,
    in_memory_storage,
    local_filesystem_storage,
    local_filesystem_store,
)
from icechunk.distributed import merge_sessions


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
    [(10_000, ChunkType.INLINE), (1, ChunkType.NATIVE)],
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

    assert session.chunk_type("/air_temp", [0, 0]) == ChunkType.VIRTUAL
    assert session.chunk_type("/air_temp", [0, 2]) == chunk_type
    assert session.chunk_type("/air_temp", [0, 3]) == ChunkType.UNINITIALIZED

    assert await session.chunk_type_async("/air_temp", [0, 0]) == ChunkType.VIRTUAL
    assert await session.chunk_type_async("/air_temp", [0, 2]) == chunk_type
    assert await session.chunk_type_async("/air_temp", [0, 3]) == ChunkType.UNINITIALIZED


def test_session_mode() -> None:
    repo = Repository.create(storage=in_memory_storage())

    # writable session
    writable = repo.writable_session("main")
    assert writable.mode == SessionMode.WRITABLE
    assert not writable.read_only

    # readonly session from branch
    readonly = repo.readonly_session(branch="main")
    assert readonly.mode == SessionMode.READONLY
    assert readonly.read_only

    # readonly session from snapshot
    readonly_snap = repo.readonly_session(snapshot_id=writable.snapshot_id)
    assert readonly_snap.mode == SessionMode.READONLY
    assert readonly_snap.read_only

    # rearrange session (requires spec_version >= 2)
    repo_v2 = Repository.create(storage=in_memory_storage(), spec_version=2)
    rearrange = repo_v2.rearrange_session("main")
    assert rearrange.mode == SessionMode.REARRANGE
    assert not rearrange.read_only

    # after commit, session becomes readonly
    writable = repo.writable_session("main")
    assert writable.mode == SessionMode.WRITABLE
    # opening a group to make a change is necessary
    # until https://github.com/earth-mover/icechunk/issues/1595 fixed
    zarr.group(writable.store)
    writable.commit("test")
    assert writable.mode == SessionMode.READONLY  # type: ignore[comparison-overlap]
