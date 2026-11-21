import pickle
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path

import numpy as np
import pytest

import icechunk as ic
import zarr


@pytest.mark.parametrize("use_async", [False, True])
@pytest.mark.parametrize("inline_threshold", [0, 512])
async def test_chunk_refs_pinned_batches(
    use_async: bool, inline_threshold: int, any_spec_version: int | None
) -> None:
    repo = ic.Repository.create(
        ic.in_memory_storage(),
        config=ic.RepositoryConfig(inline_chunk_threshold_bytes=inline_threshold),
        spec_version=any_spec_version,
    )
    session = repo.writable_session("main")
    array = zarr.create_array(
        session.store, shape=(6,), chunks=(1,), dtype="i4", fill_value=-1
    )
    array[:] = [10, 20, -1, 40, 50, 60]
    original = session.commit("source")
    source = repo.readonly_session(snapshot_id=original)
    repo.create_branch("maintenance", original)

    async def get(coordinates: Sequence[Sequence[int]]) -> list[ic.ChunkReference | None]:
        if use_async:
            return await source.get_chunk_refs_async("/", coordinates)
        return source.get_chunk_refs("/", coordinates)

    assert await get([]) == []
    refs = await get([[0], [2], [1], [0], [6]])
    assert refs[0] == refs[3]
    assert refs[1] is None
    assert refs[4] is None
    assert refs[0] != refs[2]
    assert isinstance(refs[0], ic.ChunkReference)
    with pytest.raises(TypeError):
        pickle.dumps(refs[0])
    with pytest.raises(AttributeError):
        refs[0].payload = b"changed"  # type: ignore[attr-defined]
    with pytest.raises(TypeError):
        ic.ChunkReference()

    for start in [0, 2, 4]:
        destination = repo.writable_session("maintenance")
        coords = [[i] for i in range(start, min(start + 2, 5))]
        updates = [
            ([coord[0] + 1], ref)
            for coord, ref in zip(coords, await get(coords), strict=True)
        ]
        if start == 0:
            updates.append(([0], None))
        if use_async:
            await destination.set_chunk_refs_async("/", updates)
        else:
            destination.set_chunk_refs("/", updates)
        completed = destination.commit("move batch")
        del destination

    result = repo.readonly_session(snapshot_id=completed)
    np.testing.assert_array_equal(
        zarr.open_array(result.store)[:], [-1, 10, 20, -1, 40, 50]
    )
    np.testing.assert_array_equal(
        zarr.open_array(source.store)[:], [10, 20, -1, 40, 50, 60]
    )
    assert result.get_chunk_refs("/", [[1], [3], [2], [1]]) == refs[:4]
    assert repo.lookup_branch("main") == original
    repo.reset_branch("main", completed, from_snapshot_id=original)
    assert repo.lookup_branch("main") == completed
    assert source.snapshot_id == original


@pytest.mark.parametrize("use_async", [False, True])
async def test_chunk_refs_without_native_payloads(
    tmp_path: Path, use_async: bool
) -> None:
    repo = ic.Repository.create(
        ic.local_filesystem_storage(str(tmp_path)),
        config=ic.RepositoryConfig(inline_chunk_threshold_bytes=0),
    )
    session = repo.writable_session("main")
    array = zarr.create_array(session.store, shape=(3,), chunks=(1,), dtype="i4")
    array[0] = 42
    snapshot = session.commit("native chunk")
    chunk_files = list((tmp_path / "chunks").iterdir())
    assert chunk_files
    for chunk_file in chunk_files:
        chunk_file.unlink()
    source = repo.readonly_session(snapshot_id=snapshot)
    destination = repo.writable_session("main")
    if use_async:
        refs = await source.get_chunk_refs_async("/", [[0], [1]])
        await destination.set_chunk_refs_async("/", [([1], refs[0]), ([2], refs[1])])
    else:
        refs = source.get_chunk_refs("/", [[0], [1]])
        destination.set_chunk_refs("/", [([1], refs[0]), ([2], refs[1])])
    destination.set_chunk_refs("/", [([0], None)])
    moved = destination.commit("metadata-only move")
    assert (
        repo.readonly_session(snapshot_id=moved).get_chunk_refs("/", [[1], [2]]) == refs
    )
    assert source.get_chunk_refs("/", [[0], [1]]) == refs
    assert list((tmp_path / "chunks").iterdir()) == []


@pytest.mark.parametrize("use_async", [False, True])
async def test_virtual_chunk_refs_preserve_metadata(
    use_async: bool, any_spec_version: int | None
) -> None:
    config = ic.RepositoryConfig()
    config.set_virtual_chunk_container(
        ic.VirtualChunkContainer("s3://unavailable/", ic.s3_store(region="us-east-1"))
    )
    repo = ic.Repository.create(
        ic.in_memory_storage(), config=config, spec_version=any_spec_version
    )
    session = repo.writable_session("main")
    zarr.create_array(session.store, shape=(8,), chunks=(1,), dtype="i4")
    checksums: list[str | datetime | None] = [
        None,
        "etag-one",
        "etag-two",
        datetime(2025, 1, 2, tzinfo=UTC),
    ]
    for i, checksum in enumerate(checksums):
        session.store.set_virtual_ref(
            f"c/{i}",
            "s3://unavailable/chunks",
            offset=17,
            length=4,
            checksum=checksum,
        )
    snapshot = session.commit("unavailable virtual chunks")
    source = repo.readonly_session(snapshot_id=snapshot)
    coordinates = [[i] for i in range(4)]
    refs = (
        await source.get_chunk_refs_async("/", coordinates)
        if use_async
        else source.get_chunk_refs("/", coordinates)
    )
    assert all(ref is not None for ref in refs)
    assert all(refs[i] != refs[j] for i in range(4) for j in range(i))
    destination = repo.writable_session("main")
    updates = [([i + 4], ref) for i, ref in enumerate(refs)]
    if use_async:
        await destination.set_chunk_refs_async("/", updates)
    else:
        destination.set_chunk_refs("/", updates)
    copied = destination.commit("copy references")
    assert (
        repo.readonly_session(snapshot_id=copied).get_chunk_refs(
            "/", [[i] for i in range(4, 8)]
        )
        == refs
    )
    assert source.get_chunk_refs("/", [[i] for i in range(4, 8)]) == [None] * 4


@pytest.mark.parametrize("use_async", [False, True])
async def test_chunk_refs_validation(use_async: bool) -> None:
    repo = ic.Repository.create(ic.in_memory_storage())
    session = repo.writable_session("main")
    root = zarr.group(session.store)
    array = root.create_array("data", shape=(3,), chunks=(1,), dtype="i4")
    array[0] = 42
    session.store.set_virtual_ref(
        "data/c/1", "unsupported://chunks", offset=5, length=4, validate_container=False
    )
    snapshot = session.commit("source")
    source = repo.readonly_session(snapshot_id=snapshot)
    ref, virtual = source.get_chunk_refs("/data", [[0], [1]])
    destination = repo.writable_session("main")

    async def put(
        target: ic.Session,
        path: str,
        updates: Sequence[tuple[Sequence[int], ic.ChunkReference | None]],
    ) -> None:
        if use_async:
            await target.set_chunk_refs_async(path, updates)
        else:
            target.set_chunk_refs(path, updates)

    for coord in ([3], [], [0, 0]):
        with pytest.raises(ic.InvalidInputError) as exc:
            await put(destination, "/data", [([0], None), (coord, ref)])
        assert exc.value.kind == ic.ErrorKind.INVALID_CHUNK_INDEX
        assert not destination.has_uncommitted_changes
        assert source.get_chunk_refs("/data", [coord]) == [None]
    for coord in ([-1], [2**32]):
        with pytest.raises(OverflowError):
            await put(destination, "/data", [([0], None), (coord, ref)])
        assert not destination.has_uncommitted_changes
    with pytest.raises(ic.IcechunkError):
        await put(destination, "/data", [([0], None), ([2], virtual)])
    assert not destination.has_uncommitted_changes
    for path in ("/", "/missing"):
        with pytest.raises(ic.IcechunkError):
            await put(destination, path, [([0], ref)])
        with pytest.raises(ic.IcechunkError):
            source.get_chunk_refs(path, [])
    for updates in ([], [([0], None)]):
        with pytest.raises(ic.ReadOnlyError):
            await put(source, "/data", updates)
        with pytest.raises(ic.SessionStateError):
            await put(repo.rearrange_session("main"), "/data", updates)

    await put(destination, "/data", [])
    assert not destination.has_uncommitted_changes
    await put(destination, "/data", [([2], ref), ([2], None)])
    assert destination.get_chunk_refs("/data", [[2]]) == [None]
    await put(destination, "/data", [([2], None), ([2], ref)])
    assert destination.get_chunk_refs("/data", [[2]]) == [ref]

    other = ic.Repository.create(ic.in_memory_storage()).writable_session("main")
    zarr.create_array(other.store, shape=(3,), chunks=(1,), dtype="i4")
    before = other._session.as_bytes()
    with pytest.raises(ValueError, match="same repository storage handle"):
        await put(other, "/", [([0], None), ([1], ref)])
    assert other._session.as_bytes() == before

    reopened = repo.reopen().writable_session("main")
    await put(reopened, "/data", [([2], ref)])
    assert reopened.get_chunk_refs("/data", [[2]]) == [ref]

    fork = reopened.fork()
    await put(fork, "/data", [([0], None)])
    reopened.merge(fork)
    assert reopened.get_chunk_refs("/data", [[0], [2]]) == [None, ref]


@pytest.mark.parametrize("shape", [(), (2, 2)])
def test_chunk_refs_coordinate_rank(shape: tuple[int, ...]) -> None:
    repo = ic.Repository.create(ic.in_memory_storage())
    session = repo.writable_session("main")
    array = zarr.create_array(
        session.store, shape=shape, chunks=(1,) * len(shape), dtype="i4"
    )
    origin = (0,) * len(shape)
    target = (1,) * len(shape)
    array[origin] = 7
    snapshot = session.commit("source")
    source = repo.readonly_session(snapshot_id=snapshot)
    refs = source.get_chunk_refs("/", (origin,))
    assert refs[0] is not None
    destination = repo.writable_session("main")
    destination.set_chunk_refs("/", [(origin, None), (target, refs[0])])
    result = zarr.open_array(destination.store)
    assert result[target] == 7
    if shape:
        assert result[origin] == 0
    assert source.get_chunk_refs("/", (origin,)) == refs


def test_chunk_refs_reject_independent_storage_handles(tmp_path: Path) -> None:
    repo = ic.Repository.create(ic.local_filesystem_storage(str(tmp_path)))
    session = repo.writable_session("main")
    array = zarr.create_array(session.store, shape=(2,), chunks=(1,), dtype="i4")
    array[0] = 42
    snapshot = session.commit("source")
    refs = repo.readonly_session(snapshot_id=snapshot).get_chunk_refs("/", [[0]])
    other_handle = ic.Repository.open(ic.local_filesystem_storage(str(tmp_path)))
    destination = other_handle.writable_session("main")
    with pytest.raises(ValueError, match="same repository storage handle"):
        destination.set_chunk_refs("/", [([1], refs[0])])
    assert not destination.has_uncommitted_changes
