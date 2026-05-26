"""Smoke test for `icechunk.from_zarr`.

Builds a tiny zarr v3 hierarchy in an obstore `MemoryStore`, copies it
into an icechunk repo via `from_zarr`, and verifies the round-trip.
"""

from __future__ import annotations

import numpy as np
import obstore.store
import pytest

import icechunk
import zarr
import zarr.storage
from icechunk.ingest import IngestResult
from icechunk.testing.models import ArrayNode, GroupNode


@pytest.fixture
def src_store(tmp_path) -> tuple[zarr.storage.ObjectStore, icechunk.Storage]:  # type: ignore[no-untyped-def]
    """A zarr 3.x store backed by an obstore LocalStore (cross-library
    safe, unlike MemoryStore which can't be cloned across crates), paired
    with an `icechunk.Storage` pointing at the same path for `from_zarr`.
    Builds one tiny array we can sanity-check after copy."""
    raw = obstore.store.LocalStore(str(tmp_path))
    src = zarr.storage.ObjectStore(raw)
    root = zarr.create_group(store=src, overwrite=True)
    arr = root.create_array(
        name="a",
        shape=(4, 4),
        chunks=(2, 2),
        dtype="uint8",
    )
    arr[:] = np.arange(16, dtype="uint8").reshape(4, 4)
    return src, icechunk.local_filesystem_storage(str(tmp_path))


def test_from_zarr_roundtrip(
    src_store: tuple[zarr.storage.ObjectStore, icechunk.Storage],
) -> None:
    _src, src_storage = src_store
    repo = icechunk.Repository.create(icechunk.in_memory_storage())

    result = icechunk.from_zarr(src_storage, repo, message="ingest test")

    assert isinstance(result, IngestResult)
    assert result.stats.keys == 6  # root + array zarr.json + 4 chunks
    assert result.stats.bytes > 0
    assert result.snapshot_id

    # Read back and verify byte-for-byte the array matches.
    sess = repo.readonly_session(branch="main")
    out_root = zarr.open_group(store=sess.store, mode="r")
    out_arr = out_root["a"]
    expected = np.arange(16, dtype="uint8").reshape(4, 4)
    np.testing.assert_array_equal(np.asarray(out_arr[:]), expected)


def test_from_zarr_rejects_unsupported_source() -> None:
    repo = icechunk.Repository.create(icechunk.in_memory_storage())
    # Anything that is not an `icechunk.Storage` must be rejected.
    with pytest.raises(TypeError):
        icechunk.from_zarr(object(), repo)


def make_src(tmp_path_or_str: str, value: int) -> icechunk.Storage:
    """Build a tiny source store with a single 1-element array filled with `value`
    and return an `icechunk.Storage` pointing at the same directory."""
    raw = obstore.store.LocalStore(tmp_path_or_str)
    src = zarr.storage.ObjectStore(raw)
    tree = GroupNode(
        children={
            "x": ArrayNode(
                shape=(1,),
                dtype=np.dtype("uint8"),
                chunks=(1,),
                data=np.array([value], dtype="uint8"),
            ),
        },
    )
    tree.materialize(src)
    return icechunk.local_filesystem_storage(tmp_path_or_str)


def test_overwrite_clobbers(tmp_path_factory: pytest.TempPathFactory) -> None:
    """overwrite=True replaces existing destination keys with the new bytes."""
    src1_dir = tmp_path_factory.mktemp("src1")
    src2_dir = tmp_path_factory.mktemp("src2")

    src1 = make_src(str(src1_dir), value=42)
    src2 = make_src(str(src2_dir), value=99)

    repo = icechunk.Repository.create(icechunk.in_memory_storage())

    # First ingest — no collision yet
    icechunk.from_zarr(src1, repo, message="first")

    # Second ingest with on_collision=Overwrite — should replace
    result = icechunk.from_zarr(
        src2,
        repo,
        on_collision=icechunk.CollisionPolicy.Overwrite,
        message="overwrite",
    )

    assert isinstance(result, IngestResult)
    sess = repo.readonly_session(branch="main")
    out_root = zarr.open_group(store=sess.store, mode="r")
    got = np.asarray(out_root["x"][:])
    np.testing.assert_array_equal(got, np.array([99], dtype="uint8"))


def test_overwrite_collision_raises(tmp_path_factory: pytest.TempPathFactory) -> None:
    """The default Fail policy raises ValueError when a key already exists."""
    src1_dir = tmp_path_factory.mktemp("src1")
    src2_dir = tmp_path_factory.mktemp("src2")

    src1 = make_src(str(src1_dir), value=1)
    src2 = make_src(str(src2_dir), value=2)

    repo = icechunk.Repository.create(icechunk.in_memory_storage())
    icechunk.from_zarr(src1, repo, message="first")

    with pytest.raises(ValueError):
        icechunk.from_zarr(src2, repo, message="collision")


def test_session_sink_rejected(tmp_path: pytest.TempPathFactory) -> None:
    """Passing a Session as the sink must raise TypeError; only Repository is accepted."""
    raw = obstore.store.LocalStore(str(tmp_path))
    src = zarr.storage.ObjectStore(raw)
    root = zarr.create_group(store=src, overwrite=True)
    arr = root.create_array(name="b", shape=(2,), chunks=(2,), dtype="uint8")
    arr[:] = np.array([7, 8], dtype="uint8")

    repo = icechunk.Repository.create(icechunk.in_memory_storage())
    session = repo.writable_session("main")

    src_storage = icechunk.local_filesystem_storage(str(tmp_path))
    with pytest.raises(TypeError):
        icechunk.from_zarr(src_storage, session, message="session sink test")


def test_progress_callback_called(tmp_path: pytest.TempPathFactory) -> None:
    """The on_progress callback is invoked at least once with monotonically increasing keys."""
    from icechunk.ingest import IngestStats

    raw = obstore.store.LocalStore(str(tmp_path))
    src = zarr.storage.ObjectStore(raw)
    root = zarr.create_group(store=src, overwrite=True)
    arr = root.create_array(name="c", shape=(4,), chunks=(1,), dtype="uint8")
    arr[:] = np.array([1, 2, 3, 4], dtype="uint8")

    repo = icechunk.Repository.create(icechunk.in_memory_storage())

    calls: list[IngestStats] = []

    def cb(stats: IngestStats) -> None:
        calls.append(stats)

    src_storage = icechunk.local_filesystem_storage(str(tmp_path))
    icechunk.from_zarr(src_storage, repo, on_progress=cb, message="progress test")

    assert len(calls) >= 1, "progress callback was never called"
    for i in range(1, len(calls)):
        assert calls[i].keys >= calls[i - 1].keys
        assert calls[i].bytes >= calls[i - 1].bytes


def test_empty_source(tmp_path: pytest.TempPathFactory) -> None:
    """An empty zarr hierarchy (root zarr.json only) ingests without error."""
    raw = obstore.store.LocalStore(str(tmp_path))
    src = zarr.storage.ObjectStore(raw)
    zarr.create_group(store=src, overwrite=True)

    repo = icechunk.Repository.create(icechunk.in_memory_storage())
    src_storage = icechunk.local_filesystem_storage(str(tmp_path))
    result = icechunk.from_zarr(src_storage, repo, message="empty source")

    assert isinstance(result, IngestResult)
    assert result.stats.keys == 1  # root zarr.json
    assert result.snapshot_id
