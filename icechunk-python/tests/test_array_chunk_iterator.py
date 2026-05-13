"""Tests for IcechunkStore.array_chunk_iterator — the parser-facing batched
columnar iterator consumed by VirtualiZarr's IcechunkParser.

Per-array async generator. Each batch is a 6-tuple:

    (coords, kinds, paths, offsets, lengths, inlined)

where:
    coords:   np.ndarray[uint32, (n, ndim)]  — chunk grid coordinates
    kinds:    np.ndarray[uint8]              — values match `icechunk.ChunkType`
    paths:    list[str]                      — URL (virtual) | chunk_id (native) | "" (inline)
    offsets:  np.ndarray[uint64]
    lengths:  np.ndarray[uint64]
    inlined:  dict[int, bytes]               — keyed by index within this batch
"""

from __future__ import annotations

import asyncio

import numpy as np
import pytest

import icechunk
from icechunk import ChunkType


def _drain(async_iter) -> list:
    """Synchronously drain an async generator into a list of batches."""

    async def _collect():
        out = []
        async for b in async_iter:
            out.append(b)
        return out

    return asyncio.run(_collect())


def _mixed_repo() -> icechunk.Repository:
    """A repo with virtual, inline, and (forced) native chunks on /a (shape=2x2x2)."""
    config = icechunk.RepositoryConfig.default()
    repo = icechunk.Repository.create(
        storage=icechunk.in_memory_storage(),
        config=config,
    )
    session = repo.writable_session("main")
    store = session.store
    import zarr

    group = zarr.group(store=store, overwrite=True)
    arr = group.create_array(
        "a", shape=(2, 2, 2), chunks=(1, 1, 1), dtype="i4", compressors=None
    )

    # inline (small)
    arr[0, 0, 0] = 1

    # virtual
    store.set_virtual_ref(
        "a/c/0/0/1",
        "s3://bucket/data.nc",
        offset=100,
        length=50,
        validate_container=False,
    )
    # (0,1,0) — missing (won't appear in iterator)
    # other slots — missing

    session.commit("init")
    return repo


def test_iterator_yields_columnar_batch_for_mixed_payloads() -> None:
    """Shape/dtype contract + per-row values for a virtual+inline mix."""
    repo = _mixed_repo()
    session = repo.readonly_session(branch="main")
    (batch,) = _drain(session.store.array_chunk_iterator("a", batch_size=100))
    coords, kinds, paths, offsets, lengths, inlined = batch

    # Columnar contract: aligned uint{32,8,64} numpy arrays + list[str] + dict.
    n = coords.shape[0]
    assert n == 2
    assert coords.shape == (2, 3) and coords.dtype == np.uint32
    assert kinds.shape == (2,) and kinds.dtype == np.uint8
    assert offsets.shape == (2,) and offsets.dtype == np.uint64
    assert lengths.shape == (2,) and lengths.dtype == np.uint64
    assert isinstance(paths, list) and len(paths) == 2
    assert isinstance(inlined, dict)

    # Coord-keyed lookup so the test isn't sensitive to iteration order.
    by_coord = {tuple(c.tolist()): i for i, c in enumerate(coords)}
    assert set(by_coord) == {(0, 0, 0), (0, 0, 1)}

    inline_i = by_coord[(0, 0, 0)]
    assert kinds[inline_i] == ChunkType.inline
    assert paths[inline_i] == ""
    assert offsets[inline_i] == 0
    assert lengths[inline_i] == 4
    assert len(inlined[inline_i]) == 4

    virt_i = by_coord[(0, 0, 1)]
    assert kinds[virt_i] == ChunkType.virtual
    assert paths[virt_i] == "s3://bucket/data.nc"
    assert offsets[virt_i] == 100
    assert lengths[virt_i] == 50
    assert virt_i not in inlined


def test_iterator_native_chunk_returns_bare_id() -> None:
    """Native (managed) chunk paths come back as bare chunk_ids — no prefix."""
    config = icechunk.RepositoryConfig.default()
    config.inline_chunk_threshold_bytes = 0  # force native
    repo = icechunk.Repository.create(
        storage=icechunk.in_memory_storage(),
        config=config,
    )
    session = repo.writable_session("main")
    import zarr

    group = zarr.group(store=session.store, overwrite=True)
    arr = group.create_array(
        "v", shape=(2,), chunks=(1,), dtype="i4", compressors=None
    )
    arr[0] = 9
    arr[1] = 10
    session.commit("c")

    session = repo.readonly_session(branch="main")
    batches = _drain(session.store.array_chunk_iterator("v", batch_size=100))
    assert len(batches) == 1
    coords, kinds, paths, offsets, lengths, inlined = batches[0]
    assert kinds.tolist() == [ChunkType.native, ChunkType.native]
    # Bare chunk_id strings — no URL scheme, no '/' separator
    for p in paths:
        assert "://" not in p
        assert "/" not in p
    assert lengths.tolist() == [4, 4]
    assert inlined == {}


def test_iterator_batches_split_at_batch_size() -> None:
    """Many chunks crossing batch_size boundary are split across yields."""
    config = icechunk.RepositoryConfig.default()
    repo = icechunk.Repository.create(
        storage=icechunk.in_memory_storage(),
        config=config,
    )
    session = repo.writable_session("main")
    import zarr

    group = zarr.group(store=session.store, overwrite=True)
    arr = group.create_array(
        "b", shape=(7,), chunks=(1,), dtype="i4", compressors=None
    )
    # Start from 1 — writing 0 (the default fill value) into an int chunk
    # is treated as a fill-value write and won't materialize a chunk.
    for i in range(7):
        arr[i] = i + 1
    session.commit("c")

    session = repo.readonly_session(branch="main")
    batches = _drain(session.store.array_chunk_iterator("b", batch_size=3))
    # 7 chunks at batch_size 3 = 3 + 3 + 1
    total = sum(b[0].shape[0] for b in batches)
    assert total == 7
    assert len(batches) == 3
