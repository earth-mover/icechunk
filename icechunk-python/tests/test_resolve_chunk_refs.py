"""Tests for IcechunkStore.resolve_chunk_refs — batch resolution of explicit
chunk coordinates to their references without scanning the whole manifest.

The result is a columnar 5-tuple aligned with the input coords (row ``i``
describes ``coords[i]``)::

    kinds:    np.ndarray[uint8]    values of icechunk.ChunkType (0 = uninitialized)
    paths:    list[str]            URL (virtual) | chunk_id (native) | "" otherwise
    offsets:  np.ndarray[uint64]
    lengths:  np.ndarray[uint64]
    inlined:  dict[int, bytes]     inline rows only, keyed by row index

Columns must match what ``array_chunk_iterator`` reports for the same coords.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import Any

import numpy as np

import icechunk
from icechunk import ChunkType


def _drain(async_iter: AsyncIterator[Any]) -> list[Any]:
    async def _collect() -> list[Any]:
        return [b async for b in async_iter]

    return asyncio.run(_collect())


def _mixed_repo() -> icechunk.Repository:
    """A repo with a virtual and an inline chunk on /a (shape=2x2x2)."""
    repo = icechunk.Repository.create(
        storage=icechunk.in_memory_storage(),
        config=icechunk.RepositoryConfig.default(),
    )
    session = repo.writable_session("main")
    store = session.store
    import zarr

    group = zarr.group(store=store, overwrite=True)
    arr = group.create_array(
        "a", shape=(2, 2, 2), chunks=(1, 1, 1), dtype="i4", compressors=None
    )
    arr[0, 0, 0] = 1  # inline
    store.set_virtual_ref(
        "a/c/0/0/1",
        "s3://bucket/data.nc",
        offset=100,
        length=50,
        validate_container=False,
    )
    session.commit("init")
    return repo


def test_resolve_matches_iterator_and_reports_uninitialized() -> None:
    repo = _mixed_repo()
    session = repo.readonly_session(branch="main")

    coords = [(0, 0, 0), (0, 0, 1), (1, 1, 1)]  # inline, virtual, uninitialized
    kinds, paths, offsets, lengths, inlined = session.store.resolve_chunk_refs(
        "a", coords
    )

    # Columnar contract: aligned uint arrays + list[str] + dict, length n.
    n = len(coords)
    assert kinds.shape == (n,) and kinds.dtype == np.uint8
    assert offsets.shape == (n,) and offsets.dtype == np.uint64
    assert lengths.shape == (n,) and lengths.dtype == np.uint64
    assert isinstance(paths, list) and len(paths) == n
    assert isinstance(inlined, dict)

    # row 0: inline
    assert kinds[0] == ChunkType.inline
    assert paths[0] == ""
    assert offsets[0] == 0
    assert lengths[0] == 4
    assert len(inlined[0]) == 4

    # row 1: virtual
    assert kinds[1] == ChunkType.virtual
    assert paths[1] == "s3://bucket/data.nc"
    assert offsets[1] == 100
    assert lengths[1] == 50
    assert 1 not in inlined

    # row 2: uninitialized
    assert kinds[2] == ChunkType.uninitialized
    assert paths[2] == ""
    assert lengths[2] == 0
    assert 2 not in inlined

    # Cross-check against array_chunk_iterator for the same coords.
    (batch,) = _drain(session.store.array_chunk_iterator("a", batch_size=100))
    it_coords, it_kinds, it_paths, it_offsets, it_lengths, _inlined = batch
    by_coord = {tuple(c.tolist()): i for i, c in enumerate(it_coords)}
    for row, coord in enumerate(coords):
        if coord not in by_coord:
            assert kinds[row] == ChunkType.uninitialized
            continue
        j = by_coord[coord]
        assert kinds[row] == int(it_kinds[j])
        assert paths[row] == it_paths[j]
        assert offsets[row] == int(it_offsets[j])
        assert lengths[row] == int(it_lengths[j])


def test_resolve_preserves_input_order() -> None:
    repo = _mixed_repo()
    session = repo.readonly_session(branch="main")

    coords = [(0, 0, 1), (1, 1, 1), (0, 0, 0)]
    kinds, _paths, _offsets, _lengths, _inlined = session.store.resolve_chunk_refs(
        "a", coords
    )
    assert kinds.tolist() == [
        int(ChunkType.virtual),
        int(ChunkType.uninitialized),
        int(ChunkType.inline),
    ]


def test_resolve_async_matches_sync() -> None:
    repo = _mixed_repo()
    session = repo.readonly_session(branch="main")
    coords = [(0, 0, 0), (0, 0, 1)]

    s_kinds, s_paths, s_offsets, s_lengths, s_inlined = session.store.resolve_chunk_refs(
        "a", coords
    )
    a_kinds, a_paths, a_offsets, a_lengths, a_inlined = asyncio.run(
        session.store.resolve_chunk_refs_async("a", coords)
    )
    assert s_kinds.tolist() == a_kinds.tolist()
    assert s_paths == a_paths
    assert s_offsets.tolist() == a_offsets.tolist()
    assert s_lengths.tolist() == a_lengths.tolist()
    assert s_inlined.keys() == a_inlined.keys()


def test_resolve_native_chunk_returns_bare_id() -> None:
    config = icechunk.RepositoryConfig.default()
    config.inline_chunk_threshold_bytes = 0  # force native
    repo = icechunk.Repository.create(
        storage=icechunk.in_memory_storage(),
        config=config,
    )
    session = repo.writable_session("main")
    import zarr

    group = zarr.group(store=session.store, overwrite=True)
    arr = group.create_array("v", shape=(2,), chunks=(1,), dtype="i4", compressors=None)
    arr[0] = 9
    session.commit("c")

    session = repo.readonly_session(branch="main")
    kinds, paths, _offsets, lengths, inlined = session.store.resolve_chunk_refs(
        "v", [(0,)]
    )
    assert kinds[0] == ChunkType.native
    assert "://" not in paths[0] and "/" not in paths[0]
    assert lengths[0] == 4
    assert inlined == {}


def test_resolve_empty_and_uninitialized_array() -> None:
    repo = icechunk.Repository.create(
        storage=icechunk.in_memory_storage(),
        config=icechunk.RepositoryConfig.default(),
    )
    session = repo.writable_session("main")
    import zarr

    group = zarr.group(store=session.store, overwrite=True)
    group.create_array("z", shape=(4,), chunks=(1,), dtype="i4", compressors=None)
    session.commit("c")

    session = repo.readonly_session(branch="main")

    kinds, paths, _offsets, _lengths, inlined = session.store.resolve_chunk_refs("z", [])
    assert kinds.shape == (0,)
    assert paths == []
    assert inlined == {}

    kinds, paths, _offsets, lengths, inlined = session.store.resolve_chunk_refs(
        "z", [(0,), (3,)]
    )
    assert kinds.tolist() == [int(ChunkType.uninitialized), int(ChunkType.uninitialized)]
    assert paths == ["", ""]
    assert lengths.tolist() == [0, 0]
    assert inlined == {}
