"""Tests for IcechunkStore.get_many_chunks — bulk coalesced reads.

Acceptance the coalescing prompt asks for, at the Python layer:

- **Correctness:** every chunk's bytes are byte-identical to an individual
  ``store.get`` across virtual / native / inline / missing mixes.
- **Cross-array:** chunks from different arrays that land in the same backing
  object are served in one call.
- **Missing:** uninitialized coords come back as ``(index, None)``.

Span-count / over-read coalescing behavior is unit-tested on the pure planner in
the Rust core (``plan_spans_tests``); it is not observable from this per-chunk
Python API, which only hands back the resolved bytes.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

import icechunk
import zarr
from icechunk.credentials import LocalFileSystemAccess


def _drain(store: icechunk.IcechunkStore, requests: list[tuple[str, list[int]]], **kw):
    async def _collect() -> list[tuple[int, bytes | None]]:
        out = []
        async for item in store.get_many_chunks(requests, **kw):
            index, data = item
            # Materialize the (possibly zero-copy) buffer into plain bytes so the
            # comparison doesn't depend on the span buffer staying alive.
            out.append((index, None if data is None else bytes(data)))
        return out

    return asyncio.run(_collect())


def _virtual_repo(tmp_path: Path) -> tuple[icechunk.Repository, bytes]:
    """Repo whose chunks are virtual refs into one local backing file.

    Returns the repo and the backing file's bytes so tests can check offsets.
    """
    backing = bytes(range(256)) * 8  # 2048 deterministic bytes
    data_file = tmp_path / "data.bin"
    data_file.write_bytes(backing)

    prefix = f"file://{tmp_path}/"
    config = icechunk.RepositoryConfig.default()
    config.set_virtual_chunk_container(
        icechunk.VirtualChunkContainer(
            prefix, icechunk.local_filesystem_store(str(tmp_path))
        )
    )
    repo = icechunk.Repository.create(
        storage=icechunk.in_memory_storage(),
        config=config,
        authorize_virtual_chunk_access={prefix: LocalFileSystemAccess},
    )
    return repo, backing


def test_virtual_bytes_identical_to_individual_gets(tmp_path: Path) -> None:
    repo, backing = _virtual_repo(tmp_path)
    session = repo.writable_session("main")
    store = session.store

    group = zarr.group(store=store, overwrite=True)
    # 4 chunks: two adjacent (coalescable), one gapped, one missing.
    group.create_array("v", shape=(4,), chunks=(1,), dtype="u1", compressors=None)
    loc = f"file://{tmp_path}/data.bin"
    # (coord, offset, length): 0 and 1 are adjacent; 2 has a gap; 3 left unset.
    refs = [(0, 0, 100), (1, 100, 100), (2, 500, 80)]
    for coord, offset, length in refs:
        store.set_virtual_ref(f"v/c/{coord}", loc, offset=offset, length=length)
    session.commit("virtual")

    session = repo.readonly_session(branch="main")
    store = session.store
    requests = [("v", [0]), ("v", [1]), ("v", [2]), ("v", [3])]

    results: dict[int, bytes | None] = dict(_drain(store, requests, max_gap=0))

    # Every present chunk is byte-identical to the backing-file slice AND to a
    # plain individual get.
    for i, (coord, offset, length) in enumerate(refs):
        assert results[i] == backing[offset : offset + length]
        got = zarr.core.sync.sync(
            store.get(
                f"v/c/{coord}", prototype=zarr.core.buffer.default_buffer_prototype()
            )
        )
        assert got is not None
        assert results[i] == got.to_bytes()

    # Uninitialized coord -> None.
    assert results[3] is None


def test_cross_array_same_object(tmp_path: Path) -> None:
    """Chunks from two different arrays that point into the same file are served
    correctly in one call. (They live in separate manifests, so they are not
    coalesced together, but the bytes must still be right.)"""
    repo, backing = _virtual_repo(tmp_path)
    session = repo.writable_session("main")
    store = session.store
    loc = f"file://{tmp_path}/data.bin"

    group = zarr.group(store=store, overwrite=True)
    group.create_array("a", shape=(1,), chunks=(1,), dtype="u1", compressors=None)
    group.create_array("b", shape=(1,), chunks=(1,), dtype="u1", compressors=None)
    # a/0 and b/0 are adjacent in the same backing file -> one coalesced span.
    store.set_virtual_ref("a/c/0", loc, offset=0, length=128)
    store.set_virtual_ref("b/c/0", loc, offset=128, length=128)
    session.commit("cross")

    session = repo.readonly_session(branch="main")
    store = session.store

    results = dict(_drain(store, [("a", [0]), ("b", [0])], max_gap=0))
    assert results[0] == backing[0:128]
    assert results[1] == backing[128:256]


def test_native_and_inline_chunks(tmp_path: Path) -> None:
    """get_many_chunks also serves native and inline chunks, byte-identical."""
    config = icechunk.RepositoryConfig.default()
    config.inline_chunk_threshold_bytes = 0  # force native
    repo = icechunk.Repository.create(storage=icechunk.in_memory_storage(), config=config)
    session = repo.writable_session("main")
    store = session.store
    group = zarr.group(store=store, overwrite=True)
    arr = group.create_array("n", shape=(3,), chunks=(1,), dtype="i4", compressors=None)
    arr[0] = 7
    arr[1] = 8
    session.commit("native")

    session = repo.readonly_session(branch="main")
    store = session.store
    proto = zarr.core.buffer.default_buffer_prototype()

    results = dict(_drain(store, [("n", [0]), ("n", [1]), ("n", [2])]))
    for coord in (0, 1):
        got = zarr.core.sync.sync(store.get(f"n/c/{coord}", prototype=proto))
        assert got is not None
        assert results[coord] == got.to_bytes()
    assert results[2] is None  # never written


def test_empty_requests(tmp_path: Path) -> None:
    repo, _ = _virtual_repo(tmp_path)
    session = repo.readonly_session(branch="main")
    assert _drain(session.store, []) == []


def _virtual_array_repo(tmp_path: Path) -> icechunk.Repository:
    """Repo with array 'v': chunk 0 @[0,100), 1 @[100,200) (adjacent), 2 @[500,580)."""
    repo, _ = _virtual_repo(tmp_path)
    session = repo.writable_session("main")
    store = session.store
    group = zarr.group(store=store, overwrite=True)
    group.create_array("v", shape=(3,), chunks=(1,), dtype="u1", compressors=None)
    loc = f"file://{tmp_path}/data.bin"
    for coord, offset, length in [(0, 0, 100), (1, 100, 100), (2, 500, 80)]:
        store.set_virtual_ref(f"v/c/{coord}", loc, offset=offset, length=length)
    session.commit("virtual")
    return repo


def test_coalescing_report_merge_ratio_and_over_read(tmp_path: Path) -> None:
    repo = _virtual_array_repo(tmp_path)
    store = repo.readonly_session(branch="main").store
    reqs = [("v", [0]), ("v", [1]), ("v", [2])]

    # max_gap=0: chunks 0 and 1 are adjacent -> one span; chunk 2 is gapped ->
    # its own span. 3 virtual chunks collapse to 2 spans with zero over-read.
    # `spans`/`over_read_bytes` report the coalescing plan; `fetched_spans`
    # reports the GET count after get_many_chunks' parallelism floor, which (with
    # the default concurrency of 10 and one manifest) splits the merged pair back
    # apart for free -> 3 GETs.
    rep = store.coalescing_report(reqs, max_gap=0)
    assert rep["virtual_chunks"] == 3
    assert rep["spans"] == 2
    assert rep["fetched_spans"] == 3
    assert rep["over_read_bytes"] == 0
    assert rep["useful_bytes"] == 280

    # Gap big enough to bridge 200->500 merges all three into one span; the
    # skipped 300 bytes between chunk 1's end and chunk 2 become over-read.
    # (All three coords are in the same array = same manifest, so they can merge.)
    # The floor then splits that span back to 3 singleton GETs, which at these
    # offsets fetch no over-read -- fetched_spans exposes the merge won't survive.
    rep = store.coalescing_report(reqs, max_gap=300)
    assert rep["spans"] == 1
    assert rep["fetched_spans"] == 3
    assert rep["over_read_bytes"] == 300
