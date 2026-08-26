"""Tests for IcechunkStore.get_many_chunks — bulk coalesced reads.

Acceptance the coalescing prompt asks for, at the Python layer:

- **Correctness:** every chunk's bytes are byte-identical to an individual
  ``store.get`` across virtual / native / inline / missing mixes.
- **Cross-array:** chunks from different arrays that land in the same backing
  object are served in one call.
- **Missing:** uninitialized coords come back as ``(index, None)``.

Span-count / over-read coalescing behavior is unit-tested on the pure planner in
the Rust core (``plan_spans_tests``). Each yielded batch is one span (or one
group of chunks needing no fetch), but the tests here flatten the batches and
assert on bytes rather than on how they were grouped.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

import icechunk
import zarr
from icechunk.credentials import LocalFileSystemAccess


def _drain_outcomes(
    store: icechunk.IcechunkStore, requests: list[tuple[str, list[int]]], **kw
) -> list[tuple[int, bytes | None, BaseException | None]]:
    """Collect every chunk's full outcome, per-chunk errors included."""

    async def _collect() -> list[tuple[int, bytes | None, BaseException | None]]:
        out = []
        async for batch in store.get_many_chunks(requests, **kw):
            assert len(batch) > 0, "empty batches are never yielded"
            for index, data, error in batch:
                # Materialize the (possibly zero-copy) buffer into plain bytes so
                # the comparison doesn't depend on the span buffer staying alive.
                out.append((index, None if data is None else bytes(data), error))
        return out

    return asyncio.run(_collect())


def _drain(store: icechunk.IcechunkStore, requests: list[tuple[str, list[int]]], **kw):
    """Collect (index, bytes|None), raising the first per-chunk error.

    For tests that only care about the happy path; use `_drain_outcomes` to
    inspect per-chunk failures.
    """
    outcomes = _drain_outcomes(store, requests, **kw)
    for _, _, error in outcomes:
        if error is not None:
            raise error
    return [(index, data) for index, data, _ in outcomes]


def _virtual_repo(
    tmp_path: Path, config: icechunk.RepositoryConfig | None = None
) -> tuple[icechunk.Repository, bytes]:
    """Repo whose chunks are virtual refs into one local backing file.

    Returns the repo and the backing file's bytes so tests can check offsets.
    `config` lets a test add its own settings (e.g. manifest splitting); the
    virtual-chunk container is registered on top of whatever is passed.
    """
    backing = bytes(range(256)) * 8  # 2048 deterministic bytes
    data_file = tmp_path / "data.bin"
    data_file.write_bytes(backing)

    prefix = f"file://{tmp_path}/"
    config = config or icechunk.RepositoryConfig.default()
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


def _commit_virtual_refs(
    repo: icechunk.Repository,
    tmp_path: Path,
    refs: list[tuple[int, int, int]],
    *,
    name: str = "v",
) -> icechunk.IcechunkStore:
    """Create array `name` sized to `refs`, point each chunk at the backing file
    via `(coord, offset, length)`, commit, and return a read-only store."""
    session = repo.writable_session("main")
    store = session.store
    group = zarr.group(store=store, overwrite=True)
    group.create_array(
        name, shape=(len(refs),), chunks=(1,), dtype="u1", compressors=None
    )
    loc = f"file://{tmp_path}/data.bin"
    for coord, offset, length in refs:
        store.set_virtual_ref(f"{name}/c/{coord}", loc, offset=offset, length=length)
    session.commit("virtual refs")
    return repo.readonly_session(branch="main").store


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

    results: dict[int, bytes | None] = dict(_drain(store, requests, max_gap_bytes=0))

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

    results = dict(_drain(store, [("a", [0]), ("b", [0])], max_gap_bytes=0))
    assert results[0] == backing[0:128]
    assert results[1] == backing[128:256]


def test_disagreeing_checksums_do_not_coalesce(tmp_path: Path) -> None:
    """Adjacent refs into one object that disagree about its checksum must not
    share a span. A span is fetched with a single precondition, so merging them
    would validate one member against the other's checksum -- and where one has
    no checksum, drop validation entirely and hand back bytes the single-chunk
    read path rejects as modified."""
    repo, _ = _virtual_repo(tmp_path)
    session = repo.writable_session("main")
    store = session.store
    loc = f"file://{tmp_path}/data.bin"

    group = zarr.group(store=store, overwrite=True)
    group.create_array("v", shape=(2,), chunks=(1,), dtype="u1", compressors=None)
    # Byte-adjacent, so they would coalesce on offset alone. Chunk 0 carries no
    # checksum; chunk 1 carries one that cannot match the file.
    store.set_virtual_ref("v/c/0", loc, offset=0, length=100)
    store.set_virtual_ref("v/c/1", loc, offset=100, length=100, checksum='"stale-etag"')
    session.commit("disagreeing checksums")

    store = repo.readonly_session(branch="main").store
    proto = zarr.core.buffer.default_buffer_prototype()

    # Establish the single-chunk baseline: 0 reads, 1 is rejected as modified.
    assert zarr.core.sync.sync(store.get("v/c/0", prototype=proto)) is not None
    with pytest.raises(icechunk.IcechunkError, match="checksum"):
        zarr.core.sync.sync(store.get("v/c/1", prototype=proto))

    # Coalescing must not launder chunk 1 past its failed precondition.
    with pytest.raises(icechunk.IcechunkError, match="checksum"):
        _drain(store, [("v", [0]), ("v", [1])], max_gap_bytes=0)


@pytest.mark.parametrize(
    ("threshold", "kind"),
    # A 4-byte i4 chunk is inlined when the threshold exceeds it, written as a
    # native chunk when the threshold is 0. Both must be served, so parametrize
    # rather than pick one -- the inline path is otherwise untested.
    [(0, "native"), (512, "inline")],
)
def test_native_and_inline_chunks(threshold: int, kind: str) -> None:
    """get_many_chunks also serves native and inline chunks, byte-identical."""
    config = icechunk.RepositoryConfig.default()
    config.inline_chunk_threshold_bytes = threshold
    repo = icechunk.Repository.create(storage=icechunk.in_memory_storage(), config=config)
    session = repo.writable_session("main")
    store = session.store
    group = zarr.group(store=store, overwrite=True)
    arr = group.create_array("n", shape=(3,), chunks=(1,), dtype="i4", compressors=None)
    arr[0] = 7
    arr[1] = 8
    session.commit(kind)

    session = repo.readonly_session(branch="main")
    store = session.store
    proto = zarr.core.buffer.default_buffer_prototype()

    # Confirm the fixture really produced the kind this case is about, so neither
    # case silently degenerates into the other.
    reqs = [("n", [0]), ("n", [1]), ("n", [2])]
    rep = store.coalescing_report(reqs)
    assert rep[f"{kind}_chunks"] == 2

    results = dict(_drain(store, reqs))
    for coord in (0, 1):
        got = zarr.core.sync.sync(store.get(f"n/c/{coord}", prototype=proto))
        assert got is not None
        assert results[coord] == got.to_bytes()
    assert results[2] is None  # never written


def test_empty_requests(tmp_path: Path) -> None:
    repo, _ = _virtual_repo(tmp_path)
    session = repo.readonly_session(branch="main")
    assert _drain(session.store, []) == []


def test_coalescing_report_merge_ratio_and_over_read(tmp_path: Path) -> None:
    repo, _ = _virtual_repo(tmp_path)
    # chunk 0 @[0,100), 1 @[100,200) (adjacent), 2 @[500,580) (gapped).
    store = _commit_virtual_refs(
        repo, tmp_path, [(0, 0, 100), (1, 100, 100), (2, 500, 80)]
    )
    reqs = [("v", [0]), ("v", [1]), ("v", [2])]

    # max_gap_bytes=0: chunks 0 and 1 are adjacent -> one span; chunk 2 is gapped ->
    # its own span. 3 virtual chunks collapse to 2 spans with zero over-read, and
    # `spans` is the GET count -- nothing splits a merged span back apart.
    rep = store.coalescing_report(reqs, max_gap_bytes=0)
    assert rep["virtual_chunks"] == 3
    assert rep["spans"] == 2
    assert rep["over_read_bytes"] == 0
    assert rep["useful_bytes"] == 280

    # Gap big enough to bridge 200->500 merges all three into one span; the
    # skipped 300 bytes between chunk 1's end and chunk 2 become over-read.
    # (All three coords are in the same array = same manifest, so they can merge.)
    rep = store.coalescing_report(reqs, max_gap_bytes=300)
    assert rep["spans"] == 1
    assert rep["over_read_bytes"] == 300

    # A byte cap smaller than the merged span forces the split back apart, so the
    # over-read the gap would have cost is not paid.
    rep = store.coalescing_report(reqs, max_gap_bytes=300, max_coalesced_bytes=200)
    assert rep["spans"] == 2
    assert rep["over_read_bytes"] == 0


def test_bytes_are_right_when_a_span_over_reads(tmp_path: Path) -> None:
    """Reading through a span that spans a gap must still slice each member out
    correctly. The zero-gap cases can't catch a bad relative offset, because
    there every member starts where the last one ended."""
    repo, backing = _virtual_repo(tmp_path)
    refs = [(0, 0, 100), (1, 100, 100), (2, 500, 80)]
    store = _commit_virtual_refs(repo, tmp_path, refs)
    reqs = [("v", [0]), ("v", [1]), ("v", [2])]

    # One span covering 0..580, of which 200..500 is over-read.
    assert store.coalescing_report(reqs, max_gap_bytes=300)["spans"] == 1

    results = dict(_drain(store, reqs, max_gap_bytes=300))
    for i, (_, offset, length) in enumerate(refs):
        assert results[i] == backing[offset : offset + length]


def test_duplicate_coords_are_each_served(tmp_path: Path) -> None:
    """The same coord requested twice yields both request indices, and the byte
    accounting counts its bytes once rather than double-counting them (which
    would make over_read_bytes underflow)."""
    repo, backing = _virtual_repo(tmp_path)
    store = _commit_virtual_refs(repo, tmp_path, [(0, 0, 100), (1, 100, 100)])
    reqs = [("v", [0]), ("v", [1]), ("v", [0])]

    rep = store.coalescing_report(reqs, max_gap_bytes=0)
    assert rep["requested"] == 3
    assert rep["virtual_chunks"] == 3
    assert rep["spans"] == 1
    # 0..200 fetched, all of it wanted -- chunk 0's bytes counted once, not twice.
    assert rep["useful_bytes"] == 200
    assert rep["over_read_bytes"] == 0

    results = dict(_drain(store, reqs, max_gap_bytes=0))
    assert sorted(results) == [0, 1, 2]
    assert results[0] == backing[0:100]
    assert results[1] == backing[100:200]
    assert results[2] == backing[0:100]


def test_unknown_array_path_raises(tmp_path: Path) -> None:
    """A path that names no array is an error about the request, not a per-chunk
    outcome. A malformed path is rejected by the call itself; one that parses but
    resolves to nothing surfaces on first iteration, since resolving it needs the
    session lock the iterator holds."""
    repo, _ = _virtual_repo(tmp_path)
    store = _commit_virtual_refs(repo, tmp_path, [(0, 0, 100)])

    with pytest.raises(icechunk.IcechunkError):
        _drain(store, [("no_such_array", [0])])

    # A group where an array was expected is equally a request error.
    with pytest.raises(icechunk.IcechunkError):
        _drain(store, [("", [0])])


def test_merging_reduces_the_get_count(tmp_path: Path) -> None:
    """The point of the whole exercise: merging fewer, larger reads. A read whose
    chunks are adjacent must issue fewer GETs than it has chunks, and the bytes
    must still be right."""
    repo, backing = _virtual_repo(tmp_path)
    store = _commit_virtual_refs(repo, tmp_path, [(c, c * 64, 64) for c in range(16)])
    reqs = [("v", [c]) for c in range(16)]

    # 16 byte-adjacent chunks collapse into a single GET, with zero over-read --
    # even though 16 > the default concurrency of 10.
    rep = store.coalescing_report(reqs, max_gap_bytes=0)
    assert rep["virtual_chunks"] == 16
    assert rep["spans"] == 1
    assert rep["over_read_bytes"] == 0

    results = dict(_drain(store, reqs, max_gap_bytes=0))
    assert len(results) == 16
    for coord in range(16):
        assert results[coord] == backing[coord * 64 : (coord + 1) * 64]


def test_session_writes_take_precedence(tmp_path: Path) -> None:
    """Chunks written in the current, uncommitted session are served from the
    change set rather than the committed manifest."""
    repo, _ = _virtual_repo(tmp_path)
    session = repo.writable_session("main")
    store = session.store
    group = zarr.group(store=store, overwrite=True)
    arr = group.create_array("n", shape=(2,), chunks=(1,), dtype="i4", compressors=None)
    arr[0] = 1
    arr[1] = 2
    session.commit("committed")

    # Overwrite chunk 0 without committing.
    session = repo.writable_session("main")
    store = session.store
    zarr.open_array(store=store, path="n", mode="r+")[0] = 99

    proto = zarr.core.buffer.default_buffer_prototype()
    results = dict(_drain(store, [("n", [0]), ("n", [1])]))
    for i, coord in enumerate((0, 1)):
        got = zarr.core.sync.sync(store.get(f"n/c/{coord}", prototype=proto))
        assert got is not None
        assert results[i] == got.to_bytes()
    # The uncommitted value, not the committed one.
    assert results[0] == (99).to_bytes(4, "little")


def test_split_manifests_are_all_served(tmp_path: Path) -> None:
    """With a split manifest, one request's coords land in several manifests. Each
    is fetched and resolved separately (that is what the driver pipelines), and
    coalescing is scoped to each -- so chunks that are byte-adjacent but in
    different splits do not merge, while every chunk still comes back."""
    # Split "v" every 2 chunks along its only dimension -> 3 manifests for 6 chunks.
    sconfig = icechunk.ManifestSplittingConfig.from_dict(
        {
            icechunk.ManifestSplitCondition.name_matches("v"): {
                icechunk.ManifestSplitDimCondition.Any(): 2
            }
        }
    )
    repo, backing = _virtual_repo(
        tmp_path,
        icechunk.RepositoryConfig(manifest=icechunk.ManifestConfig(splitting=sconfig)),
    )
    # All 6 byte-adjacent, so only the manifest boundaries can prevent merging.
    store = _commit_virtual_refs(repo, tmp_path, [(c, c * 64, 64) for c in range(6)])
    reqs = [("v", [c]) for c in range(6)]

    # 3 splits of 2 adjacent chunks each -> 2 chunks merge within a split, but
    # never across splits: 3 spans, not 1.
    rep = store.coalescing_report(reqs, max_gap_bytes=0)
    assert rep["virtual_chunks"] == 6
    assert rep["spans"] == 3
    assert rep["over_read_bytes"] == 0

    results = dict(_drain(store, reqs, max_gap_bytes=0))
    assert len(results) == 6
    for coord in range(6):
        assert results[coord] == backing[coord * 64 : (coord + 1) * 64]


def test_one_failed_chunk_does_not_fail_the_batch(tmp_path: Path) -> None:
    """A chunk that cannot be read is reported against itself; every other chunk
    in the request is still delivered."""
    repo, backing = _virtual_repo(tmp_path)
    session = repo.writable_session("main")
    store = session.store
    group = zarr.group(store=store, overwrite=True)
    group.create_array("v", shape=(3,), chunks=(1,), dtype="u1", compressors=None)
    loc = f"file://{tmp_path}/data.bin"
    store.set_virtual_ref("v/c/0", loc, offset=0, length=100)
    # Far enough away not to coalesce with chunk 0, and pointing past the end of
    # the 2048-byte backing file so its own span fails to fetch.
    store.set_virtual_ref("v/c/1", loc, offset=100_000, length=100)
    store.set_virtual_ref("v/c/2", loc, offset=1000, length=100)
    session.commit("one bad ref")

    store = repo.readonly_session(branch="main").store
    outcomes = {
        index: (data, error)
        for index, data, error in _drain_outcomes(
            store, [("v", [0]), ("v", [1]), ("v", [2])], max_gap_bytes=0
        )
    }
    # All three chunks are accounted for.
    assert set(outcomes) == {0, 1, 2}
    # The two readable chunks came back intact...
    assert outcomes[0] == (backing[0:100], None)
    assert outcomes[2] == (backing[1000:1100], None)
    # ...and the unreadable one carries its own error rather than aborting.
    data, error = outcomes[1]
    assert data is None
    assert isinstance(error, icechunk.IcechunkError)


def _drain_get_many(
    store: icechunk.IcechunkStore, keys: list[str]
) -> dict[int, bytes | None | BaseException]:
    """Collect `IcechunkStore.get_many` results, keyed by request index."""
    from zarr.core.buffer import default_buffer_prototype

    async def _collect() -> dict[int, bytes | None | BaseException]:
        out: dict[int, bytes | None | BaseException] = {}
        async for batch in store.get_many(keys, prototype=default_buffer_prototype()):
            assert len(batch) > 0, "empty batches are never yielded"
            for index, value in batch:
                assert index not in out, "each request is reported exactly once"
                if isinstance(value, BaseException) or value is None:
                    out[index] = value
                else:
                    out[index] = value.to_bytes()
        return out

    return asyncio.run(_collect())


def test_get_many_serves_chunk_keys(tmp_path: Path) -> None:
    """Zarr's bulk hook resolves chunk keys through the coalescing path."""
    repo, backing = _virtual_repo(tmp_path)
    store = _commit_virtual_refs(
        repo, tmp_path, [(0, 0, 100), (1, 100, 100), (2, 500, 80)]
    )

    values = _drain_get_many(store, ["v/c/0", "v/c/1", "v/c/2"])

    assert values == {0: backing[0:100], 1: backing[100:200], 2: backing[500:580]}


def test_get_many_mixes_metadata_and_chunk_keys(tmp_path: Path) -> None:
    """Keys that are not whole chunks fall back to `get` rather than being dropped."""
    repo, backing = _virtual_repo(tmp_path)
    store = _commit_virtual_refs(repo, tmp_path, [(0, 0, 100)])

    values = _drain_get_many(store, ["v/zarr.json", "v/c/0", "v/c/9"])

    metadata = values[0]
    assert isinstance(metadata, bytes)
    assert b'"zarr_format"' in metadata
    assert values[1] == backing[0:100]
    # Coord outside the array's grid: absent, not an error.
    assert values[2] is None


@pytest.mark.parametrize("max_gap_bytes,expected_spans", [(0, 8), (256 * 1024, 1)])
def test_repo_config_max_gap_drives_coalescing(
    tmp_path: Path, max_gap_bytes: int, expected_spans: int
) -> None:
    """`max_gap_bytes` is reachable from the repo config, so a plain array read -- which
    has nowhere to pass a per-call value -- still gets the configured policy."""
    config = icechunk.RepositoryConfig.default()
    config.coalescing = icechunk.CoalescingConfig(max_gap_bytes=max_gap_bytes)
    repo, _ = _virtual_repo(tmp_path, config)
    # 8 chunks of 64B, each separated by a 64B hole: merging them costs over-read.
    store = _commit_virtual_refs(repo, tmp_path, [(c, c * 128, 64) for c in range(8)])
    reqs = [("v", [c]) for c in range(8)]

    assert store.coalescing_report(reqs)["spans"] == expected_spans
    assert len(_drain(store, reqs)) == 8


def test_get_many_uses_the_configured_max_gap(tmp_path: Path) -> None:
    """The zarr bulk-read hook takes no coalescing arguments, so the repo config is
    the only way its reads can be tuned."""
    config = icechunk.RepositoryConfig.default()
    config.coalescing = icechunk.CoalescingConfig(max_gap_bytes=0)
    repo, backing = _virtual_repo(tmp_path, config)
    store = _commit_virtual_refs(repo, tmp_path, [(c, c * 128, 64) for c in range(8)])

    assert store.coalescing_report([("v", [c]) for c in range(8)])["spans"] == 8
    values = _drain_get_many(store, [f"v/c/{c}" for c in range(8)])
    assert values == {
        coord: backing[coord * 128 : coord * 128 + 64] for coord in range(8)
    }
