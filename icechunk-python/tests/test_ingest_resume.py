"""Tests for the resumable `from_zarr` ingest.

Covers:
- Final commit has phase="complete" and all cursors DONE.
- A second call on an already-complete repo with overwrite=True succeeds
  and produces a new snapshot id.
- A repo whose latest commit carries phase="skeleton" triggers the resume
  path (chunks loop runs without re-doing the skeleton).
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

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

# TODO(constants): expose from Rust
PROP_PHASE = "icechunk.ingest.phase"
PROP_CURSORS = "icechunk.ingest.cursors"
CURSOR_DONE = "DONE"


def make_source(
    directory: str,
) -> tuple[zarr.storage.ObjectStore, icechunk.Storage]:
    """Build a small multi-array source hierarchy.

    Returns the zarr writer store (used by tests that introspect the source
    via the zarr API) paired with an `icechunk.Storage` pointing at the same
    directory (used as the `from_zarr` source).

    Two arrays with several chunks each so the cursor map is non-trivial.
    """
    raw = obstore.store.LocalStore(directory)
    src = zarr.storage.ObjectStore(raw)
    tree = GroupNode(
        children={
            "temperature": ArrayNode(
                shape=(4, 4),
                dtype=np.dtype("f4"),
                chunks=(2, 2),
                data=np.arange(16, dtype="f4").reshape(4, 4),
            ),
            "humidity": ArrayNode(
                shape=(4,),
                dtype=np.dtype("f4"),
                chunks=(2,),
                data=np.array([10.0, 20.0, 30.0, 40.0], dtype="f4"),
            ),
        },
    )
    tree.materialize(src)
    return src, icechunk.local_filesystem_storage(directory)


# ---------------------------------------------------------------------------
# Test 1 – final commit properties
# ---------------------------------------------------------------------------


def test_resume_state_visible_in_commit_properties(
    tmp_path_factory: pytest.TempPathFactory,
) -> None:
    """After a successful ingest the latest commit's metadata must have
    phase='complete' and every array cursor set to DONE."""
    src_dir = tmp_path_factory.mktemp("src")
    _src_store, src = make_source(str(src_dir))

    repo = icechunk.Repository.create(icechunk.in_memory_storage())

    # Use a small checkpoint_every so we get multiple commits and the
    # cursor map is exercised.
    result = icechunk.from_zarr(src, repo, checkpoint_every=2)

    assert isinstance(result, IngestResult)
    assert result.snapshot_id

    # Inspect the final commit's properties.
    snap_id = repo.lookup_branch("main")
    info = repo.lookup_snapshot(snap_id)
    metadata = info.metadata

    assert metadata.get(PROP_PHASE) == "complete", (
        f"Expected phase='complete', got {metadata.get(PROP_PHASE)!r}"
    )

    # Every cursor entry must be DONE (not null / partial key).
    cursors = metadata.get(PROP_CURSORS, {})
    for array_path, cursor in cursors.items():
        assert cursor == CURSOR_DONE, (
            f"Array {array_path!r} cursor should be DONE, got {cursor!r}"
        )

    # Round-trip sanity: data is still readable.
    sess = repo.readonly_session(branch="main")
    out = zarr.open_group(store=sess.store, mode="r")
    np.testing.assert_array_equal(
        np.asarray(out["temperature"][:]),
        np.arange(16, dtype="f4").reshape(4, 4),
    )


# ---------------------------------------------------------------------------
# Test 2 – second call with overwrite=True
# ---------------------------------------------------------------------------


def test_second_call_short_circuits_after_complete(
    tmp_path_factory: pytest.TempPathFactory,
) -> None:
    """Calling from_zarr a second time with overwrite=True against an already
    complete repo should succeed and yield a new snapshot id."""
    src_dir = tmp_path_factory.mktemp("src")
    _src_store, src = make_source(str(src_dir))

    repo = icechunk.Repository.create(icechunk.in_memory_storage())

    first = icechunk.from_zarr(src, repo)
    assert isinstance(first, IngestResult)

    # Second call — same source, same repo, on_collision=Overwrite.
    second = icechunk.from_zarr(
        src, repo, on_collision=icechunk.CollisionPolicy.Overwrite
    )
    assert isinstance(second, IngestResult)

    # A new snapshot must have been created.
    assert second.snapshot_id != first.snapshot_id

    # Data is still readable and correct.
    sess = repo.readonly_session(branch="main")
    out = zarr.open_group(store=sess.store, mode="r")
    np.testing.assert_array_equal(
        np.asarray(out["temperature"][:]),
        np.arange(16, dtype="f4").reshape(4, 4),
    )


# ---------------------------------------------------------------------------
# Test 3 – resume after a skeleton-phase commit
# ---------------------------------------------------------------------------


def test_resume_after_partial_skeleton(
    tmp_path_factory: pytest.TempPathFactory,
) -> None:
    """If the repo's latest commit has phase='skeleton' (simulated by
    manually writing and committing the metadata keys with that property),
    from_zarr should detect the skeleton state, skip re-doing the skeleton
    phase, and proceed to copy chunks.

    We inject the skeleton state via a writable session that sets the
    zarr.json metadata files and commits with the skeleton phase property.
    Then from_zarr is called and must complete the ingest successfully.
    """
    src_dir = tmp_path_factory.mktemp("src")
    src_store, src = make_source(str(src_dir))

    repo = icechunk.Repository.create(icechunk.in_memory_storage())

    # --- Inject skeleton phase manually ---
    # Open a writable session and write just the zarr.json metadata keys.
    # This simulates what the Rust skeleton phase does before it commits.
    session = repo.writable_session("main")

    # zarr.json metadata keys: root plus one per non-root node.
    src_tree = GroupNode.from_store(src_store)
    zarr_json_keys = ["zarr.json"] + [f"{p}/zarr.json" for p in src_tree.nodes()]

    # Write each zarr.json into the icechunk session via the async store
    # interface. IcechunkStore.set() requires a zarr Buffer, not raw bytes.
    import asyncio

    from zarr.core.buffer import cpu as zarr_cpu

    dest_store = session.store

    async def _write_keys() -> None:
        for key in zarr_json_keys:
            buf = zarr_cpu.Buffer.from_bytes((src_dir / key).read_bytes())
            await dest_store.set(key, buf)

    asyncio.run(_write_keys())

    # Commit with phase=skeleton to simulate a partial ingest.
    session.commit(
        "skeleton phase (injected for test)",
        metadata={PROP_PHASE: "skeleton"},
    )

    # --- Now call from_zarr which should resume from skeleton ---
    result = icechunk.from_zarr(src, repo)

    assert isinstance(result, IngestResult), "from_zarr should return IngestResult"
    assert result.snapshot_id, "should have a final snapshot id"

    # Final commit must be complete.
    snap_id = repo.lookup_branch("main")
    info = repo.lookup_snapshot(snap_id)
    assert info.metadata.get(PROP_PHASE) == "complete", (
        f"Expected complete phase, got {info.metadata.get(PROP_PHASE)!r}"
    )

    # Data round-trip must work.
    sess_r = repo.readonly_session(branch="main")
    out = zarr.open_group(store=sess_r.store, mode="r")
    np.testing.assert_array_equal(
        np.asarray(out["temperature"][:]),
        np.arange(16, dtype="f4").reshape(4, 4),
    )
    np.testing.assert_array_equal(
        np.asarray(out["humidity"][:]),
        np.array([10.0, 20.0, 30.0, 40.0], dtype="f4"),
    )
