"""Network integration test for `icechunk.from_zarr` against a public S3 source.

This test requires network access to ``https://livingobjects.ebi.ac.uk`` and is
gated with ``@pytest.mark.network`` so it can be excluded from offline runs::

    pytest -m "not network"   # skip this test
    pytest -m network         # run only network tests

The dataset is a public anonymous-access IDR zarr at:
``s3://idr/zarr/v0.5/idr0066/ExpA_VIP_ASLM_on.zarr``

Only pyramid level ``5`` (the smallest) is ingested to keep wall time short.
"""

from __future__ import annotations

import numpy as np
import pytest

obstore = pytest.importorskip("obstore", reason="obstore is required for this test")
obstore_store = pytest.importorskip(
    "obstore.store", reason="obstore.store is required for this test"
)

import icechunk
import zarr
import zarr.storage
from icechunk.ingest import IngestResult

BUCKET = "idr"
PREFIX = "zarr/v0.5/idr0066/ExpA_VIP_ASLM_on.zarr"
ENDPOINT = "https://livingobjects.ebi.ac.uk"

# Only ingest the smallest pyramid level to stay well under 30 s.
# Root metadata (zarr.json) is auto-included via ancestor_metadata_keys.
INGEST_PATHS = ["5"]

# Expected properties of level 5
EXPECTED_SHAPE = (1937, 64, 64)
EXPECTED_DTYPE = np.dtype("uint16")
EXPECTED_CHUNKS = (1, 256, 256)


@pytest.mark.network
def test_ingest_from_s3_public() -> None:
    """Ingest a public S3 zarr store into an in-memory icechunk repo and
    verify the round-trip for pyramid level 5."""
    raw = obstore_store.S3Store(
        bucket=BUCKET,
        prefix=PREFIX,
        endpoint=ENDPOINT,
        skip_signature=True,
    )
    src = zarr.storage.ObjectStore(raw, read_only=True)

    repo = icechunk.Repository.create(icechunk.in_memory_storage())

    result = icechunk.from_zarr(src, repo, paths=INGEST_PATHS, message="network test")

    assert isinstance(result, IngestResult)
    assert result.snapshot_id, "expected a non-empty snapshot id"
    # Level 5 alone has well over 100 keys and several MB of chunk data
    assert result.stats.keys >= 100, f"expected >= 100 keys, got {result.stats.keys}"
    assert result.stats.bytes >= 1_000_000, (
        f"expected >= 1 MB ingested, got {result.stats.bytes} bytes"
    )

    # Read back via zarr and verify array properties
    sess = repo.readonly_session(branch="main")
    dst_root = zarr.open_group(store=sess.store, mode="r")
    dst_arr = dst_root["5"]

    assert dst_arr.shape == EXPECTED_SHAPE, (
        f"shape mismatch: {dst_arr.shape} != {EXPECTED_SHAPE}"
    )
    assert dst_arr.dtype == EXPECTED_DTYPE, (
        f"dtype mismatch: {dst_arr.dtype} != {EXPECTED_DTYPE}"
    )
    assert tuple(dst_arr.chunks) == EXPECTED_CHUNKS, (
        f"chunks mismatch: {dst_arr.chunks} != {EXPECTED_CHUNKS}"
    )

    # Spot-check: frame 0 from src and dst must match exactly
    src_root = zarr.open_group(store=src, mode="r")
    src_arr = src_root["5"]
    frame_src = np.asarray(src_arr[0])
    frame_dst = np.asarray(dst_arr[0])
    assert np.array_equal(frame_src, frame_dst), "frame 0 data mismatch after ingest"
