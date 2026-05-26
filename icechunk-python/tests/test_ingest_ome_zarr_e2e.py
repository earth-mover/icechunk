"""End-to-end ingest test: copy an OME-Zarr dataset from public S3 into a
local on-disk icechunk repo and verify the round-trip across a fresh
``Repository.open`` (durability) and a sharded-codec read path."""

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
INGEST_PATHS = ["5"]

PROP_PHASE = "icechunk.ingest.phase"


@pytest.mark.network
def test_ingest_ome_zarr_to_local_disk(tmp_path) -> None:
    """Round-trip a public OME-Zarr level into a local-filesystem icechunk
    repo, reopen the repo from disk, and verify byte-faithful equality."""
    raw = obstore_store.S3Store(
        bucket=BUCKET,
        prefix=PREFIX,
        endpoint=ENDPOINT,
        skip_signature=True,
    )
    src = zarr.storage.ObjectStore(raw, read_only=True)
    src_storage = icechunk.s3_storage(
        bucket=BUCKET,
        prefix=PREFIX,
        endpoint_url=ENDPOINT,
        anonymous=True,
    )

    storage = icechunk.local_filesystem_storage(str(tmp_path))
    repo = icechunk.Repository.create(storage)

    result = icechunk.from_zarr(
        src_storage, repo, paths=INGEST_PATHS, message="ome-zarr e2e"
    )
    assert isinstance(result, IngestResult)
    assert result.snapshot_id

    # Final commit must be marked complete.
    snap_id = repo.lookup_branch("main")
    info = repo.lookup_snapshot(snap_id)
    assert info.metadata.get(PROP_PHASE) == "complete", (
        f"expected phase='complete', got {info.metadata.get(PROP_PHASE)!r}"
    )

    # Re-open the repo from disk to prove durability.
    del repo
    repo2 = icechunk.Repository.open(icechunk.local_filesystem_storage(str(tmp_path)))

    sess = repo2.readonly_session(branch="main")
    dst_root = zarr.open_group(store=sess.store, mode="r")
    src_root = zarr.open_group(store=src, mode="r")

    dst_arr = dst_root["5"]
    src_arr = src_root["5"]

    assert dst_arr.shape == src_arr.shape
    assert dst_arr.dtype == src_arr.dtype
    assert tuple(dst_arr.chunks) == tuple(src_arr.chunks)
    assert dict(dst_arr.attrs) == dict(src_arr.attrs)

    # Byte-faithful chunk comparison. OME-Zarr v0.5 uses sharded chunks;
    # iterate keys under the array prefix and compare raw bytes.
    import asyncio

    async def collect_keys() -> list[str]:
        return [k async for k in src.list_prefix("5/")]

    src_keys = sorted(asyncio.run(collect_keys()))
    assert src_keys, "source listed zero keys under '5/'"

    async def get_bytes(store: zarr.abc.store.Store, key: str) -> bytes:
        buf = await store.get(key, prototype=zarr.core.buffer.default_buffer_prototype())
        assert buf is not None, f"missing key {key!r}"
        return bytes(buf.to_bytes())

    dst_store = sess.store
    for key in src_keys:
        sb = asyncio.run(get_bytes(src, key))
        db = asyncio.run(get_bytes(dst_store, key))
        assert sb == db, f"byte mismatch for key {key!r} ({len(sb)} vs {len(db)})"

    # Exercise the sharded-codec read path: decoded data must match.
    src_data = np.asarray(src_arr[:])
    dst_data = np.asarray(dst_arr[:])
    assert np.array_equal(src_data, dst_data)

    # A second from_zarr on a complete repo must fail with the default
    # Fail collision policy.
    with pytest.raises(ValueError):
        icechunk.from_zarr(src_storage, repo2, paths=INGEST_PATHS, message="should fail")
