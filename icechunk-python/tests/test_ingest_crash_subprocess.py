"""Hard-kill resume test for `icechunk.from_zarr`.

Spawns a subprocess running an ingest with a small `checkpoint_every`
so a chunk batch commits durably after only a couple of chunks, SIGKILLs
it once the subprocess reports progress on stdout, then resumes the
ingest in the parent and asserts a byte-faithful round-trip.

The stdout marker acts as a synchronization barrier — no timing knob.
"""

from __future__ import annotations

import signal
import subprocess
import sys
from pathlib import Path

import numpy as np
import obstore.store

import icechunk
import zarr
import zarr.storage
from icechunk.testing.models import ArrayNode, GroupNode

_RUNNER = Path(__file__).parent / "_ingest_subprocess_runner.py"


def _build_source(directory: Path) -> tuple[icechunk.Storage, dict[str, np.ndarray]]:
    src = zarr.storage.ObjectStore(obstore.store.LocalStore(str(directory)))
    arrays = {f"arr{i}": np.arange(128, dtype="uint16") + i * 1000 for i in range(4)}
    tree = GroupNode(
        children={
            name: ArrayNode(shape=data.shape, dtype=data.dtype, chunks=(1,), data=data)
            for name, data in arrays.items()
        }
    )
    tree.materialize(src)
    return icechunk.local_filesystem_storage(str(directory)), arrays


def test_kill_resume_completes_byte_faithfully(tmp_path: Path) -> None:
    src_dir = tmp_path / "src"
    src_dir.mkdir()
    repo_dir = tmp_path / "repo"

    src, expected_arrays = _build_source(src_dir)
    icechunk.Repository.create(icechunk.local_filesystem_storage(str(repo_dir)))

    proc = subprocess.Popen(
        [sys.executable, str(_RUNNER), str(src_dir), str(repo_dir)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        # Wait for two PROGRESS lines: first = skeleton commit, second =
        # first chunk batch commit. Killing after the second guarantees
        # we crash mid-chunks-phase with durable state on disk.
        # Skip non-marker lines.
        assert proc.stdout is not None
        markers: list[int] = []
        while len(markers) < 2:
            line = proc.stdout.readline()
            if not line:
                break
            if line.startswith("PROGRESS keys="):
                markers.append(int(line.removeprefix("PROGRESS keys=").strip()))
        assert len(markers) >= 2, (
            f"subprocess exited before two commits; saw progress at keys={markers}; "
            f"stderr={proc.stderr.read() if proc.stderr else ''!r}"
        )
        proc.send_signal(signal.SIGKILL)
        proc.wait(timeout=5.0)
        assert proc.returncode == -signal.SIGKILL
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=5.0)

    repo = icechunk.Repository.open(icechunk.local_filesystem_storage(str(repo_dir)))

    # Before resuming, prove the post-crash tip is mid-ingest — phase
    # is "chunks" with a non-empty cursor map. Without this assertion
    # the test could pass even if the subprocess never produced
    # durable state, since the resume would just redo the whole copy.
    pre_snap_id = repo.lookup_branch("main")
    pre_info = repo.lookup_snapshot(pre_snap_id)
    assert pre_info.metadata.get("icechunk.ingest.phase") == "chunks", (
        f"post-crash tip must be mid-ingest, got "
        f"{pre_info.metadata.get('icechunk.ingest.phase')!r}"
    )
    pre_cursors = pre_info.metadata.get("icechunk.ingest.cursors")
    assert isinstance(pre_cursors, dict) and pre_cursors, (
        f"post-crash tip must carry a non-empty cursor map, got {pre_cursors!r}"
    )

    icechunk.from_zarr(src, repo)

    sess = repo.readonly_session(branch="main")
    out = zarr.open_group(store=sess.store, mode="r")
    for name, expected in expected_arrays.items():
        np.testing.assert_array_equal(np.asarray(out[name][:]), expected)
