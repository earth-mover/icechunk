"""User-facing entry point for byte-faithful ingest of a zarr v3 store
into an icechunk repository.

The Rust binding (`py_ingest_zarr`) takes an `icechunk.Storage`
(same type used to construct the destination `Repository`) and an
`icechunk.Repository`, then drives the entire copy: opens writable
sessions on the target branch, copies keys batch-by-batch, commits
between batches so progress is durable, and returns the final
snapshot id.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Literal

from icechunk._icechunk_python import CollisionPolicy, py_ingest_zarr
from icechunk.repository import Repository
from icechunk.storage import Storage

__all__ = ["CollisionPolicy", "IngestResult", "IngestStats", "from_zarr"]


@dataclass(frozen=True, slots=True)
class IngestStats:
    """Counters reported during and after an ingest. All fields
    reflect state that is durable on the branch at the moment a
    progress callback fires."""

    keys: int
    bytes: int
    arrays_done: int


@dataclass(frozen=True, slots=True)
class IngestResult:
    """Outcome of an `from_zarr` call: stats plus the final snapshot id."""

    snapshot_id: str
    stats: IngestStats


def from_zarr(
    source: Storage,
    sink: Repository,
    *,
    paths: Sequence[str] | None = None,
    branch: str = "main",
    message: str | None = None,
    concurrency: int = 32,
    on_progress: Callable[[IngestStats], None] | None = None,
    on_collision: CollisionPolicy = CollisionPolicy.Fail,
    mode: Literal["copy"] = "copy",
    checkpoint_every: int | None = None,
    verify_source_unchanged: bool = True,
) -> IngestResult:
    """Byte-faithful, resumable ingest of a zarr v3 store into an
    icechunk repository.

    The copy is driven as a series of small commits on ``branch``: a
    skeleton commit holding every ``zarr.json``, then one commit per
    ``checkpoint_every`` chunks per array. Progress is recorded in the
    latest commit's ``SnapshotProperties``, so a fresh call to
    ``from_zarr`` against the same repo will resume where the previous
    one stopped.

    Parameters
    ----------
    source
        An `icechunk.Storage` pointing at the zarr v3 store to copy
        from. Construct via the same helpers used for the destination
        repo: `icechunk.s3_storage`, `icechunk.gcs_storage`,
        `icechunk.azure_storage`, `icechunk.local_filesystem_storage`,
        `icechunk.http_storage`, etc. Source-side path scoping goes
        through the ``prefix=`` argument on those constructors.
    sink
        Target `Repository`. The ingest core opens its own writable
        sessions on ``branch`` and commits them — there is no need to
        pass a pre-opened Session.
    paths
        Source-side paths to ingest. Defaults to the entire store
        (equivalent to ``[""]``).
    branch
        Target branch.
    message
        Commit message; a generic per-phase default is used if omitted.
    concurrency
        Per-batch task count for the source→dest copy.
    on_progress
        Optional callback invoked after every successful commit
        (skeleton + each chunk batch + final close-out) with running
        `IngestStats` that reflect what's durable on the branch.
    on_collision
        How to react when the destination already contains a key the
        source wants to write. See `CollisionPolicy`. Defaults to
        ``CollisionPolicy.Fail``.
    mode
        Copy mode. Only ``"copy"`` (byte-faithful) is supported today.
    checkpoint_every
        Maximum chunk keys copied between commits. Each per-array
        commit in the chunks phase processes at most this many chunks
        before recording a fresh cursor and starting a new session.
        Defaults to 1000 when omitted.
    verify_source_unchanged
        When resuming, compare the bytes of every skeleton
        ``zarr.json`` between source and destination, not just the
        set of paths. Defaults to True. Disable when the source is
        provably stable and the extra GETs at resume matter.

    Returns
    -------
    IngestResult
        The final commit's snapshot id and the running counters.
    """
    if mode != "copy":
        raise ValueError(f"only mode='copy' is supported; got {mode!r}")

    if not isinstance(source, Storage):
        raise TypeError(
            f"source must be an icechunk.Storage; got {type(source).__name__}. "
            "Construct via icechunk.s3_storage, icechunk.local_filesystem_storage, "
            "icechunk.gcs_storage, icechunk.azure_storage, icechunk.http_storage, etc."
        )

    if not isinstance(sink, Repository):
        raise TypeError(
            f"sink must be a Repository; got {type(sink).__name__}. "
            "Pass a Repository and the ingest will drive its own "
            "sessions and commits."
        )

    progress_cb: Callable[[int, int, int], None] | None
    if on_progress is None:
        progress_cb = None
    else:
        cb = on_progress

        def progress_cb(keys: int, nbytes: int, arrays_done: int) -> None:
            cb(IngestStats(keys=keys, bytes=nbytes, arrays_done=arrays_done))

    outcome = py_ingest_zarr(
        source,
        sink._repository,
        list(paths) if paths is not None else [""],
        str(branch),
        int(concurrency),
        on_collision,
        bool(verify_source_unchanged),
        checkpoint_every,
        message,
        progress_cb,
    )
    return IngestResult(
        snapshot_id=outcome.snapshot_id,
        stats=IngestStats(
            keys=outcome.stats.keys,
            bytes=outcome.stats.bytes,
            arrays_done=outcome.stats.arrays_done,
        ),
    )
