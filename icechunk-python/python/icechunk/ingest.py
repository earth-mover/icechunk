"""User-facing entry point for byte-faithful ingest of a zarr v3 store
into an icechunk repository.

The Rust binding (`py_ingest_zarr`) drives the entire copy: it opens
writable sessions on the target branch, copies keys batch-by-batch,
commits between batches so progress is durable, and returns the final
snapshot id. This module sniffs the Python `source` argument, unwraps
it down to an obstore `ObjectStore` instance, and dispatches.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

from icechunk._icechunk_python import py_ingest_zarr
from icechunk.repository import Repository

if TYPE_CHECKING:
    import zarr.abc.store

__all__ = ["IngestResult", "IngestStats", "from_zarr"]


@dataclass(frozen=True, slots=True)
class IngestStats:
    """Counters reported after an ingest."""

    keys: int
    bytes: int


@dataclass(frozen=True, slots=True)
class IngestResult:
    """Outcome of an `from_zarr` call: stats plus the final snapshot id."""

    snapshot_id: str
    stats: IngestStats


def _unwrap_source(source: zarr.abc.store.Store) -> object:
    """Return the underlying obstore `ObjectStore` for a supported zarr Store.

    Raises `NotImplementedError` if `source` is not a recognised
    obstore-backed zarr Store.
    """
    # Lazy imports: callers that don't use ingest shouldn't need obstore
    # or zarr at import time.
    try:
        import zarr.storage
    except ImportError as e:
        raise ImportError(
            "from_zarr requires zarr-python; install it with `pip install zarr`."
        ) from e

    # `zarr.storage.ObjectStore` is the experimental zarr 3.x wrapper
    # around an obstore Store. Older zarr-python releases don't have it.
    obj_store_cls = getattr(zarr.storage, "ObjectStore", None)
    if obj_store_cls is not None and isinstance(source, obj_store_cls):
        return source.store

    if isinstance(source, zarr.storage.LocalStore):
        try:
            import obstore.store as _obstore_store
        except ImportError as e:
            raise ImportError(
                "from_zarr with a zarr.storage.LocalStore source requires "
                "obstore; install it with `pip install obstore`."
            ) from e
        return _obstore_store.LocalStore(str(source.root))

    raise NotImplementedError(
        "ingest source must be a zarr.storage.ObjectStore (wrapping an "
        "obstore Store) or a zarr.storage.LocalStore; got "
        f"{type(source).__name__}"
    )


def from_zarr(
    source: zarr.abc.store.Store,
    sink: Repository,
    *,
    paths: Sequence[str] = ("",),
    branch: str = "main",
    message: str | None = None,
    concurrency: int = 32,
    on_progress: Callable[[IngestStats], None] | None = None,
    skip_existing: bool = False,
    overwrite: bool = False,
    mode: Literal["copy"] = "copy",
    checkpoint_every: int | None = None,
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
        A zarr-python Store. Currently supports
        `zarr.storage.ObjectStore` (any obstore-backed Store) and
        `zarr.storage.LocalStore`.

        Any source-side prefix should be configured directly on the
        underlying obstore Store (e.g.
        ``obstore.store.S3Store(bucket=..., prefix="path/to/root")``).
        There is no separate ``source_prefix`` kwarg — embed it in the
        Store itself before wrapping with ``zarr.storage.ObjectStore``.
    sink
        Target `Repository`. The ingest core opens its own writable
        sessions on ``branch`` and commits them — there is no need to
        pass a pre-opened Session.
    paths
        Source-side paths to ingest. Defaults to the entire store.
    branch
        Target branch.
    message
        Commit message; a generic per-phase default is used if omitted.
    concurrency
        Per-batch task count for the source→dest copy.
    on_progress
        Optional callback invoked periodically with running
        `IngestStats`.
    skip_existing
        Skip keys already present in the destination. Mutually
        exclusive with `overwrite`.
    overwrite
        Allow clobbering existing destination keys. Re-runs an ingest
        from scratch even if the target branch already holds a
        completed ingest.
    mode
        Copy mode. Only ``"copy"`` (byte-faithful) is supported today.
        Anticipated future values include ``"virtual"`` (write virtual
        references instead of copying chunk bytes) and
        ``"same-bucket-copy"`` (use ObjectStore server-side copy ops
        when source and sink share a bucket).
    checkpoint_every
        Maximum chunk keys copied between commits. Each per-array
        commit in the chunks phase processes at most this many chunks
        before recording a fresh cursor and starting a new session.
        Defaults to 1000 when omitted.

    Returns
    -------
    IngestResult
        The final commit's snapshot id and the running counters.
    """
    if mode != "copy":
        raise ValueError(f"only mode='copy' is supported; got {mode!r}")

    if not isinstance(sink, Repository):
        raise TypeError(
            f"sink must be a Repository; got {type(sink).__name__}. "
            "Session sinks are no longer supported — pass a Repository "
            "and the ingest will drive its own sessions/commits."
        )

    obstore_source = _unwrap_source(source)

    progress_cb: Callable[[int, int], None] | None
    if on_progress is None:
        progress_cb = None
    else:
        # Adapt the (keys, bytes) shape from the Rust binding into the
        # public dataclass.
        cb = on_progress

        def progress_cb(keys: int, nbytes: int) -> None:
            cb(IngestStats(keys=keys, bytes=nbytes))

    outcome = py_ingest_zarr(
        obstore_source,
        "",
        sink._repository,
        list(paths),
        str(branch),
        int(concurrency),
        bool(skip_existing),
        bool(overwrite),
        checkpoint_every,
        message,
        progress_cb,
    )
    return IngestResult(
        snapshot_id=outcome.snapshot_id,
        stats=IngestStats(keys=outcome.stats.keys, bytes=outcome.stats.bytes),
    )
