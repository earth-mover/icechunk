from collections.abc import Mapping
from typing import Any, TypeAlias

from packaging.version import Version

import dask
import dask.array
import dask.array as da
import zarr
from dask.array.core import Array
from icechunk import Session
from icechunk.distributed import extract_session, merge_sessions

SimpleGraph: TypeAlias = Mapping[tuple[str, int], tuple[Any, ...]]


def _assert_correct_dask_version() -> None:
    if Version(dask.__version__) < Version("2024.11.0"):
        raise ValueError(
            f"Writing to icechunk requires dask>=2024.11.0 but you have {dask.__version__}. Please upgrade."
        )


def store_dask(
    session: Session,
    *,
    sources: list[Array],
    targets: list[zarr.Array],
    regions: list[tuple[slice, ...]] | None = None,
    split_every: int | None = None,
    **store_kwargs: Any,
) -> None:
    """
    A version of ``dask.array.store`` for Icechunk stores.

    This method will eagerly execute writes to the Icechunk store, and will
    merge the changesets corresponding to each write task. The `store` object
    passed in will be updated in-place with the fully merged changeset.

    For distributed or multi-processing writes, this method must be called within
    the `Session.allow_pickling()` context. All Zarr arrays in `targets` must also
    be created within this context since they contain a reference to the Session.

    Parameters
    ----------
    session: Sessions
        Icechunk writable session
    sources: list of `dask.array.Array`
        List of dask arrays to write.
    targets : list of `zarr.Array`
        Corresponding list of Zarr array objects to write to.
    regions: list of tuple of slice, optional
        Corresponding region for each of `targets` to write to.
    split_every: int, optional
        Number of changesets to merge at a given time.
    **store_kwargs:
        Arbitrary keyword arguments passed to `dask.array.store`. Notably `compute`,
        `return_stored`, `load_stored`, and `lock` are unsupported.
    """
    _assert_correct_dask_version()
    stored_arrays = dask.array.store(
        sources=sources,
        targets=targets,  # type: ignore[arg-type]
        regions=regions,
        compute=False,
        return_stored=True,
        load_stored=False,
        lock=False,
        **store_kwargs,
    )
    # Now we tree-reduce all changesets
    # reduce the individual arrays since concatenation isn't always trivial due
    # to different shapes
    merged_sessions = [
        da.reduction(
            arr,
            name="ice-changeset",
            chunk=extract_session,
            aggregate=merge_sessions,
            split_every=split_every,
            concatenate=False,
            dtype=object,
            **store_kwargs,
        )
        for arr in stored_arrays
    ]
    merged_session = merge_sessions(*da.compute(*merged_sessions))
    session.merge(merged_session)
