import functools
from collections.abc import Callable, Mapping
from typing import Any, ParamSpec, TypeAlias, TypeVar

import numpy as np
from packaging.version import Version

import dask
import dask.array
import dask.array as da
import zarr
from dask.array.core import Array
from icechunk.distributed import extract_session, merge_sessions
from icechunk.session import ForkSession

SimpleGraph: TypeAlias = Mapping[tuple[str, int], tuple[Any, ...]]


# We wrap `extract_session` and `merge_sessions` to explicitly handle the `meta` computation.

P = ParamSpec("P")
R = TypeVar("R")


def computing_meta(func: Callable[P, R]) -> Callable[P, Any]:
    """
    A decorator to handle the dask-specific `computing_meta` flag.

    If `computing_meta` is True in the keyword arguments, the decorated
    function will return a placeholder meta object (np.array([object()], dtype=object)).
    Otherwise, it will execute the original function.
    """

    @functools.wraps(func)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> Any:
        if kwargs.get("computing_meta", False):
            return np.array([object()], dtype=object)
        return func(*args, **kwargs)

    return wrapper


def merge_sessions_array_kwargs(
    *sessions: Any, axis: Any = None, keepdims: Any = None
) -> ForkSession:
    return merge_sessions(*sessions)


def _assert_correct_dask_version() -> None:
    if Version(dask.__version__) < Version("2024.11.0"):
        raise ValueError(
            f"Writing to icechunk requires dask>=2024.11.0 but you have {dask.__version__}. Please upgrade."
        )


def store_dask(
    *,
    sources: list[Array],
    targets: list[zarr.Array],
    regions: list[tuple[slice, ...]] | None = None,
    split_every: int | None = None,
    **store_kwargs: Any,
) -> ForkSession:
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
    return session_merge_reduction(stored_arrays, split_every=split_every, **store_kwargs)


def session_merge_reduction(
    arrays: Array | list[Array], *, split_every: int | None, **store_kwargs: Any
) -> ForkSession:
    if isinstance(arrays, da.Array):
        arrays = [arrays]
    # Now we tree-reduce all changesets
    # reduce the individual arrays since concatenation isn't always trivial due
    # to different shapes
    merged_sessions = [
        da.reduction(  # type: ignore[no-untyped-call]
            arr,
            name="ice-changeset",
            chunk=computing_meta(extract_session),
            aggregate=computing_meta(merge_sessions_array_kwargs),
            split_every=split_every,
            concatenate=False,
            keepdims=False,
            dtype=object,
            meta=np.array([object()], dtype=object),
            **store_kwargs,
        )
        for arr in arrays
    ]
    merged_session = merge_sessions(
        *da.compute(*merged_sessions)  # type: ignore[no-untyped-call]
    )
    return merged_session
