import itertools
from collections.abc import Callable, Iterable, Mapping, MutableMapping, Sequence
from typing import (
    TYPE_CHECKING,
    Any,
    Literal,
    TypeAlias,
    overload,
)

from packaging.version import Version

import dask
import dask.array
import zarr
from dask import config
from dask.array.core import Array
from dask.base import compute_as_if_collection, tokenize
from dask.core import flatten
from dask.delayed import Delayed
from dask.highlevelgraph import HighLevelGraph
from icechunk import IcechunkStore
from icechunk.distributed import extract_store, merge_stores

SimpleGraph: TypeAlias = Mapping[tuple[str, int], tuple[Any, ...]]


def _assert_correct_dask_version() -> None:
    if Version(dask.__version__) < Version("2024.11.0"):
        raise ValueError(
            f"Writing to icechunk requires dask>=2024.11.0 but you have {dask.__version__}. Please upgrade."
        )


def store_dask(
    store: IcechunkStore,
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

    Parameters
    ----------
    store: IcechunkStore
        Icechunk store to write to.
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
    stored_arrays = dask.array.store(  # type: ignore[attr-defined]
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
    merged_store = stateful_store_reduce(
        stored_arrays,
        prefix="ice-changeset",
        chunk=extract_store,
        aggregate=merge_stores,
        split_every=split_every,
        compute=True,
        **store_kwargs,
    )
    store.merge(merged_store.change_set_bytes())


# tree-reduce all changesets, regardless of array
def partial_reduce(
    aggregate: Callable[..., Any],
    keys: Iterable[tuple[Any, ...]],
    *,
    layer_name: str,
    split_every: int,
) -> SimpleGraph:
    """
    Creates a new dask graph layer, that aggregates `split_every` keys together.
    """
    from toolz import partition_all

    return {
        (layer_name, i): (aggregate, *keys_batch)
        for i, keys_batch in enumerate(partition_all(split_every, keys))
    }


@overload
def stateful_store_reduce(
    stored_arrays: Sequence[Array],
    *,
    chunk: Callable[..., Any],
    aggregate: Callable[..., Any],
    prefix: str | None = None,
    split_every: int | None = None,
    compute: Literal[False] = False,
    **kwargs: Any,
) -> Delayed: ...


@overload
def stateful_store_reduce(
    stored_arrays: Sequence[Array],
    *,
    chunk: Callable[..., Any],
    aggregate: Callable[..., Any],
    compute: Literal[True] = True,
    prefix: str | None = None,
    split_every: int | None = None,
    **kwargs: Any,
) -> IcechunkStore: ...


def stateful_store_reduce(
    stored_arrays: Sequence[Array],
    *,
    chunk: Callable[..., Any],
    aggregate: Callable[..., Any],
    compute: bool = True,
    prefix: str | None = None,
    split_every: int | None = None,
    **kwargs: Any,
) -> IcechunkStore | Delayed:
    _assert_correct_dask_version()

    split_every = split_every or config.get("split_every", 8)

    layers: MutableMapping[str, SimpleGraph] = {}
    dependencies: MutableMapping[str, set[str]] = {}

    array_names = tuple(a.name for a in stored_arrays)
    all_array_keys = list(
        # flatten is untyped
        itertools.chain(*[flatten(array.__dask_keys__()) for array in stored_arrays])  # type: ignore[no-untyped-call]
    )
    token = tokenize(array_names, chunk, aggregate, split_every)

    # Each write task returns one Zarr array,
    # now extract the changeset (as bytes) from each of those Zarr arrays
    map_layer_name = f"{prefix}-blockwise-{token}"
    map_dsk: SimpleGraph = {
        (map_layer_name, i): (chunk, key) for i, key in enumerate(all_array_keys)
    }
    layers[map_layer_name] = map_dsk
    dependencies[map_layer_name] = set(array_names)
    latest_layer = map_layer_name

    if aggregate is not None:
        # Now tree-reduce across *all* write tasks,
        # regardless of which Array the task belongs to
        aggprefix = f"{prefix}-merge"

        depth = 0
        keys = map_dsk.keys()
        while len(keys) > split_every:
            latest_layer = f"{aggprefix}-{depth}-{token}"

            layers[latest_layer] = partial_reduce(
                aggregate, keys, layer_name=latest_layer, split_every=split_every
            )
            previous_layer, *_ = next(iter(keys))
            dependencies[latest_layer] = {previous_layer}

            keys = layers[latest_layer].keys()
            depth += 1

        # last one
        latest_layer = f"{aggprefix}-final-{token}"
        layers[latest_layer] = partial_reduce(
            aggregate, keys, layer_name=latest_layer, split_every=split_every
        )
        previous_layer, *_ = next(iter(keys))
        dependencies[latest_layer] = {previous_layer}

    store_dsk = HighLevelGraph.merge(
        HighLevelGraph(layers, dependencies),  # type: ignore[arg-type]
        *[array.__dask_graph__() for array in stored_arrays],
    )
    if compute:
        # copied from dask.array.store
        merged_store, *_ = compute_as_if_collection(  # type: ignore[no-untyped-call]
            Array, store_dsk, list(layers[latest_layer].keys()), **kwargs
        )
        if TYPE_CHECKING:
            assert isinstance(merged_store, IcechunkStore)
        return merged_store

    else:
        key = "stateful-store-" + tokenize(array_names)
        store_dsk = HighLevelGraph.merge(
            HighLevelGraph({key: {key: (latest_layer, 0)}}, {key: {latest_layer}}),
            store_dsk,
        )
        return Delayed(key, store_dsk)  # type: ignore[no-untyped-call]
