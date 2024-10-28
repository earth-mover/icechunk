#!/usr/bin/env python3
from collections.abc import Hashable, Mapping, MutableMapping
from typing import Literal

import zarr

from icechunk import IcechunkStore
from xarray import Dataset
from xarray.core.types import ZarrWriteModes  # TODO: delete this


def extract_stores(zarray: zarr.Array) -> IcechunkStore:
    return zarray.store


def merge_stores(*stores: IcechunkStore):
    store, *rest = stores
    for other in rest:
        store.merge(other.change_set_bytes())
    return store


def to_icechunk(
    dataset: Dataset,
    store: IcechunkStore,
    group: str | None = None,
    write_empty_chunks: bool | None = None,
    safe_chunks: bool = True,
    mode: ZarrWriteModes | None = None,
    append_dim: Hashable | None = None,
    region: Mapping[str, slice | Literal["auto"]] | Literal["auto"] | None = None,
    encoding: Mapping | None = None,
    chunkmanager_store_kwargs: MutableMapping | None = None,
    # compute: bool = True,
    split_every: int | None = None,
    **kwargs,
) -> IcechunkStore:

    from xarray.backends.api import (
        _validate_dataset_names,
        dump_to_store,
    )
    from xarray.backends.zarr import ZarrStore, _choose_default_mode
    from xarray.backends.common import ArrayWriter

    from .dask import stateful_store_reduce

    # if not isinstance(store, IcechunkStore):
    #    raise ValueError
    # validate Dataset keys, DataArray names
    _validate_dataset_names(dataset)

    if encoding is None:
        encoding = {}

    mode = _choose_default_mode(mode=mode, append_dim=append_dim, region=region)
    zstore = ZarrStore.open_group(
            store=store,
            group=group,
            mode=mode,
            zarr_format=3,
            append_dim=append_dim,
            write_region=region,
            safe_chunks=safe_chunks,
            write_empty=write_empty_chunks,
            synchronizer=None,
            consolidated=False,
            consolidate_on_close=False,
            zarr_version=None,
            **kwargs,
        )

    dataset = zstore._validate_and_autodetect_region(dataset)
    zstore._validate_encoding(encoding)

    writer = ArrayWriter()
    # TODO: figure out how to properly handle unlimited_dims
    dump_to_store(dataset, zstore, writer, encoding=encoding)

    # TODO: assert dask
    chunkmanager_store_kwargs = chunkmanager_store_kwargs or {}
    chunkmanager_store_kwargs["load_stored"] = False
    chunkmanager_store_kwargs["return_stored"] = True

    # This writes to Zarr, and we receive a dask array where each chunk is a Zarr array
    # each of those zarr.Array.store contains the changesets we need
    stored_arrays = writer.sync(
        compute=False, chunkmanager_store_kwargs=chunkmanager_store_kwargs
    )

    # Now we tree-reduce all changesets
    merged_store = stateful_store_reduce(
        stored_arrays,
        prefix="ice-changeset",
        chunk=extract_stores,
        aggregate=merge_stores,
        split_every=split_every,
        compute=True,
        **chunkmanager_store_kwargs,
    )

    # needed in the future
    store.merge(merged_store.change_set_bytes())

    # TODO: IDK about all this.
    # if compute:
    #     _finalize_store(task, zstore)
    # else:
    #     import dask

    #     return dask.delayed(_finalize_store)(task, zstore)

    # return merged_store
