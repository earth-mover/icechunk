#!/usr/bin/env python3
from collections.abc import Hashable, Mapping, MutableMapping
from typing import Any, Literal, Union

import zarr

from icechunk import IcechunkStore
from .dask import stateful_store_reduce

from xarray import Dataset
from xarray.core.types import ZarrWriteModes  # TODO: delete this
from xarray.backends.api import (
    _validate_dataset_names,
    dump_to_store,
)
from xarray.backends.zarr import ZarrStore, _choose_default_mode
from xarray.backends.common import ArrayWriter
from dataclasses import dataclass, field

# TODO: import-time check on Xarray version
#
try:
    from dask.delayed import Delayed

    has_dask = True
except ImportError:
    Delayed = None  # type: ignore
    has_dask = False


def extract_stores(zarray: zarr.Array) -> IcechunkStore:
    return zarray.store


def merge_stores(*stores: IcechunkStore) -> IcechunkStore:
    store, *rest = stores
    for other in rest:
        store.merge(other.change_set_bytes())
    return store


def is_chunked_array(x: Any) -> bool:
    if has_dask:
        import dask

        return dask.base.is_dask_collection(x)
    else:
        return False


class LazyArrayWriter(ArrayWriter):
    def __init__(self) -> None:
        super().__init__()

        self.eager_sources = []
        self.eager_targets = []
        self.eager_regions = []

    def add(self, source, target, region=None):
        if is_chunked_array(source):
            self.sources.append(source)
            self.targets.append(target)
            self.regions.append(region)
        else:
            self.eager_sources.append(source)
            self.eager_targets.append(target)
            self.eager_regions.append(region)

    def write_eager(self) -> None:
        for source, target, region in zip(
            self.eager_sources, self.eager_targets, self.eager_regions, strict=True
        ):
            source[region or ...] = target


@dataclass
class XarrayDatasetWriter:
    dataset: Dataset
    store: IcechunkStore = field(kw_only=True)

    delayed_write_task: Union[Delayed, None] = field(init=False, default=None)
    xarray_store: ZarrStore = field(init=False, repr=False)
    writer: LazyArrayWriter = field(init=False, repr=False)

    def __post_init__(self) -> None:
        if not isinstance(self.store, IcechunkStore):
            raise ValueError(
                f"Please pass in an IcechunkStore. Recevied {type(self.store)!r} instead."
            )

    def write_metadata(
        self,
        *,
        group: str | None = None,
        mode: ZarrWriteModes | None = None,
        encoding: Mapping | None = None,
        append_dim: Hashable | None = None,
        region: Mapping[str, slice | Literal["auto"]] | Literal["auto"] | None = None,
        write_empty_chunks: bool | None = None,
        safe_chunks: bool = True,
    ) -> None:
        """
        This method creates new Zarr arrays when necessary, writes attributes,
        and any in-memory arrays.
        """
        from xarray.backends.zarr import _choose_default_mode
        from xarray.backends.api import _validate_dataset_names, dump_to_store

        # validate Dataset keys, DataArray names
        _validate_dataset_names(self.dataset)

        self.mode = _choose_default_mode(mode=mode, append_dim=append_dim, region=region)

        self.xarray_store = ZarrStore.open_group(
            store=self.store,
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
        )

        if encoding is None:
            encoding = {}
        self.xarray_store._validate_encoding(encoding)

        dataset = self.xarray_store._validate_and_autodetect_region(self.dataset)

        self.writer = LazyArrayWriter()
        # TODO: figure out how to properly handle unlimited_dims
        dump_to_store(dataset, self.xarray_store, self.writer, encoding=encoding)

    def write_eager(self):
        self.writer.write_eager()

    def write_lazy(
        self,
        chunkmanager_store_kwargs: MutableMapping | None = None,
        split_every: int | None = None,
    ) -> None:
        if not self.writer.sources:
            return

        chunkmanager_store_kwargs = chunkmanager_store_kwargs or {}
        chunkmanager_store_kwargs["load_stored"] = False
        chunkmanager_store_kwargs["return_stored"] = True

        # This writes to Zarr, and we receive a dask array where each chunk is a Zarr array
        # each of those zarr.Array.store contains the changesets we need
        stored_arrays = self.writer.sync(
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
        self.store.merge(merged_store.change_set_bytes())


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
    split_every: int | None = None,
    **kwargs,
) -> None:
    writer = XarrayDatasetWriter(dataset, store=store)
    writer.write_metadata(group=group, mode=mode)  # write metadata
    writer.write_eager()  # write in-memory arrays
    writer.write_lazy()  # eagerly write dask arrays
