#!/usr/bin/env python3
import importlib
from collections.abc import Hashable, Mapping, MutableMapping
from dataclasses import dataclass, field
from typing import Any, Literal

import numpy as np
from packaging.version import Version

import xarray as xr
import zarr
from icechunk import IcechunkStore
from icechunk.dask import stateful_store_reduce
from icechunk.distributed import extract_store, merge_stores
from icechunk.vendor.xarray import _choose_default_mode
from xarray import Dataset
from xarray.backends.common import ArrayWriter
from xarray.backends.zarr import ZarrStore

Region = Mapping[str, slice | Literal["auto"]] | Literal["auto"] | None
ZarrWriteModes = Literal["w", "w-", "a", "a-", "r+", "r"]


try:
    has_dask = importlib.util.find_spec("dask") is not None
except ImportError:
    has_dask = False

if Version(xr.__version__) < Version("2024.10.0"):
    raise ValueError(
        f"Writing to icechunk requires Xarray>=2024.10.0 but you have {xr.__version__}. Please upgrade."
    )


def is_chunked_array(x: Any) -> bool:
    if has_dask:
        import dask

        return dask.base.is_dask_collection(x)
    else:
        return False


class LazyArrayWriter(ArrayWriter):
    def __init__(self) -> None:
        super().__init__()  # type: ignore[no-untyped-call]

        self.eager_sources: list[np.ndarray[Any, Any]] = []
        self.eager_targets: list[zarr.Array] = []
        self.eager_regions: list[tuple[slice, ...]] = []

    def add(self, source: Any, target: Any, region: Any = None) -> Any:
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
            target[region or ...] = source
        self.eager_sources = []
        self.eager_targets = []
        self.eager_regions = []


@dataclass
class XarrayDatasetWriter:
    """
    Write Xarray Datasets to a group in an Icechunk store.

    This class is private API. Please do not use it.
    """

    dataset: Dataset = field(repr=False)
    store: IcechunkStore = field(kw_only=True)

    safe_chunks: bool = field(kw_only=True, default=True)
    # TODO: uncomment when Zarr has support
    # write_empty_chunks: bool = field(kw_only=True, default=True)

    _initialized: bool = field(default=False, repr=False)

    xarray_store: ZarrStore = field(init=False, repr=False)
    writer: LazyArrayWriter = field(init=False, repr=False)

    def __post_init__(self) -> None:
        if not isinstance(self.store, IcechunkStore):
            raise ValueError(
                f"Please pass in an IcechunkStore. Received {type(self.store)!r} instead."
            )

    def _open_group(
        self,
        *,
        group: str | None,
        mode: ZarrWriteModes | None,
        append_dim: Hashable | None,
        region: Region,
    ) -> None:
        concrete_mode: ZarrWriteModes = _choose_default_mode(
            mode=mode, append_dim=append_dim, region=region
        )

        self.xarray_store = ZarrStore.open_group(
            store=self.store,
            group=group,
            mode=concrete_mode,
            zarr_format=3,
            append_dim=append_dim,
            write_region=region,
            safe_chunks=self.safe_chunks,
            # TODO: uncomment when Zarr has support
            # write_empty=self.write_empty_chunks,
            synchronizer=None,
            consolidated=False,
            consolidate_on_close=False,
            zarr_version=None,
        )

    def write_metadata(self, encoding: Mapping[Any, Any] | None = None) -> None:
        """
        This method creates new Zarr arrays when necessary, writes attributes,
        and any in-memory arrays.
        """
        from xarray.backends.api import _validate_dataset_names, dump_to_store

        # validate Dataset keys, DataArray names
        _validate_dataset_names(self.dataset)

        if encoding is None:
            encoding = {}
        self.xarray_store._validate_encoding(encoding)

        # This writes the metadata (zarr.json) for all arrays
        # This also will resize arrays for any appends
        self.writer = LazyArrayWriter()
        dump_to_store(self.dataset, self.xarray_store, self.writer, encoding=encoding)  # type: ignore[no-untyped-call]

        self._initialized = True

    def write_eager(self) -> None:
        """
        Write in-memory variables to store.

        Returns
        -------
        None
        """
        if not self._initialized:
            raise ValueError("Please call `write_metadata` first.")
        self.writer.write_eager()

    def write_lazy(
        self,
        chunkmanager_store_kwargs: MutableMapping[Any, Any] | None = None,
        split_every: int | None = None,
    ) -> None:
        """
        Write lazy arrays (e.g. dask) to store.
        """
        if not self._initialized:
            raise ValueError("Please call `write_metadata` first.")

        if not self.writer.sources:
            return

        chunkmanager_store_kwargs = chunkmanager_store_kwargs or {}
        chunkmanager_store_kwargs["load_stored"] = False
        chunkmanager_store_kwargs["return_stored"] = True

        # This calls dask.array.store, and we receive a dask array where each chunk is a Zarr array
        # each of those zarr.Array.store contains the changesets we need
        stored_arrays = self.writer.sync(
            compute=False, chunkmanager_store_kwargs=chunkmanager_store_kwargs
        )  # type: ignore[no-untyped-call]

        # Now we tree-reduce all changesets
        merged_store = stateful_store_reduce(
            stored_arrays,
            prefix="ice-changeset",
            chunk=extract_store,
            aggregate=merge_stores,
            split_every=split_every,
            compute=True,
            **chunkmanager_store_kwargs,
        )
        self.store.merge(merged_store.change_set_bytes())


def to_icechunk(
    dataset: Dataset,
    store: IcechunkStore,
    *,
    group: str | None = None,
    mode: ZarrWriteModes | None = None,
    # TODO: uncomment when Zarr has support
    # write_empty_chunks: bool | None = None,
    safe_chunks: bool = True,
    append_dim: Hashable | None = None,
    region: Region = None,
    encoding: Mapping[Any, Any] | None = None,
    chunkmanager_store_kwargs: MutableMapping[Any, Any] | None = None,
    split_every: int | None = None,
    **kwargs: Any,
) -> None:
    """
    Write an Xarray Dataset to a group of an icechunk store.

    Parameters
    ----------
    store : MutableMapping, str or path-like, optional
        Store or path to directory in local or remote file system.
    mode : {"w", "w-", "a", "a-", r+", None}, optional
        Persistence mode: "w" means create (overwrite if exists);
        "w-" means create (fail if exists);
        "a" means override all existing variables including dimension coordinates (create if does not exist);
        "a-" means only append those variables that have ``append_dim``.
        "r+" means modify existing array *values* only (raise an error if
        any metadata or shapes would change).
        The default mode is "a" if ``append_dim`` is set. Otherwise, it is
        "r+" if ``region`` is set and ``w-`` otherwise.
    group : str, optional
        Group path. (a.k.a. `path` in zarr terminology.)
    encoding : dict, optional
        Nested dictionary with variable names as keys and dictionaries of
        variable specific encodings as values, e.g.,
        ``{"my_variable": {"dtype": "int16", "scale_factor": 0.1,}, ...}``
    append_dim : hashable, optional
        If set, the dimension along which the data will be appended. All
        other dimensions on overridden variables must remain the same size.
    region : dict or "auto", optional
        Optional mapping from dimension names to either a) ``"auto"``, or b) integer
        slices, indicating the region of existing zarr array(s) in which to write
        this dataset's data.

        If ``"auto"`` is provided the existing store will be opened and the region
        inferred by matching indexes. ``"auto"`` can be used as a single string,
        which will automatically infer the region for all dimensions, or as
        dictionary values for specific dimensions mixed together with explicit
        slices for other dimensions.

        Alternatively integer slices can be provided; for example, ``{'x': slice(0,
        1000), 'y': slice(10000, 11000)}`` would indicate that values should be
        written to the region ``0:1000`` along ``x`` and ``10000:11000`` along
        ``y``.

        Users are expected to ensure that the specified region aligns with
        Zarr chunk boundaries, and that dask chunks are also aligned.
        Xarray makes limited checks that these multiple chunk boundaries line up.
        It is possible to write incomplete chunks and corrupt the data with this
        option if you are not careful.
    safe_chunks : bool, default: True
        If True, only allow writes to when there is a many-to-one relationship
        between Zarr chunks (specified in encoding) and Dask chunks.
        Set False to override this restriction; however, data may become corrupted
        if Zarr arrays are written in parallel.
        In addition to the many-to-one relationship validation, it also detects partial
        chunks writes when using the region parameter,
        these partial chunks are considered unsafe in the mode "r+" but safe in
        the mode "a".
        Note: Even with these validations it can still be unsafe to write
        two or more chunked arrays in the same location in parallel if they are
        not writing in independent regions.
    chunkmanager_store_kwargs : dict, optional
        Additional keyword arguments passed on to the `ChunkManager.store` method used to store
        chunked arrays. For example for a dask array additional kwargs will be passed eventually to
        `dask.array.store()`. Experimental API that should not be relied upon.
    split_every: int, optional
        Number of tasks to merge at every level of the tree reduction.

    Returns
    -------
    None

    Notes
    -----
    Two restrictions apply to the use of ``region``:

      - If ``region`` is set, _all_ variables in a dataset must have at
        least one dimension in common with the region. Other variables
        should be written in a separate single call to ``to_zarr()``.
      - Dimensions cannot be included in both ``region`` and
        ``append_dim`` at the same time. To create empty arrays to fill
        in with ``region``, use the `XarrayDatasetWriter` directly.
    """
    writer = XarrayDatasetWriter(dataset, store=store)

    writer._open_group(group=group, mode=mode, append_dim=append_dim, region=region)

    # write metadata
    writer.write_metadata(encoding)
    # write in-memory arrays
    writer.write_eager()
    # eagerly write dask arrays
    writer.write_lazy(chunkmanager_store_kwargs=chunkmanager_store_kwargs)
