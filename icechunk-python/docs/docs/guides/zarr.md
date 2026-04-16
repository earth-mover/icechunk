# Icechunk + Zarr

Icechunk implements the [Zarr store interface](../understanding/faq.md#what-is-icechunks-relationship-to-zarr), so all standard Zarr operations work out of the box.
This page covers Zarr-specific features that are worth highlighting when used with Icechunk.

For basic read/write operations, see the [Quickstart](../getting-started/quickstart.md) and [How To](../getting-started/howto.md#reading-writing-and-modifying-data-with-zarr) guides.

## Sharding

Icechunk supports [Zarr sharding](https://zarr-specs.readthedocs.io/en/latest/v3/codecs/sharding-indexed/v1.0.html), which packs multiple chunks into a single storage object. This can dramatically reduce the number of objects in storage for datasets with many small chunks.

```python exec="on" session="zarr" source="material-block"
import numpy as np
import zarr
import icechunk

repo = icechunk.Repository.create(storage=icechunk.in_memory_storage())
session = repo.writable_session("main")
store = session.store

arr = zarr.create_array(
    store=store,
    name="sharded_array",
    shape=(100, 100),
    dtype="float64",
    fill_value=0.0,
    chunks=(10, 10),       # inner chunk size
    shards=(50, 50),       # shard size (must be a multiple of chunk size)
)

arr[:] = np.random.default_rng(0).standard_normal((100, 100))
```

```python exec="on" session="zarr" source="material-block" result="code"
print(session.commit("add sharded array"))
```

## Rectilinear chunk grids

Icechunk supports Zarr's [rectilinear chunk grid](https://zarr.dev/zeps/draft/ZEP0003.html) feature
([zarr-python PR](https://github.com/zarr-developers/zarr-python/pull/3802)),
which allows chunk sizes to vary along each dimension.
This is useful when your data has a natural non-uniform partitioning
(e.g. variable-length time intervals or irregular spatial tiles).

!!! note

    Rectilinear chunk grids require an unreleased development version of zarr-python:

    ```
    pip install zarr @ git+https://github.com/maxrjones/zarr-python.git@main
    ```

```python
import numpy as np
import zarr
import icechunk

zarr.config.set({"array.rectilinear_chunks": True})

repo = icechunk.Repository.create(storage=icechunk.in_memory_storage())
session = repo.writable_session("main")
store = session.store

# Chunks of size 3, 3, 4 along dim 0 and 4, 4, 4 along dim 1
chunks = [[3, 3, 4], [4, 4, 4]]

arr = zarr.create_array(
    store=store,
    name="rectilinear_2d",
    shape=(10, 12),
    dtype="float64",
    fill_value=0.0,
    chunks=chunks,
)

arr[:] = np.arange(120, dtype="float64").reshape(10, 12)
session.commit("add rectilinear array")
```

## Virtually ingesting existing Zarr stores

If your data is already in Zarr format — either Zarr v2 or Zarr v3 — you can virtually ingest it into Icechunk without rewriting any chunks. Every chunk stays in the source store; Icechunk just records a reference to it. This is a fast way to bring an existing Zarr archive under Icechunk's version control, and a stepping stone for consolidating multiple stores into a single Icechunk repository. See the [Virtual Datasets guide](./virtual.md) for background on how virtual references work.

Use VirtualiZarr's [`ZarrParser`](https://virtualizarr.readthedocs.io/en/latest/usage.html), which handles both v2 and v3 stores and auto-detects the format. This requires `virtualizarr >= 2.5.0` (sharded v3 arrays require `>= 2.5.1`).

```shell
pip install "virtualizarr>=2.5.1" icechunk
```

Point `open_virtual_dataset` at the existing store:

```python
from virtualizarr import open_virtual_dataset
from virtualizarr.parsers import ZarrParser
from obstore.store import from_url
from obspec_utils.registry import ObjectStoreRegistry

bucket = "s3://mybucket/"
store = from_url(bucket, region="us-east-1")
registry = ObjectStoreRegistry({bucket: store})

virtual_ds = open_virtual_dataset(
    url="s3://mybucket/path/to/store.zarr",
    parser=ZarrParser(),
    registry=registry,
)
```

If the source store contains multiple groups, select one with the `group` argument, or use [`open_virtual_datatree`](https://virtualizarr.readthedocs.io/en/latest/usage.html) to open them all at once:

```python
virtual_ds = open_virtual_dataset(
    url="s3://mybucket/store.zarr",
    parser=ZarrParser(),
    group="my_group",
    registry=registry,
)
```

Writing the result to an Icechunk repo works the same as for any other virtual dataset — configure a [`VirtualChunkContainer`](../reference/virtual.md#icechunk.virtual.VirtualChunkContainer) matching the source location, then call `.vz.to_icechunk`:

```python
import icechunk as ic

storage = ic.local_filesystem_storage(path="my_repo")
config = ic.config.RepositoryConfig.default()
config.set_virtual_chunk_container(
    ic.virtual.VirtualChunkContainer("s3://mybucket/", ic.storage.s3_store(region="us-east-1"))
)
credentials = ic.credentials.containers_credentials(
    {"s3://mybucket/": ic.credentials.s3_credentials(anonymous=True)}
)
repo = ic.Repository.create(storage, config, credentials)

session = repo.writable_session("main")
virtual_ds.vz.to_icechunk(session.store)
session.commit("Virtually ingest existing Zarr store")
```

!!! note

    Virtual ingestion does not move or copy the original chunks — the Icechunk repo will fail to read if the source store is deleted. If you later want Icechunk to own the data, you can read back the virtual dataset and rewrite it as native chunks (e.g. with `xarray`'s `to_zarr` into a fresh Icechunk session).
