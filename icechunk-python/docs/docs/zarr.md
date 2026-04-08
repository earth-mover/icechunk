# Icechunk + Zarr

Icechunk implements the [Zarr store interface](./faq.md#what-is-icechunks-relationship-to-zarr), so all standard Zarr operations work out of the box.
This page covers Zarr-specific features that are worth highlighting when used with Icechunk.

For basic read/write operations, see the [Quickstart](./quickstart.md) and [How To](./howto.md#reading-writing-and-modifying-data-with-zarr) guides.

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

    Rectilinear chunk grids require a development version of zarr-python with the feature branch:

    ```
    pip install zarr @ git+https://github.com/maxrjones/zarr-python.git@poc/unified-chunk-grid
    ```

```python exec="on" session="zarr-rect" source="material-block"
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
```

```python exec="on" session="zarr-rect" source="material-block" result="code"
print(session.commit("add rectilinear array"))
```
