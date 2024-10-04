# Quickstart

## Basic Usage

Once you have everything installed, here's an example of how to use Icechunk.

```python
from icechunk import IcechunkStore, StorageConfig
from zarr import Array, Group


# Example using memory store
storage = StorageConfig.memory("test")
store = await IcechunkStore.open(storage=storage, mode='r+')

# Example using file store
storage = StorageConfig.filesystem("/path/to/root")
store = await IcechunkStore.open(storage=storage, mode='r+')

# Example using S3
s3_storage = StorageConfig.s3_from_env(bucket="icechunk-test", prefix="oscar-demo-repository")
store = await IcechunkStore.open(storage=storage, mode='r+')
```