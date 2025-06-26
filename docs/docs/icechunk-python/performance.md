# Performance

!!! info

    This is advanced material, and you will need it only if you have arrays with more than a million chunks.
    Icechunk aims to provide an excellent experience out of the box.

## Scalability

Icechunk is designed to be cloud native, making it able to take advantage of the horizontal scaling of cloud providers. To learn more, check out [this blog post](https://earthmover.io/blog/exploring-icechunk-scalability) which explores just how well Icechunk can perform when matched with AWS S3.

## Preloading manifests

Coming Soon.

## Splitting manifests

Icechunk stores chunk references in a chunk manifest file stored in `manifests/`.
By default, Icechunk stores all chunk references in a single manifest file per array.
For very large arrays (millions of chunks), these files can get quite large.
Requesting even a single chunk will require downloading the entire manifest.
In some cases, this can result in a slow time-to-first-byte or large memory usage.
Similarly, appending a small amount of data to a large array requires
downloading and rewriting the entire manifest.

!!! note

    Note that the chunk sizes in the following examples are tiny for demonstration purposes.


### Configuring splitting

To solve this issue, Icechunk lets you __split_ the manifest files by specifying a ``ManifestSplittingConfig``.
```python exec="on" session="perf" source="material-block"
import icechunk as ic
from icechunk import ManifestSplitCondition, ManifestSplittingConfig, ManifestSplitDimCondition

split_config = ManifestSplittingConfig.from_dict(
    {
        ManifestSplitCondition.AnyArray(): {
            ManifestSplitDimCondition.DimensionName("time"): 365 * 24
        }
    }
)
repo_config = ic.RepositoryConfig(
    manifest=ic.ManifestConfig(splitting=split_config),
)
```

Then pass the `config` to `Repository.open` or `Repository.create`
```python
repo = ic.Repository.open(..., config=repo_config)
```

!!! important

    Once you find a splitting configuration you like, remember to persist it on-disk using `repo.save_config`.

This particular example splits manifests so that each manifest contains `365 * 24` chunks along the time dimension, and every chunk along every other dimension in a single file.

Options for specifying the arrays whose manifest you want to split are:

1. [`ManifestSplitCondition.name_matches`](./reference.md#icechunk.ManifestSplitCondition.name_matches) takes a regular expression used to match an array's name;
2. [`ManifestSplitCondition.path_matches`](./reference.md#icechunk.ManifestSplitCondition.path_matches) takes a regular expression used to match an array's path;
3. [`ManifestSplitCondition.and_conditions`](./reference.md#icechunk.ManifestSplitCondition.and_conditions) to combine (1), (2), and (4) together; and
4. [`ManifestSplitCondition.or_conditions`](./reference.md#icechunk.ManifestSplitCondition.or_conditions) to combine (1), (2), and (3) together.


`And` and `Or` may be used to combine multiple path and/or name matches. For example,
```python exec="on" session="perf" source="material-block"
array_condition = ManifestSplitCondition.or_conditions(
    [
        ManifestSplitCondition.name_matches("temperature"),
        ManifestSplitCondition.name_matches("salinity"),
    ]
)
sconfig = ManifestSplittingConfig.from_dict(
    {array_condition: {ManifestSplitDimCondition.DimensionName("longitude"): 3}}
)
```

Options for specifying how to split along a specific axis or dimension are:

1. [`ManifestSplitDimCondition.Axis`](./reference.md#icechunk.ManifestSplitDimCondition.Axis) takes an integer axis;
2. [`ManifestSplitDimCondition.DimensionName`](./reference.md#icechunk.ManifestSplitDimCondition.DimensionName) takes a regular expression used to match the dimension names of the array;
3. [`ManifestSplitDimCondition.Any`](./reference.md#icechunk.ManifestSplitDimCondition.Any) matches any _remaining_ dimension name or axis.


For example, for an array with dimensions `time, latitude, longitude`, the following config
```python exec="on" session="perf" source="material-block"
from icechunk import ManifestSplitDimCondition

{
    ManifestSplitDimCondition.DimensionName("longitude"): 3,
    ManifestSplitDimCondition.Axis(1): 2,
    ManifestSplitDimCondition.Any(): 1,
}
```
will result in splitting manifests so that each manifest contains (3 longitude chunks x 2 latitude chunks x 1 time chunk) = 6 chunks per manifest file.


!!! note

    Python dictionaries preserve insertion order, so the first condition encountered takes priority.



### Splitting behaviour

By default, Icechunk minimizes the number of chunk refs that are written in a single commit.

Consider this simple example: a 1D array with split size 1 along axis 0.
```python exec="on" session="perf" source="material-block"
import random

import icechunk as ic
from icechunk import (
    ManifestSplitCondition,
    ManifestSplitDimCondition,
    ManifestSplittingConfig,
)

split_config = ManifestSplittingConfig.from_dict(
    {ManifestSplitCondition.AnyArray(): {ManifestSplitDimCondition.Any(): 1}}
)
repo_config = ic.RepositoryConfig(manifest=ic.ManifestConfig(splitting=split_config))

storage = ic.local_filesystem_storage(
    f"/tmp/splitting-test/{random.randint(100, 20000)}"
)
# Note any config passed to Repository.create is persisted to disk.
repo = ic.Repository.create(storage, config=repo_config)
```

Create an array
```python exec="on" session="perf" source="material-block"
import zarr

session = repo.writable_session("main")
root = zarr.group(session.store)
name = "array"
array = root.create_array(name=name, shape=(10,), dtype=int, chunks=(1,))
```

Now lets write 5 chunk references
```python exec="on" session="perf" source="material-block"
import numpy as np

array[:5] = np.arange(10, 15)
print(session.status())
```

And commit
```python exec="on" session="perf" source="material-block"
snap = session.commit("Add 5 chunks")
```

Use [`repo.lookup_snapshot`](./reference.md#icechunk.Repository.lookup_snapshot) to examine the manifests associated with a Snapshot
```python exec="on" session="perf" source="material-block"
print(repo.lookup_snapshot(snap).manifests)
```

Let's open the Repository again with a different splitting config --- where 5 chunk references are in a single manifest.
```python exec="on" session="perf" source="material-block"
split_config = ManifestSplittingConfig.from_dict(
    {ManifestSplitCondition.AnyArray(): {ManifestSplitDimCondition.Any(): 5}}
)
repo_config = ic.RepositoryConfig(manifest=ic.ManifestConfig(splitting=split_config))
new_repo = ic.Repository.open(storage, config=repo_config)
print(new_repo.config.manifest)
```

Now let's append data.
```python exec="on" session="perf" source="material-block"
session = new_repo.writable_session("main")
array = zarr.open_array(session.store, path=name, mode="a")
array[6:9] = [1, 2, 3]
print(session.status())
```

```python  exec="on" session="perf" source="material-block"
snap2 = session.commit("appended data")
repo.lookup_snapshot(snap2).manifests
```

Look carefully, only one new manifest with the 3 new chunk refs has been written.

Why?

Icechunk minimizes how many chunk references are rewritten at each commit (to save time and memory). The previous splitting configuration (split size of 1) results in manifests that are _compatible_ with the current configuration (split size of 5) because the bounding box of every existing manifest `slice(0, 1)`, `slice(1, 2)`, etc. is fully contained in the bounding boxes implied by the new configuration `[slice(0, 5), slice(5, 10)]`.

Now for a more complex example: let's rewrite the references in `slice(3,7)` i.e. spanning the break in manifests

```python  exec="on" session="perf" source="material-block"
session = new_repo.writable_session("main")
array = zarr.open_array(session.store, path=name, mode="a")
array[3:7] = [1, 2, 3, 4]
print(session.status())
```

```python  exec="on" session="perf" source="material-block"
snap3 = session.commit("rewrite [3,7)")
print(repo.lookup_snapshot(snap3).manifests)
```
This ends up rewriting all refs to two new manifests.

### Rewriting manifests

Remember, by default Icechunk only writes one manifest per array regardless of size.
For large enough arrays, you might see a relative performance hit while committing a new update (e.g. an append),
or when reading from a Repository object that was just created.
At that point, you will want to experiment with different manifest split configurations.

To force Icechunk to rewrite all chunk refs to the current splitting configuration use [`rewrite_manifests`](./reference.md#icechunk.Repository.rewrite_manifests)
--- for the current example this will consolidate to two manifests.

To illustrate, we will use a split size of 3.
```python exec="on" session="perf" source="material-block"
split_config = ManifestSplittingConfig.from_dict(
    {ManifestSplitCondition.AnyArray(): {ManifestSplitDimCondition.Any(): 3}}
)
repo_config = ic.RepositoryConfig(
    manifest=ic.ManifestConfig(splitting=split_config),
)
new_repo = ic.Repository.open(storage, config=repo_config)

snap4 = new_repo.rewrite_manifests(
    f"rewrite_manifests with new config", branch="main"
)
```

`rewrite_snapshots` will create a new commit on `branch` with the provided `message`.
```python exec="on" session="perf" source="material-block"
print(repo.lookup_snapshot(snap4).manifests)
```

The splitting configuration is saved in the snapshot metadata.
```python exec="on" session="perf" source="material-block"
print(repo.lookup_snapshot(snap4).metadata)
```

!!! important

    Once you find a splitting configuration you like, remember to persist it on-disk using `repo.save_config`.


### Example workflow

Here is an example workflow for experimenting with splitting

```python exec="on" session="perf" source="material-block"
# first define a new config
split_config = ManifestSplittingConfig.from_dict(
    {ManifestSplitCondition.AnyArray(): {ManifestSplitDimCondition.Any(): 5}}
)
repo_config = ic.RepositoryConfig(
    manifest=ic.ManifestConfig(splitting=split_config),
)
# open the repo with the new config.
repo = ic.Repository.open(storage, config=repo_config)
```

We will rewrite the manifests on a different branch
```python exec="on" session="perf" source="material-block"
repo.create_branch("split-experiment-1")
snap = repo.rewrite_manifests(
    f"rewrite_manifests with new config", branch="split-experiment-1"
)
```
Now benchmark reads on `main` vs `split-experiment-1`
```python exec="on" session="perf" source="material-block"
store = repo.readonly_session("main").store
store_split = repo.readonly_session("split-experiment-1").store
# ...
```
Assume we decided the configuration on `split-experiment-1` was good.
First we persist that configuration to disk
```python exec="on" session="perf" source="material-block"
repo.save_config()
```

Now point main to the commit with rewritten manifests
```python exec="on" session="perf" source="material-block"
repo.reset_branch("main", repo.lookup_branch("split-experiment-1"))
```

Notice that the persisted config is restored when opening a Repository
```python exec="on" session="perf" source="material-block"
print(ic.Repository.open(storage).config.manifest)
```
