# Performance

!!! info

    This is advanced material, and you will need it only if you have arrays with more than a million chunks.
    Icechunk aims to provide an excellent experience out of the box.

## Scalability

Icechunk is designed to be cloud native, making it able to take advantage of the horizontal scaling of cloud providers. To learn more, check out [this blog post](https://earthmover.io/blog/exploring-icechunk-scalability) which explores just how well Icechunk can perform when matched with AWS S3.

## Cold buckets and repos

Modern object stores usually reshard their buckets on-the-fly, based on perceived load. The
strategies they use are not published and very hard to discover. The details are not super important
anyway, the important take away is that on new buckets and even on new repositories, the scalability
of the object store may not be great from the start. You are expected to slowly ramp up load, as you
write data to the repository.

Once you have applied consistently high write/read load to a repository for a few minutes, the object
store will usually reshard your bucket allowing for more load. While this resharding happens, different
object stores can respond in different ways. For example, S3 returns 5xx errors with a "SlowDown"
indication. GCS returns 429 responses.

Icechunk helps this process by retrying failed requests with an exponential backoff. In our
experience, the default configuration is enough to ingest into a fresh bucket using around 100 machines.
But if this is not the case for you, you can tune the retry configuration using [StorageRetriesSettings](https://icechunk.io/en/latest/icechunk-python/reference/#icechunk.StorageRetriesSettings).

To learn more about how Icechunk manages object store prefixes, read our
[blog post](https://earthmover.io/blog/exploring-icechunk-scalability)
on Icechunk scalability.

!!! warning

    Currently, Icechunk implementation of retry logic during resharding is not
    [working properly](https://github.com/earth-mover/icechunk/issues/954) on GCS.
    We have a [pull request open](https://github.com/apache/arrow-rs-object-store/pull/410) to
    one of Icechunk's dependencies that will solve this.
    In the meantime, if you get 429 errors from your Google bucket, please lower concurrency and try
    again. Increase concurrency slowly until errors disappear.

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

To solve this issue, Icechunk lets you __split__ the manifest files by specifying a ``ManifestSplittingConfig``.
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

To illustrate, we will use a split size of 3 --- for the current example this will consolidate to two manifests.
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
repo.create_branch("split-experiment-1", repo.lookup_branch("main"))
snap = repo.rewrite_manifests(
    f"rewrite_manifests with new config", branch="split-experiment-1"
)
print(repo.lookup_snapshot(snap).manifests)
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

Now point the `main` branch to the commit with rewritten manifests
```python exec="on" session="perf" source="material-block"
repo.reset_branch("main", repo.lookup_branch("split-experiment-1"))
```

Notice that the persisted config is restored when opening a Repository
```python exec="on" session="perf" source="material-block"
print(ic.Repository.open(storage).config.manifest)
```

## Preloading manifests

While [manifest splitting](./performance.md#splitting-manifests) is a great way to control the size of manifests, it can be useful to configure the manner in which manifests are loaded. In Icechunk manifests are loaded lazily by default, meaning that when you read a chunk, Icechunk will only load the manifest for that chunk when it is needed to fetch the chunk data. While this is good for memory performance, it can increase the latency to first the first elements of an array. Once a manifest has been loaded, it will usually remain in memory for future chunks in the same manifest.

To address this, Icechunk provides a way to preload manifests at the time of opening a Session, loading all manifests that match specific conditions into the cache. This means when it is time to read a chunk, the manifest will already be in the cache and the chunk can be found without having to first load the manifest from storage.

### Configuring preloading

To configure manifest preloading, you can use the `ManifestPreloadConfig` class to specify the `ic.RepositoryConfig.manifest.preload` field.

```python exec="on" session="perf" source="material-block"
preload_config = ic.ManifestPreloadConfig(
    preload_if=ic.ManifestPreloadCondition.name_matches("^x$"),  # preload all manifests with the name "x"
)

repo_config = ic.RepositoryConfig(
    manifest=ic.ManifestConfig(
        preload=preload_config,
    )
)
```

Then pass the `config` to `Repository.open` or `Repository.create`

```python
repo = ic.Repository.open(..., config=repo_config)
```

This example will preload all manifests that match the regex "x" when opening a Session. While this is a simple example, you can use the `ManifestPreloadCondition` class to create more complex preload conditions using the following options:
- `ManifestPreloadCondition.name_matches` takes a regular expression used to match an array's name;
- `ManifestPreloadCondition.path_matches` takes a regular expression used to match an array's path;
- `ManifestPreloadCondition.and_conditions` to combine (1), (2), and (4) together; and
- `ManifestPreloadCondition.or_conditions` to combine (1), (2), and (3) together.

`And` and `Or` may be used to combine multiple path and/or name matches. For example,

```python exec="on" session="perf" source="material-block"
preload_config = ic.ManifestPreloadConfig(
    preload_if=ic.ManifestPreloadCondition.or_conditions(
        ic.ManifestPreloadCondition.name_matches("^x$"),
        ic.ManifestPreloadCondition.path_matches("y"),
    )
)
```

This will preload all manifests that match the array name "x" or where the array path contains "y".

!!! important

    `name_matches` and `path_matches` are regular expressions, so if you only want to match the exact string, you need to use `^x$` instead of just "x". We plan to add more explicit string matching options in the future, see [this issue](https://github.com/earth-mover/icechunk/issues/996).

Preloading can also be limited to manifests that are within a limited size range. This can be useful to limit the amount of memory used by the preload cache, when some manifest may be very large. This can be configured using the `ic.RepositoryConfig.manifest.preload.num_refs` field.

```python exec="on" session="perf" source="material-block"
preload_config = ic.ManifestPreloadConfig(
    preload_if=ic.ManifestPreloadCondition.and_conditions(
        ic.ManifestPreloadCondition.name_matches("x"),
        ic.ManifestPreloadCondition.num_refs(1000, 10000),
    )
)
```

This will preload all manifests that match the array name "x" and have between 1000 and 10000 chunk references.

Lastly, the number of total manifests that can be preloaded can be limited using the `ic.RepositoryConfig.manifest.preload.max_total_refs` field.

```python exec="on" session="perf" source="material-block"
preload_config = ic.ManifestPreloadConfig(
    preload_if=ic.ManifestPreloadCondition.name_matches("x"),
    max_total_refs=10000,
)
```

This will preload all manifests that match the array name "x" while the number of total chunk references that have been preloaded is less than 10000.

!!! important

    Once you find a preload configuration you like, remember to persist it on-disk using `repo.save_config`. The saved config can be overridden at runtime for different applications.


#### Default preload configuration

Icechunk has a default `preload_if` configuration that will preload all manifests that match [cf-xarrays coordinate axis regex](https://github.com/xarray-contrib/cf-xarray/blob/1591ff5ea7664a6bdef24055ef75e242cd5bfc8b/cf_xarray/criteria.py#L149-L160).

Meanwhile, the default `max_total_refs` is set to `10_000`.
