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

## Preloading manifests

Coming Soon.

## Splitting manifests

Icechunk stores chunk references in a chunk manifest file stored in `manifests/`.
For very large arrays (millions of chunks), these files can get quite large.
By default, Icechunk stores all chunk references in a single manifest file per array.
Requesting even a single chunk requires downloading the entire manifest.
In some cases, this can result in a slow time-to-first-byte or large memory usage.

!!! note

    Note that the chunk sizes in the following examples are tiny for demonstration purposes.

To avoid that, Icechunk lets you split the manifest files by specifying a ``ManifestSplittingConfig``.

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
repo_config = ic.RepositoryConfig(manifest=ic.ManifestConfig(splitting=split_config))
```

Then pass the config to `Repository.open` or `Repository.create`

```python
repo = ic.Repository.open(..., config=repo_config)
```

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
