# Performance

!!! info

    This is advanced material, and you are unlikely to need it. Icechunk aims to provide an excellent experience out of the box.

## Preloading manifests

Coming Soon.

## Splitting manifests

Icechunk stores chunk references in a chunk manifest file stored in `manifests/`.
For very large arrays (millions of chunks), these files can get quite large.
By default, Icechunk stores all chunk references in a single manifest file.
Requesting even a single chunk requires downloading the entire manifest.
In some cases, this can result in a slow time-to-first-byte.

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
