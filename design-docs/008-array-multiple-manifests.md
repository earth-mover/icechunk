# Multiple manifests per array

## Current Status (Icechunk v0.1.2)

1. A single array has a single manifest, regardless of size. [PR 598](https://github.com/earth-mover/icechunk/pull/598)
2. Manifests are preloaded using regexes: [PR 649](https://github.com/earth-mover/icechunk/pull/649)
3. The yaml config described below has not been implemented yet, but the current status is equivalent to:
```yaml
chunk-manifests:
  - default:
    arrays-per-manifest: 1
    max-manifest-size: null
    cardinality: null
```

## Next
Our next job is to split the manifest files for large arrays, while providing the power-user with a lot of configurability.
How might we optimize the splitting of a manifest file?
1. User: "I expect to only read the most recent data most of the time." Split along "time" so that N days of data are in a single manifest file. For ERA5 for example, we might split by a time frequency: a year's worth of references, or a month's worth of references, in a single manifest.
2. User: "I expect to generally read a small number of vertical levels (<5) for a dataset with O(50) levels.". Split so that a single level's references are in a single file.
3. User: "I have created a large (nominally) 20TB spatial datacube but have only populated relatively small geographical regions of it." Split in space, so that as new regions are populated, existing manifests don't need to be rewritten.

As has been pointed out, this is similar to sharding, and we general want to align the manifest shards with expected read patterns.
Some considerations
1. Zarr does not enforce dimension names, so we must allow splitting by axes too. `dimension_names=null` and dimension_names=`['time', null]` are allowed.
2. Manifest extents must form non-overlapping bounding boxes, so using a `dict` to specify shard sizes seems like the right API.
3. There needs to be a priority order for dimensions. So for example, I may always want different vertical levels in different manifest files, and only split those files once there are many timesteps in that. Thus we use a sequence of key-value pairs in the YAML instead of a map (which is [not guaranteed to be ordered](https://yaml.org/spec/1.2.2/#3221-mapping-key-order)).
4. Repeated dimension names are possible, the user can use axis numbers to set different splits for different axes if needed.
5. We disallow combining dimension names and axis numbers in the same specification.

```yaml
rules:
  - path: ./2m_temperature  # regex, 3D variable: (null, latitude, longitude)
    manifest-split-sizes:
      - 0: 120
  - path: ./temperature  # 4D variable: (time, level, latitude, longitude)
    manifest-split-sizes:
      - "level": 1  # alternatively 0: 1
      - "time": 12  #           and 1: 12
  - path: ./temperature
    manifest-split-sizes:
      - "level": 1
      - "time": 8760  # ~1 year
      - "latitude": null  # for unspecified, default is null, which means never split.
  - path: ./*   # the default rules
    manifest-split-sizes: null  # no splitting, just a single manifest per array
```
