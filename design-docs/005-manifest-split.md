# Multiple Manifests per Snapshot

Aka: Manifest split

## Current state

Icechunk format allows to have multiple manifest files per snapshot,
multiple manifest per array, and multiple arrays in a single manifest.

Nevertheless, the current Icechunk implementation generates a single manifest
per snapshot, putting in it all the chunks for all arrays in the repo.

Currently `ManifestExtents` are part of the format but are not implemented,
they remain empty for the manifest.

During `flush`, chunks are processed in a streaming fashion, without looking
at specific nodes. All chunks are retrieved, from all nodes, and they are
pushed into a new manifest.

This current state is not great:

* Reading coordinate arrays takes a long time, the full manifest, for all
arrays needs to be downloaded and parsed.
* Each commit needs to rewrite the full manifest.
* Space is wasted, both on disk and in memory, manifest become too large
(~10MB per million virtual references, compressed on disk).

## What we want

* Icechunk to be fast for interactive usage, when a user is exploring a dataset
* Small reads not to parse the whole manifest
* Small writes not to rewrite the whole manifest

## Discussion

This an optimization problem and there are many ways to decide what "ideal"
means. Examples:

* Datasets that are periodically appended along the time dimension, may
benefit from putting in the same manifest several arrays split by time.
Most reads and write will only need to look at the latest manifest.
* Small arrays, particularly coordinate arrays, would benefit from packing
multiple arrays in a single, small manifest that can be read quickly during
interactive usage.
* Huge arrays require splitting into multiple manifests, and it becomes
important to know along what dimension they should be split for effective
locality.

At this point, we want to build only the first iteration of this. We don't need
to be super smart, as long as interactive usage is not too bad. Developing the
first iteration would help better defining the datastructures and changes
required to the on-disk format.

Since there are so many cases, we want the ability to let the user configure
the algorithm to some degree. That way we don't need to be as smart with the
defaults, while at the same time offering full power for advanced users.

We have analyzed real world datasets, and verified that there is a bimodal
distribution of number of chunks per array. Some arrays have very low number
of chunks, these are usually the coordinate arrays; and some others have a much larger
number of chunks, the actual variables. We can use this fact to pack together the
manifests for all small arrays, in a small, fast to parse manifest. This is
probably a good default behavior, if we put it together with a limit to
the manifest sizes for the remaining arrays.

## Design

We don't implement array splitting at this time. Each array will go fully into
a single manifest. This is of course a limitation for very large arrays, but one
we will lift in a future iteration. The on-disk format model supports the split
and the current design must not block it.

We define _manifest sets_. A manifest set is a group of manifests that
is intended to optimize for certain types of arrays. We let the user define
the sets, but we offer good defaults that should work reasonably well in
common cases.

We let the user indicate _rules_ that assign arrays to
a specific manifest set. These rules can be based on array paths or number
of chunks. In the future we could add more power to the rules system.

Other new feature we provide is the ability to preload certain manifests.
These manifests will be fetched in the background as soon as a session is
created, speeding up interactive use. In the future, we may add the ability
to prefetch manifests for certain branches/tags as soon as the Repository is
open.

Manifest sets, array rules, and manifest prefetch, are configured in the
persistent configuration of the repository and can be overloaded on open as
any other configuration value.

The system may seem complicated when looking at all the knobs, but:

* Average users won't need to do any tuning, default configuration is good enough
* When they need to, because their repo is very particular or very large, they can
* It's not hard to implement (see algorithm below). At least not much harder
than any other mechanism. The difficulties lie in generating the split
manifests, and that is needed with any system. The extra work of classifying what arrays
go where is simple.

To understand how the feature works, we show an example configuration. Comments
in the yaml file explain what the different settings do.

```yaml
chunk-manifests:
  sets:                           # the lists of manifest sets and their properties
                                  # it's a list because order is important

    - coord1:                     # a name for the set, used later with rules

        max-manifest-size: 10000  # this set will contain manifests with <= 10k references

        arrays-per-manifest: null # pack this many arrays in each manifest
                                  # max-manifest-size and arrays-per-manifest are
                                  # mutually exclusive

        overflow-to: coord2       # if an array cannot fit in the set, send it to set coord2
                                  # if no overflow-to is indicated it goes to
                                  # the default set (see below)

        cardinality: 2            # this set cannot have more than two manifests
                                  # cardinality = null means unlimited

    - coord2:
        max-manifest-size: 10000
                                  # overflow-to: default       # by default
                                  # arrays-per-manifest: null  # by default
                                  # cardinality: 1             # by default

    - big-array:
        max-manifest-size: null
        arrays-per-manifest: 1
        cardinality: null

    - default:                  # default set must always exist, from defaults if needed
                                # max-size: 1_000_000 by default (we'll have to tune)
                                # default set is the only that can have members that
                                # go beyond the max-size
                                # (in case of large arrays that don't fit)
                                # default always has cardinality = null

  rules:                        # what arrays go to each manifest set, an ordered list

    # each rule has a target (the manifest set) and a set of conditions that are and-ed
    # rules are applied in the order declared, and they break on the first match

    - path: .*/(latitude|longitude|time)  # an optional regex matching on path

      metadata-chunks: [0, 500]           # arrays having number of chunks in this range
                                          # both extremes can be null
                                          # in the future we'll implement initialized-chunks
                                          # that will be useful for sparse arrays

      target: coord1                      # arrays that match will go to this manifest set
      

    - metadata-chunks: [0, 200]           # arrays < 200 chunks will go to coord2
                                          # but only if they didn't get assigned by the rule above
      target: coord2


    - metadata-chunks: [2000000, null]    # huge arrays have their special manifest set
      target: big-array


  preload:                      # what manifests to asynchronously preload on session start
    max-manifest-size: 50000    # don't preload manifests larger than this
    max-manifests: 10           # don't preload more than this many manifests
    arrays:                     # currently we support selecting by path only
                                # we will refine with more power if needed

      - path: .*/time           # will preload manifests for all arrays matching this regex
      - path: .*/latitude
      - path: .*/longitude
```

The full power of the configuration is there for users that really need it. But
usually the default configuration will be enough.

Here is the default configuration:

```yaml
chunk-manifests:  # of course, we'll have to tune all these numbers
  sets:
    - coordinates:                # we intend coordinate arrays to be assigned here
        max-manifest-size: 50000
        cardinality: 1
        max-arrays-per-manifest: null 
        overflow-to: default

    - default:
        max-manifest-size: 1000000
        cardinality: null
        max-arrays-per-manifest: null
        overflow-to: null

  rules:
    - metadata-chunks: [0, 5000]
      path: .*
      target: coordinates


  # should we actually do this by default or is no-preload a better default?
  preload:
    max-manifest-size: 50000
    max-manifests: 1
    arrays:
      - path: .*/time
      - path: .*/latitude
      - path: .*/longitude
```

### Algorithm

Notice there are two different methods for packing, one based on
`max-manifest-size` and the other on `arrays-per-manifest`. We may chose to
implement the second one at a later stage.

Configuration is always validated before saving it:
  * type checks
  * `target` exists
  * no `overflow-to` loops
  * one of `max-manifest-size` or `arrays-per-manifest`
  * default cannot have cardinality
  * etc.

#### Packing by `max-manifest-size`

We define the closure of a set of arrays. In plain English, the closure of a
set of arrays `A` is the set of all arrays that would need their manifest
rewritten if we update all arrays in `A`. We are going to re-pack all arrays
in the closure of the set of modified arrays.

Example:

Manifest 1 has arrays: a, b
Manifest 2 has arrays: a, b, c
Manifest 3 has arrays: c

* `closure({a}) = {a, b, c}`
* `closure({a, b}) = {a, b, c}`
* `closure({a, b, c}) = {a, b, c}`
* `closure({c}) = {a, b, c}`


<details>
<summary>Pseudo-code to compute the closure`</summary>

```python
def manifests(a: Array):
  # the set of manifests that have chunks from array a

def arrays(m: Manifest):
  # the set of arrays that have chunks in manifest m

def closure({}):
  return {}

def closure({a, b, more*}):
  union(closure(a), closure(b), *[closure(x) for x in more])

def closure({a}):
  # TODO: what happens with delete + create of an array**
  if a is an array created in this session:
    return {a}

  found = {}
  to_process = {a}
  manifests_seen = {}
  
  while x in to_process.peek():
    for manifest in manifests(x) if manifest not in manifests_seen:
      manifests_seen.add(manifest)
      for array in arrays(manifest) if array not in found:
        to_process.add(array)
    found.add(x)
    to_process.delete(x)

  return found
```
</details>


Manifest packing works roughly like this:

* At `flush` time we pack all the arrays in the closure of the modified arrays
* We try to put each array in a set of manifest according to the rules in config
but that's not always possible
* We use an heuristic bin-packing algorithm, to try to create the minimum number
of manifests in each manifest set, and filling them as much as possible
* Manifest sets are processed in sequence and in topological order of their
`overflow_to` fields, breaking ties with the configuration order. This means
that if an array is not packed in a manifest set, it will be tried in the one
that is overflowed to.
* Arrays that overflow all the way to `default` set, will always find a place.

At the end, we get a list of manifests, each with a list of arrays that must
go into it. The manifests that were not affected by the closure, can stay
unmodified, and linked from the new snapshot.

<details>
<summary>Pseudo-code to compute the new manifest contents</summary>

```python
def manifest_assignments(modified_arrays: set[array]) -> list[[array]]:
  cl = closure(modified_arrays)
  desired_assignments: dict[manifest_set, array] = {}
  result = []

  # we run the configured rules to get a desired assignment of arrays to manifest sets
  for array in cl:
    desired_manifest_set = apply_rules(array)
    desired_assignments[desired_manifest_set].push(array)

  # we sort the manifest sets, making sure overflow goes to a set
  # that has not been processed yet
  for manifest_set in sort_topologically(desired_assignments.keys()):

    assignable_arrays = []

    # skip the arrays that cannot fit in this manifest_set
    for array in desired_assignments[manifest_set]:
      if arrays.metadata_chunks > manifest_set.max_manifest_size:
        # if we are in the last manifest set, we need to create one manifest per array
        # because they are larger than the desired limit
        if manifest_set.overflow_to == None:
          # this is the default (last) manifest set
          result.push([array])
        else:
          # the array is too large for this manifest set, but it may fit 
          # in the overflow set
          desired_assignments[manifest_set.overflow_to].push(array)
      else:
        assignable_arrays.push(array)

    # assignable_arrays now contains the list of arrays that we
    # have a chance to assign in this manifest set

    # bin pack them using a heuristic algorithm
    packages = bin_pack(bin_size=manifest_set.max_manifest_size, packages=[Package(size=a.metadata_chunks, content=a) for a in assignable_arrays])

    # now we need to check we don't create more manifests than allowed by cardinality configuration
    (fitting, overflow) = packages.split_at(manifest_set.cardinality) if manifest_set.cardinality else (packages, [])
    result.extend([package.content for package in fitting])
    desired_assignments[manifest_set.overflow_to].extend([package.content for package in overflow])


  def apply_rules(a: array) -> manifest_set:
    # Rules are applied in order
    # First rule that matches returns the result
    ...

  def sort_topologically(ss: list[manifest_set]) -> list[manifest_set]:
    # sort topologically using `overflow_to` as the graph edges
    # break ties using the position in the configuration list
    # Note: there are trivial Rust libraries that implement this
    ...

  def bin_pack(bin_size: int, packages: Package) -> list[list[Package]]:
    # use heuristics to bin pack the packages into bins
    # returns the list of bins containing multiple packages each
    # no result item has length > bin_size
    # Note: there are trivial Rust libraries that implement this
```
</details>

#### Packing by `arrays-per-manifest`

Packing an `arrays-per-manifest` manifest set is easier. We try to create the
manifests with the given number of arrays, matching the biggest arrays with
the smallest ones, to try to achieve uniform sizes.

Last manifest can contain fewer than `arrays-per-manifest` arrays.

There may be better algorithms, but something like matching `arrays-per-manifest/2`
of the biggest arrays with `arrays-per-manifest/2` of the smallest ones may work.

We probably don't need to implement this mode initially.

### How `flush` changes

We need to start by running the algorithm above to define what new manifests to
create. For this we need to know all the modified arrays (easy), and their
metadata size (easy), together with the metadata for all other arrays in the
closure (not hard). If we want to implement `initialized-chunks` instead of
`metadata-chunks`, things are harder, because we need a way to keep a count
of chunks for each array. This is tricky because the current session can update
delete or add new chunks, so we would need to check what type of operation it is.

Then, we need to create all the new manifests, with the corresponding arrays. We
will need support to fetch chunks on a per node basis (there is some already).
This operation is parallelizable, on a per manifest basis, or possibly even
more, on a per node basis.

We need a way to search manifests for an array (we have it), and arrays for a
manifest (we don't have an efficient way, maybe it doesn't matter).

We need to keep track of what manifests can remain the same and just be linked
in the new snapshot. These are the ones not visited by the closure.

We need a topological sort: [this one](https://crates.io/crates/topological-sort/0.2.2)
has no dependencies.

We need a bin-pack: [this one](https://crates.io/crates/rpack/0.2.2/dependencies)
is fast at our scale and has no dependencies.

