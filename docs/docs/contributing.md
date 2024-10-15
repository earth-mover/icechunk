---
title: Contributing
---
# Contributing

👋 Hi! Thanks for your interest in contributing to Icechunk!

Icechunk is an open source (Apache 2.0) project and welcomes contributions in the form of:

- Usage questions - [open a GitHub issue](https://github.com/earth-mover/icechunk/issues)
- Bug reports - [open a GitHub issue](https://github.com/earth-mover/icechunk/issues)
- Feature requests - [open a GitHub issue](https://github.com/earth-mover/icechunk/issues)
- Documentation improvements - [open a GitHub pull request](https://github.com/earth-mover/icechunk/pulls)
- Bug fixes and enhancements - [open a GitHub pull request](https://github.com/earth-mover/icechunk/pulls)


## Development

### Python Development Workflow

Create / activate a virtual environment:

=== "Venv"

    ```bash
    python3 -m venv .venv
    source .venv/bin/activate
    ```

=== "Conda / Mamba"

    ```bash
    mamba create -n icechunk python=3.12 rust zarr
    mamba activate icechunk
    ```

Install `maturin`:

```bash
pip install maturin
```

Build the project in dev mode:

```bash
maturin develop
```

or build the project in editable mode:

```bash
pip install -e icechunk@.
```


### Rust Development Workflow

TODO

## Roadmap

The initial release of Icechunk is just the beginning. We have a lot more planned for the format and the API. 

### Core format

The core format is where we’ve put most of our effort to date and we plan to continue work in this area. Leading up to the 1.0 release, we will be focused on stabilizing data structures for snapshots, chunk manifests, attribute files and references. We’ll also document and add more mechanisms for on-disk format evolution. The intention is to guarantee that any new version of Icechunk can always read repositories generated with any previous versions. We expect to evolve the [spec](https://icechunk.io/spec/) and the Rust implementation as we stabilize things.

### Optimizations

While the initial performance benchmarks of Icechunk are very encouraging, we know that we have only scratched the surface of what is possible. We are looking forward to investing in a number of optimizations that will really make Icechunk fly!

- Chunk compaction on write
- Request batching and splitting
- Manifest compression and serialization improvements
- Manifest split heuristics
- Bringing parts of the codec pipeline to the Rust side
- Better caching, in memory and optionally on local disk
- Performance statistics, tests, baseline and evolution

### Other Utilities

On top of the foundation of the Icechunk format, we are looking to build a suite of other utilities that operate on data stored in Icechunk. Some examples:

- Garbage collection - version controlled data has the potential to accumulate data that is no longer needed but is still included in the store. A garbage collection process will allow users to safely cleanup data from old versions of an Icechunk dataset.
- Chunk compaction - data written by Zarr may result in many small chunks in object storage. A chunk compaction service will allow users to retroactively compact small chunks into larger objects (similar to Zarr’s sharding format), resulting in potential performance improvements and fewer objects in storage.
- Manifest optimization - knowing how the data is queried would allow to optimize the shape and splits of the chunk manifests, in such a way as to minimize the amount of data needed to execute the most frequent queries.

### Zarr-related

We’re very excited about a number of extensions to Zarr that would work great with Icechunk. 

- [Variable length chunks](https://zarr.dev/zeps/draft/ZEP0003.html)
- [Chunk-level statistics](https://zarr.dev/zeps/draft/ZEP0005.html)

### Miscellaneous

There’s much more than what we’ve written above on the roadmap. Some examples:

- Distributed write support with `dask.array`
- Multi-language support (R, Julia, …)
- Exposing high level API (groups and arrays) to Python users
- Make more details of the format accessible through configuration
- Improve Xarray backend to integrate more directly with Icechunk
