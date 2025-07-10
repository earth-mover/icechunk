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

The Python code is developed in the `icechunk-python` subdirectory. To make changes first enter that directory:

```bash
cd icechunk-python
```

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

# or with the optional dependencies
maturin develop --extras=test,benchmark
```

or build the project in editable mode:

```bash
pip install -e icechunk@.
```

### Rust Development Workflow

TODO

## Roadmap

### Features

- Support more object stores and more of their custom features
- Better Python API and helper functions
- Bindings to other languages: C, Wasm
- Better, faster, more secure distributed sessions
- Savepoints and persistent sessions
- Chunk and repo level statistics and metrics
- More powerful conflict detection and resolution
- Efficient move operation
- Telemetry
- Zarr-less usage from Python and other languages
- Better documentation and examples

### Performance

- Lower changeset memory footprint
- Optimize virtual dataset prefixes
- Bring back manifest joining for small arrays
- Improve performance of `ancestry`, `garbage_collect`, `get_size` and other metrics
- More flexible caching hierarchy
- Better I/O pipeline
- Better GIL management
- Request batching and splitting
- Bringing parts of the codec pipeline to the Rust side
- Chunk compaction

### Zarr-related

We’re very excited about a number of extensions to Zarr that would work great with Icechunk.

- [Variable length chunks](https://zarr.dev/zeps/draft/ZEP0003.html)
- [Chunk-level statistics](https://zarr.dev/zeps/draft/ZEP0005.html)
