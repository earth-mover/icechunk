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

=== "uv"

    ```bash
    uv sync
    ```

Install `maturin`:

=== "Venv"

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

=== "uv"

    uv manages rebuilding as needed, so it will run the Maturin build when using `uv run`.

    To explicitly use Maturin, install it globally.

    ```bash
    uv tool install maturin
    ```

    Maturin may need to know it should work with uv, so add `--uv` to the CLI.

    ```bash
    maturin develop --uv --extras=test,benchmark
    ```

#### Testing

The full Python test suite depends on S3 and Azure compatible object stores.

They can be run from the root of the repo with `docker compose up` (`ctrl-c` then `docker compose down` once done to clean up.).

=== "uv"

    ```bash
    uv run pytest
    ```

#### Running Xarray Backend Tests

Icechunk includes integration tests that verify compatibility with Xarray's zarr backend API. These tests require the Xarray repository to be cloned locally.

Set the environment variables (adjust `XARRAY_DIR` to point to your local Xarray clone):

```bash
export ICECHUNK_XARRAY_BACKENDS_TESTS=1
export XARRAY_DIR=~/Documents/dev/xarray  # or your xarray location
```

Run the Xarray backend tests:

```bash
python -m pytest -xvs tests/run_xarray_backends_tests.py \
  -c $XARRAY_DIR/pyproject.toml \
  -W ignore \
  --override-ini="addopts="
```

To run a specific Xarray test you have first specify a class defined in `@icechunk-python/tests/run_xarray_backends_tests.py` and then specify an xarray test. For example:

```bash
python -m pytest -xvs tests/run_xarray_backends_tests.py::TestIcechunkStoreFilesystem::test_pickle \
  -c $XARRAY_DIR/pyproject.toml \
  -W ignore \
  --override-ini="addopts="
```

#### Checking Xarray Documentation Consistency

Icechunk's `to_icechunk` function shares several parameters with Xarray's `to_zarr` function. To ensure documentation stays in sync, use the documentation checker script.

From the `icechunk-python` directory:

```bash
# Set XARRAY_DIR to point to your local Xarray clone
export XARRAY_DIR=~/Documents/dev/xarray

# Run the documentation consistency check
uv run scripts/check_xarray_docs_sync.py
```

The script will display a side-by-side comparison of any documentation differences, with missing text highlighted in red.

**Known Differences**: Some differences are acceptable (e.g., Sphinx formatting like `:py:func:` doesn't work in mkdocs). These are tracked in `scripts/known-xarray-doc-diffs.json`. Known differences are displayed but don't cause the check to fail.

**Updating Known Differences**: After making intentional documentation changes, update the known diffs file:

```bash
# Mark current diffs as known (creates/updates scripts/known-xarray-doc-diffs.json)
uv run scripts/check_xarray_docs_sync.py --update-known-diffs

# Edit scripts/known-xarray-doc-diffs.json to add reasons for each difference
```

**CI Integration**: The script returns exit code 0 if only known differences exist, allowing CI to pass while still displaying diffs for review.

### Rust Development Workflow

#### Prerequisites

Install the `just` command runner (used for build tasks and pre-commit hooks):

```bash
cargo install just
```

Or using other package managers:

- **macOS**: `brew install just`
- **Ubuntu**: `snap install --edge --classic just`

#### Building

Build the Rust workspace:

```bash
# Build all packages
just build

# Build release version
just build-release

# Compile tests without running them
just compile-tests
```

#### Testing

```bash
# Run all tests
just test

# Run tests with logs enabled
just test-logs debug

# Run only specific tests
cargo test test_name
```

#### Code Quality

We use a tiered pre-commit system for fast development:

```bash
# Fast checks (~3 seconds) - format and lint only
just pre-commit-fast

# Medium checks (~2-3 minutes) - includes compilation and deps
just pre-commit

# Full CI checks (~5+ minutes) - includes all tests and examples
just pre-commit-ci
```

Individual checks:

```bash
# Format code
just format

# Check formatting without changing files
just format --check

# Lint with clippy
just lint

# Check dependencies for security issues
just check-deps
```

#### Pre-commit Hooks

We use [pre-commit](https://pre-commit.com/) to automatically run checks. Install it:

```bash
pip install pre-commit
pre-commit install
```

The pre-commit configuration automatically runs:

- **Every commit**: Fast Python and Rust checks (~2 seconds total)
- **Before push**: Medium Rust checks (compilation + dependencies)
- **Manual**: Full CI-level checks when needed

To run manually:

```bash
# Run on changed files only
pre-commit run

# Run on all files
pre-commit run --all-files

# Run full CI checks manually
pre-commit run rust-pre-commit-ci --hook-stage manual
```

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
