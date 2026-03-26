# Icechunk Python

Python library for Icechunk Zarr Stores

## Getting Started

### For Development

The recommended way to set up for development is with [uv](https://docs.astral.sh/uv/):

```bash
# Bring up local dev object stores used by certain tests, otherwise you will get some failures
docker compose up -d

# Install all development dependencies (includes test dependencies, mypy, ruff, maturin)
uv sync

# Configure maturin-import-hook for fast incremental Rust compilation
uv run -m maturin_import_hook site install

# Build the Rust extension
uv run maturin develop --uv

# Activate the virtual environment
source .venv/bin/activate

# Run tests
uv run pytest

# Run type checking
uv run mypy python tests

# Run linting
uv run ruff check python
```

The `maturin-import-hook` automatically rebuilds the Rust extension when changes are detected during import, so you don't need to manually re-run `maturin develop` after Rust code changes.

### Alternative: Manual Setup with pip

If not using uv:

```bash
python3 -m venv .venv
source .venv/bin/activate

# Install maturin
pip install maturin

# Install dev dependencies (includes test, mypy, ruff)
pip install --group dev

# Build the Rust extension
maturin develop
```

Note: Modern pip (>=21.3) supports dependency groups. If you have an older pip, upgrade with `pip install --upgrade pip`. When the Rust code changes, re-run `maturin develop` to rebuild.

**Note**: When using editable mode, only the Python source code is editable; the Rust code will need to be recompiled when it changes.

Now you can create or open an icechunk store for use with `zarr-python`:

```python
from icechunk import Repository, IcechunkStore, StorageConfig
from zarr import Array, Group

storage = StorageConfig.memory("test")
repo = Repository.open_or_create(storage=storage)

# create a session for writing to the store
session = repo.writable_session(branch="main")

root = Group.from_store(store=session.store(), zarr_format=zarr_format)
foo = root.create_array("foo", shape=(100,), chunks=(10,), dtype="i4")
```

You can then commit your changes to save progress or share with others:

```python
snapshot_id = session.commit("Create foo array")

async for parent in repo.ancestry(snapshot_id):
    print(parent.message)
```

See [`tests/test_timetravel.py`](tests/test_timetravel.py) for more example usage of the transactional features.
