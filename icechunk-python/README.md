# Icechunk Python

Python library for Icechunk Zarr Stores

## Getting Started

Activate the virtual environment:

```bash
python3 -m venv .venv
source .venv/bin/activate
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

**Note**: This only makes the python source code editable, the rust will need to be recompiled when it changes

Now you can create or open an icechunk store for use with `zarr-python`:

```python
from icechunk import IcechunkStore, StorageConfig
from zarr import Array, Group

storage = StorageConfig.memory("test")
store = IcechunkStore.open_or_create(storage=storage, mode='r+')

root = Group.from_store(store=store, zarr_format=zarr_format)
foo = root.create_array("foo", shape=(100,), chunks=(10,), dtype="i4")
```

You can then commit your changes to save progress or share with others:

```python
store.commit("Create foo array")

async for parent in store.ancestry():
    print(parent.message)
```

See [`tests/test_timetravel.py`](tests/test_timetravel.py) for more example usage of the transactional features.
