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
