# Installation

Icechunk is currently designed to support the [Zarr V3 Specification](https://zarr-specs.readthedocs.io/en/latest/v3/core/v3.0.html).
Using it today requires installing the [still unreleased] Zarr Python V3 branch.

To set up an Icechunk development environment, follow these steps

Activate your preferred virtual environment (here we use `virtualenv`):

```bash
python3 -m venv .venv
source .venv/bin/activate
```

Alternatively, create a conda environment

```bash
mamba create -n icechunk rust python=3.12
conda activate icechunk
```

Install `maturin`:

```bash
pip install maturin
```

Build the project in dev mode:

```bash
cd icechunk-python/
maturin develop
```

or build the project in editable mode:

```bash
cd icechunk-python/
pip install -e icechunk@.
```

```{warning}
This only makes the python source code editable, the rust will need to
be recompiled when it changes
```

