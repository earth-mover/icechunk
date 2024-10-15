# Contributing

👋 Hi! Thanks for your interest in contributing to Icechunk!

Icechunk is an open source (Apache 2.0) project and welcomes contributions in the form of:

- Usage questions - [open a GitHub issue](https://github.com/earth-mover/icechunk/issues)
- Bug reports - [open a GitHub issue](https://github.com/earth-mover/icechunk/issues)
- Feature requests - [open a GitHub issue](https://github.com/earth-mover/icechunk/issues)
- Documentation improvements - [open a GitHub pull request](https://github.com/earth-mover/icechunk/pulls)
- Bug fixes and enhancements - [open a GitHub pull request](https://github.com/earth-mover/icechunk/pulls)

## Python Development Workflow

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


## Rust Development Worflow

TODO
