# Icechunk Command Line Interface

## Functionality

## Arguments

## Configuration

## Python packaging
Maturin docs [suggest wrapping the CLI in a Python entrypoint](https://www.maturin.rs/bindings#both-binary-and-library). To make this work, I
- implemented the CLI in `icechunk/src/cli.rs` and exposed it through the library
  - exposed it in `icechunk/src/bin/icechunk/main.rs` to still get a rust binary
- wrote an entrypoint function in `icechunk-python` and exposed it in `pyproject.toml` as:
```ini
[project.scripts]
icechunk = "icechunk._icechunk_python:cli_entrypoint"
```

The disadvantage is that Python users need to call Python, resulting in ~200ms latency.

## Implementation

- clap
- anyhow