# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Icechunk is a transactional storage engine for Zarr (tensor/ND-array) data on cloud object storage. It provides serializable transaction isolation, time travel, version control (branches/tags), chunk sharding and references, and schema evolution. The core is written in Rust with Python bindings via PyO3/maturin.

## Build & Development Commands

Uses `just` (justfile) as the primary build tool and `cargo nextest` for tests.

```bash
just test                       # run all tests (cargo nextest, dev profile)
just test -- -E 'test(name)'    # run a single test by name (nextest filter expression)
just profile=ci test            # run tests with CI profile (faster compile, no optimizations)
just doctest                    # run doc tests
just test-logs info             # run tests with logging at given level
just build                      # debug build
just build-release              # release build (LTO, single codegen unit)
just lint                       # clippy with --all-features
just format                     # rustfmt
just check-deps                 # cargo deny
just pre-commit                 # medium checks: compile, build, format, lint, deps (~2-3 min)
just pre-commit-ci              # full CI: all of the above + tests + doctests + examples
```

Python (icechunk-python/):
```bash
maturin develop                 # build and install Python extension in dev mode
just pre-commit-python          # format + lint Python code
pytest                          # run Python tests (asyncio_mode=auto, xdist parallel)
ruff check .                    # Python linting (line-length 90)
ruff format .                   # Python formatting
mypy                            # type checking (strict mode, Python 3.11)
```

Local testing services via `docker compose up`: minio (S3 on :9000), azurite (Azure Blob on :10000), toxiproxy (:8474/:9002).

## Architecture

```
Repository ─creates─► Session
                          ├── ChangeSet (uncommitted modifications)
                          └── AssetManager (typed I/O with caching)
                                  └── Storage (S3, GCS, Azure, local, in-memory)
```

**Repository** (`icechunk/src/repository.rs`): Entry point. Holds shared resources (storage, config). Creates Sessions and manages branches, tags, and snapshot history. Access versions via `VersionInfo` (snapshot ID, branch tip, tag, or timestamp).

**Session** (`icechunk/src/session.rs`): Transaction context. Three modes: `Readonly`, `Writable`, `Rearrange`. Tracks a base snapshot, accumulates changes in a `ChangeSet`, handles commits with conflict detection and rebase.

**Store** (`icechunk/src/store.rs`): Zarr-compatible key-value interface backed by a Session. Translates Zarr string keys (e.g. `"array/c/0/0"`) into typed Icechunk operations. This is the interface Zarr libraries consume.

**Storage** (`icechunk/src/storage/`): `Storage` trait abstracts object store operations (get/put/delete/list). Implementations: native S3 (`s3/`), object_store-based (local FS, in-memory, Azure, GCS), HTTP redirect.

**Format** (`icechunk/src/format/`): Binary serialization layer. Snapshots, manifests, transaction logs, and chunk references. Uses FlatBuffers (`flatbuffers/`) and MessagePack. The `format_constants` module tracks spec versions.

**Config** (`icechunk/src/config.rs`): `RepositoryConfig`, `ObjectStoreConfig` (storage backend enum), credential types, caching, compression, and manifest splitting configuration.

**Conflicts** (`icechunk/src/conflicts/`): Conflict detection and resolution for concurrent commits on the same branch.

## Workspace Structure

- `icechunk/` — core Rust library (also has a `cli` feature for binary)
- `icechunk-python/` — PyO3 bindings; `src/` has Rust glue, `python/icechunk/` has pure Python wrappers
- `icechunk-macros/` — proc-macro crate (support macros)

Default workspace member is `icechunk` only. Use `--workspace` or `-p icechunk-python` explicitly.

## Key Conventions

- **Rust edition 2024**, toolchain 1.91.0, max line width 90 (`rustfmt.toml`)
- **Clippy lints**: `expect_used`, `unwrap_used`, `panic`, `todo`, `unimplemented`, `dbg_macro` are all `warn` — avoid these in production code
- **Python**: ≥3.11, ruff for lint/format (line-length 90), mypy strict
- **Error types**: Each module has its own `ErrorKind` enum wrapped in an `ICError` for context. Pattern: `SessionErrorKind`, `RepositoryErrorKind`, `StoreErrorKind`, `StorageErrorKind`
- **Async everywhere**: Core operations are async (tokio). Python bindings use `pyo3-async-runtimes` for bridging
- The Python module is `icechunk._icechunk_python` (cdylib), wrapped by pure Python in `python/icechunk/`

## Testing

- Rust: `cargo nextest` (parallel), proptest for property-based tests
- Python: pytest with pytest-asyncio (auto mode), pytest-xdist for parallel
- Integration tests require docker compose services for S3/Azure emulation
- CI runs on Linux (x86 + ARM), macOS (Intel + Apple Silicon); Windows has a separate check workflow
