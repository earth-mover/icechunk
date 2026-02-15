# Benchmarks CLAUDE.md

## Overview

Integration benchmarks exercising the Xarray/Zarr/Icechunk stack end-to-end, built on `pytest-benchmark`.

## Quick Reference

```bash
just bench-build                  # uv sync + maturin develop --release
just bench-setup                  # create datasets (once, ~3 min)
just bench                        # run all benchmarks
just bench "-k getsize"           # run specific benchmarks
just bench-compare 0020 0021      # compare saved runs
```

All commands run from the repo root. Under the hood they `cd icechunk-python` and use `uv run`.

## File Map

| File | Purpose |
|---|---|
| `test_benchmark_reads.py` | Read benchmarks (store open, getsize, zarr/xarray open, chunk reads, first-byte) |
| `test_benchmark_writes.py` | Write benchmarks (task-based writes, 1D writes, chunk refs, virtual refs, split manifests) |
| `datasets.py` | Dataset definitions (`BenchmarkReadDataset`, `BenchmarkWriteDataset`, `IngestDataset`) and setup functions |
| `conftest.py` | Pytest fixtures, custom options (`--where`, `--icechunk-prefix`, `--force-setup`), markers |
| `runner.py` | Multi-version orchestration: clones repo, builds each ref, runs setup + benchmarks, compares |
| `tasks.py` | Low-level concurrent write tasks using `ForkSession` with thread/process pool executors |
| `helpers.py` | Utilities: logger, coiled kwargs, git commit resolution, `repo_config_with()`, splitting config |
| `lib.py` | Math/timing: `stats()`, `slices_from_chunks()`, `normalize_chunks()`, `Timer` context manager |
| `create_era5.py` | ERA5 dataset creation using Coiled + Dask (separate from `setup_benchmarks` due to cost) |

## Architecture

### Datasets (`datasets.py`)

`StorageConfig` wraps bucket/prefix/region and constructs `ic.Storage` objects. `Dataset` wraps a `StorageConfig` and provides `create()` (with optional `clear`) and a `.store` property.

Two specialized subclasses:
- **`BenchmarkReadDataset`** — adds `load_variables`, `chunk_selector`, `full_load_selector`, `first_byte_variable`, `setupfn`
- **`BenchmarkWriteDataset`** — adds `num_arrays`, `shape`, `chunks`

Predefined datasets:

| Name | Type | Description |
|---|---|---|
| `ERA5_SINGLE` | Read | Single NCAR ERA5 netCDF (~17k chunks) |
| `ERA5_ARCO` | Read | ARCO-ERA5 from GCP (metadata only, no data arrays written) |
| `GB_8MB_CHUNKS` | Read | 512^3 int64 array, 4x512x512 chunks |
| `GB_128MB_CHUNKS` | Read | 512^3 int64 array, 64x512x512 chunks |
| `LARGE_MANIFEST_UNSHARDED` | Read | 500M x 1000 array, no manifest splitting |
| `LARGE_MANIFEST_SHARDED` | Read | 500M x 1000 array, split_size=100k |
| `PANCAKE_WRITES` | Write | 320x720x1441, chunks=(1,-1,-1) |
| `SIMPLE_1D` | Write | 2M elements, chunks=1000 |
| `LARGE_1D` | Write | 500M elements, chunks=1000 |

### Storage Targets

Controlled by `--where` flag. Buckets defined in `TEST_BUCKETS` dict:

| Store | Bucket | Region |
|---|---|---|
| `local` | platformdirs cache | - |
| `s3` | `icechunk-ci` | us-east-1 |
| `s3_ob` | (same as s3, uses `s3_object_store_storage`) | us-east-1 |
| `gcs` | `icechunk-test-gcp` | us-east1 |
| `tigris` | `icechunk-test` | iad |
| `r2` | `icechunk-test-r2` | us-east-1 |

### Pytest Markers

- `@pytest.mark.setup_benchmarks` — dataset creation (run with `-m setup_benchmarks`)
- `@pytest.mark.read_benchmark` — all read tests
- `@pytest.mark.write_benchmark` — all write tests

### Fixtures (`conftest.py`)

- `synth_dataset` — parameterized over read datasets (currently large-manifest-no-split, large-manifest-split)
- `synth_write_dataset` — PANCAKE_WRITES
- `simple_write_dataset` — SIMPLE_1D
- `large_write_dataset` — LARGE_1D
- `repo` — local tmpdir repo with virtual chunk container configured

`request_to_dataset()` applies `--where` and `--icechunk-prefix` to any dataset fixture.

### Runner (`runner.py`)

Multi-version benchmarking orchestrator. Two runner classes:

- **`LocalRunner`** — clones repo to `/tmp/icechunk-bench-{commit}`, runs `uv sync --group benchmark`, `maturin develop --release`, copies benchmarks/ from CWD, executes via `uv run`
- **`CoiledRunner`** — creates Coiled software environments, runs on cloud VMs (m5.4xlarge / n2-standard-16), syncs results via S3

Usage: `python benchmarks/runner.py [--where local|s3|gcs] [--setup force|skip] [--pytest "-k pattern"] ref1 ref2 ...`

Datasets written to `{bucket}/benchmarks/{ref}_{shortcommit}/`.

### Task-Based Writes (`tasks.py`)

Uses `ForkSession` for concurrent writes:
1. Create tasks with `ForkSession` + region slices
2. Submit to thread/process pool
3. Each worker writes a chunk region via zarr
4. Merge all `ForkSession`s back into parent session
5. Commit

## Read Benchmarks (`test_benchmark_reads.py`)

| Test | What It Measures |
|---|---|
| `test_time_create_store` | Repository.open + readonly_session + store creation |
| `test_time_getsize_key` | `store.getsize(key)` for zarr.json metadata keys |
| `test_time_getsize_prefix` | `array.nbytes_stored()` (prefix-based size aggregation) |
| `test_time_zarr_open` | Cold `zarr.open_group` (re-downloads snapshot each round) |
| `test_time_zarr_members` | `group.members()` enumeration |
| `test_time_xarray_open` | `xr.open_zarr` with `chunks=None, consolidated=False` |
| `test_time_xarray_read_chunks_cold_cache` | Full open + isel + compute (parameterized: single-chunk vs full-read, with preload fixture) |
| `test_time_xarray_read_chunks_hot_cache` | Repeated compute on pre-opened dataset (measures chunk fetch, not metadata) |
| `test_time_first_bytes` | Open group + read coordinate array (sensitive to manifest splitting) |

The `preload` fixture parameterizes `ManifestPreloadConfig` (default vs off).

## Write Benchmarks (`test_benchmark_writes.py`)

| Test | What It Measures |
|---|---|
| `test_write_chunks_with_tasks` | Concurrent task-based writes (ThreadPool/ProcessPool), captures per-task timings |
| `test_write_simple_1d` | Simple array write + commit cycle (good for comparing S3 vs GCS latency) |
| `test_write_many_chunk_refs` | Writing 10k chunk refs, parameterized: inlined vs not, committed vs not |
| `test_set_many_virtual_chunk_refs` | Setting 100k virtual chunk refs via `store.set_virtual_ref()` |
| `test_write_split_manifest_refs_full_rewrite` | Commit time for 500k virtual refs (full rewrite), parameterized by splitting |
| `test_write_split_manifest_refs_append` | Commit time for incremental appends of virtual refs, 10 rounds |

The `splitting` fixture parameterizes `ManifestSplittingConfig` (None vs split_size=10000).

## Key Patterns

- **Benchmark results** saved to `.benchmarks/` as JSON via `--benchmark-autosave` or `--benchmark-save=NAME`
- **Comparing runs**: `pytest-benchmark compare 0020 0021 --group=func,param --columns=median --name=short`
- **Cold vs hot cache**: cold benchmarks re-create store/repo inside the benchmarked function; hot benchmarks create once outside
- **`pedantic` mode**: used in task writes and split-manifest tests for finer control (`setup=` callback, explicit `rounds`/`iterations`)
- **`benchmark.extra_info`**: task-based writes record per-task timing statistics in the JSON output
- **zarr async concurrency**: set to 64 globally in read benchmarks, configurable in writes
- **Version compatibility**: `conftest.py` uses `pytest_configure` hook instead of `pyproject.toml` markers to support older icechunk versions
