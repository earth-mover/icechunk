# Icechunk Benchmarks

This is a benchmark suite based on `pytest-benchmark`.
It is best to think of these benchmarks as benchmarking "integration" workflows that exercise the ecosystem from the Xarray/Zarr level down to Icechunk.

## Setup

Install the necessary dependencies with the `[benchmarks]` extra.

The Datasets used for benchmarking are listed in `datasets.py` and can include both synthetic and "real-world" datasets.
These should be easy to extend.

Running the read benchmarks requires benchmark datasets be created.
These will need to be (re)created any time the `Dataset` changes, or the format changes.
Do so with
``` sh
pytest -nauto -m setup_benchmarks benchmarks/
```
As of Jan 20, 2025 this command takes about 3 minutes to run.

Use the `--force-setup` flag to avoid re-creating datasets if possible.

``` sh
pytest -nauto -m setup_benchmarks --force-setup=False benchmarks/
```

## Running benchmarks
Following `pytest` convention, benchmarks are in `benchmarks/test_*.py`.

### Just aliases

> [!WARNING]
> This doesn't work yet

Some useful `just` aliases:

| Compare these benchmark runs | `just bench-compare 0020 0021 0022` |

### Run the read benchmarks:
``` sh
pytest benchmarks/test_benchmark_reads.py
```

This simply runs the benchmarks, but does not print or save anything.

`--benchmark-autosave` will save timings to a JSON file in `.benchmarks` and print (too many) numbers to the screen.
```sh
pytest --benchmark-autosave benchmarks/test_benchmark_reads.py
```

### Save to a specific file

Here is an example run that runs a specific benchmark `test_write_chunks` and saves it to a specific file.
```sh
pytest --benchmark-save=write-chunks benchmarks/test_benchmark_writes.py::test_write_chunks
```

### Comparing runs:

``` sh
pytest --benchmark-compare benchmarks/*.py
```

This will automatically compare the run on `HEAD` against the most recently run benchmark.

#### Compare `HEAD` to `main`

We can use the above to somewhat quickly compare `HEAD` to `main`

``` sh
git switch main
maturin develop --release
pytest --benchmark-autosave benchmarks/*.py

git switch PR-BRANCH-NAME
maturin develop --release
pytest --benchmark-compare benchmarks/*.py
```

#### Comparing specific runs

The best I have found is to run with `--benchmark-autosave` or `--benchmark-save=SOME_NAME_YOU_LIKE`. This will persist benchmarks to
`.benchmarks`.

``` sh
pytest-benchmark list
```
which for me prints
```
...
/Users/deepak/repos/icechunk/icechunk-python/.benchmarks/Darwin-CPython-3.12-64bit/0019_icechunk-v0.1.0-alpha.12.json
/Users/deepak/repos/icechunk/icechunk-python/.benchmarks/Darwin-CPython-3.12-64bit/0020_icechunk-v0.1.0-alpha.8.json
/Users/deepak/repos/icechunk/icechunk-python/.benchmarks/Darwin-CPython-3.12-64bit/0021_icechunk-v0.1.0-alpha.10.json
```

Note the 4 digit ID of the runs you want. Then

``` sh
pytest-benchmark compare 0019 0020 0021 --group=func,param --sort=name --columns=median --name=short
```
Passing `--histogram=compare` will save a boatload of `compare-*.svg` files.

To easily run benchmarks for some named refs use `benchmarks/run_refs.py`

## Design decisions / future choices

1. We chose `pytest-benchmark` instead of `asv` because it seemed easier to learn --- all our pytest knowledge and idioms carry over (e.g. fixtures, `-k` to subselect benchmarks to run, `-s` to print stdout/sterr etc.). For example `pytest -nauto -m setup_benchmarks benchmarks` gives easy selection and parallelization of setup steps!

1. A downside relative to `asv` is that simply comparing numbers between the `main` branch and PR branch `HEAD` is not easy. For now, we can do this manually or write a helper script. In the worst case, it is not too hard to switch to `asv`.

1. In the future, it would be good to add Rust micro-benchmarks that test specific pieces like deserializing a manifest for example.
