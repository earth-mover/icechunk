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
pytest -nauto -m setup_benchmarks
```
As of Jan 20, 2025 this command takes about 3 minutes to run.

## Running benchmarks
Following `pytest` convention, benchmarks are in `benchmarks/test_*.py`.

- [ ] How do I customize the folder name?
- [ ] `--benchmark-autosave` is probably good, and then we write helper scripts around it?

Run the read benchmarks:
``` sh
pytest benchmarks/test_benchmark_reads.py
```

Here is an example run that runs a specific benchmark `test_write_chunks` and saves it to a specific file.
```sh
pytest --benchmark-save=write-chunks benchmarks/test_benchmark_writes.py::test_write_chunks
```

Compare `HEAD` to `main`

``` sh
git switch main
pytest --benchmark-autosave benchmarks/*.py

git switch PR-BRANCH-NAME
pytest --benchmark-autosave benchmarks/*.py



```


## Design decisions / future choices

1. We chose `pytest-benchmark` instead of `asv` because it seemed easier to learn --- all our pytest knowledge and idioms carry over (e.g. fixtures, `-k` to subselect benchmarks to run, `-s` to print stdout/sterr etc.). For example `pytest -nauto -m setup_benchmarks benchmarks/test_benchmark_reads.py` gives easy selection and parallelization of setup steps!

1. A downside relative to `asv` is that simply comparing numbers between the `main` branch and PR branch `HEAD` is not easy. For now, we can do this manually or write a helper script. In the worst case, it is not too hard to switch to `asv`.

1. In the future, it would be good to add Rust micro-benchmarks that test specific pieces like deserializing a manifest for example.