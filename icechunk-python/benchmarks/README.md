# Icechunk Benchmarks

This is a benchmark suite based on `pytest-benchmark`.
It is best to think of these benchmarks as benchmarking "integration" workflows that exercise the ecosystem from the Xarray/Zarr level down to Icechunk.

## Running benchmarks
Following `pytest` convention, benchmarks are in `benchmarks/test_*.py`.


Here is an example run that runs a specific benchmark `test_write_chunks` and saves it to a specific file.
```
pytest --benchmark-save=write-chunks benchmarks/test_benchmark_writes.py::test_write_chunks
```

## Choice of benchmark workflows


## Design decisions / future choices

1. We chose `pytest-benchmark` instead of `asv` because it seemed easier to learn --- all our pytest knowledge and idioms carry over (e.g. fixtures, `-k` to subselect benchmarks to run, `-s` to print stdout/sterr etc.).

1. A downside relative to `asv` is that simply comparing numbers between the `main` branch and PR branch `HEAD` is not easy. For now, we can do this manually or write a helper script. In the worst case, it is not too hard to switch to `asv`.

1. In the future, it would be good to add Rust micro-benchmarks that test specific pieces like deserializing a manifest for example.
