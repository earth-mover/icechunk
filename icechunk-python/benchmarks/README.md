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
Use `---icechunk-prefix` to add an extra prefix during both setup and running of benchmarks.

### ERA5

`benchmarks/create_era5.py` creates an ERA5 dataset.
As of now, this writes 4 arrays with 5 years of data so 5*365*24=43_800 chunks per array for ~200k chunks.
It is separate from the default `setup_benchmarks` infrastructure because it is a relatively big ingest, and requires an account with Coiled (for now).
Run this in an environment with the icechunk version you want.
It records the dask performance report to `reports/` though it would be nice to instrument this more.
This takes about 5 minutes to run (depending on cloud scaling time).

## Running benchmarks
Following `pytest` convention, benchmarks are in `benchmarks/test_*.py`.

### TL;DR

Assuming no format changes between `main` and the PR branch, here's how I benchmarked the `getsize` improvement.

I added a new benchmarks: `test_time_getsize` and `test_time_getsize_prefix`

``` sh
# Start on the PR branch
git switch push-uknyqnpypzro
# this will build the main branch, and re-create the datasets
# And uses the pytest -k option to only run the new benchmarks
python benchmarks/runner.py --pytest "-k getsize" main
# It will print this to the screen
# > pytest [...] --benchmark-save=main_3abfa48a --icechunk-prefix=benchmarks/main_3abfa48a/  benchmarks/
# note the created prefix: main_(first-8-characters-of-commit), for convenienve export it
export PREFIX=benchmarks/main_3abfa48a/
pytest --benchmark-compare -k getsize --benchmark-group-by=group,func,param --benchmark-columns=median --benchmark-sort=name --icechunk-prefix=$PREFIX benchmarks/
```

This prints out
```
-------- benchmark 'test_time_getsize_key era5-single': 2 tests --------
Name (time in us)                                       Median
------------------------------------------------------------------------
test_time_getsize_key[era5-single] (0034_main_3a)     110.2501 (1.05)
test_time_getsize_key[era5-single] (NOW)              104.7080 (1.0)
------------------------------------------------------------------------

-------- benchmark 'test_time_getsize_key gb-128mb': 2 tests --------
Name (time in us)                                    Median
---------------------------------------------------------------------
test_time_getsize_key[gb-128mb] (0034_main_3a)     106.5000 (1.05)
test_time_getsize_key[gb-128mb] (NOW)              101.2499 (1.0)
---------------------------------------------------------------------

-------- benchmark 'test_time_getsize_key gb-8mb': 2 tests --------
Name (time in us)                                  Median
-------------------------------------------------------------------
test_time_getsize_key[gb-8mb] (0034_main_3a)     105.6660 (1.05)
test_time_getsize_key[gb-8mb] (NOW)              100.8749 (1.0)
-------------------------------------------------------------------

------- benchmark 'test_time_getsize_prefix era5-single': 2 tests --------
Name (time in ms)                                         Median
--------------------------------------------------------------------------
test_time_getsize_prefix[era5-single] (0034_main_3a)     68.8355 (31.10)
test_time_getsize_prefix[era5-single] (NOW)               2.2133 (1.0)
--------------------------------------------------------------------------
```

### Notes
### Where to run the benchmarks?

- Pass the `--where [local|s3|s3_ob|gcs|tigris]` flag to control where benchmarks are run.
- `s3_ob` uses the `s3_object_store_storage` constructor.
- Pass multiple stores with `--where 's3|gcs'`

```sh
python benchmarks/runner.py --where gcs v0.1.2
```

By default all benchmarks are run locally:
1. A temporary directory is used as a staging area.
2. A new virtual env is created there and the dev version is installed using `pip` and a github URI. *This means that you can only benchmark commits that have been pushed to Github.*

It is possible to run the benchmarks in the cloud using Coiled. You will need to be a member of the Coiled workspaces: `earthmover-devs` (AWS), `earthmover-devs-gcp` (GCS) and `earthmover-devs-azure` (Azure).
1. We create a new "coiled software environment" with a specific name.
2. We use `coiled run` targeting a specific machine type, with a specific software env.
4. The VM stays alive for 10 minutes to allow for quick iteration.
5. Coiled does not sync stdout until the pytest command is done, for some reason. See the logs on the Coiled platform for quick feedback.
6. We use the `--sync` flag, so you will need [`mutagen`](https://mutagen.io/documentation/synchronization/) installed on your system. This will sync the benchmark JSON outputs between the VM and your machine.
Downsides:
1. At the moment, we can only benchmark released versions of icechunk. We may need a more complicated Docker container strategy in the future to support dev branch benchmarks.
2. When a new env is created, the first run always fails :/. The second run works though, so just re-run.

### `runner.py`

`runner.py` abstracts the painful task of setting up envs with different versions (with potential format changes), and recreating datasets where needed.
Datasets are written to `s3://icechunk-test/benchmarks/REFNAME_SHORTCOMMIT`.

Usage:
``` sh
python benchmarks/runner.py icechunk-v0.1.0-alpha.12 main
```
This will
1. setup a virtual env with the icechunk version
2. compile it,
3. run `setup_benchmarks` with `force-setup=False`. This will recreate datasets if the version in the bucket cannot be opened by this icechunk version.
4. Runs the benchmarks.
5. Compares the benchmarks.

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
python benchmarks/runner.py --pytest="-k read_benchmark" --refs main
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

### Comparing across multiple stores

```sh
python benchmarks/runner.py --skip-setup --pytest '-k test_write_simple' --where 's3|s3_ob|gcs' main
```

``` sh
-------- benchmark 'test_write_simple_1d simple-1d': 3 tests --------
Name (time in s)                                     Median
---------------------------------------------------------------------
test_write_simple_1d[simple-1d] (g/gcs_main_95e)     5.2314 (3.15)
test_write_simple_1d[simple-1d] (g/s3_main_95ef)     1.6622 (1.0)
test_write_simple_1d[simple-1d] (g/s3_ob_main_9)     1.6909 (1.02)
---------------------------------------------------------------------
```

## Design decisions / future choices

1. We chose `pytest-benchmark` instead of `asv` because it seemed easier to learn --- all our pytest knowledge and idioms carry over (e.g. fixtures, `-k` to subselect benchmarks to run, `-s` to print stdout/sterr etc.). For example `pytest -nauto -m setup_benchmarks benchmarks` gives easy selection and parallelization of setup steps!

1. A downside relative to `asv` is that simply comparing numbers between the `main` branch and PR branch `HEAD` is not easy. For now, we can do this manually or write a helper script. In the worst case, it is not too hard to switch to `asv`.

1. In the future, it would be good to add Rust micro-benchmarks that test specific pieces like deserializing a manifest for example.
