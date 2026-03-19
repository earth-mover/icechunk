# Rust benchmarks

The benchmarks are written using `criterion.rs` as harness.
1. Run all benchmarks with `cargo bench`
2. Run the specific `commit_split_manifests` group of benchmarks with `cargo bench -- "commit_split_manifests"`. The name is set by statements like `c.benchmark_group("commit_split_manifests");`
3. Run those benchmarks only for inline chunks: `cargo bench -- "commit_split_manifests/inline"`
4. Run those benchmarks only for inline chunks and specifically 1000 manifests: `cargo bench -- "commit_split_manifests/virtual/1000$"`
5. Examine logs for a particular benchmark: `ICECHUNK_LOG=icechunk=trace cargo bench --features logs -- "commit_rebase_split_manifests/inline" --test --nocapture`.
6. To compare `main` vs `HEAD` do it manually using "baselines":
    ``` sh
    git switch support/v1.x \
    && cargo bench -- --save-baseline v1  \
    && git switch optimize-manifest-writes \
    && cargo bench -- --baseline v1
    ```

Settings for the default `bench` profile have been edited to include some, but not all, optimizations for faster compiles and `debuginfo` for profiling.

Upon completion, you can find HTML output in `target/criterion/`.

## Environment variables

- `ICECHUNK_BENCH_LATENCY_MS=<ms>` — Run benchmarks against S3 (MinIO) behind toxiproxy instead of in-memory storage. The value sets downstream latency in ms (e.g. `100`). The latency toxic is applied only during the timed `get_chunk` iterations, not during setup. Requires `docker compose up -d` to start MinIO and toxiproxy. Example:
  ```sh
  ICECHUNK_BENCH_LATENCY_MS=100 cargo bench -- get_chunks
  ```

# Profiling

## Concepts

In general, it's possible to profile any example, benchmark, or test.
1. Compile the required executable with `--profile bench` (or `release`)
2. Run that executable with your chosen profiler and the appropriate command line args.

The drawback is that its annoying and requires figuring the name of the compiled executable, and appropriate command line flags to actually execute the code.

For example, to profile memory allocations on macOS one would type out something like
``` sh
xcrun xctrace record \
    --template 'Allocations' \
    --output test_large_manifests_alloc2.trace \
    --launch -- \
    target/perf/deps/test_large_manifests-f2bc61fc2535fa95 \  # compiled executable
    test_write_large_number_of_refs  # command-line arg to specify which benchmark to run
```

An alternative is to find useful cargo subcommands that make our life easy.

## [`cargo-samply`](https://docs.rs/cargo-samply/latest/cargo_samply/)

samply is an extremely good cross-platform profiler. I have found it quite useful, though reading the traces takes some effort given our heavy use of iterators.

``` sh
just samply "commit_rebase_split_manifests/type/inline"
```

will run that benchmark once and open up a profile in the Firefox Profiler.

The `just samply` command automatically wires up `tracing-samply` so all
`#[instrument]` annotations in the codebase appear as profiler markers in the
samply UI.

## [`cargo-instruments`](https://github.com/cmyr/cargo-instruments)

(macOS only).

This handy subcommand makes it easy to generate profile for macOS' Instruments app.
Example:

``` sh
cargo instruments -t Allocations --profile bench --example large_manifests
```

``` sh
cargo instruments -t Allocations --profile bench -- "commit/virtual/1000$"
```

 Sadly this one doesn't allow profiling tests directly; you'll have to do something manual like
``` sh
$ cargo test --package icechunk --test test_large_manifests --profile bench --no-run

$ xcrun xctrace record \
    --template 'Allocations' \
    --output test_large_manifests_alloc2.trace \
    --launch -- \
    target/perf/deps/test_large_manifests-f2bc61fc2535fa95 \  # compiled executable
    test_write_large_number_of_refs  # command-line arg to specify which benchmark to run
```

Or... just convert the test to an example.
