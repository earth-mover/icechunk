# Rust benchmarks

The benchmarks are written using `criterion.rs` as harness.
1. Run all benchmarks in `benches/manifest.rs` with `cargo bench --bench manifest`
2. Run the specific `commit_split_manifests` group of benchmarks with `cargo bench --bench manifest -- "commit_split_manifests"`. The name is set by statements like `c.benchmark_group("commit_split_manifests");`
3. Run those benchmarks only for inline chunks: `cargo bench --bench manifest -- "commit_split_manifests/inline"`
4. Run those benchmarks only for inline chunks and specifically 1000 manifests: `cargo bench --bench manifest -- "commit_split_manifests/virtual/1000$"`
5. Examine logs for a particular benchmark: `ICECHUNK_LOG=icechunk=trace cargo bench --features logs --bench manifest -- "commit_rebase_split_manifests/inline" --test --nocapture`. YOu will need to add the `initialize_tracing(None)` line at the beginning of the benchmark function.

Settings for the default `bench` profile have been edited to include some, but not all, optimizations for faster compiles and `debuginfo` for profiling.

Upon completion, you can find HTML output in `target/criterion/`.

# Profiling

## Concepts

In general, it's possible to profile any example, benchmark, or test.
1. Compile the required executable with `--profile bench` (or `release`)
2. Run that executable with appropriate command line args


The drawback is that its annoying and requires figuring the name of the compiled executable, and appropriate command line flags to actually execute the code.

For example, on macOS to profile memory allocations one would type out something like
``` sh
xcrun xctrace record \                                                                                                                                                       130 ↵
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
cargo samply --test
```

``` sh
cargo samply --test
```

## [`cargo-instruments`](https://github.com/cmyr/cargo-instruments)

(macOS only).

This handy subcommand makes it easy to generate profile for macOS' Instruments app.
Example:

``` sh
cargo instruments -t Allocations --profile bench --example large_manifests
```

``` sh
cargo instruments-t Allocations --profile bench --bench manifest -- "commit/virtual/1000$"
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
