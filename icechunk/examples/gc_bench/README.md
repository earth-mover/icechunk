# gc_bench

Benchmark tool for garbage collection and repo stats.

`build`, `gc` and `stats` all work against a synthetic repository on local
RustFS, so start the container stack first (`just contup`).

- `build` writes the repository: pick a size with `--preset
  small|medium|large|huge`, or override any individual dataset parameter.
- `gc` runs garbage collection against it. It is a dry run unless `--delete` is
  passed.
- `stats` runs `repo_chunks_storage` against it.

`gc` and `stats` can route their traffic through toxiproxy with `--latency-ms`
and `--bandwidth-kbps`, to see how the operation behaves on a slow link.

Each run prints per-phase wall times, per-operation request and byte counts, a
per-second read timeline, peak RSS and CPU time.

```sh
just contup
just gc-bench build --name smoke --preset small --force
just gc-bench gc --name smoke
just gc-bench stats --name smoke
just gc-bench gc --name smoke --delete
```

See `just gc-bench --help` for the full flag list.
