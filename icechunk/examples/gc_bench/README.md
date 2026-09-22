# gc_bench

Benchmark tool for garbage collection, repo stats, and object listing.

- `build`, `gc`, `stats` work against a synthetic repository on local RustFS
  (`just contup` first). See `just gc-bench --help`.
- `list` measures listing throughput only. It is the only subcommand that
  accepts a real S3 target, because it is the only one that is strictly read
  only: it issues list requests and nothing else. It never reads, writes or
  deletes an object, and it never prints anything that identifies one.

## Running `list` on an EC2 instance in the bucket's region

```sh
# on the instance: install rustup (stable), clone the repo at this commit, then
cargo build --profile bench --features logs --example gc_bench

# temporary credentials, e.g. from `aws sts assume-role` or the console
export AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... AWS_SESSION_TOKEN=...

# the bench profile writes to target/release, not target/bench
B=./target/release/examples/gc_bench
$B list --s3-bucket <bucket> --s3-prefix <prefix> --s3-region <region> --mode single --duration-secs 30
$B list --s3-bucket <bucket> --s3-prefix <prefix> --s3-region <region> --mode asset-manager --streams 64 --duration-secs 30
$B list --s3-bucket <bucket> --s3-prefix <prefix> --s3-region <region> --mode asset-manager --streams 128 --duration-secs 30
```

`asset-manager` is the production path, `AssetManager::list_*_with_concurrency`,
the one GC uses. `single` is the store's per-stream baseline; the production
rate should scale with `--streams` until the cores parsing listing responses
are saturated.

Credentials are resolved in this order, and the run prints which source it used,
never the values:

1. `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` in the environment become
   static credentials, with `AWS_SESSION_TOKEN` if it is set. This is how
   temporary STS credentials are passed.
2. Otherwise the AWS SDK's default chain (instance role, shared config profile),
   reported as `credentials: FromEnv`.

## Modes

- `single`: one `list_objects` stream over `<kind>/`, driven by one task
  through `MeteringStorage`.
- `asset-manager`: `AssetManager::list_*_with_concurrency` with `--streams`
  listing tasks, one prefix each, off a shared queue. These tasks list on the
  raw storage, not through `MeteringStorage`: keys are counted in a task-local
  counter that is published to a shared atomic once per 1000 keys, and a ticker
  samples that atomic once a second for the timeline.

The report ends with `available cores` and the process CPU time, so a run that
is CPU bound on one core is visible as such.

`--kind chunks|manifests|snapshots|transaction_logs` picks the prefix
(`chunks` by default), `--duration-secs` bounds the run (30 by default) and
`--max-keys` stops it early. On the duration limit the streams are dropped,
abandoning in-flight requests.

The report gives the keys-per-second timeline, min/median/max/mean over the
seconds after the first (the first second includes request warm-up), total keys,
elapsed time and the approximate page count (`keys/1000`).

Against the local `large` dataset instead of S3:

```sh
just gc-bench list --name large --mode asset-manager --streams 64 --duration-secs 20
```
