# Governor A/B benchmarks

Compares this checkout of icechunk (the "branch") against the last released
wheel (the "baseline") to verify the IoGovernor work (design-docs/019) is
never slower than the code it replaces. Both versions are installed in one
venv — the released wheel is renamed to `icechunk_baseline` by
[third-wheel](https://pypi.org/project/third-wheel/) — so rounds interleave
A/B/A/B in a single process, cancelling environment drift.

## Arms

| arm | module | configuration |
|---|---|---|
| `baseline` | released wheel | defaults (256-permit semaphore) |
| `compat` | branch | `governor=None` → default compat governor (the drop-in path) |
| `bandwidth` | branch | `BandwidthGovernor`, S3 defaults, B=25 Gbps / M=4 GB |

Multi-repo scenarios (N repos driven concurrently) add:

| arm | configuration |
|---|---|
| `baseline-split` | N repos, each `max_concurrent_requests = 256/N` (capacity-fair analog of sharing) |
| `compat-shared` | N repos sharing one `CompatGovernor(256)` |
| `bandwidth-shared` | N repos sharing one `BandwidthGovernor` |

**Gates** (nonzero exit on failure): `compat` vs `baseline`, and
`compat-shared` vs `baseline-split`, with median-time ratio ≤ 1 +
`--tolerance` (default 5%). The `bandwidth*` arms are reported but warn-only
(`--gate-bandwidth` upgrades them).

Governor parameters: `--bandwidth-opt key=value` (repeatable; e.g.
`read.target_bandwidth=25Gbps`, `memory_budget=4GB`,
`read.request_latency_us=30000`), `--compat-permits N`, or whole custom arms
via `--arms-config` (see `arms.example.toml`). Unit suffixes: `Gbps`/`Mbps`
are bits, `GB`/`MiB` etc. are bytes.

## Scenarios and what each round actually does

Chunk profiles (synthetic float32, no compression): `small` 32 KiB × 65536
(2 GiB), `512k` 512 KiB × 16384 (8 GiB), `medium` 4 MiB × 512 (2 GiB), `big`
128 MiB × 24 (multipart writes, 3 GiB), `mixed` = the small+medium+big
arrays in one repo (512k excluded to keep mixed rounds short). The small and
512k datasets are deliberately large in request count: latency-bound reads
against them spend seconds in steady state, where a concurrency-gate gap is
measurable — but that also means `write-small` rounds issue 65536 PUTs each
(minutes per round on an object store), so filter to read scenarios unless
you mean it.

Every round opens the repo(s) fresh (cold caches, cold governor) and fans
out over `--threads` (default 8) worker threads on top of zarr
`async.concurrency` (`--zarr-concurrency`, default 64), so in-flight chunk
requests ≈ threads × concurrency — well past the 256-permit gate. The timed
region is everything a user would wait for: repository open, zarr opens,
the parallel streams, and any commit.

- **read** — the profile's array (written at setup) is split into
  contiguous chunk-aligned slabs, one per thread; each thread reads its
  slab with `arr[start:stop]`, so the whole array is fetched exactly once
  per round.
- **write** — each round creates a throwaway branch off `main`, creates a
  fresh array per profile on it, and each thread writes its slab (an
  uncompressed non-fill buffer, so chunks can't be elided as empty). The
  commit is timed; the branch is deleted untimed afterwards.
- **readwrite** — both at once on the same repo: half the streams read the
  existing `data-*` arrays while the other half write `w-*` arrays on a
  throwaway branch, so reads and writes contend for one governor.
- **mixed** (as a profile) — the repo holds all three arrays and the
  threads are spread across them, interleaving 32 KiB and 128 MiB requests
  through one governor.
- **multi-read / multi-readwrite** — N repos (`--num-repos`, default 2),
  each with a medium-profile dataset, driven concurrently with threads/N
  streams each. The arms differ in how the repos are governed: separate
  256/N-permit configs (`baseline-split`), one shared governor
  (`compat-shared`/`bandwidth-shared`), or per-repo defaults.
- **external reads** — `--where gcs` adds `gcs-read-external` against an
  existing repo we didn't write (`--gcs-repo`, default
  `earthmover-demos/era5-perf-gcp`); `--s3-repo` adds the same for S3. The
  harness walks the hierarchy, takes the largest arrays, and reads a
  chunk-aligned prefix of each's leading dimension until ~`--external-bytes`
  (default 1 GB, ÷8 with `--quick`) in total — a realistic-shape read
  instead of a synthetic one.
- **contend** — the noisy-neighbor demonstration, and the one scenario where
  the bandwidth governor should be *much better* than anything baseline can
  do. A victim (unmodified baseline module, `read-medium`) is measured while
  an aggressor continuously re-reads the `big` dataset in the same process
  (branch module, chunk cache off, long-lived warm governor). Sequential
  blocks instead of interleaving: `solo` (reference), `agg-ungoverned`
  (today's world — expect the victim to lose ~2× on a saturated NIC),
  `agg-governed` (aggressor capped at `--aggressor-bandwidth`, default
  4 Gbps — expect the victim back within a few % of solo), and
  `knob-before`/`knob-after` (aggressor starts effectively uncapped, then
  its `read_bandwidth` is cut live between blocks — the runtime-knob demo;
  the flip is visible in the per-round aggressor metrics in the JSON).
  Ratios are reported vs `solo`; nothing is gated. `--arms` does not apply.
  The governed aggressor's config raises `min_connection_bandwidth` to
  90 MB/s: with the default 7.5 MB/s cold floor the cap leaks ~2× while the
  bandwidth estimate warms (enforcement accuracy tracks the estimate).
  Note: `local-contend` exercises the machinery but contends on page
  cache/CPU, which a bandwidth cap doesn't govern — only the S3 variant is
  a meaningful demonstration.

Backends only change where the bytes live: `local` (tmpdir; no network
latency, so pure per-operation overhead is most visible), `s3` (Arraylake
repos in us-east-1), `gcs` (read-only, cross-cloud by design).

## Setup

```sh
# 1. branch build — MUST be a release build, dev-profile ratios are garbage
just profile=release develop

# 2. released wheel as icechunk_baseline (bump the version on new releases)
just io-governor-ab-baseline

# 3. harness extras (arraylake, rich) — deliberately NOT in the `benchmark`
#    group: that group is a pixi feature and would invalidate pixi.lock
uv sync --group io-governor-ab --no-install-project   # or: uv pip install arraylake

# 4. Arraylake auth for --where s3/gcs
export ARRAYLAKE_TOKEN=...                          # or `arraylake auth login`

# 5. create + populate the S3 repos
just io-governor-ab setup --org <org> --bucket-nickname <bucket-nickname>
# optional: a second fleet on a different store (Tigris/R2) to explore other
# latency/rate profiles; setup prints the bucket's platform+region — the
# latency between the benchmark machine and the bucket is part of every number
just io-governor-ab setup --org <org> --bucket-nickname <other-bucket> --repo-prefix ic-gov-tigris
```

Repeat steps 1–2 after any `uv sync`: it reinstalls the PyPI icechunk over
the maturin build and removes the renamed baseline.

## Live knob demo

```sh
just io-governor-ab knob --org <org> --regime 512k
```

Continuous governed reads of one dataset (`--regime` picks the chunk size)
with a live display: achieved MB/s (5 s / 30 s / lifetime) next to the
governor's target, effective bandwidth, in-flight cost, and queue depth.
Keys while running: `+`/`-` step the read target by 1 Gbps, `b`/`m` type an
absolute bandwidth or memory budget, `q` quits. Physics to expect: raising
a knob admits queued work immediately; lowering one never cancels in-flight
requests, so achieved lags the new target by the drain time (seconds for
big chunks). A persistent target↔achieved gap means the cost model's
`request_latency` / connection constants don't match the store at this
operating point — that's the enforcement accuracy the config controls, not
a bug in the display. The demo never writes remote datasets; a stale fleet
is an error pointing at `setup`.

## Running

```sh
just io-governor-ab run --org <org> --where local,s3,gcs   # full matrix, ~5 rounds each
just io-governor-ab run --org <org> --where s3 --repo-prefix ic-gov-tigris  # Tigris fleet
just io-governor-ab run --quick --scenarios local-read-medium   # fast smoke, no org needed
just io-governor-ab run --org <org> --where s3 --scenarios s3-contend --rounds 5  # noisy neighbor
just io-governor-ab report io-governor-ab-<stamp>.json    # re-render saved results
just io-governor-ab teardown --org <org> --yes            # delete the S3 repos
```

Output: interleaved per-round lines, a per-scenario table (median, IQR,
MB/s, ratio vs baseline), a gate table, and a JSON artifact with every
round, stream timings, governor metrics snapshots, and versions/paths of
both modules. Exit code 1 iff a gated comparison regresses.

## The real run (EC2)

- us-east-1, e.g. `c5n.9xlarge` (guaranteed 50 Gbps, 96 GB RAM) or
  `m6in.4xlarge`; ≥200 GB gp3 for the local-FS scenarios (write rounds leave
  orphaned chunks under `--local-dir` until you delete it — local-FS results
  measure the EBS volume as much as the code).
- Bootstrap: install rustup + uv + just, clone the branch, then the Setup
  steps above (`uv sync --group dev --group io-governor-ab --no-install-project`
  first so maturin exists).
- Sanity-check saturation before trusting ratios: on the small/medium read
  scenarios the `baseline` arm should get within shouting distance of the
  instance NIC (or CPU-bound). If throughput is far below both, raise
  `--threads` / `--zarr-concurrency` — a workload that never queues can't
  regress.
- S3/GCS medians are noisy; the response to a flaky gate is more `--rounds`
  (not more tolerance). GCS reads cross clouds by design ("GCS at least for
  reading").

## Layout

`impls.py` modules + arms · `scenarios.py` profiles + matrix ·
`workloads.py` load drivers · `arraylake_bridge.py` repo resolution +
per-module storage construction (the arraylake client is hard-bound to the
module named `icechunk`, so the harness resolves bucket/prefix/credentials
via the metastore and builds each module's Storage itself) · `runner.py`
orchestration + backends · `stats.py` gate math · `report.py` tables + JSON
· `__main__.py` CLI. `python -m benchmarks.io_governor_ab selftest` checks the pure
logic.
