# Ingest benchmarking notes

Records of local perf runs and a recipe for a proper cloud-infra benchmark
once we have one set up.

## ⚠ Stale relative to current architecture

The throughput numbers below were collected against the **V1 ingest**
(single mega-commit: list everything, copy everything in one writable
session, commit once). The current ingest is **V2 resumable** (skeleton
commit, then one commit per `checkpoint_every` chunks per array).

Expect V2 to be slower in absolute throughput because each batch carries
a session-open + commit overhead. Per-batch commits give us
crash-resumability, atomic per-checkpoint progress, and bounded session
memory — but the throughput shape is different and must be re-measured.

**Action for the next cloud benchmark run**: sweep `checkpoint_every`
alongside `concurrency` to find the knee where commit overhead stops
dominating. Reasonable starting grid: `checkpoint_every ∈ {100, 1000,
10000}`, `concurrency ∈ {32, 64, 128, 256}`.

## Local-network runs (May 2026, V1 — stale)

### What we measured

`icechunk.from_zarr` against a public OME-zarr at IDR (Cambridge UK), copying
into an in-memory icechunk repo:

```
s3://idr/zarr/v0.5/idr0066/ExpA_VIP_ASLM_on.zarr   (anonymous, custom endpoint)
endpoint=https://livingobjects.ebi.ac.uk
```

Source has 6 pyramid levels (0–5). Each level is sharded with the
`sharding_indexed` codec; outer chunks of `[10, 2048, 2048]` containing
inner `[1, 256, 256]` blosc/zstd-compressed sub-chunks. Per-level layout:

| Level | Shape (z, y, x)         | Outer keys | Bytes (compressed) |
|-------|-------------------------|------------|--------------------|
| 5     | 1937 × 64 × 64          | 195        | 9.9 MB             |
| 4     | 1937 × 128 × 128        | 195        | 30.6 MB            |
| 3     | 1937 × 256 × 256        | 195        | 111.7 MB           |
| 2     | 1937 × 512 × 512        | 195        | (not measured)     |
| 1     | 1937 × 1024 × 1024      | 195        | (not measured)     |
| 0     | 1937 × 2048 × 2048      | 195        | (not measured)     |

(One outer key per shard. Each shard ≈ 575 KB compressed at level 3.)

### Throughput at concurrency=32 (default)

| Level | Time | Ingest      | Network     | RSS Δ  |
|-------|------|-------------|-------------|--------|
| 5     | 3.8s | 2.6 MB/s    | 2.79 MB/s   | +24 MB |
| 4     | 9.0s | 3.4 MB/s    | 3.59 MB/s   | +6 MB  |
| 3     | 24.3s| 4.6 MB/s    | 4.85 MB/s   | +1 MB  |

Network bytes ≈ ingested bytes + ~5% HTTP overhead — expected, since the
ingest is the only network activity. Memory is bounded — well under
~25 MB peak delta even on the largest run.

### Concurrency sweep on level 3

| Concurrency | Time   | Throughput  |
|-------------|--------|-------------|
| 8           | 62.0s  | 1.80 MB/s   |
| 32          | 30.9s  | 3.61 MB/s   |
| 64          | 22.8s  | 4.90 MB/s   |
| 128         | 22.0s  | 5.07 MB/s   |
| 256         | 19.3s  | 5.80 MB/s   |

Monotonic improvement up to c=256 (3.2× over c=8). That rules out
destination-side lock contention — that would *worsen* with higher
concurrency. Strongly suggests source/network side is the limiter.

### Honest disclaimer

These numbers were taken on a developer laptop over residential WiFi.
Anything in the path can be the bottleneck:

- WiFi link itself (~50 Mb/s = 6.25 MB/s, suspiciously close to our
  c=256 plateau).
- Residential ISP transatlantic peering.
- IDR endpoint per-IP rate limiting / connection pooling.

Don't draw conclusions about icechunk's ingest ceiling from these
runs. The signals worth keeping:

1. Memory is bounded — we don't have a leak.
2. Concurrency scales sub-linearly but monotonically up to at least 256.
3. Default `concurrency=32` is conservative — 64 or 128 would likely be
   a better default for cloud workloads.
4. Sharded zarr v3 round-trips byte-faithfully (decode-side spot check
   matched on src + dst at multiple frames).

## What a proper benchmark needs

Run from cloud infra with the source and sink in the same region. EC2
or Coiled instances near AWS S3 / GCS:

- **Co-located source.** Pick a public zarr-on-S3 dataset and run the
  ingest from a US-east-1 host (or wherever the bucket lives). Removes
  WAN latency.
- **Multi-region check.** Same dataset, ingest from a host in a
  *different* region. The delta tells us how much our throughput
  follows latency vs bandwidth.
- **Sweep concurrency.** 8, 32, 64, 128, 256, 512. Report the knee,
  the plateau, and where (if ever) per-process throughput stops
  scaling.
- **Sweep chunk size.** Run against a few sources with different chunk
  size distributions: tiny (~10 KB), normal (~1 MB), large (~100 MB),
  multi-GB shards. Report bytes/sec and keys/sec for each — the shape
  of the curve tells us where the per-key overhead dominates and where
  per-byte does.
- **Sweep destination.** in-memory storage vs S3 storage on the same
  region. The S3-dest run includes the actual icechunk-side persistence
  cost; in-memory isolates client-side overhead.
- **Compare to a baseline.** A trivial `for k in keys: dest.set(k,
  source.get(k))` Python loop. If our ingest is within 80% of `aws s3
  sync`, we're fine; if we're 10×, something's broken.
- **Track:** wall time, ingest MB/s, network in/out, peak RSS, p50/p99
  per-key latency, # HTTP connections (`/proc/.../net/tcp` count or
  reqwest stats if exposed).

## Things to suspect once we have real data

If we're far from the cloud-cloud ceiling, the targets to investigate
are documented in the source as `// TODO(scale)` and
`// IMPROVEMENT(...)` comments:

1. **Streaming `Store::set`** — multi-GB shards currently buffer fully
   in memory per task. Fix is in icechunk core (`Storage` trait gets a
   `put_object_streaming` method). See subagent investigation
   summarized in commit history.
2. **Default concurrency.** 32 → 64 or 128 is probably free.
3. **`list_source_keys` materialization.** For million-key stores,
   stringly buffering hurts time-to-first-byte.
4. **HTTP/2 negotiation.** reqwest + S3 endpoints — confirm we're on
   HTTP/2 with multiplexing. Probably already are with AWS S3, less
   sure with custom endpoints.
5. **Pre-list destination round trip.** Skipped if `overwrite=true`,
   otherwise unconditional. For first-ingest-into-fresh-repo, this is
   wasted work.

## Candidate datasets (verified May 2026)

Researched anonymously-accessible zarr datasets for benchmarks. All are
HEAD-verified reachable. Pyramid levels and sub-paths chosen to keep
quick benchmarks under ~1 GB of transfer.

| Dataset | Entry | Provider | Zarr | Chunk profile | Codec | Notes |
|---------|-------|----------|------|---------------|-------|-------|
| OME-Zarr SciVis aneurism | `s3://ome-zarr-scivis/v0.5/96x2/aneurism.ome.zarr` | AWS us-east-1 | **v3** | 64×64×128 uint8 ≈ 64 KB | zstd | Smallest v3 case (0.4 MB total). Smoke baseline. |
| OME-Zarr SciVis chameleon scale1 | `s3://ome-zarr-scivis/v0.5/96x2/chameleon.ome.zarr/scale1` | AWS us-east-1 | **v3** | 68×128×128 uint16 ≈ 2.2 MB | zstd | Mid-size 3D scan (~470 MB). |
| OME-Zarr SciVis woodbranch scale0 | `s3://ome-zarr-scivis/v0.5/96x2/woodbranch.ome.zarr/scale0` | AWS us-east-1 | **v3** | 128×128×128 uint16 ≈ 4 MB | zstd | Heavy single-array (~6.9 GB). |
| HRRR TMP surface | `s3://hrrrzarr/sfc/20210101/20210101_00z_anl.zarr/surface/TMP` | AWS us-west-1 | v2 ⚠ | 150×150 float16 ≈ 44 KB | blosc/lz4 | Tiny-chunk extreme; per-key overhead. |
| IDR idr0062A 6001240 | `https://uk1s3.embassy.ebi.ac.uk/idr/zarr/v0.4/idr0062A/6001240.zarr` | EBI EU (HTTP) | v2 ⚠ | 1×1×275×271 uint16 ≈ 146 KB | blosc/lz4 | Non-AWS S3-compatible endpoint. |
| CMIP6 CESM2 Amon tas (S3) | `s3://cmip6-pds/CMIP6/CMIP/NCAR/CESM2/historical/r1i1p1f1/Amon/tas/gn/v20190308` | AWS us-west-2 | v2 ⚠ | 600×192×288 float32 ≈ 127 MB | blosc/lz4 | Large regular-chunk climate. |
| CMIP6 CESM2 Amon tas (GCS) | `gs://cmip6/.../tas/gn/v20190308` | GCS us-central1 | v2 ⚠ | same as above | blosc/lz4 | Same data, different provider. |
| NASA POWER MERRA-2 daily | `s3://nasa-power/merra2/temporal/power_merra2_daily_temporal_utc.zarr` (var T2M) | AWS us-west-2 | v2 ⚠ | 5844×15×15 float32 ≈ 5 MB | blosc/zstd + fixedscaleoffset | Filter-pipeline coverage. |

⚠ = zarr v2. Our ingest only supports v3; these would need a v2 source
adapter (not in scope) or are listed for completeness.

### Coverage gaps

- **No public zarr v3 sharded data found.** NEXRAD ARCO and
  dynamical.org GFS use sharding internally but are wrapped in
  icechunk's on-disk format, so they're not plain-zarr-readable. We
  verified locally (IDR idr0066) that our ingest handles sharding, but
  there's no public sharded source for an anonymous benchmark.
  Workaround: synthesize a sharded store on the cloud benchmark host.
- **No public zarr v3 on GCS or Azure** confirmed. The benchmark for
  cross-provider ingest will probably use the SciVis datasets on S3 +
  CMIP6 on GCS (v2), with a v3 mirror to set up later.

## Pointers

- Test scripts: `/tmp/ome_perf.py`, `/tmp/ome_concurrency.py`
  (recover from history if needed; not committed).
- Source code: `icechunk/src/ingest/mod.rs`.
- Subagent investigation of streaming `Store::set` feasibility:
  see chat history with agent `acb1b05c731c99f25`.
- Subagent investigation of pyo3 tokio-runtime concerns (concluded:
  no runtime mismatch): agent `aee77175fcac3bc70`.
- Subagent dataset research: agent `a2ab2b0ac37908810`.
