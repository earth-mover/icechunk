# Copying a Zarr v3 store into icechunk

Runnable examples live next to this page in the repo:

- [`examples/from_zarr_local.py`](https://github.com/earth-mover/icechunk/blob/main/icechunk-python/examples/from_zarr_local.py) — local Zarr → local icechunk, with round-trip verification and resume detection.
- [`examples/from_zarr_ome.py`](https://github.com/earth-mover/icechunk/blob/main/icechunk-python/examples/from_zarr_ome.py) — anonymous read of a public OME-Zarr S3 dataset → local icechunk.


`icechunk.from_zarr` does a byte-faithful, resumable copy of a Zarr v3
store into an icechunk repository. Use it to bring an existing Zarr
dataset under icechunk's branching, snapshotting, and versioning model.

The copy is driven as a series of commits on the target branch:

1. A **skeleton** commit holding every `zarr.json`.
2. One commit per chunk batch per array.
3. A final marker commit recording `phase = "complete"`.

If the process dies mid-copy, a subsequent call against the same
repository resumes from the last committed batch — no separate state
file, just the commit properties on the branch tip.

## Quick start

```python
import icechunk

src = icechunk.s3_storage(
    bucket="idr",
    prefix="zarr/v0.5/idr0066/ExpA_VIP_ASLM_on.zarr",
    endpoint_url="https://livingobjects.ebi.ac.uk",
    anonymous=True,
)

repo = icechunk.Repository.create(
    icechunk.local_filesystem_storage("./ome.icechunk")
)

result = icechunk.from_zarr(src, repo, message="ome-zarr import")
print(result.snapshot_id, result.stats.keys, "keys copied")
```

## Use cases

### One-shot bulk import

Copy an entire Zarr dataset into a fresh icechunk repository.

```python
icechunk.from_zarr(src, repo)
```

### Subset import

Restrict to one or more zarr node paths inside the source.

```python
icechunk.from_zarr(src, repo, paths=["5"])             # one resolution level
icechunk.from_zarr(src, repo, paths=["raw", "labels"]) # two top-level groups
```

### Crash recovery

If a long-running copy is killed (SIGKILL, OOM, network outage),
re-running the *same* call resumes from the last committed batch.
Don't pass any special flag — resume is automatic.

```python
icechunk.from_zarr(src, repo)  # first run: crashes after 30 minutes
icechunk.from_zarr(src, repo)  # second run: picks up where it stopped
```

### Layered ingest

You've already populated the target repo with some chunks by hand;
copy the rest from the source but leave existing destination keys
untouched.

```python
icechunk.from_zarr(src, repo, on_collision=icechunk.CollisionPolicy.Skip)
```

### Re-ingest after fix

The source had bad data; replace what's in icechunk with a fresh copy.

```python
icechunk.from_zarr(
    src, repo, on_collision=icechunk.CollisionPolicy.Overwrite,
)
```

### Progress reporting

`on_progress` fires once per durable commit (skeleton, each chunk
batch, each per-array DONE marker, and the final close-out).

```python
def show(stats):
    print(f"copied {stats.keys} keys, {stats.bytes / 1e9:.1f} GB")

icechunk.from_zarr(src, repo, on_progress=show)
```

## Collision policies

| Policy                            | If the destination key already exists |
|-----------------------------------|----------------------------------------|
| `CollisionPolicy.Fail` (default)  | Raise `ValueError` (`KeyCollision`). Also fails if the branch's last commit is already `phase=complete`. |
| `CollisionPolicy.Skip`            | Leave the destination key as-is; don't copy. |
| `CollisionPolicy.Overwrite`       | Replace the destination key with the source bytes. Required to re-ingest a completed branch. |

## What gets stored in commit properties

State for resume lives in each commit's `SnapshotProperties` under
these keys:

| Key                          | Meaning                                              |
|------------------------------|------------------------------------------------------|
| `icechunk.ingest.phase`      | `"skeleton"`, `"chunks"`, or `"complete"`.           |
| `icechunk.ingest.cursors`    | `{array_path: last_chunk_key_or_"DONE"}`             |

You normally don't read these directly — they're an implementation
detail of the resume protocol — but they're visible via
`repo.lookup_snapshot(snap_id).metadata` for debugging.

## Tuning

- `checkpoint_every` — chunks copied between commits. Default 1000.
  Lower = finer-grained resume, more commits. Higher = fewer commits,
  more lost work on crash.
- `concurrency` — per-batch parallelism for source → destination
  copies. Default 32.
- `verify_source_unchanged` — on resume, compare each `zarr.json`'s
  bytes between source and destination (catches a mutated source).
  Default `True`. Disable for very deep hierarchies where the extra
  GETs at resume start noticeably hurt and the source is provably
  stable.

## What is not supported today

- **No destination-side path remap.** Source keys land at the same
  relative path in icechunk; you can't, say, copy `/raw/2024` to
  `/archive/2024`.
- **No partial / regional chunk-level ingest.** It's all chunks for
  the requested `paths`, or none.
- **No virtual-reference mode.** Chunk bytes are always copied; there
  is no flag to record virtual references to the source store instead.
- **Source must be an `icechunk.Storage`** built from one of the
  standard constructors: `s3_storage`, `gcs_storage`, `azure_storage`,
  `http_storage`, `local_filesystem_storage`, `in_memory_storage`.
  Wrapped storages like `LatencyStorage` aren't accepted as sources.
- **Very large hierarchies.** Array enumeration is in-memory; the
  practical ceiling is on the order of 10⁶ arrays per ingest.

## How chunk bytes move (and where they could move faster)

Today, every chunk takes the same path:

```
source GET → bytes in client memory → destination PUT (icechunk Storage)
```

That works for any source–sink pair, but it round-trips every byte
through the ingest process. Since icechunk picks its own chunk-object
names, we can skip the round-trip by materialising the chunk at the
name icechunk wants via the cheapest mechanism available, then writing
just the manifest entry pointing at it:

```
generate ChunkId → stage bytes at icechunk's path (cheap)
                → session.set_chunk_ref(ChunkPayload::Ref { id, offset, length })
```

The resulting `ChunkPayload::Ref` is indistinguishable from one written
the slow way — the icechunk repo is fully self-contained and has no
dependency on the source after ingest.

| Future mode                | "Stage bytes cheaply" mechanism                                                | Trade-off                                                                                |
|----------------------------|--------------------------------------------------------------------------------|------------------------------------------------------------------------------------------|
| `mode="same-bucket-copy"`  | S3 `CopyObject` (and equivalents on GCS / Azure). Bytes stay server-side.      | Requires source and sink in the same bucket/account.                                     |
| `mode="local-clone"`       | `copy_file_range` / hardlink on the same filesystem.                           | Local FS only; modest single-machine gain.                                               |
| `mode="virtual"`           | No staging at all — write `ChunkPayload::Virtual` pointing at the source object. | Source store must remain alive and unchanged; deleting it breaks the icechunk repo.      |

`mode="same-bucket-copy"` is the most impactful for typical cloud
ingests; it produces a real, self-contained icechunk repo at a tiny
fraction of the I/O cost. `mode="virtual"` is the most impactful when
you'd otherwise duplicate enormous datasets and can guarantee the
source's permanence — different trade-off, but the manifest plumbing
(`set_chunk_ref` with a `Virtual` payload) is the same shape.
