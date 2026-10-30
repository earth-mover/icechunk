# icechunk.session

Sessions for reading and writing data. Includes `ForkSession` for distributed writes and `SessionMode`.

## Batched chunk-reference maintenance

`Session.get_chunk_refs` and `Session.set_chunk_refs` look up and update
caller-selected chunk coordinates without enumerating the array or reading native
or virtual chunk data. The opaque, immutable `ChunkReference` preserves the
complete stored reference, including offsets, lengths and checksums. Inline
chunk bytes remain inline. Both methods also have `_async` variants.

For overlapping moves, keep reads pinned to the original snapshot and commit
bounded batches to a private branch:

```python
original = repo.lookup_branch("main")
source = repo.readonly_session(snapshot_id=original)
repo.create_branch("maintenance", original)
completed = original

# An external planner supplies bounded batches of chunk-coordinate pairs.
for source_coords, destination_coords in planned_batches:
    refs = source.get_chunk_refs("/data", source_coords)
    destination = repo.writable_session("maintenance")
    destination.set_chunk_refs(
        "/data", list(zip(destination_coords, refs, strict=True))
    )
    completed = destination.commit("apply reference batch")
    del destination, refs

repo.reset_branch("main", completed, from_snapshot_id=original)
```

An uninitialized source produces `None`; writing it clears an occupied
destination. To implement moves, the planner must also emit explicit deletions
for vacated positions without deleting positions filled by another mapping.
Duplicate destinations within a call use the last update. Every update is
validated before the batch is applied, including coordinates and virtual
containers. Read-only and rearrange sessions cannot accept reference writes.
The caller is responsible for compatible destination array metadata.

References are process-local and cannot be constructed or pickled. Source and
destination sessions must share a storage handle, as sessions from one
`Repository` (or its `reopen` result) do. Independently opened storage handles
and cross-repository copying are explicitly rejected. Retain the source snapshot
against expiration and garbage collection until maintenance completes; holding
a session or reference alone does not protect it.

Fixed-size batches do not bound an indefinitely growing writable session.
Commit and release destination sessions between batches.

::: icechunk.session
