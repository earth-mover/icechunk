# Distributed writers: flush on the worker, merge on the coordinator

Issue: [#2060](https://github.com/earth-mover/icechunk/issues/2060)

## Motivation

Icechunk has two ways to write from many processes. Both move a `Session`
object between the coordinator and the workers:

* Cooperative distributed writes. The coordinator creates a writable session,
  pickles it, sends it to the workers, and receives the modified sessions back.
  It merges them with `Session.merge` and commits once.
* Fork sessions. The coordinator forks a session that captures uncommitted
  state. Workers write against the fork and send it back.

Some deployments cannot move a `Session`. The workers run on different
machines under a job scheduler. The scheduler can pass strings to a task and
return strings from a task, but not arbitrary Python objects. Zarr-python plans
disjoint region writes and delegates atomicity to the storage engine. It also
needs a coordinator step that accepts opaque handles.

This design lets each worker save its changes as a detached snapshot and return
only the snapshot id. The coordinator merges the ids into one commit.

## Design

The worker side needs no new API. A worker opens a writable session, writes
its region, and calls `Session.flush`. Flush writes the manifests, the snapshot
file, and the transaction log. Then it registers the snapshot in repo info with
a compare-and-swap update of type `NewDetachedSnapshotUpdate`. The registration
records the parent snapshot. No branch points at the new snapshot. The worker
returns the id string.

The coordinator calls `Repository.merge_snapshots`:

```rust
pub async fn merge_snapshots(
    &self,
    branch: &str,
    snapshots: &[SnapshotId],
    message: &str,
    properties: Option<SnapshotProperties>,
) -> SessionResult<SnapshotId>
```

```python
def merge_snapshots(
    self,
    branch: str,
    snapshots: list[str],
    message: str,
    *,
    metadata: dict[str, Any] | None = None,
) -> str
```

The call returns the id of the new commit on `branch`. The Python method has
a sync and an async form. Properties are the repository default commit
metadata plus the caller values, as in `Session.commit`.

The merge lives in `icechunk/src/ops/merge.rs`. It reuses the flush helpers in
`session.rs` for manifest writes and the commit compare-and-swap. `Session.flush`
does not change.

### Decisions

| Question | Decision |
|---|---|
| Merge level | Manifest level. Manifests stream one split at a time. The coordinator holds every source transaction log. Memory grows with the count of written chunk coordinates, not with chunk references. |
| Conflicts | Fail. The error lists every conflict. There is no solver and no priority option. |
| Branch tip moved past a source parent | Merge onto the current tip. Check each source against the intervening commits. Fail on conflict. |
| Spec versions | V2 only. V1 fails with the same error `amend` uses. |
| Source cleanup | None. The sources become unreachable after the merge. Garbage collection removes them. |
| Error type | `SessionResult`, as `Repository::diff` returns, because the reused flush helpers return session errors. |

### Algorithm

One repo info read serves the whole run.

1. **Plan.** Resolve `branch` to the tip T. Read the parent P of each source
   from repo info. Walk the ancestry of T until every P appears. A source whose
   parent is not an ancestor of T fails with `SnapshotNotInBranchHistory`. The
   commits strictly between P and T are the intervening commits of that source.
   The pruned-ancestor logs of an intervening commit count as intervening logs,
   as in `Session::rebase`. A missing pruned log fails with
   `MissingPrunedAncestorTxLog`.
2. **Fetch logs.** Fetch every source log and every intervening log. A log with
   move operations adds a `MoveOperationCannotBeRebased` conflict.
3. **Check conflicts.** Compare each source against its intervening logs, then
   each pair of sources. The checks are the ones `ConflictDetector` performs
   for rebase, with two differences. `ChunksUpdatedInUpdatedArray` and
   `NewNodeInInvalidGroup` run in both directions, because neither source has
   seen the other. Chunk coordinate checks run per node and load only the
   coordinates of one node from one log at a time. The merge collects every
   conflict and fails once with the full list.
4. **Merge nodes.** Start from the node list of T. Per source, remove deleted
   nodes, replace the metadata of updated nodes, and add new nodes. A new array
   takes its manifest refs from the source snapshot as they are.
5. **Merge manifests.** For each array that a source wrote to, compute the
   splits as flush does, from the coordinator config and the result shape. For
   each split a source touched, either reuse or rewrite. Reuse applies when one
   source touched the split and its parent is T. That source must also have
   exactly one manifest that intersects the split, and it must lie fully inside
   the split. Reuse costs no read and no write. Otherwise the merge builds the
   modified chunk table for the split from the source manifests. It passes the
   table and the manifests of T to `write_manifest_with_changes`. A coordinate
   present in a source log but absent from its manifest is a deletion.
6. **Write and commit.** Merge the source logs with `TransactionLog::merge`.
   Build the snapshot, write it with its log, and call `do_commit_v2` with
   parent T. If the tip moved during the merge, the commit fails with the
   existing `Conflict` error. The sources are untouched, so the caller runs
   the merge again.

A failed merge leaves no partial state on the branch. Objects written by a
merge that lost the commit race are unreachable, and garbage collection removes
them.

### Errors

New variants on `SessionErrorKind`:

* `MergeConflict { conflicts }`. Each item names the two snapshots and the
  `Conflict` kind.
* `SnapshotNotInBranchHistory { snapshot, branch }`.
* `NoSnapshotsToMerge`. The source list is empty.

Python adds `MergeConflictError`, a subclass of `ConflictError`, built like
`RebaseFailedError`. The other two kinds map to `InvalidInputError`.

## Alternatives considered

### Coordinator assigns snapshot ids before dispatch

The coordinator mints one snapshot id per task and passes it to the worker.
The worker calls `flush` with that id. The coordinator checks that each id
exists in repo info, then merges. The merge itself does not change.

This flow needs no return channel from the worker. That helps with schedulers
that discard task results. It also lets a coordinator run the merge later from
a stored id list, and name the task that did not finish.

It has costs that the chosen design does not:

* Task retries collide. Schedulers retry failed tasks. Two attempts of one task
  flush the same id. The second registration fails with `DuplicateSnapshotId`,
  even when the data is complete. With random ids, a retry produces a second
  detached snapshot. The scheduler returns the id of the attempt that
  succeeded. Garbage collection removes the other.
* A partial flush plus a retry overwrites a snapshot key. Flush writes the
  snapshot file and then registers it. If attempt one loses the registration,
  attempt two writes the same key again. Icechunk treats snapshot files as
  write-once.
* Snapshot ids must stay unique for the life of the repository. A caller who
  derives ids from a task index collides on the second run. The safe form
  restricts ids to values from an icechunk mint function, which equals a
  random id with extra plumbing.
* New API surface on `flush` in Rust and Python, plus the mint function.

The chosen design does not exclude this flow. An optional id argument on
`flush` is an additive change, see Future work.

### Session-level merge of detached snapshots

A `Session` method that pulls detached snapshots into the current change set,
then commits. This form needs the change set to hold every chunk reference of
every source in memory. The manifest-level merge streams one split at a time
and only holds the transaction logs. The session form also duplicates the
conflict logic that the merge already runs against the branch.

### Conflict solver

A `merge_snapshots` option that resolves conflicts with a policy, as
`Session.rebase` does with `ConflictSolver`. The region writes this design
targets do not overlap, so they produce no conflicts. A conflict in that
setting is a bug in the caller's partition of the work. An error with the full
conflict list is the useful answer. A solver stays possible as a later option.

## Drawbacks

* **Expiration and garbage collection do not know about pending sources.**
  Both treat a detached snapshot as unreachable. A run with a cutoff newer than
  a pending flush deletes the flush before the merge uses it. The docs warn
  about this. There is no lock or lease.
* **Concurrent flushes need conditional writes.** Every flush updates repo
  info with a compare-and-swap. Local file system storage has no conditional
  writes and loses concurrent registrations. The same limitation already
  applies to concurrent commits.
* **Repo info grows with every flush.** Each detached snapshot adds an entry
  until garbage collection removes it. A job with many small tasks adds many
  entries between garbage collection runs.
* **Manifest reuse is narrow.** Reuse needs the source parent to be the tip
  and one manifest per split. A source written with a finer split
  configuration takes the rewrite path for every split it touched. So does a
  source whose parent is behind the tip. The rewrite is correct but costs one
  manifest read and write per split.
* **The coordinator holds every source log.** Memory grows with the total
  count of written chunk coordinates across all sources. A job that writes
  millions of chunks per task needs a large coordinator.
* **Coordinate variables conflict across workers.** Xarray region writes
  rewrite coordinate variables in every worker. Two workers that write the
  same coordinate chunks fail with `ChunkDoubleUpdate`. The caller must drop
  shared variables before the write, as the docs example does.
* **The sources stay in the repository.** They are unreachable but present
  until garbage collection runs. A user who lists all snapshots sees them.

## Testing

* Rust unit tests in `ops/merge.rs` on in-memory storage. They cover one test
  per conflict variant, the merge plan, the node merge, and manifest reuse and
  rewrite. End to end tests cover a moved tip, a conflict in a pruned log, and
  a V1 repository. One test runs garbage collection after a merge.
* Rust integration: flush and merge against MinIO in
  `tests/test_distributed_writes.rs`.
* Python, in `tests/test_merge_snapshots.py`: sync and async entry points,
  the `MergeConflictError` payload and its pickle round trip, and a process
  pool end to end test.
* Python stateful: a rule in `VersionControlStateMachine` flushes several
  sessions from the tip and merges them.

## Future work

* **Caller-supplied snapshot ids on `flush`.** Some workers have no return
  channel. One example is a NetCDF extension that writes through icechunk from
  a process the coordinator cannot read results from. The change adds an
  optional id argument on `flush` in Rust and Python and a function that mints
  valid ids. A duplicate id must report a clear error. The merge does not
  change. The retry collision described above needs an answer first. One
  option is a flush that succeeds when the registered snapshot has the same
  parent and content hash.
* **List detached snapshots.** A coordinator that does not receive worker
  results needs to find pending sources in the store. Repo info records every
  flushed snapshot, but nothing marks it as detached. The listing computes the
  set: every snapshot minus those reachable from a branch tip or a tag.
  Garbage collection computes the same set as `pointed_snaps`. Proposed shape,
  next to `list_branches` and `list_tags`:

  ```python
  def list_detached_snapshots(self) -> list[SnapshotInfo]
  async def list_detached_snapshots_async(self) -> list[SnapshotInfo]
  ```

  It returns the existing `SnapshotInfo` type, oldest first, from one repo
  info read. Callers filter on `metadata` set at flush time, such as a run id.
  No filter arguments, because repo info is already in memory. V1 fails with
  `BadRepoVersion`, because V1 flush does not register snapshots. Callers must
  handle three cases. Merged sources stay listed until garbage collection
  removes them. A retried worker leaves two snapshots with overlapping chunks,
  so the caller keeps one per task. Flushes from other jobs appear too, so the
  metadata filter scopes the list.
* **A "merged from" snapshot property.** Record the source ids on the merge
  commit for provenance.
* **Source cleanup option.** Delete the source snapshots after a successful
  merge instead of waiting for garbage collection.
* **Protect pending sources from expiration and garbage collection.** A lease
  or a minimum age for detached snapshots.
* **Conflict solver option.** Reuse `ConflictSolver` policies when two sources
  touch the same chunk.
* **Session-level entry point.** Merge detached snapshots into an open
  session. A caller can then add more changes before the commit.
