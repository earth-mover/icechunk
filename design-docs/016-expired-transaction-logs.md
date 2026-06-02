# Expired transaction logs

## Motivation

Expiration rewrites `RepoInfo` so that the oldest non-expired
snapshot on each branch has its `parent_id` reset to
the new root for that branch. The expired ancestors are removed from
`RepoInfo.snapshots`. Eventually GC deletes their snapshot files and their
transaction log files.

The transaction log of the edited snapshot, is left untouched. It
still describes only the per-commit delta from the edited snapshot's
*original* parent. This original parent a snapshot no longer exists in `RepoInfo` and
will be deleted by GC. Any code that interprets that transaction
log as "the changes between this snapshot and its current parent in
`RepoInfo`" now gets the wrong answer.

### What this breaks today

1. **`Diff`**. Walks the
   `RepoInfo` ancestry from `to` back to `from` and folds in one transaction
   log per snapshot. `diff(Root, D)` collects only `tx_D` (C → D) and reports
   that as the full Root → D diff. **The most visible and most likely user-
   facing manifestation of the bug.**

2. **Amend**. On
   amend, the previous commit's transaction log is merged with the new
   change set to produce the amended commit's transaction log. If the
   branch tip happens to be an edited snapshot it won't contain the whole list of operations.

3. **`Rebase`**. The conflict
   solver compares the rebasing session's change set against each commit's
   transaction log. If between the session's `snapshot_id` and the current
   branch tip there are now expired-and-gone commits whose transaction logs
   would have flagged a conflict, those conflicts become invisible.

4. **`inspect::transaction_log_json`**. The tx log file is incomplete.

### Merge transaction logs at expiration time?

- **Flatbuffer size limits.** The buffer cannot exceed ~2 GB. Tx log merges would accumulate until they exceed the limit.

## Design

### Non breaking format change

Add an optional field to `SnapshotInfo` in `repo.fbs`:

```flatbuffers
table SnapshotInfo {
    id: ObjectId12 (required);
    parent_offset: int32;
    flushed_at: uint64;
    message: string (required);
    metadata: [MetadataItem];

    // Tx-log ids of ancestor commits removed by expiration.
    pruned_ancestor_tx_logs: [ObjectId12];
}
```

This is a purely additive change. Flatbuffer readers built against the
older schema parse new `SnapshotInfo` records without error.

We do not bump the spec version.

### Population at expiration time

In expiration, when computing the rewritten `parent_id` for an edited
snapshot D (whose old parent was released), also compute its
`pruned_ancestor_tx_logs`:

1. Walk D's ancestry in the **pre-expiration** `RepoInfo` from D's old
   parent backwards to (but excluding) the branch root, which becomes
   D's new `RepoInfo` parent. Re-parenting D to the root drops every
   snapshot X on this path from D's ancestry. Most are being released in
   this run, but X may instead be *retained* (e.g. an old snapshot a tag
   tip protects); either way its log must be carried, since X is no
   longer on D's path.
2. Build the list by concatenating, oldest first,
   for each such X: `X.pruned_ancestor_tx_logs` followed by `X`'s own
   id. Pulling in each ancestor's own list is what carries accumulation
   across repeated expirations — a later expiration edits a *new*
   boundary snapshot E, and the previously accumulated ids live on the
   now-pruned D that sits on E's path, not on E itself.
3. Set the new `SnapshotInfo` for D in the post-expiration `RepoInfo`
   with this list. The list grows monotonically across repeated
   expirations.

### GC retention rule

GC will not delete tx logs that are present in any snapshot's
`pruned_ancestor_tx_logs`. It computes this by reading the repo info
object and unioning those ids into the tx-log keep set.

The corresponding **snapshot** files for the ids in
`pruned_ancestor_tx_logs` are still eligible for deletion.

### Internal consumers

#### `Repository::diff`

When we fetch transaction logs
expand each snapshot's
contribution to fetch its own transaction log **plus** every id listed
in its `pruned_ancestor_tx_logs`. Order within a single snapshot's
contribution follows `pruned_ancestor_tx_logs` order (oldest first),
then the snapshot's own log last.

A tx log referenced by `pruned_ancestor_tx_logs` may be missing if an
old GC binary deleted it (see Compatibility). For `diff` we **warn and
skip** the missing log, producing a degraded (incomplete) diff rather
than failing.

#### Amend

Amend fetches `previous_log` and merges it with `this_tx_log` to produce
the tx_log of the amended snapshot D'. We do **not** absorb
`pruned_ancestor_tx_logs` into this merge, instead the chain is copied
onto D'.

#### `Session::rebase`

Rebase already loops over `commits_to_rebase` and calls `solver.solve`
once per commit, threading the resulting `patched_changeset` into the
next iteration. We extend the inner loop: for each commit, first call
`solver.solve` for each id in its `pruned_ancestor_tx_logs` (oldest
first), then call it for the commit's own tx_log. No
`TransactionLog::merge` is needed, so no overflow risk.

The solver's `session` argument for a pruned-ancestor-log iteration is
the edited snapshot's readonly session.

Because a missing log here would silently hide conflicts (a correctness
hazard, not just a degraded view), rebase **errors** rather than skips
if a referenced `pruned_ancestor_tx_logs` log is missing. This is
infrequent since it would require rebasing over expired commits.

#### `inspect`

The `transaction_log_json` view of an edited snapshot should reflect
the cumulative delta the user expects. The simplest implementation:
when an inspected snapshot has a non-empty `pruned_ancestor_tx_logs`,
fetch and merge them with the snapshot's own log before rendering
(warn and skip any missing log).

A small UI hint indicating that the displayed log is a synthetic
composite would be valuable.

## Bounded growth

`pruned_ancestor_tx_logs` grows monotonically with repeated expirations
on the same branch. Each entry is a 12-byte `ObjectId12`.

In the future we could implement **periodic compaction**: a separate operation can merge several
  consecutive entries in `pruned_ancestor_tx_logs` into a single new
  transaction log object (so long as the merge stays under the
  flatbuffer ceiling), and rewrite the list accordingly

The launch design ships only the list. Compaction is a follow-up if and
when growth becomes a real problem.

## Compatibility

The format change is additive. Old readers and writers see new repos correctly with
the degraded behavior of the current bug.
No data is corrupted; reads and writes continue to function.

An old icechunk binary running **GC** against a
new repo doesn't know about `pruned_ancestor_tx_logs` and will delete
the transaction log files those ids reference, regressing the fix on
that repo. Consumers therefore handle a missing referenced log:
`diff`/`inspect` warn and skip; `rebase` errors (see above).

We accept this risk. The fix is shipped as a normal release; users on
the new release benefit; users mixing old and new icechunk binaries
have some risk.

## Migration of existing repos

Repos that have already run `expire_v2` before this fix lands have lost
the transaction logs of their expired ancestors (GC has run, or will
run, and delete them). For those snapshots no migration can recover
correctness; the data is gone. Going forward, every new expiration
populates `pruned_ancestor_tx_logs` correctly.

## Out of scope

- IC1 repositories. IC1's has its own
  variant of this bug but is not fixed here.
- Composing moves correctly inside `DiffBuilder`. `DiffBuilder` today
  appends moves without composing them across multiple transaction
  logs. This is a pre-existing issue independent of expiration; this
  document does not address it but the work here makes addressing it
  in a follow-up trivial (the necessary logs are now reachable).
- Periodic compaction of `pruned_ancestor_tx_logs`. Punted until growth is
  shown to be a real problem.
