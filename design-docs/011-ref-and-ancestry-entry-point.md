# Single Entry Point Object for Refs and Ancestry

This designs tries to solve multiple issues at once,
see [Notes Towards an Icechunk 2.0Single](./010-notes-towards-an-IC-2.md)

## Design

We add a new object at the root of the repo file structure: `$ROOT/repo`.
The new object is a flatbuffer with the following structure:

```flatbuffer
table Tag {
    name: string (required);
    snapshot: ObjectId12 (required);
}

table Branch {
    name: string (required);
    snapshot: ObjectId12 (required);
}

table MetadataItem {
  name: string (required);
  value: [uint8] (required);
}

table SnapshotInfo {
    id: ObjectId12 (required);
    parent_offset: u32;
    flushed_at: uint64 (required);
    message: str (required);
    metadata: [MetadataItem] (required);
}

enum RepoAvailability : ubyte { Online = 0, ReadOnly, Offline }

table RepoStatus {
    availability: RepoAvailability (required);
    limited_availability_reason: string;
    set_at: uint64 (required);
}

table Repo {

  // sorted by name
  tags: [Tag] (required);

  // sorted by name
  branches: [Branch] (required);

  // sorted by name
  deleted_tags: [string] (required);

  // sorted by id
  snapshots: [SnapshotInfo] (required);

  last_updated_at: uint64 (required);

  status: RepoStatus (required);
  spec_version: string (required);
}

root_type Repo;
```

The `repo` object stores:

* The spec version used on the last write to this repo
* The timestamp for the last update
* A status flag that identifies the availability for the repo:
online, read-only, offline
* The list of all tag names with their SnapshotId, sorted by name to be able to
binary search them.
* The list of all branch names with their SnapshotId, sorted by name to be able to
binary search them.
* The list of deleted tags to reject recreation of a deleted tag.
* The list of all snapshots, sorted by id.
  * Each snapshot includes its parent id, and metadata needed for `ancestry`
  * We don't include the manifest files for the snapshot, that will have
  to be removed from the `ancestry` result. Not a big deal, don't imagine
  anybody using it. If we don't want to break the API, we'll have to implement
  it as a function instead of a field.
  * Each snapshot takes ~ 256 bytes:
    * SnapshotId: 12 bytes
    * Parent offset: 4 bytes
    * Flushed at: 8 bytes
    * Message: 200 bytes
    * Metadata: 30 bytes
  * In memory for 10k snapshots ~ 2.5 MB
  * Total storage on 10k snapshots ~ 12 GB ~ $3/year. Compression will help some.

### Repository open

The `repo` object is fetched, decompressed and kept in memory in the `Repository`.
Together with the `repo` object we maintain its etag/generation.

Spec version is checked, and an error generated if trying to read from a newer repo.

On `writable_session`, fail if the repo is older, warning that this can
break forward compatibility. User can pass a flag to allow the breakage.

### Ancestry

`ancestry` becomes much faster because it only needs to look at the in memory
`repo` flatbuffer. On each new pull from the iterator, the parent offset of
the current snapshot is fetched.

Notice that `snapshot` objects still have a `parent_id`, but this is not
used for ancestry any more, or for anything else currently.

### Commit

The process for commit only changes when it's time to update the branch ref.

* A new `repo` object is generated with:
  * the new snapshot added
  * the updated branch pointing to the new snapshot
* Update the `repo` object conditionally on not being modified since the
  repo was opened
  * If successful, commit done
  * If condition violation, pull the new `repo` object.
    * Verify conditions for the commit:
      * Repo is online
      * Spec version matches
      * If tip of branch has not changed, merge the files and save new file conditionally.
        Iterate as needed. The merge process is adds the new snapshot to the file
      fetched from storage
    * If tip of branch has changed, rebase is needed

### Rebase

TODO

### Amend

`amend` requires the same commit process as before, but use parent's
parent as snapshot parent in the `repo` file.

Then, find any snapshots in `repo` that have the amended commit as parent and make
them point to new snap. Finally, find any tags/branches pointing to the amended
commit and make them point to the new snapshot.

Finally the usual commit process is followed. So it only differs in the
preparation of the snapshot object (different parent) and the `repo` object.

### Expiration

`expire_snapshots` becomes an operation done only on the `repo` file. In particular
expiration no longer needs to overwrite snapshots.

Once a new `repo` file is generated in memory, it's written using conditionals.
Failures are recovered by editing the file again.

Expiration can generate conflict with ongoing sessions, because it modifies
the `repo` file, but those generate recoverable conflicts which can be
handled within `commit` without user intervention.

### GC

Not affected, just faster because ancestry is faster.

Should this change the updated_at flag in `repo`? Maybe we need more fields.

### Storage stats

Not affected, just faster because ancestry is faster.

### Branch create

A simple conditional update on the `repo` file. Failures are retried.
Rejected if branch already exists.

### Branch delete

* Delete branch and all snapshots that are only accessible from it.
* Conditional update of `repo` file with retries. Rejected if branch changed.

### Branch reset

* Update the branch pointer. Delete any snapshots only accessible by the old branch.
* Conditional update of `repo` file with retries. Reject if branch changed.

### Change repo status

Updates `repo` conditionally. Conflicts on ongoing commits need to be handled
rejecting writes if needed.

### What to do with SnapshotInfo in snapshots that are duplicated now

* We keep them there for now

### Store extra info in the `refs` file

* We could put more stuff in here: stats, etc

## Upgrade from 1.x repos

* TODO

## Trade-offs

* Pros:
  * Fast ancestry, fast expiration
  * Can implement amend, which should enable shorter histories
  * More consistency
* Cons:
  * `repo` object can reach ~  2.5 MB
  * Need to write this larger object on every commit, including when creating
  the repo.
  * Overhead on commit to write the `repo` object
  * Overhead on rebase to read the larger `repo` object, potentially multiple times.
  * More storage overhead.
  * Ref operations are more complex

## Questions

* Should we move the config to this new object?
* What other fields should we add?
  * Arbitrary user properties on the repo?
  * Global stats?
