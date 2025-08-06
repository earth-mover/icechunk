# Notes Towards an Icechunk 2.0

This is intended to be a living document, receiving updates as we come up with
concrete plans and design documents.

## Current limitations and issues

This is a list of potential candidates to improve on with Icechunk 2.0

### Slow `ancestry`

Not a major problem, we don't expect users to navigate ancestry too deeply.
But it becomes more of an issue when it's time to compute expiration, GC,
or storage statistics, because those require navigating all ancestry.

On repos with many versions, this could take minutes, because it's very sequential.

* See [Single Entry Point Object for Refs and Ancestry](./011-ref-and-ancestry-entry-point.md)

### Cannot `amend` instead of `commit`

Users don't really care about having perfect time travel. If they could, they
would `amend` instead of `commit` very frequently. This would create shorter
histories, and less wasted space.

* See [Single Entry Point Object for Refs and Ancestry](./011-ref-and-ancestry-entry-point.md)

### Cannot squash

* See previous section

### Cannot set repository state (lock)

It would be nice to flag repos as online, read-only, archived, what else?.

Of course this status cannot be enforced but it would be useful. Icechunk
can refuse to write to a read-only repo, or to open an archived one. It
gives users an extra level of protection against bugs.

* See [Single Entry Point Object for Refs and Ancestry](./011-ref-and-ancestry-entry-point.md)

### Cannot distribute dirty sessions

A current limitation is that `Session.fork` can only be called in an empty session.
To lift this limitation we need a double-changeset mechanism. The first change-set
tracks the main session, the second change-set tracks the in-session changes.
Merges must only merge in-session changes.

Alternative thought: on fork, if the session is dirty, create an anonymous
snapshot on storage. Use that snapshot as the base for the spawned sessions.
Then skip it in the parent_id, it will be GCed eventually.

### Two object store implementations

Can we move to object_store only? There are trade-offs, particularly because
object_store doesn't support everything we need, and it's slow moving.

To make matters more complicated, we are currently unable to upgrade AWS SDK
because it's hard to build the wheels with newer versions (I'm sure there
are workarounds but it's definitely not easy).

### Cannot persist partial sessions

It would be great to be able to persist session as a form of checkpointing
and to free memory. Also, combining forked sessions could be done
via disk, instead of network + memory.

This can be achieved using anonymous snapshots.

### Cannot `move` efficiently

We currently don't support an efficient `move` operation to change the
structure of the Zarr hierarchy. To implement this nicely it would be
good to add a new `moved` operation to our transaction logs and conflict
detection.

### Hard to know repo spec version

Currently there is no real way to know the spec version of a repo. Best
chance is going through every branch/tag looking at the spec version in
the snapshot file

* See [Single Entry Point Object for Refs and Ancestry](./011-ref-and-ancestry-entry-point.md)

### Support more complex array updates

Example: backfills, inserts, and upserts. This would require some
changes to the manifest to make it efficient, as well as upstream
API additions to Zarr.

### Is Icechunk efficient in the presence of many thousands of nodes?

### Config changes are not tracked
