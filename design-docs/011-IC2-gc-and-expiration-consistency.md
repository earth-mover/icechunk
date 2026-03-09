# GC and expiration consistency in IC2

In IC1 there was no way to "save" things if a new branch/tag was created while
GC is running. If that branch points to a deletable snapshot, it will become
invalid after GC is done.

In IC2 we have opportunity to avoid this issue. Here is the plan for GC:

* When GC runs it starts by collecting the list  of snapshots, manifests, chunks
that it can delete.
* When that's done it updates repo object removing the snapshots that are not
present in the list of retained snapshots.
* This ensures no readers opening sessions after this point will be able to
"see" those snapshots.
* During the update of repo object, certain situations require the whole GC
process to restart. These situations are:
  * New branches or tags pointing to one of the about-to-be-deleted snapshots
  * New snapshots that have as parent one of the about-to-be-deleted snapshots
* In the above situations, we fully restart the GC process, that will pick up
the changes and limit the deleted objects. We need an option for maximum number
of retries.
* Any new snapshots found during repo object update must be retained.
  * We assume, as in IC1, some level of protection provided by the user not
  passing a recent timestamp to GC. In this way, we ensure new manifests and
  chunks created by these new snapshots, won't be deleted because of the
  timestamp check.
  * For this reason, we don't need to add the new snapshots to the set of
  retained ones for the delete algorithm, they won't be deleted because they are
  new.

The analysis above is also true of expiration, which "soft-deletes" snapshots
and, optionally, deletes branches and tags.
