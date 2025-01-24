# Deleting Tags

We like our tags immutable. That gives Icechunk users a lot of power.
For example, they can always cache anything coming out
from an Icechunk session initiated on a tag:
```python
repo = icechunk.Repository.open(...)
session = repo.readonly_session(tag="v42")
store = session.store
```

All data read from `store` will be immutable, it can be cached for as
long as needed. This is only true if tags cannot be edited. If
somebody can edit `v42` and move it to a different snapshot,
there are no longer any guarantees.

## The problems

* If a user makes a spelling mistake, or they assign a tag to the wrong
snapshot, they cannot fix it. This is a minor problem, and we made
an explicit trade-off here. The benefits justify this minor inconvenience.
The will have to create a new, different tag.
* In a world in which we want to expire on squash history, tags will have to
disappear. If a tag is pointing to a snapshot the user wants to expire,
we somehow need to delete that tag. Otherwise, it would either block the
expiration, or become dangling pointer.

## Tag delete

We established we want to allow tag delete:

* Will tag delete break Icechunk? No. Tag resolution is a point-in-time event.
Tags are used to resolve a snapshot, for example when creating a
read only session. If the tag disappears before it's resolved, the
user won't be able to get to the snapshot (good). If the tag disappears
after the user has resolved the snapshot, they can continue to operate
without having to resolve the tag again.
* This is **the problematic flow**:
  1. Create tag `foo` pointing to snapshot `ABC123`
  1. Delete tag `foo`
  1. Create tag `foo` pointing to snapshot `DEF456`

  This is essentially tag edit, which we explicitly want to avoid, as
  explained before.

## Design

We implement logic delete for tags. Tags will be flagged
as deleted and they won't resolve. But new tags won't be able to
use the name of a deleted tag.

The current on-disk structure for a tag is the file:

```
refs/tag.$TAG_NAME/ref.json

```

The contents of that file is a JSON document such as

```json
{snapshot: "ABC123"}

```

When a tag is deleted we create a new empty document:

```
refs/tag.$TAG_NAME/ref.json.deleted
```

This automatically gives us the behavior of not allowing new
tags to have the same name as a deleted tag.

When it's time to resolve a tag `foo`, we fetch in parallel two keys:

```
refs/tag.foo/ref.json
refs/tag.foo/ref.json.deleted
```

And wait for both to resolve. If `ref.json.deleted` is not found, we use
the contents of `ref.json` to find the snapshot. If `ref.json.deleted`
is found we carry on as if `ref.json` was not found.

### Optional improvement

On tag creation, we may chose to offer different error messages
when the tag already exists and when it was deleted. They both
will be blocked, but if we do nothing, the "tag already exists"
error message could be confusing.
