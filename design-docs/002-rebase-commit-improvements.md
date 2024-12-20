# Bringing Conflict Detection and Rebasing to Python

## Proposal

Currently the process for committing from python looks like this

```python
session = repo.writable_session(branch="main")
store = session.store()

# Do mutable stuff with the store here

commit_id = session.commit("yay")
```

However the `commit` call is not guaranteed to succeed, and an error is thrown if there is a conflict. So we can catch it:

```python
try:
    commit_id = session.commit("yay")
except ConflictError as e:
    print(f"Commit failed: {e}")
```

To resolve this, the `rebase` method was added in the rust layer. This has not yet been added to the python API, but it could look like this:

```python
try:
    commit_id = session.commit("yay")
except ConflictError as e:
    session.rebase(solver=BasicConflictSolver())
    commit_id = session.commit("merged")
```

This is a common pattern that will need to be followed. Two notes on the errors returned for failures in the rust core:

1. When commit fails with a conflict, the error that is returned is `SessionError::Conflict { expected_parent, actual_parent }` , which indicates that the branch tip is not the same as the parent snapshot as the new commit. This indicates that a `rebase` is needed to match up the parents.
2. When rebase fails it will return the error `SessionError::RebaseFailed { snapshot, conflicts }` where the `snapshot` is the parent commit where the conflict arose, and `conflicts` are a list of the conflict reasons encountered.

Currently, none of this messaging is returned to the user in Python. This informs designing an API that gives the best UX for both power users and users that want to minimally deal with this:

1. Create custom python Exceptions that allow for introspecting the commit conflict and the rebase conflict errors programmatically. This also means printing a pretty message with as much relevant info as possible when encountered.

```python
try:
    commit_id = session.commit("yay")
except ConflictError as e:
    # just an example of what we could pull for data, the message would be more informative
    print(f"Commit failed: expected parent: {e.expected_parent}, actual parent: {e.actual_parent}")

...

try:
    session.rebase(solver=BasicConflictSolver())
    commit_id = session.commit("merged")
except RebaseFailedError as e:
    # just an example of what we could pull for data, the message would be more informative
    print(f"Failed to rebase at snapshot {e.snapshot_id} because of conflicts:")
    for conflict in e.conflicts:
        print(f"{c.path}: {c.reason}")
```

2. Create a context manager workflow that will create a scoped session and automatically commit and rebase (if needed) when context is finished (see below).

```python
with repo.open_writable_session("main", message="Update data", solver=BasicConflictSolver()) as session:
	  store = session.store()
	  # Do Stuff with the store

# Session is finished, committed and rebased automatically
```
 
The context manager could handle the loop of trying to commit and rebase iteratively. When it fails, a new error is returned that has the same conflict context 
as the normal `RebaseFailedError`, but with extra access to the session so the changes are recoverable if desired:

```python

try: 
    with repo.open_writable_session("main", message="Update data", solver=BasicConflictSolver(), retries=5) as session:
	    store = session.store()
	    # Do Stuff with the store
except AutomaticRebaseFailedError as e:
    print(f"Rebase failed at snapshot {e.snapshot_id}")

    # We can get the session
    session = e.session

    # or just the conflict info like normal, so we can make changes to the session to fixup the session and try again
    print(f"Found conflicts:")
    for conflict in e.conflicts:
        print(f"{c.path}: {c.reason}")

```
