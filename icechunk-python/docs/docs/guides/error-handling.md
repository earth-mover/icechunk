# Error Handling

All exceptions raised by Icechunk derive from
[`IcechunkError`](../reference/index.md#icechunk.IcechunkError). Subclasses group
failures into categories by what a handler would do about them, and every
instance carries a stable machine-readable `kind` code identifying the
precise failure.

## The exception tree

```
IcechunkError
├── ConflictError
│   └── RebaseFailedError
├── NotFoundError                 (also a KeyError)
│   ├── NodeNotFoundError
│   ├── SnapshotNotFoundError
│   ├── RefNotFoundError
│   └── RepositoryNotFoundError
├── AlreadyExistsError
├── ReadOnlyError
├── InvalidInputError             (also a ValueError)
├── SessionStateError
├── StorageError
├── FormatError
└── InternalError
```

| Exception | Raised when |
|-----------|-------------|
| [`ConflictError`](../reference/index.md#icechunk.ConflictError) | A concurrent writer got there first: commit conflicts, branch update conflicts |
| [`RebaseFailedError`](../reference/index.md#icechunk.RebaseFailedError) | A rebase could not resolve all conflicts; carries `snapshot` and `conflicts` |
| [`NotFoundError`](../reference/index.md#icechunk.NotFoundError) | Something that was asked for does not exist |
| [`NodeNotFoundError`](../reference/index.md#icechunk.NodeNotFoundError) | No group, array, or chunk at the given path |
| [`SnapshotNotFoundError`](../reference/index.md#icechunk.SnapshotNotFoundError) | No snapshot with the given id (or at the given time) |
| [`RefNotFoundError`](../reference/index.md#icechunk.RefNotFoundError) | No branch or tag with the given name |
| [`RepositoryNotFoundError`](../reference/index.md#icechunk.RepositoryNotFoundError) | No Icechunk repository at the given storage location |
| [`AlreadyExistsError`](../reference/index.md#icechunk.AlreadyExistsError) | Something being created already exists (node, branch, tag...) |
| [`ReadOnlyError`](../reference/index.md#icechunk.ReadOnlyError) | A write through a read-only session, store, or repository |
| [`InvalidInputError`](../reference/index.md#icechunk.InvalidInputError) | An argument, key, index, or metadata value is invalid |
| [`SessionStateError`](../reference/index.md#icechunk.SessionStateError) | The operation is not allowed in the current session or repository state |
| [`StorageError`](../reference/index.md#icechunk.StorageError) | The object store or local storage failed, or is misconfigured; often transient |
| [`FormatError`](../reference/index.md#icechunk.FormatError) | On-disk data could not be read or written: corruption, unsupported spec version |
| [`InternalError`](../reference/index.md#icechunk.InternalError) | An unexpected internal error; likely a bug worth [reporting](https://github.com/earth-mover/icechunk/issues) |

## Catching exceptions

Catch a category when your handler treats every failure in it the same way:

```python
import icechunk

try:
    session.commit("append forecast")
except icechunk.ConflictError:
    session.rebase(icechunk.ConflictDetector())
    session.commit("append forecast")
```

```python
try:
    session = repo.readonly_session(tag=name)
except icechunk.NotFoundError:
    session = repo.readonly_session(branch="main")
```

Catch `IcechunkError` when you only want to know "was this Icechunk?":

```python
try:
    ingest(repo)
except icechunk.IcechunkError as e:
    log.error("ingest failed", kind=e.kind, message=str(e))
    raise
```

### Dispatching on `kind`

When a category is too coarse, match the `kind` attribute — an
[`ErrorKind`](#all-error-kinds) code that pins down the exact failure
without needing one class per failure:

```python
try:
    repo.create_tag(name, snapshot_id=snapshot_id)
except icechunk.AlreadyExistsError as e:
    if e.kind == icechunk.ErrorKind.TAG_PREVIOUSLY_DELETED:
        raise RuntimeError(f"tag {name} was deleted and cannot be recreated") from e
    pass  # already tagged: fine, idempotent
```

Codes are append-only: new ones may appear in any release, but existing ones
are never renamed or removed, so it is safe to persist and compare them.

### Compatibility with builtin exceptions

Two categories also derive from the builtin exception you'd expect, so
pre-existing handlers keep working:

- `NotFoundError` is a `KeyError`
- `InvalidInputError` is a `ValueError`

```python
try:
    session = repo.readonly_session(branch="nope")
except KeyError:  # RefNotFoundError is a KeyError
    ...
```

The `KeyError` is most relevant for `zarr`, which internally checks for
it when handling missing keys. `ValueError` is kept for preserving
compatibility with earlier Icechunk releases, which raised it for
invalid input.

## What an error looks like

`str(e)` (also available as `e.message`) is the chain of causes followed by
the operation context — which Icechunk operations were running, with their
arguments:

```python
>>> try:
...     repo.readonly_session(branch="nope")
... except icechunk.IcechunkError as e:
...     print(e)
ref not found `nope`

context:
   0: icechunk::repository::lookup_branch_v2
           with branch="nope" repo_info=Some(RepoInfo { ... })
             at icechunk/src/repository.rs:1240
   1: icechunk::repository::resolve_ref_version_v2
           with version=BranchTipRef("nope")
             at icechunk/src/repository.rs:1690
   2: icechunk::repository::resolve_version_v2
           with version=BranchTipRef("nope")
             at icechunk/src/repository.rs:1772
   3: icechunk::repository::readonly_session
           with version=BranchTipRef("nope")
             at icechunk/src/repository.rs:1925
```

`repr(e)` identifies the class and kind:

```python
>>> repr(e)
'icechunk.RefNotFoundError(message="ref not found `nope`...", kind="ref-not-found")'
```

Uncaught, the traceback shows the exception as usual, plus the full
diagnostic report attached as a
[PEP 678](https://peps.python.org/pep-0678/) note:

```
Traceback (most recent call last):
  File "ingest.py", line 4, in <module>
    repo.readonly_session(branch="nope")
  File ".../icechunk/repository.py", line 1534, in readonly_session
    self._repository.readonly_session(
icechunk._exceptions.RefNotFoundError: ref not found `nope`

context:
   0: icechunk::repository::lookup_branch_v2
   ...
  x ref not found `nope`
  |
  | context:
  |    0: icechunk::repository::lookup_branch_v2
  |    ...
  `-> ref not found `nope`
```

## All error kinds

::: icechunk.ErrorKind
    options:
      show_if_no_docstring: true
      show_source: false
      summary: false
