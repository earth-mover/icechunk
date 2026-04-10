# Breaking Changes in 2.0

This page lists all breaking Python API changes between Icechunk 1.x and 2.0, with before/after examples. For migrating the on-disk format from v1 to v2, see [Migrate to 2.0](migration-2.md). For migrating from pre-1.0, see [Migration guide](migration.md).

## Python Version

Icechunk 2.0 requires **Python 3.12 or later**. Python 3.11 support was dropped ([#1905](https://github.com/earth-mover/icechunk/pull/1905)).

## ChunkType Enum Variants

`ChunkType` variants changed from `UPPER_CASE` to `snake_case` ([#1812](https://github.com/earth-mover/icechunk/pull/1812)):

| 1.x | 2.0 |
|-----|-----|
| `ChunkType.INLINE` | `ChunkType.inline` |
| `ChunkType.NATIVE` | `ChunkType.native` |
| `ChunkType.VIRTUAL` | `ChunkType.virtual` |
| `ChunkType.UNINITIALIZED` | `ChunkType.uninitialized` |

No other existing enums had their variants renamed. `VersionSelection`, `ConflictType`, `CompressionAlgorithm`, `ManifestPreloadCondition`, `ManifestSplitCondition`, and `ManifestSplitDimCondition` are unchanged. `ConflictType` gained one new variant (`MoveOperationCannotBeRebased`) but all existing variant names are the same.

## Module Reorganization

The flat `icechunk` namespace was split into focused submodules ([#1995](https://github.com/earth-mover/icechunk/pull/1995)). **All existing top-level imports continue to work**, so `from icechunk import X` is unaffected.

The new submodule layout is:

| Module | Contents |
|--------|----------|
| `icechunk.config` | `RepositoryConfig`, `CachingConfig`, `CompressionConfig`, `ManifestConfig`, ... |
| `icechunk.conflicts` | `ConflictSolver`, `BasicConflictSolver`, `Conflict`, `ConflictType`, `VersionSelection` |
| `icechunk.credentials` | All credential types and factory functions |
| `icechunk.ops` | [`Update`](ops.md), [`UpdateType`](ops.md), `GCSummary` |
| `icechunk.session` | `Session`, `ForkSession`, `SessionMode` |
| `icechunk.snapshots` | `SnapshotInfo`, `Diff`, `ManifestFileInfo` |
| `icechunk.storage` | `Storage`, `S3Options`, `StorageSettings`, storage factory functions |
| `icechunk.store` | `IcechunkStore` |
| `icechunk.virtual` | `VirtualChunkContainer`, `VirtualChunkSpec` |

## Manifest API Changes

Manifest access was consolidated into a single method on `Repository`. The `manifests` field was removed from `SnapshotInfo`, and `Repository.manifest_files()` was renamed to [`Repository.list_manifest_files()`][icechunk.repository.Repository.list_manifest_files] ([#1428](https://github.com/earth-mover/icechunk/pull/1428)):

=== "2.0"

    ```python
    manifests = repo.list_manifest_files(snapshot_id)
    ```

=== "1.x"

    ```python
    # Via SnapshotInfo (removed in 2.0)
    info = repo.ancestry(branch="main")[0]
    manifests = info.manifests

    # Via Repository method (renamed in 2.0)
    manifests = repo.manifest_files(snapshot_id)
    ```

## inspect_snapshot() Return Type

[`Repository.inspect_snapshot()`][icechunk.repository.Repository.inspect_snapshot] now returns a `dict` instead of a JSON string. The `pretty` parameter was removed.

=== "2.0"

    ```python
    data = repo.inspect_snapshot(snapshot_id)
    # data is a dict — access fields directly
    print(data["message"])
    ```

=== "1.x"

    ```python
    import json
    text = repo.inspect_snapshot(snapshot_id)
    data = json.loads(text)
    print(data["message"])
    ```

## Session.commit() and Session.flush() Signatures

Several parameters became keyword-only and a new `allow_empty` parameter was added ([#1603](https://github.com/earth-mover/icechunk/pull/1603)):

| Method | Now keyword-only | New parameter |
|--------|-----------------|---------------|
| [`Session.commit()`][icechunk.session.Session.commit] | `rebase_with`, `rebase_tries` | `allow_empty=False` |
| [`Session.flush()`][icechunk.session.Session.flush] | `metadata` | — |

=== "2.0"

    ```python
    session.commit("msg", rebase_with=solver, rebase_tries=5)
    session.commit("Metadata-only update", allow_empty=True)
    session.flush("snapshot", metadata={"key": "value"})
    ```

=== "1.x (positional — no longer works)"

    ```python
    session.commit("msg", None, solver, 5)
    session.flush("snapshot", {"key": "value"})
    ```

## Distributed Writes

`ForkSession` was redesigned in 2.0 ([#1876](https://github.com/earth-mover/icechunk/pull/1876)) to allow significantly simpler distributed writes:

- `ForkSession` is now based on an anonymous snapshot that captures uncommitted state. Users need not commit unstaged changes before creating a ForkSession.
- `ForkSession.commit()` raises `IcechunkError` instead of `TypeError`.
- Merge direction is enforced: `Session.merge(ForkSession)` works, but `ForkSession.merge(Session)` does not.

See [Parallel / Distributed Writing](../understanding/parallel.md) for the updated patterns.

## Repr Strings

All Python classes now have structured `__repr__`, `__str__`, and `_repr_html_` output ([#1948](https://github.com/earth-mover/icechunk/pull/1948)). Repr strings now use fully-qualified module paths:

```python
# 1.x
RepositoryConfig(inline_chunk_threshold_bytes=512, ...)

# 2.0
icechunk.config.RepositoryConfig(inline_chunk_threshold_bytes=512, ...)
```

If your code or tests parse repr strings, they will need updating.

## Deprecations

[`list_objects`][icechunk.storage.Storage.list_objects] has been deprecated in favor of [`list_objects_metadata`][icechunk.storage.Storage.list_objects_metadata] which returns the complete info about objects. Now including the `created_at` time.
