# Repository

## The Big Picture: Entry Point

The **Repository** is where everything starts. It's the handle you use to access a versioned collection of arrays and groups—similar to how a Git repository handle lets you access commits, branches, and files.

But `Repository` itself is lightweight. It doesn't hold [Snapshot](core-types.md#snapshot) data or track modifications—that's what [Sessions](session.md) are for. Think of `Repository` as the factory that creates Sessions, plus the manager for branches and tags.

This document explains the `Repository` type and its supporting types: how versions are specified, how branches and tags work, and how configuration flows through the system.

> **Scope**: This document covers the `Repository` handle and version management. For the transaction layer where reading and writing happens, see [Session & ChangeSet](session.md). For the data structures that get persisted, see [Core Types](core-types.md).

---

## Version Specification

Before diving into Repository itself, we need to understand how versions are specified—this is fundamental to how Repository creates Sessions.

### VersionInfo

When you want to access data at a specific version, you use `VersionInfo` to specify which one:

```rust
pub enum VersionInfo {
    SnapshotId(SnapshotId),    // Direct reference to a specific commit
    TagRef(String),            // Named tag like "v1.0"
    BranchTipRef(String),      // Latest commit on a branch like "main"
    AsOf { branch: String, at: DateTime<Utc> },  // Point-in-time on a branch
}
```

| Variant | Resolves To | Use Case |
|---------|-------------|----------|
| `SnapshotId` | The exact snapshot | Reproducible access to a known version |
| `TagRef` | Snapshot the tag points to | Accessing releases or milestones |
| `BranchTipRef` | Current tip of the branch | Working with latest data |
| `AsOf` | Snapshot at or before the timestamp | Time-travel queries |

**Resolution** happens when creating a Session:

- **SnapshotId**: Used directly (no resolution needed)
- **TagRef**: Read `refs/tag.{name}/ref.json` → extract snapshot ID
- **BranchTipRef**: Read `refs/branch.{name}/ref.json` → extract snapshot ID
- **AsOf**: Walk snapshot ancestry until finding one where `flushed_at <= at`

---

## Refs: Branches and Tags

Refs are named pointers to [Snapshots](core-types.md#snapshot). They make it possible to refer to versions by human-readable names instead of random IDs.

### Branches

A branch tracks a line of work. When you commit, the branch moves to point to your new snapshot.

**Storage**: `refs/branch.{name}/ref.json`

**Key properties**:
- Mutable—updated on each commit to that branch
- The `main` branch is created automatically and cannot be deleted
- Updates use **conditional writes** (ETags) for [conflict detection](conflicts.md)

### Tags

A tag permanently marks a specific snapshot—like a release version.

**Storage**: `refs/tag.{name}/ref.json`

**Key properties**:
- Immutable—once created, always points to the same snapshot
- Cannot be overwritten (creation fails if tag exists)
- Deletion writes a **tombstone** file that prevents recreation

### Why Tombstones?

When a tag is deleted, Icechunk writes a tombstone marker instead of just removing the file. This prevents a subtle bug: if tag "v1.0" is deleted and later recreated pointing to a different snapshot, anyone with a cached reference to the old "v1.0" would silently get wrong data. The tombstone ensures the tag name can never be reused.

---

## The Repository Handle

`Repository` (defined in `repository.rs`) is the entry point for all operations. It holds shared resources that are used across all Sessions created from it.

```rust
pub struct Repository {
    spec_version: SpecVersionBin,
    config: RepositoryConfig,
    storage_settings: storage::Settings,
    config_version: storage::VersionInfo,
    storage: Arc<dyn Storage + Send + Sync>,
    asset_manager: Arc<AssetManager>,
    virtual_resolver: Arc<VirtualChunkResolver>,
    authorized_virtual_containers: HashMap<String, Option<Credentials>>,
    default_commit_metadata: SnapshotProperties,
}
```

### Why These Fields?

| Field | Purpose |
|-------|---------|
| `spec_version` | Format version (v1.x vs v2.0+) for compatibility |
| `config` | User-provided settings (inline threshold, concurrency, etc.) |
| `storage_settings` | Backend-specific settings (compression, chunking) |
| `storage` | The actual storage backend ([S3, local, etc.](storage.md)) |
| `asset_manager` | **Shared cache** for snapshots, manifests, chunks |
| `virtual_resolver` | Resolves references to external files (HDF5, NetCDF) |
| `authorized_virtual_containers` | Credentials for accessing virtual chunk sources |
| `default_commit_metadata` | Properties added to every commit |

The `Arc` wrappers enable sharing across threads and Sessions. When you clone a Repository or create a Session from it, they share the same `AssetManager` cache.

### Creation vs Opening

**Creating** a new repository:
1. Validates storage is writable and empty
2. Writes the initial empty [Snapshot](core-types.md#snapshot)
3. Creates the `main` branch (or `RepoInfo` for v2.0+)

**Opening** an existing repository:
1. Reads saved configuration from storage
2. Validates the repository exists (checks for `main` branch or `RepoInfo`)
3. Sets up the `AssetManager` with cached config

The key difference: `create` fails if the storage isn't empty; `open` fails if there's no existing repository.

---

## Configuration

`RepositoryConfig` (defined in `config.rs`) controls behavior across all Sessions:

```rust
pub struct RepositoryConfig {
    /// Chunks ≤ this size are embedded in manifests (default: 512 bytes)
    pub inline_chunk_threshold_bytes: Option<u16>,

    /// Parallel fetches for get_partial_values
    pub get_partial_values_concurrency: Option<u16>,

    /// Compression for snapshots/manifests
    pub compression: Option<CompressionConfig>,

    /// Max concurrent storage requests (default: 256)
    pub max_concurrent_requests: Option<u16>,

    /// Cache sizes and policies
    pub caching: Option<CachingConfig>,

    /// Storage backend settings
    pub storage: Option<storage::Settings>,

    /// Virtual chunk container configurations
    pub virtual_chunk_containers: Option<HashMap<String, VirtualChunkContainer>>,

    /// Manifest splitting configuration
    pub manifest: Option<ManifestConfig>,

    /// Migration support
    pub previous_file: Option<String>,
}
```

All fields are `Option` because config is merged with defaults—you only specify what you want to override.

### Inline Chunk Threshold

The `inline_chunk_threshold_bytes` setting (default 512) determines whether chunks are:
- **Stored separately** in `chunks/{ChunkId}` (large chunks)
- **Embedded in the manifest** as `ChunkPayload::Inline` (small chunks)

Small chunks benefit from embedding because it avoids a storage round-trip. This is especially useful for coordinate arrays which are often tiny.

---

## Error Types

```rust
pub enum RepositoryErrorKind {
    // Wrapped errors from lower layers
    StorageError(StorageErrorKind),
    FormatError(IcechunkFormatErrorKind),
    Ref(RefErrorKind),

    // Version/ref errors
    SnapshotNotFound { id: SnapshotId },
    InvalidAsOfSpec { branch: String, at: DateTime<Utc> },
    InvalidSnapshotId(String),
    Tag(String),

    // Repository state errors
    ParentDirectoryNotClean,
    RepositoryDoesntExist,
    ConfigWasUpdated,
    RepoInfoUpdated,
    CannotDeleteMain,
    ReadonlyStorage(String),
    BadRepoVersion { minimum_spec_version: SpecVersionBin },

    // Conflict errors
    Conflict { expected_parent: Option<SnapshotId>, actual_parent: Option<SnapshotId> },
    ConflictingPathNotFound(NodeId),

    // Operational errors
    NoAmendForInitialCommit,
    RepoUpdateAttemptsLimit(u64),
    SerializationError(Box<rmp_serde::encode::Error>),
    DeserializationError(Box<rmp_serde::decode::Error>),
    ConfigDeserializationError(serde_yaml_ng::Error),
    IOError(std::io::Error),
    ConcurrencyError(JoinError),
    AcquireError(AcquireError),
    Other(String),
}
```

The most important ones for understanding the system:

- **`Conflict`**: Two writers tried to update the same branch—see [Conflict Resolution](conflicts.md)
- **`RepositoryDoesntExist`**: `open()` called on empty storage
- **`ParentDirectoryNotClean`**: `create()` called on non-empty storage
- **`ReadonlyStorage`**: Write operation attempted on read-only backend

---

## Next Steps

- [Session & ChangeSet](session.md) - Where reading and writing actually happens
- [Core Types](core-types.md) - The data structures that get persisted
- [Conflict Resolution](conflicts.md) - How concurrent writes are handled
