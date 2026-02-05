# Architecture

## The Big Picture: Layered Design

Icechunk separates concerns into three abstraction layers.

This document explains these layers and how data flows through them.

> **Scope**: This document provides a high-level map of how components relate. For details on each component, see the individual pages: [Repository](repository.md), [Session](session.md), [Store](store.md), [Core Types](core-types.md), [Storage](storage.md).

### 1. High-Level API: Repository

The [`Repository`](repository.md) type (in `repository.rs`) is the entry point for version control operations:

- Opening/creating repositories
- Working with branches and tags (see [Refs](core-types.md#refs-branches-and-tags))
- Resolving version specifiers (`VersionInfo`)
- Creating [Sessions](#2-transaction-layer-session--changeset) for reading or writing

`Repository` doesn't directly handle data—it delegates to `Session` for all read/write operations.

### 2. Transaction Layer: Session + ChangeSet

The [`Session`](session.md) type (in `session.rs`) maintains:

- A reference to the base [Snapshot](core-types.md#snapshot) being read or modified
- A [`ChangeSet`](session.md#changeset) that accumulates all modifications
- Methods for reading/writing arrays, groups, and chunks

The `ChangeSet` (in `change_set.rs`) is a data structure that tracks:

- New groups and arrays created
- Updated array metadata
- Chunks written, updated, or deleted (as [ChunkPayloads](core-types.md#chunkpayload-aka-chunk-reference))
- Move/rename operations

When a session commits, the `ChangeSet` is serialized into a new [Snapshot](core-types.md#snapshot) and [TransactionLog](core-types.md#transactionlog).

### 3. Zarr Interface: Store

The [`Store`](store.md) type (in `store.rs`) provides a key-value interface that Zarr libraries expect:

- `get(key)` / `set(key, value)` for zarr keys like `"myarray/c/0/0"`
- `list_dir(prefix)` for directory listing
- Key parsing to translate Zarr [Paths](core-types.md#path) and [ChunkIndices](core-types.md#chunkindices) to Icechunk operations

Store is backed by a [`Session`](session.md) and translates string keys to typed method calls.

## Data Flow

### Read Path

<!-- TODO: Simplify this diagram
```mermaid
sequenceDiagram
    participant User
    participant Store
    participant Session
    participant ChangeSet
    participant AssetManager
    participant Storage

    User->>Store: get("array/c/0/0")
    Store->>Session: get_chunk(path, indices)
    Session->>Session: Resolve path to NodeId

    Session->>ChangeSet: Check for uncommitted chunk
    alt Chunk in ChangeSet (uncommitted write)
        ChangeSet-->>Session: ChunkPayload
        alt Inline payload
            Session-->>Store: Return inline bytes directly
        else Ref payload (already written to storage)
            Session->>Storage: Fetch by ChunkId
            Storage-->>Session: Bytes
            Session-->>Store: Chunk data
        end
    else Chunk not in ChangeSet
        Session->>Session: Look up chunk in base snapshot's manifests
        Session->>AssetManager: fetch_chunk(chunk_ref)
        AssetManager->>AssetManager: Check cache
        alt Cache hit
            AssetManager-->>Session: Cached bytes
        else Cache miss
            AssetManager->>Storage: get_object(chunk_id)
            Storage-->>AssetManager: Bytes from S3/disk
            AssetManager->>AssetManager: Cache result
            AssetManager-->>Session: Bytes
        end
        Session-->>Store: Chunk data
    end
    Store-->>User: Bytes
```
-->

Key points:

- **ChangeSet is checked first** for uncommitted writes in the current session
- **Inline chunks** return data directly from memory (no storage fetch)
- **Ref chunks** (uncommitted but already written) require a storage fetch
- **Committed chunks** are looked up via the base snapshot's manifests, with caching

### Write Path

<!-- TODO: Simplify this diagram
```mermaid
sequenceDiagram
    participant User
    participant Store
    participant Session
    participant ChangeSet
    participant Storage

    User->>Store: set("array/c/0/0", data)
    Store->>Session: get_chunk_writer()
    Session-->>Store: writer closure

    alt Large chunk (> 512 bytes)
        Note over Store: Generate random ChunkId
        Store->>Storage: writer(data) writes immediately
        Storage-->>Store: Confirm write
        Store->>Session: set_chunk_ref(path, coords, Ref{id, offset, len})
    else Small chunk (≤ 512 bytes)
        Store->>Session: set_chunk_ref(path, coords, Inline{data})
    end

    Session->>ChangeSet: Record ChunkPayload

    Note over User,Storage: More set_chunk calls...

    User->>Store: commit()
    Store->>Session: commit()
    Session->>ChangeSet: Read all new/updated arrays
    Session->>ChangeSet: Read all chunk payloads
    Note over Session: Generate random ManifestIds
    Session->>Session: Build Manifests (inline chunks embedded here)
    Session->>Storage: Write manifest files
    Note over Session: Generate random SnapshotId
    Session->>ChangeSet: Read all changes for TransactionLog
    Session->>Session: Build Snapshot + TransactionLog
    Session->>Storage: Write snapshot file
    Session->>Storage: Write transaction log file
    Session->>Storage: Update branch ref (atomic conditional write)
    Session-->>Store: New SnapshotId
    Store-->>User: Commit successful
```
-->

Key points:

- **Object IDs** (`ChunkId`, `ManifestId`, `SnapshotId`): randomly generated when the object is created
- **Large chunks** (> `inline_chunk_threshold_bytes`, default 512): written to storage immediately during `set_chunk`
- **Small chunks** (≤ threshold): stored in `ChangeSet`, embedded in manifest files at commit time
- **Commit reads from ChangeSet** to build manifests, snapshot, and transaction log
- **Transaction log** records what changed, written alongside the snapshot

### Commit with Conflict

<!-- TODO: Simplify this diagram
```mermaid
sequenceDiagram
    participant Session
    participant Storage
    participant ConflictSolver

    Session->>Storage: Conditional update branch ref
    Storage-->>Session: Conflict! (branch moved)
    Session->>Storage: Fetch new snapshot
    Session->>Session: Load TransactionLog
    Session->>ConflictSolver: solve(previous_log, current_changes)
    alt Resolvable
        ConflictSolver-->>Session: Patched ChangeSet
        Session->>Session: Retry commit with patched changes
    else Unsolvable
        ConflictSolver-->>Session: Conflict list
        Session-->>Session: Return error to user
    end
```
-->

## Module Dependencies

The dependency graph flows downward (higher modules depend on lower ones):

```
┌─────────────────────────────────────────────┐
│                 repository.rs               │
│            (version control API)            │
└─────────────────────┬───────────────────────┘
                      │
┌─────────────────────▼───────────────────────┐
│     session.rs          store.rs            │
│   (transactions)    (zarr interface)        │
└────────┬────────────────────┬───────────────┘
         │                    │
┌────────▼────────┐  ┌────────▼────────┐
│  change_set.rs  │  │ asset_manager.rs│
│ (modifications) │  │    (caching)    │
└─────────────────┘  └────────┬────────┘
                              │
         ┌────────────────────▼───────────────┐
         │    format/              storage/   │
         │  (serialization)    (I/O backends) │
         └────────────────────────────────────┘
```

> **Note**: `change_set.rs` and `asset_manager.rs` are parallel components both used by `session.rs`—`ChangeSet` tracks modifications while `AssetManager` handles cached reads.

## Key Relationships

| Component | Owns/Contains | Used By |
|-----------|---------------|---------|
| `Repository` | Config, storage ref | External callers, `Store` |
| `Session` | `ChangeSet`, snapshot ref, `AssetManager` | `Store`, `Repository` |
| `ChangeSet` | In-memory modifications | `Session` |
| `Store` | `Session` (wrapped) | Python bindings |
| `AssetManager` | Caches, `Storage` ref | `Session` |
| `Storage` | Backend connection | `AssetManager` |

## Thread Safety

- `Repository` is [`Clone`](https://doc.rust-lang.org/std/clone/trait.Clone.html) and can be shared across threads
- `Session` is [`!Sync`](https://doc.rust-lang.org/nomicon/send-and-sync.html)—each thread/task should have its own session
- `AssetManager` is internally synchronized with [`Arc`](https://doc.rust-lang.org/std/sync/struct.Arc.html) and uses async-aware caches
- `Storage` implementations are [`Send + Sync`](https://doc.rust-lang.org/nomicon/send-and-sync.html)

## Error Handling

Each layer defines its own error kind enum:

- `RepositoryErrorKind` - version control errors
- `SessionErrorKind` - transaction errors (wraps lower errors)
- `StoreErrorKind` - zarr interface errors (wraps session errors)
- `StorageErrorKind` - I/O errors
- `IcechunkFormatErrorKind` - serialization errors

All errors are wrapped in `ICError<E>` which captures a [`SpanTrace`](https://docs.rs/tracing-error/latest/tracing_error/struct.SpanTrace.html) from the [`tracing`](https://docs.rs/tracing/latest/tracing/) crate for debugging async call stacks.

## Next Steps

- [Core Types](core-types.md) - Understanding Snapshots, Manifests, and IDs
- [Session & ChangeSet](session.md) - Deep dive into the transaction layer
