# Store

## The Big Picture: Zarr Bridge

Zarr libraries (like [zarr-python](https://zarr.readthedocs.io/)) expect a simple key-value store: give me the bytes at `"myarray/c/0/0"`, let me write bytes to `"myarray/zarr.json"`. But Icechunk has [Snapshots](core-types.md#snapshot), [Manifests](core-types.md#manifest), and [typed IDs](core-types.md#content-addressable-ids)—a much richer model.

The `Store` type bridges this gap. It parses Zarr-style string keys, translates them into Icechunk operations on a [`Session`](session.md), and returns the results. This is how Python users interact with Icechunk without needing to know about the underlying architecture.

This document explains how Store works internally—how keys are parsed, how operations translate to Session calls, and what errors can occur.

> **Scope**: This document covers the Zarr key-value interface. For the underlying transaction layer, see [Session & ChangeSet](session.md). For the Python bindings that wrap Store, see the icechunk-python crate.

---

## Store Structure

`Store` (defined in `store.rs`) is backed by a Session with concurrency control:

```rust
#[derive(Clone)]
pub struct Store {
    session: Arc<RwLock<Session>>,
    get_partial_values_concurrency: u16,
}
```

### Why These Fields?

| Field | Purpose |
|-------|---------|
| `session` | The underlying Session, wrapped in RwLock for concurrent access |
| `get_partial_values_concurrency` | Limits concurrent fetches in batch operations |

The `Arc<RwLock<Session>>` pattern allows:
- Multiple concurrent readers (Zarr often reads many chunks in parallel)
- Exclusive access for writes
- Sharing the same Store across async tasks

---

## Key Parsing

Zarr keys are parsed into a `Key` enum that determines which Session operation to call:

```rust
enum Key {
    Metadata { node_path: Path },
    Chunk { node_path: Path, coords: ChunkIndices },
    ZarrV2(String),
}

impl Key {
    const ROOT_KEY: &'static str = "zarr.json";
    const METADATA_SUFFIX: &'static str = "/zarr.json";
    const CHUNK_COORD_PREFIX: &'static str = "c";

    fn parse(key: &str) -> Result<Self, StoreError> {
        // Handles:
        // - "zarr.json" -> root metadata
        // - "group/zarr.json" -> group or array metadata
        // - "array/c/0/1/2" -> chunk at indices [0, 1, 2]
        // - ".zarray", ".zgroup" -> ZarrV2 (legacy)
        ...
    }
}
```

### Key Patterns

| Key Pattern | Parsed As | Example |
|-------------|-----------|---------|
| `zarr.json` | `Key::Metadata { node_path: "/" }` | Root group |
| `{path}/zarr.json` | `Key::Metadata { node_path }` | `group/array/zarr.json` |
| `{path}/c/{i0}/{i1}/...` | `Key::Chunk { node_path, coords }` | `array/c/0/1/2` |
| `.zarray`, `.zgroup`, etc. | `Key::ZarrV2` | Legacy Zarr v2 keys |

The `c/` prefix distinguishes chunks from metadata in the key hierarchy.

---

## Core Operations

### Get (Read)

```rust
impl Store {
    pub async fn get(&self, key: &str, byte_range: &ByteRange) -> StoreResult<Bytes> {
        let repo = self.session.read().await;
        get_key(key, byte_range, &repo).await
    }
}
```

The `byte_range` parameter supports partial reads—useful for reading just part of a large chunk.

### Set (Write)

```rust
impl Store {
    pub async fn set(&self, key: &str, value: Bytes) -> StoreResult<()> {
        if self.read_only().await {
            return Err(StoreErrorKind::ReadOnly.into());
        }

        match Key::parse(key)? {
            Key::Metadata { node_path } => {
                // Parse JSON to determine if it's an array or group
                if let Ok(array_meta) = serde_json::from_slice(value.as_ref()) {
                    self.set_array_meta(node_path, value, array_meta, None).await
                } else {
                    // Try parsing as group metadata
                    ...
                }
            }
            Key::Chunk { node_path, coords } => {
                // Get writer closure (handles inline vs. ref decision)
                let writer = self.session.read().await.get_chunk_writer()?;
                let payload = writer(value).await?;
                self.session.write().await
                    .set_chunk_ref(node_path, coords, Some(payload)).await?;
                Ok(())
            }
            Key::ZarrV2(_) => Err(StoreErrorKind::Unimplemented(
                "Icechunk cannot set Zarr V2 metadata keys",
            ).into()),
        }
    }
}
```

### Why the Two-Phase Chunk Write?

1. **Get writer** with read lock (cheap, allows concurrent operations)
2. **Write bytes** to storage (no lock held—storage I/O can be slow)
3. **Update reference** with write lock (quick metadata update)

This minimizes time spent holding the write lock, improving concurrency.

### Delete

```rust
impl Store {
    pub async fn delete(&self, key: &str) -> StoreResult<()> {
        if self.read_only().await {
            return Err(StoreErrorKind::ReadOnly.into());
        }
        let mut session = self.session.write().await;
        match Key::parse(key)? {
            Key::Metadata { node_path } => {
                session.delete_node(&node_path).await?;
            }
            Key::Chunk { node_path, coords } => {
                session.delete_chunks(&node_path, vec![coords]).await?;
            }
            Key::ZarrV2(_) => {} // No-op for legacy keys
        }
        Ok(())
    }
}
```

### List Directory

```rust
impl Store {
    /// Returns keys and prefixes as strings
    pub async fn list_dir(
        &self,
        prefix: &str,
    ) -> StoreResult<impl Stream<Item = StoreResult<String>>> {
        // Internally uses list_dir_items and converts to strings
        ...
    }

    /// Returns structured items distinguishing keys from prefixes
    pub async fn list_dir_items(
        &self,
        prefix: &str,
    ) -> StoreResult<impl Stream<Item = StoreResult<ListDirItem>>> {
        let guard = self.session.read().await;
        ...
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ListDirItem {
    Key(String),    // A file (metadata or chunk)
    Prefix(String), // A directory (group or array path)
}
```

> **Note**: `list_dir` returns strings for compatibility with Zarr; use `list_dir_items` if you need to distinguish between files and directories.

---

## Virtual References

Store supports setting virtual chunk references that point to external files:

```rust
impl Store {
    pub async fn set_virtual_ref(
        &self,
        key: &str,
        reference: VirtualChunkRef,
        validate_container: bool,
    ) -> StoreResult<()> {
        match Key::parse(key)? {
            Key::Chunk { node_path, coords } => {
                let mut session = self.session.write().await;
                if validate_container
                    && session.matching_container(&reference.location).is_none()
                {
                    return Err(StoreErrorKind::InvalidVirtualChunkContainer {
                        chunk_location: reference.location.url().to_string(),
                    }.into());
                }
                session.set_chunk_ref(
                    node_path,
                    coords,
                    Some(ChunkPayload::Virtual(reference)),
                ).await?;
                Ok(())
            }
            _ => Err(StoreErrorKind::NotAllowed(...).into()),
        }
    }
}
```

The `validate_container` flag checks that a matching [VirtualChunkContainer](storage.md#virtual-chunk-resolution) is configured before accepting the reference.

---

## Batch Operations

For performance, Store provides batch fetch:

```rust
impl Store {
    pub async fn get_partial_values(
        &self,
        key_ranges: impl IntoIterator<Item = (String, ByteRange)>,
    ) -> StoreResult<Vec<StoreResult<Bytes>>> {
        // Uses Stream::for_each_concurrent for parallel fetches
        // Concurrency limited by get_partial_values_concurrency
        ...
    }
}
```

This uses concurrent (not parallel) execution on a single thread—sufficient for I/O-bound workloads where most time is spent waiting for storage responses.

---

## Store Errors

```rust
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum StoreErrorKind {
    // Wrapped errors from lower layers
    #[error(transparent)]
    SessionError(SessionErrorKind),
    #[error(transparent)]
    RepositoryError(RepositoryErrorKind),
    #[error(transparent)]
    RefError(RefErrorKind),

    // Key parsing errors
    #[error("invalid zarr key format `{key}`")]
    InvalidKey { key: String },
    #[error("bad key prefix ({prefix}): {message}")]
    BadKeyPrefix { prefix: String, message: String },

    // Not found errors
    #[error(transparent)]
    NotFound(#[from] KeyNotFoundError),

    // Operation errors
    #[error("this operation is not allowed: {0}")]
    NotAllowed(String),
    #[error("store method `{0}` is not implemented by Icechunk")]
    Unimplemented(&'static str),
    #[error("cannot write to read-only store")]
    ReadOnly,
    #[error("uncommitted changes in repository, commit changes or reset repository and try again.")]
    UncommittedChanges,

    // Commit errors
    #[error("cannot commit when no snapshot is present")]
    NoSnapshot,
    #[error("all commits must be made on a branch")]
    NotOnBranch,
    #[error("error merging stores: `{0}`")]
    MergeError(String),

    // Virtual reference errors
    #[error("invalid chunk location, no matching virtual chunk container: `{chunk_location}`")]
    InvalidVirtualChunkContainer { chunk_location: String },

    // Serialization errors
    #[error("bad metadata")]
    BadMetadata(#[from] serde_json::Error),
    #[error("deserialization error")]
    DeserializationError(#[from] Box<rmp_serde::decode::Error>),
    #[error("serialization error")]
    SerializationError(#[from] Box<rmp_serde::encode::Error>),
    #[error("could not create path from prefix")]
    PathError(#[from] PathError),

    // Concurrent operation errors
    #[error("error during parallel execution of get_partial_values")]
    PartialValuesPanic,

    // Generic errors
    #[error("{0}")]
    Other(String),
    #[error("unknown store error")]
    Unknown(Box<dyn std::error::Error + Send + Sync>),
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum KeyNotFoundError {
    #[error("chunk cannot be find for key `{key}` (path={path}, coords={coords:?})")]
    ChunkNotFound { key: String, path: Path, coords: ChunkIndices },
    #[error("node not found at `{path}`")]
    NodeNotFound { path: Path },
    #[error("v2 key not found at `{key}`")]
    ZarrV2KeyNotFound { key: String },
}
```

### Error Wrapping

Store errors wrap lower-layer errors while adding Zarr-specific context. The `#[non_exhaustive]` attribute allows adding new variants without breaking downstream code.

---

## Store vs Session

| Aspect | Store | Session |
|--------|-------|---------|
| **Interface** | String keys (`"array/c/0/0"`) | Typed methods (`get_chunk(&path, &coords)`) |
| **Primary users** | Python bindings, Zarr libraries | Internal Rust code |
| **Key parsing** | Handles Zarr key formats | Expects pre-parsed input |
| **Concurrency** | Built-in RwLock for safe sharing | Not Sync—wrap in `Arc<RwLock<Session>>` for sharing |
| **Abstraction level** | Higher (key-value semantics) | Lower (typed operations) |

For internal Rust code, prefer using `Session` directly—it's more type-safe and avoids parsing overhead.

---

## Serialization

Store can be serialized for persistence (e.g., saving session state):

```rust
impl Store {
    pub fn from_bytes(bytes: Bytes) -> StoreResult<Self> {
        let session: Session = rmp_serde::from_slice(&bytes)?;
        Ok(Self::from_session_and_config(
            Arc::new(RwLock::new(session)),
            session.config().get_partial_values_concurrency(),
        ))
    }

    pub async fn as_bytes(&self) -> StoreResult<Bytes> {
        let session = self.session.write().await;
        let bytes = rmp_serde::to_vec(session.deref())?;
        Ok(Bytes::from(bytes))
    }
}
```

This uses MessagePack serialization, which is more compact than JSON for binary data.

---

## Next Steps

- [Session & ChangeSet](session.md) - The underlying transaction layer
- [Storage Backends](storage.md) - How data is persisted
- [Architecture](architecture.md) - How the layers fit together
