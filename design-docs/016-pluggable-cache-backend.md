# Proposal: Pluggable Cache Backend for Icechunk

## Motivation

Icechunk currently uses in-memory LRU caches (`quick_cache`) in `AssetManager` for snapshots, manifests, transactions, and chunks. This works well for long-lived processes but falls short in serverless environments where process memory is ephemeral. We need to support external cache backends (Redis, disk, dynamo, object storage, etc.) that persist across invocations, while keeping the in-memory backend as the default.

## Design

### 1. Crate Layout

Following the same pattern as storage (`icechunk-storage` for the trait, `icechunk-s3` / `icechunk-arrow-object-store` for backends), caching is split into separate crates:

| Crate | Contents |
|---|---|
| `icechunk-cache` | `CacheBackend` trait, error types, `InMemoryCache` |
| `icechunk-cache-redis` | `RedisCache` implementation (optional, behind feature flag) |
| `icechunk-cache-disk` | (future) `DiskCache` implementation |

The main `icechunk` crate depends on `icechunk-cache` unconditionally and on backend crates as optional deps gated behind feature flags, mirroring how `s3 = ["dep:icechunk-s3"]` works today. It re-exports everything through `icechunk/src/cache/mod.rs`.

Workspace `Cargo.toml` adds the new members:

```toml
members = [
  # ... existing ...
  "icechunk-cache",
  "icechunk-cache-redis",
]
```

Main `icechunk/Cargo.toml`:

```toml
[dependencies]
icechunk-cache.workspace = true
icechunk-cache-redis = { workspace = true, optional = true }

[features]
default = [
  # ... existing ...
  "cache-redis",
]
cache-redis = ["dep:icechunk-cache-redis"]
```

### 2. `CacheBackend` Trait (`icechunk-cache`)

A new trait abstracts over cache storage. Since external backends require serialized data, **all backends operate on bytes**. Deserialization happens in `AssetManager` on every cache hit.

The trait uses the sealed pattern (from `icechunk-types`) to prevent downstream implementations, and `typetag` for polymorphic serialization — identical to `Storage`.

```rust
// icechunk-cache/src/lib.rs

#[async_trait]
#[typetag::serde(tag = "type")]
pub trait CacheBackend: sealed::Sealed + Send + Sync + Debug {
    /// Get a value by key. Returns None on miss.
    async fn get(&self, key: &str) -> Result<Option<Bytes>, CacheError>;

    /// Insert a value. Weight is a hint for eviction (byte count, node count, etc.)
    async fn insert(&self, key: &str, value: Bytes, weight: u64) -> Result<(), CacheError>;

    /// Remove a specific entry.
    async fn remove(&self, key: &str) -> Result<(), CacheError>;

    /// Clear all entries with a given prefix (e.g., "chunk:" or "manifest:").
    async fn clear_prefix(&self, prefix: &str) -> Result<(), CacheError>;
}
```

Key encoding: `AssetManager` constructs string keys like `"snapshot:{id}"`, `"manifest:{id}"`, `"chunk:{id}:{start}-{end}"`, `"tx:{id}"`. This keeps the trait simple and backend-agnostic. Since all object IDs are already cryptographically secure random identifiers, keys are naturally unique across repositories — no repo-level namespace prefix is needed. This means a shared backend (e.g., a single Redis instance) transparently serves multiple repos, and repos sharing the same underlying storage get free cross-repo cache hits for identical content.

`icechunk-cache` depends only on `icechunk-types`, `async-trait`, `bytes`, `serde`, `typetag`, and `quick_cache` (for `InMemoryCache`). It has no storage or format dependencies.

### 3. Built-in Implementations (`icechunk-cache`)

**InMemoryCache** (default) — wraps `quick_cache` as today. Config: max capacity per cache category (reuses existing `CachingConfig` knobs). Single `Cache<String, Bytes, ...>` under the hood, with weight-based eviction.

```rust
#[derive(Debug, Serialize, Deserialize)]
pub struct InMemoryCache { /* quick_cache::Cache */ }

impl sealed::Sealed for InMemoryCache {}

#[async_trait]
#[typetag::serde]
impl CacheBackend for InMemoryCache { ... }
```

To disable caching entirely (e.g., write-heavy workloads, benchmarking), set all `CachingConfig` fields to `0` — `AssetManager` skips cache calls before they reach the backend, so no separate `NoCache` type is needed.

### 4. External Backend Implementations

**RedisCache** (`icechunk-cache-redis`) — uses `redis` crate with async connection pool. Config: connection URL, optional TTL, optional max memory hint. Lives in its own crate so the `redis` dependency is only pulled in when the `cache-redis` feature is enabled.

**DiskCache** (`icechunk-cache-disk`, future) — local disk-backed cache using a path on a local filesystem. Config: directory path, max size.

### 5. Re-exports (`icechunk/src/cache/mod.rs`)

```rust
// Always available
pub use icechunk_cache::{CacheBackend, CacheError, InMemoryCache};

// Feature-gated
#[cfg(feature = "cache-redis")]
pub use icechunk_cache_redis::RedisCache;
```

### 6. `CachingConfig` — Current State and Evolution

The cache backend is passed as a separate argument to `Repository::open()` / `Repository::create()`, which forwards it to `AssetManager`. It never enters `CachingConfig` or any serialized config. If no backend is provided, `AssetManager` defaults to `InMemoryCache`.

**Current `CachingConfig`** controls capacity/enablement per category and is part of the persisted `RepositoryConfig`:

```rust
pub struct CachingConfig {
    pub num_snapshot_nodes: Option<u64>,      // default: 500,000
    pub num_chunk_refs: Option<u64>,          // default: 15,000,000
    pub num_transaction_changes: Option<u64>, // default: 0 (disabled)
    pub num_bytes_attributes: Option<u64>,    // default: 0 (disabled)
    pub num_bytes_chunks: Option<u64>,        // default: 0 (disabled)
}
```

These fields serve two purposes today:
1. **Enablement gate** — a value of `0` means "don't cache this category at all"
2. **Capacity limit** — for `InMemoryCache`, directly configures `quick_cache` weighted capacity

**How it works with pluggable backends:**

The enablement gate (purpose 1) applies universally — `AssetManager` checks these before calling the backend. If `num_bytes_chunks` is `0`, no chunk cache calls are made regardless of backend.

**Should `CachingConfig` remain in the persisted repo config?**

Each backend has fundamentally different eviction semantics:
- **InMemoryCache**: weighted LRU with capacity limits (node counts, ref counts, bytes)
- **Redis**: `maxmemory` + server-side eviction policy, TTLs
- **DynamoDB**: TTL-based expiration, no eviction
- **Disk**: size quota, manual cleanup

Beyond backend differences, the same repo accessed by different workloads has completely different caching needs. A **read-heavy workload** (e.g., analytical queries scanning many chunks) benefits from a large chunk cache and aggressive manifest caching. A **write-heavy workload** (e.g., an ingestion pipeline) performance is hit by caching so often and will want to turn off the cache. Today these are the same config because they're stored in the repo, but they can be overwritten at runtime.

This suggests `CachingConfig` should **move out of the persisted `RepositoryConfig`** and become a runtime-only argument, just like the cache backend itself. The cache backend and its tuning are properties of the *deployment environment and workload*, not of the repository.

**Backend-specific configuration:**

Each `CacheBackend` implementation carries its own config as struct fields (connection URL, TTL, capacity limits, etc.), following the same pattern as storage backends. `InMemoryCache` holds capacity limits; `RedisCache` holds connection URL and TTL. No shared `Settings` struct is needed — the trait itself is the interface, and each backend's serializable struct is the config.

### 7. `AssetManager` Changes

The four `quick_cache::Cache` fields are replaced by a single `Arc<dyn CacheBackend>`. The fetch methods change from:

```rust
// Before
match self.manifest_cache.get_value_or_guard_async(id).await {
    Ok(manifest) => Ok(manifest),
    Err(guard) => {
        let manifest = self.fetch_and_deserialize(id).await?;
        guard.insert(Arc::clone(&manifest));
        Ok(manifest)
    }
}
```

To:

```rust
// After
let key = format!("manifest:{id}");
if let Some(bytes) = self.cache.get(&key).await? {
    return Ok(Arc::new(Manifest::deserialize(&bytes)?));
}
let (bytes, manifest) = self.fetch_and_deserialize(id).await?;
self.cache.insert(&key, bytes, manifest.len() as u64).await?;
Ok(manifest)
```

The capacity fields in `CachingConfig` still gate whether a category is cached at all (if `num_chunk_refs == Some(0)`, skip the cache call entirely). For `InMemoryCache`, they directly configure the underlying `quick_cache` capacity. For external backends, they're passed as weight hints.

**Concurrent fill protection**: The current `get_value_or_guard_async` pattern prevents thundering herd on cache misses. With the trait abstraction, we lose this. To compensate, `AssetManager` keeps a lightweight `DashMap<String, Arc<Notify>>` of in-flight fetches. On miss, the first caller inserts a notify, fetches, caches, and notifies. Subsequent callers for the same key await the notification then read from cache.

### 8. Python API

```python
import icechunk

# Default in-memory (no change from today)
repo = icechunk.Repository.open(storage=s3_storage)

# Explicit in-memory with custom sizing
repo = icechunk.Repository.open(
    storage=s3_storage,
    cache=icechunk.InMemoryCache(),
    caching_config=icechunk.CachingConfig(num_chunk_refs=30_000_000),
)

# Redis backend
repo = icechunk.Repository.open(
    storage=s3_storage,
    cache=icechunk.RedisCache(url="redis://localhost:6379", ttl_seconds=3600),
    caching_config=icechunk.CachingConfig(num_bytes_chunks=1_000_000_000),
)
```

`cache` is an optional kwarg on `open()` and `create()`. `CachingConfig` remains separate since it controls category enablement regardless of backend.

### 9. What Doesn't Change

- **Repo info cache** — stays as a simple `RwLock<Option<...>>` in `AssetManager`. It's a single conditional-fetch entry, not worth abstracting.
- **Snapshot node sub-cache** — stays in `Snapshot`. It's a tiny (2-entry) decode cache internal to a single object.
- **Virtual chunk fetcher cache** — stays in-memory. It caches client objects, not serializable data.
- **Settings/persistence** — cache backend is never written to repository config.

## Trade-offs

**Cost of serialization on in-memory hit**: Deserializing on every cache hit is slower than returning `Arc<T>` directly. This is the main regression for the in-memory path. Mitigation options for later discussion:
- A two-tier approach where `InMemoryCache` stores deserialized objects and bypasses the trait
- An `Arc<Bytes>` + lazy deserialization cache in `AssetManager` on top of the backend

**Loss of typed keys**: Moving to string keys loses compile-time type safety. Mitigation: key construction is centralized in `AssetManager` helper methods, not spread across the codebase.

**Async trait overhead**: `CacheBackend` is async even for in-memory. For `InMemoryCache`, the async overhead is negligible since `quick_cache` operations are synchronous and just get wrapped in a ready future.
