# Storage Backends

## The Big Picture: Persistence Layer

All the [Snapshots](core-types.md#snapshot), [Manifests](core-types.md#manifest), and chunk data need to be persisted somewhere—that's what the Storage layer handles. Whether your data lives on S3, Google Cloud Storage, Azure Blob, or a local filesystem, Icechunk abstracts it behind a single trait.

The `AssetManager` sits on top of storage, adding caching and concurrency control so you don't overwhelm your cloud provider or re-fetch the same manifest a hundred times.

This document explains how the Storage trait works, what the AssetManager does, and how virtual chunks resolve external references.

> **Scope**: This document covers the persistence layer internals. For the data structures being stored, see [Core Types](core-types.md). For how Sessions use storage during commits, see [Session & ChangeSet](session.md).

---

## Storage Trait

The `Storage` trait (defined in `storage/mod.rs`) is the abstraction for all I/O operations:

```rust
#[async_trait]
#[typetag::serde(tag = "type")]
pub trait Storage: fmt::Debug + fmt::Display + private::Sealed + Sync + Send {
    async fn default_settings(&self) -> StorageResult<Settings>;
    async fn can_write(&self) -> StorageResult<bool>;

    async fn get_object(
        &self,
        settings: &Settings,
        path: &str,
        range: Option<&Range<u64>>,
    ) -> StorageResult<(Pin<Box<dyn AsyncBufRead + Send>>, VersionInfo)>;

    async fn get_object_range(
        &self,
        settings: &Settings,
        path: &str,
        range: Option<&Range<u64>>,
    ) -> StorageResult<(
        Pin<Box<dyn Stream<Item = Result<Bytes, StorageError>> + Send>>,
        VersionInfo,
    )>;

    async fn put_object(
        &self,
        settings: &Settings,
        path: &str,
        bytes: Bytes,
        content_type: Option<&str>,
        metadata: Vec<(String, String)>,
        previous_version: Option<&VersionInfo>,
    ) -> StorageResult<VersionedUpdateResult>;

    async fn copy_object(
        &self,
        settings: &Settings,
        from: &str,
        to: &str,
        content_type: Option<&str>,
        version: &VersionInfo,
    ) -> StorageResult<VersionedUpdateResult>;

    async fn list_objects<'a>(
        &'a self,
        settings: &Settings,
        prefix: &str,
    ) -> StorageResult<BoxStream<'a, StorageResult<ListInfo<String>>>>;

    async fn delete_batch(
        &self,
        settings: &Settings,
        prefix: &str,
        batch: Vec<(String, u64)>,
    ) -> StorageResult<DeleteObjectsResult>;

    async fn get_object_last_modified(
        &self,
        path: &str,
        settings: &Settings,
    ) -> StorageResult<DateTime<Utc>>;

    async fn delete_objects(
        &self,
        settings: &Settings,
        prefix: &str,
        ids: BoxStream<'_, (String, u64)>,
    ) -> StorageResult<DeleteObjectsResult>;

    async fn root_is_clean(&self) -> StorageResult<bool>;
}
```

### Key Design Decisions

**Why `Settings` passed to every method?** Settings can vary per-operation—you might want different storage classes for chunks vs. metadata. Passing settings explicitly keeps the trait stateless.

**Why streaming returns?** Large objects (especially manifests) shouldn't be buffered entirely in memory. Streaming allows processing as data arrives.

**Why `VersionInfo` in returns?** Every read returns version information (ETags, generation numbers) needed for conditional writes. This enables [conflict detection](conflicts.md).

**Why `typetag::serde`?** Storage implementations need to be serializable for session persistence. The `typetag` crate enables serializing trait objects.

---

## Version Info and Conditional Writes

Conditional writes are how Icechunk detects concurrent modifications:

```rust
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone)]
pub struct VersionInfo {
    pub etag: Option<ETag>,
    pub generation: Option<Generation>,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone, Hash, PartialOrd, Ord)]
pub struct ETag(pub String);

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone, Default)]
pub struct Generation(pub String);
```

**ETags** are content hashes returned by S3-compatible storage. **Generations** are version numbers used by GCS. When writing, you can pass the expected version—if it doesn't match, someone else modified the object.

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VersionedUpdateResult {
    Updated { new_version: VersionInfo },
    NotOnLatestVersion,
}
```

`NotOnLatestVersion` signals a conflict—the caller needs to fetch the new version and retry or report the conflict.

---

## Settings

Storage behavior is configured via `Settings` (defined in `storage/mod.rs`):

```rust
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone, Default)]
pub struct Settings {
    pub concurrency: Option<ConcurrencySettings>,
    pub retries: Option<RetriesSettings>,
    pub unsafe_use_conditional_update: Option<bool>,
    pub unsafe_use_conditional_create: Option<bool>,
    pub unsafe_use_metadata: Option<bool>,
    pub storage_class: Option<String>,
    pub metadata_storage_class: Option<String>,
    pub chunks_storage_class: Option<String>,
    pub minimum_size_for_multipart_upload: Option<u64>,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone, Default)]
pub struct ConcurrencySettings {
    pub max_concurrent_requests_for_object: Option<NonZeroU16>,
    pub ideal_concurrent_request_size: Option<NonZeroU64>,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone, Default)]
pub struct RetriesSettings {
    pub max_tries: Option<NonZeroU16>,
    pub initial_backoff_ms: Option<u32>,
    pub max_backoff_ms: Option<u32>,
}
```

### Why These Settings?

| Setting | Purpose |
|---------|---------|
| `concurrency` | Controls parallel requests for large objects |
| `retries` | Exponential backoff for transient failures |
| `unsafe_use_conditional_*` | Disable conditional writes for backends that don't support them |
| `storage_class` | S3 storage class (STANDARD, GLACIER, etc.) |
| `metadata_storage_class` | Separate class for snapshots/manifests |
| `chunks_storage_class` | Separate class for chunk data |
| `minimum_size_for_multipart_upload` | Threshold for multipart uploads (default: 100MB) |

The `unsafe_` prefix warns that disabling conditional operations loses conflict detection guarantees.

---

## Object Paths

Storage organizes objects with predictable path prefixes:

| Object Type | Path Pattern | Example |
|-------------|--------------|---------|
| Snapshot | `snapshots/{id}` | `snapshots/VY76P925PRY57WFEK410` |
| Manifest | `manifests/{id}` | `manifests/ABC123...` |
| Chunk | `chunks/{id}` | `chunks/XYZ789...` |
| TransactionLog | `transactions/{id}` | `transactions/DEF456...` |
| Branch Ref | `refs/branch.{name}/ref.json` | `refs/branch.main/ref.json` |
| Tag Ref | `refs/tag.{name}/ref.json` | `refs/tag.v1.0/ref.json` |
| Config | `config.yaml` | `config.yaml` |
| RepoInfo | `repo.info` | `repo.info` |

The IDs are encoded using Crockford Base32 (see [Core Types](core-types.md#how-ids-work)).

---

## Storage Error Types

```rust
#[derive(Debug, Error)]
pub enum StorageErrorKind {
    #[error("object not found")]
    ObjectNotFound,
    #[error("object store error {0}")]
    ObjectStore(#[from] Box<::object_store::Error>),
    #[error("bad object store prefix {0:?}")]
    BadPrefix(OsString),

    // S3-specific errors
    #[error("error getting object from object store {0}")]
    S3GetObjectError(#[from] Box<SdkError<GetObjectError, HttpResponse>>),
    #[error("error writing object to object store {0}")]
    S3PutObjectError(#[from] Box<SdkError<PutObjectError, HttpResponse>>),
    #[error("error creating multipart upload {0}")]
    S3CreateMultipartUploadError(#[from] Box<SdkError<CreateMultipartUploadError, HttpResponse>>),
    #[error("error uploading multipart part {0}")]
    S3UploadPartError(#[from] Box<SdkError<UploadPartError, HttpResponse>>),
    #[error("error completing multipart upload {0}")]
    S3CompleteMultipartUploadError(#[from] Box<SdkError<CompleteMultipartUploadError, HttpResponse>>),
    #[error("error copying object in object store {0}")]
    S3CopyObjectError(#[from] Box<SdkError<CopyObjectError, HttpResponse>>),
    #[error("error getting object metadata from object store {0}")]
    S3HeadObjectError(#[from] Box<SdkError<HeadObjectError, HttpResponse>>),
    #[error("error listing objects in object store {0}")]
    S3ListObjectError(#[from] Box<SdkError<ListObjectsV2Error, HttpResponse>>),
    #[error("error deleting objects in object store {0}")]
    S3DeleteObjectError(#[from] Box<SdkError<DeleteObjectsError, HttpResponse>>),
    #[error("error streaming bytes from object store {0}")]
    S3StreamError(#[from] Box<ByteStreamError>),

    // Other errors
    #[error("I/O error: {0}")]
    IOError(#[from] std::io::Error),
    #[error("storage configuration error: {0}")]
    R2ConfigurationError(String),
    #[error("error parsing URL: {url:?}")]
    CannotParseUrl { cause: url::ParseError, url: String },
    #[error("Redirect Storage error: {0}")]
    BadRedirect(String),
    #[error("storage error: {0}")]
    Other(String),
}
```

### Why So Many S3-Specific Errors?

S3 operations can fail in many distinct ways, and the error type affects retry logic. A `S3GetObjectError` might be a 404 (don't retry) or a 503 (retry with backoff). Wrapping the SDK errors preserves this information.

---

## Backend Implementations

### Constructor Functions

The module provides constructor functions for each backend:

```rust
// S3 (native SDK client)
pub fn new_s3_storage(
    config: S3Options,
    bucket: String,
    prefix: Option<String>,
    credentials: Option<S3Credentials>,
) -> StorageResult<Arc<dyn Storage>>;

// Cloudflare R2
pub fn new_r2_storage(
    config: S3Options,
    bucket: Option<String>,
    prefix: Option<String>,
    account_id: Option<String>,
    credentials: Option<S3Credentials>,
) -> StorageResult<Arc<dyn Storage>>;

// Tigris (with consistency options)
pub fn new_tigris_storage(
    config: S3Options,
    bucket: String,
    prefix: Option<String>,
    credentials: Option<S3Credentials>,
    use_weak_consistency: bool,
) -> StorageResult<Arc<dyn Storage>>;

// In-memory (for testing)
pub async fn new_in_memory_storage() -> StorageResult<Arc<dyn Storage>>;

// Local filesystem
pub async fn new_local_filesystem_storage(
    path: &Path,
) -> StorageResult<Arc<dyn Storage>>;

// HTTP/HTTPS
pub fn new_http_storage(
    base_url: &str,
    config: Option<HashMap<String, String>>,
) -> StorageResult<Arc<dyn Storage>>;

// Google Cloud Storage
pub fn new_gcs_storage(
    bucket: String,
    prefix: Option<String>,
    credentials: Option<GcsCredentials>,
    config: Option<HashMap<String, String>>,
) -> StorageResult<Arc<dyn Storage>>;

// Azure Blob Storage
pub async fn new_azure_blob_storage(
    account: String,
    container: String,
    prefix: Option<String>,
    credentials: Option<AzureCredentials>,
    config: Option<HashMap<String, String>>,
) -> StorageResult<Arc<dyn Storage>>;
```

### Why Two S3 Implementations?

There are two S3 implementations: `S3Storage` (native AWS SDK) and `ObjectStorage` (via `object_store` crate). The native SDK provides better control for S3-specific features like multipart uploads and conditional writes. The `object_store`-based implementation provides a unified interface for multiple cloud providers.

---

## AssetManager

The `AssetManager` (defined in `asset_manager.rs`) sits between [Session](session.md) and Storage, providing caching and concurrency control:

```rust
#[derive(Debug, Serialize, Deserialize)]
pub struct AssetManager {
    storage: Arc<dyn Storage + Send + Sync>,
    storage_settings: storage::Settings,
    spec_version: SpecVersionBin,

    // Cache size configuration
    num_snapshot_nodes: u64,
    num_chunk_refs: u64,
    num_transaction_changes: u64,
    num_bytes_attributes: u64,
    num_bytes_chunks: u64,

    compression_level: u8,
    max_concurrent_requests: u16,

    // Caches (skipped during serialization, recreated on deserialize)
    #[serde(skip)]
    snapshot_cache: Cache<SnapshotId, Arc<Snapshot>, FileWeighter>,
    #[serde(skip)]
    manifest_cache: Cache<ManifestId, Arc<Manifest>, FileWeighter>,
    #[serde(skip)]
    transactions_cache: Cache<SnapshotId, Arc<TransactionLog>, FileWeighter>,
    #[serde(skip)]
    chunk_cache: Cache<(ChunkId, Range<ChunkOffset>), Bytes, FileWeighter>,

    #[serde(skip)]
    request_semaphore: Semaphore,
}
```

### Why These Fields?

| Field | Purpose |
|-------|---------|
| `storage` | The underlying Storage implementation |
| `storage_settings` | Default settings for operations |
| `spec_version` | Format version for serialization |
| `num_*` | Cache capacity limits (in weighted units) |
| `compression_level` | Zstd compression level (0-22) |
| `snapshot_cache` | LRU cache for Snapshot objects |
| `manifest_cache` | LRU cache for Manifest objects |
| `chunk_cache` | LRU cache for chunk bytes |
| `request_semaphore` | Limits concurrent storage requests |

### Cache Implementation

Caches use the [`quick_cache`](https://docs.rs/quick_cache/latest/quick_cache/) crate with weighted eviction:

```rust
#[derive(Debug, Clone)]
struct FileWeighter;

impl Weighter<ManifestId, Arc<Manifest>> for FileWeighter {
    fn weight(&self, _: &ManifestId, val: &Arc<Manifest>) -> u64 {
        val.len() as u64  // Number of chunk references
    }
}

impl Weighter<SnapshotId, Arc<Snapshot>> for FileWeighter {
    fn weight(&self, _: &SnapshotId, val: &Arc<Snapshot>) -> u64 {
        val.len() as u64  // Number of nodes
    }
}
```

Weights correspond to logical units (nodes, chunk refs) rather than raw bytes. This means the `num_chunk_refs` setting controls how many chunk references can be cached across all manifests, not a fixed number of manifests.

### Deduplication

The caches use `get_value_or_guard_async` to prevent duplicate fetches:

```rust
pub async fn fetch_manifest(
    &self,
    manifest_id: &ManifestId,
    manifest_size: u64,
) -> RepositoryResult<Arc<Manifest>> {
    match self.manifest_cache.get_value_or_guard_async(manifest_id).await {
        Ok(manifest) => Ok(manifest),  // Cache hit
        Err(guard) => {
            // Cache miss - fetch and insert
            let manifest = fetch_manifest(...).await?;
            let _fail_is_ok = guard.insert(Arc::clone(&manifest));
            Ok(manifest)
        }
    }
}
```

If two concurrent requests ask for the same manifest, only one actually fetches—the other waits for the guard.

---

## Virtual Chunk Resolution

For chunks that reference external files (HDF5, NetCDF, etc.), the `VirtualChunkResolver` handles fetching:

```rust
#[derive(Debug, Serialize, Deserialize)]
pub struct VirtualChunkResolver {
    containers: Vec<VirtualChunkContainer>,
    credentials: HashMap<String, Option<Credentials>>,
    settings: storage::Settings,
    #[serde(skip, default = "new_cache")]
    fetchers: ChunkFetcherCache,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct VirtualChunkContainer {
    #[serde(default)]
    pub name: Option<ContainerName>,
    url_prefix: String,
    pub store: ObjectStoreConfig,
}
```

### How Resolution Works

1. **Find matching container** by URL prefix (longest match wins)
2. **Get or create fetcher** for that container (cached)
3. **Fetch byte range** from external source

```rust
pub async fn fetch_chunk(
    &self,
    chunk_location: &str,
    range: &Range<ChunkOffset>,
    checksum: Option<&Checksum>,
) -> Result<Bytes, VirtualReferenceError> {
    let url = Url::parse(chunk_location)?;
    let fetcher = self.get_fetcher(&url).await?;
    fetcher.fetch_chunk(&url, range, checksum).await
}
```

### Checksum Validation

Virtual references can include checksums to detect if the source file changed:

```rust
pub enum Checksum {
    LastModified(SecondsSinceEpoch),
    ETag(ETag),
}
```

If the checksum doesn't match, the fetch fails with `ObjectModified`—this prevents silently returning stale data.

---

## Concurrent Request Splitting

For large objects, requests are split into parallel byte-range fetches:

```rust
pub fn split_in_multiple_requests(
    range: &Range<u64>,
    ideal_req_size: u64,
    max_requests: u16,
) -> impl Iterator<Item = Range<u64>>
```

This function splits a range into `ceil(size/ideal_req_size)` requests, capped at `max_requests`. The default settings (12MB chunks, 18 parallel requests) are tuned for S3 performance based on AWS recommendations.

---

## Next Steps

- [Core Types](core-types.md) - The objects being stored
- [Session & ChangeSet](session.md) - How Sessions use storage during commits
- [Conflict Resolution](conflicts.md) - How conditional writes enable conflict detection
