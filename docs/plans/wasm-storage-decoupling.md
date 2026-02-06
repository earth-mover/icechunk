# Plan: Decouple Storage Backends for WASM Support

## Goal

Enable the core `icechunk` crate to compile for `wasm32-wasip1-threads` by decoupling
built-in storage backends from core types. This also lets Rust users bring their own
`Storage` implementations without pulling in heavy cloud SDK dependencies.

## Current Coupling

The core crate unconditionally depends on the AWS SDK, cloud-specific `object_store`
features, and `reqwest`. These dependencies cannot compile for `wasm32-wasip1-threads`.

The coupling exists in three places outside the backend modules themselves:

### 1. `StorageErrorKind` (`storage/mod.rs:72-117`)

Nine error variants that directly embed AWS SDK types:

```rust
S3GetObjectError(#[from] Box<SdkError<GetObjectError, HttpResponse>>),
S3PutObjectError(#[from] Box<SdkError<PutObjectError, HttpResponse>>),
// ...7 more AWS SDK variants
```

This forces `storage/mod.rs` to `use aws_sdk_s3::...` at the top level. Every consumer
of `StorageError` transitively depends on the AWS SDK.

### 2. `ObjectStoreConfig` enum (`config.rs:64-77`)

A closed enum that hardcodes all eight backend variants:

```rust
pub enum ObjectStoreConfig {
    InMemory,
    LocalFileSystem(PathBuf),
    Http(HashMap<String, String>),
    S3Compatible(S3Options),
    S3(S3Options),
    Gcs(HashMap<String, String>),
    Azure(HashMap<String, String>),
    Tigris(S3Options),
}
```

This enum is used in two key roles:

1. **Primary storage creation** -- The Python bindings expose it as `PyObjectStoreConfig`
   and use the factory functions in `storage/mod.rs` (`new_s3_storage`, `new_gcs_storage`,
   etc.) to construct `Arc<dyn Storage>` for opening/creating repositories.
   `RedirectStorage` also dispatches on URL scheme to call these same factory functions.

2. **Virtual chunk containers** -- Held by `VirtualChunkContainer` in `RepositoryConfig`
   and matched in `virtual_chunks.rs::mk_fetcher_for()` to construct `ChunkFetcher`
   instances for reading external data.

Adding or removing a backend requires modifying this core enum and every match site.

### 3. `virtual_chunks.rs` (lines 16, 39-43, 342-508)

The worst offender. Directly imports:
- `aws_sdk_s3::Client`
- `storage::s3::{mk_client, range_to_header}`
- `AzureObjectStoreBackend`, `GcsObjectStoreBackend`, `HttpObjectStoreBackend`

Contains a 160-line `match &cont.store` that constructs concrete `S3Fetcher` or
`ObjectStoreFetcher` instances for each `ObjectStoreConfig` variant.

### What is already decoupled

`Repository`, `Session`, `Store`, and `AssetManager` only know
`Arc<dyn Storage + Send + Sync>`. They have zero references to backend-specific types.
A Rust user can already pass in any `Storage` impl -- the problem is that the heavy
dependencies are still unconditionally compiled.

## Target Architecture

```
                      CORE (no backend knowledge)
┌──────────────────────────────────────────────────────────────┐
│  Storage trait (unchanged)                                   │
│  ChunkFetcher trait (unchanged)                              │
│  ObjectStoreConfig trait (replaces enum, typetag-serializable│
│    - make_storage(): required                                │
│    - mk_fetcher():   default returns Err (not all support it)│
│  VirtualChunkContainer { store: Arc<dyn ObjectStoreConfig> } │
│  VirtualChunkResolver  (calls store.mk_fetcher())            │
│  StorageErrorKind (no AWS types)                             │
│  RepositoryConfig, CachingConfig, etc. (backend-agnostic)    │
└──────────────────────────────────────────────────────────────┘
          ▲                    ▲                    ▲
          │                    │                    │
   ┌──────┴──────┐    ┌───────┴──────┐    ┌───────┴───────┐
   │ s3 feature  │    │ gcs feature  │    │ azure feature │  ...
   │             │    │              │    │               │
   │ S3Config    │    │ GcsConfig    │    │ AzureConfig   │
   │ S3Storage   │    │ GcsBackend   │    │ AzureBackend  │
   │ S3Fetcher   │    │ GcsFetcher   │    │ AzureFetcher  │
   │ S3Options   │    │ GcsCredentials│   │ AzureCredentials│
   │ impl Storage│    │ impl Storage │    │ impl Storage  │
   │ impl ObjCfg │    │ impl ObjCfg  │    │ impl ObjCfg   │
   └─────────────┘    └──────────────┘    └───────────────┘
```

Each backend is a self-contained module that registers itself via `typetag`. Core has
zero knowledge of specific backends. Users can add their own by implementing `Storage`
and/or `ObjectStoreConfig`.

## Steps

### Step 1: Genericize `StorageErrorKind`

**Scope**: `storage/mod.rs`, `storage/s3.rs`

Replace the nine AWS SDK error variants with a single generic variant:

```rust
// BEFORE (9 variants importing AWS SDK types)
S3GetObjectError(#[from] Box<SdkError<GetObjectError, HttpResponse>>),
S3PutObjectError(#[from] Box<SdkError<PutObjectError, HttpResponse>>),
S3CreateMultipartUploadError(..),
S3UploadPartError(..),
S3CompleteMultipartUploadError(..),
S3CopyObjectError(..),
S3HeadObjectError(..),
S3ListObjectError(..),
S3DeleteObjectError(..),
S3StreamError(..),

// AFTER
S3Error(String),
```

`S3Storage` converts SDK errors to strings before returning `StorageError`. All
`aws_sdk_s3` imports are removed from `storage/mod.rs`.

**Why first**: Smallest change, isolated to two files, no behavioral impact. Removes AWS
SDK types from the core error path that every consumer touches.

### Step 2: Convert `ObjectStoreConfig` from enum to trait

**Scope**: `config.rs`, `virtual_chunks.rs`, `storage/mod.rs`, backend modules

Replace the closed `ObjectStoreConfig` enum with a `typetag`-serializable trait. The
trait covers both roles the enum currently fills: creating primary `Storage` backends
and optionally creating `ChunkFetcher` instances for virtual chunks.

Not all backends support virtual chunks (e.g. `InMemory`, `Redirect`). Rather than
forcing every impl to provide a fetcher, `mk_fetcher()` has a default implementation
that returns an error. Backends that support virtual chunks override it.

```rust
#[async_trait]
#[typetag::serde(tag = "object_store_type")]
pub trait ObjectStoreConfig: fmt::Debug + Send + Sync {
    /// Create a Storage backend from this configuration.
    async fn make_storage(
        &self,
        credentials: Option<&Credentials>,
    ) -> StorageResult<Arc<dyn Storage>>;

    /// Validate that the provided credentials match this store type.
    fn validate_credentials(&self, cred: Option<&Credentials>) -> Result<(), String>;

    /// Validate that this config is appropriate for the given URL scheme.
    fn validate_url(&self, url: &Url) -> Result<(), String>;

    /// Whether the fetcher cache key needs a bucket/host component.
    fn is_bucket_constrained(&self) -> bool { false }

    /// Construct a ChunkFetcher for virtual chunk access.
    /// Default returns an error; backends that support virtual chunks override this.
    async fn mk_fetcher(
        &self,
        _credentials: Option<&Credentials>,
        _settings: &storage::Settings,
        _chunk_location: &Url,
    ) -> Result<Arc<dyn ChunkFetcher>, VirtualReferenceError> {
        Err(VirtualReferenceErrorKind::VirtualChunksNotSupported.into())
    }
}
```

Each backend implements this trait. Backends that support virtual chunks override
`mk_fetcher()`:

```rust
// In storage/s3.rs -- supports virtual chunks
#[derive(Debug, Serialize, Deserialize)]
pub struct S3StoreConfig {
    pub options: S3Options,
    pub bucket: String,
    pub prefix: Option<String>,
}

#[typetag::serde(name = "s3")]
impl ObjectStoreConfig for S3StoreConfig {
    async fn make_storage(&self, credentials: Option<&Credentials>) -> StorageResult<Arc<dyn Storage>> {
        let creds = extract_s3_credentials(credentials)?;
        new_s3_storage(self.options.clone(), self.bucket.clone(), self.prefix.clone(), creds)
    }

    async fn mk_fetcher(
        &self,
        credentials: Option<&Credentials>,
        settings: &storage::Settings,
        chunk_location: &Url,
    ) -> Result<Arc<dyn ChunkFetcher>, VirtualReferenceError> {
        let creds = extract_s3_credentials(credentials)?;
        Ok(Arc::new(S3Fetcher::new(&self.options, &creds, settings.clone()).await))
    }
    // ...
}

// InMemory -- inherits default mk_fetcher() which returns Err(VirtualChunksNotSupported)
#[typetag::serde(name = "in_memory")]
impl ObjectStoreConfig for InMemoryConfig {
    async fn make_storage(&self, _credentials: Option<&Credentials>) -> StorageResult<Arc<dyn Storage>> {
        Ok(Arc::new(ObjectStorage::new_in_memory().await?))
    }
    fn validate_credentials(&self, cred: Option<&Credentials>) -> Result<(), String> { ... }
    fn validate_url(&self, _url: &Url) -> Result<(), String> { ... }
}
```

**`VirtualChunkContainer`** changes from holding the enum to a trait object:

```rust
pub struct VirtualChunkContainer {
    url_prefix: String,
    pub store: Arc<dyn ObjectStoreConfig>,
}
```

**The 160-line match in `mk_fetcher_for()`** collapses to:

```rust
async fn mk_fetcher_for(&self, cont: &VirtualChunkContainer, chunk_location: &Url)
    -> Result<Arc<dyn ChunkFetcher>, VirtualReferenceError>
{
    let creds = self.credentials.get(&cont.url_prefix).and_then(|c| c.as_ref());
    cont.store.mk_fetcher(creds, &self.settings, chunk_location).await
}
```

**The factory functions in `storage/mod.rs`** (`new_s3_storage`, `new_gcs_storage`, etc.)
remain as convenience constructors but now live in their backend modules. They can also
be expressed as thin wrappers around `ObjectStoreConfig::make_storage()`.

**`RedirectStorage::mk_storage()`** constructs backend-specific config structs and calls
`make_storage()` instead of calling factory functions directly.

**`Credentials` enum**: Stays as-is for now. It is a small enum at the `Repository` API
boundary with no heavy dependencies. Each `ObjectStoreConfig` impl matches on the
variant it expects. This can be further decoupled later.

**Serde migration**: The old `ObjectStoreConfig` used serde's built-in enum tagging
(`#[serde(rename_all = "snake_case")]`). The new approach uses typetag's tag field
(`"object_store_type": "s3"`). A migration adapter can deserialize the old format and
produce new trait objects if needed.

### Step 3: Move backend-specific types to their modules

**Scope**: `config.rs` types move to backend modules

| Type | From | To |
|------|------|----|
| `S3Options` | `config.rs` | `storage/s3.rs` |
| `S3Credentials`, `S3StaticCredentials`, `S3CredentialsFetcher` | `config.rs` | `storage/s3.rs` |
| `GcsCredentials`, `GcsStaticCredentials`, `GcsBearerCredential`, `GcsCredentialsFetcher` | `config.rs` | GCS module |
| `AzureCredentials`, `AzureStaticCredentials` | `config.rs` | Azure module |
| `S3Fetcher` | `virtual_chunks.rs` | `storage/s3.rs` |
| `ObjectStoreFetcher` (per-backend methods) | `virtual_chunks.rs` | Respective modules |

After this step `config.rs` retains only backend-agnostic types: `RepositoryConfig`,
`CachingConfig`, `CompressionConfig`, `ManifestConfig`, `Credentials`.

Each backend module becomes self-contained: its `ObjectStoreConfig` impl, its
`Storage` impl, its config types, and its fetcher.

### Step 4: Feature-gate the backends

Now trivial because each backend is an island.

```toml
[features]
default = ["s3", "gcs", "azure", "http-store", "redirect"]
s3 = [
    "dep:aws-sdk-s3", "dep:aws-config", "dep:aws-credential-types",
    "dep:aws-smithy-runtime", "dep:aws-smithy-types-convert", "dep:typed-path",
]
gcs = ["object_store/gcp"]
azure = ["object_store/azure"]
http-store = ["object_store/http"]
redirect = ["dep:reqwest"]
```

Backend modules are gated with `#[cfg(feature = "...")]`. No `#[cfg]` is needed in any
core module. Backends register themselves via `typetag` when their feature is enabled.

### Step 5: WASM target support

With minimal features (no `s3`, no cloud backends), the dependency tree is dramatically
smaller. Remaining work:

- Target-conditional tokio features for `wasm32-wasip1-threads`
- Verify `object_store` base (`InMemory`, `LocalFileSystem`) compiles
- Fix any remaining platform-specific issues

**`typetag`/`inventory` WASM compatibility -- confirmed via napi-rs spike**:

`typetag` works end-to-end under napi-rs's `wasm32-wasip1-threads` target. A
`#[typetag::serde(tag = "type")]` trait with multiple implementations round-trips
correctly through `serde_json` -- serialization produces the correct `"type"`
discriminator and deserialization resolves to the right concrete type.

No special `__wasm_call_ctors` call is needed. The napi-rs WASM loader automatically
calls all `__napi_register__*` exports during module instantiation, which triggers
`inventory`/`ctor` initialization. The existing `#[typetag::serde]` annotations will
work as-is in WASM -- no code changes needed for typetag compatibility.

## Change Matrix

| File | Step 1 | Step 2 | Step 3 | Step 4 |
|------|--------|--------|--------|--------|
| `storage/mod.rs` | Remove AWS SDK error types + imports | Replace factory fns with trait calls | | Gate cloud factory fns |
| `storage/s3.rs` | Wrap errors before returning | Add `ObjectStoreConfig` impl | Receive types from config.rs | Gate module |
| `storage/object_store.rs` | | Add per-backend `ObjectStoreConfig` impls | Receive credential types | Gate cloud backends |
| `storage/redirect.rs` | | Use `ObjectStoreConfig::make_storage()` | | Gate module |
| `virtual_chunks.rs` | | Replace enum with trait call, collapse match | Move fetcher impls out | |
| `config.rs` | | Replace enum with trait def | Move backend types out | |
| `lib.rs` | | | Update exports | |
| `Cargo.toml` | | | | Add features, make deps optional |

Each step is independently shippable and testable. Steps 1-3 are internal
reorganizations with no impact on public API behavior. Step 4 adds Cargo features with
`default` enabling all, so existing users are unaffected.

## Notes

- `typetag` works under `wasm32-wasip1-threads` with napi-rs -- **confirmed via spike**.
  Registration happens automatically through napi-rs's `__napi_register__*` export calls
  at module init time. No manual `__wasm_call_ctors` needed.
  See also <https://github.com/dtolnay/typetag/pull/96>.
- The `Storage` trait already uses `typetag` for serialization, so this pattern is
  established in the codebase.
- `Repository`, `Session`, `Store`, and `AssetManager` already use
  `Arc<dyn Storage + Send + Sync>` and need no changes.
- The Python bindings (`PyObjectStoreConfig`) will need updating in step 2 to construct
  backend-specific config structs instead of the enum. The `PyStorage` class methods
  (`new_s3`, `new_gcs`, etc.) can stay as-is since they already call backend-specific
  factory functions directly.
