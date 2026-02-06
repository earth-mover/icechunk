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

This enum is held by `VirtualChunkContainer` and stored in `RepositoryConfig`. It is the
dispatch mechanism for virtual chunk fetcher construction. Adding or removing a backend
requires modifying core types.

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
│  VirtualStoreConfig trait (new, typetag-serializable)        │
│  VirtualChunkContainer { store: Arc<dyn VirtualStoreConfig> }│
│  VirtualChunkResolver  (calls store.mk_fetcher())            │
│  StorageErrorKind (no AWS types)                             │
│  RepositoryConfig, CachingConfig, etc. (backend-agnostic)    │
└──────────────────────────────────────────────────────────────┘
          ▲                    ▲                    ▲
          │                    │                    │
   ┌──────┴──────┐    ┌───────┴──────┐    ┌───────┴───────┐
   │ s3 feature  │    │ gcs feature  │    │ azure feature │  ...
   │             │    │              │    │               │
   │ S3Storage   │    │ GcsBackend   │    │ AzureBackend  │
   │ S3Options   │    │ GcsCredentials│   │ AzureCredentials│
   │ S3Fetcher   │    │ GcsFetcher   │    │ AzureFetcher  │
   │ impl Storage│    │ impl Storage │    │ impl Storage  │
   │ impl VirtCfg│    │ impl VirtCfg │    │ impl VirtCfg  │
   └─────────────┘    └──────────────┘    └───────────────┘
```

Each backend is a self-contained module that registers itself via `typetag`. Core has
zero knowledge of specific backends. Users can add their own by implementing `Storage`
and/or `VirtualStoreConfig`.

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

### Step 2: Introduce `VirtualStoreConfig` trait

**Scope**: `virtual_chunks.rs`, new trait definition

Create a `typetag`-serializable trait that replaces `ObjectStoreConfig` for virtual chunk
dispatch:

```rust
#[async_trait]
#[typetag::serde(tag = "virtual_store_type")]
pub trait VirtualStoreConfig: fmt::Debug + Send + Sync {
    /// Validate that this config is appropriate for the given URL scheme.
    fn validate_url(&self, url: &Url) -> Result<(), String>;

    /// Validate that the provided credentials match this store type.
    fn validate_credentials(&self, cred: Option<&Credentials>) -> Result<(), String>;

    /// Whether the fetcher cache key needs a bucket/host component.
    fn is_bucket_constrained(&self) -> bool { false }

    /// Construct a ChunkFetcher for this store.
    async fn mk_fetcher(
        &self,
        credentials: Option<&Credentials>,
        settings: &storage::Settings,
        chunk_location: &Url,
    ) -> Result<Arc<dyn ChunkFetcher>, VirtualReferenceError>;
}
```

`VirtualChunkContainer` changes from holding `ObjectStoreConfig` (closed enum) to
`Arc<dyn VirtualStoreConfig>` (open trait object).

The 160-line `mk_fetcher_for()` match collapses to a single trait method call:

```rust
async fn mk_fetcher_for(
    &self,
    cont: &VirtualChunkContainer,
    chunk_location: &Url,
) -> Result<Arc<dyn ChunkFetcher>, VirtualReferenceError> {
    let creds = self.credentials.get(&cont.url_prefix).and_then(|c| c.as_ref());
    cont.store.mk_fetcher(creds, &self.settings, chunk_location).await
}
```

Each backend implements `VirtualStoreConfig` in its own module. The `S3Fetcher` and
`ObjectStoreFetcher` types move to their respective modules.

**`Credentials` enum**: Stays as-is for now. It is a small enum at the `Repository` API
boundary with no heavy dependencies. Each `VirtualStoreConfig` impl matches on the
variant it expects. This can be further decoupled later.

**Serde migration**: The old `ObjectStoreConfig` used serde's built-in enum tagging
(`#[serde(rename_all = "snake_case")]`). The new approach uses typetag's tag field
(`"virtual_store_type": "s3"`). A migration adapter can deserialize the old format and
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

Each backend module becomes self-contained: its `Storage` impl, its
`VirtualStoreConfig` impl, its config types, and its fetcher.

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
- Call `__wasm_call_ctors` for `typetag`/`inventory` initialization on wasm
- Fix any remaining platform-specific issues

## Change Matrix

| File | Step 1 | Step 2 | Step 3 | Step 4 |
|------|--------|--------|--------|--------|
| `storage/mod.rs` | Remove AWS SDK error types + imports | Remove factory fn cloud imports | | Gate cloud factory fns |
| `storage/s3.rs` | Wrap errors before returning | Add `VirtualStoreConfig` impl | Receive types from config.rs | Gate module |
| `storage/object_store.rs` | | Add per-backend `VirtualStoreConfig` impls | Receive credential types | Gate cloud backends |
| `storage/redirect.rs` | | | | Gate module |
| `virtual_chunks.rs` | | Replace enum with trait, collapse match | Move fetcher impls out | |
| `config.rs` | | Remove `ObjectStoreConfig` enum | Move backend types out | |
| `lib.rs` | | | Update exports | |
| `Cargo.toml` | | | | Add features, make deps optional |

Each step is independently shippable and testable. Steps 1-3 are internal
reorganizations with no impact on public API behavior. Step 4 adds Cargo features with
`default` enabling all, so existing users are unaffected.

## Notes

- `typetag` supports wasm via the `inventory` crate's `__wasm_call_ctors` mechanism.
  See <https://github.com/dtolnay/typetag/pull/96>.
- The `Storage` trait already uses `typetag` for serialization, so this pattern is
  established in the codebase.
- `Repository`, `Session`, `Store`, and `AssetManager` already use
  `Arc<dyn Storage + Send + Sync>` and need no changes.
