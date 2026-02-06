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

**Key finding: this enum is only used for virtual chunk containers.** Primary storage
creation goes through factory functions directly:

- **Python bindings** (`PyStorage::new_s3()`, etc.) call `icechunk::storage::new_s3_storage()`
  with concrete parameters -- they never go through `ObjectStoreConfig`.
- **`RedirectStorage::mk_storage()`** dispatches on URL scheme to call factory functions
  directly -- it also never uses `ObjectStoreConfig`.

The enum's sole consumer is `VirtualChunkContainer`, which holds it in
`VirtualChunkContainer.store` and matches on it in the 160-line `mk_fetcher_for()`.

### 3. `virtual_chunks.rs` (lines 16, 39-43, 342-508)

The worst offender. Directly imports:
- `aws_sdk_s3::Client`
- `storage::s3::{mk_client, range_to_header}`
- `AzureObjectStoreBackend`, `GcsObjectStoreBackend`, `HttpObjectStoreBackend`

Contains a 160-line `match &cont.store` that constructs concrete `S3Fetcher` or
`ObjectStoreFetcher` instances for each `ObjectStoreConfig` variant.

Also contains `VirtualChunkContainer::new()` with a scheme-to-variant validation match,
and `validate_credentials()` with a variant-to-credentials match.

### What is already decoupled

`Repository`, `Session`, `Store`, and `AssetManager` only know
`Arc<dyn Storage + Send + Sync>`. They have zero references to backend-specific types.
A Rust user can already pass in any `Storage` impl -- the problem is that the heavy
dependencies are still unconditionally compiled.

Primary storage creation is also effectively decoupled: factory functions take concrete
parameters (not the enum), and callers already know which backend they want.

## Target Architecture

```
                      CORE (no backend knowledge)
┌──────────────────────────────────────────────────────────────┐
│  Storage trait (unchanged)                                   │
│  ChunkFetcher trait (unchanged)                              │
│  ObjectStoreConfig trait (replaces enum, typetag-serializable│
│    - mk_fetcher():           create a ChunkFetcher           │
│    - validate_credentials(): check cred/store match          │
│    - validate_url():         check URL scheme/store match    │
│    - is_host_scoped():       cache key scoping               │
│    - NO make_storage()       (not needed, see design notes)  │
│  VirtualChunkContainer { store: Arc<dyn ObjectStoreConfig> } │
│  VirtualChunkResolver  (calls store.mk_fetcher())            │
│  StorageErrorKind (no AWS types)                             │
│  Factory functions (unchanged, feature-gated later)          │
│  RepositoryConfig, CachingConfig, etc. (backend-agnostic)    │
└──────────────────────────────────────────────────────────────┘
          ▲                    ▲                    ▲
          │                    │                    │
   ┌──────┴──────┐    ┌───────┴──────┐    ┌───────┴───────┐
   │ s3 feature  │    │ gcs feature  │    │ azure feature │  ...
   │             │    │              │    │               │
   │ S3StoreConf │    │ GcsStoreCfg  │    │ AzureStoreCfg │
   │ S3Storage   │    │ GcsBackend   │    │ AzureBackend  │
   │ S3Fetcher   │    │ ObjStoreFetch│    │ ObjStoreFetch │
   │ S3Options   │    │ GcsCredentials│   │ AzureCredentials│
   │ impl Storage│    │ impl Storage │    │ impl Storage  │
   │ impl ObjCfg │    │ impl ObjCfg  │    │ impl ObjCfg   │
   └─────────────┘    └──────────────┘    └───────────────┘
```

### Design Rationale: No `make_storage()` on the Trait

The original plan put both `make_storage()` and `mk_fetcher()` on `ObjectStoreConfig`.
PR review feedback (paraseba) correctly identified this as **too much responsibility** --
a single type should not be responsible for both primary storage creation and virtual
chunk fetching.

The code confirms this: `ObjectStoreConfig` is only consumed by `VirtualChunkContainer`
for virtual chunk fetching. Primary storage creation already has its own path (factory
functions called directly by Python bindings and `RedirectStorage`). These paths remain
unchanged.

This separation means:
- The `ObjectStoreConfig` trait is focused: it configures virtual chunk data access
- Factory functions continue handling primary storage creation (feature-gated later)
- `RedirectStorage` continues dispatching on URL scheme to factory functions
- No awkward `make_storage()` method that some impls don't need

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

**Check first**: Verify no code matches on specific S3 error variants for retry logic or
error classification. If so, consider `S3Error(Box<dyn std::error::Error + Send + Sync>)`
to preserve error inspection capability.

**Why first**: Smallest change, isolated to two files, no behavioral impact. Removes AWS
SDK types from the core error path that every consumer touches.

### Step 2: Convert `ObjectStoreConfig` from enum to trait

**Scope**: `config.rs`, `virtual_chunks.rs`, backend modules

Replace the closed `ObjectStoreConfig` enum with a `typetag`-serializable trait focused
on virtual chunk fetcher creation:

```rust
#[async_trait]
#[typetag::serde(tag = "object_store_type")]
pub trait ObjectStoreConfig: fmt::Debug + Send + Sync {
    /// Create a ChunkFetcher for reading data from this store.
    async fn mk_fetcher(
        &self,
        credentials: Option<&Credentials>,
        settings: &storage::Settings,
        chunk_location: &Url,
    ) -> Result<Arc<dyn ChunkFetcher>, VirtualReferenceError>;

    /// Validate that the provided credentials match this store type.
    fn validate_credentials(&self, cred: Option<&Credentials>) -> Result<(), String>;

    /// Validate that this config is appropriate for the given URL.
    fn validate_url(&self, url: &Url) -> Result<(), String>;

    /// Whether fetchers are scoped to a specific host/bucket.
    /// When true, the fetcher cache key includes the URL host.
    /// GCS, Azure, and HTTP need per-host fetchers; S3 does not.
    fn is_host_scoped(&self) -> bool { false }
}
```

Each backend provides its own config struct implementing the trait:

```rust
// In storage/s3.rs
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct S3StoreConfig(pub S3Options);

#[typetag::serde(name = "s3")]
impl ObjectStoreConfig for S3StoreConfig {
    async fn mk_fetcher(
        &self,
        credentials: Option<&Credentials>,
        settings: &storage::Settings,
        _chunk_location: &Url,
    ) -> Result<Arc<dyn ChunkFetcher>, VirtualReferenceError> {
        let creds = extract_s3_credentials(credentials)?;
        Ok(Arc::new(S3Fetcher::new(&self.0, &creds, settings.clone()).await))
    }

    fn validate_credentials(&self, cred: Option<&Credentials>) -> Result<(), String> {
        match cred {
            Some(Credentials::S3(_)) | None => Ok(()),
            _ => Err("S3 store requires S3 credentials".into()),
        }
    }

    fn validate_url(&self, url: &Url) -> Result<(), String> {
        match url.scheme() {
            "s3" => {
                if url.has_host() { Ok(()) }
                else { Err("s3:// URL must include a host".into()) }
            }
            scheme => Err(format!("S3 store cannot handle {scheme}:// URLs")),
        }
    }
    // is_host_scoped() defaults to false -- S3 fetcher works across buckets
}

// S3Compatible wraps S3Options identically but registers with a different name
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct S3CompatibleStoreConfig(pub S3Options);

#[typetag::serde(name = "s3_compatible")]
impl ObjectStoreConfig for S3CompatibleStoreConfig {
    // same impl as S3StoreConfig
}

// Tigris wraps S3Options, injects default endpoint
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TigrisStoreConfig(pub S3Options);

#[typetag::serde(name = "tigris")]
impl ObjectStoreConfig for TigrisStoreConfig {
    async fn mk_fetcher(
        &self,
        credentials: Option<&Credentials>,
        settings: &storage::Settings,
        _chunk_location: &Url,
    ) -> Result<Arc<dyn ChunkFetcher>, VirtualReferenceError> {
        let creds = extract_s3_credentials(credentials)?;
        let opts = if self.0.endpoint_url.is_some() {
            &self.0
        } else {
            &S3Options { endpoint_url: Some("https://t3.storage.dev".into()), ..self.0.clone() }
        };
        Ok(Arc::new(S3Fetcher::new(opts, &creds, settings.clone()).await))
    }
    // ...
}
```

```rust
// In storage/object_store.rs (GCS example)
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GcsStoreConfig(pub HashMap<String, String>);

#[typetag::serde(name = "gcs")]
impl ObjectStoreConfig for GcsStoreConfig {
    async fn mk_fetcher(
        &self,
        credentials: Option<&Credentials>,
        settings: &storage::Settings,
        chunk_location: &Url,
    ) -> Result<Arc<dyn ChunkFetcher>, VirtualReferenceError> {
        let creds = extract_gcs_credentials(credentials)?;
        let bucket = chunk_location.host_str()
            .ok_or(VirtualReferenceErrorKind::CannotParseBucketName(
                "No bucket name found".into(),
            ))?;
        Ok(Arc::new(
            ObjectStoreFetcher::new_gcs(
                urlencoding::decode(bucket)?.into_owned(),
                None,
                Some(creds),
                self.0.clone(),
            ).await?,
        ))
    }

    fn is_host_scoped(&self) -> bool { true }

    fn validate_credentials(&self, cred: Option<&Credentials>) -> Result<(), String> {
        match cred {
            Some(Credentials::Gcs(_)) | None => Ok(()),
            _ => Err("GCS store requires GCS credentials".into()),
        }
    }

    fn validate_url(&self, url: &Url) -> Result<(), String> {
        match url.scheme() {
            "gcs" | "gs" => {
                if url.has_host() { Ok(()) }
                else { Err("GCS URL must include a host".into()) }
            }
            scheme => Err(format!("GCS store cannot handle {scheme}:// URLs")),
        }
    }
}

// Azure, HTTP, LocalFileSystem follow the same pattern
```

**`VirtualChunkContainer`** changes from holding the enum to a trait object:

```rust
pub struct VirtualChunkContainer {
    #[serde(default)]
    pub name: Option<ContainerName>,
    url_prefix: String,
    pub store: Arc<dyn ObjectStoreConfig>,
}
```

**`VirtualChunkContainer::new()`** simplifies -- the match on (scheme, variant) is
replaced by delegation:

```rust
impl VirtualChunkContainer {
    pub fn new(url_prefix: String, store: Arc<dyn ObjectStoreConfig>) -> Result<Self, String> {
        if !url_prefix.ends_with('/') {
            return Err("VirtualChunkContainer url_prefix must end in a / character".into());
        }
        let url = Url::parse(&url_prefix).map_err(|e| e.to_string())?;
        store.validate_url(&url)?;
        Ok(Self { url_prefix, store, name: None })
    }

    pub fn validate_credentials(&self, cred: Option<&Credentials>) -> Result<(), String> {
        self.store.validate_credentials(cred)
    }
}
```

**The 160-line match in `mk_fetcher_for()`** collapses to:

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

**`is_fetcher_bucket_constrained()`** is replaced by `store.is_host_scoped()`:

```rust
fn fetcher_cache_key(
    cont: &VirtualChunkContainer,
    location: &Url,
) -> Result<CacheKey, VirtualReferenceError> {
    if cont.store.is_host_scoped() {
        let host = location.host_str()
            .ok_or(VirtualReferenceErrorKind::CannotParseBucketName(
                "No host found".into(),
            ))?;
        Ok((cont.url_prefix.clone(), Some(format!("{}://{}", location.scheme(), host))))
    } else {
        Ok((cont.url_prefix.clone(), None))
    }
}
```

After this step, `virtual_chunks.rs` has **zero** backend-specific imports.

**What stays unchanged**:
- Factory functions in `storage/mod.rs` (`new_s3_storage`, `new_gcs_storage`, etc.)
- `RedirectStorage::mk_storage()` dispatch on URL scheme
- `PyStorage` class methods (`new_s3`, `new_gcs`, etc.)
- `Credentials` enum (lightweight, no heavy deps)
- `ChunkFetcher` trait definition

**`Credentials` enum**: Stays as-is. It is a small enum at the `Repository` API
boundary with no heavy dependencies. Each `ObjectStoreConfig` impl matches on the
variant it expects. This does not break WASM compilation since `Credentials` contains
only our own lightweight types. Can be further decoupled later if needed.

**Serde migration**: `ObjectStoreConfig` is serialized into `config.yaml` (YAML) and
into transient Session/Repository MessagePack bytes. The enum-to-trait change alters
the format (see **Serialization Impact** section for full details). A custom
`Deserialize` on `VirtualChunkContainer` handles both old and new formats transparently.
This is a one-way migration: once rewritten, old icechunk versions cannot read the
config (old version backed up to `overwritten/`).

### Step 3: Move backend-specific types to their modules

**Scope**: `config.rs` types move to backend modules, fetcher impls move from
`virtual_chunks.rs`

| Type | From | To |
|------|------|----|
| `S3Options` | `config.rs` | `storage/s3.rs` |
| `S3Credentials`, `S3StaticCredentials`, `S3CredentialsFetcher` | `config.rs` | `storage/s3.rs` |
| `GcsCredentials`, `GcsStaticCredentials`, `GcsBearerCredential`, `GcsCredentialsFetcher` | `config.rs` | GCS module |
| `AzureCredentials`, `AzureStaticCredentials` | `config.rs` | Azure module |
| `S3Fetcher` struct + `ChunkFetcher` impl | `virtual_chunks.rs` | `storage/s3.rs` |
| `ObjectStoreFetcher` struct + constructor methods | `virtual_chunks.rs` | respective backend modules |

After this step `config.rs` retains only backend-agnostic types: `RepositoryConfig`,
`CachingConfig`, `CompressionConfig`, `ManifestConfig`, `Credentials`.

After this step `virtual_chunks.rs` retains only:
- `VirtualChunkContainer`, `VirtualChunkResolver` (using trait objects)
- `ChunkFetcher` trait definition
- `sort_containers`, `find_container`, `fetcher_cache_key` helpers
- Zero backend imports

Each backend module becomes self-contained: its `ObjectStoreConfig` impl, its
`Storage` impl, its config/credential types, and its `ChunkFetcher` impl.

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

Factory functions in `storage/mod.rs` are also gated behind their respective features.

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
| `storage/mod.rs` | Remove AWS SDK error types + imports | | | Gate cloud factory fns |
| `storage/s3.rs` | Wrap errors before returning | Add `ObjectStoreConfig` impl, absorb `S3Fetcher` | Receive types from config.rs | Gate module |
| `storage/object_store.rs` | | Add per-backend `ObjectStoreConfig` impls, absorb `ObjectStoreFetcher` | Receive credential types | Gate cloud backends |
| `storage/redirect.rs` | | (unchanged -- still dispatches on URL scheme) | | Gate module |
| `virtual_chunks.rs` | | Replace enum match with trait call, remove all backend imports | Move fetcher impls out | |
| `config.rs` | | Replace enum with trait def | Move backend types out | |
| `lib.rs` | | | Update exports | |
| `Cargo.toml` | | | | Add features, make deps optional |
| `icechunk-python/src/config.rs` | | Update `PyObjectStoreConfig` to construct backend config structs | | |

Each step is independently shippable and testable. Steps 1-3 are internal
reorganizations with no impact on public API behavior. Step 4 adds Cargo features with
`default` enabling all, so existing users are unaffected.

## Serialization Impact

`ObjectStoreConfig` is serialized through three paths. Understanding these is critical
for backward compatibility.

### Path 1: `config.yaml` (YAML) — persistent, per-repository

```
RepositoryConfig
  → virtual_chunk_containers: HashMap<String, VirtualChunkContainer>
    → VirtualChunkContainer.store: ObjectStoreConfig
      → serde_yaml_ng::to_string() / from_slice()
      → stored as "config.yaml" in the repository's object store
```

Code: `asset_manager.rs:252-311`. Written on `save_config()`, read on every
`Repository::open()`. Old versions are backed up to `overwritten/` before updates.

**This is the critical path.** Every existing repository with virtual chunk containers
has a `config.yaml` containing `ObjectStoreConfig` in the old enum format.

### Path 2: Session/Repository `as_bytes()` (MessagePack) — transient

```
Session { config: RepositoryConfig, virtual_resolver: VirtualChunkResolver }
  → RepositoryConfig.virtual_chunk_containers → VirtualChunkContainer.store
  → VirtualChunkResolver.containers → VirtualChunkContainer.store
  → rmp_serde::to_vec() / from_slice()
```

Used for marshalling between processes (distributed writers). Both `Session` and
`Repository` derive `Serialize`/`Deserialize` and contain `ObjectStoreConfig` through
both `RepositoryConfig` and `VirtualChunkResolver`. This is transient (not stored on
disk), so cross-version compat is less critical — both sides typically run the same
version.

### Path 3: Proptest roundtrips (MessagePack) — tests only

The `roundtrip_serialization_tests!` macro in `config.rs` roundtrips `RepositoryConfig`
through `rmp_serde`. These tests just need to keep passing.

### What is NOT affected

Snapshots (flatbuffer), manifests, transaction logs, and repo info do NOT contain
`ObjectStoreConfig` or `RepositoryConfig`. Only `config.yaml` and transient
Session/Repository byte marshalling are affected.

### Format difference: old vs new

**Old format** (serde externally-tagged enum with `#[serde(rename_all = "snake_case")]`):
```yaml
store:
  s3:                              # variant name is a key wrapping the content
    region: us-east-1
    endpoint_url: null
    anonymous: false
    ...
```
Unit variants: `store: in_memory`
Newtype variants: `store: { local_file_system: /some/path }`

**New format** (typetag internally-tagged with `#[typetag::serde(tag = "object_store_type")]`):
```yaml
store:
  object_store_type: s3            # tag is a field alongside content
  region: us-east-1
  endpoint_url: null
  anonymous: false
  ...
```

These are structurally incompatible — a typetag deserializer won't read the old format
and vice versa.

### Migration strategy: custom `Deserialize` on `VirtualChunkContainer`

Since `VirtualChunkContainer` is the type that directly holds `ObjectStoreConfig`, it
gets a custom `Deserialize` implementation that handles both formats:

1. Deserialize the `store` field as a raw `serde_value::Value` (or similar)
2. If it contains an `object_store_type` key → delegate to typetag (new format)
3. Otherwise → parse as old externally-tagged enum, convert to the matching trait object

Serialization always writes the new format (typetag). This makes migration transparent
and automatic:
- Old `config.yaml` files are read correctly
- On next `save_config()`, they're rewritten in new format (old version backed up)
- The same serde logic handles both YAML and MessagePack paths

**One-way migration**: Once rewritten, old icechunk versions cannot read the config.
This matches the existing v1→v2 migration precedent. No data loss is possible since
old configs are backed up.

**No format version field needed**: The deserializer can distinguish old from new format
by inspecting the structure of the `store` value (presence of `object_store_type` key).

### Open question: `S3Error(String)` vs `S3Error(Box<dyn Error>)`

Need to check whether any code inspects specific S3 error variants for retry logic.
`String` is simplest and most WASM-friendly; `Box<dyn Error>` preserves more diagnostic
info. Check before implementing Step 1.

## Notes

- `typetag` works under `wasm32-wasip1-threads` with napi-rs -- **confirmed via spike**.
  Registration happens automatically through napi-rs's `__napi_register__*` export calls
  at module init time. No manual `__wasm_call_ctors` needed.
  See also <https://github.com/dtolnay/typetag/pull/96>.
- The `Storage` trait already uses `typetag` for serialization, so this pattern is
  established in the codebase.
- `Repository`, `Session`, `Store`, and `AssetManager` already use
  `Arc<dyn Storage + Send + Sync>` and need no changes.
- The Python bindings' `PyStorage` class methods (`new_s3`, `new_gcs`, etc.) stay as-is
  since they call factory functions directly, not `ObjectStoreConfig`.
- `PyObjectStoreConfig` needs updating in Step 2 to construct backend-specific config
  structs (e.g. `S3StoreConfig`, `GcsStoreConfig`) instead of the removed enum.
  Its API surface to Python can remain identical.
