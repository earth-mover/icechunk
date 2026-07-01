//! [`Storage`](icechunk_storage::Storage) implementation using the `object_store` crate.
//!
//! Supports local filesystem, in-memory, Azure Blob, and Google Cloud Storage.

pub use object_store;

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, TimeDelta, Utc};
use futures::{
    Stream, StreamExt as _, TryStreamExt as _,
    stream::{self, BoxStream},
};
use http::header::{HeaderName, HeaderValue};
#[cfg(feature = "s3")]
use icechunk_storage::s3_config::{S3Credentials, S3Options};
use icechunk_storage::strip_quotes;
use icechunk_storage::{
    ConcurrencySettings, DeleteObjectsResult, ETag, Generation, GetModifiedResult,
    ListInfo, RepositoryCreation, RetriesSettings, Settings, Storage, StorageError,
    StorageErrorKind, StorageInfo, StorageResult, VersionInfo, VersionedUpdateResult,
    obj_not_found_res, obj_store_error, obj_store_error_res, other_error, sealed,
};
use icechunk_types::ICResultExt as _;
#[cfg(any(feature = "s3", feature = "gcs", feature = "azure", feature = "http"))]
use object_store::ClientConfigKey;
#[cfg(feature = "s3")]
use object_store::aws::AmazonS3Builder;
#[cfg(feature = "azure")]
use object_store::azure::{
    AzureAccessKey, AzureConfigKey, AzureCredential, MicrosoftAzureBuilder,
};
#[cfg(feature = "gcs")]
use object_store::gcp::{GcpCredential, GoogleCloudStorageBuilder, GoogleConfigKey};
#[cfg(feature = "http")]
use object_store::http::HttpBuilder;
#[cfg(feature = "fs")]
use object_store::local::LocalFileSystem;
use object_store::{
    Attribute, AttributeValue, Attributes, GetOptions, ObjectMeta, ObjectStore,
    ObjectStoreExt as _, PutMode, PutOptions, UpdateVersion, memory::InMemory,
    path::Path as ObjectPath,
};
#[cfg(any(feature = "s3", feature = "gcs", feature = "azure", feature = "http"))]
use object_store::{BackoffConfig, RetryConfig};
#[cfg(any(feature = "s3", feature = "gcs", feature = "http"))]
use object_store::{ClientOptions, HeaderMap};
#[cfg(any(feature = "gcs", feature = "azure"))]
use object_store::{CredentialProvider, StaticCredentialProvider};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    fmt::{self, Debug, Display},
    future::ready,
    num::{NonZeroU16, NonZeroU64},
    ops::Range,
    path::{Path as StdPath, PathBuf},
    pin::Pin,
    sync::Arc,
};
use tokio::sync::{OnceCell, RwLock};

use tokio_util::io::StreamReader;
use tracing::instrument;
use url::Url;

/// Whether a storage operation reads from or writes to the object store.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Read,
    Write,
}

fn parse_header(k: &str, v: &str) -> Result<(HeaderName, HeaderValue), StorageError> {
    let name = k
        .parse::<HeaderName>()
        .map_err(|e| other_error(format!("invalid HTTP header name {k:?}: {e}")))?;
    let value = HeaderValue::from_str(v)
        .map_err(|e| other_error(format!("invalid HTTP header value for {k:?}: {e}")))?;
    Ok((name, value))
}

/// Validate that each name/value parses as a well-formed HTTP header, so invalid
/// input fails early (at storage construction) with a clear error rather than
/// surfacing mid-request. Used by the bindings layer before threading
/// user-supplied headers into a backend.
pub fn validate_extra_headers(headers: &[(String, String)]) -> Result<(), StorageError> {
    for (k, v) in headers {
        parse_header(k, v)?;
    }
    Ok(())
}

/// All [`ClientConfigKey`]s. The enum has no iterator, so this must be
/// kept in sync with `object_store`; a missing entry only means a newly-added
/// client option wouldn't be preserved when custom headers are set, so, not terrible
#[cfg(any(feature = "s3", feature = "gcs"))]
const CLIENT_CONFIG_KEYS: &[ClientConfigKey] = &[
    ClientConfigKey::AllowHttp,
    ClientConfigKey::AllowInvalidCertificates,
    ClientConfigKey::ConnectTimeout,
    ClientConfigKey::DefaultContentType,
    ClientConfigKey::Http1Only,
    ClientConfigKey::Http2KeepAliveInterval,
    ClientConfigKey::Http2KeepAliveTimeout,
    ClientConfigKey::Http2KeepAliveWhileIdle,
    ClientConfigKey::Http2MaxFrameSize,
    ClientConfigKey::Http2Only,
    ClientConfigKey::PoolIdleTimeout,
    ClientConfigKey::PoolMaxIdlePerHost,
    ClientConfigKey::ProxyUrl,
    ClientConfigKey::ProxyCaCertificate,
    ClientConfigKey::ProxyExcludes,
    ClientConfigKey::RandomizeAddresses,
    ClientConfigKey::Timeout,
    ClientConfigKey::UserAgent,
];

/// Build a [`HeaderMap`] from name/value pairs, failing on invalid input so
/// the error surfaces at construction rather than mid-request.
#[cfg(any(feature = "s3", feature = "gcs", feature = "http"))]
fn headers_to_header_map(
    headers: &[(String, String)],
) -> Result<HeaderMap, StorageError> {
    let mut map = HeaderMap::with_capacity(headers.len());
    for (k, v) in headers {
        let (name, value) = parse_header(k, v)?;
        map.insert(name, value);
    }
    Ok(map)
}

/// Build a [`ClientOptions`] carrying `header_map` as default headers, while
/// preserving the client options already resolved on a builder.
///
/// `object_store`'s only way to set default headers is
/// [`ClientOptions::with_default_headers`], and applying it via
/// `with_client_options` *replaces* the builder's whole [`ClientOptions`] (no
/// merge, no getter). To avoid dropping env- or config-derived client options
/// (e.g. a proxy set through `AWS_*`), we read each known [`ClientConfigKey`]
/// back through `read_config` and re-apply it alongside the headers.
#[cfg(any(feature = "s3", feature = "gcs"))]
fn client_options_with_headers<F>(header_map: HeaderMap, read_config: F) -> ClientOptions
where
    F: Fn(ClientConfigKey) -> Option<String>,
{
    let mut opts = ClientOptions::new().with_default_headers(header_map);
    for &key in CLIENT_CONFIG_KEYS {
        if let Some(value) = read_config(key) {
            opts = opts.with_config(key, value);
        }
    }
    opts
}

/// Sort headers so equal sets compare equal regardless of order (see
/// `read_write_headers_differ`).
#[cfg(any(feature = "s3", feature = "gcs"))]
fn sorted_headers(mut headers: Vec<(String, String)>) -> Vec<(String, String)> {
    headers.sort_unstable();
    headers
}

// --- GCS credential types ---

#[cfg(feature = "gcs")]
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
// We need to adjacently tag because we more than one variant with matching inner types https://github.com/serde-rs/serde/issues/1307
#[serde(tag = "gcs_static_credential_type", content = "__field0")]
#[serde(rename_all = "snake_case")]
pub enum GcsStaticCredentials {
    ServiceAccount(PathBuf),
    ServiceAccountKey(String),
    ApplicationCredentials(PathBuf),
    BearerToken(GcsBearerCredential),
}

#[cfg(feature = "gcs")]
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct GcsBearerCredential {
    pub bearer: String,
    pub expires_after: Option<DateTime<Utc>>,
}

#[cfg(feature = "gcs")]
impl From<&GcsBearerCredential> for GcpCredential {
    fn from(value: &GcsBearerCredential) -> Self {
        GcpCredential { bearer: value.bearer.clone() }
    }
}

#[cfg(feature = "gcs")]
#[async_trait]
#[typetag::serde(tag = "gcs_credentials_fetcher_type")]
pub trait GcsCredentialsFetcher: Debug + Sync + Send {
    async fn get(&self) -> Result<GcsBearerCredential, String>;
}

/// Google Cloud Storage authentication credentials.
#[cfg(feature = "gcs")]
#[derive(Clone, Debug, Deserialize, Serialize, Default)]
#[serde(tag = "gcs_credential_type")]
#[serde(rename_all = "snake_case")]
pub enum GcsCredentials {
    #[default]
    FromEnv,
    Anonymous,
    Static(GcsStaticCredentials),
    Refreshable(Arc<dyn GcsCredentialsFetcher>),
}

// --- Azure credential types ---

#[cfg(feature = "azure")]
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
// We need to adjacently tag because we more than one variant with matching inner types https://github.com/serde-rs/serde/issues/1307
#[serde(tag = "az_static_credential_type", content = "__field0")]
#[serde(rename_all = "snake_case")]
pub enum AzureStaticCredentials {
    AccessKey(String),
    SASToken(String),
    BearerToken(String),
}

/// A refreshable Azure credential with optional expiration.
///
/// Mirrors [`AzureStaticCredentials`] but includes an expiration time so the
/// credential provider knows when to call the fetcher again.
#[cfg(feature = "azure")]
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(tag = "az_refreshable_credential_type", content = "__field0")]
#[serde(rename_all = "snake_case")]
pub enum AzureRefreshableCredential {
    AccessKey { key: String, expires_after: Option<DateTime<Utc>> },
    SASToken { token: String, expires_after: Option<DateTime<Utc>> },
    BearerToken { bearer: String, expires_after: Option<DateTime<Utc>> },
}

#[cfg(feature = "azure")]
impl AzureRefreshableCredential {
    pub fn expires_after(&self) -> Option<DateTime<Utc>> {
        match self {
            Self::AccessKey { expires_after, .. }
            | Self::SASToken { expires_after, .. }
            | Self::BearerToken { expires_after, .. } => *expires_after,
        }
    }
}

#[cfg(feature = "azure")]
#[async_trait]
#[typetag::serde(tag = "az_credentials_fetcher_type")]
pub trait AzureCredentialsFetcher: Debug + Sync + Send {
    async fn get(&self) -> Result<AzureRefreshableCredential, String>;
}

/// Azure Blob Storage authentication credentials.
#[cfg(feature = "azure")]
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "az_credential_type")]
#[serde(rename_all = "snake_case")]
pub enum AzureCredentials {
    FromEnv,
    Anonymous,
    Static(AzureStaticCredentials),
    Refreshable(Arc<dyn AzureCredentialsFetcher>),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ObjectStorage {
    backend: Arc<dyn ObjectStoreBackend>,
    /// Test/internal escape hatch permitting repository creation at an empty
    /// prefix.
    #[serde(skip)]
    allow_empty_prefix_creation: bool,
    /// Lazily-built client for read operations. We use `OnceCell` to allow async
    /// initialization, because serde does not support async function calls from
    /// deserialization.
    ///
    /// When the backend's read and write header sets are equal (the common case,
    /// including no custom headers at all) this single client serves both roles
    /// and `write_client` stays empty — preserving the single-connection-pool
    /// behavior. The two cells diverge only when read and write headers differ.
    #[serde(skip)]
    read_client: OnceCell<Arc<dyn ObjectStore>>,
    #[serde(skip)]
    write_client: OnceCell<Arc<dyn ObjectStore>>,
}

impl ObjectStorage {
    fn from_backend(backend: Arc<dyn ObjectStoreBackend>) -> ObjectStorage {
        ObjectStorage {
            backend,
            allow_empty_prefix_creation: false,
            read_client: OnceCell::new(),
            write_client: OnceCell::new(),
        }
    }

    /// Test/internal escape hatch: permit creating a new repository at an empty
    /// prefix (the bucket root) on a cloud object store, which
    /// [`Storage::can_create_repository`] would otherwise refuse.
    pub fn unsafe_allow_empty_prefix_creation(mut self) -> Self {
        self.allow_empty_prefix_creation = true;
        self
    }

    /// Create an in memory Storage implementation
    ///
    /// This implementation should not be used in production code.
    pub async fn new_in_memory() -> Result<ObjectStorage, StorageError> {
        let backend = Arc::new(InMemoryObjectStoreBackend);
        let storage = ObjectStorage::from_backend(backend);
        Ok(storage)
    }

    /// Create an local filesystem Storage implementation
    ///
    /// This implementation should not be used in production code.
    #[cfg(feature = "fs")]
    pub async fn new_local_filesystem(
        prefix: &StdPath,
    ) -> Result<ObjectStorage, StorageError> {
        tracing::warn!(
            "The LocalFileSystem storage is not safe for concurrent commits. If more than one thread/process will attempt to commit at the same time, prefer using object stores."
        );
        let backend =
            Arc::new(LocalFileSystemObjectStoreBackend { path: prefix.to_path_buf() });
        let storage = ObjectStorage::from_backend(backend);
        Ok(storage)
    }

    #[cfg(feature = "s3")]
    pub async fn new_s3(
        bucket: String,
        prefix: Option<String>,
        credentials: Option<S3Credentials>,
        config: Option<S3Options>,
        extra_read_headers: Vec<(String, String)>,
        extra_write_headers: Vec<(String, String)>,
    ) -> Result<ObjectStorage, StorageError> {
        let backend = Arc::new(S3ObjectStoreBackend {
            bucket,
            prefix,
            credentials,
            config,
            extra_read_headers: sorted_headers(extra_read_headers),
            extra_write_headers: sorted_headers(extra_write_headers),
        });
        let storage = ObjectStorage::from_backend(backend);

        Ok(storage)
    }

    #[cfg(feature = "azure")]
    pub async fn new_azure(
        account: String,
        container: String,
        prefix: Option<String>,
        credentials: Option<AzureCredentials>,
        config: Option<HashMap<AzureConfigKey, String>>,
    ) -> Result<ObjectStorage, StorageError> {
        let backend = Arc::new(AzureObjectStoreBackend {
            account,
            container,
            prefix,
            credentials,
            config,
        });
        let storage = ObjectStorage::from_backend(backend);

        Ok(storage)
    }

    #[cfg(feature = "gcs")]
    pub fn new_gcs(
        bucket: String,
        prefix: Option<String>,
        credentials: Option<GcsCredentials>,
        config: Option<HashMap<GoogleConfigKey, String>>,
        extra_read_headers: Vec<(String, String)>,
        extra_write_headers: Vec<(String, String)>,
    ) -> Result<ObjectStorage, StorageError> {
        let backend = Arc::new(GcsObjectStoreBackend {
            bucket,
            prefix,
            credentials,
            config,
            extra_read_headers: sorted_headers(extra_read_headers),
            extra_write_headers: sorted_headers(extra_write_headers),
        });
        let storage = ObjectStorage::from_backend(backend);

        Ok(storage)
    }

    #[cfg(feature = "http")]
    pub fn new_http(
        url: &Url,
        config: Option<HashMap<ClientConfigKey, String>>,
        headers: Option<HashMap<String, String>>,
    ) -> Result<ObjectStorage, StorageError> {
        let backend =
            Arc::new(HttpObjectStoreBackend { url: url.to_string(), config, headers });
        let storage = ObjectStorage::from_backend(backend);
        Ok(storage)
    }

    /// Get the client, initializing it if it hasn't been initialized yet. This is necessary because the
    /// client is not serializeable and must be initialized after deserialization. Under normal construction
    /// the original client is returned immediately.
    #[instrument(skip_all)]
    async fn get_client(
        &self,
        settings: &Settings,
        role: Role,
    ) -> StorageResult<&Arc<dyn ObjectStore>> {
        // When read and write headers are equal (the default, and the
        // no-custom-headers case), a single client serves both roles so we keep
        // exactly one connection pool. Only when they differ do we build a
        // second, role-specific client.
        if self.backend.read_write_headers_differ() {
            match role {
                Role::Read => {
                    self.read_client
                        .get_or_try_init(|| async {
                            self.backend.mk_object_store(settings, Role::Read)
                        })
                        .await
                }
                Role::Write => {
                    self.write_client
                        .get_or_try_init(|| async {
                            self.backend.mk_object_store(settings, Role::Write)
                        })
                        .await
                }
            }
        } else {
            self.read_client
                .get_or_try_init(|| async {
                    self.backend.mk_object_store(settings, Role::Read)
                })
                .await
        }
    }

    /// We need this because `object_store`'s local file implementation doesn't sort refs. Since this
    /// implementation is used only for tests, it's OK to sort in memory.
    pub fn artificially_sort_refs_in_mem(&self) -> bool {
        self.backend.artificially_sort_refs_in_mem()
    }

    /// Return all keys in the store
    ///
    /// Intended for testing and debugging purposes only.
    pub async fn all_keys(&self) -> StorageResult<Vec<String>> {
        self.get_client(&self.backend.default_settings(), Role::Read)
            .await?
            .list(None)
            .map_ok(|obj| obj.location.to_string())
            .try_collect()
            .await
            .capture_box()
    }

    fn prefixed_path(&self, path: &str) -> ObjectPath {
        let path = format!("{}/{path}", self.backend.prefix());
        ObjectPath::from(path)
    }

    fn metadata_to_attributes(
        &self,
        settings: &Settings,
        metadata: Vec<(String, String)>,
    ) -> Attributes {
        if settings.unsafe_use_metadata() {
            Attributes::from_iter(metadata.into_iter().map(|(key, val)| {
                (
                    Attribute::Metadata(std::borrow::Cow::Owned(key)),
                    AttributeValue::from(val),
                )
            }))
        } else {
            Attributes::new()
        }
    }

    fn get_put_mode(
        &self,
        settings: &Settings,
        previous_version: Option<&VersionInfo>,
    ) -> PutMode {
        if let Some(previous_version) = previous_version {
            match (
                previous_version.is_create(),
                settings.unsafe_use_conditional_create(),
                settings.unsafe_use_conditional_update(),
            ) {
                (true, true, _) => PutMode::Create,
                (true, false, _) => PutMode::Overwrite,

                (false, _, true) => PutMode::Update(UpdateVersion {
                    e_tag: previous_version.etag().cloned(),
                    version: previous_version.generation().cloned(),
                }),
                (false, _, false) => PutMode::Overwrite,
            }
        } else {
            PutMode::Overwrite
        }
    }
}

impl Display for ObjectStorage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ObjectStorage(backend={})", self.backend)
    }
}

impl sealed::Sealed for ObjectStorage {}

#[async_trait]
#[typetag::serde]
impl Storage for ObjectStorage {
    fn storage_info(&self) -> StorageInfo {
        self.backend.storage_info()
    }

    async fn can_write(&self) -> StorageResult<bool> {
        Ok(self.backend.can_write())
    }

    async fn can_create_repository(&self) -> StorageResult<RepositoryCreation> {
        if self.backend.prefix().is_empty()
            && !self.allow_empty_prefix_creation
            && self.backend.restricts_empty_prefix_creation()
        {
            Ok(RepositoryCreation::RefusedEmptyPrefix)
        } else {
            Ok(RepositoryCreation::Allowed)
        }
    }

    async fn create_location_if_needed(&self) -> StorageResult<()> {
        self.backend.create_location_if_needed()
    }

    #[instrument(skip_all)]
    async fn default_settings(&self) -> StorageResult<Settings> {
        Ok(self.backend.default_settings())
    }

    async fn put_object(
        &self,
        settings: &Settings,
        path: &str,
        bytes: Bytes,
        content_type: Option<&str>,
        metadata: Vec<(String, String)>,
        previous_version: Option<&VersionInfo>,
    ) -> StorageResult<VersionedUpdateResult> {
        let path = self.prefixed_path(path);
        let mut attributes = Attributes::new();
        if settings.unsafe_use_metadata() {
            if let Some(content_type) = content_type {
                attributes.insert(
                    Attribute::ContentType,
                    AttributeValue::from(content_type.to_string()),
                );
            }
            for (att, value) in self.metadata_to_attributes(settings, metadata).iter() {
                attributes.insert(att.clone(), value.clone());
            }
        };

        let mode = self.get_put_mode(settings, previous_version);
        let options = PutOptions { mode, attributes, ..PutOptions::default() };
        // FIXME: use multipart
        let res = self
            .get_client(settings, Role::Write)
            .await?
            .put_opts(&path, bytes.into(), options)
            .await;
        match res {
            Ok(res) => {
                let new_version = VersionInfo {
                    etag: res.e_tag.map(ETag),
                    generation: res.version.map(Generation),
                };
                Ok(VersionedUpdateResult::Updated { new_version })
            }
            Err(object_store::Error::Precondition { .. })
            | Err(object_store::Error::AlreadyExists { .. }) => {
                Ok(VersionedUpdateResult::NotOnLatestVersion)
            }
            Err(err) => {
                Err(StorageError::capture(StorageErrorKind::ObjectStore(Box::new(err))))
            }
        }
    }

    async fn copy_object(
        &self,
        settings: &Settings,
        from: &str,
        to: &str,
        _content_type: Option<&str>,
        version: &VersionInfo,
    ) -> StorageResult<VersionedUpdateResult> {
        let from = self.prefixed_path(from);
        let to = self.prefixed_path(to);

        if settings.unsafe_use_conditional_update() && version.etag().is_some() {
            // object_store has no conditional copy, so we do a conditional GET
            // (verifying the source etag) followed by a PUT.
            let opts = GetOptions {
                if_match: version.etag().map(|e| strip_quotes(e).into()),
                ..Default::default()
            };
            let result =
                self.get_client(settings, Role::Write).await?.get_opts(&from, opts).await;
            match result {
                Ok(result) => {
                    let bytes = result
                        .bytes()
                        .await
                        .map_err(|e| StorageErrorKind::ObjectStore(Box::new(e)))
                        .capture()?;
                    self.get_client(settings, Role::Write)
                        .await?
                        .put(&to, bytes.into())
                        .await
                        .map_err(|e| StorageErrorKind::ObjectStore(Box::new(e)))
                        .capture()?;
                    Ok(VersionedUpdateResult::Updated { new_version: version.clone() })
                }
                Err(object_store::Error::Precondition { .. }) => {
                    Ok(VersionedUpdateResult::NotOnLatestVersion)
                }
                Err(object_store::Error::NotFound { .. }) => obj_not_found_res(),
                Err(err) => Err(obj_store_error(err)),
            }
        } else {
            match self.get_client(settings, Role::Write).await?.copy(&from, &to).await {
                Ok(_) => {
                    Ok(VersionedUpdateResult::Updated { new_version: version.clone() })
                }
                Err(object_store::Error::NotFound { .. }) => obj_not_found_res(),
                Err(err) => Err(obj_store_error(err)),
            }
        }
    }

    #[instrument(skip(self, settings))]
    async fn list_objects<'a>(
        &'a self,
        settings: &Settings,
        prefix: &str,
    ) -> StorageResult<BoxStream<'a, StorageResult<ListInfo<String>>>> {
        let prefix = ObjectPath::from(format!("{}/{}", self.backend.prefix(), prefix));
        let stream =
            self.get_client(settings, Role::Read).await?.list(Some(&prefix)).map(
                move |object| {
                    let prefix = prefix.clone();
                    object
                        .map_err(obj_store_error)
                        .and_then(|object| object_to_list_info(&prefix, &object))
                },
            );
        Ok(stream.boxed())
    }

    #[instrument(skip(self, batch))]
    async fn delete_batch(
        &self,
        settings: &Settings,
        prefix: &str,
        batch: Vec<(String, u64)>,
    ) -> StorageResult<DeleteObjectsResult> {
        let mut sizes = HashMap::new();
        let mut ids = Vec::new();
        for (id, size) in batch {
            let path = self.prefixed_path(format!("{prefix}/{}", id.as_str()).as_str());
            ids.push(Ok(path.clone()));
            sizes.insert(path, size);
        }
        let results = self
            .get_client(settings, Role::Write)
            .await?
            .delete_stream(stream::iter(ids).boxed());
        let res = results
            .fold(DeleteObjectsResult::default(), |mut res, delete_result| {
                if let Ok(deleted_path) = delete_result {
                    if let Some(size) = sizes.get(&deleted_path) {
                        res.deleted_objects += 1;
                        res.deleted_bytes += *size;
                    }
                } else {
                    tracing::error!(
                        error = ?delete_result,
                        "Error deleting object",
                    );
                }
                ready(res)
            })
            .await;
        Ok(res)
    }

    #[instrument(skip(self, settings))]
    async fn get_object_last_modified(
        &self,
        path: &str,
        settings: &Settings,
    ) -> StorageResult<DateTime<Utc>> {
        let path = self.prefixed_path(path);
        let res = self
            .get_client(settings, Role::Read)
            .await?
            .head(&path)
            .await
            .map_err(Box::new)
            .capture_box()?;
        Ok(res.last_modified)
    }

    #[instrument(skip(self, settings))]
    async fn get_object_conditional(
        &self,
        settings: &Settings,
        path: &str,
        previous_version: Option<&VersionInfo>,
    ) -> StorageResult<GetModifiedResult> {
        match self
            .get_object_range_conditional(settings, path, None, previous_version)
            .await
        {
            Ok(Some((stream, new_version))) => {
                let reader = StreamReader::new(stream.map_err(std::io::Error::other));
                Ok(GetModifiedResult::Modified { data: Box::pin(reader), new_version })
            }
            Ok(None) => Ok(GetModifiedResult::OnLatestVersion),
            Err(e) => Err(e),
        }
    }

    #[instrument(skip(self))]
    async fn get_object_range(
        &self,
        settings: &Settings,
        path: &str,
        range: Option<&Range<u64>>,
    ) -> StorageResult<(
        Pin<Box<dyn Stream<Item = Result<Bytes, StorageError>> + Send>>,
        VersionInfo,
    )> {
        self.get_object_range_conditional(settings, path, range, None).await.map(|v| {
            // If we got a result, then we can unwrap safely here:
            // Errors would be in the other branch, and None is only expected
            // if previous_version was passed in function call, but we set it to None
            #[expect(clippy::expect_used)]
            v.expect("Logic bug in get_object_range_conditional, should not get None")
        })
    }
}

impl ObjectStorage {
    async fn get_object_range_conditional(
        &self,
        settings: &Settings,
        path: &str,
        range: Option<&Range<u64>>,
        previous_version: Option<&VersionInfo>,
    ) -> StorageResult<
        Option<(
            Pin<Box<dyn Stream<Item = Result<Bytes, StorageError>> + Send>>,
            VersionInfo,
        )>,
    > {
        let full_key = self.prefixed_path(path);
        let range = range.map(|range| {
            let usize_range = range.start..range.end;
            usize_range.into()
        });
        let opts = GetOptions {
            range,
            if_none_match: previous_version
                .as_ref()
                .and_then(|v| v.etag().map(|e| strip_quotes(e).into())),
            ..Default::default()
        };
        let res =
            self.get_client(settings, Role::Read).await?.get_opts(&full_key, opts).await;

        match res {
            Ok(result) => {
                let version = VersionInfo {
                    etag: result.meta.e_tag.as_ref().cloned().map(ETag),
                    generation: result.meta.version.as_ref().cloned().map(Generation),
                };
                let stream = Box::pin(result.into_stream().map_err(obj_store_error));
                Ok(Some((stream, version)))
            }
            Err(object_store::Error::NotFound { .. }) => obj_not_found_res(),
            Err(object_store::Error::NotModified { .. }) => Ok(None),
            Err(err) => obj_store_error_res(err),
        }
    }
}

#[typetag::serde(tag = "object_store_provider_type")]
pub trait ObjectStoreBackend: Debug + Display + Sync + Send {
    /// Build the underlying `object_store` client for the given `role`.
    ///
    /// `role` selects which custom-header set (read vs write) the client carries.
    /// Backends without per-role headers (or without an HTTP layer) ignore it.
    fn mk_object_store(
        &self,
        settings: &Settings,
        role: Role,
    ) -> Result<Arc<dyn ObjectStore>, StorageError>;

    /// Whether this backend's read and write header sets differ.
    ///
    /// When `false` (the default, and always the case with no custom headers),
    /// [`ObjectStorage`] uses a single shared client for both roles. When `true`,
    /// it builds two role-specific clients.
    fn read_write_headers_differ(&self) -> bool {
        false
    }

    /// The prefix for the object store.
    fn prefix(&self) -> String;

    /// Return structured metadata about this backend for display/repr.
    fn storage_info(&self) -> StorageInfo;

    /// We need this because `object_store`'s local file implementation doesn't sort refs. Since this
    /// implementation is used only for tests, it's OK to sort in memory.
    fn artificially_sort_refs_in_mem(&self) -> bool {
        false
    }

    fn default_settings(&self) -> Settings;

    fn can_write(&self) -> bool {
        true
    }

    /// Whether this backend should refuse creating a new repository at an empty
    /// prefix (the bucket root).
    fn restricts_empty_prefix_creation(&self) -> bool {
        false
    }

    fn create_location_if_needed(&self) -> Result<(), StorageError> {
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct InMemoryObjectStoreBackend;

impl Display for InMemoryObjectStoreBackend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "InMemoryObjectStoreBackend")
    }
}

#[typetag::serde(name = "in_memory_object_store_provider")]
impl ObjectStoreBackend for InMemoryObjectStoreBackend {
    fn storage_info(&self) -> StorageInfo {
        StorageInfo { backend_type: "in-memory", fields: vec![] }
    }

    fn mk_object_store(
        &self,
        _settings: &Settings,
        _role: Role,
    ) -> Result<Arc<dyn ObjectStore>, StorageError> {
        Ok(Arc::new(InMemory::new()))
    }

    fn prefix(&self) -> String {
        "".to_string()
    }

    fn default_settings(&self) -> Settings {
        Settings {
            concurrency: Some(ConcurrencySettings {
                // we do != 1 because we use this store for tests
                max_concurrent_requests_for_object: Some(
                    NonZeroU16::new(5).unwrap_or(NonZeroU16::MIN),
                ),
                ideal_concurrent_request_size: Some(
                    NonZeroU64::new(1).unwrap_or(NonZeroU64::MIN),
                ),
            }),
            retries: Some(RetriesSettings {
                max_tries: Some(NonZeroU16::MIN),
                initial_backoff_ms: Some(0),
                max_backoff_ms: Some(0),
            }),

            ..Default::default()
        }
    }
}

#[cfg(feature = "fs")]
#[derive(Debug, Serialize, Deserialize)]
pub struct LocalFileSystemObjectStoreBackend {
    path: PathBuf,
}

#[cfg(feature = "fs")]
impl Display for LocalFileSystemObjectStoreBackend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "LocalFileSystemObjectStoreBackend(path={})", self.path.display())
    }
}

#[cfg(feature = "fs")]
#[typetag::serde(name = "local_file_system_object_store_provider")]
impl ObjectStoreBackend for LocalFileSystemObjectStoreBackend {
    fn storage_info(&self) -> StorageInfo {
        StorageInfo {
            backend_type: "local filesystem",
            fields: vec![("path", self.path.display().to_string())],
        }
    }

    fn mk_object_store(
        &self,
        _settings: &Settings,
        _role: Role,
    ) -> Result<Arc<dyn ObjectStore>, StorageError> {
        let path = std::fs::canonicalize(&self.path).map_err(|err| {
            if err.kind() == std::io::ErrorKind::NotFound {
                StorageError::capture(StorageErrorKind::ObjectNotFound)
            } else {
                StorageError::capture(StorageErrorKind::IOError(err))
            }
        })?;
        let fs = LocalFileSystem::new_with_prefix(path).capture_box()?;
        Ok(Arc::new(fs))
    }

    fn prefix(&self) -> String {
        "".to_string()
    }

    fn artificially_sort_refs_in_mem(&self) -> bool {
        true
    }

    fn default_settings(&self) -> Settings {
        Settings {
            concurrency: Some(ConcurrencySettings {
                max_concurrent_requests_for_object: Some(
                    NonZeroU16::new(5).unwrap_or(NonZeroU16::MIN),
                ),
                ideal_concurrent_request_size: Some(
                    NonZeroU64::new(4 * 1024).unwrap_or(NonZeroU64::MIN),
                ),
            }),
            unsafe_use_conditional_update: Some(false),
            unsafe_use_metadata: Some(false),
            retries: Some(RetriesSettings {
                max_tries: Some(NonZeroU16::new(1).unwrap_or(NonZeroU16::MIN)),
                initial_backoff_ms: Some(0),
                max_backoff_ms: Some(0),
            }),
            ..Default::default()
        }
    }

    fn create_location_if_needed(&self) -> Result<(), StorageError> {
        std::fs::create_dir_all(&self.path).capture()?;
        Ok(())
    }
}

#[cfg(feature = "http")]
#[derive(Debug, Serialize, Deserialize)]
pub struct HttpObjectStoreBackend {
    pub url: String,
    pub config: Option<HashMap<ClientConfigKey, String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub headers: Option<HashMap<String, String>>,
}

#[cfg(feature = "http")]
impl Display for HttpObjectStoreBackend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let config_str = self
            .config
            .as_ref()
            .map(|c| {
                c.iter().map(|(k, v)| format!("{k:?}={v}")).collect::<Vec<_>>().join(", ")
            })
            .unwrap_or_else(|| "None".to_string());
        let headers_str = self
            .headers
            .as_ref()
            .map(|h| {
                h.keys().map(|k| format!("{k}=<redacted>")).collect::<Vec<_>>().join(", ")
            })
            .unwrap_or_else(|| "None".to_string());
        write!(
            f,
            "HttpObjectStoreBackend(url={}, config={}, headers=[{}])",
            self.url, config_str, headers_str
        )
    }
}

#[cfg(feature = "http")]
#[typetag::serde(name = "http_object_store_provider")]
impl ObjectStoreBackend for HttpObjectStoreBackend {
    fn storage_info(&self) -> StorageInfo {
        StorageInfo { backend_type: "HTTP", fields: vec![("url", self.url.clone())] }
    }

    fn mk_object_store(
        &self,
        settings: &Settings,
        // HTTP storage is read-only; its `headers` apply to every (read) request.
        _role: Role,
    ) -> Result<Arc<dyn ObjectStore>, StorageError> {
        let empty = HashMap::new();
        let config = self.config.as_ref().unwrap_or(&empty);

        // Build a single ClientOptions accumulating all settings so that
        // with_client_options (which replaces, not merges) is called exactly once.
        // Start with the icechunk UserAgent default; user-supplied opts applied
        // after so they can override it if needed.
        let mut client_opts = ClientOptions::new()
            .with_config(ClientConfigKey::UserAgent, icechunk_types::user_agent());
        client_opts = config
            .iter()
            .fold(client_opts, |opts, (key, value)| opts.with_config(*key, value));

        // Auto-enable AllowHttp for plain http:// URLs unless the user already set it.
        if !config.contains_key(&ClientConfigKey::AllowHttp)
            && self.url.starts_with("http:")
        {
            client_opts = client_opts.with_allow_http(true);
        }

        if let Some(hdrs) = &self.headers
            && !hdrs.is_empty()
        {
            let mut header_map = HeaderMap::new();
            for (k, v) in hdrs {
                let name = k.parse::<HeaderName>().map_err(|e| {
                    other_error(format!("invalid HTTP header name {k:?}: {e}"))
                })?;
                let value = HeaderValue::from_str(v).map_err(|e| {
                    other_error(format!("invalid HTTP header value for {k:?}: {e}"))
                })?;
                header_map.insert(name, value);
            }
            client_opts = client_opts.with_default_headers(header_map);
        }

        let builder = HttpBuilder::new()
            .with_url(&self.url)
            .with_client_options(client_opts)
            .with_retry(RetryConfig {
                backoff: BackoffConfig {
                    init_backoff: core::time::Duration::from_millis(
                        settings.retries().initial_backoff_ms() as u64,
                    ),
                    max_backoff: core::time::Duration::from_millis(
                        settings.retries().max_backoff_ms() as u64,
                    ),
                    base: 2.,
                },
                max_retries: settings.retries().max_tries().get() as usize - 1,
                retry_timeout: core::time::Duration::from_secs(5 * 60),
            });

        let store = builder.build().capture_box()?;

        Ok(Arc::new(store))
    }

    fn prefix(&self) -> String {
        "".to_string()
    }

    fn default_settings(&self) -> Settings {
        Default::default()
    }

    fn can_write(&self) -> bool {
        false
    }
}

#[cfg(feature = "s3")]
#[derive(Debug, Serialize, Deserialize)]
pub struct S3ObjectStoreBackend {
    pub bucket: String,
    pub prefix: Option<String>,
    pub credentials: Option<S3Credentials>,
    pub config: Option<S3Options>,
    /// Extra HTTP headers sent on read requests.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub extra_read_headers: Vec<(String, String)>,
    /// Extra HTTP headers sent on write requests.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub extra_write_headers: Vec<(String, String)>,
}

#[cfg(feature = "s3")]
impl Display for S3ObjectStoreBackend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "S3ObjectStoreBackend(bucket={}, prefix={}, config={})",
            self.bucket,
            self.prefix.as_deref().unwrap_or(""),
            self.config.as_ref().map(|c| c.to_string()).unwrap_or("None".to_string())
        )
    }
}

#[cfg(feature = "s3")]
#[typetag::serde(name = "s3_object_store_provider")]
impl ObjectStoreBackend for S3ObjectStoreBackend {
    fn storage_info(&self) -> StorageInfo {
        let mut fields = vec![("bucket", self.bucket.clone())];
        if let Some(prefix) = &self.prefix {
            fields.push(("prefix", prefix.clone()));
        }
        if let Some(config) = &self.config {
            fields.extend(config.info_fields());
        }
        StorageInfo { backend_type: "S3", fields }
    }

    fn mk_object_store(
        &self,
        settings: &Settings,
        role: Role,
    ) -> Result<Arc<dyn ObjectStore>, StorageError> {
        let builder = AmazonS3Builder::new();

        let builder = match self.credentials.as_ref() {
            Some(S3Credentials::Static(credentials)) => {
                let builder = builder
                    .with_access_key_id(credentials.access_key_id.clone())
                    .with_secret_access_key(credentials.secret_access_key.clone());

                if let Some(session_token) = credentials.session_token.as_ref() {
                    builder.with_token(session_token.clone())
                } else {
                    builder
                }
            }
            Some(S3Credentials::Anonymous) => builder.with_skip_signature(true),
            // TODO: Support refreshable credentials
            _ => AmazonS3Builder::from_env(),
        };

        let builder = if let Some(config) = self.config.as_ref() {
            let builder = if let Some(region) = config.region.as_ref() {
                builder.with_region(region.clone())
            } else {
                builder
            };

            let builder = if let Some(endpoint) = config.endpoint_url.as_ref() {
                builder.with_endpoint(endpoint.clone())
            } else {
                builder
            };

            builder
                .with_skip_signature(config.anonymous)
                .with_allow_http(config.allow_http)
        } else {
            builder
        };

        // Defaults
        let builder = builder
            .with_bucket_name(&self.bucket)
            .with_conditional_put(object_store::aws::S3ConditionalPut::ETagMatch)
            .with_config(
                object_store::aws::AmazonS3ConfigKey::Client(ClientConfigKey::UserAgent),
                icechunk_types::user_agent(),
            );

        let builder = builder.with_retry(RetryConfig {
            backoff: BackoffConfig {
                init_backoff: core::time::Duration::from_millis(
                    settings.retries().initial_backoff_ms() as u64,
                ),
                max_backoff: core::time::Duration::from_millis(
                    settings.retries().max_backoff_ms() as u64,
                ),
                base: 2.,
            },
            max_retries: settings.retries().max_tries().get() as usize - 1,
            retry_timeout: core::time::Duration::from_secs(5 * 60),
        });

        // Apply custom headers last: read every resolved client option back out
        // and re-pack it with the headers, so the replacing `with_client_options`
        // doesn't drop env/config-derived options.
        let headers = match role {
            Role::Read => &self.extra_read_headers,
            Role::Write => &self.extra_write_headers,
        };
        let builder = if headers.is_empty() {
            builder
        } else {
            let header_map = headers_to_header_map(headers)?;
            let opts = client_options_with_headers(header_map, |key| {
                builder
                    .get_config_value(&object_store::aws::AmazonS3ConfigKey::Client(key))
            });
            builder.with_client_options(opts)
        };

        let store = builder.build().capture_box()?;
        Ok(Arc::new(store))
    }

    // Headers are stored sorted (see `new_s3`/`new_gcs`), so this is order-insensitive.
    fn read_write_headers_differ(&self) -> bool {
        self.extra_read_headers != self.extra_write_headers
    }

    fn prefix(&self) -> String {
        self.prefix.clone().unwrap_or("".to_string())
    }

    fn restricts_empty_prefix_creation(&self) -> bool {
        true
    }

    fn default_settings(&self) -> Settings {
        Default::default()
    }
}

#[cfg(feature = "azure")]
#[derive(Debug, Serialize, Deserialize)]
pub struct AzureObjectStoreBackend {
    pub account: String,
    pub container: String,
    pub prefix: Option<String>,
    pub credentials: Option<AzureCredentials>,
    pub config: Option<HashMap<AzureConfigKey, String>>,
}

#[cfg(feature = "azure")]
impl Display for AzureObjectStoreBackend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "AzureObjectStoreBackend(account={}, container={}, prefix={})",
            self.account,
            self.container,
            self.prefix.as_deref().unwrap_or("")
        )
    }
}

#[cfg(feature = "azure")]
#[typetag::serde(name = "azure_object_store_provider")]
impl ObjectStoreBackend for AzureObjectStoreBackend {
    fn storage_info(&self) -> StorageInfo {
        let mut fields = vec![
            ("account", self.account.clone()),
            ("container", self.container.clone()),
        ];
        if let Some(prefix) = &self.prefix {
            fields.push(("prefix", prefix.clone()));
        }
        StorageInfo { backend_type: "Azure", fields }
    }

    fn mk_object_store(
        &self,
        settings: &Settings,
        // TODO: Azure custom-header support
        _role: Role,
    ) -> Result<Arc<dyn ObjectStore>, StorageError> {
        let builder = MicrosoftAzureBuilder::new();

        let builder = match self.credentials.as_ref() {
            Some(AzureCredentials::Static(AzureStaticCredentials::AccessKey(key))) => {
                builder.with_access_key(key)
            }
            Some(AzureCredentials::Static(AzureStaticCredentials::SASToken(token))) => {
                builder.with_config(AzureConfigKey::SasKey, token)
            }
            Some(AzureCredentials::Static(AzureStaticCredentials::BearerToken(
                token,
            ))) => builder.with_bearer_token_authorization(token),
            Some(AzureCredentials::Refreshable(fetcher)) => {
                let credential_provider =
                    AzureRefreshableCredentialProvider::new(Arc::clone(fetcher));
                builder.with_credentials(Arc::new(credential_provider))
            }
            Some(AzureCredentials::Anonymous) => builder.with_skip_signature(true),
            None | Some(AzureCredentials::FromEnv) => MicrosoftAzureBuilder::from_env(),
        };

        // Either the account name should be provided or user_emulator should be set to true to use the default account
        let builder = builder
            .with_account(&self.account)
            .with_container_name(&self.container)
            .with_config(
                AzureConfigKey::Client(ClientConfigKey::UserAgent),
                icechunk_types::user_agent(),
            );

        // Add options (user config takes precedence over defaults)
        let builder = self
            .config
            .as_ref()
            .unwrap_or(&HashMap::new())
            .iter()
            .fold(builder, |builder, (key, value)| builder.with_config(*key, value));

        let builder = builder.with_retry(RetryConfig {
            backoff: BackoffConfig {
                init_backoff: core::time::Duration::from_millis(
                    settings.retries().initial_backoff_ms() as u64,
                ),
                max_backoff: core::time::Duration::from_millis(
                    settings.retries().max_backoff_ms() as u64,
                ),
                base: 2.,
            },
            max_retries: settings.retries().max_tries().get() as usize - 1,
            retry_timeout: core::time::Duration::from_secs(5 * 60),
        });

        let store = builder.build().capture_box()?;
        Ok(Arc::new(store))
    }

    fn prefix(&self) -> String {
        self.prefix.clone().unwrap_or("".to_string())
    }

    fn restricts_empty_prefix_creation(&self) -> bool {
        true
    }

    fn default_settings(&self) -> Settings {
        Default::default()
    }
}

#[cfg(feature = "gcs")]
#[derive(Debug, Serialize, Deserialize)]
pub struct GcsObjectStoreBackend {
    pub bucket: String,
    pub prefix: Option<String>,
    pub credentials: Option<GcsCredentials>,
    pub config: Option<HashMap<GoogleConfigKey, String>>,
    /// Extra HTTP headers sent on read requests. Runtime-only: serialized on the
    /// struct (so they travel with pickled repos) but never persisted in the
    /// repository config. `skip_serializing_if` keeps the serialized form
    /// unchanged when unused, for back-compat with existing blobs.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub extra_read_headers: Vec<(String, String)>,
    /// Extra HTTP headers sent on write requests. See [`Self::extra_read_headers`].
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub extra_write_headers: Vec<(String, String)>,
}

#[cfg(feature = "gcs")]
impl Display for GcsObjectStoreBackend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "GcsObjectStoreBackend(bucket={}, prefix={})",
            self.bucket,
            self.prefix.as_deref().unwrap_or("")
        )
    }
}

#[cfg(feature = "gcs")]
#[typetag::serde(name = "gcs_object_store_provider")]
impl ObjectStoreBackend for GcsObjectStoreBackend {
    fn storage_info(&self) -> StorageInfo {
        let mut fields = vec![("bucket", self.bucket.clone())];
        if let Some(prefix) = &self.prefix {
            fields.push(("prefix", prefix.clone()));
        }
        StorageInfo { backend_type: "GCS", fields }
    }

    fn mk_object_store(
        &self,
        settings: &Settings,
        role: Role,
    ) -> Result<Arc<dyn ObjectStore>, StorageError> {
        let builder = GoogleCloudStorageBuilder::new();

        let builder = match self.credentials.as_ref() {
            Some(GcsCredentials::Static(GcsStaticCredentials::ServiceAccount(path))) => {
                let path = path.clone().into_os_string().into_string().map_err(|_| {
                    other_error("invalid service account path".to_string())
                })?;
                builder.with_service_account_path(path)
            }
            Some(GcsCredentials::Static(GcsStaticCredentials::ServiceAccountKey(
                key,
            ))) => builder.with_service_account_key(key),
            Some(GcsCredentials::Static(
                GcsStaticCredentials::ApplicationCredentials(path),
            )) => {
                let path = path.clone().into_os_string().into_string().map_err(|_| {
                    other_error("invalid application credentials path".to_string())
                })?;
                builder.with_application_credentials(path)
            }
            Some(GcsCredentials::Static(GcsStaticCredentials::BearerToken(token))) => {
                let provider = StaticCredentialProvider::new(GcpCredential::from(token));
                builder.with_credentials(Arc::new(provider))
            }
            Some(GcsCredentials::Refreshable(fetcher)) => {
                let credential_provider =
                    GcsRefreshableCredentialProvider::new(Arc::clone(fetcher));
                builder.with_credentials(Arc::new(credential_provider))
            }
            Some(GcsCredentials::Anonymous) => builder.with_skip_signature(true),
            None | Some(GcsCredentials::FromEnv) => GoogleCloudStorageBuilder::from_env(),
        };

        let builder = builder.with_bucket_name(&self.bucket).with_config(
            GoogleConfigKey::Client(ClientConfigKey::UserAgent),
            icechunk_types::user_agent(),
        );

        // Add options (user config takes precedence over defaults)
        let builder = self
            .config
            .as_ref()
            .unwrap_or(&HashMap::new())
            .iter()
            .fold(builder, |builder, (key, value)| builder.with_config(*key, value));

        let builder = builder.with_retry(RetryConfig {
            backoff: BackoffConfig {
                init_backoff: core::time::Duration::from_millis(
                    settings.retries().initial_backoff_ms() as u64,
                ),
                max_backoff: core::time::Duration::from_millis(
                    settings.retries().max_backoff_ms() as u64,
                ),
                base: 2.,
            },
            max_retries: settings.retries().max_tries().get() as usize - 1,
            retry_timeout: core::time::Duration::from_secs(5 * 60),
        });

        // Apply custom headers last, preserving resolved client options. See the
        // matching block in `S3ObjectStoreBackend::mk_object_store` for rationale.
        let headers = match role {
            Role::Read => &self.extra_read_headers,
            Role::Write => &self.extra_write_headers,
        };
        let builder = if headers.is_empty() {
            builder
        } else {
            let header_map = headers_to_header_map(headers)?;
            let opts = client_options_with_headers(header_map, |key| {
                builder.get_config_value(&GoogleConfigKey::Client(key))
            });
            builder.with_client_options(opts)
        };

        let store = builder.build().capture_box()?;
        Ok(Arc::new(store))
    }

    // Headers are stored sorted (see `new_s3`/`new_gcs`), so this is order-insensitive.
    fn read_write_headers_differ(&self) -> bool {
        self.extra_read_headers != self.extra_write_headers
    }

    fn prefix(&self) -> String {
        self.prefix.clone().unwrap_or("".to_string())
    }

    fn restricts_empty_prefix_creation(&self) -> bool {
        true
    }

    fn default_settings(&self) -> Settings {
        Default::default()
    }
}

#[cfg(feature = "gcs")]
#[derive(Debug)]
pub struct GcsRefreshableCredentialProvider {
    last_credential: Arc<RwLock<Option<GcsBearerCredential>>>,
    refresher: Arc<dyn GcsCredentialsFetcher>,
}

#[cfg(feature = "gcs")]
impl GcsRefreshableCredentialProvider {
    pub fn new(refresher: Arc<dyn GcsCredentialsFetcher>) -> Self {
        Self { last_credential: Arc::new(RwLock::new(None)), refresher }
    }

    pub async fn get_or_update_credentials(
        &self,
    ) -> Result<GcsBearerCredential, StorageError> {
        let last_credential = self.last_credential.read().await;

        // If we have a credential and it hasn't expired, return it
        if let Some(creds) = last_credential.as_ref()
            && let Some(expires_after) = creds.expires_after
            && expires_after
                > Utc::now() + TimeDelta::seconds(rand::random_range(120..=180))
        {
            return Ok(creds.clone());
        }

        drop(last_credential);
        let mut last_credential = self.last_credential.write().await;

        // Otherwise, refresh the credential and cache it
        let creds = self.refresher.get().await.map_err(other_error)?;
        *last_credential = Some(creds.clone());
        Ok(creds)
    }
}

#[async_trait]
#[cfg(feature = "gcs")]
impl CredentialProvider for GcsRefreshableCredentialProvider {
    type Credential = GcpCredential;

    async fn get_credential(&self) -> object_store::Result<Arc<Self::Credential>> {
        let creds = self.get_or_update_credentials().await.map_err(|e| {
            object_store::Error::Generic { store: "gcp", source: Box::new(e) }
        })?;
        Ok(Arc::new(GcpCredential::from(&creds)))
    }
}

#[cfg(feature = "azure")]
#[derive(Debug)]
pub struct AzureRefreshableCredentialProvider {
    last_credential: Arc<RwLock<Option<AzureRefreshableCredential>>>,
    refresher: Arc<dyn AzureCredentialsFetcher>,
}

#[cfg(feature = "azure")]
impl AzureRefreshableCredentialProvider {
    pub fn new(refresher: Arc<dyn AzureCredentialsFetcher>) -> Self {
        Self { last_credential: Arc::new(RwLock::new(None)), refresher }
    }

    pub async fn get_or_update_credentials(
        &self,
    ) -> Result<AzureRefreshableCredential, StorageError> {
        let last_credential = self.last_credential.read().await;

        // If we have a credential and it hasn't expired, return it
        if let Some(creds) = last_credential.as_ref()
            && let Some(expires_after) = creds.expires_after()
            && expires_after
                > Utc::now() + TimeDelta::seconds(rand::random_range(120..=180))
        {
            return Ok(creds.clone());
        }

        drop(last_credential);
        let mut last_credential = self.last_credential.write().await;

        // Otherwise, refresh the credential and cache it
        let creds = self.refresher.get().await.map_err(other_error)?;
        *last_credential = Some(creds.clone());
        Ok(creds)
    }
}

#[cfg(feature = "azure")]
fn to_azure_credential(
    cred: &AzureRefreshableCredential,
) -> object_store::Result<AzureCredential> {
    match cred {
        AzureRefreshableCredential::BearerToken { bearer, .. } => {
            Ok(AzureCredential::BearerToken(bearer.clone()))
        }
        AzureRefreshableCredential::SASToken { token, .. } => {
            Ok(AzureCredential::SASToken(
                url::form_urlencoded::parse(token.as_bytes()).into_owned().collect(),
            ))
        }
        AzureRefreshableCredential::AccessKey { key, .. } => {
            Ok(AzureCredential::AccessKey(AzureAccessKey::try_new(key)?))
        }
    }
}

#[async_trait]
#[cfg(feature = "azure")]
impl CredentialProvider for AzureRefreshableCredentialProvider {
    type Credential = AzureCredential;

    async fn get_credential(&self) -> object_store::Result<Arc<Self::Credential>> {
        let creds = self.get_or_update_credentials().await.map_err(|e| {
            object_store::Error::Generic { store: "azure", source: Box::new(e) }
        })?;
        Ok(Arc::new(to_azure_credential(&creds)?))
    }
}

fn object_to_list_info(
    prefix: &ObjectPath,
    object: &ObjectMeta,
) -> StorageResult<ListInfo<String>> {
    let created_at = object.last_modified;
    let id =
        ObjectPath::from_iter(object.location.prefix_match(prefix).ok_or_else(|| {
            StorageError::capture(StorageErrorKind::BadPrefix(
                object.location.to_string().into(),
            ))
        })?)
        .to_string();
    let size_bytes = object.size;
    Ok(ListInfo { id, created_at, size_bytes })
}

#[cfg(all(test, feature = "fs"))]
mod tests {
    use std::path::PathBuf;

    use icechunk_macros::tokio_test;
    use tempfile::TempDir;

    use super::ObjectStorage;

    #[tokio_test]
    async fn test_serialize_object_store() {
        let tmp_dir = TempDir::new().unwrap();
        let store = ObjectStorage::new_local_filesystem(tmp_dir.path()).await.unwrap();

        let serialized = serde_json::to_string(&store).unwrap();

        let deserialized: ObjectStorage = serde_json::from_str(&serialized).unwrap();
        assert_eq!(
            store.backend.as_ref().prefix(),
            deserialized.backend.as_ref().prefix()
        );
    }

    struct TestLocalPath(String);

    impl From<&TestLocalPath> for PathBuf {
        fn from(path: &TestLocalPath) -> Self {
            PathBuf::from(&path.0)
        }
    }

    impl Drop for TestLocalPath {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    #[tokio_test]
    async fn test_canonicalize_path() {
        // Absolute path
        let tmp_dir = TempDir::new().unwrap();
        let store = ObjectStorage::new_local_filesystem(tmp_dir.path()).await;
        assert!(store.is_ok());

        // Relative path
        let rel_path = "relative/path";
        let store =
            ObjectStorage::new_local_filesystem(PathBuf::from(&rel_path).as_path()).await;
        assert!(store.is_ok());

        // Relative with leading ./
        let rel_path = TestLocalPath("./other/path".to_string());
        let store =
            ObjectStorage::new_local_filesystem(PathBuf::from(&rel_path).as_path()).await;
        assert!(store.is_ok());
    }
}

// Factory functions

pub async fn new_in_memory_storage() -> StorageResult<Arc<dyn Storage + Send + Sync>> {
    let st = ObjectStorage::new_in_memory().await?;
    Ok(Arc::new(st))
}

#[cfg(feature = "fs")]
pub async fn new_local_filesystem_storage(
    path: &StdPath,
) -> StorageResult<Arc<dyn Storage + Send + Sync>> {
    let st = ObjectStorage::new_local_filesystem(path).await?;
    Ok(Arc::new(st))
}

#[cfg(feature = "http")]
pub fn new_http_storage(
    base_url: &str,
    config: Option<HashMap<String, String>>,
    headers: Option<HashMap<String, String>>,
) -> StorageResult<Arc<dyn Storage + Send + Sync>> {
    use std::str::FromStr as _;
    let base_url = Url::parse(base_url)
        .map_err(|e| StorageErrorKind::CannotParseUrl {
            cause: e,
            url: base_url.to_string(),
        })
        .capture()?;
    let config = config
        .unwrap_or_default()
        .iter()
        .filter_map(|(k, v)| {
            ClientConfigKey::from_str(k).ok().map(|key| (key, v.clone()))
        })
        .collect();
    let st = ObjectStorage::new_http(&base_url, Some(config), headers)?;
    Ok(Arc::new(st))
}

#[cfg(feature = "s3")]
pub async fn new_s3_object_store_storage(
    config: S3Options,
    bucket: String,
    prefix: Option<String>,
    credentials: Option<S3Credentials>,
    extra_read_headers: Vec<(String, String)>,
    extra_write_headers: Vec<(String, String)>,
) -> StorageResult<Arc<dyn Storage + Send + Sync>> {
    if let Some(endpoint) = &config.endpoint_url
        && (endpoint.contains("fly.storage.tigris.dev")
            || endpoint.contains("t3.storage.dev"))
    {
        return Err(StorageError::from(other_error(
            "Tigris Storage is not S3 compatible, use the Tigris specific constructor instead"
                .to_string(),
        )));
    }
    let storage = ObjectStorage::new_s3(
        bucket,
        prefix,
        credentials,
        Some(config),
        extra_read_headers,
        extra_write_headers,
    )
    .await?;
    Ok(Arc::new(storage))
}

#[cfg(feature = "azure")]
pub async fn new_azure_blob_storage(
    account: String,
    container: String,
    prefix: Option<String>,
    credentials: Option<AzureCredentials>,
    config: Option<HashMap<String, String>>,
) -> StorageResult<Arc<dyn Storage + Send + Sync>> {
    use object_store::azure::AzureConfigKey;
    let config = config
        .unwrap_or_default()
        .into_iter()
        .filter_map(|(key, value)| key.parse::<AzureConfigKey>().map(|k| (k, value)).ok())
        .collect();
    let storage =
        ObjectStorage::new_azure(account, container, prefix, credentials, Some(config))
            .await?;
    Ok(Arc::new(storage))
}

#[cfg(feature = "gcs")]
pub fn new_gcs_storage(
    bucket: String,
    prefix: Option<String>,
    credentials: Option<GcsCredentials>,
    config: Option<HashMap<String, String>>,
    extra_read_headers: Vec<(String, String)>,
    extra_write_headers: Vec<(String, String)>,
) -> StorageResult<Arc<dyn Storage + Send + Sync>> {
    use object_store::gcp::GoogleConfigKey;
    let config = config
        .unwrap_or_default()
        .into_iter()
        .filter_map(|(key, value)| {
            key.parse::<GoogleConfigKey>().map(|k| (k, value)).ok()
        })
        .collect();
    let storage = ObjectStorage::new_gcs(
        bucket,
        prefix,
        credentials,
        Some(config),
        extra_read_headers,
        extra_write_headers,
    )?;
    Ok(Arc::new(storage))
}

#[cfg(all(test, feature = "s3"))]
mod s3_header_tests {
    use std::sync::Arc;

    use icechunk_macros::tokio_test;
    use icechunk_storage::{Settings, s3_config::S3Options};

    use super::{ObjectStorage, ObjectStoreBackend as _, Role, S3ObjectStoreBackend};

    fn backend(read: &[(&str, &str)], write: &[(&str, &str)]) -> S3ObjectStoreBackend {
        let to_vec = |hs: &[(&str, &str)]| {
            hs.iter().map(|(k, v)| ((*k).to_string(), (*v).to_string())).collect()
        };
        S3ObjectStoreBackend {
            bucket: "testbucket".to_string(),
            prefix: Some("p".to_string()),
            credentials: None,
            config: Some(S3Options::default().with_region("us-east-1")),
            extra_read_headers: to_vec(read),
            extra_write_headers: to_vec(write),
        }
    }

    /// Whether the read-role and write-role clients are the *same* `Arc`. Goes
    /// through `new_s3` so the construction-time header sort is exercised.
    #[expect(clippy::unwrap_used)]
    async fn read_and_write_share_client(
        read: &[(&str, &str)],
        write: &[(&str, &str)],
    ) -> bool {
        let to_vec = |hs: &[(&str, &str)]| {
            hs.iter().map(|(k, v)| ((*k).to_string(), (*v).to_string())).collect()
        };
        let storage = ObjectStorage::new_s3(
            "testbucket".to_string(),
            Some("p".to_string()),
            None,
            Some(S3Options::default().with_region("us-east-1")),
            to_vec(read),
            to_vec(write),
        )
        .await
        .unwrap();
        let settings = Settings::default();
        let read_client = storage.get_client(&settings, Role::Read).await.unwrap();
        let write_client = storage.get_client(&settings, Role::Write).await.unwrap();
        Arc::ptr_eq(read_client, write_client)
    }

    /// No headers, or equal read/write sets (in any order) -> a single shared
    /// client serves both roles (one connection pool).
    #[tokio_test]
    async fn test_equal_headers_share_one_client() {
        assert!(read_and_write_share_client(&[], &[]).await);
        let same = &[("x-amz-meta-a", "1")];
        assert!(read_and_write_share_client(same, same).await);
        // Same set, different order -> still one client (headers stored sorted).
        assert!(
            read_and_write_share_client(
                &[("x-amz-meta-a", "1"), ("x-amz-meta-b", "2")],
                &[("x-amz-meta-b", "2"), ("x-amz-meta-a", "1")],
            )
            .await
        );
    }

    /// Differing read/write sets -> two distinct, role-specific clients.
    #[tokio_test]
    async fn test_differing_headers_use_two_clients() {
        assert!(
            !read_and_write_share_client(
                &[],
                &[("x-amz-acl", "bucket-owner-full-control")]
            )
            .await
        );
        assert!(
            !read_and_write_share_client(
                &[("x-amz-meta-r", "1")],
                &[("x-amz-meta-w", "1")]
            )
            .await
        );
    }

    /// Building the client succeeds for each role when headers are set: this
    /// exercises the read-back + `with_client_options` path.
    #[test]
    fn test_mk_object_store_with_headers_builds() {
        let b = backend(
            &[("x-amz-meta-reader", "r")],
            &[("x-amz-acl", "bucket-owner-full-control")],
        );
        assert!(b.mk_object_store(&Settings::default(), Role::Read).is_ok());
        assert!(b.mk_object_store(&Settings::default(), Role::Write).is_ok());
    }

    /// An invalid header name surfaces as an error at client-build time.
    #[test]
    fn test_mk_object_store_invalid_header_errs() {
        let b = backend(&[], &[("bad header", "v")]);
        assert!(b.mk_object_store(&Settings::default(), Role::Write).is_err());
    }

    /// Headers round-trip through serde, and `skip_serializing_if` keeps the
    /// serialized form unchanged when they are empty (back-compat).
    #[test]
    fn test_headers_serde_roundtrip() {
        let empty = backend(&[], &[]);
        let json = serde_json::to_string(&empty).unwrap();
        assert!(!json.contains("extra_read_headers"), "got: {json}");
        assert!(!json.contains("extra_write_headers"), "got: {json}");

        let with = backend(&[], &[("x-amz-acl", "bucket-owner-full-control")]);
        let json = serde_json::to_string(&with).unwrap();
        assert!(json.contains("x-amz-acl"), "got: {json}");
        let back: S3ObjectStoreBackend = serde_json::from_str(&json).unwrap();
        assert_eq!(back.extra_write_headers, with.extra_write_headers);
        assert!(back.extra_read_headers.is_empty());
    }
}

#[cfg(all(test, feature = "http"))]
mod http_tests {
    use std::collections::HashMap;

    use icechunk_storage::Settings;

    use super::{HttpObjectStoreBackend, ObjectStoreBackend as _};

    #[expect(clippy::expect_used, reason = "test helper, panicking on bad input is fine")]
    fn backend(
        opts: &[(&str, &str)],
        headers: &[(&str, &str)],
    ) -> HttpObjectStoreBackend {
        let config = opts
            .iter()
            .map(|(k, v)| (k.parse().expect("valid ClientConfigKey"), (*v).to_string()))
            .collect();
        let headers = headers
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect::<HashMap<_, _>>();
        HttpObjectStoreBackend {
            url: "https://example.com/".to_string(),
            config: Some(config),
            headers: if headers.is_empty() { None } else { Some(headers) },
        }
    }

    /// Store builds with opts only (no headers).
    #[test]
    fn test_mk_object_store_opts_only() {
        let b = backend(&[("allow_http", "true")], &[]);
        assert!(b.mk_object_store(&Settings::default(), super::Role::Read).is_ok());
    }

    /// Store builds with headers only (no opts).
    #[test]
    fn test_mk_object_store_headers_only() {
        let b = backend(&[], &[("Authorization", "Bearer token123")]);
        assert!(b.mk_object_store(&Settings::default(), super::Role::Read).is_ok());
    }

    /// Store builds when both opts and headers are present — the opts-clobber
    /// bug would have caused `allow_http` to be silently dropped in this case.
    #[test]
    fn test_mk_object_store_opts_and_headers() {
        let b =
            backend(&[("allow_http", "true")], &[("Authorization", "Bearer token123")]);
        assert!(b.mk_object_store(&Settings::default(), super::Role::Read).is_ok());
    }

    /// A header name containing a space is invalid and must return Err.
    #[test]
    fn test_mk_object_store_invalid_header_name() {
        let b = backend(&[], &[("bad header", "value")]);
        assert!(b.mk_object_store(&Settings::default(), super::Role::Read).is_err());
    }

    /// A header value containing a newline is invalid and must return Err.
    #[test]
    fn test_mk_object_store_invalid_header_value() {
        let b = backend(&[], &[("X-Custom", "val\nue")]);
        assert!(b.mk_object_store(&Settings::default(), super::Role::Read).is_err());
    }
}
