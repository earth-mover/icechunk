//! [`Storage`](super::Storage) implementation using the `object_store` crate.
//!
//! Supports local filesystem, in-memory, Azure Blob, and Google Cloud Storage.

use crate::private;
#[cfg(feature = "s3")]
use crate::storage::s3::{S3Credentials, S3Options};
use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, TimeDelta, Utc};
use futures::{
    Stream, StreamExt, TryStreamExt,
    stream::{self, BoxStream},
};
use object_store::{
    Attribute, AttributeValue, Attributes, GetOptions, ObjectMeta, ObjectStore,
    ObjectStoreExt as _, PutMode, PutOptions, UpdateVersion, memory::InMemory,
    path::Path as ObjectPath,
};
#[cfg(feature = "local-store")]
use object_store::local::LocalFileSystem;
#[cfg(any(feature = "s3", feature = "gcs", feature = "azure", feature = "http-store"))]
use object_store::{BackoffConfig, RetryConfig};
#[cfg(any(feature = "s3", feature = "gcs", feature = "azure", feature = "http-store"))]
use object_store::ClientConfigKey;
#[cfg(any(feature = "gcs", feature = "azure"))]
use object_store::{CredentialProvider, StaticCredentialProvider};
#[cfg(feature = "s3")]
use object_store::aws::AmazonS3Builder;
#[cfg(feature = "azure")]
use object_store::azure::{AzureConfigKey, MicrosoftAzureBuilder};
#[cfg(feature = "gcs")]
use object_store::gcp::{GcpCredential, GoogleCloudStorageBuilder, GoogleConfigKey};
#[cfg(feature = "http-store")]
use object_store::http::HttpBuilder;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    fmt::{self, Debug, Display},
    fs::create_dir_all,
    future::ready,
    num::{NonZeroU16, NonZeroU64},
    ops::Range,
    path::{Path as StdPath, PathBuf},
    pin::Pin,
    sync::Arc,
};
use tokio::sync::{OnceCell, RwLock};
use tracing::instrument;
use url::Url;

use super::{
    ConcurrencySettings, DeleteObjectsResult, ETag, Generation, ListInfo,
    RetriesSettings, Settings, Storage, StorageError, StorageErrorKind, StorageResult,
    VersionInfo, VersionedUpdateResult,
};

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
pub trait GcsCredentialsFetcher: fmt::Debug + Sync + Send {
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

/// Azure Blob Storage authentication credentials.
#[cfg(feature = "azure")]
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(tag = "az_credential_type")]
#[serde(rename_all = "snake_case")]
pub enum AzureCredentials {
    FromEnv,
    Static(AzureStaticCredentials),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ObjectStorage {
    backend: Arc<dyn ObjectStoreBackend>,
    #[serde(skip)]
    /// We need to use OnceCell to allow async initialization, because serde
    /// does not support async cfunction calls from deserialization. This gives
    /// us a way to lazily initialize the client.
    client: OnceCell<Arc<dyn ObjectStore>>,
}

impl ObjectStorage {
    /// Create an in memory Storage implementation
    ///
    /// This implementation should not be used in production code.
    pub async fn new_in_memory() -> Result<ObjectStorage, StorageError> {
        let backend = Arc::new(InMemoryObjectStoreBackend);
        let storage = ObjectStorage { backend, client: OnceCell::new() };
        Ok(storage)
    }

    /// Create an local filesystem Storage implementation
    ///
    /// This implementation should not be used in production code.
    #[cfg(feature = "local-store")]
    pub async fn new_local_filesystem(
        prefix: &StdPath,
    ) -> Result<ObjectStorage, StorageError> {
        tracing::warn!(
            "The LocalFileSystem storage is not safe for concurrent commits. If more than one thread/process will attempt to commit at the same time, prefer using object stores."
        );
        let backend =
            Arc::new(LocalFileSystemObjectStoreBackend { path: prefix.to_path_buf() });
        let storage = ObjectStorage { backend, client: OnceCell::new() };
        Ok(storage)
    }

    #[cfg(feature = "s3")]
    pub async fn new_s3(
        bucket: String,
        prefix: Option<String>,
        credentials: Option<S3Credentials>,
        config: Option<S3Options>,
    ) -> Result<ObjectStorage, StorageError> {
        let backend =
            Arc::new(S3ObjectStoreBackend { bucket, prefix, credentials, config });
        let storage = ObjectStorage { backend, client: OnceCell::new() };

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
        let storage = ObjectStorage { backend, client: OnceCell::new() };

        Ok(storage)
    }

    #[cfg(feature = "gcs")]
    pub fn new_gcs(
        bucket: String,
        prefix: Option<String>,
        credentials: Option<GcsCredentials>,
        config: Option<HashMap<GoogleConfigKey, String>>,
    ) -> Result<ObjectStorage, StorageError> {
        let backend =
            Arc::new(GcsObjectStoreBackend { bucket, prefix, credentials, config });
        let storage = ObjectStorage { backend, client: OnceCell::new() };

        Ok(storage)
    }

    #[cfg(feature = "http-store")]
    pub fn new_http(
        url: Url,
        config: Option<HashMap<ClientConfigKey, String>>,
    ) -> Result<ObjectStorage, StorageError> {
        let backend = Arc::new(HttpObjectStoreBackend { url: url.to_string(), config });
        let storage = ObjectStorage { backend, client: OnceCell::new() };
        Ok(storage)
    }

    /// Get the client, initializing it if it hasn't been initialized yet. This is necessary because the
    /// client is not serializeable and must be initialized after deserialization. Under normal construction
    /// the original client is returned immediately.
    #[instrument(skip_all)]
    async fn get_client(&self, settings: &Settings) -> &Arc<dyn ObjectStore> {
        self.client
            .get_or_init(|| async {
                // TODO: handle error better?
                #[allow(clippy::expect_used)]
                self.backend
                    .mk_object_store(settings)
                    .expect("failed to create object store")
            })
            .await
    }

    /// We need this because object_store's local file implementation doesn't sort refs. Since this
    /// implementation is used only for tests, it's OK to sort in memory.
    pub fn artificially_sort_refs_in_mem(&self) -> bool {
        self.backend.artificially_sort_refs_in_mem()
    }

    /// Return all keys in the store
    ///
    /// Intended for testing and debugging purposes only.
    pub async fn all_keys(&self) -> StorageResult<Vec<String>> {
        Ok(self
            .get_client(&self.backend.default_settings())
            .await
            .list(None)
            .map_ok(|obj| obj.location.to_string())
            .try_collect()
            .await
            .map_err(Box::new)?)
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

impl fmt::Display for ObjectStorage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ObjectStorage(backend={})", self.backend)
    }
}

impl private::Sealed for ObjectStorage {}

#[async_trait]
#[typetag::serde]
impl Storage for ObjectStorage {
    async fn can_write(&self) -> StorageResult<bool> {
        Ok(self.backend.can_write())
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
        let res =
            self.get_client(settings).await.put_opts(&path, bytes.into(), options).await;
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
            Err(err) => Err(Box::new(err).into()),
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
        // FIXME: add support for content type, version check and metadata
        let from = self.prefixed_path(from);
        let to = self.prefixed_path(to);
        match self.get_client(settings).await.copy(&from, &to).await {
            Ok(_) => Ok(VersionedUpdateResult::Updated { new_version: version.clone() }),
            Err(object_store::Error::NotFound { .. }) => {
                Err(StorageErrorKind::ObjectNotFound.into())
            }
            Err(err) => Err(Box::new(err).into()),
        }
    }

    #[instrument(skip(self, settings))]
    async fn list_objects<'a>(
        &'a self,
        settings: &Settings,
        prefix: &str,
    ) -> StorageResult<BoxStream<'a, StorageResult<ListInfo<String>>>> {
        let prefix = ObjectPath::from(format!("{}/{}", self.backend.prefix(), prefix));
        let stream = self
            .get_client(settings)
            .await
            .list(Some(&prefix))
            // TODO: we should signal error instead of filtering
            .try_filter_map(move |object| {
                let prefix = prefix.clone();
                async move {
                    let info = object_to_list_info(&prefix, &object);
                    if info.is_none() {
                        tracing::error!(object=?object, "Found bad object while listing");
                    }
                    Ok(info)
                }
            })
            .map_err(Box::new)
            .err_into();
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
        let results =
            self.get_client(settings).await.delete_stream(stream::iter(ids).boxed());
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
        let res = self.get_client(settings).await.head(&path).await.map_err(Box::new)?;
        Ok(res.last_modified)
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
        let full_key = self.prefixed_path(path);
        let range = range.map(|range| {
            let usize_range = range.start..range.end;
            usize_range.into()
        });
        let opts = GetOptions { range, ..Default::default() };
        let res = self.get_client(settings).await.get_opts(&full_key, opts).await;

        match res {
            Ok(result) => {
                let version = VersionInfo {
                    etag: result.meta.e_tag.as_ref().cloned().map(ETag),
                    generation: result.meta.version.as_ref().cloned().map(Generation),
                };
                let stream =
                    Box::pin(result.into_stream().map_err(|e| Box::new(e).into()));
                Ok((stream, version))
            }
            Err(object_store::Error::NotFound { .. }) => {
                Err(StorageErrorKind::ObjectNotFound.into())
            }
            Err(err) => Err(Box::new(err).into()),
        }
    }
}

#[typetag::serde(tag = "object_store_provider_type")]
pub trait ObjectStoreBackend: Debug + Display + Sync + Send {
    fn mk_object_store(
        &self,
        settings: &Settings,
    ) -> Result<Arc<dyn ObjectStore>, StorageError>;

    /// The prefix for the object store.
    fn prefix(&self) -> String;

    /// We need this because object_store's local file implementation doesn't sort refs. Since this
    /// implementation is used only for tests, it's OK to sort in memory.
    fn artificially_sort_refs_in_mem(&self) -> bool {
        false
    }

    fn default_settings(&self) -> Settings;

    fn can_write(&self) -> bool {
        true
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct InMemoryObjectStoreBackend;

impl fmt::Display for InMemoryObjectStoreBackend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "InMemoryObjectStoreBackend")
    }
}

#[typetag::serde(name = "in_memory_object_store_provider")]
impl ObjectStoreBackend for InMemoryObjectStoreBackend {
    fn mk_object_store(
        &self,
        _settings: &Settings,
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

#[cfg(feature = "local-store")]
#[derive(Debug, Serialize, Deserialize)]
pub struct LocalFileSystemObjectStoreBackend {
    path: PathBuf,
}

#[cfg(feature = "local-store")]
impl fmt::Display for LocalFileSystemObjectStoreBackend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "LocalFileSystemObjectStoreBackend(path={})", self.path.display())
    }
}

#[cfg(feature = "local-store")]
#[typetag::serde(name = "local_file_system_object_store_provider")]
impl ObjectStoreBackend for LocalFileSystemObjectStoreBackend {
    fn mk_object_store(
        &self,
        _settings: &Settings,
    ) -> Result<Arc<dyn ObjectStore>, StorageError> {
        create_dir_all(&self.path).map_err(|e| StorageErrorKind::Other(e.to_string()))?;

        let path = std::fs::canonicalize(&self.path)
            .map_err(|e| StorageErrorKind::Other(e.to_string()))?;
        Ok(Arc::new(
            LocalFileSystem::new_with_prefix(path)
                .map_err(|e| StorageErrorKind::Other(e.to_string()))?,
        ))
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
}

#[cfg(feature = "http-store")]
#[derive(Debug, Serialize, Deserialize)]
pub struct HttpObjectStoreBackend {
    pub url: String,
    pub config: Option<HashMap<ClientConfigKey, String>>,
}

#[cfg(feature = "http-store")]
impl fmt::Display for HttpObjectStoreBackend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "HttpObjectStoreBackend(url={}, config={})",
            self.url,
            self.config
                .as_ref()
                .map(|c| c
                    .iter()
                    .map(|(k, v)| format!("{k:?}={v}"))
                    .collect::<Vec<_>>()
                    .join(", "))
                .unwrap_or("None".to_string())
        )
    }
}

#[cfg(feature = "http-store")]
#[typetag::serde(name = "http_object_store_provider")]
impl ObjectStoreBackend for HttpObjectStoreBackend {
    fn mk_object_store(
        &self,
        settings: &Settings,
    ) -> Result<Arc<dyn ObjectStore>, StorageError> {
        let builder = HttpBuilder::new().with_url(&self.url);

        let empty = HashMap::new();
        let config = self.config.as_ref().unwrap_or(&empty);

        // Add options
        let mut builder = config
            .iter()
            .fold(builder, |builder, (key, value)| builder.with_config(*key, value));

        if !config.contains_key(&ClientConfigKey::AllowHttp)
            && self.url.starts_with("http:")
        {
            builder = builder.with_config(ClientConfigKey::AllowHttp, "true")
        }

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

        let store =
            builder.build().map_err(|e| StorageErrorKind::Other(e.to_string()))?;

        Ok(Arc::new(store))
    }

    fn prefix(&self) -> String {
        "".to_string()
    }

    fn default_settings(&self) -> Settings {
        Default::default()
    }

    fn can_write(&self) -> bool {
        // TODO: Support write operations?
        false
    }
}

#[cfg(feature = "s3")]
#[derive(Debug, Serialize, Deserialize)]
pub struct S3ObjectStoreBackend {
    bucket: String,
    prefix: Option<String>,
    credentials: Option<S3Credentials>,
    config: Option<S3Options>,
}

#[cfg(feature = "s3")]
impl fmt::Display for S3ObjectStoreBackend {
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
    fn mk_object_store(
        &self,
        settings: &Settings,
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
                builder.with_region(region.to_string())
            } else {
                builder
            };

            let builder = if let Some(endpoint) = config.endpoint_url.as_ref() {
                builder.with_endpoint(endpoint.to_string())
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
            .with_conditional_put(object_store::aws::S3ConditionalPut::ETagMatch);

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

        let store =
            builder.build().map_err(|e| StorageErrorKind::Other(e.to_string()))?;
        Ok(Arc::new(store))
    }

    fn prefix(&self) -> String {
        self.prefix.clone().unwrap_or("".to_string())
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
impl fmt::Display for AzureObjectStoreBackend {
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
    fn mk_object_store(
        &self,
        settings: &Settings,
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
            None | Some(AzureCredentials::FromEnv) => MicrosoftAzureBuilder::from_env(),
        };

        // Either the account name should be provided or user_emulator should be set to true to use the default account
        let builder =
            builder.with_account(&self.account).with_container_name(&self.container);

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

        let store =
            builder.build().map_err(|e| StorageErrorKind::Other(e.to_string()))?;
        Ok(Arc::new(store))
    }

    fn prefix(&self) -> String {
        self.prefix.clone().unwrap_or("".to_string())
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
}

#[cfg(feature = "gcs")]
impl fmt::Display for GcsObjectStoreBackend {
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
    fn mk_object_store(
        &self,
        settings: &Settings,
    ) -> Result<Arc<dyn ObjectStore>, StorageError> {
        let builder = GoogleCloudStorageBuilder::new();

        let builder = match self.credentials.as_ref() {
            Some(GcsCredentials::Static(GcsStaticCredentials::ServiceAccount(path))) => {
                let path = path.clone().into_os_string().into_string().map_err(|_| {
                    StorageErrorKind::Other("invalid service account path".to_string())
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
                    StorageErrorKind::Other(
                        "invalid application credentials path".to_string(),
                    )
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

        let builder = builder.with_bucket_name(&self.bucket);

        // Add options
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
        let store =
            builder.build().map_err(|e| StorageErrorKind::Other(e.to_string()))?;
        Ok(Arc::new(store))
    }

    fn prefix(&self) -> String {
        self.prefix.clone().unwrap_or("".to_string())
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
        let creds = self
            .refresher
            .get()
            .await
            .map_err(|e| StorageErrorKind::Other(e.to_string()))?;
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

fn object_to_list_info(
    prefix: &ObjectPath,
    object: &ObjectMeta,
) -> Option<ListInfo<String>> {
    let created_at = object.last_modified;
    let id = ObjectPath::from_iter(object.location.prefix_match(prefix)?).to_string();
    let size_bytes = object.size;
    Some(ListInfo { id, created_at, size_bytes })
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
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

    impl From<&TestLocalPath> for std::path::PathBuf {
        fn from(path: &TestLocalPath) -> Self {
            std::path::PathBuf::from(&path.0)
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
