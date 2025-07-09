use crate::{
    config::{
        AzureCredentials, AzureStaticCredentials, GcsBearerCredential, GcsCredentials,
        GcsCredentialsFetcher, GcsStaticCredentials, S3Credentials, S3Options,
    },
    format::{ChunkId, ChunkOffset, FileTypeTag, ManifestId, ObjectId, SnapshotId},
    private,
};
use async_trait::async_trait;
use bytes::{Buf, Bytes};
use chrono::{DateTime, Utc};
use futures::{
    StreamExt, TryStreamExt,
    stream::{self, BoxStream},
};
use object_store::{
    Attribute, AttributeValue, Attributes, BackoffConfig, ClientConfigKey,
    CredentialProvider, GetOptions, ObjectMeta, ObjectStore, PutMode, PutOptions,
    PutPayload, RetryConfig, StaticCredentialProvider, UpdateVersion,
    aws::AmazonS3Builder,
    azure::{AzureConfigKey, MicrosoftAzureBuilder},
    gcp::{GcpCredential, GoogleCloudStorageBuilder, GoogleConfigKey},
    http::HttpBuilder,
    local::LocalFileSystem,
    memory::InMemory,
    path::Path as ObjectPath,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    fmt::{self, Debug, Display},
    fs::create_dir_all,
    future::ready,
    num::{NonZeroU16, NonZeroU64},
    ops::Range,
    path::{Path as StdPath, PathBuf},
    sync::Arc,
};
use tokio::{
    io::AsyncRead,
    sync::{Mutex, OnceCell},
};
use tokio_util::compat::FuturesAsyncReadCompatExt;
use tracing::instrument;

use super::{
    CHUNK_PREFIX, CONFIG_PATH, ConcurrencySettings, DeleteObjectsResult, ETag,
    FetchConfigResult, Generation, GetRefResult, ListInfo, MANIFEST_PREFIX, REF_PREFIX,
    Reader, RetriesSettings, SNAPSHOT_PREFIX, Settings, Storage, StorageError,
    StorageErrorKind, StorageResult, TRANSACTION_PREFIX, UpdateConfigResult, VersionInfo,
    WriteRefResult,
};

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

    pub async fn new_gcs(
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

    fn get_path_str(&self, file_prefix: &str, id: &str) -> ObjectPath {
        let path = format!("{}/{}/{}", self.backend.prefix(), file_prefix, id);
        ObjectPath::from(path)
    }

    fn get_path<const SIZE: usize, T: FileTypeTag>(
        &self,
        file_prefix: &str,
        id: &ObjectId<SIZE, T>,
    ) -> ObjectPath {
        // we serialize the url using crockford
        self.get_path_str(file_prefix, id.to_string().as_str())
    }

    fn get_config_path(&self) -> ObjectPath {
        self.get_path_str("", CONFIG_PATH)
    }

    fn get_snapshot_path(&self, id: &SnapshotId) -> ObjectPath {
        self.get_path(SNAPSHOT_PREFIX, id)
    }

    fn get_manifest_path(&self, id: &ManifestId) -> ObjectPath {
        self.get_path(MANIFEST_PREFIX, id)
    }

    fn get_transaction_path(&self, id: &SnapshotId) -> ObjectPath {
        self.get_path(TRANSACTION_PREFIX, id)
    }

    fn get_chunk_path(&self, id: &ChunkId) -> ObjectPath {
        self.get_path(CHUNK_PREFIX, id)
    }

    fn drop_prefix(&self, prefix: &ObjectPath, path: &ObjectPath) -> Option<ObjectPath> {
        path.prefix_match(&ObjectPath::from(format!("{prefix}"))).map(|it| it.collect())
    }

    fn ref_key(&self, ref_key: &str) -> ObjectPath {
        // ObjectPath knows how to deal with empty path parts: bar//foo
        ObjectPath::from(format!("{}/{}/{}", self.backend.prefix(), REF_PREFIX, ref_key))
    }

    async fn get_object_reader(
        &self,
        settings: &Settings,
        path: &ObjectPath,
    ) -> StorageResult<impl AsyncRead + use<>> {
        Ok(self
            .get_client(settings)
            .await
            .get(path)
            .await
            .map_err(Box::new)?
            .into_stream()
            .err_into()
            .into_async_read()
            .compat())
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

    fn get_ref_name(&self, prefix: &ObjectPath, meta: &ObjectMeta) -> Option<String> {
        let relative_key = self.drop_prefix(prefix, &meta.location)?;
        let parent = relative_key.parts().next()?;
        Some(parent.as_ref().to_string())
    }

    fn get_put_mode(
        &self,
        settings: &Settings,
        previous_version: &VersionInfo,
    ) -> PutMode {
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
    fn can_write(&self) -> bool {
        self.backend.can_write()
    }

    #[instrument(skip_all)]
    fn default_settings(&self) -> Settings {
        self.backend.default_settings()
    }

    #[instrument(skip_all)]
    async fn fetch_config(
        &self,
        settings: &Settings,
    ) -> StorageResult<FetchConfigResult> {
        let path = self.get_config_path();
        let response = self.get_client(settings).await.get(&path).await;

        match response {
            Ok(result) => {
                let version = VersionInfo {
                    etag: result.meta.e_tag.as_ref().cloned().map(ETag),
                    generation: result.meta.version.as_ref().cloned().map(Generation),
                };

                Ok(FetchConfigResult::Found {
                    bytes: result.bytes().await.map_err(Box::new)?,
                    version,
                })
            }
            Err(object_store::Error::NotFound { .. }) => Ok(FetchConfigResult::NotFound),
            Err(err) => Err(Box::new(err).into()),
        }
    }
    #[instrument(skip(self, settings, config))]
    async fn update_config(
        &self,
        settings: &Settings,
        config: Bytes,
        previous_version: &VersionInfo,
    ) -> StorageResult<UpdateConfigResult> {
        let path = self.get_config_path();
        let attributes = if settings.unsafe_use_metadata() {
            Attributes::from_iter(vec![(
                Attribute::ContentType,
                AttributeValue::from("application/yaml"),
            )])
        } else {
            Attributes::new()
        };

        let mode = self.get_put_mode(settings, previous_version);

        let options = PutOptions { mode, attributes, ..PutOptions::default() };
        let res =
            self.get_client(settings).await.put_opts(&path, config.into(), options).await;
        match res {
            Ok(res) => {
                let new_version = VersionInfo {
                    etag: res.e_tag.map(ETag),
                    generation: res.version.map(Generation),
                };
                Ok(UpdateConfigResult::Updated { new_version })
            }
            Err(object_store::Error::Precondition { .. }) => {
                Ok(UpdateConfigResult::NotOnLatestVersion)
            }
            Err(err) => Err(Box::new(err).into()),
        }
    }

    #[instrument(skip(self, settings))]
    async fn fetch_snapshot(
        &self,
        settings: &Settings,
        id: &SnapshotId,
    ) -> StorageResult<Box<dyn AsyncRead + Unpin + Send>> {
        let path = self.get_snapshot_path(id);
        Ok(Box::new(self.get_object_reader(settings, &path).await?))
    }

    #[instrument(skip(self, settings))]
    async fn fetch_manifest_known_size(
        &self,
        settings: &Settings,
        id: &ManifestId,
        size: u64,
    ) -> StorageResult<Reader> {
        let path = self.get_manifest_path(id);
        self.get_object_concurrently(settings, path.as_ref(), &(0..size)).await
    }

    #[instrument(skip(self, settings))]
    async fn fetch_manifest_unknown_size(
        &self,
        settings: &Settings,
        id: &ManifestId,
    ) -> StorageResult<Box<dyn AsyncRead + Unpin + Send>> {
        let path = self.get_manifest_path(id);
        Ok(Box::new(self.get_object_reader(settings, &path).await?))
    }

    #[instrument(skip(self, settings))]
    async fn fetch_transaction_log(
        &self,
        settings: &Settings,
        id: &SnapshotId,
    ) -> StorageResult<Box<dyn AsyncRead + Unpin + Send>> {
        let path = self.get_transaction_path(id);
        Ok(Box::new(self.get_object_reader(settings, &path).await?))
    }

    #[instrument(skip(self, settings, metadata, bytes))]
    async fn write_snapshot(
        &self,
        settings: &Settings,
        id: SnapshotId,
        metadata: Vec<(String, String)>,
        bytes: Bytes,
    ) -> StorageResult<()> {
        let path = self.get_snapshot_path(&id);
        let attributes = self.metadata_to_attributes(settings, metadata);
        let options = PutOptions { attributes, ..PutOptions::default() };
        // FIXME: use multipart
        self.get_client(settings)
            .await
            .put_opts(&path, bytes.into(), options)
            .await
            .map_err(Box::new)?;
        Ok(())
    }

    #[instrument(skip(self, settings, metadata, bytes))]
    async fn write_manifest(
        &self,
        settings: &Settings,
        id: ManifestId,
        metadata: Vec<(String, String)>,
        bytes: Bytes,
    ) -> StorageResult<()> {
        let path = self.get_manifest_path(&id);
        let attributes = self.metadata_to_attributes(settings, metadata);
        let options = PutOptions { attributes, ..PutOptions::default() };
        // FIXME: use multipart
        self.get_client(settings)
            .await
            .put_opts(&path, bytes.into(), options)
            .await
            .map_err(Box::new)?;
        Ok(())
    }

    #[instrument(skip(self, settings, metadata, bytes))]
    async fn write_transaction_log(
        &self,
        settings: &Settings,
        id: SnapshotId,
        metadata: Vec<(String, String)>,
        bytes: Bytes,
    ) -> StorageResult<()> {
        let path = self.get_transaction_path(&id);
        let attributes = self.metadata_to_attributes(settings, metadata);
        let options = PutOptions { attributes, ..PutOptions::default() };
        // FIXME: use multipart
        self.get_client(settings)
            .await
            .put_opts(&path, bytes.into(), options)
            .await
            .map_err(Box::new)?;
        Ok(())
    }

    #[instrument(skip(self, settings))]
    async fn fetch_chunk(
        &self,
        settings: &Settings,
        id: &ChunkId,
        range: &Range<ChunkOffset>,
    ) -> Result<Bytes, StorageError> {
        let path = self.get_chunk_path(id);
        self.get_object_concurrently(settings, path.as_ref(), range)
            .await?
            .to_bytes((range.end - range.start + 16) as usize)
            .await
    }

    #[instrument(skip(self, settings, bytes))]
    async fn write_chunk(
        &self,
        settings: &Settings,
        id: ChunkId,
        bytes: Bytes,
    ) -> Result<(), StorageError> {
        let path = self.get_chunk_path(&id);
        self.get_client(settings)
            .await
            .put(&path, bytes.into())
            .await
            .map_err(Box::new)?;
        Ok(())
    }

    #[instrument(skip(self, settings))]
    async fn get_ref(
        &self,
        settings: &Settings,
        ref_key: &str,
    ) -> StorageResult<GetRefResult> {
        let key = self.ref_key(ref_key);
        match self.get_client(settings).await.get(&key).await {
            Ok(res) => {
                let etag = res.meta.e_tag.clone().map(ETag);
                let generation = res.meta.version.clone().map(Generation);
                Ok(GetRefResult::Found {
                    bytes: res.bytes().await.map_err(Box::new)?,
                    version: VersionInfo { etag, generation },
                })
            }
            Err(object_store::Error::NotFound { .. }) => Ok(GetRefResult::NotFound),
            Err(err) => Err(Box::new(err).into()),
        }
    }

    #[instrument(skip(self, settings))]
    async fn ref_names(&self, settings: &Settings) -> StorageResult<Vec<String>> {
        let prefix = &self.ref_key("");

        Ok(self
            .get_client(settings)
            .await
            .list(Some(prefix.clone()).as_ref())
            .try_filter_map(|meta| async move {
                let name = self.get_ref_name(prefix, &meta);
                if name.is_none() {
                    tracing::error!(object = ?meta, "Bad ref name")
                }
                Ok(name)
            })
            .try_collect()
            .await
            .map_err(Box::new)?)
    }

    #[instrument(skip(self, settings, bytes))]
    async fn write_ref(
        &self,
        settings: &Settings,
        ref_key: &str,
        bytes: Bytes,
        previous_version: &VersionInfo,
    ) -> StorageResult<WriteRefResult> {
        let key = self.ref_key(ref_key);
        let mode = self.get_put_mode(settings, previous_version);
        let opts = PutOptions { mode, ..PutOptions::default() };

        match self
            .get_client(settings)
            .await
            .put_opts(&key, PutPayload::from_bytes(bytes), opts)
            .await
        {
            Ok(_) => Ok(WriteRefResult::Written),
            Err(object_store::Error::Precondition { .. })
            | Err(object_store::Error::AlreadyExists { .. }) => {
                Ok(WriteRefResult::WontOverwrite)
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
            .try_filter_map(|object| async move {
                let info = object_to_list_info(&object);
                if info.is_none() {
                    tracing::error!(object=?object, "Found bad object while listing");
                }
                Ok(info)
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
            let path = self.get_path_str(prefix, id.as_str());
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
    async fn get_snapshot_last_modified(
        &self,
        settings: &Settings,
        snapshot: &SnapshotId,
    ) -> StorageResult<DateTime<Utc>> {
        let path = self.get_snapshot_path(snapshot);
        let res = self.get_client(settings).await.head(&path).await.map_err(Box::new)?;
        Ok(res.last_modified)
    }

    #[instrument(skip(self))]
    async fn get_object_range_buf(
        &self,
        settings: &Settings,
        key: &str,
        range: &Range<u64>,
    ) -> StorageResult<Box<dyn Buf + Unpin + Send>> {
        let path = ObjectPath::from(key);
        let usize_range = range.start..range.end;
        let range = Some(usize_range.into());
        let opts = GetOptions { range, ..Default::default() };
        Ok(Box::new(
            self.get_client(settings)
                .await
                .get_opts(&path, opts)
                .await
                .map_err(Box::new)?
                .bytes()
                .await
                .map_err(Box::new)?,
        ))
    }

    #[instrument(skip(self))]
    async fn get_object_range_read(
        &self,
        settings: &Settings,
        key: &str,
        range: &Range<u64>,
    ) -> StorageResult<Box<dyn AsyncRead + Unpin + Send>> {
        let path = ObjectPath::from(key);
        let usize_range = range.start..range.end;
        let range = Some(usize_range.into());
        let opts = GetOptions { range, ..Default::default() };
        let res: Box<dyn AsyncRead + Unpin + Send> = Box::new(
            self.get_client(settings)
                .await
                .get_opts(&path, opts)
                .await
                .map_err(Box::new)?
                .into_stream()
                .err_into()
                .into_async_read()
                .compat(),
        );
        Ok(res)
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

#[derive(Debug, Serialize, Deserialize)]
pub struct LocalFileSystemObjectStoreBackend {
    path: PathBuf,
}

impl fmt::Display for LocalFileSystemObjectStoreBackend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "LocalFileSystemObjectStoreBackend(path={})", self.path.display())
    }
}

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

#[derive(Debug, Serialize, Deserialize)]
pub struct HttpObjectStoreBackend {
    pub url: String,
    pub config: Option<HashMap<ClientConfigKey, String>>,
}

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

#[typetag::serde(name = "http_object_store_provider")]
impl ObjectStoreBackend for HttpObjectStoreBackend {
    fn mk_object_store(
        &self,
        settings: &Settings,
    ) -> Result<Arc<dyn ObjectStore>, StorageError> {
        let builder = HttpBuilder::new().with_url(&self.url);

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

#[derive(Debug, Serialize, Deserialize)]
pub struct S3ObjectStoreBackend {
    bucket: String,
    prefix: Option<String>,
    credentials: Option<S3Credentials>,
    config: Option<S3Options>,
}

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

#[derive(Debug, Serialize, Deserialize)]
pub struct AzureObjectStoreBackend {
    account: String,
    container: String,
    prefix: Option<String>,
    credentials: Option<AzureCredentials>,
    config: Option<HashMap<AzureConfigKey, String>>,
}

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

#[derive(Debug, Serialize, Deserialize)]
pub struct GcsObjectStoreBackend {
    pub bucket: String,
    pub prefix: Option<String>,
    pub credentials: Option<GcsCredentials>,
    pub config: Option<HashMap<GoogleConfigKey, String>>,
}

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

#[derive(Debug)]
pub struct GcsRefreshableCredentialProvider {
    last_credential: Arc<Mutex<Option<GcsBearerCredential>>>,
    refresher: Arc<dyn GcsCredentialsFetcher>,
}

impl GcsRefreshableCredentialProvider {
    pub fn new(refresher: Arc<dyn GcsCredentialsFetcher>) -> Self {
        Self { last_credential: Arc::new(Mutex::new(None)), refresher }
    }

    pub async fn get_or_update_credentials(
        &self,
    ) -> Result<GcsBearerCredential, StorageError> {
        let mut last_credential = self.last_credential.lock().await;

        // If we have a credential and it hasn't expired, return it
        if let Some(creds) = last_credential.as_ref() {
            if let Some(expires_after) = creds.expires_after {
                if expires_after > Utc::now() {
                    return Ok(creds.clone());
                }
            }
        }

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
impl CredentialProvider for GcsRefreshableCredentialProvider {
    type Credential = GcpCredential;

    async fn get_credential(&self) -> object_store::Result<Arc<Self::Credential>> {
        let creds = self.get_or_update_credentials().await.map_err(|e| {
            object_store::Error::Generic { store: "gcp", source: Box::new(e) }
        })?;
        Ok(Arc::new(GcpCredential::from(&creds)))
    }
}

fn object_to_list_info(object: &ObjectMeta) -> Option<ListInfo<String>> {
    let created_at = object.last_modified;
    let id = object.location.filename()?.to_string();
    let size_bytes = object.size;
    Some(ListInfo { id, created_at, size_bytes })
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use std::path::PathBuf;

    use icechunk_macros::tokio_test;
    use tempfile::TempDir;

    use crate::format::{ChunkId, ManifestId, SnapshotId};

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

    #[tokio_test]
    async fn test_object_store_paths() {
        let store = ObjectStorage::new_local_filesystem(PathBuf::from(".").as_path())
            .await
            .unwrap();

        let ref_key = "ref_key";
        let ref_path = store.ref_key(ref_key);
        assert_eq!(ref_path.to_string(), format!("refs/{ref_key}"));

        let snapshot_id = SnapshotId::random();
        let snapshot_path = store.get_snapshot_path(&snapshot_id);
        assert_eq!(snapshot_path.to_string(), format!("snapshots/{snapshot_id}"));

        let manifest_id = ManifestId::random();
        let manifest_path = store.get_manifest_path(&manifest_id);
        assert_eq!(manifest_path.to_string(), format!("manifests/{manifest_id}"));

        let chunk_id = ChunkId::random();
        let chunk_path = store.get_chunk_path(&chunk_id);
        assert_eq!(chunk_path.to_string(), format!("chunks/{chunk_id}"));

        let transaction_id = SnapshotId::random();
        let transaction_path = store.get_transaction_path(&transaction_id);
        assert_eq!(
            transaction_path.to_string(),
            format!("transactions/{transaction_id}")
        );
    }
}
