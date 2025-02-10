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
    stream::{self, BoxStream},
    StreamExt, TryStreamExt,
};
use object_store::{
    aws::AmazonS3Builder,
    azure::{AzureConfigKey, MicrosoftAzureBuilder},
    gcp::{GcpCredential, GoogleCloudStorageBuilder, GoogleConfigKey},
    local::LocalFileSystem,
    memory::InMemory,
    path::Path as ObjectPath,
    Attribute, AttributeValue, Attributes, CredentialProvider, GetOptions, ObjectMeta,
    ObjectStore, PutMode, PutOptions, PutPayload, UpdateVersion,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    fmt::Debug,
    fs::create_dir_all,
    future::ready,
    num::{NonZeroU16, NonZeroU64},
    ops::Range,
    path::{Path as StdPath, PathBuf},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};
use tokio::{
    io::AsyncRead,
    sync::{Mutex, OnceCell},
};
use tokio_util::compat::FuturesAsyncReadCompatExt;
use tracing::instrument;

use super::{
    ConcurrencySettings, FetchConfigResult, GetRefResult, ListInfo, Reader, Settings,
    Storage, StorageError, StorageErrorKind, StorageResult, UpdateConfigResult,
    WriteRefResult, CHUNK_PREFIX, CONFIG_PATH, MANIFEST_PREFIX, REF_PREFIX,
    SNAPSHOT_PREFIX, TRANSACTION_PREFIX,
};

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ObjectStorageConfig {
    pub url: String,
    pub prefix: String,
    pub options: Vec<(String, String)>,
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
        let client = backend.mk_object_store().await?;
        let storage = ObjectStorage { backend, client: OnceCell::new_with(Some(client)) };
        Ok(storage)
    }

    /// Create an local filesystem Storage implementation
    ///
    /// This implementation should not be used in production code.
    pub async fn new_local_filesystem(
        prefix: &StdPath,
    ) -> Result<ObjectStorage, StorageError> {
        let backend =
            Arc::new(LocalFileSystemObjectStoreBackend { path: prefix.to_path_buf() });
        let client = backend.mk_object_store().await?;
        let storage = ObjectStorage { backend, client: OnceCell::new_with(Some(client)) };

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
        let client = backend.mk_object_store().await?;
        let storage = ObjectStorage { backend, client: OnceCell::new_with(Some(client)) };

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
        let client = backend.mk_object_store().await?;
        let storage = ObjectStorage { backend, client: OnceCell::new_with(Some(client)) };

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
        let client = backend.mk_object_store().await?;
        let storage = ObjectStorage { backend, client: OnceCell::new_with(Some(client)) };

        Ok(storage)
    }

    /// Get the client, initializing it if it hasn't been initialized yet. This is necessary because the
    /// client is not serializeable and must be initialized after deserialization. Under normal construction
    /// the original client is returned immediately.
    #[instrument(skip(self))]
    async fn get_client(&self) -> &Arc<dyn ObjectStore> {
        self.client
            .get_or_init(|| async {
                // TODO: handle error better?
                #[allow(clippy::expect_used)]
                self.backend
                    .mk_object_store()
                    .await
                    .expect("failed to create object store")
            })
            .await
    }

    /// We need this because object_store's local file implementation doesn't sort refs. Since this
    /// implementation is used only for tests, it's OK to sort in memory.
    pub fn artificially_sort_refs_in_mem(&self) -> bool {
        self.backend.artificially_sort_refs_in_mem()
    }

    /// We need this because object_store's local file implementation doesn't support metadata.
    pub fn supports_metadata(&self) -> bool {
        self.backend.supports_metadata()
    }

    /// Return all keys in the store
    ///
    /// Intended for testing and debugging purposes only.
    pub async fn all_keys(&self) -> StorageResult<Vec<String>> {
        Ok(self
            .get_client()
            .await
            .list(None)
            .map_ok(|obj| obj.location.to_string())
            .try_collect()
            .await?)
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
        path.prefix_match(&ObjectPath::from(format!("{}", prefix))).map(|it| it.collect())
    }

    fn ref_key(&self, ref_key: &str) -> ObjectPath {
        // ObjectPath knows how to deal with empty path parts: bar//foo
        ObjectPath::from(format!("{}/{}/{}", self.backend.prefix(), REF_PREFIX, ref_key))
    }

    async fn do_ref_versions(&self, ref_name: &str) -> BoxStream<StorageResult<String>> {
        let prefix = self.ref_key(ref_name);
        self.get_client()
            .await
            .list(Some(prefix.clone()).as_ref())
            .map_err(|e| e.into())
            .and_then(move |meta| {
                ready(
                    self.drop_prefix(&prefix, &meta.location)
                        .map(|path| path.to_string())
                        .ok_or(
                            StorageErrorKind::Other(
                                "Bug in ref prefix logic".to_string(),
                            )
                            .into(),
                        ),
                )
            })
            .boxed()
    }

    async fn delete_batch(
        &self,
        prefix: &str,
        batch: Vec<String>,
    ) -> StorageResult<usize> {
        let keys = batch.iter().map(|id| Ok(self.get_path_str(prefix, id)));
        let results = self.get_client().await.delete_stream(stream::iter(keys).boxed());
        // FIXME: flag errors instead of skipping them
        Ok(results.filter(|res| ready(res.is_ok())).count().await)
    }

    async fn get_object_reader(
        &self,
        _settings: &Settings,
        path: &ObjectPath,
    ) -> StorageResult<impl AsyncRead> {
        Ok(self
            .get_client()
            .await
            .get(path)
            .await?
            .into_stream()
            .err_into()
            .into_async_read()
            .compat())
    }

    fn metadata_to_attributes(&self, metadata: Vec<(String, String)>) -> Attributes {
        if self.supports_metadata() {
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
}

impl private::Sealed for ObjectStorage {}

#[async_trait]
#[typetag::serde]
impl Storage for ObjectStorage {
    #[instrument(skip(self))]
    fn default_settings(&self) -> Settings {
        self.backend.default_settings()
    }

    #[instrument(skip(self, _settings))]
    async fn fetch_config(
        &self,
        _settings: &Settings,
    ) -> StorageResult<FetchConfigResult> {
        let path = self.get_config_path();
        let response = self.get_client().await.get(&path).await;

        match response {
            Ok(result) => match result.meta.e_tag.clone() {
                Some(etag) => {
                    Ok(FetchConfigResult::Found { bytes: result.bytes().await?, etag })
                }
                None => Ok(FetchConfigResult::NotFound),
            },
            Err(object_store::Error::NotFound { .. }) => Ok(FetchConfigResult::NotFound),
            Err(err) => Err(err.into()),
        }
    }
    #[instrument(skip(self, _settings, config))]
    async fn update_config(
        &self,
        _settings: &Settings,
        config: Bytes,
        etag: Option<&str>,
    ) -> StorageResult<UpdateConfigResult> {
        let path = self.get_config_path();
        let attributes = if self.supports_metadata() {
            Attributes::from_iter(vec![(
                Attribute::ContentType,
                AttributeValue::from("application/yaml"),
            )])
        } else {
            Attributes::new()
        };

        let mode = if let Some(etag) = etag {
            PutMode::Update(UpdateVersion {
                e_tag: Some(etag.to_string()),
                version: None,
            })
        } else {
            PutMode::Create
        };

        let options = PutOptions { mode, attributes, ..PutOptions::default() };
        let res = self.get_client().await.put_opts(&path, config.into(), options).await;
        match res {
            Ok(res) => {
                let new_etag = res.e_tag.ok_or(StorageErrorKind::Other(
                    "Config object should have an etag".to_string(),
                ))?;
                Ok(UpdateConfigResult::Updated { new_etag })
            }
            Err(object_store::Error::Precondition { .. }) => {
                Ok(UpdateConfigResult::NotOnLatestVersion)
            }
            Err(err) => Err(err.into()),
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

    #[instrument(skip(self, _settings, metadata, bytes))]
    async fn write_snapshot(
        &self,
        _settings: &Settings,
        id: SnapshotId,
        metadata: Vec<(String, String)>,
        bytes: Bytes,
    ) -> StorageResult<()> {
        let path = self.get_snapshot_path(&id);
        let attributes = self.metadata_to_attributes(metadata);
        let options = PutOptions { attributes, ..PutOptions::default() };
        // FIXME: use multipart
        self.get_client().await.put_opts(&path, bytes.into(), options).await?;
        Ok(())
    }

    #[instrument(skip(self, _settings, metadata, bytes))]
    async fn write_manifest(
        &self,
        _settings: &Settings,
        id: ManifestId,
        metadata: Vec<(String, String)>,
        bytes: Bytes,
    ) -> StorageResult<()> {
        let path = self.get_manifest_path(&id);
        let attributes = self.metadata_to_attributes(metadata);
        let options = PutOptions { attributes, ..PutOptions::default() };
        // FIXME: use multipart
        self.get_client().await.put_opts(&path, bytes.into(), options).await?;
        Ok(())
    }

    #[instrument(skip(self, _settings, metadata, bytes))]
    async fn write_transaction_log(
        &self,
        _settings: &Settings,
        id: SnapshotId,
        metadata: Vec<(String, String)>,
        bytes: Bytes,
    ) -> StorageResult<()> {
        let path = self.get_transaction_path(&id);
        let attributes = self.metadata_to_attributes(metadata);
        let options = PutOptions { attributes, ..PutOptions::default() };
        // FIXME: use multipart
        self.get_client().await.put_opts(&path, bytes.into(), options).await?;
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

    #[instrument(skip(self, _settings))]
    async fn write_chunk(
        &self,
        _settings: &Settings,
        id: ChunkId,
        bytes: bytes::Bytes,
    ) -> Result<(), StorageError> {
        let path = self.get_chunk_path(&id);
        let upload = self.get_client().await.put_multipart(&path).await?;
        // TODO: new_with_chunk_size?
        let mut write = object_store::WriteMultipart::new(upload);
        write.write(&bytes);
        write.finish().await?;
        Ok(())
    }

    #[instrument(skip(self, _settings))]
    async fn get_ref(
        &self,
        _settings: &Settings,
        ref_key: &str,
    ) -> StorageResult<GetRefResult> {
        let key = self.ref_key(ref_key);
        match self.get_client().await.get(&key).await {
            Ok(res) => Ok(GetRefResult::Found { bytes: res.bytes().await? }),
            Err(object_store::Error::NotFound { .. }) => Ok(GetRefResult::NotFound),
            Err(err) => Err(err.into()),
        }
    }

    #[instrument(skip(self, _settings))]
    async fn ref_names(&self, _settings: &Settings) -> StorageResult<Vec<String>> {
        // FIXME: i don't think object_store's implementation of list_with_delimiter is any good
        // we need to test if it even works beyond 1k refs
        let prefix = self.ref_key("");

        Ok(self
            .get_client()
            .await
            .list_with_delimiter(Some(prefix.clone()).as_ref())
            .await?
            .common_prefixes
            .iter()
            .filter_map(|path| {
                self.drop_prefix(&prefix, path).map(|path| path.to_string())
            })
            .collect())
    }

    #[instrument(skip(self, _settings))]
    async fn ref_versions(
        &self,
        _settings: &Settings,
        ref_name: &str,
    ) -> StorageResult<BoxStream<StorageResult<String>>> {
        let res = self.do_ref_versions(ref_name).await;
        if self.artificially_sort_refs_in_mem() {
            #[allow(clippy::expect_used)]
            // This branch is used for local tests, not in production. We don't expect the size of
            // these streams to be large, so we can collect in memory and fail early if there is an
            // error
            let mut all =
                res.try_collect::<Vec<_>>().await.expect("Error fetching ref versions");
            all.sort();
            Ok(futures::stream::iter(all.into_iter().map(Ok)).boxed())
        } else {
            Ok(res)
        }
    }

    #[instrument(skip(self, _settings, bytes))]
    async fn write_ref(
        &self,
        _settings: &Settings,
        ref_key: &str,
        overwrite_refs: bool,
        bytes: Bytes,
    ) -> StorageResult<WriteRefResult> {
        let key = self.ref_key(ref_key);
        let mode = if overwrite_refs { PutMode::Overwrite } else { PutMode::Create };
        let opts = PutOptions { mode, ..PutOptions::default() };

        match self
            .get_client()
            .await
            .put_opts(&key, PutPayload::from_bytes(bytes), opts)
            .await
        {
            Ok(_) => Ok(WriteRefResult::Written),
            Err(object_store::Error::AlreadyExists { .. }) => {
                Ok(WriteRefResult::WontOverwrite)
            }
            Err(err) => Err(err.into()),
        }
    }

    #[instrument(skip(self, _settings))]
    async fn list_objects<'a>(
        &'a self,
        _settings: &Settings,
        prefix: &str,
    ) -> StorageResult<BoxStream<'a, StorageResult<ListInfo<String>>>> {
        let prefix = ObjectPath::from(format!("{}/{}", self.backend.prefix(), prefix));
        let stream = self
            .get_client()
            .await
            .list(Some(&prefix))
            // TODO: we should signal error instead of filtering
            .try_filter_map(|object| ready(Ok(object_to_list_info(&object))))
            .err_into();
        Ok(stream.boxed())
    }

    #[instrument(skip(self, _settings, ids))]
    async fn delete_objects(
        &self,
        _settings: &Settings,
        prefix: &str,
        ids: BoxStream<'_, String>,
    ) -> StorageResult<usize> {
        let deleted = AtomicUsize::new(0);
        ids.chunks(1_000)
            // FIXME: configurable concurrency
            .for_each_concurrent(10, |batch| {
                let deleted = &deleted;
                async move {
                    // FIXME: handle error instead of skipping
                    let new_deletes = self.delete_batch(prefix, batch).await.unwrap_or(0);
                    deleted.fetch_add(new_deletes, Ordering::Release);
                }
            })
            .await;
        Ok(deleted.into_inner())
    }

    #[instrument(skip(self, _settings))]
    async fn get_snapshot_last_modified(
        &self,
        _settings: &Settings,
        snapshot: &SnapshotId,
    ) -> StorageResult<DateTime<Utc>> {
        let path = self.get_snapshot_path(snapshot);
        let res = self.get_client().await.head(&path).await?;
        Ok(res.last_modified)
    }

    #[instrument(skip(self))]
    async fn root_is_clean(&self) -> StorageResult<bool> {
        Ok(self
            .get_client()
            .await
            .list(Some(&ObjectPath::from(self.backend.prefix())))
            .next()
            .await
            .is_none())
    }

    #[instrument(skip(self))]
    async fn get_object_range_buf(
        &self,
        key: &str,
        range: &Range<u64>,
    ) -> StorageResult<Box<dyn Buf + Unpin + Send>> {
        let path = ObjectPath::from(key);
        let usize_range = range.start as usize..range.end as usize;
        let range = Some(usize_range.into());
        let opts = GetOptions { range, ..Default::default() };
        Ok(Box::new(self.get_client().await.get_opts(&path, opts).await?.bytes().await?))
    }

    #[instrument(skip(self))]
    async fn get_object_range_read(
        &self,
        key: &str,
        range: &Range<u64>,
    ) -> StorageResult<Box<dyn AsyncRead + Unpin + Send>> {
        let path = ObjectPath::from(key);
        let usize_range = range.start as usize..range.end as usize;
        let range = Some(usize_range.into());
        let opts = GetOptions { range, ..Default::default() };
        let res: Box<dyn AsyncRead + Unpin + Send> = Box::new(
            self.get_client()
                .await
                .get_opts(&path, opts)
                .await?
                .into_stream()
                .err_into()
                .into_async_read()
                .compat(),
        );
        Ok(res)
    }
}

#[async_trait]
#[typetag::serde(tag = "object_store_provider_type")]
pub trait ObjectStoreBackend: Debug + Sync + Send {
    async fn mk_object_store(&self) -> Result<Arc<dyn ObjectStore>, StorageError>;

    /// The prefix for the object store.
    fn prefix(&self) -> String;

    /// We need this because object_store's local file implementation doesn't sort refs. Since this
    /// implementation is used only for tests, it's OK to sort in memory.
    fn artificially_sort_refs_in_mem(&self) -> bool {
        false
    }

    /// We need this because object_store's local file implementation doesn't support metadata.
    fn supports_metadata(&self) -> bool {
        true
    }

    fn default_settings(&self) -> Settings {
        Settings::default()
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct InMemoryObjectStoreBackend;

#[async_trait]
#[typetag::serde(name = "in_memory_object_store_provider")]
impl ObjectStoreBackend for InMemoryObjectStoreBackend {
    async fn mk_object_store(&self) -> Result<Arc<dyn ObjectStore>, StorageError> {
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
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct LocalFileSystemObjectStoreBackend {
    path: PathBuf,
}

#[async_trait]
#[typetag::serde(name = "local_file_system_object_store_provider")]
impl ObjectStoreBackend for LocalFileSystemObjectStoreBackend {
    async fn mk_object_store(&self) -> Result<Arc<dyn ObjectStore>, StorageError> {
        _ = create_dir_all(&self.path)
            .map_err(|e| StorageErrorKind::Other(e.to_string()))?;

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

    fn supports_metadata(&self) -> bool {
        false
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
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct S3ObjectStoreBackend {
    bucket: String,
    prefix: Option<String>,
    credentials: Option<S3Credentials>,
    config: Option<S3Options>,
}

#[async_trait]
#[typetag::serde(name = "s3_object_store_provider")]
impl ObjectStoreBackend for S3ObjectStoreBackend {
    async fn mk_object_store(&self) -> Result<Arc<dyn ObjectStore>, StorageError> {
        let builder = AmazonS3Builder::new();

        let builder = match self.credentials.as_ref() {
            Some(S3Credentials::Static(credentials)) => {
                let builder = builder
                    .with_access_key_id(credentials.access_key_id.clone())
                    .with_secret_access_key(credentials.secret_access_key.clone());

                let builder =
                    if let Some(session_token) = credentials.session_token.as_ref() {
                        builder.with_token(session_token.clone())
                    } else {
                        builder
                    };

                builder
            }
            Some(S3Credentials::Anonymous) => builder.with_skip_signature(true),
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

        // Defualt for consistency
        let builder = builder
            .with_bucket_name(&self.bucket)
            .with_conditional_put(object_store::aws::S3ConditionalPut::ETagMatch);

        let store =
            builder.build().map_err(|e| StorageErrorKind::Other(e.to_string()))?;
        Ok(Arc::new(store))
    }

    fn prefix(&self) -> String {
        self.prefix.clone().unwrap_or("".to_string())
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

#[async_trait]
#[typetag::serde(name = "azure_object_store_provider")]
impl ObjectStoreBackend for AzureObjectStoreBackend {
    async fn mk_object_store(&self) -> Result<Arc<dyn ObjectStore>, StorageError> {
        let builder = MicrosoftAzureBuilder::new();

        let builder = match self.credentials.as_ref() {
            Some(AzureCredentials::Static(AzureStaticCredentials::AccessKey(key))) => {
                builder.with_access_key(key)
            }
            Some(AzureCredentials::Static(AzureStaticCredentials::SASToken(_token))) => {
                // TODO: add sas token
                todo!()
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

        let store =
            builder.build().map_err(|e| StorageErrorKind::Other(e.to_string()))?;
        Ok(Arc::new(store))
    }

    fn prefix(&self) -> String {
        self.prefix.clone().unwrap_or("".to_string())
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct GcsObjectStoreBackend {
    bucket: String,
    prefix: Option<String>,
    credentials: Option<GcsCredentials>,
    config: Option<HashMap<GoogleConfigKey, String>>,
}

#[async_trait]
#[typetag::serde(name = "gcs_object_store_provider")]
impl ObjectStoreBackend for GcsObjectStoreBackend {
    async fn mk_object_store(&self) -> Result<Arc<dyn ObjectStore>, StorageError> {
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
            Some(GcsCredentials::Refreshable(_)) => {
                todo!()
            }
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

        let store =
            builder.build().map_err(|e| StorageErrorKind::Other(e.to_string()))?;
        Ok(Arc::new(store))
    }

    fn prefix(&self) -> String {
        self.prefix.clone().unwrap_or("".to_string())
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
        Ok(Arc::new(creds.into()))
    }
}

fn object_to_list_info(object: &ObjectMeta) -> Option<ListInfo<String>> {
    let created_at = object.last_modified;
    let id = object.location.filename()?.to_string();
    Some(ListInfo { id, created_at })
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use std::path::PathBuf;

    use tempfile::TempDir;

    use super::ObjectStorage;

    #[tokio::test]
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

    #[tokio::test]
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
