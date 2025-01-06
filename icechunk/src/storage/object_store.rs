use crate::{
    format::{
        attributes::AttributesTable, format_constants, manifest::Manifest,
        snapshot::Snapshot, transaction_log::TransactionLog, AttributesId, ByteRange,
        ChunkId, FileTypeTag, ManifestId, ObjectId, SnapshotId,
    },
    private,
    storage::CompressionAlgorithm,
};
use async_trait::async_trait;
use bytes::Bytes;
use futures::{
    stream::{self, BoxStream, FuturesOrdered},
    StreamExt, TryStreamExt,
};
use object_store::{
    local::LocalFileSystem, parse_url_opts, path::Path as ObjectPath, Attribute,
    AttributeValue, Attributes, GetOptions, GetRange, ObjectMeta, ObjectStore, PutMode,
    PutOptions, PutPayload, UpdateVersion,
};
use serde::{Deserialize, Serialize};
use std::{
    fs::create_dir_all,
    future::ready,
    num::{NonZeroU16, NonZeroU64},
    ops::Range,
    path::Path as StdPath,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};
use tokio::io::AsyncRead;
use tokio_util::{compat::FuturesAsyncReadCompatExt, io::SyncIoBridge};
use url::Url;

use super::{
    split_in_multiple_requests, ConcurrencySettings, ETag, ListInfo, Settings, Storage,
    StorageError, StorageResult, CHUNK_PREFIX, CONFIG_PATH, MANIFEST_PREFIX, REF_PREFIX,
    SNAPSHOT_PREFIX, TRANSACTION_PREFIX,
};

// Get Range is object_store specific, keep it with this module
impl From<&ByteRange> for Option<GetRange> {
    fn from(value: &ByteRange) -> Self {
        match value {
            ByteRange::Bounded(Range { start, end }) => {
                Some(GetRange::Bounded(*start as usize..*end as usize))
            }
            ByteRange::From(start) if *start == 0u64 => None,
            ByteRange::From(start) => Some(GetRange::Offset(*start as usize)),
            ByteRange::Last(n) => Some(GetRange::Suffix(*n as usize)),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ObjectStorageConfig {
    pub url: String,
    pub prefix: String,
    pub options: Vec<(String, String)>,
}

#[derive(Debug, Serialize)]
#[serde(transparent)]
pub struct ObjectStorage {
    config: ObjectStorageConfig,
    #[serde(skip)]
    store: Arc<dyn ObjectStore>,
}

impl ObjectStorage {
    /// Create an in memory Storage implementation
    ///
    /// This implementation should not be used in production code.
    pub fn new_in_memory() -> Result<ObjectStorage, StorageError> {
        let url = "memory:/".to_string();
        let config = ObjectStorageConfig { url, prefix: "".to_string(), options: vec![] };
        Self::from_config(config)
    }

    /// Create an local filesystem Storage implementation
    ///
    /// This implementation should not be used in production code.
    pub fn new_local_filesystem(prefix: &StdPath) -> Result<ObjectStorage, StorageError> {
        create_dir_all(prefix).map_err(|e| StorageError::Other(e.to_string()))?;

        let prefix = std::fs::canonicalize(prefix)
            .map_err(|e| StorageError::Other(e.to_string()))?;
        let prefix =
            prefix.into_os_string().into_string().map_err(StorageError::BadPrefix)?;
        let url = format!("file://{prefix}");
        let config = ObjectStorageConfig { url, prefix, options: vec![] };
        Self::from_config(config)
    }

    /// Create an ObjectStore client from a URL and provided options
    pub fn from_config(
        config: ObjectStorageConfig,
    ) -> Result<ObjectStorage, StorageError> {
        let url: Url = Url::parse(config.url.as_str())
            .map_err(|e| StorageError::Other(e.to_string()))?;
        if url.scheme() == "file" {
            let path = url.path();
            let store = Arc::new(LocalFileSystem::new_with_prefix(path)?);
            return Ok(ObjectStorage {
                store,
                config: ObjectStorageConfig {
                    url: url.to_string(),
                    prefix: "".to_string(),
                    options: config.options,
                },
            });
        }

        let (store, path) = parse_url_opts(&url, config.options.clone())
            .map_err(|e| StorageError::Other(e.to_string()))?;
        let store: Arc<dyn ObjectStore> = Arc::from(store);
        Ok(ObjectStorage {
            store,
            config: ObjectStorageConfig {
                url: url.to_string(),
                prefix: path.to_string(),
                options: config.options,
            },
        })
    }

    /// We need this because object_store's local file implementation doesn't sort refs. Since this
    /// implementation is used only for tests, it's OK to sort in memory.
    pub fn artificially_sort_refs_in_mem(&self) -> bool {
        self.config.url.starts_with("file")
    }

    /// We need this because object_store's local file implementation doesn't support metadata.
    pub fn supports_metadata(&self) -> bool {
        !self.config.url.starts_with("file")
    }

    /// Return all keys in the store
    ///
    /// Intended for testing and debugging purposes only.
    pub async fn all_keys(&self) -> StorageResult<Vec<String>> {
        Ok(self
            .store
            .list(None)
            .map_ok(|obj| obj.location.to_string())
            .try_collect()
            .await?)
    }

    fn get_path_str(&self, file_prefix: &str, id: &str) -> ObjectPath {
        let path = format!("{}/{}/{}", self.config.prefix, file_prefix, id);
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
        ObjectPath::from(format!(
            "{}/{}/{}",
            self.config.prefix.as_str(),
            REF_PREFIX,
            ref_key
        ))
    }

    async fn do_ref_versions(&self, ref_name: &str) -> BoxStream<StorageResult<String>> {
        let prefix = self.ref_key(ref_name);
        self.store
            .list(Some(prefix.clone()).as_ref())
            .map_err(|e| e.into())
            .and_then(move |meta| {
                ready(
                    self.drop_prefix(&prefix, &meta.location)
                        .map(|path| path.to_string())
                        .ok_or(StorageError::Other(
                            "Bug in ref prefix logic".to_string(),
                        )),
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
        let results = self.store.delete_stream(stream::iter(keys).boxed());
        // FIXME: flag errors instead of skipping them
        Ok(results.filter(|res| ready(res.is_ok())).count().await)
    }

    async fn get_object_concurrently(
        &self,
        settings: &Settings,
        path: &ObjectPath,
        size: u64,
    ) -> StorageResult<impl AsyncRead> {
        let mut results = split_in_multiple_requests(
            size,
            settings.concurrency.min_concurrent_request_size.get(),
            settings.concurrency.max_concurrent_requests_for_object.get(),
        )
        .map(|(req_offset, req_size)| async move {
            let store = Arc::clone(&self.store);
            let range = Some(GetRange::from(
                req_offset as usize..req_offset as usize + req_size as usize,
            ));
            let opts = GetOptions { range, ..Default::default() };
            let path = path.clone();
            store.get_opts(&path, opts).await
        })
        .collect::<FuturesOrdered<_>>();

        let mut res = stream::empty().boxed();
        while let Some(result) = results.try_next().await? {
            res = res.chain(result.into_stream()).boxed();
        }

        Ok(res.err_into().into_async_read().compat())
    }
}

impl private::Sealed for ObjectStorage {}

impl<'de> serde::Deserialize<'de> for ObjectStorage {
    fn deserialize<D>(deserializer: D) -> Result<ObjectStorage, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let config = ObjectStorageConfig::deserialize(deserializer)?;
        ObjectStorage::from_config(config).map_err(serde::de::Error::custom)
    }
}

#[async_trait]
#[typetag::serde]
impl Storage for ObjectStorage {
    fn default_settings(&self) -> Settings {
        let base = Settings::default();
        let url = Url::parse(self.config.url.as_str());
        let scheme = url.as_ref().map(|url| url.scheme()).unwrap_or("s3");
        match scheme {
            "file" => Settings {
                concurrency: ConcurrencySettings {
                    max_concurrent_requests_for_object: NonZeroU16::new(5)
                        .unwrap_or(NonZeroU16::MIN),
                    min_concurrent_request_size: NonZeroU64::new(4 * 1024)
                        .unwrap_or(NonZeroU64::MIN),
                },
                ..base
            },
            "memory" => Settings {
                concurrency: ConcurrencySettings {
                    // we do != 1 because we use this store for tests
                    max_concurrent_requests_for_object: NonZeroU16::new(5)
                        .unwrap_or(NonZeroU16::MIN),
                    min_concurrent_request_size: NonZeroU64::new(1)
                        .unwrap_or(NonZeroU64::MIN),
                },
                ..base
            },

            _ => base,
        }
    }

    async fn fetch_config(
        &self,
        _settings: &Settings,
    ) -> StorageResult<Option<(Bytes, ETag)>> {
        let path = self.get_config_path();
        let response = self.store.get(&path).await;

        match response {
            Ok(result) => match result.meta.e_tag.clone() {
                Some(etag) => Ok(Some((result.bytes().await?, etag))),
                None => Err(StorageError::Other("No ETag found for config".to_string())),
            },
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(err) => Err(err.into()),
        }
    }
    async fn update_config(
        &self,
        _settings: &Settings,
        config: Bytes,
        etag: Option<&str>,
    ) -> StorageResult<ETag> {
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
        let res = self.store.put_opts(&path, config.into(), options).await;
        match res {
            Ok(res) => {
                let etag = res.e_tag.ok_or(StorageError::Other(
                    "Config object should have an etag".to_string(),
                ))?;
                Ok(etag)
            }
            Err(object_store::Error::Precondition { .. }) => {
                Err(StorageError::ConfigUpdateConflict)
            }
            Err(err) => Err(err.into()),
        }
    }

    async fn fetch_snapshot(
        &self,
        _settings: &Settings,
        id: &SnapshotId,
    ) -> Result<Arc<Snapshot>, StorageError> {
        let path = self.get_snapshot_path(id);
        let bytes = self.store.get(&path).await?.bytes().await?;
        // TODO: optimize using from_read
        let res = rmp_serde::from_slice(bytes.as_ref())?;
        Ok(Arc::new(res))
    }

    async fn fetch_attributes(
        &self,
        _settings: &Settings,
        _id: &AttributesId,
    ) -> Result<Arc<AttributesTable>, StorageError> {
        todo!();
    }

    async fn fetch_manifests(
        &self,
        settings: &Settings,
        id: &ManifestId,
        size: u64,
    ) -> Result<Arc<Manifest>, StorageError> {
        let path = self.get_manifest_path(id);
        let object_read = self.get_object_concurrently(settings, &path, size).await?;
        let manifest = tokio::task::spawn_blocking(move || {
            let sync_read = SyncIoBridge::new(object_read);
            let decompressor = zstd::stream::Decoder::new(sync_read)?;
            rmp_serde::from_read(decompressor).map_err(StorageError::MsgPackDecodeError)
        })
        .await??;
        Ok(Arc::new(manifest))
    }

    async fn fetch_transaction_log(
        &self,
        _settings: &Settings,
        id: &SnapshotId,
    ) -> StorageResult<Arc<TransactionLog>> {
        let path = self.get_transaction_path(id);
        let bytes = self.store.get(&path).await?.bytes().await?;
        // TODO: optimize using from_read
        let res = rmp_serde::from_slice(bytes.as_ref())?;
        Ok(Arc::new(res))
    }

    async fn write_snapshot(
        &self,
        _settings: &Settings,
        id: SnapshotId,
        snapshot: Arc<Snapshot>,
    ) -> Result<(), StorageError> {
        let path = self.get_snapshot_path(&id);
        let bytes = rmp_serde::to_vec(snapshot.as_ref())?;
        let attributes = if self.supports_metadata() {
            Attributes::from_iter(vec![
                (
                    Attribute::ContentType,
                    AttributeValue::from(
                        format_constants::LATEST_ICECHUNK_SNAPSHOT_CONTENT_TYPE,
                    ),
                ),
                (
                    Attribute::Metadata(std::borrow::Cow::Borrowed(
                        format_constants::LATEST_ICECHUNK_SNAPSHOT_VERSION_METADATA_KEY,
                    )),
                    AttributeValue::from(
                        snapshot.icechunk_snapshot_format_version.to_string(),
                    ),
                ),
            ])
        } else {
            Attributes::new()
        };
        let options = PutOptions { attributes, ..PutOptions::default() };
        // FIXME: use multipart
        self.store.put_opts(&path, bytes.into(), options).await?;
        Ok(())
    }

    async fn write_attributes(
        &self,
        _settings: &Settings,
        _id: AttributesId,
        _table: Arc<AttributesTable>,
    ) -> Result<(), StorageError> {
        todo!()
    }

    async fn write_manifests(
        &self,
        settings: &Settings,
        id: ManifestId,
        manifest: Arc<Manifest>,
    ) -> Result<u64, StorageError> {
        let path = self.get_manifest_path(&id);
        let manifest_c = Arc::clone(&manifest);
        debug_assert_eq!(settings.compression.algorithm, CompressionAlgorithm::Zstd);
        let compression_level = settings.compression.level as i32;
        let buffer = tokio::task::spawn_blocking(move || {
            let buffer = Vec::new(); // TODO: initialize capacity
            let mut compressor = zstd::stream::Encoder::new(buffer, compression_level)?;
            rmp_serde::encode::write(&mut compressor, manifest_c.as_ref())
                .map_err(StorageError::MsgPackEncodeError)?;
            compressor.finish().map_err(StorageError::IOError)
        })
        .await??;

        let attributes = if self.supports_metadata() {
            Attributes::from_iter(vec![
                (
                    Attribute::ContentType,
                    AttributeValue::from(
                        format_constants::LATEST_ICECHUNK_MANIFEST_CONTENT_TYPE,
                    ),
                ),
                (
                    Attribute::Metadata(std::borrow::Cow::Borrowed(
                        format_constants::LATEST_ICECHUNK_MANIFEST_VERSION_METADATA_KEY,
                    )),
                    AttributeValue::from(
                        manifest.icechunk_manifest_format_version.to_string(),
                    ),
                ),
            ])
        } else {
            Attributes::new()
        };
        let options = PutOptions { attributes, ..PutOptions::default() };
        let len = buffer.len() as u64;
        // FIXME: use multipart
        self.store.put_opts(&path, buffer.into(), options).await?;
        Ok(len)
    }

    async fn write_transaction_log(
        &self,
        _settings: &Settings,
        id: SnapshotId,
        log: Arc<TransactionLog>,
    ) -> StorageResult<()> {
        let path = self.get_transaction_path(&id);
        let bytes = rmp_serde::to_vec(log.as_ref())?;
        let attributes = if self.supports_metadata() {
            Attributes::from_iter(vec![
                (
                    Attribute::ContentType,
                    AttributeValue::from(
                        format_constants::LATEST_ICECHUNK_TRANSACTION_LOG_CONTENT_TYPE,
                    ),
                ),
                (
                    Attribute::Metadata(std::borrow::Cow::Borrowed(
                        format_constants::LATEST_ICECHUNK_TRANSACTION_LOG_VERSION_METADATA_KEY,
                    )),
                    AttributeValue::from(
                        log.icechunk_transaction_log_format_version.to_string(),
                    ),
                ),
            ])
        } else {
            Attributes::new()
        };
        let options = PutOptions { attributes, ..PutOptions::default() };
        // FIXME: use multipart
        self.store.put_opts(&path, bytes.into(), options).await?;
        Ok(())
    }

    async fn fetch_chunk(
        &self,
        _settings: &Settings,
        id: &ChunkId,
        range: &ByteRange,
    ) -> Result<Bytes, StorageError> {
        let path = self.get_chunk_path(id);
        // TODO: shall we split `range` into multiple ranges and use get_ranges?
        // I can't tell that `get_range` does splitting
        let options =
            GetOptions { range: Option::<GetRange>::from(range), ..Default::default() };
        let chunk = self.store.get_opts(&path, options).await?.bytes().await?;
        Ok(chunk)
    }

    async fn write_chunk(
        &self,
        _settings: &Settings,
        id: ChunkId,
        bytes: bytes::Bytes,
    ) -> Result<(), StorageError> {
        let path = self.get_chunk_path(&id);
        let upload = self.store.put_multipart(&path).await?;
        // TODO: new_with_chunk_size?
        let mut write = object_store::WriteMultipart::new(upload);
        write.write(&bytes);
        write.finish().await?;
        Ok(())
    }

    async fn get_ref(&self, _settings: &Settings, ref_key: &str) -> StorageResult<Bytes> {
        let key = self.ref_key(ref_key);
        match self.store.get(&key).await {
            Ok(res) => Ok(res.bytes().await?),
            Err(object_store::Error::NotFound { .. }) => {
                Err(StorageError::RefNotFound(key.to_string()))
            }
            Err(err) => Err(err.into()),
        }
    }

    async fn ref_names(&self, _settings: &Settings) -> StorageResult<Vec<String>> {
        // FIXME: i don't think object_store's implementation of list_with_delimiter is any good
        // we need to test if it even works beyond 1k refs
        let prefix = self.ref_key("");

        Ok(self
            .store
            .list_with_delimiter(Some(prefix.clone()).as_ref())
            .await?
            .common_prefixes
            .iter()
            .filter_map(|path| {
                self.drop_prefix(&prefix, path).map(|path| path.to_string())
            })
            .collect())
    }

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

    async fn write_ref(
        &self,
        _settings: &Settings,
        ref_key: &str,
        overwrite_refs: bool,
        bytes: Bytes,
    ) -> StorageResult<()> {
        let key = self.ref_key(ref_key);
        let mode = if overwrite_refs { PutMode::Overwrite } else { PutMode::Create };
        let opts = PutOptions { mode, ..PutOptions::default() };

        self.store
            .put_opts(&key, PutPayload::from_bytes(bytes), opts)
            .await
            .map_err(|e| match e {
                object_store::Error::AlreadyExists { path, .. } => {
                    StorageError::RefAlreadyExists(path)
                }
                _ => e.into(),
            })
            .map(|_| ())
    }

    async fn list_objects<'a>(
        &'a self,
        _settings: &Settings,
        prefix: &str,
    ) -> StorageResult<BoxStream<'a, StorageResult<ListInfo<String>>>> {
        let prefix =
            ObjectPath::from(format!("{}/{}", self.config.prefix.as_str(), prefix));
        let stream = self
            .store
            .list(Some(&prefix))
            // TODO: we should signal error instead of filtering
            .try_filter_map(|object| ready(Ok(object_to_list_info(&object))))
            .err_into();
        Ok(stream.boxed())
    }

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

    #[test]
    fn test_serialize_object_store() {
        let tmp_dir = TempDir::new().unwrap();
        let store = ObjectStorage::new_local_filesystem(tmp_dir.path()).unwrap();

        let serialized = serde_json::to_string(&store).unwrap();

        assert_eq!(
            serialized,
            format!(
                r#"{{"url":"file://{}","prefix":"","options":[]}}"#,
                std::fs::canonicalize(tmp_dir.path()).unwrap().to_str().unwrap()
            )
        );

        let deserialized: ObjectStorage = serde_json::from_str(&serialized).unwrap();
        assert_eq!(store.config, deserialized.config);
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

    #[test]
    fn test_canonicalize_path() {
        // Absolute path
        let tmp_dir = TempDir::new().unwrap();
        let store = ObjectStorage::new_local_filesystem(tmp_dir.path());
        assert!(store.is_ok());

        // Relative path
        let rel_path = "relative/path";
        let store =
            ObjectStorage::new_local_filesystem(PathBuf::from(&rel_path).as_path());
        assert!(store.is_ok());

        // Relative with leading ./
        let rel_path = TestLocalPath("./other/path".to_string());
        let store =
            ObjectStorage::new_local_filesystem(PathBuf::from(&rel_path).as_path());
        assert!(store.is_ok());
    }
}
