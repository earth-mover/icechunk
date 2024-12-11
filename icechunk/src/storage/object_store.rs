use crate::{
    format::{
        attributes::AttributesTable, format_constants, manifest::Manifest,
        snapshot::Snapshot, transaction_log::TransactionLog, AttributesId, ByteRange,
        ChunkId, FileTypeTag, ManifestId, ObjectId, SnapshotId,
    },
    private,
};
use async_trait::async_trait;
use bytes::Bytes;
use futures::{
    stream::{self, BoxStream},
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
    ops::Range,
    path::Path as StdPath,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};
use url::Url;

use super::{
    ETag, ListInfo, Storage, StorageError, StorageResult, CHUNK_PREFIX, CONFIG_PATH,
    MANIFEST_PREFIX, REF_PREFIX, SNAPSHOT_PREFIX, TRANSACTION_PREFIX,
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

#[derive(Debug, Serialize, Deserialize)]
pub struct ObjectStorageConfig {
    url: String,
    prefix: String,
    options: Vec<(String, String)>,
}

#[derive(Debug, Serialize)]
pub struct ObjectStorage {
    #[serde(skip)]
    store: Arc<dyn ObjectStore>,
    config: ObjectStorageConfig,
}

impl ObjectStorage {
    /// Create an in memory Storage implementation
    ///
    /// This implementation should not be used in production code.
    pub fn new_in_memory_store(prefix: Option<String>) -> Result<ObjectStorage, String> {
        #[allow(clippy::expect_used)]
        let prefix =
            prefix.or(Some("".to_string())).expect("bad prefix but this should not fail");

        let url = format!("memory:/{prefix}");
        Ok(Self::from_url(&url, vec![])?)
    }

    /// Create an local filesystem Storage implementation
    ///
    /// This implementation should not be used in production code.
    pub fn new_local_store(prefix: &StdPath) -> Result<ObjectStorage, String> {
        create_dir_all(prefix).map_err(|e| e.to_string())?;
        let prefix = prefix.display().to_string();
        let url = format!("file://{prefix}");
        Ok(Self::from_url(&url, vec![])?)
    }

    /// Create an ObjectStore client from a URL and provided options
    pub fn from_url(
        url: &str,
        options: Vec<(String, String)>,
    ) -> Result<ObjectStorage, String> {
        let url: Url = Url::parse(url).map_err(|e| e.to_string())?;
        if url.scheme() == "file" {
            let path = url.path();
            let store = Arc::new(
                LocalFileSystem::new_with_prefix(path).map_err(|e| e.to_string())?,
            );
            return Ok(ObjectStorage {
                store,
                config: ObjectStorageConfig {
                    url: url.to_string(),
                    prefix: "".to_string(),
                    options,
                },
            });
        }

        let (store, path) =
            parse_url_opts(&url, options.clone()).map_err(|e| e.to_string())?;
        let store: Arc<dyn ObjectStore> = Arc::from(store);
        Ok(ObjectStorage {
            store,
            config: ObjectStorageConfig {
                url: url.to_string(),
                prefix: path.to_string(),
                options,
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
}

impl private::Sealed for ObjectStorage {}

impl<'de> serde::Deserialize<'de> for ObjectStorage {
    fn deserialize<D>(deserializer: D) -> Result<ObjectStorage, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let config = ObjectStorageConfig::deserialize(deserializer)?;
        ObjectStorage::from_url(&config.url, config.options)
            .map_err(serde::de::Error::custom)
    }
}

#[async_trait]
#[typetag::serde]
impl Storage for ObjectStorage {
    async fn fetch_config(&self) -> StorageResult<Option<(Bytes, ETag)>> {
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
        _id: &AttributesId,
    ) -> Result<Arc<AttributesTable>, StorageError> {
        todo!();
    }

    async fn fetch_manifests(
        &self,
        id: &ManifestId,
    ) -> Result<Arc<Manifest>, StorageError> {
        let path = self.get_manifest_path(id);
        let bytes = self.store.get(&path).await?.bytes().await?;
        // TODO: optimize using from_read
        let res = rmp_serde::from_slice(bytes.as_ref())?;
        Ok(Arc::new(res))
    }

    async fn fetch_transaction_log(
        &self,
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
        _id: AttributesId,
        _table: Arc<AttributesTable>,
    ) -> Result<(), StorageError> {
        todo!()
    }

    async fn write_manifests(
        &self,
        id: ManifestId,
        manifest: Arc<Manifest>,
    ) -> Result<(), StorageError> {
        let path = self.get_manifest_path(&id);
        let bytes = rmp_serde::to_vec(manifest.as_ref())?;
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
        // FIXME: use multipart
        self.store.put_opts(&path, bytes.into(), options).await?;
        Ok(())
    }

    async fn write_transaction_log(
        &self,
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

    async fn get_ref(&self, ref_key: &str) -> StorageResult<Bytes> {
        let key = self.ref_key(ref_key);
        match self.store.get(&key).await {
            Ok(res) => Ok(res.bytes().await?),
            Err(object_store::Error::NotFound { .. }) => {
                Err(StorageError::RefNotFound(key.to_string()))
            }
            Err(err) => Err(err.into()),
        }
    }

    async fn ref_names(&self) -> StorageResult<Vec<String>> {
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
