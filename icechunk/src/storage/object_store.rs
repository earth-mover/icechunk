use crate::format::{
    attributes::AttributesTable, manifest::Manifest, snapshot::Snapshot, AttributesId,
    ByteRange, ChunkId, FileTypeTag, ManifestId, ObjectId, SnapshotId,
};
use async_trait::async_trait;
use bytes::Bytes;
use core::fmt;
use futures::{stream::BoxStream, StreamExt, TryStreamExt};
use object_store::{
    aws::{AmazonS3Builder, S3ConditionalPut},
    local::LocalFileSystem,
    memory::InMemory,
    path::Path as ObjectPath,
    GetOptions, GetRange, ObjectStore, PutMode, PutOptions, PutPayload,
};
use serde::{Deserialize, Serialize};
use std::{
    fs::create_dir_all, future::ready, ops::Bound, path::Path as StdPath, sync::Arc,
};

use super::{Storage, StorageError, StorageResult};

// Get Range is object_store specific, keep it with this module
impl From<&ByteRange> for Option<GetRange> {
    fn from(value: &ByteRange) -> Self {
        match (value.0, value.1) {
            (Bound::Included(start), Bound::Excluded(end)) => {
                Some(GetRange::Bounded(start as usize..end as usize))
            }
            (Bound::Included(start), Bound::Unbounded) => {
                Some(GetRange::Offset(start as usize))
            }
            (Bound::Included(start), Bound::Included(end)) => {
                Some(GetRange::Bounded(start as usize..end as usize + 1))
            }
            (Bound::Excluded(start), Bound::Excluded(end)) => {
                Some(GetRange::Bounded(start as usize + 1..end as usize))
            }
            (Bound::Excluded(start), Bound::Unbounded) => {
                Some(GetRange::Offset(start as usize + 1))
            }
            (Bound::Excluded(start), Bound::Included(end)) => {
                Some(GetRange::Bounded(start as usize + 1..end as usize + 1))
            }
            (Bound::Unbounded, Bound::Excluded(end)) => {
                Some(GetRange::Bounded(0..end as usize))
            }
            (Bound::Unbounded, Bound::Included(end)) => {
                Some(GetRange::Bounded(0..end as usize + 1))
            }
            (Bound::Unbounded, Bound::Unbounded) => None,
        }
    }
}

const SNAPSHOT_PREFIX: &str = "snapshots/";
const MANIFEST_PREFIX: &str = "manifests/";
// const ATTRIBUTES_PREFIX: &str = "attributes/";
const CHUNK_PREFIX: &str = "chunks/";
const REF_PREFIX: &str = "refs";

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct S3Credentials {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct S3Config {
    pub region: Option<String>,
    pub endpoint: Option<String>,
    pub credentials: Option<S3Credentials>,
    pub allow_http: Option<bool>,
}

// TODO: Hide this behind a feature flag?
impl S3Config {
    pub fn to_builder(&self) -> AmazonS3Builder {
        let builder = if let Some(credentials) = &self.credentials {
            let builder = AmazonS3Builder::new()
                .with_access_key_id(credentials.access_key_id.clone())
                .with_secret_access_key(credentials.secret_access_key.clone());

            if let Some(token) = &credentials.session_token {
                builder.with_token(token.clone())
            } else {
                builder
            }
        } else {
            AmazonS3Builder::from_env()
        };

        let builder = if let Some(region) = &self.region {
            builder.with_region(region.clone())
        } else {
            builder
        };

        let builder = if let Some(endpoint) = &self.endpoint {
            builder.with_endpoint(endpoint.clone())
        } else {
            builder
        };

        if let Some(allow_http) = self.allow_http {
            builder.with_allow_http(allow_http)
        } else {
            builder
        }
    }
}

pub struct ObjectStorage {
    store: Arc<dyn ObjectStore>,
    prefix: String,
    // We need this because object_store's local file implementation doesn't sort refs. Since this
    // implementation is used only for tests, it's OK to sort in memory.
    artificially_sort_refs_in_mem: bool,

    supports_create_if_not_exists: bool,
}

impl ObjectStorage {
    /// Create an in memory Storage implementantion
    ///
    /// This implementation should not be used in production code.
    pub fn new_in_memory_store(prefix: Option<String>) -> ObjectStorage {
        #[allow(clippy::expect_used)]
        let prefix =
            prefix.or(Some("".to_string())).expect("bad prefix but this should not fail");
        ObjectStorage {
            store: Arc::new(InMemory::new()),
            prefix,
            artificially_sort_refs_in_mem: false,
            supports_create_if_not_exists: true,
        }
    }

    /// Create an local filesystem Storage implementantion
    ///
    /// This implementation should not be used in production code.
    pub fn new_local_store(prefix: &StdPath) -> Result<ObjectStorage, std::io::Error> {
        create_dir_all(prefix)?;
        let prefix = prefix.display().to_string();
        let store = Arc::new(LocalFileSystem::new_with_prefix(prefix.clone())?);
        Ok(ObjectStorage {
            store,
            prefix: "".to_string(),
            artificially_sort_refs_in_mem: true,
            supports_create_if_not_exists: true,
        })
    }

    pub fn new_s3_store(
        bucket_name: impl Into<String>,
        prefix: impl Into<String>,
        config: Option<S3Config>,
    ) -> Result<ObjectStorage, StorageError> {
        let config = config.unwrap_or_default();
        let builder = config.to_builder();
        let builder = builder.with_conditional_put(S3ConditionalPut::ETagMatch);
        let store = builder.with_bucket_name(bucket_name.into()).build()?;
        Ok(ObjectStorage {
            store: Arc::new(store),
            prefix: prefix.into(),
            artificially_sort_refs_in_mem: false,
            supports_create_if_not_exists: true,
        })
    }

    fn get_path<const SIZE: usize, T: FileTypeTag>(
        &self,
        file_prefix: &str,
        extension: &str,
        id: &ObjectId<SIZE, T>,
    ) -> ObjectPath {
        // TODO: be careful about allocation here
        // we serialize the url using crockford
        let path = format!("{}/{}/{}{}", self.prefix, file_prefix, id, extension);
        ObjectPath::from(path)
    }

    fn get_snapshot_path(&self, id: &SnapshotId) -> ObjectPath {
        self.get_path(SNAPSHOT_PREFIX, ".msgpack", id)
    }

    fn get_manifest_path(&self, id: &ManifestId) -> ObjectPath {
        self.get_path(MANIFEST_PREFIX, ".msgpack", id)
    }

    fn get_chunk_path(&self, id: &ChunkId) -> ObjectPath {
        self.get_path(CHUNK_PREFIX, "", id)
    }

    fn drop_prefix(&self, prefix: &ObjectPath, path: &ObjectPath) -> Option<ObjectPath> {
        path.prefix_match(&ObjectPath::from(format!("{}", prefix))).map(|it| it.collect())
    }

    fn ref_key(&self, ref_key: &str) -> ObjectPath {
        // ObjectPath knows how to deal with empty path parts: bar//foo
        ObjectPath::from(format!("{}/{}/{}", self.prefix.as_str(), REF_PREFIX, ref_key))
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
}

impl fmt::Debug for ObjectStorage {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "ObjectStorage, prefix={}, store={}", self.prefix, self.store)
    }
}
#[async_trait]
impl Storage for ObjectStorage {
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

    async fn write_snapshot(
        &self,
        id: SnapshotId,
        table: Arc<Snapshot>,
    ) -> Result<(), StorageError> {
        let path = self.get_snapshot_path(&id);
        let bytes = rmp_serde::to_vec(table.as_ref())?;
        // FIXME: use multipart
        self.store.put(&path, bytes.into()).await?;
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
        table: Arc<Manifest>,
    ) -> Result<(), StorageError> {
        let path = self.get_manifest_path(&id);
        let bytes = rmp_serde::to_vec(table.as_ref())?;
        // FIXME: use multipart
        self.store.put(&path, bytes.into()).await?;
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

    async fn ref_versions(&self, ref_name: &str) -> BoxStream<StorageResult<String>> {
        let res = self.do_ref_versions(ref_name).await;
        if self.artificially_sort_refs_in_mem {
            #[allow(clippy::expect_used)]
            // This branch is used for local tests, not in production. We don't expect the size of
            // these streams to be large, so we can collect in memory and fail early if there is an
            // error
            let mut all =
                res.try_collect::<Vec<_>>().await.expect("Error fetching ref versions");
            all.sort();
            futures::stream::iter(all.into_iter().map(Ok)).boxed()
        } else {
            res
        }
    }

    async fn write_ref(
        &self,
        ref_key: &str,
        overwrite_refs: bool,
        bytes: Bytes,
    ) -> StorageResult<()> {
        let key = self.ref_key(ref_key);
        let mode = if overwrite_refs || !self.supports_create_if_not_exists {
            PutMode::Overwrite
        } else {
            PutMode::Create
        };
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
}
