use core::fmt;
use std::{
    collections::HashMap, fs::create_dir_all, future::ready, ops::Bound,
    path::Path as StdPath, sync::Arc,
};

use crate::format::{
    attributes::AttributesTable,
    manifest::{Manifest, VirtualChunkLocation, VirtualReferenceError},
    snapshot::Snapshot,
    ByteRange, ObjectId,
};
use crate::storage::virtual_ref::VirtualChunkResolver;
use async_trait::async_trait;
use bytes::Bytes;
use futures::{stream::BoxStream, StreamExt, TryStreamExt};
use object_store::{
    aws::AmazonS3Builder, local::LocalFileSystem, memory::InMemory,
    path::Path as ObjectPath, GetOptions, GetRange, ObjectStore, PutMode, PutOptions,
    PutPayload,
};
use tokio::sync::RwLock;
use url;

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
                Some(GetRange::Suffix(end as usize))
            }
            (Bound::Unbounded, Bound::Included(end)) => {
                Some(GetRange::Suffix(end as usize + 1))
            }
            (Bound::Unbounded, Bound::Unbounded) => None,
        }
    }
}

const SNAPSHOT_PREFIX: &str = "s/";
const MANIFEST_PREFIX: &str = "m/";
// const ATTRIBUTES_PREFIX: &str = "a/";
const CHUNK_PREFIX: &str = "c/";
const REF_PREFIX: &str = "r";

pub struct ObjectStorage {
    store: Arc<dyn ObjectStore>,
    prefix: String,
    // We need this because object_store's local file implementation doesn't sort refs. Since this
    // implementation is used only for tests, it's OK to sort in memory.
    artificially_sort_refs_in_mem: bool,

    // We need this because object_store's hasn't implemented support for create-if-not-exists in
    // S3 yet. We'll delete this after they do.
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
        access_key_id: Option<impl Into<String>>,
        secret_access_key: Option<impl Into<String>>,
        session_token: Option<impl Into<String>>,
        endpoint: Option<impl Into<String>>,
    ) -> Result<ObjectStorage, StorageError> {
        use object_store::aws::AmazonS3Builder;

        let builder = if let (Some(access_key_id), Some(secret_access_key)) =
            (access_key_id, secret_access_key)
        {
            AmazonS3Builder::new()
                .with_access_key_id(access_key_id)
                .with_secret_access_key(secret_access_key)
        } else {
            AmazonS3Builder::from_env()
        };

        let builder = if let Some(session_token) = session_token {
            builder.with_token(session_token)
        } else {
            builder
        };

        // FIXME: this is a hack to pretend we do this only for S3
        // this will go away once object_store supports create-if-not-exist on S3
        let supports_create_if_not_exists = endpoint.is_some();

        let builder = if let Some(endpoint) = endpoint {
            builder.with_endpoint(endpoint).with_allow_http(true)
        } else {
            builder
        };

        let store = builder.with_bucket_name(bucket_name.into()).build()?;
        Ok(ObjectStorage {
            store: Arc::new(store),
            prefix: prefix.into(),
            artificially_sort_refs_in_mem: false,
            supports_create_if_not_exists,
        })
    }

    fn get_path(&self, file_prefix: &str, extension: &str, id: &ObjectId) -> ObjectPath {
        // TODO: be careful about allocation here
        // we serialize the url using crockford
        let path = format!("{}/{}/{}{}", self.prefix, file_prefix, id, extension);
        ObjectPath::from(path)
    }

    fn get_snapshot_path(&self, id: &ObjectId) -> ObjectPath {
        self.get_path(SNAPSHOT_PREFIX, ".msgpack", id)
    }

    fn get_manifest_path(&self, id: &ObjectId) -> ObjectPath {
        self.get_path(MANIFEST_PREFIX, ".msgpack", id)
    }

    fn get_chunk_path(&self, id: &ObjectId) -> ObjectPath {
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
    async fn fetch_snapshot(&self, id: &ObjectId) -> Result<Arc<Snapshot>, StorageError> {
        let path = self.get_snapshot_path(id);
        let bytes = self.store.get(&path).await?.bytes().await?;
        // TODO: optimize using from_read
        let res = rmp_serde::from_slice(bytes.as_ref())?;
        Ok(Arc::new(res))
    }

    async fn fetch_attributes(
        &self,
        _id: &ObjectId,
    ) -> Result<Arc<AttributesTable>, StorageError> {
        todo!();
    }

    async fn fetch_manifests(
        &self,
        id: &ObjectId,
    ) -> Result<Arc<Manifest>, StorageError> {
        let path = self.get_manifest_path(id);
        let bytes = self.store.get(&path).await?.bytes().await?;
        // TODO: optimize using from_read
        let res = rmp_serde::from_slice(bytes.as_ref())?;
        Ok(Arc::new(res))
    }

    async fn write_snapshot(
        &self,
        id: ObjectId,
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
        _id: ObjectId,
        _table: Arc<AttributesTable>,
    ) -> Result<(), StorageError> {
        todo!()
    }

    async fn write_manifests(
        &self,
        id: ObjectId,
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
        id: &ObjectId,
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
        id: ObjectId,
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

#[derive(PartialEq, Eq, Hash, Clone, Debug, Default)]
struct StoreCacheKey(String, String);

#[derive(Debug, Default)]
pub struct ObjectStoreVirtualChunkResolver {
    stores: Arc<RwLock<HashMap<StoreCacheKey, Arc<dyn ObjectStore>>>>,
}

#[async_trait]
impl VirtualChunkResolver for ObjectStoreVirtualChunkResolver {
    async fn fetch_chunk(
        &self,
        location: &VirtualChunkLocation,
        range: &ByteRange,
    ) -> StorageResult<Bytes> {
        let VirtualChunkLocation::Absolute(location) = location;
        let parsed =
            url::Url::parse(location).map_err(VirtualReferenceError::CannotParseUrl)?;
        let bucket_name = parsed
            .host_str()
            .ok_or(VirtualReferenceError::CannotParseBucketName(
                "error parsing bucket name".into(),
            ))?
            .to_string();
        let path = ObjectPath::parse(parsed.path())
            .map_err(VirtualReferenceError::CannotParsePath)?;
        let scheme = parsed.scheme();
        let cache_key = StoreCacheKey(scheme.into(), bucket_name);

        let options =
            GetOptions { range: Option::<GetRange>::from(range), ..Default::default() };
        let store = {
            let stores = self.stores.read().await;
            #[allow(clippy::expect_used)]
            stores.get(&cache_key).map(|x| Arc::clone(x))
        };
        match store {
            Some(store) => Ok(store.get_opts(&path, options).await?.bytes().await?),
            None => {
                let builder = match scheme {
                    "s3" => AmazonS3Builder::from_env(),
                    _ => {
                        Err(VirtualReferenceError::UnsupportedScheme(scheme.to_string()))?
                    }
                };
                let new_store: Arc<dyn ObjectStore> =
                    Arc::new(builder.with_bucket_name(&cache_key.1).build()?);
                {
                    self.stores
                        .write()
                        .await
                        .insert(cache_key.clone(), Arc::clone(&new_store));
                }
                Ok(new_store.get_opts(&path, options).await?.bytes().await?)
            }
        }
    }
}
