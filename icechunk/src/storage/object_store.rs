use core::fmt;
use std::{fs::create_dir_all, future::ready, ops::Bound, sync::Arc};

use async_trait::async_trait;
use bytes::Bytes;
use futures::{stream::BoxStream, StreamExt, TryStreamExt};
use object_store::{
    local::LocalFileSystem, memory::InMemory, path::Path as ObjectPath, GetOptions,
    GetRange, ObjectStore, PutMode, PutOptions, PutPayload,
};

use crate::format::{
    attributes::AttributesTable, manifest::Manifest, snapshot::Snapshot, ByteRange,
    ObjectId, Path,
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
}

impl ObjectStorage {
    pub fn new_in_memory_store() -> ObjectStorage {
        ObjectStorage { store: Arc::new(InMemory::new()), prefix: "".into() }
    }
    pub fn new_local_store(prefix: &Path) -> Result<ObjectStorage, std::io::Error> {
        create_dir_all(prefix.as_path())?;
        let prefix = prefix.display().to_string();
        let store = Arc::new(LocalFileSystem::new_with_prefix(prefix.clone())?);
        Ok(ObjectStorage { store, prefix: "".to_string() })
    }

    pub fn new_s3_store_from_env(
        bucket_name: impl Into<String>,
        prefix: impl Into<String>,
    ) -> Result<ObjectStorage, StorageError> {
        use object_store::aws::AmazonS3Builder;
        let store =
            AmazonS3Builder::from_env().with_bucket_name(bucket_name.into()).build()?;
        Ok(ObjectStorage { store: Arc::new(store), prefix: prefix.into() })
    }

    pub fn new_s3_store_with_config(
        bucket_name: impl Into<String>,
        prefix: impl Into<String>,
        access_key_id: impl Into<String>,
        secret_access_key: impl Into<String>,
        endpoint: Option<impl Into<String>>,
    ) -> Result<ObjectStorage, StorageError> {
        use object_store::aws::AmazonS3Builder;
        let builder = AmazonS3Builder::new()
            .with_access_key_id(access_key_id)
            .with_secret_access_key(secret_access_key);

        let builder = if let Some(endpoint) = endpoint {
            // TODO: Check if HTTP is allowed always or based on endpoint
            builder.with_endpoint(endpoint).with_allow_http(true)
        } else {
            builder
        };

        let store = builder.with_bucket_name(bucket_name.into()).build()?;
        Ok(ObjectStorage { store: Arc::new(store), prefix: prefix.into() })
    }

    fn get_path(&self, file_prefix: &str, id: &ObjectId) -> ObjectPath {
        // TODO: be careful about allocation here
        // we serialize the url using crockford
        let path = format!("{}/{}/{}.msgpack", self.prefix, file_prefix, id);
        ObjectPath::from(path)
    }

    fn drop_prefix(&self, prefix: &ObjectPath, path: &ObjectPath) -> Option<ObjectPath> {
        path.prefix_match(&ObjectPath::from(format!("{}/{}", self.prefix, prefix)))
            .map(|it| it.collect())
    }

    fn ref_key(&self, ref_key: &str) -> ObjectPath {
        // ObjectPath knows how to deal with empty path parts: bar//foo
        ObjectPath::from(format!("{}/{}/{}", self.prefix.as_str(), REF_PREFIX, ref_key))
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
        let path = self.get_path(SNAPSHOT_PREFIX, id);
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
        let path = self.get_path(MANIFEST_PREFIX, id);
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
        let path = self.get_path(SNAPSHOT_PREFIX, &id);
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
        // let path = ObjectStorage::get_path(ATTRIBUTES_PREFIX, &id);
        // self.write_parquet(&path, &table.batch).await?;
        // Ok(())
    }

    async fn write_manifests(
        &self,
        id: ObjectId,
        table: Arc<Manifest>,
    ) -> Result<(), StorageError> {
        let path = self.get_path(MANIFEST_PREFIX, &id);
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
        let path = self.get_path(CHUNK_PREFIX, id);
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
        let path = self.get_path(CHUNK_PREFIX, &id);
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
        let ref_prefix = ObjectPath::from(REF_PREFIX);

        Ok(self
            .store
            .list_with_delimiter(Some(prefix).as_ref())
            .await?
            .common_prefixes
            .iter()
            .filter_map(|path| {
                self.drop_prefix(&ref_prefix, path).map(|path| path.to_string())
            })
            .collect())
    }

    async fn ref_versions(&self, ref_name: &str) -> BoxStream<StorageResult<String>> {
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
}
