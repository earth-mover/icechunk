use std::{
    future::ready,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use async_stream::try_stream;
use async_trait::async_trait;
use azure_core::prelude::Range;
use azure_storage::{prelude::*, CloudLocation};
use azure_storage_blobs::{ container::operations::BlobItem, prelude::*};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use time::format_description::well_known::Rfc3339;
use futures::{
    stream::{self, BoxStream},
    StreamExt, TryStreamExt,
};
use serde::{Deserialize, Serialize};

use crate::{
    format::{
        attributes::AttributesTable, manifest::Manifest,
        snapshot::Snapshot, AttributesId, ByteRange, ChunkId, FileTypeTag, ManifestId,
        SnapshotId,
    }, private, zarr::ObjectId, Storage, StorageError
};

use super::{ListInfo, StorageResult};

#[derive(Debug)]
pub struct AzureBlobStorage {
    client: Arc<ContainerClient>,
    prefix: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, Default)]
#[serde(tag = "type")]
pub enum AzureStorageCredentials {
    #[default]
    #[serde(rename = "anonymous")]
    Anonymous,
    #[serde(rename = "access_key")]
    AccessKey(Option<String>),
    #[serde(rename = "sas_token")]
    SASToken(String),
    #[serde(rename = "bearer_token")]
    BearerToken(String),
    // todo
    // #[serde(rename = "token_credential")]
    // TokenCredential(Arc<dyn TokenCredential>),
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub enum Location {
    
    #[serde(rename = "public")]
    Public(String),
    #[serde(rename = "china")]
    China(String),
    #[serde(rename = "emulator")]
    Emulator(String, u16),
    #[serde(rename = "custom")]
    Custom(String, String),
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct AzureBlobConfig {
    pub cloud_location: Location,
    pub credentials: AzureStorageCredentials,
}

pub async fn mk_client(config: &AzureBlobConfig) -> ClientBuilder {
    let (location, account) = match config.cloud_location.clone() {
        Location::Public(account) => (CloudLocation::Public { account: account.clone() }, Some(account)),
            Location::China(account) => (CloudLocation::China { account: account.clone() }, Some(account)),
            Location::Emulator(address, port) => (CloudLocation::Emulator { address, port }, None),
            Location::Custom(account, uri) => (CloudLocation::Custom {  account: account.clone(), uri }, Some(account)),
    };

    let storage_credentials = match config.credentials.clone() {
        AzureStorageCredentials::Anonymous => StorageCredentials::anonymous(),
        AzureStorageCredentials::AccessKey(key) => {
            if matches!(config.cloud_location, Location::Emulator { .. }) {
                StorageCredentials::emulator()
            } else {
                StorageCredentials::access_key(account.unwrap(), key.unwrap())
            }
        },
        AzureStorageCredentials::SASToken(token) => StorageCredentials::sas_token(token).unwrap(),
        AzureStorageCredentials::BearerToken(token) => StorageCredentials::bearer_token(token),
    };

    ClientBuilder::with_location(location, storage_credentials)
}

const SNAPSHOT_PREFIX: &str = "snapshots/";
const MANIFEST_PREFIX: &str = "manifests/";
// const ATTRIBUTES_PREFIX: &str = "attributes/";
const CHUNK_PREFIX: &str = "chunks/";
const REF_PREFIX: &str = "refs";

impl AzureBlobStorage {
    pub async fn new_azure_blob_store(
        container_name: impl Into<String>,
        prefix: impl Into<String>,
        config: &AzureBlobConfig,
    ) -> Result<AzureBlobStorage, StorageError> {
        let client_builder = mk_client(config).await;
        let client = Arc::new(client_builder.container_client(container_name));
        Ok(AzureBlobStorage { client, prefix: prefix.into() })
    }

    fn get_path_str(&self, file_prefix: &str, id: &str) -> StorageResult<String> {
        let path = PathBuf::from_iter([self.prefix.as_str(), file_prefix, id]);
        path.into_os_string().into_string().map_err(StorageError::BadPrefix)
    }

    fn get_path<const SIZE: usize, T: FileTypeTag>(
        &self,
        file_prefix: &str,
        id: &ObjectId<SIZE, T>,
    ) -> StorageResult<String> {
        // we serialize the url using crockford
        self.get_path_str(file_prefix, id.to_string().as_str())
    }

    fn get_manifest_path(&self, id: &ManifestId) -> StorageResult<String> {
        self.get_path(MANIFEST_PREFIX, id)
    }

    fn get_chunk_path(&self, id: &ChunkId) -> StorageResult<String> {
        self.get_path(CHUNK_PREFIX, id)
    }

    fn get_snapshot_path(&self, id: &SnapshotId) -> StorageResult<String> {
        self.get_path(SNAPSHOT_PREFIX, id)
    }

    fn get_blob_client(&self, blob_name: &str) -> BlobClient {
            self.client.blob_client(blob_name)
    }

    async fn get_object(&self, key: &str) -> StorageResult<Bytes> {
        let blob_client = self.get_blob_client(key);
        let result = blob_client.get_content().await?;
        Ok(Bytes::from(result))
    }

    async fn get_object_range(&self, key: &str, byte_range: &ByteRange) -> StorageResult<Bytes> {
        let blob_client = self.get_blob_client(key);
        Ok(get_object_range(&blob_client, byte_range)
            .await
            .map_err(StorageError::from)?)
    }

    async fn put_object(
        &self,
        key: &str,
        bytes: Vec<u8>,
    ) -> StorageResult<()> {
        let blob_client = self.get_blob_client(key);
        let body: azure_core::Body = bytes.into();
        blob_client.put_block_blob(body).await?;
        Ok(())
    }

    
    fn ref_key(&self, ref_key: &str) -> StorageResult<String> {
        let path = PathBuf::from_iter([self.prefix.as_str(), REF_PREFIX, ref_key]);
        path.into_os_string().into_string().map(|s| s.replace("\\", "/")).map_err(StorageError::BadPrefix)
    }

    async fn create_container_if_not_exists(&self) -> Result<(), StorageError> {
        if self.client.exists().await? {
            return Ok(());
        }

        self.client.create().await.map_err(StorageError::from)
    }

}

pub async fn get_object_range(blob_client: &BlobClient, byte_range: &ByteRange) -> StorageResult<Bytes> {
    let range = match byte_range {
        ByteRange::Bounded(range) => Some(Range::new(range.start, range.end)),
        ByteRange::From(offset) => Some(Range::new(*offset, u64::MAX)),
        ByteRange::Last(n) => Some(Range::new(u64::MAX - n, u64::MAX)),
    };

    let mut result: Vec<u8> = vec![];

    // The stream is composed of individual calls to the get blob endpoint
    let mut stream = blob_client.get().range(range.unwrap()).into_stream();
    while let Some(value) = stream.next().await {
        let mut body = value?.data;
        // For each response, we stream the body instead of collecting it all
        // into one large allocation.
        while let Some(value) = body.next().await {
            let value = value?;
            result.extend(&value);
        }
    }

    Ok(Bytes::from(result))
}

pub async fn create_container_if_not_exists(
    storage: &AzureBlobStorage,
) -> Result<(), StorageError> {
    storage.create_container_if_not_exists().await
}

impl private::Sealed for AzureBlobStorage {}

#[async_trait]
impl Storage for AzureBlobStorage {
    async fn fetch_snapshot(&self, id: &SnapshotId) -> StorageResult<Arc<Snapshot>> {
        let key = self.get_snapshot_path(id)?;
        let bytes = self.get_object(&key).await?;
        let res = rmp_serde::from_slice(bytes.as_ref())?;
        Ok(Arc::new(res))
    }

    async fn fetch_attributes(
        &self,
        _id: &AttributesId,
    ) -> StorageResult<Arc<AttributesTable>> {
        todo!()
    }

    async fn fetch_manifests(&self, id: &ManifestId) -> StorageResult<Arc<Manifest>> {
        let key = self.get_manifest_path(id)?;
        let bytes = self.get_object(key.as_str()).await?;
        // TODO: optimize using from_read
        let res = rmp_serde::from_slice(bytes.as_ref())?;
        Ok(Arc::new(res))
    }

    async fn fetch_chunk(&self, id: &ChunkId, range: &ByteRange) -> StorageResult<Bytes> {
        let key = self.get_chunk_path(id)?;
        let bytes = self.get_object_range(key.as_str(), range).await?;
        Ok(bytes)
    }

    async fn write_snapshot(
        &self,
        id: SnapshotId,
        snapshot: Arc<Snapshot>,
    ) -> StorageResult<()> {
        let key = self.get_snapshot_path(&id)?;
        let bytes = rmp_serde::to_vec(snapshot.as_ref())?;
        self.put_object(
            key.as_str(),
            bytes,
        )
        .await
    }

    async fn write_attributes(
        &self,
        _id: AttributesId,
        _table: Arc<AttributesTable>,
    ) -> StorageResult<()> {
        todo!()
    }

    async fn write_manifests(
        &self,
        id: ManifestId,
        manifest: Arc<Manifest>,
    ) -> Result<(), StorageError> {
        let key = self.get_manifest_path(&id)?;
        let bytes = rmp_serde::to_vec(manifest.as_ref())?;
        self.put_object(
            key.as_str(),
            bytes,
        )
        .await
    }

    async fn write_chunk(
        &self,
        id: ChunkId,
        bytes: bytes::Bytes,
    ) -> Result<(), StorageError> {
        let key = self.get_chunk_path(&id)?;
        //FIXME: use multipart upload
        let body = bytes.to_vec();
        self.put_object(key.as_str(), body).await
    }

    async fn get_ref(&self, ref_key: &str) -> StorageResult<Bytes> {
        let key = self.ref_key(ref_key)?;
        let blob_client = self.get_blob_client(key.as_str());
        if blob_client.exists().await? {
            let res = blob_client.get_content().await?;
            Ok(Bytes::from(res))
        } else {
            Err(StorageError::RefNotFound(key))
        }
    }

    async fn ref_names(&self) -> StorageResult<Vec<String>> {
        let prefix = self.ref_key("")?;
        let mut stream = self.client.list_blobs().prefix(prefix.clone()).delimiter("/").into_stream();
        let mut res = Vec::new();
        while let Some(value) = stream.next().await {
            for grouped_prefix in value?.blobs.prefixes() {
                if let Some(key) = grouped_prefix
                    .name
                    .strip_prefix(prefix.clone().as_str())
                    .and_then(|key| key.strip_suffix('/'))
                {
                    res.push(key.to_string());
                }                
            }
        }

        Ok(res)
    }

    async fn ref_versions(
        &self,
        ref_name: &str,
    ) -> StorageResult<BoxStream<StorageResult<String>>> {
        let prefix = self.ref_key(ref_name)?;
        let mut pageable = self.client.list_blobs().prefix(prefix.clone()).into_stream();
        
        let prefix = prefix + "/";
        let stream = try_stream! {
            while let Some(value) = pageable.next().await {
                let value = value?;
                for blob in value.blobs.blobs() {
                    if let Some(key) = blob.name.strip_prefix(prefix.as_str()).map(|s| s.to_string())
                    {
                        yield key.to_string()
                    }
                }
            }
        };
        Ok(stream.boxed())
    }

    async fn write_ref(
        &self,
        ref_key: &str,
        overwrite_refs: bool,
        bytes: Bytes,
    ) -> StorageResult<()> {
        let key = self.ref_key(ref_key)?;
        let blob_client = self.get_blob_client(key.as_str());
        if !overwrite_refs {
            if blob_client.exists().await? {
                return Err(StorageError::RefAlreadyExists(key));
            }
        }
        let body: azure_core::Body = bytes.into();
        blob_client.put_block_blob(body).await?;
        Ok(())
    }

    async fn list_objects<'a>(
        &'a self,
        prefix: &str,
    ) -> StorageResult<BoxStream<'a, StorageResult<ListInfo<String>>>> {
        let prefix = PathBuf::from_iter([self.prefix.as_str(), prefix])
            .into_os_string()
            .into_string()
            .map_err(StorageError::BadPrefix)?;
        let stream = self
            .client
            .list_blobs()
            .prefix(prefix.clone())
            .into_stream()
            .try_filter_map(|page| {
                let contents = Some(page.blobs.items).map(|cont| stream::iter(cont).map(Ok));
                ready(Ok(contents))
            })
            .try_flatten()
            // TODO: we should signal error instead of filtering
            .try_filter_map(|object| ready(Ok(object_to_list_info(&object))));
        Ok(stream.boxed())
    }

    async fn delete_objects(
        &self,
        prefix: &str,
        ids: BoxStream<'_, String>,
    ) -> StorageResult<usize> {
        let deleted = Arc::new(AtomicUsize::new(0));
        ids.chunks(1_000)
            // FIXME: configurable concurrency
            .for_each_concurrent(10, |batch| {
                let prefix = prefix.to_string();
                let deleted = &deleted;
                async move {
                    for id in batch {
                        match self.get_path_str(&prefix, &id) {
                            Ok(key) => {
                                let blob_client = self.get_blob_client(key.as_str());
                                let blob_client = blob_client.clone();
                                let _ = blob_client.delete().await.ok();
                                deleted.fetch_add(1, Ordering::Release);
                            }
                            Err(_) => continue,
                        }
                    }
                }
            })
            .await;
        Ok(deleted.load(Ordering::Acquire))
    }
}

fn object_to_list_info(object: &BlobItem) -> Option<ListInfo<String>> {
    let key = match object {
            BlobItem::Blob(blob) => Some(blob.name.clone()),
            BlobItem::BlobPrefix(_) => None,
        };
    let last_modified = match object {
        BlobItem::Blob(blob) => Some(blob.properties.last_modified.clone()),
        BlobItem::BlobPrefix(_) => None,        
    };

    // convert from time::OffsetDateTime to chrono::DateTime<Utc>
    let created_at = last_modified
        .and_then(|dt| {
            let formatted = dt.format(&Rfc3339).ok()?;
            DateTime::parse_from_rfc3339(&formatted)
                .ok()
                .map(|dt| dt.with_timezone(&Utc))
        })?;
    let id = Path::new(key.as_ref()?).file_name().and_then(|s| s.to_str())?.to_string();
    Some(ListInfo { id, created_at })
}