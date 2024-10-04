use std::{ops::Bound, path::PathBuf, sync::Arc};

use async_stream::try_stream;
use async_trait::async_trait;
use aws_config::{meta::region::RegionProviderChain, AppName, BehaviorVersion};
use aws_credential_types::Credentials;
use aws_sdk_s3::{
    config::{Builder, Region},
    error::ProvideErrorMetadata,
    primitives::ByteStream,
    Client,
};
use bytes::Bytes;
use futures::StreamExt;
use serde::{Deserialize, Serialize};

use crate::{
    format::{
        attributes::AttributesTable, format_constants, manifest::Manifest,
        snapshot::Snapshot, AttributesId, ByteRange, ChunkId, FileTypeTag, ManifestId,
        SnapshotId,
    },
    zarr::ObjectId,
    Storage, StorageError,
};

use super::StorageResult;

#[derive(Debug)]
pub struct S3Storage {
    client: Arc<Client>,
    prefix: String,
    bucket: String,
}

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

pub async fn mk_client(config: Option<&S3Config>) -> Client {
    let region = config
        .and_then(|c| c.region.as_ref())
        .map(|r| RegionProviderChain::first_try(Some(Region::new(r.clone()))))
        .unwrap_or_else(RegionProviderChain::default_provider);

    let endpoint = config.and_then(|c| c.endpoint.clone());
    let allow_http = config.and_then(|c| c.allow_http).unwrap_or(false);
    let credentials = config.and_then(|c| c.credentials.clone());
    #[allow(clippy::unwrap_used)]
    let app_name = AppName::new("icechunk").unwrap();
    let mut aws_config = aws_config::defaults(BehaviorVersion::v2024_03_28())
        .region(region)
        .app_name(app_name);

    if let Some(endpoint) = endpoint {
        aws_config = aws_config.endpoint_url(endpoint)
    }

    if let Some(credentials) = credentials {
        aws_config = aws_config.credentials_provider(Credentials::new(
            credentials.access_key_id,
            credentials.secret_access_key,
            credentials.session_token,
            None,
            "user",
        ));
    }

    let mut s3_builder = Builder::from(&aws_config.load().await);

    if allow_http {
        s3_builder = s3_builder.force_path_style(true);
    }

    let config = s3_builder.build();

    Client::from_conf(config)
}

const SNAPSHOT_PREFIX: &str = "snapshots/";
const MANIFEST_PREFIX: &str = "manifests/";
// const ATTRIBUTES_PREFIX: &str = "attributes/";
const CHUNK_PREFIX: &str = "chunks/";
const REF_PREFIX: &str = "refs";

impl S3Storage {
    pub async fn new_s3_store(
        bucket_name: impl Into<String>,
        prefix: impl Into<String>,
        config: Option<&S3Config>,
    ) -> Result<S3Storage, StorageError> {
        let client = Arc::new(mk_client(config).await);
        Ok(S3Storage { client, prefix: prefix.into(), bucket: bucket_name.into() })
    }

    fn get_path<const SIZE: usize, T: FileTypeTag>(
        &self,
        file_prefix: &str,
        id: &ObjectId<SIZE, T>,
    ) -> StorageResult<String> {
        // we serialize the url using crockford
        let path = PathBuf::from_iter([
            self.prefix.as_str(),
            file_prefix,
            id.to_string().as_str(),
        ]);
        path.into_os_string().into_string().map_err(StorageError::BadPrefix)
    }

    fn get_snapshot_path(&self, id: &SnapshotId) -> StorageResult<String> {
        self.get_path(SNAPSHOT_PREFIX, id)
    }

    fn get_manifest_path(&self, id: &ManifestId) -> StorageResult<String> {
        self.get_path(MANIFEST_PREFIX, id)
    }

    fn get_chunk_path(&self, id: &ChunkId) -> StorageResult<String> {
        self.get_path(CHUNK_PREFIX, id)
    }

    fn ref_key(&self, ref_key: &str) -> StorageResult<String> {
        let path = PathBuf::from_iter([self.prefix.as_str(), REF_PREFIX, ref_key]);
        path.into_os_string().into_string().map_err(StorageError::BadPrefix)
    }

    async fn get_object(&self, key: &str) -> StorageResult<Bytes> {
        Ok(self
            .client
            .get_object()
            .bucket(self.bucket.clone())
            .key(key)
            .send()
            .await?
            .body
            .collect()
            .await?
            .into_bytes())
    }

    async fn get_object_range(
        &self,
        key: &str,
        range: &ByteRange,
    ) -> StorageResult<Bytes> {
        let mut b = self.client.get_object().bucket(self.bucket.clone()).key(key);

        if let Some(header) = range_to_header(range) {
            b = b.range(header)
        };

        Ok(b.send().await?.body.collect().await?.into_bytes())
    }

    async fn put_object<
        I: IntoIterator<Item = (impl Into<String>, impl Into<String>)>,
    >(
        &self,
        key: &str,
        content_type: Option<impl Into<String>>,
        metadata: I,
        bytes: impl Into<ByteStream>,
    ) -> StorageResult<()> {
        let mut b = self.client.put_object().bucket(self.bucket.clone()).key(key);

        if let Some(ct) = content_type {
            b = b.content_type(ct)
        };

        for (k, v) in metadata {
            b = b.metadata(k, v);
        }

        b.body(bytes.into()).send().await?;
        Ok(())
    }
}

pub fn range_to_header(range: &ByteRange) -> Option<String> {
    match range {
        ByteRange(Bound::Unbounded, Bound::Unbounded) => None,
        ByteRange(Bound::Included(start), Bound::Excluded(end)) => {
            Some(format!("bytes={}-{}", start, end - 1))
        }
        ByteRange(Bound::Included(start), Bound::Unbounded) => {
            Some(format!("bytes={}-", start))
        }
        ByteRange(Bound::Included(start), Bound::Included(end)) => {
            Some(format!("bytes={}-{}", start, end))
        }
        ByteRange(Bound::Excluded(start), Bound::Excluded(end)) => {
            Some(format!("bytes={}-{}", start + 1, end - 1))
        }
        ByteRange(Bound::Excluded(start), Bound::Unbounded) => {
            Some(format!("bytes={}-", start + 1))
        }
        ByteRange(Bound::Excluded(start), Bound::Included(end)) => {
            Some(format!("bytes={}-{}", start + 1, end))
        }
        ByteRange(Bound::Unbounded, Bound::Excluded(end)) => {
            Some(format!("bytes=0-{}", end - 1))
        }
        ByteRange(Bound::Unbounded, Bound::Included(end)) => {
            Some(format!("bytes=0-{}", end))
        }
    }
}

#[async_trait]
impl Storage for S3Storage {
    async fn fetch_snapshot(&self, id: &SnapshotId) -> StorageResult<Arc<Snapshot>> {
        let key = self.get_snapshot_path(id)?;
        let bytes = self.get_object(key.as_str()).await?;
        // TODO: optimize using from_read
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
        let metadata = [(
            format_constants::LATEST_ICECHUNK_SNAPSHOT_VERSION_METADATA_KEY,
            snapshot.icechunk_snapshot_format_version.to_string(),
        )];
        self.put_object(
            key.as_str(),
            Some(format_constants::LATEST_ICECHUNK_SNAPSHOT_CONTENT_TYPE),
            metadata,
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
        let metadata = [(
            format_constants::LATEST_ICECHUNK_MANIFEST_VERSION_METADATA_KEY,
            manifest.icechunk_manifest_format_version.to_string(),
        )];
        self.put_object(
            key.as_str(),
            Some(format_constants::LATEST_ICECHUNK_MANIFEST_CONTENT_TYPE),
            metadata,
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
        let metadata: [(String, String); 0] = [];
        self.put_object(key.as_str(), None::<String>, metadata, bytes).await
    }

    async fn get_ref(&self, ref_key: &str) -> StorageResult<Bytes> {
        let key = self.ref_key(ref_key)?;
        let res = self
            .client
            .get_object()
            .bucket(self.bucket.clone())
            .key(key.clone())
            .send()
            .await;

        match res {
            Ok(res) => Ok(res.body.collect().await?.into_bytes()),
            Err(err)
                if err
                    .as_service_error()
                    .map(|e| e.is_no_such_key())
                    .unwrap_or(false) =>
            {
                Err(StorageError::RefNotFound(key.to_string()))
            }
            Err(err) => Err(err.into()),
        }
    }

    async fn ref_names(&self) -> StorageResult<Vec<String>> {
        let prefix = self.ref_key("")?;
        let mut paginator = self
            .client
            .list_objects_v2()
            .bucket(self.bucket.clone())
            .prefix(prefix.clone())
            .delimiter("/")
            .into_paginator()
            .send();

        let mut res = Vec::new();

        while let Some(page) = paginator.try_next().await? {
            for common_prefix in page.common_prefixes() {
                if let Some(key) = common_prefix
                    .prefix()
                    .as_ref()
                    .and_then(|key| key.strip_prefix(prefix.as_str()))
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
    ) -> StorageResult<futures::stream::BoxStream<StorageResult<String>>> {
        let prefix = self.ref_key(ref_name)?;
        let mut paginator = self
            .client
            .list_objects_v2()
            .bucket(self.bucket.clone())
            .prefix(prefix.clone())
            .into_paginator()
            .send();

        let prefix = prefix + "/";
        let stream = try_stream! {
            while let Some(page) = paginator.try_next().await? {
                for object in page.contents() {
                    if let Some(key) = object.key.as_ref().and_then(|key| key.strip_prefix(prefix.as_str())) {
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
        let mut builder =
            self.client.put_object().bucket(self.bucket.clone()).key(key.clone());

        if !overwrite_refs {
            builder = builder.if_none_match("*")
        }

        let res = builder.body(bytes.into()).send().await;

        match res {
            Ok(_) => Ok(()),
            Err(err) => {
                let code = err.as_service_error().and_then(|e| e.code()).unwrap_or("");
                if code.contains("PreconditionFailed")
                    || code.contains("ConditionalRequestConflict")
                {
                    Err(StorageError::RefAlreadyExists(key))
                } else {
                    Err(err.into())
                }
            }
        }
    }
}
