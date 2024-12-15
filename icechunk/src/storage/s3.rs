use std::{
    future::ready,
    ops::Range,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use async_stream::try_stream;
use async_trait::async_trait;
use aws_config::{
    meta::region::RegionProviderChain, retry::ProvideErrorKind, AppName, BehaviorVersion,
};
use aws_credential_types::Credentials;
use aws_sdk_s3::{
    config::{Builder, Region},
    error::SdkError,
    operation::put_object::PutObjectError,
    primitives::ByteStream,
    types::{Delete, Object, ObjectIdentifier},
    Client,
};
use aws_smithy_types_convert::{date_time::DateTimeExt, stream::PaginationStreamExt};
use bytes::Bytes;
use futures::{
    stream::{self, BoxStream},
    StreamExt, TryStreamExt,
};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::{
    format::{
        attributes::AttributesTable, format_constants, manifest::Manifest,
        snapshot::Snapshot, transaction_log::TransactionLog, AttributesId, ByteRange,
        ChunkId, FileTypeTag, ManifestId, ObjectId, SnapshotId,
    },
    private, Storage, StorageError,
};

use super::{
    ETag, ListInfo, StorageResult, CHUNK_PREFIX, CONFIG_PATH, MANIFEST_PREFIX,
    REF_PREFIX, SNAPSHOT_PREFIX, TRANSACTION_PREFIX,
};

#[derive(Debug, Serialize, Deserialize)]
#[serde(transparent)]
pub struct S3Storage {
    config: S3Config,
    #[serde(skip)]
    client: RwLock<Option<Arc<Client>>>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct StaticS3Credentials {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, Default)]
#[serde(tag = "type")]
pub enum S3Credentials {
    #[default]
    #[serde(rename = "from_env")]
    FromEnv,
    #[serde(rename = "anonymous")]
    Anonymous,
    #[serde(rename = "static")]
    Static(StaticS3Credentials),
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct S3ClientOptions {
    pub region: Option<String>,
    pub endpoint: Option<String>,
    pub credentials: S3Credentials,
    pub allow_http: bool,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct S3Config {
    pub bucket: String,
    pub prefix: String,
    pub options: Option<S3ClientOptions>,
}

pub async fn mk_client(config: Option<&S3ClientOptions>) -> Client {
    let region = config
        .as_ref()
        .and_then(|c| c.region.as_ref())
        .map(|r| RegionProviderChain::first_try(Some(Region::new(r.clone()))))
        .unwrap_or_else(RegionProviderChain::default_provider);

    let endpoint = config.as_ref().and_then(|c| c.endpoint.clone());
    let allow_http = config.as_ref().map(|c| c.allow_http).unwrap_or(false);
    let credentials =
        config.as_ref().map(|c| c.credentials.clone()).unwrap_or(S3Credentials::FromEnv);
    #[allow(clippy::unwrap_used)]
    let app_name = AppName::new("icechunk").unwrap();
    let mut aws_config = aws_config::defaults(BehaviorVersion::v2024_03_28())
        .region(region)
        .app_name(app_name);

    if let Some(endpoint) = endpoint {
        aws_config = aws_config.endpoint_url(endpoint)
    }

    match credentials {
        S3Credentials::FromEnv => {}
        S3Credentials::Anonymous => aws_config = aws_config.no_credentials(),
        S3Credentials::Static(credentials) => {
            aws_config = aws_config.credentials_provider(Credentials::new(
                credentials.access_key_id,
                credentials.secret_access_key,
                credentials.session_token,
                None,
                "user",
            ));
        }
    }

    let mut s3_builder = Builder::from(&aws_config.load().await);

    if allow_http {
        s3_builder = s3_builder.force_path_style(true);
    }

    let config = s3_builder.build();

    Client::from_conf(config)
}

impl S3Storage {
    pub async fn new_s3_store(config: &S3Config) -> Result<S3Storage, StorageError> {
        let client =
            RwLock::new(Some(Arc::new(mk_client(config.options.as_ref()).await)));
        Ok(S3Storage { client, config: config.clone() })
    }

    async fn get_client(&self) -> Arc<Client> {
        let guard = self.client.read().await;
        if let Some(client) = guard.as_ref() {
            Arc::clone(client)
        } else {
            let client = Arc::new(mk_client(self.config.options.as_ref()).await);
            *self.client.write().await = Some(Arc::clone(&client));
            client
        }
    }

    fn get_path_str(&self, file_prefix: &str, id: &str) -> StorageResult<String> {
        let path = PathBuf::from_iter([self.config.prefix.as_str(), file_prefix, id]);
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

    fn get_config_path(&self) -> StorageResult<String> {
        self.get_path_str("", CONFIG_PATH)
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

    fn get_transaction_path(&self, id: &SnapshotId) -> StorageResult<String> {
        self.get_path(TRANSACTION_PREFIX, id)
    }

    fn ref_key(&self, ref_key: &str) -> StorageResult<String> {
        let path = PathBuf::from_iter([self.config.prefix.as_str(), REF_PREFIX, ref_key]);
        path.into_os_string().into_string().map_err(StorageError::BadPrefix)
    }

    async fn get_object(&self, key: &str) -> StorageResult<Bytes> {
        Ok(self
            .get_client()
            .await
            .get_object()
            .bucket(self.config.bucket.clone())
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
        let mut b = self
            .get_client()
            .await
            .get_object()
            .bucket(self.config.bucket.clone())
            .key(key);

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
        let mut b = self
            .get_client()
            .await
            .put_object()
            .bucket(self.config.bucket.clone())
            .key(key);

        if let Some(ct) = content_type {
            b = b.content_type(ct)
        };

        for (k, v) in metadata {
            b = b.metadata(k, v);
        }

        b.body(bytes.into()).send().await?;
        Ok(())
    }

    async fn delete_batch(
        &self,
        prefix: &str,
        batch: Vec<String>,
    ) -> StorageResult<usize> {
        let keys = batch
            .iter()
            // FIXME: flag errors instead of skipping them
            .filter_map(|id| {
                let key = self.get_path_str(prefix, id).ok()?;
                let ident = ObjectIdentifier::builder().key(key).build().ok()?;
                Some(ident)
            })
            .collect();

        let delete = Delete::builder()
            .set_objects(Some(keys))
            .build()
            .map_err(|e| StorageError::Other(e.to_string()))?;

        let res = self
            .get_client()
            .await
            .delete_objects()
            .bucket(self.config.bucket.clone())
            .delete(delete)
            .send()
            .await?;

        Ok(res.deleted().len())
    }
}

pub fn range_to_header(range: &ByteRange) -> Option<String> {
    match range {
        ByteRange::Bounded(Range { start, end }) => {
            Some(format!("bytes={}-{}", start, end - 1))
        }
        ByteRange::From(offset) if *offset == 0 => None,
        ByteRange::From(offset) => Some(format!("bytes={}-", offset)),
        ByteRange::Last(n) => Some(format!("bytes={}-", n)),
    }
}

impl private::Sealed for S3Storage {}

#[async_trait]
#[typetag::serde]
impl Storage for S3Storage {
    async fn fetch_config(&self) -> StorageResult<Option<(Bytes, ETag)>> {
        let key = self.get_config_path()?;
        let res = self
            .get_client()
            .await
            .get_object()
            .bucket(self.config.bucket.clone())
            .key(key)
            .send()
            .await;

        match res {
            Ok(output) => match output.e_tag {
                Some(etag) => Ok(Some((output.body.collect().await?.into_bytes(), etag))),
                None => Err(StorageError::Other("No ETag found for config".to_string())),
            },
            Err(sdk_err) => match sdk_err.as_service_error() {
                Some(e) if e.is_no_such_key() => Ok(None),
                _ => Err(sdk_err.into()),
            },
        }
    }

    async fn update_config(
        &self,
        config: Bytes,
        etag: Option<&str>,
    ) -> StorageResult<ETag> {
        let key = self.get_config_path()?;
        let mut req = self
            .get_client()
            .await
            .put_object()
            .bucket(self.config.bucket.clone())
            .key(key)
            .content_type("application/yaml")
            .body(config.into());

        if let Some(etag) = etag {
            req = req.if_match(etag)
        } else {
            req = req.if_none_match("*")
        }

        let res = req.send().await;

        match res {
            Ok(out) => {
                let etag = out.e_tag().ok_or(StorageError::Other(
                    "Config object should have an etag".to_string(),
                ))?;
                Ok(etag.to_string())
            }
            // minio returns this
            Err(SdkError::ServiceError(err)) => {
                if err.err().meta().code() == Some("PreconditionFailed") {
                    Err(StorageError::ConfigUpdateConflict)
                } else {
                    Err(StorageError::from(SdkError::<PutObjectError>::ServiceError(err)))
                }
            }
            // S3 API documents this
            Err(SdkError::ResponseError(err)) => {
                let status = err.raw().status().as_u16();
                // see https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html#API_PutObject_RequestSyntax
                if status == 409 || status == 412 {
                    Err(StorageError::ConfigUpdateConflict)
                } else {
                    Err(StorageError::from(SdkError::<PutObjectError>::ResponseError(
                        err,
                    )))
                }
            }
            Err(err) => Err(err.into()),
        }
    }

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

    async fn fetch_transaction_log(
        &self,
        id: &SnapshotId,
    ) -> StorageResult<Arc<TransactionLog>> {
        let key = self.get_transaction_path(id)?;
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

    async fn write_transaction_log(
        &self,
        id: SnapshotId,
        log: Arc<TransactionLog>,
    ) -> StorageResult<()> {
        let key = self.get_transaction_path(&id)?;
        let bytes = rmp_serde::to_vec(log.as_ref())?;
        let metadata = [(
            format_constants::LATEST_ICECHUNK_TRANSACTION_LOG_VERSION_METADATA_KEY,
            log.icechunk_transaction_log_format_version.to_string(),
        )];
        self.put_object(
            key.as_str(),
            Some(format_constants::LATEST_ICECHUNK_TRANSACTION_LOG_CONTENT_TYPE),
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
            .get_client()
            .await
            .get_object()
            .bucket(self.config.bucket.clone())
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
            .get_client()
            .await
            .list_objects_v2()
            .bucket(self.config.bucket.clone())
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
    ) -> StorageResult<BoxStream<StorageResult<String>>> {
        let prefix = self.ref_key(ref_name)?;
        let mut paginator = self
            .get_client()
            .await
            .list_objects_v2()
            .bucket(self.config.bucket.clone())
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
        let mut builder = self
            .get_client()
            .await
            .put_object()
            .bucket(self.config.bucket.clone())
            .key(key.clone());

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

    async fn list_objects<'a>(
        &'a self,
        prefix: &str,
    ) -> StorageResult<BoxStream<'a, StorageResult<ListInfo<String>>>> {
        let prefix = PathBuf::from_iter([self.config.prefix.as_str(), prefix])
            .into_os_string()
            .into_string()
            .map_err(StorageError::BadPrefix)?;
        let stream = self
            .get_client()
            .await
            .list_objects_v2()
            .bucket(self.config.bucket.clone())
            .prefix(prefix)
            .into_paginator()
            .send()
            .into_stream_03x()
            .try_filter_map(|page| {
                let contents = page.contents.map(|cont| stream::iter(cont).map(Ok));
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

fn object_to_list_info(object: &Object) -> Option<ListInfo<String>> {
    let key = object.key()?;
    let last_modified = object.last_modified()?;
    let created_at = last_modified.to_chrono_utc().ok()?;
    let id = Path::new(key).file_name().and_then(|s| s.to_str())?.to_string();
    Some(ListInfo { id, created_at })
}

#[cfg(test)]
mod tests {
    use super::{S3ClientOptions, S3Config, S3Credentials, S3Storage};

    #[tokio::test]
    async fn test_serialize_s3_storage() {
        let config = S3Config {
            bucket: "bucket".to_string(),
            prefix: "prefix".to_string(),
            options: Some(S3ClientOptions {
                region: Some("us-west-2".to_string()),
                endpoint: Some("http://localhost:9000".to_string()),
                credentials: S3Credentials::Static(super::StaticS3Credentials {
                    access_key_id: "access_key_id".to_string(),
                    secret_access_key: "secret_access_key".to_string(),
                    session_token: Some("session_token".to_string()),
                }),
                allow_http: true,
            }),
        };
        let storage = S3Storage::new_s3_store(&config).await.unwrap();

        let serialized = serde_json::to_string(&storage).unwrap();

        assert_eq!(
            serialized,
            r#"{"bucket":"bucket","prefix":"prefix","options":{"region":"us-west-2","endpoint":"http://localhost:9000","credentials":{"type":"static","access_key_id":"access_key_id","secret_access_key":"secret_access_key","session_token":"session_token"},"allow_http":true}}"#
        );

        let deserialized: S3Storage = serde_json::from_str(&serialized).unwrap();
        assert_eq!(storage.config, deserialized.config);
    }
}
