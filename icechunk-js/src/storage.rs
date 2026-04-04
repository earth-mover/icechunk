use std::fmt;
use std::ops::Range;
use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use futures::{Stream, StreamExt};
use icechunk::storage::{
    DeleteObjectsResult, ETag, Generation, GetModifiedResult, ListInfo, Settings,
    Storage, StorageError, StorageInfo, StorageResult, VersionInfo,
    VersionedUpdateResult,
};
use napi::bindgen_prelude::{Buffer, Promise};
use napi::threadsafe_function::ThreadsafeFunction;
use napi_derive::napi;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::errors::IntoNapiResult;

#[cfg(not(target_family = "wasm"))]
use std::collections::HashMap;

#[napi(js_name = "Storage")]
pub struct JsStorage(pub(crate) Arc<dyn Storage + Send + Sync>);

// --- Custom JS-provided storage backend ---

fn other_storage_error(s: impl Into<String>) -> StorageError {
    icechunk_storage::other_error(s)
}

/// Map a JS callback error to the appropriate StorageError.
/// If the error message contains "ObjectNotFound", return ObjectNotFound.
fn js_storage_error(context: &str, e: impl std::fmt::Display) -> StorageError {
    let msg = e.to_string();
    if msg.contains("ObjectNotFound") {
        StorageError::capture(icechunk_storage::StorageErrorKind::ObjectNotFound)
    } else {
        other_storage_error(format!("{context}: {msg}"))
    }
}

/// Version information returned from JS storage callbacks
#[napi(object, js_name = "StorageVersionInfo")]
#[derive(Clone, Debug)]
pub struct JsStorageVersionInfo {
    pub etag: Option<String>,
    pub generation: Option<String>,
}

impl From<JsStorageVersionInfo> for VersionInfo {
    fn from(v: JsStorageVersionInfo) -> Self {
        VersionInfo { etag: v.etag.map(ETag), generation: v.generation.map(Generation) }
    }
}

impl From<&VersionInfo> for JsStorageVersionInfo {
    fn from(v: &VersionInfo) -> Self {
        JsStorageVersionInfo {
            etag: v.etag.as_ref().map(|e| e.0.clone()),
            generation: v.generation.as_ref().map(|g| g.0.clone()),
        }
    }
}

/// Result of a get_object_range call from JS
#[napi(object, js_name = "StorageGetObjectResponse")]
pub struct JsStorageGetObjectResponse {
    pub data: Buffer,
    pub version: JsStorageVersionInfo,
}

/// Result of a put_object or copy_object call from JS
#[napi(object, js_name = "StorageVersionedUpdateResult")]
#[derive(Clone, Debug)]
pub struct JsStorageVersionedUpdateResult {
    /// "updated" or "not_on_latest_version"
    pub kind: String,
    pub new_version: Option<JsStorageVersionInfo>,
}

impl JsStorageVersionedUpdateResult {
    fn into_rust(self) -> StorageResult<VersionedUpdateResult> {
        match self.kind.as_str() {
            "updated" => {
                let new_version = self
                    .new_version
                    .ok_or_else(|| {
                        other_storage_error(
                            "VersionedUpdateResult 'updated' missing new_version",
                        )
                    })?
                    .into();
                Ok(VersionedUpdateResult::Updated { new_version })
            }
            "not_on_latest_version" => Ok(VersionedUpdateResult::NotOnLatestVersion),
            other => Err(other_storage_error(format!(
                "Unknown VersionedUpdateResult kind: {other}"
            ))),
        }
    }
}

/// Result of a delete_batch call from JS
#[napi(object, js_name = "StorageDeleteObjectsResult")]
#[derive(Clone, Debug)]
pub struct JsStorageDeleteObjectsResult {
    pub deleted_objects: f64,
    pub deleted_bytes: f64,
}

/// Entry in a list_objects result from JS
#[napi(object, js_name = "StorageListInfo")]
#[derive(Clone, Debug)]
pub struct JsStorageListInfo {
    pub id: String,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub size_bytes: f64,
}

/// Arguments for get_object_range callback
#[napi(object, js_name = "StorageGetObjectRangeArgs")]
#[derive(Clone, Debug)]
pub struct JsStorageGetObjectRangeArgs {
    pub path: String,
    pub range_start: Option<f64>,
    pub range_end: Option<f64>,
}

/// A key-value pair for object metadata
#[napi(object, js_name = "StorageKeyValue")]
#[derive(Clone, Debug)]
pub struct JsStorageKeyValue {
    pub key: String,
    pub value: String,
}

/// Arguments for put_object callback
#[napi(object, js_name = "StoragePutObjectArgs")]
pub struct JsStoragePutObjectArgs {
    pub path: String,
    pub data: Buffer,
    pub content_type: Option<String>,
    pub metadata: Vec<JsStorageKeyValue>,
    pub previous_version: Option<JsStorageVersionInfo>,
}

/// Arguments for copy_object callback
#[napi(object, js_name = "StorageCopyObjectArgs")]
#[derive(Clone, Debug)]
pub struct JsStorageCopyObjectArgs {
    pub from: String,
    pub to: String,
    pub content_type: Option<String>,
    pub version: JsStorageVersionInfo,
}

/// An item in a delete_batch call
#[napi(object, js_name = "StorageDeleteItem")]
#[derive(Clone, Debug)]
pub struct JsStorageDeleteItem {
    pub id: String,
    pub size: f64,
}

/// Arguments for delete_batch callback
#[napi(object, js_name = "StorageDeleteBatchArgs")]
#[derive(Clone, Debug)]
pub struct JsStorageDeleteBatchArgs {
    pub prefix: String,
    pub batch: Vec<JsStorageDeleteItem>,
}

/// Arguments for get_object_conditional callback
#[napi(object, js_name = "StorageGetObjectConditionalArgs")]
#[derive(Clone, Debug)]
pub struct JsStorageGetObjectConditionalArgs {
    pub path: String,
    pub previous_version: Option<JsStorageVersionInfo>,
}

/// Result of a get_object_conditional call from JS
#[napi(object, js_name = "StorageGetModifiedResult")]
pub struct JsStorageGetModifiedResult {
    /// "modified" or "on_latest_version"
    pub kind: String,
    pub data: Option<Buffer>,
    pub new_version: Option<JsStorageVersionInfo>,
}

/// Storage backend that delegates all operations to JS callbacks.
///
/// This enables JS users to provide custom storage implementations using
/// JS libraries (fetch, @aws-sdk/client-s3, etc.), which is especially
/// useful for WASM builds where native Rust networking is unavailable.
pub(crate) struct JsCallbackStorage {
    can_write_fn: ThreadsafeFunction<(), Promise<bool>>,
    get_object_range_fn: ThreadsafeFunction<
        JsStorageGetObjectRangeArgs,
        Promise<JsStorageGetObjectResponse>,
    >,
    put_object_fn: ThreadsafeFunction<
        JsStoragePutObjectArgs,
        Promise<JsStorageVersionedUpdateResult>,
    >,
    copy_object_fn: ThreadsafeFunction<
        JsStorageCopyObjectArgs,
        Promise<JsStorageVersionedUpdateResult>,
    >,
    list_objects_fn: ThreadsafeFunction<String, Promise<Vec<JsStorageListInfo>>>,
    delete_batch_fn: ThreadsafeFunction<
        JsStorageDeleteBatchArgs,
        Promise<JsStorageDeleteObjectsResult>,
    >,
    get_object_last_modified_fn:
        ThreadsafeFunction<String, Promise<chrono::DateTime<chrono::Utc>>>,
    get_object_conditional_fn: ThreadsafeFunction<
        JsStorageGetObjectConditionalArgs,
        Promise<JsStorageGetModifiedResult>,
    >,
}

impl fmt::Debug for JsCallbackStorage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JsCallbackStorage").finish()
    }
}

impl fmt::Display for JsCallbackStorage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "JsCallbackStorage")
    }
}

impl Serialize for JsCallbackStorage {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        #[derive(Serialize)]
        struct Helper {}
        Helper {}.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for JsCallbackStorage {
    fn deserialize<D: Deserializer<'de>>(_deserializer: D) -> Result<Self, D::Error> {
        Err(serde::de::Error::custom(
            "JsCallbackStorage cannot be deserialized: JS callbacks are not serializable",
        ))
    }
}

impl icechunk_storage::sealed::Sealed for JsCallbackStorage {}

#[async_trait]
#[typetag::serde(name = "js_callback_storage")]
impl Storage for JsCallbackStorage {
    fn storage_info(&self) -> StorageInfo {
        StorageInfo { backend_type: "JsCallback", fields: vec![] }
    }

    async fn can_write(&self) -> StorageResult<bool> {
        self.can_write_fn
            .call_async(Ok(()))
            .await
            .map_err(|e| js_storage_error("JS canWrite call failed", e))?
            .await
            .map_err(|e| js_storage_error("JS canWrite failed", e))
    }

    async fn get_object_range(
        &self,
        _settings: &Settings,
        path: &str,
        range: Option<&Range<u64>>,
    ) -> StorageResult<(
        Pin<Box<dyn Stream<Item = Result<Bytes, StorageError>> + Send>>,
        VersionInfo,
    )> {
        let args = JsStorageGetObjectRangeArgs {
            path: path.to_string(),
            range_start: range.map(|r| r.start as f64),
            range_end: range.map(|r| r.end as f64),
        };
        let response = self
            .get_object_range_fn
            .call_async(Ok(args))
            .await
            .map_err(|e| js_storage_error("JS getObjectRange call failed", e))?
            .await
            .map_err(|e| js_storage_error("JS getObjectRange failed", e))?;

        let bytes = Bytes::from(response.data.to_vec());
        let version = response.version.into();
        let stream = futures::stream::once(async move { Ok(bytes) });
        Ok((Box::pin(stream), version))
    }

    async fn put_object(
        &self,
        _settings: &Settings,
        path: &str,
        bytes: Bytes,
        content_type: Option<&str>,
        metadata: Vec<(String, String)>,
        previous_version: Option<&VersionInfo>,
    ) -> StorageResult<VersionedUpdateResult> {
        let args = JsStoragePutObjectArgs {
            path: path.to_string(),
            data: bytes.to_vec().into(),
            content_type: content_type.map(|s| s.to_string()),
            metadata: metadata
                .into_iter()
                .map(|(k, v)| JsStorageKeyValue { key: k, value: v })
                .collect(),
            previous_version: previous_version.map(|v| v.into()),
        };
        let result = self
            .put_object_fn
            .call_async(Ok(args))
            .await
            .map_err(|e| js_storage_error("JS putObject call failed", e))?
            .await
            .map_err(|e| js_storage_error("JS putObject failed", e))?;
        result.into_rust()
    }

    async fn copy_object(
        &self,
        _settings: &Settings,
        from: &str,
        to: &str,
        content_type: Option<&str>,
        version: &VersionInfo,
    ) -> StorageResult<VersionedUpdateResult> {
        let args = JsStorageCopyObjectArgs {
            from: from.to_string(),
            to: to.to_string(),
            content_type: content_type.map(|s| s.to_string()),
            version: version.into(),
        };
        let result = self
            .copy_object_fn
            .call_async(Ok(args))
            .await
            .map_err(|e| js_storage_error("JS copyObject call failed", e))?
            .await
            .map_err(|e| js_storage_error("JS copyObject failed", e))?;
        result.into_rust()
    }

    async fn list_objects<'a>(
        &'a self,
        _settings: &Settings,
        prefix: &str,
    ) -> StorageResult<BoxStream<'a, StorageResult<ListInfo<String>>>> {
        let items = self
            .list_objects_fn
            .call_async(Ok(prefix.to_string()))
            .await
            .map_err(|e| js_storage_error("JS listObjects call failed", e))?
            .await
            .map_err(|e| js_storage_error("JS listObjects failed", e))?;

        let rust_items: Vec<StorageResult<ListInfo<String>>> = items
            .into_iter()
            .map(|item| {
                Ok(ListInfo {
                    id: item.id,
                    created_at: item.created_at,
                    size_bytes: item.size_bytes as u64,
                })
            })
            .collect();

        Ok(futures::stream::iter(rust_items).boxed())
    }

    async fn delete_batch(
        &self,
        _settings: &Settings,
        prefix: &str,
        batch: Vec<(String, u64)>,
    ) -> StorageResult<DeleteObjectsResult> {
        let args = JsStorageDeleteBatchArgs {
            prefix: prefix.to_string(),
            batch: batch
                .into_iter()
                .map(|(id, size)| JsStorageDeleteItem { id, size: size as f64 })
                .collect(),
        };
        let result = self
            .delete_batch_fn
            .call_async(Ok(args))
            .await
            .map_err(|e| js_storage_error("JS deleteBatch call failed", e))?
            .await
            .map_err(|e| js_storage_error("JS deleteBatch failed", e))?;

        Ok(DeleteObjectsResult {
            deleted_objects: result.deleted_objects as u64,
            deleted_bytes: result.deleted_bytes as u64,
        })
    }

    async fn get_object_last_modified(
        &self,
        path: &str,
        _settings: &Settings,
    ) -> StorageResult<chrono::DateTime<chrono::Utc>> {
        self.get_object_last_modified_fn
            .call_async(Ok(path.to_string()))
            .await
            .map_err(|e| js_storage_error("JS getObjectLastModified call failed", e))?
            .await
            .map_err(|e| js_storage_error("JS getObjectLastModified failed", e))
    }

    async fn get_object_conditional(
        &self,
        _settings: &Settings,
        path: &str,
        previous_version: Option<&VersionInfo>,
    ) -> StorageResult<GetModifiedResult> {
        let args = JsStorageGetObjectConditionalArgs {
            path: path.to_string(),
            previous_version: previous_version.map(|v| v.into()),
        };
        let result = self
            .get_object_conditional_fn
            .call_async(Ok(args))
            .await
            .map_err(|e| js_storage_error("JS getObjectConditional call failed", e))?
            .await
            .map_err(|e| js_storage_error("JS getObjectConditional failed", e))?;

        match result.kind.as_str() {
            "modified" => {
                let data = result.data.ok_or_else(|| {
                    other_storage_error("GetModifiedResult 'modified' missing data field")
                })?;
                let new_version: VersionInfo = result
                    .new_version
                    .ok_or_else(|| {
                        other_storage_error(
                            "GetModifiedResult 'modified' missing new_version field",
                        )
                    })?
                    .into();
                let bytes = Bytes::from(data.to_vec());
                let cursor = std::io::Cursor::new(bytes);
                Ok(GetModifiedResult::Modified { data: Box::pin(cursor), new_version })
            }
            "on_latest_version" => Ok(GetModifiedResult::OnLatestVersion),
            other => Err(other_storage_error(format!(
                "Unknown GetModifiedResult kind: {other}"
            ))),
        }
    }
}

#[cfg(not(target_family = "wasm"))]
mod native {
    use super::*;

    /// S3 static credentials
    #[napi(object, js_name = "S3StaticCredentials")]
    #[derive(Clone, Debug)]
    pub struct JsS3StaticCredentials {
        pub access_key_id: String,
        pub secret_access_key: String,
        pub session_token: Option<String>,
        pub expires_after: Option<chrono::DateTime<chrono::Utc>>,
    }

    impl From<JsS3StaticCredentials> for icechunk::config::S3StaticCredentials {
        fn from(creds: JsS3StaticCredentials) -> Self {
            icechunk::config::S3StaticCredentials {
                access_key_id: creds.access_key_id,
                secret_access_key: creds.secret_access_key,
                session_token: creds.session_token,
                expires_after: creds.expires_after,
            }
        }
    }

    /// S3 credentials
    #[napi(js_name = "S3Credentials")]
    pub enum JsS3Credentials {
        FromEnv,
        Anonymous,
        Static(JsS3StaticCredentials),
    }

    impl From<JsS3Credentials> for icechunk::config::S3Credentials {
        fn from(creds: JsS3Credentials) -> Self {
            match creds {
                JsS3Credentials::FromEnv => icechunk::config::S3Credentials::FromEnv,
                JsS3Credentials::Anonymous => icechunk::config::S3Credentials::Anonymous,
                JsS3Credentials::Static(c) => {
                    icechunk::config::S3Credentials::Static(c.into())
                }
            }
        }
    }

    /// S3 options
    #[napi(object, js_name = "S3Options")]
    #[derive(Clone, Debug)]
    pub struct JsS3Options {
        pub region: Option<String>,
        pub endpoint_url: Option<String>,
        pub allow_http: Option<bool>,
        pub anonymous: Option<bool>,
        pub force_path_style: Option<bool>,
        pub network_stream_timeout_seconds: Option<u32>,
        pub requester_pays: Option<bool>,
    }

    impl JsS3Options {
        #[allow(clippy::too_many_arguments)]
        pub fn new(
            region: Option<String>,
            endpoint_url: Option<String>,
            allow_http: Option<bool>,
            anonymous: Option<bool>,
            force_path_style: Option<bool>,
            network_stream_timeout_seconds: Option<u32>,
            requester_pays: Option<bool>,
        ) -> Self {
            Self {
                region,
                endpoint_url,
                allow_http,
                anonymous,
                force_path_style,
                network_stream_timeout_seconds,
                requester_pays,
            }
        }
    }

    impl From<JsS3Options> for icechunk::config::S3Options {
        fn from(opts: JsS3Options) -> Self {
            icechunk::config::S3Options {
                region: opts.region,
                endpoint_url: opts.endpoint_url,
                allow_http: opts.allow_http.unwrap_or(false),
                anonymous: opts.anonymous.unwrap_or(false),
                force_path_style: opts.force_path_style.unwrap_or(false),
                network_stream_timeout_seconds: opts.network_stream_timeout_seconds,
                requester_pays: opts.requester_pays.unwrap_or(false),
            }
        }
    }

    /// GCS static credentials
    #[napi(js_name = "GcsStaticCredentials")]
    pub enum JsGcsStaticCredentials {
        ServiceAccount(String),
        ServiceAccountKey(String),
        ApplicationCredentials(String),
        BearerToken(String),
    }

    impl From<JsGcsStaticCredentials> for icechunk::config::GcsStaticCredentials {
        fn from(creds: JsGcsStaticCredentials) -> Self {
            use icechunk::config::GcsStaticCredentials;
            match creds {
                JsGcsStaticCredentials::ServiceAccount(path) => {
                    GcsStaticCredentials::ServiceAccount(path.into())
                }
                JsGcsStaticCredentials::ServiceAccountKey(key) => {
                    GcsStaticCredentials::ServiceAccountKey(key)
                }
                JsGcsStaticCredentials::ApplicationCredentials(path) => {
                    GcsStaticCredentials::ApplicationCredentials(path.into())
                }
                JsGcsStaticCredentials::BearerToken(token) => {
                    GcsStaticCredentials::BearerToken(
                        icechunk::config::GcsBearerCredential {
                            bearer: token,
                            expires_after: None,
                        },
                    )
                }
            }
        }
    }

    /// GCS bearer credential with optional expiry
    #[napi(object, js_name = "GcsBearerCredential")]
    #[derive(Clone, Debug)]
    pub struct JsGcsBearerCredential {
        pub bearer: String,
        pub expires_after: Option<chrono::DateTime<chrono::Utc>>,
    }

    impl From<JsGcsBearerCredential> for icechunk::config::GcsBearerCredential {
        fn from(cred: JsGcsBearerCredential) -> Self {
            icechunk::config::GcsBearerCredential {
                bearer: cred.bearer,
                expires_after: cred.expires_after,
            }
        }
    }

    /// GCS credentials
    #[napi(js_name = "GcsCredentials")]
    pub enum JsGcsCredentials {
        Anonymous,
        FromEnv,
        Static(JsGcsStaticCredentials),
    }

    impl From<JsGcsCredentials> for icechunk::config::GcsCredentials {
        fn from(creds: JsGcsCredentials) -> Self {
            use icechunk::config::GcsCredentials;
            match creds {
                JsGcsCredentials::Anonymous => GcsCredentials::Anonymous,
                JsGcsCredentials::FromEnv => GcsCredentials::FromEnv,
                JsGcsCredentials::Static(c) => GcsCredentials::Static(c.into()),
            }
        }
    }

    /// Azure static credentials
    #[napi(js_name = "AzureStaticCredentials")]
    pub enum JsAzureStaticCredentials {
        AccessKey(String),
        SasToken(String),
        BearerToken(String),
    }

    impl From<JsAzureStaticCredentials> for icechunk::config::AzureStaticCredentials {
        fn from(creds: JsAzureStaticCredentials) -> Self {
            use icechunk::config::AzureStaticCredentials;
            match creds {
                JsAzureStaticCredentials::AccessKey(key) => {
                    AzureStaticCredentials::AccessKey(key)
                }
                JsAzureStaticCredentials::SasToken(token) => {
                    AzureStaticCredentials::SASToken(token)
                }
                JsAzureStaticCredentials::BearerToken(token) => {
                    AzureStaticCredentials::BearerToken(token)
                }
            }
        }
    }

    /// Azure credentials
    #[napi(js_name = "AzureCredentials")]
    pub enum JsAzureCredentials {
        FromEnv,
        Static(JsAzureStaticCredentials),
    }

    impl From<JsAzureCredentials> for icechunk::config::AzureCredentials {
        fn from(creds: JsAzureCredentials) -> Self {
            use icechunk::config::AzureCredentials;
            match creds {
                JsAzureCredentials::FromEnv => AzureCredentials::FromEnv,
                JsAzureCredentials::Static(c) => AzureCredentials::Static(c.into()),
            }
        }
    }

    /// Credentials for virtual chunk access
    #[napi(js_name = "Credentials")]
    pub enum JsCredentials {
        S3(JsS3Credentials),
        Gcs(JsGcsCredentials),
        Azure(JsAzureCredentials),
    }

    impl From<JsCredentials> for icechunk::config::Credentials {
        fn from(creds: JsCredentials) -> Self {
            match creds {
                JsCredentials::S3(c) => icechunk::config::Credentials::S3(c.into()),
                JsCredentials::Gcs(c) => icechunk::config::Credentials::Gcs(c.into()),
                JsCredentials::Azure(c) => icechunk::config::Credentials::Azure(c.into()),
            }
        }
    }

    /// Object store configuration for virtual chunk containers
    #[napi(js_name = "ObjectStoreConfig")]
    #[derive(Clone, Debug)]
    pub enum JsObjectStoreConfig {
        InMemory,
        LocalFileSystem(String),
        Http(HashMap<String, String>),
        S3Compatible(JsS3Options),
        S3(JsS3Options),
        Gcs(HashMap<String, String>),
        Azure(HashMap<String, String>),
        Tigris(JsS3Options),
    }

    impl From<JsObjectStoreConfig> for icechunk::ObjectStoreConfig {
        fn from(config: JsObjectStoreConfig) -> Self {
            match config {
                JsObjectStoreConfig::InMemory => icechunk::ObjectStoreConfig::InMemory,
                JsObjectStoreConfig::LocalFileSystem(path) => {
                    icechunk::ObjectStoreConfig::LocalFileSystem(path.into())
                }
                JsObjectStoreConfig::Http(opts) => {
                    icechunk::ObjectStoreConfig::Http(opts)
                }
                JsObjectStoreConfig::S3Compatible(opts) => {
                    icechunk::ObjectStoreConfig::S3Compatible(opts.into())
                }
                JsObjectStoreConfig::S3(opts) => {
                    icechunk::ObjectStoreConfig::S3(opts.into())
                }
                JsObjectStoreConfig::Gcs(opts) => icechunk::ObjectStoreConfig::Gcs(opts),
                JsObjectStoreConfig::Azure(opts) => {
                    icechunk::ObjectStoreConfig::Azure(opts)
                }
                JsObjectStoreConfig::Tigris(opts) => {
                    icechunk::ObjectStoreConfig::Tigris(opts.into())
                }
            }
        }
    }
    // --- Refreshable credentials support ---

    use async_trait::async_trait;
    use chrono::{TimeDelta, Utc};
    use napi::threadsafe_function::ThreadsafeFunction;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use tokio::sync::RwLock;

    /// A credentials fetcher that calls back into JS to get fresh S3 credentials.
    ///
    /// The JS callback is held via `ThreadsafeFunction` which is `Send + Sync`.
    /// This struct is NOT fully serializable — only the cached credential is
    /// persisted. The JS callback cannot survive serialization.
    pub(crate) struct JsS3CredentialsFetcher {
        pub(crate) tsfn: ThreadsafeFunction<(), JsS3StaticCredentials>,
        pub(crate) last_credential: RwLock<Option<icechunk::config::S3StaticCredentials>>,
    }

    impl std::fmt::Debug for JsS3CredentialsFetcher {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("JsS3CredentialsFetcher").finish()
        }
    }

    impl Serialize for JsS3CredentialsFetcher {
        fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
            #[derive(Serialize)]
            struct Helper {
                last_credential: Option<icechunk::config::S3StaticCredentials>,
            }
            let cached = self.last_credential.blocking_read().clone();
            Helper { last_credential: cached }.serialize(serializer)
        }
    }

    impl<'de> Deserialize<'de> for JsS3CredentialsFetcher {
        fn deserialize<D: Deserializer<'de>>(_deserializer: D) -> Result<Self, D::Error> {
            Err(serde::de::Error::custom(
                "JsS3CredentialsFetcher cannot be deserialized: JS callback is not serializable",
            ))
        }
    }

    #[async_trait]
    #[typetag::serde(name = "js_s3_credentials_fetcher")]
    impl icechunk::config::S3CredentialsFetcher for JsS3CredentialsFetcher {
        async fn get(&self) -> Result<icechunk::config::S3StaticCredentials, String> {
            // Check if cached credentials are still valid
            {
                let cached = self.last_credential.read().await;
                if let Some(creds) = cached.as_ref() {
                    match creds.expires_after {
                        None => return Ok(creds.clone()),
                        Some(expiration)
                            if expiration
                                > Utc::now()
                                    + TimeDelta::seconds(rand::random_range(
                                        120..=180,
                                    )) =>
                        {
                            return Ok(creds.clone());
                        }
                        _ => {}
                    }
                }
            }

            // Call the JS function to get fresh credentials
            let js_creds: JsS3StaticCredentials =
                self.tsfn.call_async(Ok(())).await.map_err(|e| {
                    format!("Failed to call JS S3 credentials fetcher: {e}")
                })?;

            let creds: icechunk::config::S3StaticCredentials = js_creds.into();

            // Cache the fresh credentials
            let mut cached = self.last_credential.write().await;
            *cached = Some(creds.clone());

            Ok(creds)
        }
    }

    /// A credentials fetcher that calls back into JS to get fresh GCS credentials.
    pub(crate) struct JsGcsCredentialsFetcher {
        pub(crate) tsfn: ThreadsafeFunction<(), JsGcsBearerCredential>,
        pub(crate) last_credential: RwLock<Option<icechunk::config::GcsBearerCredential>>,
    }

    impl std::fmt::Debug for JsGcsCredentialsFetcher {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("JsGcsCredentialsFetcher").finish()
        }
    }

    impl Serialize for JsGcsCredentialsFetcher {
        fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
            #[derive(Serialize)]
            struct Helper {
                last_credential: Option<icechunk::config::GcsBearerCredential>,
            }
            let cached = self.last_credential.blocking_read().clone();
            Helper { last_credential: cached }.serialize(serializer)
        }
    }

    impl<'de> Deserialize<'de> for JsGcsCredentialsFetcher {
        fn deserialize<D: Deserializer<'de>>(_deserializer: D) -> Result<Self, D::Error> {
            Err(serde::de::Error::custom(
                "JsGcsCredentialsFetcher cannot be deserialized: JS callback is not serializable",
            ))
        }
    }

    #[async_trait]
    #[typetag::serde(name = "js_gcs_credentials_fetcher")]
    impl icechunk::config::GcsCredentialsFetcher for JsGcsCredentialsFetcher {
        async fn get(&self) -> Result<icechunk::config::GcsBearerCredential, String> {
            // Check if cached credentials are still valid
            {
                let cached = self.last_credential.read().await;
                if let Some(creds) = cached.as_ref() {
                    match creds.expires_after {
                        None => return Ok(creds.clone()),
                        Some(expiration)
                            if expiration
                                > Utc::now()
                                    + TimeDelta::seconds(rand::random_range(
                                        120..=180,
                                    )) =>
                        {
                            return Ok(creds.clone());
                        }
                        _ => {}
                    }
                }
            }

            // Call the JS function to get fresh credentials
            let js_creds: JsGcsBearerCredential =
                self.tsfn.call_async(Ok(())).await.map_err(|e| {
                    format!("Failed to call JS GCS credentials fetcher: {e}")
                })?;

            let creds: icechunk::config::GcsBearerCredential = js_creds.into();

            // Cache the fresh credentials
            let mut cached = self.last_credential.write().await;
            *cached = Some(creds.clone());

            Ok(creds)
        }
    }
}

#[cfg(not(target_family = "wasm"))]
pub(crate) use native::*;

#[napi]
impl JsStorage {
    #[napi(factory)]
    pub async fn new_in_memory() -> napi::Result<JsStorage> {
        let storage = icechunk::storage::new_in_memory_storage().await.map_napi_err()?;
        Ok(JsStorage(storage))
    }

    /// Create a Storage backed by a JS object implementing the StorageBackend interface.
    ///
    /// This allows providing a custom storage implementation using JS libraries
    /// (fetch, @aws-sdk/client-s3, @google-cloud/storage, etc.), which is
    /// especially useful for WASM builds where native Rust networking is unavailable.
    #[napi(
        factory,
        ts_args_type = "backend: { canWrite: () => Promise<boolean>; getObjectRange: (args: StorageGetObjectRangeArgs) => Promise<StorageGetObjectResponse>; putObject: (args: StoragePutObjectArgs) => Promise<StorageVersionedUpdateResult>; copyObject: (args: StorageCopyObjectArgs) => Promise<StorageVersionedUpdateResult>; listObjects: (prefix: string) => Promise<Array<StorageListInfo>>; deleteBatch: (args: StorageDeleteBatchArgs) => Promise<StorageDeleteObjectsResult>; getObjectLastModified: (path: string) => Promise<Date>; getObjectConditional: (args: StorageGetObjectConditionalArgs) => Promise<StorageGetModifiedResult> }",
        ts_return_type = "Storage"
    )]
    pub fn new_custom(backend: napi::bindgen_prelude::Object) -> napi::Result<JsStorage> {
        use napi::bindgen_prelude::JsObjectValue;
        let can_write_fn = backend.get_named_property("canWrite")?;
        let get_object_range_fn = backend.get_named_property("getObjectRange")?;
        let put_object_fn = backend.get_named_property("putObject")?;
        let copy_object_fn = backend.get_named_property("copyObject")?;
        let list_objects_fn = backend.get_named_property("listObjects")?;
        let delete_batch_fn = backend.get_named_property("deleteBatch")?;
        let get_object_last_modified_fn =
            backend.get_named_property("getObjectLastModified")?;
        let get_object_conditional_fn =
            backend.get_named_property("getObjectConditional")?;

        let storage = JsCallbackStorage {
            can_write_fn,
            get_object_range_fn,
            put_object_fn,
            copy_object_fn,
            list_objects_fn,
            delete_batch_fn,
            get_object_last_modified_fn,
            get_object_conditional_fn,
        };
        Ok(JsStorage(Arc::new(storage)))
    }
}

#[cfg(not(target_family = "wasm"))]
fn default_s3_options() -> icechunk::config::S3Options {
    icechunk::config::S3Options {
        region: None,
        endpoint_url: None,
        allow_http: false,
        anonymous: false,
        force_path_style: false,
        network_stream_timeout_seconds: None,
        requester_pays: false,
    }
}

#[cfg(not(target_family = "wasm"))]
#[napi]
impl JsStorage {
    #[napi(factory)]
    pub async fn new_local_filesystem(path: String) -> napi::Result<JsStorage> {
        let storage =
            icechunk::storage::new_local_filesystem_storage(std::path::Path::new(&path))
                .await
                .map_napi_err()?;
        Ok(JsStorage(storage))
    }

    #[napi(factory)]
    pub fn new_s3(
        bucket: String,
        prefix: Option<String>,
        credentials: Option<JsS3Credentials>,
        options: Option<JsS3Options>,
    ) -> napi::Result<JsStorage> {
        let creds = credentials.map(|c| c.into());
        let opts = options.map(|o| o.into()).unwrap_or(icechunk::config::S3Options {
            region: None,
            endpoint_url: None,
            allow_http: false,
            anonymous: false,
            force_path_style: false,
            network_stream_timeout_seconds: None,
            requester_pays: false,
        });
        let storage = icechunk::storage::new_s3_storage(opts, bucket, prefix, creds)
            .map_napi_err()?;
        Ok(JsStorage(storage))
    }

    #[napi(factory)]
    pub fn new_r2(
        bucket: Option<String>,
        prefix: Option<String>,
        account_id: Option<String>,
        credentials: Option<JsS3Credentials>,
        options: Option<JsS3Options>,
    ) -> napi::Result<JsStorage> {
        let creds = credentials.map(|c| c.into());
        let opts = options.map(|o| o.into()).unwrap_or(icechunk::config::S3Options {
            region: None,
            endpoint_url: None,
            allow_http: false,
            anonymous: false,
            force_path_style: false,
            network_stream_timeout_seconds: None,
            requester_pays: false,
        });
        let storage =
            icechunk::storage::new_r2_storage(opts, bucket, prefix, account_id, creds)
                .map_napi_err()?;
        Ok(JsStorage(storage))
    }

    #[napi(factory)]
    pub fn new_tigris(
        bucket: String,
        prefix: Option<String>,
        credentials: Option<JsS3Credentials>,
        options: Option<JsS3Options>,
        use_weak_consistency: Option<bool>,
    ) -> napi::Result<JsStorage> {
        let creds = credentials.map(|c| c.into());
        let opts = options.map(|o| o.into()).unwrap_or(icechunk::config::S3Options {
            region: None,
            endpoint_url: None,
            allow_http: false,
            anonymous: false,
            force_path_style: false,
            network_stream_timeout_seconds: None,
            requester_pays: false,
        });
        let weak_consistency = use_weak_consistency.unwrap_or(false);
        let storage = icechunk::storage::new_tigris_storage(
            opts,
            bucket,
            prefix,
            creds,
            weak_consistency,
        )
        .map_napi_err()?;
        Ok(JsStorage(storage))
    }

    #[napi(factory)]
    pub async fn new_s3_object_store(
        bucket: String,
        prefix: Option<String>,
        credentials: Option<JsS3Credentials>,
        options: Option<JsS3Options>,
    ) -> napi::Result<JsStorage> {
        let creds = credentials.map(|c| c.into());
        let opts = options.map(|o| o.into()).unwrap_or(icechunk::config::S3Options {
            region: None,
            endpoint_url: None,
            allow_http: false,
            anonymous: false,
            force_path_style: false,
            network_stream_timeout_seconds: None,
            requester_pays: false,
        });
        let storage =
            icechunk::storage::new_s3_object_store_storage(opts, bucket, prefix, creds)
                .await
                .map_napi_err()?;
        Ok(JsStorage(storage))
    }

    #[napi(factory)]
    pub fn new_gcs(
        bucket: String,
        prefix: Option<String>,
        credentials: Option<JsGcsCredentials>,
        config: Option<HashMap<String, String>>,
    ) -> napi::Result<JsStorage> {
        let creds = credentials.map(|c| c.into());
        let storage = icechunk::storage::new_gcs_storage(bucket, prefix, creds, config)
            .map_napi_err()?;
        Ok(JsStorage(storage))
    }

    #[napi(factory)]
    pub async fn new_azure_blob(
        account: String,
        container: String,
        prefix: Option<String>,
        credentials: Option<JsAzureCredentials>,
        config: Option<HashMap<String, String>>,
    ) -> napi::Result<JsStorage> {
        let creds = credentials.map(|c| c.into());
        let storage = icechunk::storage::new_azure_blob_storage(
            account, container, prefix, creds, config,
        )
        .await
        .map_napi_err()?;
        Ok(JsStorage(storage))
    }

    #[napi(factory)]
    pub fn new_http(
        base_url: String,
        config: Option<HashMap<String, String>>,
    ) -> napi::Result<JsStorage> {
        let storage =
            icechunk::storage::new_http_storage(&base_url, config).map_napi_err()?;
        Ok(JsStorage(storage))
    }

    // --- Refreshable credential factory methods ---

    #[napi(factory)]
    pub fn new_s3_with_refreshable_credentials(
        bucket: String,
        prefix: Option<String>,
        #[napi(ts_arg_type = "() => Promise<S3StaticCredentials>")]
        credentials_callback: napi::threadsafe_function::ThreadsafeFunction<
            (),
            JsS3StaticCredentials,
        >,
        initial_credentials: Option<JsS3StaticCredentials>,
        options: Option<JsS3Options>,
    ) -> napi::Result<JsStorage> {
        let fetcher = JsS3CredentialsFetcher {
            tsfn: credentials_callback,
            last_credential: tokio::sync::RwLock::new(
                initial_credentials.map(|c| c.into()),
            ),
        };
        let creds =
            icechunk::config::S3Credentials::Refreshable(std::sync::Arc::new(fetcher));
        let opts = options.map(|o| o.into()).unwrap_or(default_s3_options());
        let storage =
            icechunk::storage::new_s3_storage(opts, bucket, prefix, Some(creds))
                .map_napi_err()?;
        Ok(JsStorage(storage))
    }

    #[napi(factory)]
    pub fn new_r2_with_refreshable_credentials(
        bucket: Option<String>,
        prefix: Option<String>,
        account_id: Option<String>,
        #[napi(ts_arg_type = "() => Promise<S3StaticCredentials>")]
        credentials_callback: napi::threadsafe_function::ThreadsafeFunction<
            (),
            JsS3StaticCredentials,
        >,
        initial_credentials: Option<JsS3StaticCredentials>,
        options: Option<JsS3Options>,
    ) -> napi::Result<JsStorage> {
        let fetcher = JsS3CredentialsFetcher {
            tsfn: credentials_callback,
            last_credential: tokio::sync::RwLock::new(
                initial_credentials.map(|c| c.into()),
            ),
        };
        let creds =
            icechunk::config::S3Credentials::Refreshable(std::sync::Arc::new(fetcher));
        let opts = options.map(|o| o.into()).unwrap_or(default_s3_options());
        let storage = icechunk::storage::new_r2_storage(
            opts,
            bucket,
            prefix,
            account_id,
            Some(creds),
        )
        .map_napi_err()?;
        Ok(JsStorage(storage))
    }

    #[napi(factory)]
    pub fn new_tigris_with_refreshable_credentials(
        bucket: String,
        prefix: Option<String>,
        #[napi(ts_arg_type = "() => Promise<S3StaticCredentials>")]
        credentials_callback: napi::threadsafe_function::ThreadsafeFunction<
            (),
            JsS3StaticCredentials,
        >,
        initial_credentials: Option<JsS3StaticCredentials>,
        options: Option<JsS3Options>,
        use_weak_consistency: Option<bool>,
    ) -> napi::Result<JsStorage> {
        let fetcher = JsS3CredentialsFetcher {
            tsfn: credentials_callback,
            last_credential: tokio::sync::RwLock::new(
                initial_credentials.map(|c| c.into()),
            ),
        };
        let creds =
            icechunk::config::S3Credentials::Refreshable(std::sync::Arc::new(fetcher));
        let opts = options.map(|o| o.into()).unwrap_or(default_s3_options());
        let weak_consistency = use_weak_consistency.unwrap_or(false);
        let storage = icechunk::storage::new_tigris_storage(
            opts,
            bucket,
            prefix,
            Some(creds),
            weak_consistency,
        )
        .map_napi_err()?;
        Ok(JsStorage(storage))
    }

    #[napi(factory)]
    pub async fn new_s3_object_store_with_refreshable_credentials(
        bucket: String,
        prefix: Option<String>,
        #[napi(ts_arg_type = "() => Promise<S3StaticCredentials>")]
        credentials_callback: napi::threadsafe_function::ThreadsafeFunction<
            (),
            JsS3StaticCredentials,
        >,
        initial_credentials: Option<JsS3StaticCredentials>,
        options: Option<JsS3Options>,
    ) -> napi::Result<JsStorage> {
        let fetcher = JsS3CredentialsFetcher {
            tsfn: credentials_callback,
            last_credential: tokio::sync::RwLock::new(
                initial_credentials.map(|c| c.into()),
            ),
        };
        let creds =
            icechunk::config::S3Credentials::Refreshable(std::sync::Arc::new(fetcher));
        let opts = options.map(|o| o.into()).unwrap_or(default_s3_options());
        let storage = icechunk::storage::new_s3_object_store_storage(
            opts,
            bucket,
            prefix,
            Some(creds),
        )
        .await
        .map_napi_err()?;
        Ok(JsStorage(storage))
    }

    #[napi(factory)]
    pub fn new_gcs_with_refreshable_credentials(
        bucket: String,
        prefix: Option<String>,
        #[napi(ts_arg_type = "() => Promise<GcsBearerCredential>")]
        credentials_callback: napi::threadsafe_function::ThreadsafeFunction<
            (),
            JsGcsBearerCredential,
        >,
        initial_credentials: Option<JsGcsBearerCredential>,
        config: Option<HashMap<String, String>>,
    ) -> napi::Result<JsStorage> {
        let fetcher = JsGcsCredentialsFetcher {
            tsfn: credentials_callback,
            last_credential: tokio::sync::RwLock::new(
                initial_credentials.map(|c| c.into()),
            ),
        };
        let creds =
            icechunk::config::GcsCredentials::Refreshable(std::sync::Arc::new(fetcher));
        let storage =
            icechunk::storage::new_gcs_storage(bucket, prefix, Some(creds), config)
                .map_napi_err()?;
        Ok(JsStorage(storage))
    }
}
