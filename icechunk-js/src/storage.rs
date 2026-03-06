use std::sync::Arc;

use icechunk::Storage;
use napi_derive::napi;

use crate::errors::IntoNapiResult;

#[cfg(not(target_family = "wasm"))]
use std::collections::HashMap;

#[napi(js_name = "Storage")]
pub struct JsStorage(pub(crate) Arc<dyn Storage + Send + Sync>);

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
