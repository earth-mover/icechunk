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
    }

    impl From<JsS3StaticCredentials> for icechunk::config::S3StaticCredentials {
        fn from(creds: JsS3StaticCredentials) -> Self {
            icechunk::config::S3StaticCredentials {
                access_key_id: creds.access_key_id,
                secret_access_key: creds.secret_access_key,
                session_token: creds.session_token,
                expires_after: None,
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
}
