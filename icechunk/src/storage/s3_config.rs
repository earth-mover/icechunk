//! Shared S3 configuration types used by both the native AWS SDK backend and the
//! `object_store`-based S3 backend.
//!
//! These types are always compiled (no feature gate) so that [`ObjectStoreConfig::S3`],
//! [`ObjectStoreConfig::S3Compatible`], [`ObjectStoreConfig::Tigris`], and
//! [`Credentials::S3`] are available regardless of which S3 implementation is enabled.

use core::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct S3Options {
    pub region: Option<String>,
    pub endpoint_url: Option<String>,
    pub anonymous: bool,
    pub allow_http: bool,
    // field was added in v0.2.6
    #[serde(default = "default_force_path_style")]
    pub force_path_style: bool,
    pub network_stream_timeout_seconds: Option<u32>,
    #[serde(default)]
    pub requester_pays: bool,
}

fn default_force_path_style() -> bool {
    false
}

impl fmt::Display for S3Options {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "S3Options(region={}, endpoint_url={}, anonymous={}, allow_http={}, force_path_style={}, network_stream_timeout_seconds={}, requester_pays={})",
            self.region.as_deref().unwrap_or("None"),
            self.endpoint_url.as_deref().unwrap_or("None"),
            self.anonymous,
            self.allow_http,
            self.force_path_style,
            self.network_stream_timeout_seconds
                .map(|n| n.to_string())
                .unwrap_or("None".to_string()),
            self.requester_pays,
        )
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct S3StaticCredentials {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: Option<String>,
    pub expires_after: Option<DateTime<Utc>>,
}

#[async_trait]
#[typetag::serde(tag = "s3_credentials_fetcher_type")]
pub trait S3CredentialsFetcher: fmt::Debug + Sync + Send {
    async fn get(&self) -> Result<S3StaticCredentials, String>;
}

/// S3 authentication credentials.
#[derive(Clone, Debug, Deserialize, Serialize, Default)]
#[serde(tag = "s3_credential_type")]
#[serde(rename_all = "snake_case")]
pub enum S3Credentials {
    #[default]
    FromEnv,
    Anonymous,
    Static(S3StaticCredentials),
    Refreshable(Arc<dyn S3CredentialsFetcher>),
}
