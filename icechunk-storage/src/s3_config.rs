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

/// Checksum algorithm to use for S3 write requests.
///
/// When unset, the AWS Rust SDK picks its own default for the `x-amz-checksum-*`
/// header on `PutObject`, `UploadPart`, and `DeleteObjects`. Some S3-compatible
/// services only accept a subset of algorithms; set this option to override the
/// SDK's choice.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum S3ChecksumAlgorithm {
    Crc32,
    Crc32c,
    Crc64Nvme,
    Sha1,
    Sha256,
}

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
    #[serde(default)]
    pub checksum_algorithm: Option<S3ChecksumAlgorithm>,
}

fn default_force_path_style() -> bool {
    false
}

impl S3Options {
    /// Return key-value pairs of non-default configuration for display purposes.
    pub fn info_fields(&self) -> Vec<(&'static str, String)> {
        let mut fields = Vec::new();
        if let Some(region) = &self.region {
            fields.push(("region", region.clone()));
        }
        if let Some(endpoint) = &self.endpoint_url {
            fields.push(("endpoint_url", endpoint.clone()));
        }
        if self.anonymous {
            fields.push(("anonymous", "True".to_string()));
        }
        if self.allow_http {
            fields.push(("allow_http", "True".to_string()));
        }
        if self.force_path_style {
            fields.push(("force_path_style", "True".to_string()));
        }
        if self.requester_pays {
            fields.push(("requester_pays", "True".to_string()));
        }
        if let Some(algo) = self.checksum_algorithm {
            fields.push(("checksum_algorithm", format!("{algo:?}")));
        }
        fields
    }
}

impl fmt::Display for S3ChecksumAlgorithm {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            S3ChecksumAlgorithm::Crc32 => "crc32",
            S3ChecksumAlgorithm::Crc32c => "crc32c",
            S3ChecksumAlgorithm::Crc64Nvme => "crc64nvme",
            S3ChecksumAlgorithm::Sha1 => "sha1",
            S3ChecksumAlgorithm::Sha256 => "sha256",
        };
        f.write_str(s)
    }
}

impl fmt::Display for S3Options {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "S3Options(region={}, endpoint_url={}, anonymous={}, allow_http={}, force_path_style={}, network_stream_timeout_seconds={}, requester_pays={}, checksum_algorithm={})",
            self.region.as_deref().unwrap_or("None"),
            self.endpoint_url.as_deref().unwrap_or("None"),
            self.anonymous,
            self.allow_http,
            self.force_path_style,
            self.network_stream_timeout_seconds
                .map(|n| n.to_string())
                .unwrap_or("None".to_string()),
            self.requester_pays,
            self.checksum_algorithm.map(|a| a.to_string()).unwrap_or("None".to_string()),
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
