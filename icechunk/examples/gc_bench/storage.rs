//! `RustFS` storage construction for datasets.

use std::sync::Arc;

use icechunk::{
    Storage,
    config::{S3Credentials, S3Options, S3StaticCredentials},
    new_s3_storage,
};

use crate::BoxError;

pub(crate) const RUSTFS_PORT: u16 = 4200;
pub(crate) const TOXIPROXY_PORT: u16 = 9002;
const BUCKET: &str = "testbucket";

pub(crate) fn dataset_prefix(name: &str) -> String {
    format!("gc-bench/{name}")
}

/// Storage for dataset `name`, talking to `localhost:port` (`RustFS` directly, or the toxiproxy port).
pub(crate) fn rustfs_storage(
    name: &str,
    port: u16,
) -> Result<Arc<dyn Storage + Send + Sync>, BoxError> {
    let options = S3Options::default()
        .with_region("us-east-1")
        .with_endpoint_url(format!("http://localhost:{port}"))
        .with_allow_http(true)
        .with_force_path_style(true);
    let credentials = S3Credentials::Static(S3StaticCredentials {
        access_key_id: "modify".to_string(),
        secret_access_key: "modifydata".to_string(),
        session_token: None,
        expires_after: None,
    });
    Ok(new_s3_storage(
        options,
        BUCKET.to_string(),
        Some(dataset_prefix(name)),
        Some(credentials),
        Vec::new(),
        Vec::new(),
        None,
    )?)
}

/// Where the credentials for a real S3 target came from. Only the source is
/// ever printed, never the values.
#[derive(Debug, Clone, Copy)]
pub(crate) enum CredentialSource {
    /// `AWS_ACCESS_KEY_ID` + `AWS_SECRET_ACCESS_KEY` (+ optional `AWS_SESSION_TOKEN`)
    Static { session_token: bool },
    /// The AWS SDK's default chain: instance role, shared config profile, ...
    FromEnv,
}

impl std::fmt::Display for CredentialSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Static { session_token: true } => write!(
                f,
                "Static (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN)"
            ),
            Self::Static { session_token: false } => {
                write!(f, "Static (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)")
            }
            Self::FromEnv => write!(f, "FromEnv"),
        }
    }
}

/// Static credentials if both key variables are set, the SDK default chain otherwise.
fn s3_credentials() -> (S3Credentials, CredentialSource) {
    match (std::env::var("AWS_ACCESS_KEY_ID"), std::env::var("AWS_SECRET_ACCESS_KEY")) {
        (Ok(access_key_id), Ok(secret_access_key)) => {
            let session_token = std::env::var("AWS_SESSION_TOKEN").ok();
            let source =
                CredentialSource::Static { session_token: session_token.is_some() };
            let credentials = S3Credentials::Static(S3StaticCredentials {
                access_key_id,
                secret_access_key,
                session_token,
                expires_after: None,
            });
            (credentials, source)
        }
        _ => (S3Credentials::FromEnv, CredentialSource::FromEnv),
    }
}

/// Storage for a real S3 location: no endpoint override, no toxiproxy.
pub(crate) fn s3_storage(
    bucket: &str,
    prefix: &str,
    region: &str,
) -> Result<(Arc<dyn Storage + Send + Sync>, CredentialSource), BoxError> {
    let options = S3Options::default().with_region(region);
    let (credentials, source) = s3_credentials();
    let storage = new_s3_storage(
        options,
        bucket.to_string(),
        Some(prefix.to_string()),
        Some(credentials),
        Vec::new(),
        Vec::new(),
        None,
    )?;
    Ok((storage, source))
}
