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
