#![allow(dead_code)]
use std::{env, sync::Arc};

use icechunk::{
    Storage,
    config::{S3Credentials, S3Options, S3StaticCredentials},
    new_s3_storage,
    storage::{new_r2_storage, new_tigris_storage},
};

pub(crate) fn make_minio_integration_storage(
    prefix: String,
) -> Result<Arc<dyn Storage + Send + Sync>, Box<dyn std::error::Error>> {
    let storage: Arc<dyn Storage + Send + Sync> = new_s3_storage(
        S3Options {
            region: Some("us-east-1".to_string()),
            endpoint_url: Some("http://localhost:9000".to_string()),
            allow_http: true,
            anonymous: false,
            force_path_style: true,
        },
        "testbucket".to_string(),
        Some(prefix),
        Some(S3Credentials::Static(S3StaticCredentials {
            access_key_id: "minio123".into(),
            secret_access_key: "minio123".into(),
            session_token: None,
            expires_after: None,
        })),
    )?;
    Ok(storage)
}

pub(crate) fn make_tigris_integration_storage(
    prefix: String,
) -> Result<Arc<dyn Storage + Send + Sync>, Box<dyn std::error::Error>> {
    let credentials = S3Credentials::Static(S3StaticCredentials {
        access_key_id: env::var("TIGRIS_ACCESS_KEY_ID")?,
        secret_access_key: env::var("TIGRIS_SECRET_ACCESS_KEY")?,
        session_token: None,
        expires_after: None,
    });
    let bucket = env::var("TIGRIS_BUCKET")?;
    let region = env::var("TIGRIS_REGION")?;

    let storage: Arc<dyn Storage + Send + Sync> = new_tigris_storage(
        S3Options {
            region: Some(region),
            endpoint_url: None,
            anonymous: false,
            allow_http: false,
            force_path_style: false,
        },
        bucket,
        Some(prefix),
        Some(credentials),
        false,
    )?;
    Ok(storage)
}

pub(crate) fn make_r2_integration_storage(
    prefix: String,
) -> Result<Arc<dyn Storage + Send + Sync>, Box<dyn std::error::Error>> {
    let credentials = S3Credentials::Static(S3StaticCredentials {
        access_key_id: env::var("R2_ACCESS_KEY_ID")?,
        secret_access_key: env::var("R2_SECRET_ACCESS_KEY")?,
        session_token: None,
        expires_after: None,
    });
    let bucket = env::var("R2_BUCKET")?;

    let storage: Arc<dyn Storage + Send + Sync> = new_r2_storage(
        S3Options {
            region: None,
            endpoint_url: None,
            anonymous: false,
            allow_http: false,
            force_path_style: false,
        },
        Some(bucket),
        Some(prefix),
        Some(env::var("R2_ACCOUNT_ID")?),
        Some(credentials),
    )?;
    Ok(storage)
}

pub(crate) fn make_aws_integration_storage(
    prefix: String,
) -> Result<Arc<dyn Storage + Send + Sync>, Box<dyn std::error::Error>> {
    let credentials = S3Credentials::Static(S3StaticCredentials {
        access_key_id: env::var("AWS_ACCESS_KEY_ID")?,
        secret_access_key: env::var("AWS_SECRET_ACCESS_KEY")?,
        session_token: None,
        expires_after: None,
    });
    let bucket = env::var("AWS_BUCKET")?;
    let region = env::var("AWS_REGION")?;

    let storage: Arc<dyn Storage + Send + Sync> = new_s3_storage(
        S3Options {
            region: Some(region),
            endpoint_url: None,
            anonymous: false,
            allow_http: false,
            force_path_style: false,
        },
        bucket,
        Some(prefix),
        Some(credentials),
    )?;
    Ok(storage)
}
