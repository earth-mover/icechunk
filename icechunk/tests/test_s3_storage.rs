#[path = "storage_tests/storage_tests.rs"]
mod storage_tests;

use chrono::Utc;
use icechunk::storage::{
        s3::{S3Config, S3Credentials, S3Storage, StaticS3Credentials},
        StorageResult,
    };

async fn mk_storage() -> StorageResult<S3Storage> {
    S3Storage::new_s3_store(
        "testbucket",
        "test_s3_storage__".to_string() + Utc::now().to_rfc3339().as_str(),
        Some(&S3Config {
            region: Some("us-east-1".to_string()),
            endpoint: Some("http://localhost:9000".to_string()),
            credentials: S3Credentials::Static(StaticS3Credentials {
                access_key_id: "minio123".into(),
                secret_access_key: "minio123".into(),
                session_token: None,
            }),
            allow_http: true,
        }),
    )
    .await
}

#[tokio::test]
pub async fn test_snapshot_write_read() -> Result<(), Box<dyn std::error::Error>> {
    let storage = mk_storage().await?;
    Ok(storage_tests::storage_tests::test_snapshot_write_read(storage).await?)
}

#[tokio::test]
pub async fn test_manifest_write_read() -> Result<(), Box<dyn std::error::Error>> {
    let storage = mk_storage().await?;
    Ok(storage_tests::storage_tests::test_manifest_write_read(storage).await?)
}

#[tokio::test]
pub async fn test_chunk_write_read() -> Result<(), Box<dyn std::error::Error>> {
    let storage = mk_storage().await?;
    Ok(storage_tests::storage_tests::test_chunk_write_read(storage).await?)
}

#[tokio::test]
pub async fn test_tag_write_get() -> Result<(), Box<dyn std::error::Error>> {
    let storage = mk_storage().await?;
    Ok(storage_tests::storage_tests::test_tag_write_get(storage).await?)
}

#[tokio::test]
pub async fn test_fetch_non_existing_tag() -> Result<(), Box<dyn std::error::Error>> {
    let storage = mk_storage().await?;
    Ok(storage_tests::storage_tests::test_fetch_non_existing_tag(storage).await?)
}

#[tokio::test]
pub async fn test_create_existing_tag() -> Result<(), Box<dyn std::error::Error>> {
    let storage = mk_storage().await?;
    Ok(storage_tests::storage_tests::test_create_existing_tag(storage).await?)
}

#[tokio::test]
pub async fn test_branch_initialization() -> Result<(), Box<dyn std::error::Error>> {
    let storage = mk_storage().await?;
    Ok(storage_tests::storage_tests::test_branch_initialization(storage).await?)
}

#[tokio::test]
pub async fn test_fetch_non_existing_branch() -> Result<(), Box<dyn std::error::Error>> {
    let storage = mk_storage().await?;
    Ok(storage_tests::storage_tests::test_fetch_non_existing_branch(storage).await?)
}

#[tokio::test]
pub async fn test_branch_update() -> Result<(), Box<dyn std::error::Error>> {
    let storage = mk_storage().await?;
    Ok(storage_tests::storage_tests::test_branch_update(storage).await?)
}

#[tokio::test]
pub async fn test_ref_names() -> Result<(), Box<dyn std::error::Error>> {
    let storage = mk_storage().await?;
    Ok(storage_tests::storage_tests::test_ref_names(storage).await?)
}
