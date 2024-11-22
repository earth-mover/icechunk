#[path = "storage_tests/storage_tests.rs"]
mod storage_tests;

use chrono::Utc;
use icechunk::storage::{
        azure_blob::{create_container_if_not_exists, AzureBlobConfig, AzureBlobStorage, AzureStorageCredentials, Location},
        StorageResult,
    };

async fn mk_storage() -> StorageResult<AzureBlobStorage> {
    let storage = AzureBlobStorage::new_azure_blob_store (
        "testcontainer".to_string(),
        "test_blob_storage__".to_string() + Utc::now().to_rfc3339().as_str(),
        &AzureBlobConfig {
            cloud_location: Location::Emulator("127.0.0.1".to_string(), 10000),
            credentials: AzureStorageCredentials::AccessKey(None),
        },
    )
    .await;
    if let Ok(ref s) = storage {
        create_container_if_not_exists(s).await?;
    }
    Ok(storage?)
}

#[tokio::test]
pub async fn test_blob_snapshot_write_read() -> Result<(), Box<dyn std::error::Error>> {
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
