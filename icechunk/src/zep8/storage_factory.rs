//! Storage creation utilities for ZEP 8 icechunk URLs.
//!
//! Creates appropriate Icechunk Storage instances from preceding URLs
//! in ZEP 8 pipe-chained syntax.

use crate::Storage;
use std::fmt;
use url::Url;

/// Error type for storage creation.
#[derive(Debug)]
pub enum ZepStorageError {
    UnsupportedScheme(String),
    InvalidUrl(String),
    MemoryNotSupported,
    StorageCreation(String),
}

impl fmt::Display for ZepStorageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ZepStorageError::UnsupportedScheme(scheme) => {
                write!(f, "Unsupported storage scheme: {scheme}")
            }
            ZepStorageError::InvalidUrl(url) => write!(f, "Invalid URL: {url}"),
            ZepStorageError::MemoryNotSupported => {
                write!(f, "Memory storage not supported for ZEP 8 URLs")
            }
            ZepStorageError::StorageCreation(msg) => {
                write!(f, "Storage creation failed: {msg}")
            }
        }
    }
}

impl std::error::Error for ZepStorageError {}

/// Create appropriate Icechunk storage from a URL.
///
/// # Arguments
/// * `url` - The preceding URL (e.g., "s3://bucket/path", "file:/path/to/repo")
///
/// # Returns
/// A configured Storage instance for the given URL
///
/// # Errors
/// Returns `ZepStorageError` if the URL scheme is unsupported or storage creation fails
///
/// # Examples
/// ```no_run
/// use icechunk::zep8::create_storage_from_url;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// // S3 storage
/// let storage = create_storage_from_url("s3://my-bucket/repo").await?;
///
/// // Local filesystem
/// let storage = create_storage_from_url("file:/path/to/repo").await?;
///
/// // GCS storage
/// let storage = create_storage_from_url("gs://my-bucket/repo").await?;
/// # Ok(())
/// # }
/// ```
pub async fn create_storage_from_url(
    url: &str,
) -> Result<Box<dyn Storage>, ZepStorageError> {
    // Handle memory URLs explicitly
    if url == "memory:" {
        return Err(ZepStorageError::MemoryNotSupported);
    }

    // Parse the URL
    let parsed_url = Url::parse(url).map_err(|e| {
        ZepStorageError::InvalidUrl(format!("Failed to parse URL '{url}': {e}"))
    })?;

    let scheme = parsed_url.scheme();

    match scheme {
        "file" => create_local_storage(&parsed_url).await,
        "s3" => create_s3_storage(&parsed_url).await,
        "gs" | "gcs" => create_gcs_storage(&parsed_url).await,
        _ => Err(ZepStorageError::UnsupportedScheme(scheme.to_string())),
    }
}

/// Create local filesystem storage from a file:// URL.
async fn create_local_storage(_url: &Url) -> Result<Box<dyn Storage>, ZepStorageError> {
    // TODO: Implement actual storage creation
    // This is a placeholder for the full implementation
    Err(ZepStorageError::StorageCreation("Not implemented yet".to_string()))
}

/// Create S3 storage from an s3:// URL.
async fn create_s3_storage(_url: &Url) -> Result<Box<dyn Storage>, ZepStorageError> {
    // TODO: Implement actual storage creation
    // This is a placeholder for the full implementation
    Err(ZepStorageError::StorageCreation("Not implemented yet".to_string()))
}

/// Create Google Cloud Storage from a gs:// or gcs:// URL.
async fn create_gcs_storage(_url: &Url) -> Result<Box<dyn Storage>, ZepStorageError> {
    // TODO: Implement actual storage creation
    // This is a placeholder for the full implementation
    Err(ZepStorageError::StorageCreation("Not implemented yet".to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_memory_url_rejected() {
        let rt = tokio::runtime::Runtime::new().expect("Failed to create tokio runtime");
        let result = rt.block_on(create_storage_from_url("memory:"));
        assert!(matches!(result, Err(ZepStorageError::MemoryNotSupported)));
    }

    #[test]
    fn test_invalid_url() {
        let rt = tokio::runtime::Runtime::new().expect("Failed to create tokio runtime");
        let result = rt.block_on(create_storage_from_url("not-a-url"));
        assert!(matches!(result, Err(ZepStorageError::InvalidUrl(_))));
    }

    #[test]
    fn test_unsupported_scheme() {
        let rt = tokio::runtime::Runtime::new().expect("Failed to create tokio runtime");
        let result = rt.block_on(create_storage_from_url("http://example.com/path"));
        assert!(matches!(result, Err(ZepStorageError::UnsupportedScheme(_))));
    }

    #[test]
    fn test_s3_url_parsing() {
        // This test only validates URL parsing, actual storage creation would require AWS credentials
        let url =
            Url::parse("s3://my-bucket/path/to/repo").expect("Failed to parse valid URL");
        assert_eq!(url.host_str(), Some("my-bucket"));
        assert_eq!(url.path(), "/path/to/repo");
    }

    #[test]
    fn test_gcs_url_parsing() {
        // This test only validates URL parsing, actual storage creation would require GCS credentials
        let url =
            Url::parse("gs://my-bucket/path/to/repo").expect("Failed to parse valid URL");
        assert_eq!(url.host_str(), Some("my-bucket"));
        assert_eq!(url.path(), "/path/to/repo");
    }

    #[test]
    fn test_file_url_parsing() {
        let url = Url::parse("file:/path/to/repo").expect("Failed to parse valid URL");
        assert_eq!(url.path(), "/path/to/repo");
    }

    #[test]
    fn test_storage_error_display() {
        let error = ZepStorageError::UnsupportedScheme("ftp".to_string());
        assert!(error.to_string().contains("ftp"));

        let error = ZepStorageError::MemoryNotSupported;
        assert!(error.to_string().contains("Memory storage"));
    }
}
