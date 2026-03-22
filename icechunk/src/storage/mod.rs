//! Object store abstraction layer.
//!
//! The [`Storage`] trait defines generic object store operations (get, put, delete,
//! list) for persisting Icechunk data. Constructor functions like [`new_s3_storage`],
//! [`new_gcs_storage`], [`new_in_memory_storage`] create configured storage instances.

#[cfg(feature = "object-store-http")]
use ::object_store::ClientConfigKey;
#[cfg(feature = "object-store-azure")]
use ::object_store::azure::AzureConfigKey;
#[cfg(feature = "object-store-gcs")]
use ::object_store::gcp::GoogleConfigKey;
use icechunk_types::ICResultExt as _;
use std::{collections::HashMap, path::Path, str::FromStr as _, sync::Arc};
use url::Url;

#[cfg(feature = "redirect")]
use crate::storage::redirect::RedirectStorage;
#[cfg(feature = "object-store-azure")]
use object_store::AzureCredentials;
#[cfg(feature = "object-store-gcs")]
use object_store::GcsCredentials;
#[cfg(feature = "s3")]
use s3::S3Storage;

// Re-export everything from icechunk-storage
pub use icechunk_storage::{
    ConcurrencySettings, DeleteObjectsResult, ETag, Generation, GetModifiedResult,
    ICError, ListInfo, RetriesSettings, Settings, Storage, StorageError,
    StorageErrorKind, StorageResult, TimeoutSettings, VersionInfo, VersionedUpdateResult,
    s3_config::{S3Credentials, S3CredentialsFetcher, S3Options, S3StaticCredentials},
    split_in_multiple_equal_requests, split_in_multiple_requests, strip_quotes,
};

#[cfg(test)]
pub mod logging;

/// Storage wrapper that adds artificial read/write latency (for testing).
pub mod latency;

/// Storage using the `object_store` crate (local, in-memory, Azure, GCS).
pub mod object_store;
/// HTTP redirect-based storage for read-only access.
#[cfg(feature = "redirect")]
pub mod redirect;
/// Native S3 client implementation.
#[cfg(feature = "s3")]
pub mod s3;
/// Shared S3 configuration types (always compiled).
pub mod s3_config;

pub use object_store::ObjectStorage;

#[cfg(feature = "s3")]
pub fn new_s3_storage(
    config: S3Options,
    bucket: String,
    prefix: Option<String>,
    credentials: Option<S3Credentials>,
) -> StorageResult<Arc<dyn Storage + Send + Sync>> {
    if let Some(endpoint) = &config.endpoint_url
        && (endpoint.contains("fly.storage.tigris.dev")
            || endpoint.contains("t3.storage.dev"))
    {
        use icechunk_storage::other_error;

        return Err(other_error(
            "Tigris Storage is not S3 compatible, use the Tigris specific constructor instead"
                .to_string(),
        ));
    }

    let st = S3Storage::new(
        config,
        bucket,
        prefix,
        credentials.unwrap_or(S3Credentials::FromEnv),
        true,
        Vec::new(),
        Vec::new(),
    )?;
    Ok(Arc::new(st))
}

#[cfg(feature = "s3")]
pub fn new_r2_storage(
    config: S3Options,
    bucket: Option<String>,
    prefix: Option<String>,
    account_id: Option<String>,
    credentials: Option<S3Credentials>,
) -> StorageResult<Arc<dyn Storage + Send + Sync>> {
    let (bucket, prefix) = match (bucket, prefix) {
        (Some(bucket), Some(prefix)) => (bucket, Some(prefix)),
        (None, Some(prefix)) => match prefix.split_once("/") {
            Some((bucket, prefix)) => (bucket.to_string(), Some(prefix.to_string())),
            None => (prefix, None),
        },
        (Some(bucket), None) => (bucket, None),
        (None, None) => {
            use icechunk_types::ICResultExt as _;

            return Err(StorageErrorKind::R2ConfigurationError(
                "Either bucket or prefix must be provided.".to_string(),
            ))
            .ic_err();
        }
    };

    if config.endpoint_url.is_none() && account_id.is_none() {
        use icechunk_types::ICResultExt as _;

        return Err(StorageErrorKind::R2ConfigurationError(
            "Either endpoint_url or account_id must be provided.".to_string(),
        ))
        .ic_err();
    }

    let config = S3Options {
        region: config.region.or(Some("auto".to_string())),
        endpoint_url: config
            .endpoint_url
            .or(account_id.map(|x| format!("https://{x}.r2.cloudflarestorage.com"))),
        force_path_style: true,
        ..config
    };
    let st = S3Storage::new(
        config,
        bucket,
        prefix,
        credentials.unwrap_or(S3Credentials::FromEnv),
        true,
        Vec::new(),
        Vec::new(),
    )?;
    Ok(Arc::new(st))
}

#[cfg(feature = "s3")]
pub fn new_tigris_storage(
    config: S3Options,
    bucket: String,
    prefix: Option<String>,
    credentials: Option<S3Credentials>,
    use_weak_consistency: bool,
) -> StorageResult<Arc<dyn Storage + Send + Sync>> {
    let config = S3Options {
        endpoint_url: Some(
            config.endpoint_url.unwrap_or("https://t3.storage.dev".to_string()),
        ),
        ..config
    };
    let mut extra_write_headers = Vec::with_capacity(2);
    let mut extra_read_headers = Vec::with_capacity(3);

    if !use_weak_consistency {
        // TODO: Tigris will need more than this to offer good eventually consistent behavior
        // For example: we should use no-cache for branches and config file
        if let Some(region) = config.region.as_ref() {
            extra_write_headers.push(("X-Tigris-Regions".to_string(), region.clone()));
            extra_write_headers
                .push(("X-Tigris-Consistent".to_string(), "true".to_string()));

            extra_read_headers.push(("X-Tigris-Regions".to_string(), region.clone()));
            extra_read_headers
                .push(("Cache-Control".to_string(), "no-cache".to_string()));
            extra_read_headers
                .push(("X-Tigris-Consistent".to_string(), "true".to_string()));
        } else {
            use icechunk_storage::other_error;

            return Err(other_error("Tigris storage requires a region to provide full consistency. Either set the region for the bucket or use the read-only, eventually consistent storage by passing `use_weak_consistency=True` (experts only)".to_string()));
        }
    }
    let st = S3Storage::new(
        config,
        bucket,
        prefix,
        credentials.unwrap_or(S3Credentials::FromEnv),
        !use_weak_consistency, // notice eventually consistent storage can't do writes
        extra_read_headers,
        extra_write_headers,
    )?;
    Ok(Arc::new(st))
}

pub async fn new_in_memory_storage() -> StorageResult<Arc<dyn Storage + Send + Sync>> {
    let st = ObjectStorage::new_in_memory().await?;
    Ok(Arc::new(st))
}

#[cfg(feature = "object-store-fs")]
pub async fn new_local_filesystem_storage(
    path: &Path,
) -> StorageResult<Arc<dyn Storage + Send + Sync>> {
    let st = ObjectStorage::new_local_filesystem(path).await?;
    Ok(Arc::new(st))
}

#[cfg(feature = "object-store-http")]
pub fn new_http_storage(
    base_url: &str,
    config: Option<HashMap<String, String>>,
) -> StorageResult<Arc<dyn Storage + Send + Sync>> {
    let base_url = Url::parse(base_url)
        .map_err(|e| StorageErrorKind::CannotParseUrl {
            cause: e,
            url: base_url.to_string(),
        })
        .ic_err()?;
    let config = config
        .unwrap_or_default()
        .iter()
        .filter_map(|(k, v)| {
            ClientConfigKey::from_str(k).ok().map(|key| (key, v.clone()))
        })
        .collect();
    let st = ObjectStorage::new_http(&base_url, Some(config))?;
    Ok(Arc::new(st))
}

#[cfg(feature = "redirect")]
pub fn new_redirect_storage(
    base_url: &str,
) -> StorageResult<Arc<dyn Storage + Send + Sync>> {
    let base_url = Url::parse(base_url)
        .map_err(|e| StorageErrorKind::CannotParseUrl {
            cause: e,
            url: base_url.to_string(),
        })
        .ic_err()?;
    Ok(Arc::new(RedirectStorage::new(base_url)))
}

#[cfg(feature = "object-store-s3")]
pub async fn new_s3_object_store_storage(
    config: S3Options,
    bucket: String,
    prefix: Option<String>,
    credentials: Option<S3Credentials>,
) -> StorageResult<Arc<dyn Storage + Send + Sync>> {
    if let Some(endpoint) = &config.endpoint_url
        && (endpoint.contains("fly.storage.tigris.dev")
            || endpoint.contains("t3.storage.dev"))
    {
        use icechunk_storage::other_error;

        return Err(StorageError::from(other_error(
            "Tigris Storage is not S3 compatible, use the Tigris specific constructor instead"
                .to_string(),
        )));
    }
    let storage =
        ObjectStorage::new_s3(bucket, prefix, credentials, Some(config)).await?;
    Ok(Arc::new(storage))
}

#[cfg(feature = "object-store-azure")]
pub async fn new_azure_blob_storage(
    account: String,
    container: String,
    prefix: Option<String>,
    credentials: Option<AzureCredentials>,
    config: Option<HashMap<String, String>>,
) -> StorageResult<Arc<dyn Storage + Send + Sync>> {
    let config = config
        .unwrap_or_default()
        .into_iter()
        .filter_map(|(key, value)| key.parse::<AzureConfigKey>().map(|k| (k, value)).ok())
        .collect();
    let storage =
        ObjectStorage::new_azure(account, container, prefix, credentials, Some(config))
            .await?;
    Ok(Arc::new(storage))
}

#[cfg(feature = "object-store-gcs")]
pub fn new_gcs_storage(
    bucket: String,
    prefix: Option<String>,
    credentials: Option<GcsCredentials>,
    config: Option<HashMap<String, String>>,
) -> StorageResult<Arc<dyn Storage + Send + Sync>> {
    let config = config
        .unwrap_or_default()
        .into_iter()
        .filter_map(|(key, value)| {
            key.parse::<GoogleConfigKey>().map(|k| (k, value)).ok()
        })
        .collect();
    let storage = ObjectStorage::new_gcs(bucket, prefix, credentials, Some(config))?;
    Ok(Arc::new(storage))
}

#[cfg(test)]
mod tests {

    use std::{cmp::min, collections::HashSet};

    use super::*;
    use itertools::Itertools as _;
    use proptest::prelude::*;

    #[cfg(feature = "object-store-fs")]
    #[icechunk_macros::tokio_test]
    async fn test_is_clean() {
        use std::{fs::File, io::Write as _, path::PathBuf};
        use tempfile::TempDir;

        let repo_dir = TempDir::new().unwrap();
        let s = new_local_filesystem_storage(repo_dir.path()).await.unwrap();
        assert!(s.root_is_clean(&Settings::default()).await.unwrap());

        let mut file = File::create(repo_dir.path().join("foo.txt")).unwrap();
        write!(file, "hello").unwrap();
        assert!(!s.root_is_clean(&Settings::default()).await.unwrap());

        let inside_existing =
            PathBuf::from_iter([repo_dir.path().as_os_str().to_str().unwrap(), "foo"]);
        let s = new_local_filesystem_storage(&inside_existing).await.unwrap();
        assert!(s.root_is_clean(&Settings::default()).await.unwrap());
    }

    #[cfg(feature = "object-store-gcs")]
    #[icechunk_macros::tokio_test]
    /// Regression test: we can deserialize a GCS credential with token
    async fn test_gcs_session_serialization() {
        use crate::config::{GcsBearerCredential, GcsStaticCredentials};

        let storage = new_gcs_storage(
            "bucket".to_string(),
            Some("prefix".to_string()),
            Some(GcsCredentials::Static(GcsStaticCredentials::BearerToken(
                GcsBearerCredential {
                    bearer: "the token".to_string(),
                    expires_after: None,
                },
            ))),
            None,
        )
        .unwrap();
        let bytes = rmp_serde::to_vec(&storage).unwrap();
        let dese: Result<Arc<dyn Storage>, _> = rmp_serde::from_slice(&bytes);
        assert!(dese.is_ok());
    }

    proptest! {
        #![proptest_config(ProptestConfig {
            cases: 999, .. ProptestConfig::default()
        })]

        #[icechunk_macros::test]
        fn test_split_equal_requests(offset in 0..1_000_000u64, size in 1..3_000_000_000u64, part_size in 1..16_000_000u64, max_parts in 1..100u16 ) {
            let res: Vec<_> = split_in_multiple_equal_requests(&(offset..offset+size), part_size, max_parts).collect();
            // there is always at least 1 request
            prop_assert!(!res.is_empty());

            // it does as many requests as possible
            prop_assert!(res.len() as u64 >= min(max_parts as u64, size / part_size));

            // there are never more than max_parts requests
            prop_assert!(res.len() <= max_parts as usize);

            // the request sizes add up to total size
            prop_assert_eq!(res.iter().map(|range| range.end - range.start).sum::<u64>(), size);

            let sizes: Vec<_> = res.iter().map(|range| range.end - range.start).collect();
            if sizes.len() > 1 {
                // all but last request have the same size
                assert_eq!(sizes.iter().rev().skip(1).unique().count(), 1);

                // last element is smaller or equal
                assert!(sizes.last().unwrap() <= sizes.first().unwrap());
            }

            // we split as much as possible
            assert!(res.len() >= min((size / part_size) as usize, max_parts as usize) );

            // there are no holes in the requests, nor bytes that are requested more than once
            let mut iter = res.iter();
            iter.next();
            prop_assert!(res.iter().zip(iter).all(|(r1,r2)| r1.end == r2.start));
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig {
            cases: 999, .. ProptestConfig::default()
        })]
        #[icechunk_macros::test]
        fn test_split_requests(offset in 0..1_000_000u64, size in 1..3_000_000_000u64, part_size in 1..16_000_000u64, max_parts in 1..100u16 ) {
            let res: Vec<_> = split_in_multiple_requests(&(offset..offset+size), part_size, max_parts).collect();

            // there is always at least 1 request
            prop_assert!(!res.is_empty());

            // it does as many requests as possible
            prop_assert!(res.len() as u64 >= min(max_parts as u64, size / part_size));

            // there are never more than max_parts requests
            prop_assert!(res.len() <= max_parts as usize);

            // the request sizes add up to total size
            prop_assert_eq!(res.iter().map(|range| range.end - range.start).sum::<u64>(), size);

            // there are only two request sizes
            let sizes: HashSet<_> = res.iter().map(|range| range.end - range.start).collect();
            prop_assert!(sizes.len() <= 2); // only last element is smaller
            if sizes.len() > 1 {
                // the smaller request size is one less than the big ones
                prop_assert_eq!(sizes.iter().min().unwrap() + 1, *sizes.iter().max().unwrap() );
            }

            // we split as much as possible
            assert!(res.len() >= min((size / part_size) as usize, max_parts as usize) );

            // there are no holes in the requests, nor bytes that are requested more than once
            let mut iter = res.iter();
            iter.next();
            prop_assert!(res.iter().zip(iter).all(|(r1,r2)| r1.end == r2.start));
        }

    }

    #[icechunk_macros::test]
    fn test_split_examples() {
        assert_eq!(
            split_in_multiple_requests(&(0..4), 4, 100,).collect::<Vec<_>>(),
            vec![0..4]
        );
        assert_eq!(
            split_in_multiple_requests(&(10..14), 4, 100,).collect::<Vec<_>>(),
            vec![10..14]
        );
        assert_eq!(
            split_in_multiple_requests(&(20..23), 1, 100,).collect::<Vec<_>>(),
            vec![(20..21), (21..22), (22..23),]
        );
        assert_eq!(
            split_in_multiple_requests(&(10..16), 5, 100,).collect::<Vec<_>>(),
            vec![(10..13), (13..16)]
        );
        assert_eq!(
            split_in_multiple_requests(&(10..21), 5, 100,).collect::<Vec<_>>(),
            vec![(10..14), (14..18), (18..21)]
        );
        assert_eq!(
            split_in_multiple_requests(&(0..13), 5, 2,).collect::<Vec<_>>(),
            vec![(0..7), (7..13)]
        );
        assert_eq!(
            split_in_multiple_requests(&(0..100), 5, 3,).collect::<Vec<_>>(),
            vec![(0..34), (34..67), (67..100)]
        );
        // this is data from a real example
        assert_eq!(
            split_in_multiple_requests(&(0..19579213), 12_000_000, 18)
                .collect::<Vec<_>>(),
            vec![(0..9789607), (9789607..19579213)]
        );
    }
}
