//! Object store abstraction layer.
//!
//! The [`Storage`] trait defines generic object store operations (get, put, delete,
//! list) for persisting Icechunk data. Constructor functions like [`new_s3_storage`],
//! [`new_gcs_storage`], [`new_in_memory_storage`] create configured storage instances.

use std::sync::Arc;

#[cfg(feature = "redirect")]
use crate::storage::redirect::RedirectStorage;

// Re-export everything from icechunk-storage
pub use icechunk_storage::{
    ConcurrencySettings, DeleteObjectsResult, ETag, Generation, GetModifiedResult,
    ICError, ListInfo, RetriesSettings, Settings, Storage, StorageError,
    StorageErrorKind, StorageResult, TimeoutSettings, VersionInfo, VersionedUpdateResult,
    s3_config::{S3Credentials, S3CredentialsFetcher, S3Options, S3StaticCredentials},
    split_in_multiple_equal_requests, split_in_multiple_requests, strip_quotes,
};

// Re-export from icechunk-s3
#[cfg(feature = "s3")]
pub use icechunk_s3::{
    S3Storage, mk_client, new_r2_storage, new_s3_storage, new_tigris_storage,
    range_to_header,
};

// Re-export from icechunk-arrow-object-store
pub use icechunk_arrow_object_store::{ObjectStorage, new_in_memory_storage};

#[cfg(feature = "object-store-fs")]
pub use icechunk_arrow_object_store::new_local_filesystem_storage;

#[cfg(feature = "object-store-http")]
pub use icechunk_arrow_object_store::new_http_storage;

#[cfg(feature = "object-store-s3")]
pub use icechunk_arrow_object_store::{
    S3ObjectStoreBackend, new_s3_object_store_storage,
};

#[cfg(feature = "object-store-azure")]
pub use icechunk_arrow_object_store::{
    AzureCredentials, AzureObjectStoreBackend, AzureStaticCredentials,
    new_azure_blob_storage,
};

#[cfg(feature = "object-store-gcs")]
pub use icechunk_arrow_object_store::{
    GcsBearerCredential, GcsCredentials, GcsCredentialsFetcher, GcsObjectStoreBackend,
    GcsStaticCredentials, new_gcs_storage,
};

#[cfg(feature = "object-store-http")]
pub use icechunk_arrow_object_store::HttpObjectStoreBackend;

pub use icechunk_arrow_object_store::ObjectStoreBackend;

#[cfg(test)]
pub mod logging;

/// Storage wrapper that adds artificial read/write latency (for testing).
pub mod latency;

/// HTTP redirect-based storage for read-only access.
#[cfg(feature = "redirect")]
pub mod redirect;
/// Shared S3 configuration types (always compiled).
pub mod s3_config;

#[cfg(feature = "redirect")]
pub fn new_redirect_storage(
    base_url: &str,
) -> StorageResult<Arc<dyn Storage + Send + Sync>> {
    use icechunk_types::ICResultExt as _;
    let base_url = url::Url::parse(base_url)
        .map_err(|e| StorageErrorKind::CannotParseUrl {
            cause: e,
            url: base_url.to_string(),
        })
        .capture()?;
    Ok(Arc::new(RedirectStorage::new(base_url)))
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
