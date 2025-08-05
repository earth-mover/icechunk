use futures::{Stream, StreamExt, TryStreamExt, stream::BoxStream};
use itertools::Itertools;
use std::{
    cmp::{max, min},
    iter,
    ops::Range,
    sync::Arc,
};

#[cfg(not(target_arch = "wasm32"))]
use crate::config::{AzureCredentials, GcsCredentials, S3Credentials, S3Options};

// Core modules - always available
pub mod errors;
pub mod traits;
pub mod types;

// Implementation modules - conditionally compiled
pub mod implementations;

// Test module
#[cfg(test)]
pub mod logging;

// Re-export core types and traits
pub use errors::*;
pub use traits::Storage;
pub use types::*;

// Conditional re-exports for implementations
#[cfg(not(target_arch = "wasm32"))]
pub use implementations::{ObjectStorage, S3Storage};

/// Split an object request into multiple byte range requests
///
/// Returns tuples of Range for each request.
///
/// It generates requests that are as similar as possible in size, this means no more than 1 byte
/// difference between the requests.
///
/// It tries to generate ceil(size/ideal_req_size) requests, but never exceeds max_requests.
///
/// ideal_req_size and max_requests must be > 0
pub fn split_in_multiple_requests(
    range: &Range<u64>,
    ideal_req_size: u64,
    max_requests: u16,
) -> impl Iterator<Item = Range<u64>> + use<> {
    let size = max(0, range.end - range.start);
    // we do a ceiling division, rounding always up
    let num_parts = size.div_ceil(ideal_req_size);
    // no more than max_parts, so we limit
    let num_parts = max(1, min(num_parts, max_requests as u64));

    // we split the total size into request that are as similar as possible in size
    // this means, we are going to have a few requests that are 1 byte larger
    let big_parts = size % num_parts;
    let small_parts_size = size / num_parts;
    let big_parts_size = small_parts_size + 1;

    iter::successors(Some((1, range.start..range.start)), move |(index, prev_range)| {
        let size = if *index <= big_parts { big_parts_size } else { small_parts_size };
        Some((index + 1, prev_range.end..prev_range.end + size))
    })
    .dropping(1)
    .take(num_parts as usize)
    .map(|(_, range)| range)
}

/// Split an object request into multiple byte range requests ensuring only the last request is
/// smaller
///
/// Returns tuples of Range for each request.
///
/// It tries to generate ceil(size/ideal_req_size) requests, but never exceeds max_requests.
///
/// ideal_req_size and max_requests must be > 0
pub fn split_in_multiple_equal_requests(
    range: &Range<u64>,
    ideal_req_size: u64,
    max_requests: u16,
) -> impl Iterator<Item = Range<u64>> + use<> {
    let size = max(0, range.end - range.start);
    // we do a ceiling division, rounding always up
    let num_parts = size.div_ceil(ideal_req_size);
    // no more than max_parts, so we limit
    let num_parts = max(1, min(num_parts, max_requests as u64));

    let big_parts = num_parts - 1;
    let big_parts_size = size / max(1, big_parts);
    let small_part_size = size - big_parts_size * big_parts;

    iter::successors(Some((1, range.start..range.start)), move |(index, prev_range)| {
        let size = if *index <= big_parts { big_parts_size } else { small_part_size };
        Some((index + 1, prev_range.end..prev_range.end + size))
    })
    .dropping(1)
    .take(num_parts as usize)
    .map(|(_, range)| range)
}

fn convert_list_item<Id>(item: ListInfo<String>) -> Option<ListInfo<Id>>
where
    Id: for<'b> TryFrom<&'b str>,
{
    let id = Id::try_from(item.id.as_str()).ok()?;
    let created_at = item.created_at;
    Some(ListInfo { created_at, id, size_bytes: item.size_bytes })
}

fn translate_list_infos<'a, Id>(
    s: impl Stream<Item = StorageResult<ListInfo<String>>> + Send + 'a,
) -> BoxStream<'a, StorageResult<ListInfo<Id>>>
where
    Id: for<'b> TryFrom<&'b str> + Send + std::fmt::Debug + 'a,
{
    s.try_filter_map(|info| async move {
        let info = convert_list_item(info);
        if info.is_none() {
            tracing::error!(list_info=?info, "Error processing list item metadata");
        }
        Ok(info)
    })
    .boxed()
}

// Constructor functions - conditionally compiled for non-WASM targets
#[cfg(not(target_arch = "wasm32"))]
pub fn new_s3_storage(
    config: S3Options,
    bucket: String,
    prefix: Option<String>,
    credentials: Option<S3Credentials>,
) -> StorageResult<Arc<dyn Storage>> {
    use implementations::s3::S3Storage;

    if let Some(endpoint) = &config.endpoint_url {
        if endpoint.contains("fly.storage.tigris.dev") {
            return Err(StorageError::from(StorageErrorKind::Other("Tigris Storage is not S3 compatible, use the Tigris specific constructor instead".to_string())));
        }
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

#[cfg(not(target_arch = "wasm32"))]
pub fn new_r2_storage(
    config: S3Options,
    bucket: Option<String>,
    prefix: Option<String>,
    account_id: Option<String>,
    credentials: Option<S3Credentials>,
) -> StorageResult<Arc<dyn Storage>> {
    use implementations::s3::S3Storage;

    let (bucket, prefix) = match (bucket, prefix) {
        (Some(bucket), Some(prefix)) => (bucket, Some(prefix)),
        (None, Some(prefix)) => match prefix.split_once("/") {
            Some((bucket, prefix)) => (bucket.to_string(), Some(prefix.to_string())),
            None => (prefix, None),
        },
        (Some(bucket), None) => (bucket, None),
        (None, None) => {
            return Err(StorageErrorKind::R2ConfigurationError(
                "Either bucket or prefix must be provided.".to_string(),
            )
            .into());
        }
    };

    if config.endpoint_url.is_none() && account_id.is_none() {
        return Err(StorageErrorKind::R2ConfigurationError(
            "Either endpoint_url or account_id must be provided.".to_string(),
        )
        .into());
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

#[cfg(not(target_arch = "wasm32"))]
pub fn new_tigris_storage(
    config: S3Options,
    bucket: String,
    prefix: Option<String>,
    credentials: Option<S3Credentials>,
    use_weak_consistency: bool,
) -> StorageResult<Arc<dyn Storage>> {
    use implementations::s3::S3Storage;

    let config = S3Options {
        endpoint_url: Some(
            config.endpoint_url.unwrap_or("https://fly.storage.tigris.dev".to_string()),
        ),
        ..config
    };
    let mut extra_write_headers = Vec::with_capacity(1);
    let mut extra_read_headers = Vec::with_capacity(2);

    if !use_weak_consistency {
        // TODO: Tigris will need more than this to offer good eventually consistent behavior
        // For example: we should use no-cache for branches and config file
        if let Some(region) = config.region.as_ref() {
            extra_write_headers.push(("X-Tigris-Region".to_string(), region.clone()));
            extra_read_headers.push(("X-Tigris-Region".to_string(), region.clone()));
            extra_read_headers
                .push(("Cache-Control".to_string(), "no-cache".to_string()));
        } else {
            return Err(StorageErrorKind::Other("Tigris storage requires a region to provide full consistency. Either set the region for the bucket or use the read-only, eventually consistent storage by passing `use_weak_consistency=True` (experts only)".to_string()).into());
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

// WASM-compatible constructors - always available
pub async fn new_in_memory_storage() -> StorageResult<Arc<dyn Storage>> {
    #[cfg(not(target_arch = "wasm32"))]
    {
        let st = implementations::ObjectStorage::new_in_memory().await?;
        Ok(Arc::new(st))
    }
    #[cfg(target_arch = "wasm32")]
    {
        // For WASM, we'll need a different in-memory implementation
        // This is a placeholder that will be replaced by WASM-specific implementation
        Err(StorageError::from(StorageErrorKind::Other(
            "In-memory storage not yet implemented for WASM".to_string(),
        )))
    }
}

#[cfg(not(target_arch = "wasm32"))]
pub async fn new_local_filesystem_storage(
    path: &std::path::Path,
) -> StorageResult<Arc<dyn Storage>> {
    let st = implementations::ObjectStorage::new_local_filesystem(path).await?;
    Ok(Arc::new(st))
}

#[cfg(not(target_arch = "wasm32"))]
pub async fn new_s3_object_store_storage(
    config: S3Options,
    bucket: String,
    prefix: Option<String>,
    credentials: Option<S3Credentials>,
) -> StorageResult<Arc<dyn Storage>> {
    if let Some(endpoint) = &config.endpoint_url {
        if endpoint.contains("fly.storage.tigris.dev") {
            return Err(StorageError::from(StorageErrorKind::Other("Tigris Storage is not S3 compatible, use the Tigris specific constructor instead".to_string())));
        }
    }
    let storage =
        implementations::ObjectStorage::new_s3(bucket, prefix, credentials, Some(config))
            .await?;
    Ok(Arc::new(storage))
}

#[cfg(not(target_arch = "wasm32"))]
pub async fn new_azure_blob_storage(
    account: String,
    container: String,
    prefix: Option<String>,
    credentials: Option<AzureCredentials>,
    config: Option<std::collections::HashMap<String, String>>,
) -> StorageResult<Arc<dyn Storage>> {
    use object_store::azure::AzureConfigKey;

    let config = config
        .unwrap_or_default()
        .into_iter()
        .filter_map(|(key, value)| key.parse::<AzureConfigKey>().map(|k| (k, value)).ok())
        .collect();
    let storage = implementations::ObjectStorage::new_azure(
        account,
        container,
        prefix,
        credentials,
        Some(config),
    )
    .await?;
    Ok(Arc::new(storage))
}

#[cfg(not(target_arch = "wasm32"))]
pub async fn new_gcs_storage(
    bucket: String,
    prefix: Option<String>,
    credentials: Option<GcsCredentials>,
    config: Option<std::collections::HashMap<String, String>>,
) -> StorageResult<Arc<dyn Storage>> {
    use object_store::gcp::GoogleConfigKey;

    let config = config
        .unwrap_or_default()
        .into_iter()
        .filter_map(|(key, value)| {
            key.parse::<GoogleConfigKey>().map(|k| (k, value)).ok()
        })
        .collect();
    let storage = implementations::ObjectStorage::new_gcs(
        bucket,
        prefix,
        credentials,
        Some(config),
    )
    .await?;
    Ok(Arc::new(storage))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::panic)]
mod tests {
    use std::{collections::HashSet, fs::File, io::Write, path::PathBuf};

    use crate::config::{GcsBearerCredential, GcsStaticCredentials};

    use super::*;
    use icechunk_macros::tokio_test;
    use proptest::prelude::*;
    use tempfile::TempDir;

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio_test]
    async fn test_is_clean() {
        let repo_dir = TempDir::new().unwrap();
        let s = new_local_filesystem_storage(repo_dir.path()).await.unwrap();
        assert!(s.root_is_clean().await.unwrap());

        let mut file = File::create(repo_dir.path().join("foo.txt")).unwrap();
        write!(file, "hello").unwrap();
        assert!(!s.root_is_clean().await.unwrap());

        let inside_existing =
            PathBuf::from_iter([repo_dir.path().as_os_str().to_str().unwrap(), "foo"]);
        let s = new_local_filesystem_storage(&inside_existing).await.unwrap();
        assert!(s.root_is_clean().await.unwrap());
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio_test]
    /// Regression test: we can deserialize a GCS credential with token
    async fn test_gcs_session_serialization() {
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
        .await
        .unwrap();
        let bytes = rmp_serde::to_vec(&storage).unwrap();
        let dese: Result<Arc<dyn Storage>, _> = rmp_serde::from_slice(&bytes);
        assert!(dese.is_ok())
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

            let sizes: Vec<_> = res.iter().map(|range| (range.end - range.start)).collect();
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
            let sizes: HashSet<_> = res.iter().map(|range| (range.end - range.start)).collect();
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
