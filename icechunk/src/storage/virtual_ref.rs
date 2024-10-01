use crate::format::manifest::{VirtualChunkLocation, VirtualReferenceError};
use crate::format::ByteRange;
use async_trait::async_trait;
use bytes::Bytes;
use object_store::local::LocalFileSystem;
use object_store::{
    aws::AmazonS3Builder, path::Path as ObjectPath, GetOptions, GetRange, ObjectStore,
};
use std::cmp::{max, min};
use std::collections::HashMap;
use std::fmt::Debug;
use std::ops::Bound;
use std::sync::Arc;
use tokio::sync::RwLock;
use url;

#[async_trait]
pub trait VirtualChunkResolver: Debug {
    async fn fetch_chunk(
        &self,
        location: &VirtualChunkLocation,
        range: &ByteRange,
    ) -> Result<Bytes, VirtualReferenceError>;
}

#[derive(PartialEq, Eq, Hash, Clone, Debug)]
struct StoreCacheKey(String, String);

#[derive(Debug, Default)]
pub struct ObjectStoreVirtualChunkResolver {
    stores: RwLock<HashMap<StoreCacheKey, Arc<dyn ObjectStore>>>,
}

// Converts the requested ByteRange to a valid ByteRange appropriate
// to the chunk reference of known `offset` and `length`.
pub fn construct_valid_byte_range(
    request: &ByteRange,
    chunk_offset: u64,
    chunk_length: u64,
) -> ByteRange {
    // TODO: error for offset<0
    // TODO: error if request.start > offset + length
    // FIXME: we allow creating a ByteRange(start, end) where end < start
    let new_offset = match request.0 {
        Bound::Unbounded => chunk_offset,
        Bound::Included(start) => max(start, 0) + chunk_offset,
        Bound::Excluded(start) => max(start, 0) + chunk_offset + 1,
    };
    request.length().map_or(
        ByteRange(
            Bound::Included(new_offset),
            Bound::Excluded(chunk_offset + chunk_length),
        ),
        |reqlen| {
            ByteRange(
                Bound::Included(new_offset),
                // no request can go past offset + length, so clamp it
                Bound::Excluded(min(new_offset + reqlen, chunk_offset + chunk_length)),
            )
        },
    )
}

#[async_trait]
impl VirtualChunkResolver for ObjectStoreVirtualChunkResolver {
    async fn fetch_chunk(
        &self,
        location: &VirtualChunkLocation,
        range: &ByteRange,
    ) -> Result<Bytes, VirtualReferenceError> {
        let VirtualChunkLocation::Absolute(location) = location;
        let parsed =
            url::Url::parse(location).map_err(VirtualReferenceError::CannotParseUrl)?;
        let path = ObjectPath::parse(parsed.path())
            .map_err(|e| VirtualReferenceError::OtherError(Box::new(e)))?;
        let scheme = parsed.scheme();

        let bucket_name = if let Some(host) = parsed.host_str() {
            host.to_string()
        } else if scheme == "file" {
            // Host is not required for file scheme, if it is not there,
            // we can assume the bucket name is empty and it is a local file
            "".to_string()
        } else {
            Err(VirtualReferenceError::CannotParseBucketName(
                "No bucket name found".to_string(),
            ))?
        };

        let cache_key = StoreCacheKey(scheme.into(), bucket_name);

        let options =
            GetOptions { range: Option::<GetRange>::from(range), ..Default::default() };
        let store = {
            let stores = self.stores.read().await;
            stores.get(&cache_key).cloned()
        };
        let store = match store {
            Some(store) => store,
            None => {
                let new_store: Arc<dyn ObjectStore> = match scheme {
                    "file" => {
                        let fs = LocalFileSystem::new();
                        Arc::new(fs)
                    }
                    // FIXME: allow configuring auth for virtual references
                    "s3" => Arc::new(
                        AmazonS3Builder::from_env()
                            .with_bucket_name(&cache_key.1)
                            .build()
                            .map_err(|e| {
                                VirtualReferenceError::FetchError(Box::new(e))
                            })?,
                    ),
                    _ => {
                        Err(VirtualReferenceError::UnsupportedScheme(scheme.to_string()))?
                    }
                };
                {
                    self.stores
                        .write()
                        .await
                        .insert(cache_key.clone(), Arc::clone(&new_store));
                }
                new_store
            }
        };
        Ok(store
            .get_opts(&path, options)
            .await
            .map_err(|e| VirtualReferenceError::FetchError(Box::new(e)))?
            .bytes()
            .await
            .map_err(|e| VirtualReferenceError::FetchError(Box::new(e)))?)
    }
}

#[cfg(test)]
mod tests {
    use proptest::prop_assert_eq;
    use test_strategy::proptest;

    use super::*;

    #[test]
    fn test_virtual_chunk_location_bad() {
        // errors relative chunk location
        assert!(matches!(
            VirtualChunkLocation::from_absolute_path("abcdef"),
            Err(VirtualReferenceError::CannotParseUrl(_)),
        ));
        // extra / prevents bucket name detection
        assert!(matches!(
            VirtualChunkLocation::from_absolute_path("s3:///foo/path"),
            Err(VirtualReferenceError::CannotParseBucketName(_)),
        ));
    }

    #[proptest]
    fn test_properties_construct_valid_byte_range(
        #[strategy(0..10u64)] offset: u64,
        // TODO: generate valid offsets using offset, length as input
        #[strategy(3..100u64)] length: u64,
        #[strategy(0..=2u64)] request_offset: u64,
    ) {
        // no request can go past this
        let max_end = offset + length;

        // TODO: more property tests:
        // inputs: (1) chunk_ref: offset, length
        //         (2) requested_range
        // properties: output.length() <= actual_range.length()
        //             output.length() == requested.length()
        //             output.0 >= chunk_ref.offset
        prop_assert_eq!(
            construct_valid_byte_range(
                &ByteRange(Bound::Included(0), Bound::Excluded(length)),
                offset,
                length,
            ),
            ByteRange(Bound::Included(offset), Bound::Excluded(max_end))
        );
        prop_assert_eq!(
            construct_valid_byte_range(
                &ByteRange(Bound::Unbounded, Bound::Excluded(length)),
                offset,
                length
            ),
            ByteRange(Bound::Included(offset), Bound::Excluded(max_end))
        );
        prop_assert_eq!(
            construct_valid_byte_range(
                &ByteRange(Bound::Included(request_offset), Bound::Excluded(max_end)),
                offset,
                length
            ),
            ByteRange(Bound::Included(request_offset + offset), Bound::Excluded(max_end))
        );
        prop_assert_eq!(
            construct_valid_byte_range(&ByteRange::ALL, offset, length),
            ByteRange(Bound::Included(offset), Bound::Excluded(max_end))
        );
        prop_assert_eq!(
            construct_valid_byte_range(
                &ByteRange(Bound::Excluded(request_offset), Bound::Unbounded),
                offset,
                length
            ),
            ByteRange(
                Bound::Included(offset + request_offset + 1),
                Bound::Excluded(max_end)
            )
        );
    }
}
