use crate::format::manifest::{VirtualChunkLocation, VirtualReferenceError};
use crate::format::ByteRange;
use async_trait::async_trait;
use aws_sdk_s3::Client;
use bytes::Bytes;
use object_store::local::LocalFileSystem;
use object_store::{path::Path as ObjectPath, GetOptions, GetRange, ObjectStore};
use serde::{Deserialize, Serialize};
use std::cmp::{max, min};
use std::fmt::Debug;
use std::ops::Bound;
use tokio::sync::OnceCell;
use url::{self, Url};

use super::s3::{mk_client, range_to_header, S3Config};

#[async_trait]
pub trait VirtualChunkResolver: Debug {
    async fn fetch_chunk(
        &self,
        location: &VirtualChunkLocation,
        range: &ByteRange,
    ) -> Result<Bytes, VirtualReferenceError>;
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum ObjectStoreVirtualChunkResolverConfig {
    S3(S3Config),
}

#[derive(Debug)]
pub struct ObjectStoreVirtualChunkResolver {
    s3: OnceCell<Client>,
    config: Box<Option<ObjectStoreVirtualChunkResolverConfig>>,
}

impl ObjectStoreVirtualChunkResolver {
    pub fn new(config: Option<ObjectStoreVirtualChunkResolverConfig>) -> Self {
        Self { s3: Default::default(), config: Box::new(config) }
    }

    async fn s3(&self) -> &Client {
        let config = self.config.clone();
        self.s3
            .get_or_init(|| async move {
                match config.as_ref() {
                    Some(ObjectStoreVirtualChunkResolverConfig::S3(config)) => {
                        mk_client(Some(config)).await
                    }
                    None => mk_client(None).await,
                }
            })
            .await
    }

    async fn fetch_file(
        &self,
        url: &Url,
        range: &ByteRange,
    ) -> Result<Bytes, VirtualReferenceError> {
        let store = LocalFileSystem::new();
        let options =
            GetOptions { range: Option::<GetRange>::from(range), ..Default::default() };
        let path = ObjectPath::parse(url.path())
            .map_err(|e| VirtualReferenceError::OtherError(Box::new(e)))?;

        store
            .get_opts(&path, options)
            .await
            .map_err(|e| VirtualReferenceError::FetchError(Box::new(e)))?
            .bytes()
            .await
            .map_err(|e| VirtualReferenceError::FetchError(Box::new(e)))
    }

    async fn fetch_s3(
        &self,
        url: &Url,
        range: &ByteRange,
    ) -> Result<Bytes, VirtualReferenceError> {
        let bucket_name = if let Some(host) = url.host_str() {
            host.to_string()
        } else {
            Err(VirtualReferenceError::CannotParseBucketName(
                "No bucket name found".to_string(),
            ))?
        };

        let key = url.path();
        let key = key.strip_prefix('/').unwrap_or(key);
        let mut b = self.s3().await.get_object().bucket(bucket_name).key(key);

        if let Some(header) = range_to_header(range) {
            b = b.range(header)
        };

        Ok(b.send()
            .await
            .map_err(|e| VirtualReferenceError::FetchError(Box::new(e)))?
            .body
            .collect()
            .await
            .map_err(|e| VirtualReferenceError::FetchError(Box::new(e)))?
            .into_bytes())
    }
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
        let scheme = parsed.scheme();

        match scheme {
            "file" => self.fetch_file(&parsed, range).await,
            "s3" => self.fetch_s3(&parsed, range).await,
            _ => Err(VirtualReferenceError::UnsupportedScheme(scheme.to_string())),
        }
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
