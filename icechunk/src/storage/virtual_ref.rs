use crate::format::manifest::{VirtualChunkLocation, VirtualReferenceError};
use crate::format::ByteRange;
use crate::private;
use async_trait::async_trait;
use aws_sdk_s3::Client;
use bytes::Bytes;
use object_store::local::LocalFileSystem;
use object_store::{path::Path as ObjectPath, GetOptions, GetRange, ObjectStore};
use serde::{Deserialize, Serialize};
use std::cmp::min;
use std::fmt::Debug;
use tokio::sync::OnceCell;
use url::{self, Url};

use super::s3::{mk_client, range_to_header, S3Config};

#[async_trait]
pub trait VirtualChunkResolver: Debug + private::Sealed {
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
    match request {
        ByteRange::Bounded(std::ops::Range { start: req_start, end: req_end }) => {
            let new_start =
                min(chunk_offset + req_start, chunk_offset + chunk_length - 1);
            let new_end = min(chunk_offset + req_end, chunk_offset + chunk_length);
            ByteRange::Bounded(new_start..new_end)
        }
        ByteRange::From(n) => {
            let new_start = min(chunk_offset + n, chunk_offset + chunk_length - 1);
            ByteRange::Bounded(new_start..chunk_offset + chunk_length)
        }
        ByteRange::Last(n) => {
            let new_end = chunk_offset + chunk_length;
            let new_start = new_end - n;
            ByteRange::Bounded(new_start..new_end)
        }
    }
}

impl private::Sealed for ObjectStoreVirtualChunkResolver {}

#[async_trait]
impl VirtualChunkResolver for ObjectStoreVirtualChunkResolver {
    async fn fetch_chunk(
        &self,
        location: &VirtualChunkLocation,
        range: &ByteRange,
    ) -> Result<Bytes, VirtualReferenceError> {
        let parsed = url::Url::parse(location.0.as_str())
            .map_err(VirtualReferenceError::CannotParseUrl)?;
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
            construct_valid_byte_range(&ByteRange::Bounded(0..length), offset, length,),
            ByteRange::Bounded(offset..max_end)
        );
        prop_assert_eq!(
            construct_valid_byte_range(
                &ByteRange::Bounded(request_offset..max_end),
                offset,
                length
            ),
            ByteRange::Bounded(request_offset + offset..max_end)
        );
        prop_assert_eq!(
            construct_valid_byte_range(&ByteRange::ALL, offset, length),
            ByteRange::Bounded(offset..offset + length)
        );
        prop_assert_eq!(
            construct_valid_byte_range(&ByteRange::From(request_offset), offset, length),
            ByteRange::Bounded(offset + request_offset..offset + length)
        );
        prop_assert_eq!(
            construct_valid_byte_range(&ByteRange::Last(request_offset), offset, length),
            ByteRange::Bounded(offset + length - request_offset..offset + length)
        );
    }
}
