use crate::format::manifest::{VirtualChunkLocation, VirtualReferenceError};
use crate::format::ByteRange;
use crate::storage::StorageResult;
use async_trait::async_trait;
use bytes::Bytes;
use object_store::{
    aws::AmazonS3Builder, path::Path as ObjectPath, GetOptions, GetRange, ObjectStore,
};
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;
use tokio::sync::RwLock;
use url;

#[async_trait]
pub trait VirtualChunkResolver: Debug {
    async fn fetch_chunk(
        &self,
        location: &VirtualChunkLocation,
        range: &ByteRange,
    ) -> StorageResult<Bytes>;
}

#[derive(PartialEq, Eq, Hash, Clone, Debug)]
struct StoreCacheKey(String, String);

#[derive(Debug, Default)]
pub struct ObjectStoreVirtualChunkResolver {
    stores: RwLock<HashMap<StoreCacheKey, Arc<dyn ObjectStore>>>,
}

#[async_trait]
impl VirtualChunkResolver for ObjectStoreVirtualChunkResolver {
    async fn fetch_chunk(
        &self,
        location: &VirtualChunkLocation,
        range: &ByteRange,
    ) -> StorageResult<Bytes> {
        let VirtualChunkLocation::Absolute(location) = location;
        let parsed =
            url::Url::parse(location).map_err(VirtualReferenceError::CannotParseUrl)?;
        let bucket_name = parsed
            .host_str()
            .ok_or(VirtualReferenceError::CannotParseBucketName(
                "error parsing bucket name".into(),
            ))?
            .to_string();
        let path = ObjectPath::parse(parsed.path())
            .map_err(|e| VirtualReferenceError::OtherError(Box::new(e)))?;
        let scheme = parsed.scheme();
        let cache_key = StoreCacheKey(scheme.into(), bucket_name);

        let options =
            GetOptions { range: Option::<GetRange>::from(range), ..Default::default() };
        let store = {
            let stores = self.stores.read().await;
            #[allow(clippy::expect_used)]
            stores.get(&cache_key).map(Arc::clone)
        };
        let store = match store {
            Some(store) => store,
            None => {
                let builder = match scheme {
                    // FIXME: allow configuring auth for virtual references
                    "s3" => AmazonS3Builder::from_env(),
                    _ => {
                        Err(VirtualReferenceError::UnsupportedScheme(scheme.to_string()))?
                    }
                };
                let new_store: Arc<dyn ObjectStore> =
                    Arc::new(builder.with_bucket_name(&cache_key.1).build()?);
                {
                    self.stores
                        .write()
                        .await
                        .insert(cache_key.clone(), Arc::clone(&new_store));
                }
                new_store
            }
        };
        Ok(store.get_opts(&path, options).await?.bytes().await?)
    }
}

#[cfg(test)]
mod tests {
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
}
