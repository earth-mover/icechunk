use crate::format::{manifest::VirtualChunkLocation, ByteRange};
use crate::storage::StorageResult;
use async_trait::async_trait;
use bytes::Bytes;

#[async_trait]
pub trait VirtualChunkResolver {
    async fn fetch_chunk(
        &self,
        location: &VirtualChunkLocation,
        range: &ByteRange,
    ) -> StorageResult<Bytes>;
}
