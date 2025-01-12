use ::futures::{pin_mut, Stream, TryStreamExt};
use itertools::Itertools;
use std::{collections::BTreeMap, convert::Infallible, ops::Bound, sync::Arc};
use thiserror::Error;

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::storage::ETag;

use super::{
    format_constants, ChunkId, ChunkIndices, ChunkLength, ChunkOffset,
    IcechunkFormatError, IcechunkFormatVersion, IcechunkResult, ManifestId, NodeId,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManifestExtents(pub Vec<ChunkIndices>);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManifestRef {
    pub object_id: ManifestId,
    pub extents: ManifestExtents,
}

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum VirtualReferenceError {
    #[error("no virtual chunk container can handle the chunk location ({0})")]
    NoContainerForUrl(String),
    #[error("error parsing virtual ref URL {0}")]
    CannotParseUrl(#[from] url::ParseError),
    #[error("invalid credentials for virtual reference of type {0}")]
    InvalidCredentials(String),
    #[error("virtual reference has no path segments {0}")]
    NoPathSegments(String),
    #[error("unsupported scheme for virtual chunk refs: {0}")]
    UnsupportedScheme(String),
    #[error("error parsing bucket name from virtual ref URL {0}")]
    CannotParseBucketName(String),
    #[error("error fetching virtual reference {0}")]
    FetchError(Box<dyn std::error::Error + Send + Sync>),
    #[error("the checksum of the object owning the virtual chunk has changed ({0})")]
    ObjectModified(String),
    #[error("error parsing virtual reference {0}")]
    OtherError(#[from] Box<dyn std::error::Error + Send + Sync>),
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct VirtualChunkLocation(pub String);

impl VirtualChunkLocation {
    pub fn from_absolute_path(
        path: &str,
    ) -> Result<VirtualChunkLocation, VirtualReferenceError> {
        // make sure we can parse the provided URL before creating the enum
        // TODO: consider other validation here.
        let url = url::Url::parse(path)?;
        let scheme = url.scheme();
        let new_path: String = url
            .path_segments()
            .ok_or(VirtualReferenceError::NoPathSegments(path.into()))?
            // strip empty segments here, object_store cannot handle them.
            .filter(|x| !x.is_empty())
            .join("/");

        let host = if let Some(host) = url.host() {
            host.to_string()
        } else if scheme == "file" {
            "".to_string()
        } else {
            return Err(VirtualReferenceError::CannotParseBucketName(path.into()));
        };

        let location = format!("{}://{}/{}", scheme, host, new_path,);

        Ok(VirtualChunkLocation(location))
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct SecondsSinceEpoch(pub u32);

#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Checksum {
    LastModified(SecondsSinceEpoch),
    ETag(ETag),
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct VirtualChunkRef {
    pub location: VirtualChunkLocation,
    pub offset: ChunkOffset,
    pub length: ChunkLength,
    pub checksum: Option<Checksum>,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ChunkRef {
    pub id: ChunkId,
    pub offset: ChunkOffset,
    pub length: ChunkLength,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[non_exhaustive]
pub enum ChunkPayload {
    Inline(Bytes),
    Virtual(VirtualChunkRef),
    Ref(ChunkRef),
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct ChunkInfo {
    pub node: NodeId,
    pub coord: ChunkIndices,
    pub payload: ChunkPayload,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Default)]
pub struct Manifest {
    pub icechunk_manifest_format_version: IcechunkFormatVersion,
    pub icechunk_manifest_format_flags: BTreeMap<String, rmpv::Value>,
    chunks: BTreeMap<NodeId, BTreeMap<ChunkIndices, ChunkPayload>>,
}

impl Manifest {
    pub fn get_chunk_payload(
        &self,
        node: &NodeId,
        coord: &ChunkIndices,
    ) -> IcechunkResult<&ChunkPayload> {
        self.chunks.get(node).and_then(|m| m.get(coord)).ok_or_else(|| {
            // FIXME: error
            IcechunkFormatError::ChunkCoordinatesNotFound { coords: ChunkIndices(vec![]) }
        })
    }

    pub fn iter(
        self: Arc<Self>,
        node: NodeId,
    ) -> impl Iterator<Item = (ChunkIndices, ChunkPayload)> {
        PayloadIterator { manifest: self, for_node: node, last_key: None }
    }

    pub fn new(chunks: BTreeMap<NodeId, BTreeMap<ChunkIndices, ChunkPayload>>) -> Self {
        Self {
            chunks,
            icechunk_manifest_format_version:
                format_constants::LATEST_ICECHUNK_SPEC_VERSION_BINARY,
            icechunk_manifest_format_flags: Default::default(),
        }
    }

    pub async fn from_stream<E>(
        chunks: impl Stream<Item = Result<ChunkInfo, E>>,
    ) -> Result<Self, E> {
        let mut chunk_map: BTreeMap<NodeId, BTreeMap<ChunkIndices, ChunkPayload>> =
            BTreeMap::new();
        pin_mut!(chunks);
        while let Some(chunk) = chunks.try_next().await? {
            // This could be done with BTreeMap.entry instead, but would require cloning both keys
            match chunk_map.get_mut(&chunk.node) {
                Some(m) => {
                    m.insert(chunk.coord, chunk.payload);
                }
                None => {
                    chunk_map.insert(
                        chunk.node,
                        BTreeMap::from([(chunk.coord, chunk.payload)]),
                    );
                }
            };
        }
        Ok(Self::new(chunk_map))
    }

    /// Used for tests
    pub async fn from_iter<T: IntoIterator<Item = ChunkInfo>>(
        iter: T,
    ) -> Result<Self, Infallible> {
        Self::from_stream(futures::stream::iter(iter.into_iter().map(Ok))).await
    }

    pub fn chunk_payloads(&self) -> impl Iterator<Item = &ChunkPayload> {
        self.chunks.values().flat_map(|m| m.values())
    }

    pub fn len(&self) -> usize {
        self.chunks.values().map(|m| m.len()).sum()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

struct PayloadIterator {
    manifest: Arc<Manifest>,
    for_node: NodeId,
    last_key: Option<ChunkIndices>,
}

impl Iterator for PayloadIterator {
    type Item = (ChunkIndices, ChunkPayload);

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(map) = self.manifest.chunks.get(&self.for_node) {
            match &self.last_key {
                None => {
                    if let Some((coord, payload)) = map.iter().next() {
                        self.last_key = Some(coord.clone());
                        Some((coord.clone(), payload.clone()))
                    } else {
                        None
                    }
                }
                Some(last_key) => {
                    if let Some((coord, payload)) =
                        map.range((Bound::Excluded(last_key), Bound::Unbounded)).next()
                    {
                        self.last_key = Some(coord.clone());
                        Some((coord.clone(), payload.clone()))
                    } else {
                        None
                    }
                }
            }
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {

    use std::error::Error;

    use crate::{format::manifest::ChunkInfo, format::ObjectId};

    use super::*;
    use pretty_assertions::assert_eq;

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

    #[tokio::test]
    async fn test_manifest_chunk_iterator_yields_requested_nodes_only(
    ) -> Result<(), Box<dyn Error>> {
        // This is a regression test for a bug found by hypothesis.
        // Because we use a `.range` query on the HashMap, we have to be careful
        // to not yield chunks from a node that was not requested.
        let mut array_ids = [NodeId::random(), NodeId::random()];
        array_ids.sort();

        // insert with a chunk in the manifest for the array with the larger NodeId
        let chunk1 = ChunkInfo {
            node: array_ids[1].clone(),
            coord: ChunkIndices(vec![0, 0, 0]),
            payload: ChunkPayload::Ref(ChunkRef {
                id: ObjectId::random(),
                offset: 0,
                length: 4,
            }),
        };
        let manifest = Arc::new(Manifest::from_iter(vec![chunk1]).await?);
        let chunks = manifest.iter(array_ids[0].clone()).collect::<Vec<_>>();
        assert_eq!(chunks, vec![]);

        Ok(())
    }
}
