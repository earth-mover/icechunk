use futures::{pin_mut, Stream, TryStreamExt};
use itertools::Itertools;
use std::{collections::BTreeMap, ops::Bound, sync::Arc};
use thiserror::Error;

use bytes::Bytes;
use serde::{Deserialize, Serialize};

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
    #[error("error parsing virtual ref URL {0}")]
    CannotParseUrl(#[from] url::ParseError),
    #[error("virtual reference has no path segments {0}")]
    NoPathSegments(String),
    #[error("unsupported scheme for virtual chunk refs: {0}")]
    UnsupportedScheme(String),
    #[error("error parsing bucket name from virtual ref URL {0}")]
    CannotParseBucketName(String),
    #[error("error fetching virtual reference {0}")]
    FetchError(Box<dyn std::error::Error + Send + Sync>),
    #[error("error parsing virtual reference {0}")]
    OtherError(#[from] Box<dyn std::error::Error + Send + Sync>),
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[non_exhaustive]
pub enum VirtualChunkLocation {
    Absolute(String),
    // Relative(prefix_id, String)
}

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

        Ok(VirtualChunkLocation::Absolute(location))
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct VirtualChunkRef {
    pub location: VirtualChunkLocation,
    pub offset: ChunkOffset,
    pub length: ChunkLength,
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
    chunks: BTreeMap<(NodeId, ChunkIndices), ChunkPayload>,
}

impl Manifest {
    pub fn get_chunk_payload(
        &self,
        node: &NodeId,
        coord: ChunkIndices,
    ) -> IcechunkResult<&ChunkPayload> {
        self.chunks.get(&(node.clone(), coord)).ok_or_else(|| {
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

    pub fn new(chunks: BTreeMap<(NodeId, ChunkIndices), ChunkPayload>) -> Self {
        Self {
            chunks,
            icechunk_manifest_format_version:
                format_constants::LATEST_ICECHUNK_MANIFEST_FORMAT,
            icechunk_manifest_format_flags: Default::default(),
        }
    }

    pub async fn from_stream<E>(
        chunks: impl Stream<Item = Result<ChunkInfo, E>>,
    ) -> Result<Self, E> {
        let mut chunk_map = BTreeMap::new();
        pin_mut!(chunks);
        while let Some(chunk) = chunks.try_next().await? {
            chunk_map.insert((chunk.node, chunk.coord), chunk.payload);
        }
        Ok(Self::new(chunk_map))
    }

    pub fn chunks(&self) -> &BTreeMap<(NodeId, ChunkIndices), ChunkPayload> {
        &self.chunks
    }

    pub fn len(&self) -> usize {
        self.chunks.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl FromIterator<ChunkInfo> for Manifest {
    fn from_iter<T: IntoIterator<Item = ChunkInfo>>(iter: T) -> Self {
        let chunks = iter
            .into_iter()
            .map(|chunk| ((chunk.node, chunk.coord), chunk.payload))
            .collect();
        Self::new(chunks)
    }
}

struct PayloadIterator {
    manifest: Arc<Manifest>,
    for_node: NodeId,
    last_key: Option<(NodeId, ChunkIndices)>,
}

impl Iterator for PayloadIterator {
    type Item = (ChunkIndices, ChunkPayload);

    fn next(&mut self) -> Option<Self::Item> {
        match &self.last_key {
            None => {
                if let Some((k @ (_, coord), payload)) = self
                    .manifest
                    .chunks
                    .range((
                        Bound::Included((self.for_node.clone(), ChunkIndices(vec![]))),
                        Bound::Unbounded,
                    ))
                    .next()
                {
                    self.last_key = Some(k.clone());
                    Some((coord.clone(), payload.clone()))
                } else {
                    None
                }
            }
            Some(last_key) => {
                if let Some((k @ (_, coord), payload)) = self
                    .manifest
                    .chunks
                    .range((Bound::Excluded(last_key), Bound::Unbounded))
                    .next()
                {
                    if k.0 == self.for_node {
                        self.last_key = Some(k.clone());
                        Some((coord.clone(), payload.clone()))
                    } else {
                        None
                    }
                } else {
                    None
                }
            }
        }
    }
}
