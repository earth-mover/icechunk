use itertools::Itertools;
use std::{collections::BTreeMap, ops::Bound, sync::Arc};
use thiserror::Error;

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use super::{ChunkIndices, Flags, IcechunkFormatError, IcechunkResult, NodeId, ObjectId};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManifestExtents(pub Vec<ChunkIndices>);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManifestRef {
    pub object_id: ObjectId,
    pub flags: Flags,
    pub extents: ManifestExtents,
}

#[derive(Debug, Error)]
pub enum VirtualReferenceError {
    #[error("error parsing virtual ref URL {0}")]
    CannotParseUrl(#[from] url::ParseError),
    #[error("virtual reference has no path segments {0}")]
    NoPathSegments(String),
    #[error("unsupported scheme for virtual chunk refs: {0}")]
    UnsupportedScheme(String),
    #[error("error parsing bucket name from virtual ref URL {0}")]
    CannotParseBucketName(String),
    #[error("error parsing virtual reference {0}")]
    OtherError(#[from] Box<dyn std::error::Error + Send + Sync>),
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
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
        let new_path: String = url
            .path_segments()
            .ok_or(VirtualReferenceError::NoPathSegments(path.into()))?
            // strip empty segments here, object_store cannot handle them.
            .filter(|x| !x.is_empty())
            .join("/");

        let host = url
            .host()
            .ok_or_else(|| VirtualReferenceError::CannotParseBucketName(path.into()))?;
        Ok(VirtualChunkLocation::Absolute(format!(
            "{}://{}/{}",
            url.scheme(),
            host,
            new_path,
        )))
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct VirtualChunkRef {
    pub location: VirtualChunkLocation,
    pub offset: u64,
    pub length: u64,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ChunkRef {
    pub id: ObjectId,
    pub offset: u64,
    pub length: u64,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum ChunkPayload {
    Inline(Bytes), // FIXME: optimize copies
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
    pub chunks: BTreeMap<(NodeId, ChunkIndices), ChunkPayload>,
}

impl Manifest {
    pub fn get_chunk_payload(
        &self,
        node: NodeId,
        coord: ChunkIndices,
    ) -> IcechunkResult<&ChunkPayload> {
        self.chunks.get(&(node, coord)).ok_or_else(|| {
            // FIXME: error
            IcechunkFormatError::ChunkCoordinatesNotFound { coords: ChunkIndices(vec![]) }
        })
    }

    pub fn iter(
        self: Arc<Self>,
        node: &NodeId,
    ) -> impl Iterator<Item = (ChunkIndices, ChunkPayload)> {
        PayloadIterator { manifest: self, for_node: *node, last_key: None }
    }
}

impl FromIterator<ChunkInfo> for Manifest {
    fn from_iter<T: IntoIterator<Item = ChunkInfo>>(iter: T) -> Self {
        let chunks = iter
            .into_iter()
            .map(|chunk| ((chunk.node, chunk.coord), chunk.payload))
            .collect();
        Manifest { chunks }
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
                        Bound::Included((self.for_node, ChunkIndices(vec![]))),
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
                    self.last_key = Some(k.clone());
                    Some((coord.clone(), payload.clone()))
                } else {
                    None
                }
            }
        }
    }
}
