use std::{collections::BTreeMap, mem::size_of, ops::Bound, sync::Arc};

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

#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct VirtualChunkRef {
    location: String, // FIXME: better type
    offset: u64,
    length: u64,
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
pub struct ManifestsTable {
    pub chunks: BTreeMap<(NodeId, ChunkIndices), ChunkPayload>,
}

impl ManifestsTable {
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

    pub fn estimated_size_bytes(&self) -> usize {
        // FIXME: this is really bad
        self.chunks.len()
            * (
                size_of::<(NodeId, ChunkIndices)>() //keys
                + size_of::<ChunkPayload>()  //values
                + 3 * size_of::<u64>()  // estimated number of coordinates
        + 20
                // estimated dynamic size of payload
            )
    }
}

impl FromIterator<ChunkInfo> for ManifestsTable {
    fn from_iter<T: IntoIterator<Item = ChunkInfo>>(iter: T) -> Self {
        let chunks = iter
            .into_iter()
            .map(|chunk| ((chunk.node, chunk.coord), chunk.payload))
            .collect();
        ManifestsTable { chunks }
    }
}

struct PayloadIterator {
    manifest: Arc<ManifestsTable>,
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
