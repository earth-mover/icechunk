use std::{collections::BTreeMap, ops::Bound, sync::Arc};

use bytes::Bytes;
use rkyv::{from_bytes, to_bytes, Archive, CheckBytes, Deserialize, Serialize};

use super::{
    ChunkIndices, Flags, IcechunkFormatError, IcechunkResult, NodeId, ObjectId,
    TableRegion,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManifestExtents(pub Vec<ChunkIndices>);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManifestRef {
    pub object_id: ObjectId,
    pub location: TableRegion,
    pub flags: Flags,
    pub extents: ManifestExtents,
}

#[derive(
    Clone,
    Debug,
    Hash,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    Archive,
    CheckBytes,
)]
#[archive_attr(derive(rkyv::CheckBytes))]
pub struct VirtualChunkRef {
    location: String, // FIXME: better type
    offset: u64,
    length: u64,
}

#[derive(
    Clone,
    Debug,
    Hash,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    Archive,
    CheckBytes,
)]
#[archive_attr(derive(rkyv::CheckBytes))]
pub struct ChunkRef {
    pub id: ObjectId,
    pub offset: u64,
    pub length: u64,
}

#[derive(
    Clone,
    Debug,
    Hash,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    Archive,
    CheckBytes,
)]
#[archive_attr(derive(rkyv::CheckBytes))]
#[repr(i8)]
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

#[derive(Debug, PartialEq, Serialize, Deserialize, Archive, CheckBytes, Default)]
#[archive_attr(derive(CheckBytes))]
pub struct ManifestsTable {
    pub chunks: BTreeMap<(NodeId, ChunkIndices), ChunkPayload>,
}

impl ManifestsTable {
    pub fn from_bytes(b: Bytes) -> IcechunkResult<Self> {
        from_bytes(b.as_ref()).map_err(|_e|
            //FIXME
            IcechunkFormatError::InvalidArrayManifest {
                index: 0,
                field: "unknown".to_string(),
                message: "rkyv deserialization issue".to_string(),
            })
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        //FIXME: awful performance
        to_bytes::<_, 1024>(self).unwrap().to_vec()
    }

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
                let mut it = self.manifest.chunks.range((
                    Bound::Included((self.for_node, ChunkIndices(vec![]))),
                    Bound::Excluded((self.for_node + 1, ChunkIndices(vec![]))),
                ));

                self.last_key = it.next().map(|kv| kv.0).cloned();
                self.next()
            }
            Some(last_key) => self
                .manifest
                .chunks
                .range((Bound::Included(last_key), Bound::Included(last_key)))
                .next()
                .map(|((_, coord), payload)| (coord.clone(), payload.clone())),
        }
    }
}

impl ChunkIndices {}

//#[cfg(test)]
//#[allow(clippy::panic, clippy::unwrap_used, clippy::expect_used)]
//mod tests {
//    use std::convert::Infallible;
//
//    use super::*;
//    use pretty_assertions::assert_eq;
//    use proptest::prelude::*;
//
//    proptest! {
//
//        #[test]
//        fn coordinate_encoding_roundtrip(v: Vec<u64>) {
//            let arr = ChunkIndices(v);
//            let as_vec: Vec<u8> = (&arr).into();
//            let roundtrip = ChunkIndices::try_from_slice(arr.0.len(), as_vec.as_slice()).unwrap();
//            assert_eq!(arr, roundtrip);
//        }
//    }
//
//    #[tokio::test]
//    async fn test_get_chunk_info() -> Result<(), Box<dyn std::error::Error>> {
//        let c1a = ChunkInfo {
//            node: 1,
//            coord: ChunkIndices(vec![0, 0, 0]),
//            payload: ChunkPayload::Ref(ChunkRef {
//                id: ObjectId::random(),
//                offset: 0,
//                length: 128,
//            }),
//        };
//        let c2a = ChunkInfo {
//            node: 1,
//            coord: ChunkIndices(vec![0, 0, 1]),
//            payload: ChunkPayload::Ref(ChunkRef {
//                id: ObjectId::random(),
//                offset: 42,
//                length: 43,
//            }),
//        };
//        let c3a = ChunkInfo {
//            node: 1,
//            coord: ChunkIndices(vec![1, 0, 1]),
//            payload: ChunkPayload::Ref(ChunkRef {
//                id: ObjectId::random(),
//                offset: 9999,
//                length: 1,
//            }),
//        };
//
//        let c1b = ChunkInfo {
//            node: 2,
//            coord: ChunkIndices(vec![0, 0, 0]),
//            payload: ChunkPayload::Inline("hello".into()),
//        };
//
//        let c1c = ChunkInfo {
//            node: 2,
//            coord: ChunkIndices(vec![0, 0, 0]),
//            payload: ChunkPayload::Virtual(VirtualChunkRef {
//                location: "s3://foo.bar".to_string(),
//                offset: 99,
//                length: 100,
//            }),
//        };
//
//        let table = mk_manifests_table::<Infallible>(
//            futures::stream::iter(vec![
//                c1a.clone(),
//                c2a.clone(),
//                c3a.clone(),
//                c1b.clone(),
//                c1c.clone(),
//            ])
//            .map(Ok),
//        )
//        .await
//        .map_err(|e| format!("error creating manifest {e:?}"))?;
//
//        let res = table.get_chunk_info(&ChunkIndices(vec![0, 0, 0]), &TableRegion(1, 3));
//        assert_eq!(
//            res.as_ref(),
//            Err(&IcechunkFormatError::ChunkCoordinatesNotFound {
//                coords: ChunkIndices(vec![0, 0, 0])
//            })
//        );
//        let res = table.get_chunk_info(&ChunkIndices(vec![0, 0, 0]), &TableRegion(0, 3));
//        assert_eq!(res.as_ref(), Ok(&c1a));
//        let res = table.get_chunk_info(&ChunkIndices(vec![0, 0, 0]), &TableRegion(0, 1));
//        assert_eq!(res.as_ref(), Ok(&c1a));
//
//        let res = table.get_chunk_info(&ChunkIndices(vec![0, 0, 1]), &TableRegion(2, 3));
//        assert_eq!(
//            res.as_ref(),
//            Err(&IcechunkFormatError::ChunkCoordinatesNotFound {
//                coords: ChunkIndices(vec![0, 0, 1])
//            })
//        );
//        let res = table.get_chunk_info(&ChunkIndices(vec![0, 0, 1]), &TableRegion(0, 3));
//        assert_eq!(res.as_ref(), Ok(&c2a));
//        let res = table.get_chunk_info(&ChunkIndices(vec![0, 0, 1]), &TableRegion(0, 2));
//        assert_eq!(res.as_ref(), Ok(&c2a));
//        let res = table.get_chunk_info(&ChunkIndices(vec![0, 0, 1]), &TableRegion(1, 3));
//        assert_eq!(res.as_ref(), Ok(&c2a));
//
//        let res = table.get_chunk_info(&ChunkIndices(vec![1, 0, 1]), &TableRegion(0, 3));
//        assert_eq!(res.as_ref(), Ok(&c3a));
//        let res = table.get_chunk_info(&ChunkIndices(vec![1, 0, 1]), &TableRegion(1, 3));
//        assert_eq!(res.as_ref(), Ok(&c3a));
//        let res = table.get_chunk_info(&ChunkIndices(vec![1, 0, 1]), &TableRegion(2, 3));
//        assert_eq!(res.as_ref(), Ok(&c3a));
//
//        let res = table.get_chunk_info(&ChunkIndices(vec![0, 0, 0]), &TableRegion(3, 4));
//        assert_eq!(res.as_ref(), Ok(&c1b));
//
//        let res = table.get_chunk_info(&ChunkIndices(vec![0, 0, 0]), &TableRegion(4, 5));
//        assert_eq!(res.as_ref(), Ok(&c1c));
//        Ok(())
//    }
//}
