use std::{convert::Infallible, sync::Arc};

use bytes::Bytes;
use futures::{Stream, TryStreamExt};
use itertools::Either;

use crate::format::{
    manifest::{Checksum, ChunkPayload, ChunkRef},
    manifest_generated::generated,
    IcechunkFormatError, IcechunkFormatErrorKind,
};

use super::{
    manifest::{ChunkInfo, SecondsSinceEpoch, VirtualChunkLocation, VirtualChunkRef},
    ChunkId, ChunkIndices, IcechunkResult, NodeId,
};

struct Manifest {
    buffer: Vec<u8>,
    offset: usize,
    len: usize,
}

impl Manifest {
    pub async fn from_stream<E>(
        stream: impl Stream<Item = Result<ChunkInfo, E>>,
    ) -> Result<Option<Self>, E> {
        // TODO: capacity
        let mut builder = flatbuffers::FlatBufferBuilder::with_capacity(4096);

        // FIXME: more than one node id
        let mut node_id = None;
        let refs_vec: Vec<_> = stream
            .map_ok(|chunk| {
                node_id = Some(chunk.node);
                let payload = Some(mk_payload(&mut builder, chunk.payload));
                let index = Some(builder.create_vector(chunk.coord.0.as_slice()));
                generated::ChunkRef::create(
                    &mut builder,
                    &generated::ChunkRefArgs { index, payload },
                )
            })
            .try_collect()
            .await?;

        let node_id = Some(generated::ObjectId8::new(&node_id.unwrap().0));
        let refs = Some(builder.create_vector(refs_vec.as_slice()));
        let array_manifest = generated::ArrayManifest::create(
            &mut builder,
            &generated::ArrayManifestArgs { node_id: node_id.as_ref(), refs },
        );
        let arrays = builder.create_vector(&[array_manifest]);

        let manifest_id = generated::ObjectId12::new(&ChunkId::random().0);

        let manifest = generated::Manifest::create(
            &mut builder,
            &generated::ManifestArgs { id: Some(&manifest_id), arrays: Some(arrays) },
        );

        builder.finish(manifest, Some("Ichk"));
        let (buffer, offset) = builder.collapse();

        Ok(Some(Manifest { buffer, offset, len: refs_vec.len() }))
    }

    /// Used for tests
    pub async fn from_iter<T: IntoIterator<Item = ChunkInfo>>(
        iter: T,
    ) -> Result<Option<Self>, Infallible> {
        Self::from_stream(futures::stream::iter(iter.into_iter().map(Ok))).await
    }

    pub fn len(&self) -> usize {
        self.len
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn root(&self) -> IcechunkResult<generated::Manifest> {
        let res = flatbuffers::root::<generated::Manifest>(&self.buffer[self.offset..])
            .unwrap();
        Ok(res)
    }

    fn lookup_node<'a>(
        &self,
        manifest: generated::Manifest<'a>,
        node: &NodeId,
    ) -> Option<generated::ArrayManifest<'a>> {
        manifest.arrays().lookup_by_key(node.0, |am, id| am.node_id().0.cmp(id))
    }

    fn lookup_ref<'a>(
        &self,
        array_manifest: generated::ArrayManifest<'a>,
        coord: &ChunkIndices,
    ) -> Option<generated::ChunkRef<'a>> {
        array_manifest.refs().lookup_by_key(&coord.0, |chunk_ref, coords| {
            // FIXME: we should be able to compare without building vecs
            chunk_ref.index().iter().collect::<Vec<_>>().cmp(coords)
        })
    }

    pub fn get_chunk_payload(
        &self,
        node: &NodeId,
        coord: &ChunkIndices,
    ) -> IcechunkResult<ChunkPayload> {
        let manifest = self.root()?;
        let chunk_ref = self
            .lookup_node(manifest, node)
            .and_then(|array_manifest| self.lookup_ref(array_manifest, coord))
            .ok_or_else(|| {
                IcechunkFormatError::from(
                    IcechunkFormatErrorKind::ChunkCoordinatesNotFound {
                        coords: coord.clone(),
                    },
                )
            })?;
        let payload = chunk_ref.payload();
        payload_to_payload(payload)
    }

    //pub fn iter(
    //    self: Arc<Self>,
    //    node: NodeId,
    //) -> impl Iterator<Item = (ChunkIndices, ChunkPayload)> {
    //    let manifest = self.root().unwrap();
    //    if let Some(array_manifest) = self.lookup_node(manifest, &node) {
    //        let it = array_manifest.refs().into_iter().map(|r| todo!());
    //        Either::Right(it)
    //    } else {
    //        Either::Left(std::iter::empty())
    //    }
    //}
}

struct PayloadIterator {
    manifest: Arc<Manifest>,
    for_node: NodeId,
    last_ref_index: usize,
}

impl Iterator for PayloadIterator {
    type Item = (ChunkIndices, ChunkPayload);

    fn next(&mut self) -> Option<Self::Item> {
        let manifest = flatbuffers::root::<generated::Manifest>(
            &self.manifest.buffer[self.manifest.offset..],
        )
        .unwrap();
        let array_man = manifest.arrays().get(0);
        let chunk_ref = array_man.refs().get(self.last_ref_index);
        self.last_ref_index += 1;

        todo!()
    }
}

fn payload_to_payload(
    payload: generated::ChunkPayload<'_>,
) -> IcechunkResult<ChunkPayload> {
    if let Some(chunk_id) = payload.chunk_id() {
        let id = ChunkId::new(chunk_id.0);
        Ok(ChunkPayload::Ref(ChunkRef {
            id,
            offset: payload.offset(),
            length: payload.length(),
        }))
    } else if let Some(location) = payload.location() {
        Ok(ChunkPayload::Virtual(VirtualChunkRef {
            location: VirtualChunkLocation::from_absolute_path(location).unwrap(),
            checksum: checksum(&payload),
            offset: payload.offset(),
            length: payload.length(),
        }))
    } else if let Some(data) = payload.inline() {
        Ok(ChunkPayload::Inline(Bytes::copy_from_slice(data.bytes())))
    } else {
        panic!("invalid flatbuffer")
    }
}

fn checksum(payload: &generated::ChunkPayload<'_>) -> Option<Checksum> {
    if let Some(etag) = payload.checksum_etag() {
        Some(Checksum::ETag(etag.to_string()))
    } else if payload.checksum_last_modified() > 0 {
        Some(Checksum::LastModified(SecondsSinceEpoch(payload.checksum_last_modified())))
    } else {
        None
    }
}

fn mk_payload<'bldr>(
    builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
    chunk: ChunkPayload,
) -> flatbuffers::WIPOffset<generated::ChunkPayload<'bldr>> {
    match chunk {
        ChunkPayload::Inline(bytes) => {
            let bytes = builder.create_vector(bytes.as_ref());
            let args =
                generated::ChunkPayloadArgs { inline: Some(bytes), ..Default::default() };
            generated::ChunkPayload::create(builder, &args)
        }
        ChunkPayload::Virtual(virtual_chunk_ref) => {
            let args = generated::ChunkPayloadArgs {
                location: Some(
                    builder.create_string(virtual_chunk_ref.location.0.as_str()),
                ),
                offset: virtual_chunk_ref.offset,
                length: virtual_chunk_ref.length,
                checksum_etag: match &virtual_chunk_ref.checksum {
                    Some(cs) => match cs {
                        Checksum::LastModified(_) => None,
                        Checksum::ETag(etag) => {
                            Some(builder.create_string(etag.as_str()))
                        }
                    },
                    None => None,
                },
                checksum_last_modified: match &virtual_chunk_ref.checksum {
                    Some(cs) => match cs {
                        Checksum::LastModified(seconds) => seconds.0,
                        Checksum::ETag(_) => 0,
                    },
                    None => 0,
                },
                ..Default::default()
            };
            generated::ChunkPayload::create(builder, &args)
        }
        ChunkPayload::Ref(chunk_ref) => {
            let id = generated::ObjectId12::new(&chunk_ref.id.0);
            let args = generated::ChunkPayloadArgs {
                offset: chunk_ref.offset,
                length: chunk_ref.length,
                chunk_id: Some(&id),
                ..Default::default()
            };
            generated::ChunkPayload::create(builder, &args)
        }
    }
}
