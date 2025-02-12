use std::{convert::Infallible, io::Read, ops::Range, sync::Arc};

use bytes::Bytes;
use flatbuffers::VerifierOptions;
use futures::{Stream, TryStreamExt};
use itertools::{Either, Itertools};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    error::ICError,
    format::{
        manifest_generated::generated, IcechunkFormatError, IcechunkFormatErrorKind,
    },
    storage::ETag,
};

use super::{
    ChunkId, ChunkIndices, ChunkLength, ChunkOffset, IcechunkResult, ManifestId, NodeId,
};

type ManifestExtents = Range<ChunkIndices>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManifestRef {
    pub object_id: ManifestId,
    pub extents: ManifestExtents,
}

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum VirtualReferenceErrorKind {
    #[error("no virtual chunk container can handle the chunk location ({0})")]
    NoContainerForUrl(String),
    #[error("error parsing virtual ref URL")]
    CannotParseUrl(#[from] url::ParseError),
    #[error("invalid credentials for virtual reference of type {0}")]
    InvalidCredentials(String),
    #[error("virtual reference has no path segments {0}")]
    NoPathSegments(String),
    #[error("unsupported scheme for virtual chunk refs: {0}")]
    UnsupportedScheme(String),
    #[error("error parsing bucket name from virtual ref URL {0}")]
    CannotParseBucketName(String),
    #[error("error fetching virtual reference")]
    FetchError(#[source] Box<dyn std::error::Error + Send + Sync>),
    #[error("the checksum of the object owning the virtual chunk has changed ({0})")]
    ObjectModified(String),
    #[error("error retrieving virtual chunk, not enough data. Expected: ({expected}), available ({available})")]
    InvalidObjectSize { expected: u64, available: u64 },
    #[error("error parsing virtual reference")]
    OtherError(#[from] Box<dyn std::error::Error + Send + Sync>),
}

pub type VirtualReferenceError = ICError<VirtualReferenceErrorKind>;

// it would be great to define this impl in error.rs, but it conflicts with the blanket
// `impl From<T> for T`
impl<E> From<E> for VirtualReferenceError
where
    E: Into<VirtualReferenceErrorKind>,
{
    fn from(value: E) -> Self {
        Self::new(value.into())
    }
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
            .ok_or(VirtualReferenceErrorKind::NoPathSegments(path.into()))?
            // strip empty segments here, object_store cannot handle them.
            .filter(|x| !x.is_empty())
            .join("/");

        let host = if let Some(host) = url.host() {
            host.to_string()
        } else if scheme == "file" {
            "".to_string()
        } else {
            return Err(
                VirtualReferenceErrorKind::CannotParseBucketName(path.into()).into()
            );
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

#[derive(Debug)]
pub struct Manifest {
    buffer: Vec<u8>,
    offset: usize,
    len: usize,
    id: ManifestId,
}

impl Manifest {
    pub fn id(&self) -> &ManifestId {
        &self.id
    }

    pub fn bytes(&self) -> &[u8] {
        &self.buffer.as_slice()[self.offset..]
    }

    pub fn from_read(read: &mut dyn Read) -> Manifest {
        // FIXME
        let mut buffer = Vec::with_capacity(1024 * 1024);
        read.read_to_end(&mut buffer).unwrap();
        let manifest = flatbuffers::root_with_opts::<generated::Manifest>(
            &ROOT_OPTIONS,
            buffer.as_slice(),
        )
        .unwrap();
        let id = ManifestId::new(manifest.id().0);
        // FIXME: first array
        let len = manifest.arrays().get(0).refs().len();
        Manifest { buffer, offset: 0, id, len }
    }

    pub async fn from_stream<E>(
        stream: impl Stream<Item = Result<ChunkInfo, E>>,
    ) -> Result<Option<Self>, E> {
        // TODO: capacity
        let mut builder = flatbuffers::FlatBufferBuilder::with_capacity(4096);

        // FIXME: do somewhere else
        let mut all = stream.try_collect::<Vec<_>>().await?;

        all.sort_by(|a, b| (a.coord).cmp(&b.coord));

        // FIXME: more than one node id
        let mut node_id = None;
        let refs_vec: Vec<_> = all
            .into_iter()
            .map(|chunk| {
                node_id = Some(chunk.node.clone());
                mk_chunk_ref(&mut builder, chunk)
            })
            .collect();

        if node_id.is_none() {
            // empty manifet
            return Ok(None);
        }

        let node_id = Some(generated::ObjectId8::new(&node_id.unwrap().0));
        let refs = Some(builder.create_vector(refs_vec.as_slice()));
        let array_manifest = generated::ArrayManifest::create(
            &mut builder,
            &generated::ArrayManifestArgs { node_id: node_id.as_ref(), refs },
        );
        let arrays = builder.create_vector(&[array_manifest]);

        let manifest_id = ManifestId::random();
        let bin_manifest_id = generated::ObjectId12::new(&manifest_id.0);

        let manifest = generated::Manifest::create(
            &mut builder,
            &generated::ManifestArgs { id: Some(&bin_manifest_id), arrays: Some(arrays) },
        );

        builder.finish(manifest, Some("Ichk"));
        let (buffer, offset) = builder.collapse();

        Ok(Some(Manifest { buffer, offset, len: refs_vec.len(), id: manifest_id }))
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
        let res = flatbuffers::root_with_opts::<generated::Manifest>(
            &ROOT_OPTIONS,
            &self.buffer[self.offset..],
        )
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
            let this = chunk_ref.index().iter().collect::<Vec<_>>();
            (&this).cmp(*coords)
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
        //let payload = chunk_ref.payload();
        ref_to_payload(chunk_ref)
    }

    pub fn iter(
        self: Arc<Self>,
        node: NodeId,
    ) -> impl Iterator<Item = (ChunkIndices, ChunkPayload)> {
        PayloadIterator::new(self)
    }

    pub fn chunk_payloads(self: Arc<Self>) -> impl Iterator<Item = ChunkPayload> {
        // FIXME: should go over all nodes
        self.iter(NodeId::random()).map(|(_, p)| p)
    }
}

struct PayloadIterator {
    manifest: Arc<Manifest>,
    last_ref_index: usize,
}

impl PayloadIterator {
    fn new(manifest: Arc<Manifest>) -> Self {
        Self { manifest, last_ref_index: 0 }
    }
}

impl Iterator for PayloadIterator {
    type Item = (ChunkIndices, ChunkPayload);

    fn next(&mut self) -> Option<Self::Item> {
        let manifest = flatbuffers::root_with_opts::<generated::Manifest>(
            &ROOT_OPTIONS,
            &self.manifest.buffer[self.manifest.offset..],
        )
        .unwrap();
        if manifest.arrays().is_empty() {
            return None;
        }
        // FIXME: only one array
        let array_man = manifest.arrays().get(0);
        if self.last_ref_index >= array_man.refs().len() {
            return None;
        }

        let chunk_ref = array_man.refs().get(self.last_ref_index);
        let payload = ref_to_payload(chunk_ref).unwrap();
        let indices = ChunkIndices(chunk_ref.index().iter().collect());
        self.last_ref_index += 1;
        Some((indices, payload))
    }
}

fn ref_to_payload(payload: generated::ChunkRef<'_>) -> IcechunkResult<ChunkPayload> {
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

fn checksum(payload: &generated::ChunkRef<'_>) -> Option<Checksum> {
    if let Some(etag) = payload.checksum_etag() {
        Some(Checksum::ETag(etag.to_string()))
    } else if payload.checksum_last_modified() > 0 {
        Some(Checksum::LastModified(SecondsSinceEpoch(payload.checksum_last_modified())))
    } else {
        None
    }
}

fn mk_chunk_ref<'bldr>(
    builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
    chunk: ChunkInfo,
) -> flatbuffers::WIPOffset<generated::ChunkRef<'bldr>> {
    //                let index = Some(builder.create_vector(chunk.coord.0.as_slice()));
    //                generated::ChunkRef::create(
    //                    &mut builder,
    //                    &generated::ChunkRefArgs { index, payload },
    //                )

    let index = Some(builder.create_vector(chunk.coord.0.as_slice()));
    match chunk.payload {
        ChunkPayload::Inline(bytes) => {
            let bytes = builder.create_vector(bytes.as_ref());
            let args = generated::ChunkRefArgs {
                inline: Some(bytes),
                index,
                ..Default::default()
            };
            generated::ChunkRef::create(builder, &args)
        }
        ChunkPayload::Virtual(virtual_chunk_ref) => {
            let args = generated::ChunkRefArgs {
                index,
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
            generated::ChunkRef::create(builder, &args)
        }
        ChunkPayload::Ref(chunk_ref) => {
            let id = generated::ObjectId12::new(&chunk_ref.id.0);
            let args = generated::ChunkRefArgs {
                index,
                offset: chunk_ref.offset,
                length: chunk_ref.length,
                chunk_id: Some(&id),
                ..Default::default()
            };
            generated::ChunkRef::create(builder, &args)
        }
    }
}

static ROOT_OPTIONS: VerifierOptions = VerifierOptions {
    max_depth: 64,
    max_tables: 50_000_000,
    max_apparent_size: 1 << 31, // taken from the default
    ignore_missing_null_terminator: true,
};
