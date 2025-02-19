use std::{borrow::Cow, convert::Infallible, ops::Range, sync::Arc};

use crate::format::flatbuffers::gen;
use bytes::Bytes;
use flatbuffers::VerifierOptions;
use futures::{Stream, TryStreamExt};
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    error::ICError,
    format::{IcechunkFormatError, IcechunkFormatErrorKind},
    storage::ETag,
};

use super::{
    ChunkId, ChunkIndices, ChunkLength, ChunkOffset, IcechunkResult, ManifestId, NodeId,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManifestExtents(Vec<Range<u32>>);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManifestRef {
    pub object_id: ManifestId,
    pub extents: ManifestExtents,
}

impl ManifestExtents {
    pub fn new(from: &[u32], to: &[u32]) -> Self {
        let v = from
            .iter()
            .zip(to.iter())
            .map(|(a, b)| Range { start: *a, end: *b })
            .collect();
        Self(v)
    }

    pub fn iter(&self) -> impl Iterator<Item = &Range<u32>> {
        self.0.iter()
    }
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
}

impl Manifest {
    pub fn id(&self) -> ManifestId {
        ManifestId::new(self.root().id().0)
    }

    pub fn bytes(&self) -> &[u8] {
        self.buffer.as_slice()
    }

    pub fn from_buffer(buffer: Vec<u8>) -> Result<Manifest, IcechunkFormatError> {
        let _ = flatbuffers::root_with_opts::<gen::Manifest>(
            &ROOT_OPTIONS,
            buffer.as_slice(),
        )?;
        Ok(Manifest { buffer })
    }

    pub async fn from_stream<E>(
        stream: impl Stream<Item = Result<ChunkInfo, E>>,
    ) -> Result<Option<Self>, E> {
        // TODO: what's a good capacity?
        let mut builder = flatbuffers::FlatBufferBuilder::with_capacity(1024 * 1024);
        let mut all = stream.try_collect::<Vec<_>>().await?;
        // FIXME: should we sort here or can we sort outside?
        all.sort_by(|a, b| (&a.node, &a.coord).cmp(&(&b.node, &b.coord)));

        let mut all = all.iter().peekable();

        let mut array_manifests = Vec::with_capacity(1);
        while let Some(current_node) = all.peek().map(|chunk| &chunk.node).cloned() {
            // TODO: what is a good capacity
            let mut refs = Vec::with_capacity(8_192);
            while let Some(chunk) = all.next_if(|chunk| chunk.node == current_node) {
                refs.push(mk_chunk_ref(&mut builder, chunk));
            }

            let node_id = Some(gen::ObjectId8::new(&current_node.0));
            let refs = Some(builder.create_vector(refs.as_slice()));
            let array_manifest = gen::ArrayManifest::create(
                &mut builder,
                &gen::ArrayManifestArgs { node_id: node_id.as_ref(), refs },
            );
            array_manifests.push(array_manifest);
        }

        if array_manifests.is_empty() {
            // empty manifet
            return Ok(None);
        }

        let arrays = builder.create_vector(array_manifests.as_slice());
        let manifest_id = ManifestId::random();
        let bin_manifest_id = gen::ObjectId12::new(&manifest_id.0);

        let manifest = gen::Manifest::create(
            &mut builder,
            &gen::ManifestArgs { id: Some(&bin_manifest_id), arrays: Some(arrays) },
        );

        builder.finish(manifest, Some("Ichk"));
        let (mut buffer, offset) = builder.collapse();
        buffer.drain(0..offset);
        buffer.shrink_to_fit();
        Ok(Some(Manifest { buffer }))
    }

    /// Used for tests
    pub async fn from_iter<T: IntoIterator<Item = ChunkInfo>>(
        iter: T,
    ) -> Result<Option<Self>, Infallible> {
        Self::from_stream(futures::stream::iter(iter.into_iter().map(Ok))).await
    }

    pub fn len(&self) -> usize {
        self.root().arrays().iter().map(|am| am.refs().len()).sum()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn root(&self) -> gen::Manifest {
        // without the unsafe version this is too slow
        // if we try to keep the root in the Manifest struct, we would need a lifetime
        unsafe { flatbuffers::root_unchecked::<gen::Manifest>(&self.buffer) }
    }

    pub fn get_chunk_payload(
        &self,
        node: &NodeId,
        coord: &ChunkIndices,
    ) -> IcechunkResult<ChunkPayload> {
        let manifest = self.root();
        let chunk_ref = lookup_node(manifest, node)
            .and_then(|array_manifest| lookup_ref(array_manifest, coord))
            .ok_or_else(|| {
                IcechunkFormatError::from(
                    IcechunkFormatErrorKind::ChunkCoordinatesNotFound {
                        coords: coord.clone(),
                    },
                )
            })?;
        ref_to_payload(chunk_ref)
    }

    pub fn iter(
        self: Arc<Self>,
        node: NodeId,
    ) -> impl Iterator<Item = Result<(ChunkIndices, ChunkPayload), IcechunkFormatError>>
    {
        PayloadIterator::new(self, node)
    }

    pub fn chunk_payloads(
        &self,
    ) -> impl Iterator<Item = Result<ChunkPayload, IcechunkFormatError>> + '_ {
        self.root().arrays().iter().flat_map(move |array_manifest| {
            array_manifest.refs().iter().map(|r| ref_to_payload(r))
        })
    }
}

fn lookup_node<'a>(
    manifest: gen::Manifest<'a>,
    node: &NodeId,
) -> Option<gen::ArrayManifest<'a>> {
    manifest.arrays().lookup_by_key(node.0, |am, id| am.node_id().0.cmp(id))
}

fn lookup_ref<'a>(
    array_manifest: gen::ArrayManifest<'a>,
    coord: &ChunkIndices,
) -> Option<gen::ChunkRef<'a>> {
    let res =
        array_manifest.refs().lookup_by_key(coord.0.as_slice(), |chunk_ref, coords| {
            chunk_ref.index().iter().cmp(coords.iter().copied())
        });
    res
}

struct PayloadIterator {
    manifest: Arc<Manifest>,
    node_id: NodeId,
    last_ref_index: usize,
}

impl PayloadIterator {
    fn new(manifest: Arc<Manifest>, node_id: NodeId) -> Self {
        Self { manifest, node_id, last_ref_index: 0 }
    }
}

impl Iterator for PayloadIterator {
    type Item = Result<(ChunkIndices, ChunkPayload), IcechunkFormatError>;

    fn next(&mut self) -> Option<Self::Item> {
        let manifest = self.manifest.root();
        lookup_node(manifest, &self.node_id).and_then(|array_manifest| {
            let refs = array_manifest.refs();
            if self.last_ref_index >= refs.len() {
                return None;
            }

            let chunk_ref = refs.get(self.last_ref_index);
            self.last_ref_index += 1;
            Some(
                ref_to_payload(chunk_ref)
                    .map(|payl| (ChunkIndices(chunk_ref.index().iter().collect()), payl)),
            )
        })
    }
}

fn ref_to_payload(
    chunk_ref: gen::ChunkRef<'_>,
) -> Result<ChunkPayload, IcechunkFormatError> {
    if let Some(chunk_id) = chunk_ref.chunk_id() {
        let id = ChunkId::new(chunk_id.0);
        Ok(ChunkPayload::Ref(ChunkRef {
            id,
            offset: chunk_ref.offset(),
            length: chunk_ref.length(),
        }))
    } else if let Some(location) = chunk_ref.location() {
        let location = VirtualChunkLocation::from_absolute_path(location)?;
        Ok(ChunkPayload::Virtual(VirtualChunkRef {
            location,
            checksum: checksum(&chunk_ref),
            offset: chunk_ref.offset(),
            length: chunk_ref.length(),
        }))
    } else if let Some(data) = chunk_ref.inline() {
        Ok(ChunkPayload::Inline(Bytes::copy_from_slice(data.bytes())))
    } else {
        Err(IcechunkFormatErrorKind::InvalidFlatBuffer(
            flatbuffers::InvalidFlatbuffer::InconsistentUnion {
                field: Cow::Borrowed("chunk_id+location+inline"),
                field_type: Cow::Borrowed("invalid"),
                error_trace: Default::default(),
            },
        )
        .into())
    }
}

fn checksum(payload: &gen::ChunkRef<'_>) -> Option<Checksum> {
    if let Some(etag) = payload.checksum_etag() {
        Some(Checksum::ETag(ETag(etag.to_string())))
    } else if payload.checksum_last_modified() > 0 {
        Some(Checksum::LastModified(SecondsSinceEpoch(payload.checksum_last_modified())))
    } else {
        None
    }
}

fn mk_chunk_ref<'bldr>(
    builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
    chunk: &ChunkInfo,
) -> flatbuffers::WIPOffset<gen::ChunkRef<'bldr>> {
    let index = Some(builder.create_vector(chunk.coord.0.as_slice()));
    match &chunk.payload {
        ChunkPayload::Inline(bytes) => {
            let bytes = builder.create_vector(bytes.as_ref());
            let args =
                gen::ChunkRefArgs { inline: Some(bytes), index, ..Default::default() };
            gen::ChunkRef::create(builder, &args)
        }
        ChunkPayload::Virtual(virtual_chunk_ref) => {
            let args = gen::ChunkRefArgs {
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
                            Some(builder.create_string(etag.0.as_str()))
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
            gen::ChunkRef::create(builder, &args)
        }
        ChunkPayload::Ref(chunk_ref) => {
            let id = gen::ObjectId12::new(&chunk_ref.id.0);
            let args = gen::ChunkRefArgs {
                index,
                offset: chunk_ref.offset,
                length: chunk_ref.length,
                chunk_id: Some(&id),
                ..Default::default()
            };
            gen::ChunkRef::create(builder, &args)
        }
    }
}

static ROOT_OPTIONS: VerifierOptions = VerifierOptions {
    max_depth: 64,
    max_tables: 50_000_000,
    max_apparent_size: 1 << 31, // taken from the default
    ignore_missing_null_terminator: true,
};
