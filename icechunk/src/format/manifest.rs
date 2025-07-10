use std::{
    borrow::Cow,
    cmp::{max, min},
    convert::Infallible,
    iter::zip,
    ops::Range,
    sync::Arc,
};

use crate::{format::flatbuffers::generated, virtual_chunks::VirtualChunkContainer};
use bytes::Bytes;
use flatbuffers::VerifierOptions;
use futures::{Stream, TryStreamExt};
use itertools::{Itertools, any, multiunzip};
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Overlap {
    Complete,
    Partial,
    None,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManifestExtents(Vec<Range<u32>>);

impl ManifestExtents {
    // sentinel for a "universal set"
    pub const ALL: Self = Self(Vec::new());

    pub fn new(from: &[u32], to: &[u32]) -> Self {
        let v = from
            .iter()
            .zip(to.iter())
            .map(|(a, b)| Range { start: *a, end: *b })
            .collect();
        Self(v)
    }

    pub fn from_ranges_iter(ranges: impl IntoIterator<Item = Range<u32>>) -> Self {
        Self(ranges.into_iter().collect())
    }

    pub fn contains(&self, coord: &[u32]) -> bool {
        self.iter().zip(coord.iter()).all(|(range, that)| range.contains(that))
    }

    pub fn iter(&self) -> impl Iterator<Item = &Range<u32>> {
        self.0.iter()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn intersection(&self, other: &Self) -> Option<Self> {
        if self == &Self::ALL {
            return Some(other.clone());
        }

        debug_assert_eq!(self.len(), other.len());
        let ranges = zip(self.iter(), other.iter())
            .map(|(a, b)| max(a.start, b.start)..min(a.end, b.end))
            .collect::<Vec<_>>();
        if any(ranges.iter(), |r| r.end <= r.start) { None } else { Some(Self(ranges)) }
    }

    pub fn union(&self, other: &Self) -> Self {
        if self == &Self::ALL {
            return Self::ALL;
        }
        debug_assert_eq!(self.len(), other.len());
        Self::from_ranges_iter(
            zip(self.iter(), other.iter())
                .map(|(a, b)| min(a.start, b.start)..max(a.end, b.end)),
        )
    }

    pub fn overlap_with(&self, other: &Self) -> Overlap {
        // Important: this is not symmetric.
        if *other == Self::ALL {
            return Overlap::Complete;
        } else if *self == Self::ALL {
            return Overlap::Partial;
        }
        debug_assert!(
            self.len() == other.len(),
            "Length mismatch: self = {:?}, other = {:?}",
            &self,
            &other
        );
        let mut overlap = Overlap::Complete;
        for (a, b) in zip(other.iter(), self.iter()) {
            debug_assert!(a.start <= a.end, "Invalid range: {:?}", a.clone());
            debug_assert!(b.start <= b.end, "Invalid range: {:?}", b.clone());
            if (a.end <= b.start) || (a.start >= b.end) {
                return Overlap::None;
            } else if !((a.start <= b.start) && (a.end >= b.end)) {
                overlap = Overlap::Partial
            }
        }
        overlap
    }

    pub fn matches(&self, other: &ManifestExtents) -> bool {
        // used in `.filter`
        // ALL always matches any other extents
        if *self == Self::ALL { true } else { self == other }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManifestRef {
    pub object_id: ManifestId,
    pub extents: ManifestExtents,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManifestSplits(pub Vec<ManifestExtents>);

impl ManifestSplits {
    /// Used at read-time
    pub fn from_extents(extents: Vec<ManifestExtents>) -> Self {
        assert!(!extents.is_empty());
        Self(extents)
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    // Build up ManifestSplits from a iterable of shard edges or boundaries
    // along each dimension.
    /// # Examples
    /// ```
    /// use icechunk::format::manifest::{ManifestSplits, ManifestExtents};
    /// let actual = ManifestSplits::from_edges(vec![vec![0u32, 1, 2], vec![3u32, 4, 5]]);
    /// let expected = ManifestSplits::from_extents(vec![
    ///     ManifestExtents::new(&[0, 3], &[1, 4]),
    ///     ManifestExtents::new(&[0, 4], &[1, 5]),
    ///     ManifestExtents::new(&[1, 3], &[2, 4]),
    ///     ManifestExtents::new(&[1, 4], &[2, 5]),
    ///     ]
    /// );
    /// assert_eq!(actual, expected);
    /// ```
    pub fn from_edges(iter: impl IntoIterator<Item = Vec<u32>>) -> Self {
        // let iter = vec![vec![0u32, 1, 2], vec![3u32, 4, 5]]
        let res = iter
            .into_iter()
            // vec![(0, 1), (1, 2)], vec![(3, 4), (4, 5)]
            .map(|x| x.into_iter().tuple_windows())
            // vec![((0, 1), (3, 4)), ((0, 1), (4, 5)),
            //      ((1, 2), (3, 4)), ((1, 2), (4, 5))]
            .multi_cartesian_product()
            // vec![((0, 3), (1, 4)), ((0, 4), (1, 5)),
            //      ((1, 3), (2, 4)), ((1, 4), (2, 5))]
            .map(multiunzip)
            .map(|(from, to): (Vec<u32>, Vec<u32>)| {
                ManifestExtents::new(from.as_slice(), to.as_slice())
            });
        Self(res.collect())
    }

    pub fn iter(&self) -> impl Iterator<Item = &ManifestExtents> {
        self.0.iter()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn compatible_with(&self, other: &Self) -> bool {
        // this is not a simple zip + all(equals) because
        // ordering might differ though both sets of splits
        // must be complete.
        for ours in self.iter() {
            if any(other.iter(), |theirs| {
                ours.overlap_with(theirs) == Overlap::Partial
                    || theirs.overlap_with(ours) == Overlap::Partial
            }) {
                return false;
            }
        }
        true
    }
}

/// Helper function for constructing uniformly spaced manifest split edges
pub fn uniform_manifest_split_edges(num_chunks: u32, split_size: &u32) -> Vec<u32> {
    (0u32..=num_chunks)
        .step_by(*split_size as usize)
        .chain((num_chunks % split_size != 0).then_some(num_chunks))
        .collect()
}
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum VirtualReferenceErrorKind {
    #[error(
        "no virtual chunk container can handle the chunk location ({0}), edit the repository configuration adding a virtual chunk container for the chunk references, see https://icechunk.io/en/stable/icechunk-python/virtual/"
    )]
    NoContainerForUrl(String),
    #[error("error parsing virtual ref URL")]
    CannotParseUrl(#[from] url::ParseError),
    #[error("invalid credentials for virtual reference of type {0}")]
    InvalidCredentials(String),
    #[error("a virtual chunk in this repository resolves to the url prefix {url}, to be able to fetch the chunk you need to authorize the virtual chunk container when you open/create the repository, see https://icechunk.io/en/stable/icechunk-python/virtual/", url = .0.url_prefix())]
    UnauthorizedVirtualChunkContainer(Box<VirtualChunkContainer>),
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
    #[error(
        "error retrieving virtual chunk, not enough data. Expected: ({expected}), available ({available})"
    )]
    InvalidObjectSize { expected: u64, available: u64 },
    #[error("unknown error")]
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

        let location = format!("{scheme}://{host}/{new_path}",);

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

#[derive(PartialEq)]
pub struct Manifest {
    buffer: Vec<u8>,
}

impl std::fmt::Debug for Manifest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Manifest")
            .field("id", &self.id())
            .field("chunks", &self.len())
            .finish_non_exhaustive()
    }
}

impl Manifest {
    pub fn id(&self) -> ManifestId {
        ManifestId::new(self.root().id().0)
    }

    pub fn bytes(&self) -> &[u8] {
        self.buffer.as_slice()
    }

    pub fn from_buffer(buffer: Vec<u8>) -> Result<Manifest, IcechunkFormatError> {
        let _ = flatbuffers::root_with_opts::<generated::Manifest>(
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

            let node_id = Some(generated::ObjectId8::new(&current_node.0));
            let refs = Some(builder.create_vector(refs.as_slice()));
            let array_manifest = generated::ArrayManifest::create(
                &mut builder,
                &generated::ArrayManifestArgs { node_id: node_id.as_ref(), refs },
            );
            array_manifests.push(array_manifest);
        }

        if array_manifests.is_empty() {
            // empty manifest
            return Ok(None);
        }

        let arrays = builder.create_vector(array_manifests.as_slice());
        let manifest_id = ManifestId::random();
        let bin_manifest_id = generated::ObjectId12::new(&manifest_id.0);

        let manifest = generated::Manifest::create(
            &mut builder,
            &generated::ManifestArgs { id: Some(&bin_manifest_id), arrays: Some(arrays) },
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

    fn root(&self) -> generated::Manifest {
        // without the unsafe version this is too slow
        // if we try to keep the root in the Manifest struct, we would need a lifetime
        unsafe { flatbuffers::root_unchecked::<generated::Manifest>(&self.buffer) }
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
    manifest: generated::Manifest<'a>,
    node: &NodeId,
) -> Option<generated::ArrayManifest<'a>> {
    manifest.arrays().lookup_by_key(node.0, |am, id| am.node_id().0.cmp(id))
}

fn lookup_ref<'a>(
    array_manifest: generated::ArrayManifest<'a>,
    coord: &ChunkIndices,
) -> Option<generated::ChunkRef<'a>> {
    array_manifest.refs().lookup_by_key(coord.0.as_slice(), |chunk_ref, coords| {
        chunk_ref.index().iter().cmp(coords.iter().copied())
    })
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
    chunk_ref: generated::ChunkRef<'_>,
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

fn checksum(payload: &generated::ChunkRef<'_>) -> Option<Checksum> {
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
) -> flatbuffers::WIPOffset<generated::ChunkRef<'bldr>> {
    let index = Some(builder.create_vector(chunk.coord.0.as_slice()));
    match &chunk.payload {
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

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::*;
    use crate::strategies::{ShapeDim, manifest_extents, shapes_and_dims};
    use icechunk_macros;
    use itertools::{all, multizip};
    use proptest::collection::vec;
    use proptest::prelude::*;
    use std::error::Error;
    use test_strategy::proptest;

    #[proptest]
    fn test_property_extents_set_ops_same(
        #[strategy(manifest_extents(4))] e: ManifestExtents,
    ) {
        prop_assert_eq!(e.intersection(&e), Some(e.clone()));
        prop_assert_eq!(e.union(&e), e.clone());
        prop_assert_eq!(e.overlap_with(&e), Overlap::Complete);
    }

    #[proptest]
    fn test_property_extents_set_ops(
        #[strategy(manifest_extents(4))] e1: ManifestExtents,
        #[strategy(manifest_extents(4))] e2: ManifestExtents,
    ) {
        let union = e1.union(&e2);
        let intersection = e1.intersection(&e2);

        prop_assert_eq!(e1.intersection(&union), Some(e1.clone()));
        prop_assert_eq!(union.intersection(&e1), Some(e1.clone()));
        prop_assert_eq!(e2.intersection(&union), Some(e2.clone()));
        prop_assert_eq!(union.intersection(&e2), Some(e2.clone()));

        // order is important for the next 2
        prop_assert_eq!(e1.overlap_with(&union), Overlap::Complete);
        prop_assert_eq!(e2.overlap_with(&union), Overlap::Complete);

        if intersection.is_some() {
            let int = intersection.unwrap();
            let expected = if e1 == e1 { Overlap::Complete } else { Overlap::Partial };
            prop_assert_eq!(int.overlap_with(&e1), expected.clone());
            prop_assert_eq!(int.overlap_with(&e2), expected);
        } else {
            prop_assert_eq!(e2.overlap_with(&e1), Overlap::None);
            prop_assert_eq!(e1.overlap_with(&e2), Overlap::None);
        }
    }

    #[proptest]
    fn test_property_extents_widths(
        #[strategy(manifest_extents(4))] extent1: ManifestExtents,
        #[strategy(vec(0..100, 4))] delta_left: Vec<i32>,
        #[strategy(vec(0..100, 4))] delta_right: Vec<i32>,
    ) {
        let widths = extent1.iter().map(|r| (r.end - r.start) as i32).collect::<Vec<_>>();
        let extent2 = ManifestExtents::from_ranges_iter(
            multizip((extent1.iter(), delta_left.iter(), delta_right.iter())).map(
                |(extent, dleft, dright)| {
                    ((extent.start as i32 + dleft) as u32)
                        ..((extent.end as i32 + dright) as u32)
                },
            ),
        );

        if all(delta_left.iter(), |elem| elem == &0i32)
            && all(delta_right.iter(), |elem| elem == &0i32)
        {
            prop_assert_eq!(extent2.overlap_with(&extent1), Overlap::Complete);
        }

        let extent2 = ManifestExtents::from_ranges_iter(
            multizip((
                extent1.iter(),
                widths.iter(),
                delta_left.iter(),
                delta_right.iter(),
            ))
            .map(|(extent, width, dleft, dright)| {
                let (low, high) = (dleft.min(dright), dleft.max(dright));
                ((extent.start as i32 + width + low) as u32)
                    ..((extent.end as i32 + width + high) as u32)
            }),
        );

        prop_assert_eq!(extent2.overlap_with(&extent1), Overlap::None);

        let extent2 = ManifestExtents::from_ranges_iter(
            multizip((
                extent1.iter(),
                widths.iter(),
                delta_left.iter(),
                delta_right.iter(),
            ))
            .map(|(extent, width, dleft, dright)| {
                let (low, high) = (dleft.min(dright), dleft.max(dright));
                ((extent.start as i32 - width - high).max(0i32) as u32)
                    ..((extent.end as i32 - width - low) as u32)
            }),
        );
        prop_assert_eq!(extent2.overlap_with(&extent1), Overlap::None);

        let extent2 = ManifestExtents::from_ranges_iter(
            multizip((extent1.iter(), delta_left.iter(), delta_right.iter())).map(
                |(extent, dleft, dright)| {
                    ((extent.start as i32 - dleft - 1).max(0i32) as u32)
                        ..((extent.end as i32 + dright + 1) as u32)
                },
            ),
        );
        prop_assert_eq!(extent2.overlap_with(&extent1), Overlap::Partial);
    }

    #[icechunk_macros::test]
    fn test_overlaps() -> Result<(), Box<dyn Error>> {
        let e1 = ManifestExtents::new(
            vec![0u32, 1, 2].as_slice(),
            vec![2u32, 4, 6].as_slice(),
        );

        let e2 = ManifestExtents::new(
            vec![10u32, 1, 2].as_slice(),
            vec![12u32, 4, 6].as_slice(),
        );

        let union = ManifestExtents::new(
            vec![0u32, 1, 2].as_slice(),
            vec![12u32, 4, 6].as_slice(),
        );

        assert_eq!(e2.overlap_with(&e1), Overlap::None);
        assert_eq!(e1.intersection(&e2), None);
        assert_eq!(e1.union(&e2), union);

        let e1 = ManifestExtents::new(
            vec![0u32, 1, 2].as_slice(),
            vec![2u32, 4, 6].as_slice(),
        );
        let e2 = ManifestExtents::new(
            vec![2u32, 1, 2].as_slice(),
            vec![42u32, 4, 6].as_slice(),
        );
        assert_eq!(e2.overlap_with(&e1), Overlap::None);
        assert_eq!(e1.overlap_with(&e2), Overlap::None);

        // asymmetric case
        let e1 = ManifestExtents::new(
            vec![0u32, 1, 2].as_slice(),
            vec![3u32, 4, 6].as_slice(),
        );
        let e2 = ManifestExtents::new(
            vec![2u32, 1, 2].as_slice(),
            vec![3u32, 4, 6].as_slice(),
        );
        let union = ManifestExtents::new(
            vec![0u32, 1, 2].as_slice(),
            vec![3u32, 4, 6].as_slice(),
        );
        let intersection = ManifestExtents::new(
            vec![2u32, 1, 2].as_slice(),
            vec![3u32, 4, 6].as_slice(),
        );
        assert_eq!(e2.overlap_with(&e1), Overlap::Complete);
        assert_eq!(e1.overlap_with(&e2), Overlap::Partial);
        assert_eq!(e1.union(&e2), union.clone());
        assert_eq!(e2.union(&e1), union.clone());
        assert_eq!(e1.intersection(&e2), Some(intersection));

        // empty set
        let e1 = ManifestExtents::new(
            vec![0u32, 1, 2].as_slice(),
            vec![3u32, 4, 6].as_slice(),
        );
        let e2 = ManifestExtents::new(
            vec![2u32, 1, 2].as_slice(),
            vec![2u32, 4, 6].as_slice(),
        );
        assert_eq!(e1.intersection(&e2), None);

        // this should create non-overlapping extents
        let splits = ManifestSplits::from_edges(vec![
            vec![0, 10, 20],
            vec![0, 1, 2],
            vec![0, 21, 22],
        ]);
        for vec in splits.iter().combinations(2) {
            assert_eq!(vec[0].overlap_with(vec[1]), Overlap::None);
            assert_eq!(vec[1].overlap_with(vec[0]), Overlap::None);
        }

        Ok(())
    }

    #[proptest]
    fn test_manifest_split_from_edges(
        #[strategy(shapes_and_dims(Some(5)))] shape_dim: ShapeDim,
    ) {
        // Note: using the shape, chunks strategy to generate chunk_shape, split_shape
        let ShapeDim { shape, .. } = shape_dim;

        let num_chunks = shape.iter().map(|x| x.array_length()).collect::<Vec<_>>();
        let split_shape = shape.iter().map(|x| x.chunk_length()).collect::<Vec<_>>();

        let ndim = shape.len();
        let edges: Vec<Vec<u32>> = (0usize..ndim)
            .map(|axis| {
                uniform_manifest_split_edges(
                    num_chunks[axis] as u32,
                    &(split_shape[axis] as u32),
                )
            })
            .collect();

        let splits = ManifestSplits::from_edges(edges.into_iter());
        for edge in splits.iter() {
            // must be ndim ranges
            prop_assert_eq!(edge.len(), ndim);
            for range in edge.iter() {
                prop_assert!(range.end > range.start);
            }
        }

        // when using from_edges, extents must not exactly overlap
        for edges in splits.iter().combinations(2) {
            let is_equal = std::iter::zip(edges[0].iter(), edges[1].iter()).all(
                |(range1, range2)| {
                    (range1.start == range2.start) && (range1.end == range2.end)
                },
            );
            prop_assert!(!is_equal);
        }
    }
}
