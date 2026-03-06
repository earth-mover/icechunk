//! Chunk reference tables mapping coordinates to storage locations.

use std::{
    borrow::Cow,
    cmp::{max, min},
    iter::zip,
    ops::Range,
    string::FromUtf8Error,
    sync::Arc,
};

use crate::{format::flatbuffers::generated, virtual_chunks::VirtualChunkContainer};
use bytes::Bytes;
use flatbuffers::VerifierOptions;
use futures::{Stream, TryStreamExt};
use itertools::{Itertools, any, multiunzip};
use rand::{RngExt, rngs::SmallRng};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    config::ManifestVirtualChunkLocationCompressionConfig,
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

    #[inline(always)]
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

// ManifestSplits can be constructed from a iterable of shard edges or boundaries
// along each dimension.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManifestSplits(pub Vec<Vec<u32>>);

impl ManifestSplits {
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn from_edges(iter: impl IntoIterator<Item = Vec<u32>>) -> Self {
        Self(iter.into_iter().collect())
    }

    pub fn iter(&self) -> impl Iterator<Item = ManifestExtents> {
        self.0
            .iter()
            // assume
            // vec![vec![0u32, 1, 2], vec![3u32, 4, 5]]
            .cloned()
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
            })
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
                ours.overlap_with(&theirs) == Overlap::Partial
                    || theirs.overlap_with(&ours) == Overlap::Partial
            }) {
                return false;
            }
        }
        true
    }
}

/// Helper function for constructing uniformly spaced manifest split edges
pub(crate) fn uniform_manifest_split_edges(
    num_chunks: u32,
    split_size: &u32,
) -> Vec<u32> {
    (0u32..=num_chunks)
        .step_by(*split_size as usize)
        .chain((!num_chunks.is_multiple_of(*split_size)).then_some(num_chunks))
        .collect()
}

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum VirtualReferenceErrorKind {
    #[error(
        "no virtual chunk container can handle the chunk location ({0}), edit the repository configuration adding a virtual chunk container for the chunk references, see https://icechunk.io/en/stable/virtual/"
    )]
    NoContainerForUrl(String),
    #[error("error parsing virtual ref URL: {url:?}")]
    CannotParseUrl {
        #[source]
        cause: url::ParseError,
        url: String,
    },
    #[error("invalid credentials for virtual reference of type {0}")]
    InvalidCredentials(String),
    #[error("a virtual chunk in this repository resolves to the url prefix {url}, to be able to fetch the chunk you need to authorize the virtual chunk container when you open/create the repository, see https://icechunk.io/en/stable/virtual/", url = .0.url_prefix())]
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
    #[error("azure store configuration must include an account")]
    AzureConfigurationMustIncludeAccount,
    #[error("decoding virtual chunk url")]
    Decoding(#[from] FromUtf8Error),
    #[error(
        "no virtual chunk container named '{0}' found, check the repository configuration"
    )]
    NoContainerForName(String),
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

pub const VCC_RELATIVE_URL_SCHEME: &str = "vcc://";

#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct VirtualChunkLocation(String);

impl VirtualChunkLocation {
    pub fn url(&self) -> &str {
        self.0.as_str()
    }

    /// Returns true if this is a relative `vcc://` location.
    pub fn is_relative(&self) -> bool {
        self.0.starts_with(VCC_RELATIVE_URL_SCHEME)
    }

    /// If this is a `vcc://name/path` location, returns `(name, relative_path)`.
    pub fn parse_vcc(&self) -> Option<(&str, &str)> {
        let rest = self.0.strip_prefix(VCC_RELATIVE_URL_SCHEME)?;
        let slash = rest.find('/')?;
        Some((&rest[..slash], &rest[slash + 1..]))
    }

    /// Creates a relative location from a VCC name and a path relative to its prefix.
    pub fn from_vcc_path(
        container_name: &str,
        relative_path: &str,
    ) -> Result<VirtualChunkLocation, VirtualReferenceError> {
        if container_name.is_empty() || container_name.contains('/') {
            return Err(VirtualReferenceErrorKind::NoContainerForName(
                container_name.to_string(),
            )
            .into());
        }
        let path: String = relative_path
            .split('/')
            .filter(|s| !s.is_empty())
            .collect::<Vec<_>>()
            .join("/");
        Ok(VirtualChunkLocation(format!(
            "{VCC_RELATIVE_URL_SCHEME}{container_name}/{path}"
        )))
    }

    /// Parse a location that may be either `vcc://` relative or an absolute URL.
    pub fn from_url(path: &str) -> Result<VirtualChunkLocation, VirtualReferenceError> {
        // vcc:// URLs are valid URL scheme, so from_absolute_path handles both
        // absolute (s3://, gcs://, file://) and relative (vcc://) URLs correctly.
        Self::from_absolute_path(path)
    }

    fn from_absolute_path(
        path: &str,
    ) -> Result<VirtualChunkLocation, VirtualReferenceError> {
        // make sure we can parse the provided URL before creating the enum
        // TODO: consider other validation here.
        let url = url::Url::parse(path).map_err(|e| {
            VirtualReferenceErrorKind::CannotParseUrl { cause: e, url: path.to_string() }
        })?;
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
        } else if scheme == "vcc" {
            return Err(VirtualReferenceErrorKind::NoContainerForName(path.into()).into());
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

const COMPRESSION_ALG_NONE: u8 = 0;
const COMPRESSION_ALG_ZSTD_DICT: u8 = 1;
const MAX_DECOMPRESSED_LOCATION_SIZE: usize = 1_024;

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

    /// Create a zstd decompressor from the manifest's location dictionary, if present.
    fn decompressor(
        &self,
    ) -> Result<Option<zstd::bulk::Decompressor<'static>>, IcechunkFormatError> {
        let root = self.root();
        if root.compression_algorithm() != COMPRESSION_ALG_ZSTD_DICT {
            return Ok(None);
        }
        match root.location_dictionary() {
            Some(dict_bytes) => {
                let decompressor =
                    zstd::bulk::Decompressor::with_dictionary(dict_bytes.bytes())
                        .map_err(IcechunkFormatErrorKind::IO)?;
                Ok(Some(decompressor))
            }
            None => Ok(None),
        }
    }

    pub fn from_sorted_vec(
        manifest_id: &ManifestId,
        sorted_chunks: Vec<ChunkInfo>,
        virtual_chunks_compression_config: Option<
            &ManifestVirtualChunkLocationCompressionConfig,
        >,
    ) -> IcechunkResult<Option<Self>> {
        let location_compression_dict =
            train_location_dictionary(&sorted_chunks, virtual_chunks_compression_config)?;
        // we generate all compressed locations if needed
        let compressed_locations =
            match (&location_compression_dict, virtual_chunks_compression_config) {
                (Some(d), Some(config)) => compress_locations(&sorted_chunks, d, config),
                _ => vec![None; sorted_chunks.len()],
            };

        // Sequential FlatBuffer building using pre-compressed locations
        // TODO: what's a good capacity?
        let mut builder = flatbuffers::FlatBufferBuilder::with_capacity(1024 * 1024);

        let len = sorted_chunks.len();
        let mut all = sorted_chunks.into_iter().zip(compressed_locations).peekable();

        let mut array_manifests = Vec::with_capacity(1);
        while let Some(current_node) = all.peek().map(|(chunk, _)| chunk.node.clone()) {
            // TODO: adjust capacity when multiple arrays have their manifests consolidated in to one.
            let mut refs = Vec::with_capacity(len);
            while let Some((chunk, precompressed)) =
                all.next_if(|(chunk, _)| chunk.node == current_node)
            {
                refs.push(mk_chunk_ref(&mut builder, chunk, precompressed));
            }

            let node_id = Some(generated::ObjectId8::new(&current_node.0));
            let refs = Some(builder.create_vector(refs.as_slice()));
            let array_manifest = generated::ArrayManifest::create(
                &mut builder,
                &generated::ArrayManifestArgs {
                    node_id: node_id.as_ref(),
                    refs,
                    ..Default::default()
                },
            );
            array_manifests.push(array_manifest);
        }

        if array_manifests.is_empty() {
            // empty manifest
            return Ok(None);
        }

        let arrays = builder.create_vector(array_manifests.as_slice());
        let bin_manifest_id = generated::ObjectId12::new(&manifest_id.0);

        let (location_dictionary, compression_algorithm) =
            if let Some(ref dict) = location_compression_dict {
                (Some(builder.create_vector(dict.as_slice())), COMPRESSION_ALG_ZSTD_DICT)
            } else {
                (None, COMPRESSION_ALG_NONE)
            };

        let manifest = generated::Manifest::create(
            &mut builder,
            &generated::ManifestArgs {
                id: Some(&bin_manifest_id),
                arrays: Some(arrays),
                location_dictionary,
                compression_algorithm,
                ..Default::default()
            },
        );

        builder.finish(manifest, Some("Ichk"));
        let (mut buffer, offset) = builder.collapse();
        buffer.drain(0..offset);
        buffer.shrink_to_fit();
        Ok(Some(Manifest { buffer }))
    }

    pub async fn from_stream<E>(
        manifest_id: &ManifestId,
        stream: impl Stream<Item = Result<ChunkInfo, E>>,
        virtual_chunks_compression_config: Option<
            &ManifestVirtualChunkLocationCompressionConfig,
        >,
    ) -> Result<Option<Self>, E>
    where
        E: From<IcechunkFormatError>,
    {
        let mut all = stream.try_collect::<Vec<_>>().await?;
        all.sort_by(|a, b| (&a.node, &a.coord).cmp(&(&b.node, &b.coord)));
        Ok(Self::from_sorted_vec(manifest_id, all, virtual_chunks_compression_config)?)
    }

    /// Used for tests
    pub async fn from_iter<T: IntoIterator<Item = ChunkInfo>>(
        manifest_id: &ManifestId,
        iter: T,
        virtual_chunks_compression_config: Option<
            &ManifestVirtualChunkLocationCompressionConfig,
        >,
    ) -> IcechunkResult<Option<Self>> {
        Self::from_stream(
            manifest_id,
            futures::stream::iter(iter.into_iter().map(Ok::<_, IcechunkFormatError>)),
            virtual_chunks_compression_config,
        )
        .await
    }

    pub fn len(&self) -> usize {
        self.root().arrays().iter().map(|am| am.refs().len()).sum()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn root(&self) -> generated::Manifest<'_> {
        // without the unsafe version this is too slow
        // if we try to keep the root in the Manifest struct, we would need a lifetime
        unsafe { flatbuffers::root_unchecked::<generated::Manifest>(&self.buffer) }
    }

    pub fn arrays(&self) -> impl Iterator<Item = NodeId> {
        self.root().arrays().iter().map(|am| NodeId::from(am.node_id().0))
    }

    pub fn uses_location_compression(&self) -> bool {
        self.root().compression_algorithm() == COMPRESSION_ALG_ZSTD_DICT
    }

    pub fn location_dictionary_size(&self) -> Option<usize> {
        self.root().location_dictionary().map(|d| d.len())
    }

    pub fn num_compressed_refs(&self) -> usize {
        self.root()
            .arrays()
            .iter()
            .flat_map(|am| am.refs().iter())
            .filter(|r| r.compressed_location().is_some())
            .count()
    }

    pub fn get_chunk_payload(
        &self,
        node: &NodeId,
        coord: &ChunkIndices,
    ) -> IcechunkResult<ChunkPayload> {
        let mut decompressor = self.decompressor()?;
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
        ref_to_payload(chunk_ref, decompressor.as_mut())
    }

    pub fn iter(
        self: Arc<Self>,
        node: NodeId,
    ) -> Result<
        impl Iterator<Item = Result<(ChunkIndices, ChunkPayload), IcechunkFormatError>>,
        IcechunkFormatError,
    > {
        PayloadIterator::new(self, node)
    }

    pub fn chunk_payloads(
        &self,
    ) -> Result<
        impl Iterator<Item = Result<ChunkPayload, IcechunkFormatError>> + '_,
        IcechunkFormatError,
    > {
        let mut decompressor = self.decompressor()?;
        let refs: Vec<_> =
            self.root().arrays().iter().flat_map(|am| am.refs().iter()).collect();
        Ok(refs.into_iter().map(move |r| ref_to_payload(r, decompressor.as_mut())))
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

pub struct PayloadIterator {
    manifest: Arc<Manifest>,
    node_id: NodeId,
    last_ref_index: usize,
    decompressor: Option<zstd::bulk::Decompressor<'static>>,
}

impl PayloadIterator {
    fn new(
        manifest: Arc<Manifest>,
        node_id: NodeId,
    ) -> Result<Self, IcechunkFormatError> {
        let decompressor = manifest.decompressor()?;
        Ok(Self { manifest, node_id, last_ref_index: 0, decompressor })
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
                ref_to_payload(chunk_ref, self.decompressor.as_mut())
                    .map(|payl| (ChunkIndices(chunk_ref.index().iter().collect()), payl)),
            )
        })
    }
}

fn ref_to_payload(
    chunk_ref: generated::ChunkRef<'_>,
    decompressor: Option<&mut zstd::bulk::Decompressor<'static>>,
) -> Result<ChunkPayload, IcechunkFormatError> {
    if let Some(chunk_id) = chunk_ref.chunk_id() {
        let id = ChunkId::new(chunk_id.0);
        Ok(ChunkPayload::Ref(ChunkRef {
            id,
            offset: chunk_ref.offset(),
            length: chunk_ref.length(),
        }))
    } else if let Some(compressed) = chunk_ref.compressed_location() {
        let decompressor = decompressor
            .ok_or(IcechunkFormatErrorKind::MissingLocationCompressionDictionary)?;
        let decompressed = decompressor
            .decompress(compressed.bytes(), MAX_DECOMPRESSED_LOCATION_SIZE)
            .map_err(IcechunkFormatErrorKind::IO)?;
        let location_str = std::str::from_utf8(&decompressed).map_err(|e| {
            IcechunkFormatErrorKind::IO(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                e,
            ))
        })?;
        let location = VirtualChunkLocation::from_url(location_str)?;
        Ok(ChunkPayload::Virtual(VirtualChunkRef {
            location,
            checksum: checksum(&chunk_ref),
            offset: chunk_ref.offset(),
            length: chunk_ref.length(),
        }))
    } else if let Some(location) = chunk_ref.location() {
        let location = VirtualChunkLocation::from_url(location)?;
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

/// Sample virtual chunk URLs and train a zstd dictionary for compressing them.
///
/// Uses reservoir sampling (Algorithm R) to collect a uniform random sample in a single
/// pass without knowing the total virtual chunk count in advance.
/// See: https://en.wikipedia.org/wiki/Reservoir_sampling#Simple:_Algorithm_R
///
/// Returns `Some(dict_bytes)` if compression is enabled and there are enough virtual
/// chunks, `None` otherwise.
fn train_location_dictionary(
    chunks: &[ChunkInfo],
    virtual_chunks_compression_config: Option<
        &ManifestVirtualChunkLocationCompressionConfig,
    >,
) -> IcechunkResult<Option<Vec<u8>>> {
    let config = match virtual_chunks_compression_config {
        Some(c) => c,
        None => return Ok(None),
    };
    let max_samples = config.dictionary_max_training_samples() as usize;
    let min_chunks = config.min_virtual_chunks_to_compress() as usize;
    let max_dict_size = config.dictionary_max_size_bytes() as usize;

    let mut virtual_count: usize = 0;
    let mut reservoir: Vec<&str> = Vec::with_capacity(max_samples);
    let mut rng: SmallRng = rand::make_rng();

    for chunk in chunks {
        if let ChunkPayload::Virtual(vref) = &chunk.payload {
            let loc = vref.location.url();
            if virtual_count < max_samples {
                // Fill phase: reservoir not yet full
                reservoir.push(loc);
            } else {
                // Replace phase: include new item with decreasing probability
                let j = rng.random_range(0..=virtual_count);
                if j < max_samples {
                    reservoir[j] = loc;
                }
            }
            virtual_count += 1;
        }
    }

    if virtual_count < min_chunks {
        return Ok(None);
    }

    let sample_bytes: Vec<&[u8]> = reservoir.iter().map(|s| s.as_bytes()).collect();

    // zstd doesn't like it if many samples are too small, in that case we don't compress
    let small_count = sample_bytes.iter().filter(|s| s.len() < 8).count();
    if small_count >= sample_bytes.len() / 2 {
        tracing::warn!(
            "Skipping zstd dictionary training: at least half of the {} samples are smaller than 8 bytes",
            sample_bytes.len()
        );
        return Ok(None);
    }

    let mut sample_data: Vec<u8> =
        sample_bytes.iter().flat_map(|s| s.iter().copied()).collect();
    let mut sample_sizes: Vec<usize> = sample_bytes.iter().map(|s| s.len()).collect();
    let total_sample_size = sample_data.len();

    // zstd requires total sample data >= max_dict_size; repeat samples if needed
    if total_sample_size > 0 && total_sample_size < max_dict_size {
        let repeats = (max_dict_size / total_sample_size) + 1;
        let original_data = sample_data.clone();
        let original_sizes = sample_sizes.clone();
        for _ in 0..repeats {
            sample_data.extend_from_slice(&original_data);
            sample_sizes.extend_from_slice(&original_sizes);
        }
    }

    Ok(Some(zstd::dict::from_continuous(&sample_data, &sample_sizes, max_dict_size)?))
}

/// Compress virtual chunk locations in parallel using a pre-trained zstd dictionary.
///
/// Returns one entry per chunk: `Some(compressed_bytes)` for virtual chunks,
/// `None` for inline/ref chunks.
fn compress_locations(
    chunks: &[ChunkInfo],
    dict: &[u8],
    config: &ManifestVirtualChunkLocationCompressionConfig,
) -> Vec<Option<Vec<u8>>> {
    let compression_level = config.compression_level();
    let num_threads =
        std::thread::available_parallelism().map(|n| n.get()).unwrap_or(4).min(8);
    let slice_size = chunks.len().div_ceil(num_threads);

    std::thread::scope(|s| {
        let handles: Vec<_> = chunks
            .chunks(slice_size)
            .map(|slice| {
                s.spawn(|| {
                    let mut comp =
                        zstd::bulk::Compressor::with_dictionary(compression_level, dict)
                            .ok();
                    slice
                        .iter()
                        .map(|chunk| match (&chunk.payload, comp.as_mut()) {
                            (ChunkPayload::Virtual(vref), Some(comp)) => {
                                comp.compress(vref.location.url().as_bytes()).ok()
                            }
                            _ => None,
                        })
                        .collect::<Vec<_>>()
                })
            })
            .collect();

        #[allow(clippy::expect_used)]
        handles
            .into_iter()
            .flat_map(|h| h.join().expect("Cannot join threads compressing locations"))
            .collect()
    })
}

fn mk_chunk_ref<'bldr>(
    builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
    chunk: ChunkInfo,
    precompressed_location: Option<Vec<u8>>,
) -> flatbuffers::WIPOffset<generated::ChunkRef<'bldr>> {
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
            let location_str = virtual_chunk_ref.location.0.as_str();
            let (location, compressed_location) =
                if let Some(compressed) = precompressed_location {
                    (None, Some(builder.create_vector(&compressed)))
                } else {
                    (Some(builder.create_string(location_str)), None)
                };
            let args = generated::ChunkRefArgs {
                index,
                location,
                compressed_location,
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
    max_tables: 500_000_000,
    max_apparent_size: 1 << 31, // taken from the default
    ignore_missing_null_terminator: true,
};

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::*;
    use crate::config::ManifestVirtualChunkLocationCompressionConfig;
    use crate::roundtrip_serialization_tests;
    use crate::strategies::{
        ShapeDim, limited_width_manifest_extents, manifest_extents, manifest_ref,
        manifest_splits, shapes_and_dims,
    };
    use icechunk_macros::{self, tokio_test};
    use itertools::{all, multizip};
    use proptest::collection::vec;
    use proptest::prelude::*;
    use std::error::Error;
    use test_strategy::proptest as alt_proptest;

    roundtrip_serialization_tests!(
        serialize_and_deserialize_manifest_ref - manifest_ref,
        serialize_and_deserialize_manifest_splits - manifest_splits
    );

    #[alt_proptest]
    fn test_property_extents_set_ops_same(
        #[strategy(manifest_extents(4))] e: ManifestExtents,
    ) {
        prop_assert_eq!(e.intersection(&e), Some(e.clone()));
        prop_assert_eq!(e.union(&e), e.clone());
        prop_assert_eq!(e.overlap_with(&e), Overlap::Complete);
    }

    #[alt_proptest]
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

    #[alt_proptest]
    fn test_property_extents_widths(
        #[strategy(limited_width_manifest_extents(4))] extent1: ManifestExtents,
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
            assert_eq!(vec[0].overlap_with(&vec[1]), Overlap::None);
            assert_eq!(vec[1].overlap_with(&vec[0]), Overlap::None);
        }

        Ok(())
    }

    #[alt_proptest]
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

    const COMPRESS_CONFIG: ManifestVirtualChunkLocationCompressionConfig =
        ManifestVirtualChunkLocationCompressionConfig {
            min_virtual_chunks_to_compress: Some(10),
            dictionary_max_training_samples: Some(500),
            dictionary_max_size_bytes: Some(16 * 1024),
            compression_level: Some(3),
        };

    fn make_virtual_chunks(n: usize) -> Vec<ChunkInfo> {
        let node = NodeId::random();
        (0..n)
            .map(|i| ChunkInfo {
                node: node.clone(),
                coord: ChunkIndices(vec![i as u32]),
                payload: ChunkPayload::Virtual(VirtualChunkRef {
                    location: VirtualChunkLocation::from_url(&format!(
                        "s3://my-bucket/path/to/data/chunk_{i:06}"
                    ))
                    .unwrap(),
                    offset: i as u64 * 1024,
                    length: 1024,
                    checksum: None,
                }),
            })
            .collect()
    }

    #[tokio_test]
    async fn test_compression_round_trip() -> Result<(), Box<dyn Error>> {
        // >= threshold virtual chunks with compress=true should round-trip
        let chunks = make_virtual_chunks(50);
        let manifest = Manifest::from_iter(
            &ManifestId::random(),
            chunks.clone(),
            Some(&COMPRESS_CONFIG),
        )
        .await?
        .unwrap();

        let root = manifest.root();
        assert!(root.location_dictionary().is_some());
        assert_eq!(root.compression_algorithm(), COMPRESSION_ALG_ZSTD_DICT);
        for am in root.arrays().iter() {
            for r in am.refs().iter() {
                assert!(r.compressed_location().is_some());
                assert!(r.location().is_none());
            }
        }

        for chunk in &chunks {
            let payload = manifest.get_chunk_payload(&chunk.node, &chunk.coord).unwrap();
            assert_eq!(payload, chunk.payload);
        }
        Ok(())
    }

    #[tokio_test]
    async fn test_compression_below_threshold() -> Result<(), Box<dyn Error>> {
        // Below threshold: compress=true but few chunks → no compression, readback works
        let chunks = make_virtual_chunks(5);
        let manifest = Manifest::from_iter(
            &ManifestId::random(),
            chunks.clone(),
            Some(&COMPRESS_CONFIG),
        )
        .await?
        .unwrap();

        let root = manifest.root();
        assert!(root.location_dictionary().is_none());
        assert_eq!(root.compression_algorithm(), COMPRESSION_ALG_NONE);
        for am in root.arrays().iter() {
            for r in am.refs().iter() {
                assert!(r.compressed_location().is_none());
                assert!(r.location().is_some());
            }
        }

        for chunk in &chunks {
            let payload = manifest.get_chunk_payload(&chunk.node, &chunk.coord).unwrap();
            assert_eq!(payload, chunk.payload);
        }
        Ok(())
    }

    #[tokio_test]
    async fn test_compression_mixed_payloads() -> Result<(), Box<dyn Error>> {
        // Mix of virtual, inline, and ref payloads with compression
        let node = NodeId::random();
        let mut chunks: Vec<ChunkInfo> = (0..30)
            .map(|i| ChunkInfo {
                node: node.clone(),
                coord: ChunkIndices(vec![i]),
                payload: ChunkPayload::Virtual(VirtualChunkRef {
                    location: VirtualChunkLocation::from_url(&format!(
                        "s3://my-bucket/path/to/data/chunk_{i:06}"
                    ))
                    .unwrap(),
                    offset: i as u64 * 1024,
                    length: 1024,
                    checksum: None,
                }),
            })
            .collect();
        chunks.push(ChunkInfo {
            node: node.clone(),
            coord: ChunkIndices(vec![100]),
            payload: ChunkPayload::Inline(Bytes::from_static(b"inline data")),
        });
        chunks.push(ChunkInfo {
            node: node.clone(),
            coord: ChunkIndices(vec![101]),
            payload: ChunkPayload::Ref(ChunkRef {
                id: ChunkId::random(),
                offset: 0,
                length: 512,
            }),
        });

        let manifest = Manifest::from_iter(
            &ManifestId::random(),
            chunks.clone(),
            Some(&COMPRESS_CONFIG),
        )
        .await?
        .unwrap();

        for chunk in &chunks {
            let payload = manifest.get_chunk_payload(&chunk.node, &chunk.coord).unwrap();
            assert_eq!(payload, chunk.payload);
        }
        Ok(())
    }

    #[tokio_test]
    async fn test_compression_buffer_round_trip() -> Result<(), Box<dyn Error>> {
        // bytes() → from_buffer() round-trip
        let chunks = make_virtual_chunks(50);
        let manifest = Manifest::from_iter(
            &ManifestId::random(),
            chunks.clone(),
            Some(&COMPRESS_CONFIG),
        )
        .await?
        .unwrap();
        let bytes = manifest.bytes().to_vec();
        let manifest2 = Manifest::from_buffer(bytes)?;

        for chunk in &chunks {
            let payload = manifest2.get_chunk_payload(&chunk.node, &chunk.coord).unwrap();
            assert_eq!(payload, chunk.payload);
        }
        Ok(())
    }

    #[tokio_test]
    async fn test_compression_iterator_access() -> Result<(), Box<dyn Error>> {
        // Iterator-based access with compression
        let chunks = make_virtual_chunks(50);
        let node = chunks[0].node.clone();
        let manifest = Arc::new(
            Manifest::from_iter(
                &ManifestId::random(),
                chunks.clone(),
                Some(&COMPRESS_CONFIG),
            )
            .await?
            .unwrap(),
        );

        let iter_results: Vec<_> = manifest.iter(node)?.collect::<Vec<_>>();
        assert_eq!(iter_results.len(), 50);
        for (i, result) in iter_results.into_iter().enumerate() {
            let (coord, payload) = result?;
            assert_eq!(coord, chunks[i].coord);
            assert_eq!(payload, chunks[i].payload);
        }
        Ok(())
    }

    #[tokio_test]
    async fn test_ic1_compat_no_compression() -> Result<(), Box<dyn Error>> {
        // compress=false → locations in `location` field, no compressed_location, no dictionary
        let chunks = make_virtual_chunks(50);
        let manifest = Manifest::from_iter(&ManifestId::random(), chunks.clone(), None)
            .await?
            .unwrap();

        let root = manifest.root();
        assert!(root.location_dictionary().is_none());
        assert_eq!(root.compression_algorithm(), COMPRESSION_ALG_NONE);

        // Verify location field is populated (not compressed_location)
        for am in root.arrays().iter() {
            for r in am.refs().iter() {
                if r.chunk_id().is_none() && r.inline().is_none() {
                    assert!(r.location().is_some(), "location should be set");
                    assert!(
                        r.compressed_location().is_none(),
                        "compressed_location should be None"
                    );
                }
            }
        }

        // Readback still works
        for chunk in &chunks {
            let payload = manifest.get_chunk_payload(&chunk.node, &chunk.coord).unwrap();
            assert_eq!(payload, chunk.payload);
        }
        Ok(())
    }

    #[tokio_test]
    async fn test_ic2_compression_fields() -> Result<(), Box<dyn Error>> {
        // compress=true with enough virtual chunks → compressed_location, dictionary present
        let chunks = make_virtual_chunks(50);
        let manifest = Manifest::from_iter(
            &ManifestId::random(),
            chunks.clone(),
            Some(&COMPRESS_CONFIG),
        )
        .await?
        .unwrap();

        let root = manifest.root();
        assert!(root.location_dictionary().is_some());
        assert_eq!(root.compression_algorithm(), COMPRESSION_ALG_ZSTD_DICT);

        // Verify compressed_location is populated and location is None
        for am in root.arrays().iter() {
            for r in am.refs().iter() {
                if r.chunk_id().is_none() && r.inline().is_none() {
                    assert!(
                        r.compressed_location().is_some(),
                        "compressed_location should be set"
                    );
                    assert!(
                        r.location().is_none(),
                        "location should be None for compressed chunks"
                    );
                }
            }
        }

        // Readback still works
        for chunk in &chunks {
            let payload = manifest.get_chunk_payload(&chunk.node, &chunk.coord).unwrap();
            assert_eq!(payload, chunk.payload);
        }
        Ok(())
    }

    #[tokio_test]
    async fn test_compression_skipped_when_locations_too_short()
    -> Result<(), Box<dyn Error>> {
        // When most virtual chunk locations are < 8 bytes, dictionary training
        // is skipped and the manifest falls back to uncompressed storage.
        // With longer locations (>= 8 bytes), compression kicks in.
        let config = ManifestVirtualChunkLocationCompressionConfig {
            min_virtual_chunks_to_compress: Some(2),
            dictionary_max_training_samples: Some(500),
            dictionary_max_size_bytes: Some(256),
            compression_level: Some(3),
        };

        let node = NodeId::random();
        // "s3://a/" is 7 bytes, below the 8-byte threshold
        let short_chunks: Vec<ChunkInfo> = (0..2)
            .map(|i| ChunkInfo {
                node: node.clone(),
                coord: ChunkIndices(vec![i as u32]),
                payload: ChunkPayload::Virtual(VirtualChunkRef {
                    location: VirtualChunkLocation::from_url("s3://a/").unwrap(),
                    offset: i as u64 * 1024,
                    length: 1024,
                    checksum: None,
                }),
            })
            .collect();

        let manifest = Manifest::from_iter(
            &ManifestId::random(),
            short_chunks.clone(),
            Some(&config),
        )
        .await?
        .unwrap();

        let root = manifest.root();
        assert!(root.location_dictionary().is_none());
        assert_eq!(root.compression_algorithm(), COMPRESSION_ALG_NONE);
        for am in root.arrays().iter() {
            for r in am.refs().iter() {
                assert!(r.compressed_location().is_none());
                assert!(r.location().is_some());
            }
        }

        for chunk in &short_chunks {
            let payload = manifest.get_chunk_payload(&chunk.node, &chunk.coord).unwrap();
            assert_eq!(payload, chunk.payload);
        }

        // "s3://abc/" is 9 bytes, above the 8-byte threshold — compression should apply
        let long_chunks: Vec<ChunkInfo> = (0..2)
            .map(|i| ChunkInfo {
                node: node.clone(),
                coord: ChunkIndices(vec![i as u32]),
                payload: ChunkPayload::Virtual(VirtualChunkRef {
                    location: VirtualChunkLocation::from_url("s3://abc/").unwrap(),
                    offset: i as u64 * 1024,
                    length: 1024,
                    checksum: None,
                }),
            })
            .collect();

        let manifest = Manifest::from_iter(
            &ManifestId::random(),
            long_chunks.clone(),
            Some(&config),
        )
        .await?
        .unwrap();

        let root = manifest.root();
        assert!(root.location_dictionary().is_some());
        assert_eq!(root.compression_algorithm(), COMPRESSION_ALG_ZSTD_DICT);
        for am in root.arrays().iter() {
            for r in am.refs().iter() {
                assert!(r.compressed_location().is_some());
                assert!(r.location().is_none());
            }
        }

        for chunk in &long_chunks {
            let payload = manifest.get_chunk_payload(&chunk.node, &chunk.coord).unwrap();
            assert_eq!(payload, chunk.payload);
        }
        Ok(())
    }

    #[tokio_test]
    async fn test_compression_reduces_manifest_size() -> Result<(), Box<dyn Error>> {
        // 100 virtual chunks with highly redundant, large location strings.
        // Compression should produce a significantly smaller manifest.
        let node = NodeId::random();
        let chunks: Vec<ChunkInfo> = (0..100)
            .map(|i| ChunkInfo {
                node: node.clone(),
                coord: ChunkIndices(vec![i as u32]),
                payload: ChunkPayload::Virtual(VirtualChunkRef {
                    location: VirtualChunkLocation::from_url(&format!(
                        "s3://my-very-long-bucket-name/some/deeply/nested/path/to/dataset/v1.2.3/year=2024/month=01/day=15/chunk_{i:06}.parquet"
                    ))
                    .unwrap(),
                    offset: i as u64 * 4096,
                    length: 4096,
                    checksum: None,
                }),
            })
            .collect();

        // Build without compression
        let manifest_uncompressed =
            Manifest::from_iter(&ManifestId::random(), chunks.clone(), None)
                .await?
                .unwrap();
        let size_uncompressed = manifest_uncompressed.bytes().len();

        assert!(!manifest_uncompressed.uses_location_compression());
        assert_eq!(manifest_uncompressed.num_compressed_refs(), 0);

        // Build with compression
        let manifest_compressed = Manifest::from_iter(
            &ManifestId::random(),
            chunks.clone(),
            Some(&COMPRESS_CONFIG),
        )
        .await?
        .unwrap();
        let size_compressed = manifest_compressed.bytes().len();

        assert!(manifest_compressed.uses_location_compression());
        assert_eq!(manifest_compressed.num_compressed_refs(), 100);
        assert!(manifest_compressed.location_dictionary_size().unwrap() > 0);

        // Each compressed_location should be much shorter than the raw URL (118 bytes)
        let root = manifest_compressed.root();
        for am in root.arrays().iter() {
            for r in am.refs().iter() {
                let compressed = r.compressed_location().unwrap();
                assert!(
                    compressed.len() < 40,
                    "compressed_location ({} bytes) should be under 40 bytes \
                     (raw location is 118 bytes)",
                    compressed.len(),
                );
            }
        }

        // Compressed manifest should be meaningfully smaller
        assert!(
            size_compressed < size_uncompressed,
            "compressed ({size_compressed}) should be smaller than uncompressed ({size_uncompressed})"
        );

        let saving_pct =
            (1.0 - size_compressed as f64 / size_uncompressed as f64) * 100.0;
        assert!(
            saving_pct > 15.0,
            "expected at least 15% size reduction with redundant locations, \
             got {saving_pct:.1}% (compressed={size_compressed}, uncompressed={size_uncompressed})"
        );

        // Both manifests must round-trip correctly
        for chunk in &chunks {
            let p1 = manifest_uncompressed
                .get_chunk_payload(&chunk.node, &chunk.coord)
                .unwrap();
            let p2 =
                manifest_compressed.get_chunk_payload(&chunk.node, &chunk.coord).unwrap();
            assert_eq!(p1, chunk.payload);
            assert_eq!(p2, chunk.payload);
        }

        Ok(())
    }

    #[tokio_test]
    async fn test_chunk_payloads_with_compression() -> Result<(), Box<dyn Error>> {
        // chunk_payloads() works with compressed manifests
        let chunks = make_virtual_chunks(50);
        let manifest = Manifest::from_iter(
            &ManifestId::random(),
            chunks.clone(),
            Some(&COMPRESS_CONFIG),
        )
        .await?
        .unwrap();

        let payloads: Vec<_> =
            manifest.chunk_payloads()?.collect::<Result<Vec<_>, _>>()?;
        assert_eq!(payloads.len(), 50);
        for (i, payload) in payloads.iter().enumerate() {
            assert_eq!(payload, &chunks[i].payload);
        }
        Ok(())
    }
}
