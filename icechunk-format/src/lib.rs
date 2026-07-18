//! Icechunk data types and serialization.
//!
//! Defines ID types ([`SnapshotId`], [`ManifestId`], [`ChunkId`], [`NodeId`]),
//! [`Path`] for normalized Zarr paths, and [`ByteRange`]/[`ChunkIndices`] for
//! chunk addressing. Submodules define snapshots, manifests, and transaction logs.

use core::fmt;
use std::{
    cmp::Ordering,
    convert::Infallible,
    fmt::{Debug, Display},
    hash::Hash,
    marker::PhantomData,
    ops::Range,
};

use ::flatbuffers::InvalidFlatbuffer;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use flatbuffers::generated;
use format_constants::FileTypeBin;
use manifest::VirtualReferenceErrorKind;
use rand::{RngExt as _, rng};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use icechunk_types::error::ICError;
use icechunk_types::sealed;

/// User attributes stored on arrays and groups.
pub mod attributes;
/// Chunk reference tables.
pub mod manifest;

/// Generated Flatbuffer types for binary serialization.
#[path = "./flatbuffers/all_generated.rs"]
#[allow(clippy::all, warnings)]
pub mod flatbuffers;

/// Repository metadata (version, properties).
pub mod repo_info;
/// Flatbuffer serialization.
pub mod serializers;
/// Repository state at a point in time.
pub mod snapshot;
/// Change records for commits.
pub mod transaction_log;

pub const CONFIG_FILE_PATH: &str = "config.yaml";
// ic[impl layout.paths] this and the following prefix constants
// ic[impl repo.path] file name
pub const REPO_INFO_FILE_PATH: &str = "repo";
// ic[impl chunks.path] prefix
pub const CHUNKS_FILE_PATH: &str = "chunks";
// ic[impl manifest.path] prefix
pub const MANIFESTS_FILE_PATH: &str = "manifests";
// ic[impl snapshot.path] prefix
pub const SNAPSHOTS_FILE_PATH: &str = "snapshots";
// ic[impl txlog.required] path prefix
pub const TRANSACTION_LOGS_FILE_PATH: &str = "transactions";
pub const OVERWRITTEN_FILES_PATH: &str = "overwritten";
pub const V1_REFS_FILE_PATH: &str = "refs";

pub use icechunk_types::{Path, PathError};

/// Marker trait for object ID type tags (sealed).
pub trait FileTypeTag: sealed::Sealed {}

/// The id of a file in object store
#[derive(Hash, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ObjectId<const SIZE: usize, T: FileTypeTag>(
    #[serde(with = "serde_bytes")] pub [u8; SIZE],
    PhantomData<T>,
);

/// Type tag for [`SnapshotId`].
#[derive(Debug, Hash, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct SnapshotTag;

/// Type tag for [`ManifestId`].
#[derive(Debug, Hash, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct ManifestTag;

/// Type tag for [`ChunkId`].
#[derive(Debug, Hash, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct ChunkTag;

/// Type tag for [`AttributesId`].
#[derive(Debug, Hash, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct AttributesTag;

/// Type tag for [`NodeId`].
#[derive(Debug, Hash, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct NodeTag;

impl sealed::Sealed for SnapshotTag {}
impl sealed::Sealed for ManifestTag {}
impl sealed::Sealed for ChunkTag {}
impl sealed::Sealed for AttributesTag {}
impl sealed::Sealed for NodeTag {}
impl FileTypeTag for SnapshotTag {}
impl FileTypeTag for ManifestTag {}
impl FileTypeTag for ChunkTag {}
impl FileTypeTag for AttributesTag {}
impl FileTypeTag for NodeTag {}

// A 1e-9 conflict probability requires 2^33 ~ 8.5 bn chunks
// using this site for the calculations: https://www.bdayprob.com/

/// Unique identifier for a snapshot.
pub type SnapshotId = ObjectId<12, SnapshotTag>;
/// Unique identifier for a manifest file.
pub type ManifestId = ObjectId<12, ManifestTag>;
/// Unique identifier for a chunk.
pub type ChunkId = ObjectId<12, ChunkTag>;
/// Unique identifier for an attributes blob.
pub type AttributesId = ObjectId<12, AttributesTag>;

// A 1e-9 conflict probability requires 2^17.55 ~ 200k nodes
/// The internal id of an array or group, unique only to a single store version
pub type NodeId = ObjectId<8, NodeTag>;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Hash)]
pub enum NodeType {
    Group,
    Array,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Hash)]
pub struct Move {
    pub from: Path,
    pub to: Path,
    pub node_id: NodeId,
    pub node_type: NodeType,
}

impl From<NodeType> for generated::NodeType {
    fn from(value: NodeType) -> Self {
        match value {
            NodeType::Group => generated::NodeType::Group,
            NodeType::Array => generated::NodeType::Array,
        }
    }
}

impl NodeType {
    fn max_supported() -> u8 {
        generated::NodeType::ENUM_MAX
    }
}

impl TryFrom<generated::NodeType> for NodeType {
    type Error = IcechunkFormatErrorKind;

    fn try_from(value: generated::NodeType) -> Result<Self, Self::Error> {
        match value {
            generated::NodeType::Group => Ok(NodeType::Group),
            generated::NodeType::Array => Ok(NodeType::Array),
            generated::NodeType(v) => Err(IcechunkFormatErrorKind::InvalidNodeType {
                found: v,
                max_supported: Self::max_supported(),
            }),
        }
    }
}

impl<const SIZE: usize, T: FileTypeTag> ObjectId<SIZE, T> {
    pub fn random() -> Self {
        let mut buf = [0u8; SIZE];
        rng().fill(&mut buf[..]);
        Self(buf, PhantomData)
    }

    pub const fn new(buf: [u8; SIZE]) -> Self {
        Self(buf, PhantomData)
    }

    pub const FAKE: Self = Self([0; SIZE], PhantomData);
}

impl<const SIZE: usize, T: FileTypeTag> Debug for ObjectId<SIZE, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", String::from(self))
    }
}

impl<const SIZE: usize, T: FileTypeTag> TryFrom<&[u8]> for ObjectId<SIZE, T> {
    type Error = &'static str;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        let buf = value.try_into();
        buf.map(|buf| ObjectId(buf, PhantomData))
            .map_err(|_| "Invalid ObjectId buffer length")
    }
}

// ic[impl types.object-id.encoding] Crockford base32 decoding
impl<const SIZE: usize, T: FileTypeTag> TryFrom<&str> for ObjectId<SIZE, T> {
    type Error = &'static str;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let bytes = base32::decode(base32::Alphabet::Crockford, value);
        let Some(bytes) = bytes else { return Err("Invalid ObjectId string") };
        Self::try_from(bytes.as_slice())
    }
}

// ic[impl types.object-id.encoding] Crockford base32 encoding
impl<const SIZE: usize, T: FileTypeTag> From<&ObjectId<SIZE, T>> for String {
    fn from(value: &ObjectId<SIZE, T>) -> Self {
        base32::encode(base32::Alphabet::Crockford, &value.0)
    }
}

impl<const SIZE: usize, T: FileTypeTag> From<[u8; SIZE]> for ObjectId<SIZE, T> {
    fn from(value: [u8; SIZE]) -> Self {
        ObjectId::new(value)
    }
}

impl<const SIZE: usize, T: FileTypeTag> TryInto<String> for ObjectId<SIZE, T> {
    type Error = Infallible;

    fn try_into(self) -> Result<String, Self::Error> {
        Ok(self.to_string())
    }
}

impl<const SIZE: usize, T: FileTypeTag> TryInto<ObjectId<SIZE, T>> for String {
    type Error = &'static str;

    fn try_into(self) -> Result<ObjectId<SIZE, T>, Self::Error> {
        self.as_str().try_into()
    }
}

impl<const SIZE: usize, T: FileTypeTag> Display for ObjectId<SIZE, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", String::from(self))
    }
}

/// An ND index to an element in a chunk grid.
#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ChunkIndices(pub Vec<u32>);

/// Byte offset within a chunk.
pub type ChunkOffset = u64;
/// Size of a chunk in bytes.
pub type ChunkLength = u64;

impl<'a> From<generated::ChunkIndices<'a>> for ChunkIndices {
    fn from(value: generated::ChunkIndices<'a>) -> Self {
        ChunkIndices(value.coords().iter().collect())
    }
}

/// A byte range within a chunk or object.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ByteRange {
    /// The fixed length range represented by the given `Range`
    Bounded(Range<ChunkOffset>),
    /// All bytes from the given offset (included) to the end of the object
    From(ChunkOffset),
    /// The last n bytes in the object
    Last(ChunkLength),
    /// All bytes up to the last n bytes in the object
    Until(ChunkOffset),
}

impl From<Range<ChunkOffset>> for ByteRange {
    fn from(value: Range<ChunkOffset>) -> Self {
        ByteRange::Bounded(value)
    }
}

impl ByteRange {
    pub fn from_offset(offset: ChunkOffset) -> Self {
        Self::From(offset)
    }

    pub fn from_offset_with_length(offset: ChunkOffset, length: ChunkOffset) -> Self {
        Self::Bounded(offset..offset + length)
    }

    pub fn to_offset(offset: ChunkOffset) -> Self {
        Self::Bounded(0..offset)
    }

    pub fn bounded(start: ChunkOffset, end: ChunkOffset) -> Self {
        (start..end).into()
    }

    pub const ALL: Self = Self::From(0);

    pub fn slice(&self, bytes: &Bytes) -> Bytes {
        match self {
            ByteRange::Bounded(range) => {
                bytes.slice(range.start as usize..range.end as usize)
            }
            ByteRange::From(from) => bytes.slice(*from as usize..),
            ByteRange::Last(n) => bytes.slice(bytes.len() - *n as usize..),
            ByteRange::Until(n) => bytes.slice(0usize..bytes.len() - *n as usize),
        }
    }
}

impl From<(Option<ChunkOffset>, Option<ChunkOffset>)> for ByteRange {
    fn from((start, end): (Option<ChunkOffset>, Option<ChunkOffset>)) -> Self {
        match (start, end) {
            (Some(start), Some(end)) => Self::Bounded(start..end),
            (Some(start), None) => Self::From(start),
            // NOTE: This is relied upon by zarr python
            (None, Some(end)) => Self::Until(end),
            (None, None) => Self::ALL,
        }
    }
}

/// Offset within a manifest table.
pub type TableOffset = u32;

/// Format-level error types.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum IcechunkFormatErrorKind {
    #[error(transparent)]
    VirtualReferenceError(#[from] VirtualReferenceErrorKind),
    #[error("node not found at `{path:?}`")]
    NodeNotFound { path: Path },
    #[error("chunk coordinates not found `{coords:?}`")]
    ChunkCoordinatesNotFound { coords: ChunkIndices },
    #[error("snapshot id not found `{snapshot_id}`")]
    SnapshotIdNotFound { snapshot_id: SnapshotId },
    #[error("branch already exists `{branch} -> {snapshot_id}`")]
    BranchAlreadyExists { branch: String, snapshot_id: SnapshotId },
    #[error("branch not found `{branch}`")]
    BranchNotFound { branch: String },
    #[error("tag already exists `{tag}`")]
    TagAlreadyExists { tag: String },
    #[error("icechunk does not allow tag reuse and tag was already deleted `{tag}`")]
    TagPreviouslyDeleted { tag: String },
    #[error("tag not found `{tag}`")]
    TagNotFound { tag: String },
    #[error("snapshot id is already present in the repository: `{snapshot_id}`")]
    DuplicateSnapshotId { snapshot_id: SnapshotId },
    #[error("manifest information cannot be found in snapshot for id `{manifest_id}`")]
    ManifestInfoNotFound { manifest_id: ManifestId },
    #[error("invalid magic numbers in file")]
    InvalidMagicNumbers, // TODO: add more info
    #[error(
        "invalid icechunk header size, got {found} bytes, expected at least {expected}"
    )]
    InvalidIcechunkHeaderSize { found: usize, expected: usize },
    #[error("invalid node type {found}, max supported value is {max_supported}")]
    InvalidNodeType { found: u8, max_supported: u8 },
    #[error(
        "this repository uses Icechunk format version {found}, but this library only supports up to version {max_supported}. Please upgrade the icechunk library"
    )]
    InvalidSpecVersion { found: u8, max_supported: u8 },
    #[error("this operation is not supported for Icechunk format version {version}")]
    UnsupportedOperationForVersion { version: u8 },
    #[error("Icechunk cannot read this file type, expected {expected:?} got {got}")]
    InvalidFileType { expected: FileTypeBin, got: u8 }, // TODO: add more info
    #[error("Icechunk encountered an unknown file type code {got}")]
    UnknownFileType { got: u8 },
    #[error("Icechunk cannot read file, invalid compression algorithm")]
    InvalidCompressionAlgorithm, // TODO: add more info
    #[error("Invalid Icechunk metadata file")]
    InvalidFlatBuffer(#[from] InvalidFlatbuffer),
    #[error("error during metadata deserialization")]
    DeserializationError(#[from] Box<rmp_serde::decode::Error>),
    #[error("error during metadata serialization")]
    SerializationError(#[from] Box<rmp_serde::encode::Error>),
    #[error("error during metadata serialization")]
    SerializationErrorFlexBuffers(#[from] Box<flexbuffers::SerializationError>),
    #[error("error during metadata deserialization")]
    DeserializationErrorFlexBuffers(#[from] Box<flexbuffers::DeserializationError>),
    #[error("I/O error")]
    IO(#[from] std::io::Error),
    #[error("path error")]
    Path(#[from] PathError),
    #[error("invalid timestamp in file")]
    InvalidTimestamp,
    #[error(
        "update timestamp is invalid, please verify if the machine clock has drifted: update time: `{new_time}`, latest update time: `{latest_time}`"
    )]
    InvalidUpdateTimestamp { latest_time: DateTime<Utc>, new_time: DateTime<Utc> },
    #[error("invalid feature flag name: {name}")]
    InvalidFeatureFlagName { name: String },
    #[error("invalid feature flag id: {id}")]
    InvalidFeatureFlagId { id: u16 },
    #[error("{feature_description} is disabled by a feature flag ({feature_flag})")]
    FeatureFlagDisabled { feature_description: String, feature_flag: String },
    #[error(
        "compressed chunk location present but no decompression dictionary available"
    )]
    MissingLocationCompressionDictionary,
    #[error("Invalid array metadata: {0}")]
    InvalidArrayMetadata(String),
    #[error("Move operation missing required field '{0}'")]
    MissingRequiredField(String),
}

pub type IcechunkFormatError = ICError<IcechunkFormatErrorKind>;

impl From<Infallible> for IcechunkFormatErrorKind {
    fn from(value: Infallible) -> Self {
        match value {}
    }
}

pub type IcechunkResult<T> = Result<T, IcechunkFormatError>;

/// Binary format constants (file types, spec versions, compression).
// ic[impl format.binary-file] header constants; file assembly lives in the icechunk crate
pub mod format_constants {
    use std::sync::LazyLock;

    use icechunk_types::ICResultExt as _;
    use serde::{Deserialize, Serialize};

    use super::{IcechunkFormatError, IcechunkFormatErrorKind};

    /// Binary file type identifier in the file header.
    #[repr(u8)]
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    // ic[impl snapshot.format] file type byte
    // ic[impl manifest.format] file type byte
    // ic[impl txlog.format] file type byte
    // ic[impl repo.format] file type byte
    pub enum FileTypeBin {
        Snapshot = 1u8,
        Manifest = 2,
        Attributes = 3,
        TransactionLog = 4,
        Chunk = 5,
        RepoInfo = 6,
    }

    impl TryFrom<u8> for FileTypeBin {
        type Error = String;

        fn try_from(value: u8) -> Result<Self, Self::Error> {
            match value {
                n if n == FileTypeBin::Snapshot as u8 => Ok(FileTypeBin::Snapshot),
                n if n == FileTypeBin::Manifest as u8 => Ok(FileTypeBin::Manifest),
                n if n == FileTypeBin::Attributes as u8 => Ok(FileTypeBin::Attributes),
                n if n == FileTypeBin::RepoInfo as u8 => Ok(FileTypeBin::RepoInfo),
                n if n == FileTypeBin::TransactionLog as u8 => {
                    Ok(FileTypeBin::TransactionLog)
                }
                n if n == FileTypeBin::Chunk as u8 => Ok(FileTypeBin::Chunk),
                n => Err(format!("Bad file type code: {n}")),
            }
        }
    }

    /// Icechunk format specification version.
    #[repr(u8)]
    #[derive(
        Debug,
        Clone,
        Copy,
        PartialEq,
        Eq,
        Serialize,
        Deserialize,
        Default,
        PartialOrd,
        Ord,
    )]
    pub enum SpecVersionBin {
        V1 = 1u8,
        #[default]
        V2 = 2u8,
        // When adding new versions here, don't forget to update the
        // PySpecVersion enum in icechunk-python/src/repository.rs too!
    }

    impl TryFrom<u8> for SpecVersionBin {
        type Error = IcechunkFormatErrorKind;

        fn try_from(value: u8) -> Result<Self, Self::Error> {
            match value {
                n if n == SpecVersionBin::V1 as u8 => Ok(SpecVersionBin::V1),
                n if n == SpecVersionBin::V2 as u8 => Ok(SpecVersionBin::V2),
                n => Err(IcechunkFormatErrorKind::InvalidSpecVersion {
                    found: n,
                    max_supported: Self::current() as u8,
                }),
            }
        }
    }

    impl SpecVersionBin {
        pub fn current() -> Self {
            Default::default()
        }
    }

    impl std::fmt::Display for SpecVersionBin {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            let s = match self {
                SpecVersionBin::V1 => "1.0",
                SpecVersionBin::V2 => "2.0",
            };
            write!(f, "{s}")
        }
    }

    /// Compression algorithm used for metadata files.
    #[repr(u8)]
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum CompressionAlgorithmBin {
        None = 0u8,
        Zstd = 1u8,
    }

    impl TryFrom<u8> for CompressionAlgorithmBin {
        type Error = String;

        fn try_from(value: u8) -> Result<Self, Self::Error> {
            match value {
                n if n == CompressionAlgorithmBin::None as u8 => {
                    Ok(CompressionAlgorithmBin::None)
                }
                n if n == CompressionAlgorithmBin::Zstd as u8 => {
                    Ok(CompressionAlgorithmBin::Zstd)
                }
                n => Err(format!("Bad cmpression algorithm code: {n}")),
            }
        }
    }

    // ic[impl format.header] magic bytes
    pub const ICECHUNK_FORMAT_MAGIC_BYTES: &[u8] = "ICE🧊CHUNK".as_bytes();
    // offsets assume a 12-byte magic
    const _: () = assert!(ICECHUNK_FORMAT_MAGIC_BYTES.len() == 12);

    // Binary file header layout: magic | impl name | spec version | file type | compression.
    // Reader (check_header) and writer (binary_file_header) both derive offsets from these.
    // ic[impl format.header] field sizes and offsets
    pub const ICECHUNK_IMPL_NAME_LEN: usize = 24;
    pub const ICECHUNK_SPEC_VERSION_OFFSET: usize =
        ICECHUNK_FORMAT_MAGIC_BYTES.len() + ICECHUNK_IMPL_NAME_LEN;
    pub const ICECHUNK_FILE_TYPE_OFFSET: usize = ICECHUNK_SPEC_VERSION_OFFSET + 1;
    pub const ICECHUNK_COMPRESSION_OFFSET: usize = ICECHUNK_FILE_TYPE_OFFSET + 1;
    pub const ICECHUNK_FILE_HEADER_LEN: usize = ICECHUNK_COMPRESSION_OFFSET + 1;

    pub const LATEST_ICECHUNK_FORMAT_VERSION_METADATA_KEY: &str = "ic_spec_ver";

    pub const ICECHUNK_LIB_VERSION: &str = env!("CARGO_PKG_VERSION");

    pub static ICECHUNK_CLIENT_NAME: LazyLock<String> =
        LazyLock::new(|| "ic-".to_string() + ICECHUNK_LIB_VERSION);
    pub const ICECHUNK_CLIENT_NAME_METADATA_KEY: &str = "ic_client";

    pub const ICECHUNK_FILE_TYPE_SNAPSHOT: &str = "snapshot";
    pub const ICECHUNK_FILE_TYPE_MANIFEST: &str = "manifest";
    pub const ICECHUNK_FILE_TYPE_TRANSACTION_LOG: &str = "transaction-log";
    pub const ICECHUNK_FILE_TYPE_REPO_INFO: &str = "repo-info";
    pub const ICECHUNK_FILE_TYPE_METADATA_KEY: &str = "ic_file_type";

    pub const ICECHUNK_COMPRESSION_METADATA_KEY: &str = "ic_comp_alg";
    pub const ICECHUNK_COMPRESSION_ZSTD: &str = "zstd";

    /// Decoded contents of a metadata file's binary header.
    ///
    /// Parsing of `impl_name` and `app_name` is best effort only.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct FileHeader {
        /// Raw, trimmed implementation/client string, e.g. `"ic-2.1.0"`.
        pub impl_name: String,
        /// Best-effort app name: `impl_name` before the first `'-'` (e.g. `"ic"`).
        pub app_name: String,
        /// Best-effort version: `impl_name` after the first `'-'` (e.g. `"2.1.0"`);
        /// `None` if there is no `'-'`.
        pub app_version: Option<String>,
        pub spec_version: SpecVersionBin,
        pub file_type: FileTypeBin,
        pub compression: CompressionAlgorithmBin,
    }

    /// Parse a file header from the first [`ICECHUNK_FILE_HEADER_LEN`] bytes of any
    /// metadata file.
    ///
    /// Discovers (does not assert) the file type, so it works for every metadata
    /// file kind. Tolerant about the impl-name string content.
    // ic[impl format.header] header parsing
    pub fn parse_file_header(buf: &[u8]) -> Result<FileHeader, IcechunkFormatError> {
        use IcechunkFormatErrorKind as K;

        if buf.len() < ICECHUNK_FILE_HEADER_LEN {
            return Err(K::InvalidIcechunkHeaderSize {
                found: buf.len(),
                expected: ICECHUNK_FILE_HEADER_LEN,
            })
            .capture();
        }
        if !buf.starts_with(ICECHUNK_FORMAT_MAGIC_BYTES) {
            return Err(K::InvalidMagicNumbers).capture();
        }

        let impl_bytes =
            &buf[ICECHUNK_FORMAT_MAGIC_BYTES.len()..ICECHUNK_SPEC_VERSION_OFFSET];
        let impl_name =
            String::from_utf8_lossy(impl_bytes).trim_end_matches([' ', '\0']).to_string();
        let (app_name, app_version) = match impl_name.split_once('-') {
            Some((name, version)) => (name.to_string(), Some(version.to_string())),
            None => (impl_name.clone(), None),
        };

        let spec_version =
            SpecVersionBin::try_from(buf[ICECHUNK_SPEC_VERSION_OFFSET]).capture()?;

        let file_type_byte = buf[ICECHUNK_FILE_TYPE_OFFSET];
        let file_type = FileTypeBin::try_from(file_type_byte)
            .map_err(|_| K::UnknownFileType { got: file_type_byte })
            .capture()?;

        let compression =
            CompressionAlgorithmBin::try_from(buf[ICECHUNK_COMPRESSION_OFFSET])
                .map_err(|_| K::InvalidCompressionAlgorithm)
                .capture()?;

        Ok(FileHeader {
            impl_name,
            app_name,
            app_version,
            spec_version,
            file_type,
            compression,
        })
    }
}

#[inline(always)]
#[expect(clippy::needless_pass_by_value)]
pub fn lookup_index_by_key<'a, T: ::flatbuffers::Follow<'a> + 'a, K: Ord>(
    v: ::flatbuffers::Vector<'a, T>,
    key: K,
    f: fn(&<T as ::flatbuffers::Follow<'a>>::Inner, &K) -> Ordering,
) -> Option<usize> {
    if v.is_empty() {
        return None;
    }

    let mut left: usize = 0;
    let mut right = v.len() - 1;

    while left <= right {
        let mid = (left + right) / 2;
        let value = v.get(mid);
        match f(&value, &key) {
            Ordering::Equal => return Some(mid),
            Ordering::Less => left = mid + 1,
            Ordering::Greater => {
                if mid == 0 {
                    return None;
                }
                right = mid - 1;
            }
        }
    }

    None
}

// This macro is used for creating property tests
// which check that serializing and deserializing
// an instance of a type T is equivalent to the
// identity function
// Given pairs of test names and arbitraries to be used
// for the tests, e.g., (n1, a1), (n2, a2),... (nx, ax)
// the tests can be created by doing
// roundtrip_serialization_tests!(n1 - a1, n2 - a2, .... nx - ax)
#[macro_export]
macro_rules! roundtrip_serialization_tests {
    ($($test_name: ident - $generator: ident), +) => {
        $(
            proptest!{
                #[icechunk_macros::test]
                fn $test_name(elem in $generator()) {
                    let bytes = rmp_serde::to_vec(&elem).unwrap();
                    let roundtrip = rmp_serde::from_slice(&bytes).unwrap();
                    assert_eq!(elem, roundtrip);
                }
            }
        )*
    }
}

#[cfg(test)]
pub mod strategies;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::strategies::{attributes_id, spec_version};
    use pretty_assertions::assert_eq;
    use proptest::prelude::*;

    roundtrip_serialization_tests!(
        serialize_and_deserialize_attribute_ids - attributes_id,
        serialize_and_deserialize_spec_version_bin - spec_version
    );

    // ic[verify types.object-id.encoding]
    #[icechunk_macros::test]
    fn test_object_id_serialization() {
        let sid = SnapshotId::new([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]);
        assert_eq!(
            serde_json::to_string(&sid).unwrap(),
            r#"[[0,1,2,3,4,5,6,7,8,9,10,11],null]"#
        );
        assert_eq!(String::from(&sid), "000G40R40M30E209185G");
        assert_eq!(sid, SnapshotId::try_from("000G40R40M30E209185G").unwrap());
        let sid = SnapshotId::random();
        assert_eq!(
            serde_json::from_slice::<SnapshotId>(
                serde_json::to_vec(&sid).unwrap().as_slice()
            )
            .unwrap(),
            sid,
        );
    }

    #[icechunk_macros::test]
    fn test_unknown_spec_version_gives_nice_error() {
        use format_constants::SpecVersionBin;

        let future_version: u8 = 3;
        let result = SpecVersionBin::try_from(future_version);
        assert!(result.is_err());
        let err = result.unwrap_err();

        let msg = err.to_string();
        assert!(
            msg.contains("format version 3"),
            "Error should mention the found version: {msg}"
        );
        assert!(
            msg.contains("upgrade the icechunk library"),
            "Error should suggest upgrading: {msg}"
        );
    }

    /// Build a binary file header with an arbitrary impl string (right-padded to
    /// `ICECHUNK_IMPL_NAME_LEN`, mirroring `binary_file_header` in the `icechunk`
    /// crate), so tests can exercise `parse_file_header` directly.
    fn make_header(impl_name: &str, spec: u8, file_type: u8, compression: u8) -> Vec<u8> {
        use format_constants::*;
        let mut buf = Vec::with_capacity(ICECHUNK_FILE_HEADER_LEN);
        buf.extend_from_slice(ICECHUNK_FORMAT_MAGIC_BYTES);
        let padded = format!("{impl_name:<ICECHUNK_IMPL_NAME_LEN$}");
        buf.extend_from_slice(&padded.as_bytes()[..ICECHUNK_IMPL_NAME_LEN]);
        buf.push(spec);
        buf.push(file_type);
        buf.push(compression);
        buf
    }

    // ic[verify format.header]
    #[icechunk_macros::test]
    fn test_parse_file_header_roundtrip() {
        use format_constants::*;
        let mut buf = make_header(
            ICECHUNK_CLIENT_NAME.as_str(),
            SpecVersionBin::V2 as u8,
            FileTypeBin::Snapshot as u8,
            CompressionAlgorithmBin::Zstd as u8,
        );
        // body bytes after the header must be ignored
        buf.extend_from_slice(b"a compressed body would go here");

        let header = parse_file_header(&buf).unwrap();
        assert_eq!(header.impl_name, *ICECHUNK_CLIENT_NAME);
        assert_eq!(header.app_name, "ic");
        assert_eq!(header.app_version.as_deref(), Some(ICECHUNK_LIB_VERSION));
        assert_eq!(header.spec_version, SpecVersionBin::V2);
        assert_eq!(header.file_type, FileTypeBin::Snapshot);
        assert_eq!(header.compression, CompressionAlgorithmBin::Zstd);
    }

    #[icechunk_macros::test]
    fn test_parse_file_header_trims_padding() {
        use format_constants::*;
        // exactly the header, no body: the 24-byte impl field is space-padded
        let buf = make_header(
            "ic-9.9.9",
            SpecVersionBin::V1 as u8,
            FileTypeBin::Manifest as u8,
            CompressionAlgorithmBin::None as u8,
        );
        let header = parse_file_header(&buf).unwrap();
        assert_eq!(header.impl_name, "ic-9.9.9");
        assert_eq!(header.app_name, "ic");
        assert_eq!(header.app_version.as_deref(), Some("9.9.9"));
        assert_eq!(header.spec_version, SpecVersionBin::V1);
        assert_eq!(header.file_type, FileTypeBin::Manifest);
        assert_eq!(header.compression, CompressionAlgorithmBin::None);
    }

    #[icechunk_macros::test]
    fn test_parse_file_header_splits_on_first_dash() {
        use format_constants::*;
        let buf = make_header(
            "ic-2.0.0-alpha.4",
            SpecVersionBin::V2 as u8,
            FileTypeBin::RepoInfo as u8,
            CompressionAlgorithmBin::Zstd as u8,
        );
        let header = parse_file_header(&buf).unwrap();
        assert_eq!(header.impl_name, "ic-2.0.0-alpha.4");
        assert_eq!(header.app_name, "ic");
        assert_eq!(header.app_version.as_deref(), Some("2.0.0-alpha.4"));
    }

    #[icechunk_macros::test]
    fn test_parse_file_header_no_dash_fallback() {
        use format_constants::*;
        let buf = make_header(
            "icjs",
            SpecVersionBin::V2 as u8,
            FileTypeBin::TransactionLog as u8,
            CompressionAlgorithmBin::Zstd as u8,
        );
        let header = parse_file_header(&buf).unwrap();
        assert_eq!(header.impl_name, "icjs");
        assert_eq!(header.app_name, "icjs");
        assert_eq!(header.app_version, None);
    }

    #[icechunk_macros::test]
    fn test_parse_file_header_bad_magic() {
        use format_constants::*;
        let mut buf = make_header(
            "ic-1.0.0",
            SpecVersionBin::V2 as u8,
            FileTypeBin::Snapshot as u8,
            CompressionAlgorithmBin::Zstd as u8,
        );
        buf[0] = b'X';
        let err = parse_file_header(&buf).unwrap_err();
        assert!(matches!(err.kind(), IcechunkFormatErrorKind::InvalidMagicNumbers));
    }

    #[icechunk_macros::test]
    fn test_parse_file_header_too_short() {
        use format_constants::*;
        let buf = vec![0u8; ICECHUNK_FILE_HEADER_LEN - 1];
        let err = parse_file_header(&buf).unwrap_err();
        assert!(matches!(
            err.kind(),
            IcechunkFormatErrorKind::InvalidIcechunkHeaderSize { .. }
        ));
    }

    #[icechunk_macros::test]
    fn test_parse_file_header_unknown_file_type() {
        use format_constants::*;
        let buf = make_header(
            "ic-1.0.0",
            SpecVersionBin::V2 as u8,
            99,
            CompressionAlgorithmBin::Zstd as u8,
        );
        let err = parse_file_header(&buf).unwrap_err();
        assert!(matches!(
            err.kind(),
            IcechunkFormatErrorKind::UnknownFileType { got: 99 }
        ));
    }

    #[icechunk_macros::test]
    fn test_parse_file_header_bad_spec_version() {
        use format_constants::*;
        let buf = make_header(
            "ic-1.0.0",
            99,
            FileTypeBin::Snapshot as u8,
            CompressionAlgorithmBin::Zstd as u8,
        );
        let err = parse_file_header(&buf).unwrap_err();
        assert!(matches!(
            err.kind(),
            IcechunkFormatErrorKind::InvalidSpecVersion { found: 99, .. }
        ));
    }

    // ic[verify layout.paths]
    // ic[verify repo.path]
    // ic[verify chunks.path]
    // ic[verify manifest.path]
    // ic[verify snapshot.path]
    // ic[verify txlog.required] path construction
    #[icechunk_macros::test]
    fn test_storage_paths_match_spec() {
        use crate::snapshot::Snapshot;

        assert_eq!(REPO_INFO_FILE_PATH, "repo");
        assert_eq!(CHUNKS_FILE_PATH, "chunks");
        assert_eq!(MANIFESTS_FILE_PATH, "manifests");
        assert_eq!(SNAPSHOTS_FILE_PATH, "snapshots");
        assert_eq!(TRANSACTION_LOGS_FILE_PATH, "transactions");
        assert_eq!(OVERWRITTEN_FILES_PATH, "overwritten");

        // The spec's worked example: the initial snapshot is stored at
        // snapshots/1CECHNKREP0F1RSTCMT0, and its transaction log shares
        // the same file name under transactions/.
        let file_name = String::from(&Snapshot::INITIAL_SNAPSHOT_ID);
        assert_eq!(
            format!("{SNAPSHOTS_FILE_PATH}/{file_name}"),
            "snapshots/1CECHNKREP0F1RSTCMT0"
        );
        assert_eq!(
            format!("{TRANSACTION_LOGS_FILE_PATH}/{file_name}"),
            "transactions/1CECHNKREP0F1RSTCMT0"
        );
    }

    // ic[verify format.binary-file]
    // ic[verify repo.format]
    // ic[verify snapshot.format]
    // ic[verify manifest.format]
    // ic[verify txlog.format]
    #[icechunk_macros::test]
    fn test_binary_container_roundtrip_all_file_types()
    -> Result<(), Box<dyn std::error::Error>> {
        use bytes::Bytes;
        use chrono::DateTime;
        use format_constants::*;

        use crate::{
            manifest::{ChunkInfo, ChunkPayload, Manifest},
            repo_info::RepoInfo,
            serializers::{
                deserialize_manifest, deserialize_repo_info, deserialize_snapshot,
                deserialize_transaction_log, serialize_manifest, serialize_repo_info,
                serialize_snapshot, serialize_transaction_log,
            },
            snapshot::{Snapshot, SnapshotInfo},
            transaction_log::TransactionLog,
        };

        let spec = SpecVersionBin::V2;

        // Mirror the file assembly done in the icechunk crate: the standard
        // binary header followed by the zstd-compressed flatbuffers payload.
        let to_file =
            |file_type: FileTypeBin, payload: &[u8]| -> Result<Vec<u8>, std::io::Error> {
                let mut buf = make_header(
                    ICECHUNK_CLIENT_NAME.as_str(),
                    SpecVersionBin::V2 as u8,
                    file_type as u8,
                    CompressionAlgorithmBin::Zstd as u8,
                );
                buf.extend_from_slice(&zstd::encode_all(payload, 1)?);
                Ok(buf)
            };
        let from_file = |buf: &[u8],
                         expected: FileTypeBin|
         -> Result<Vec<u8>, Box<dyn std::error::Error>> {
            let header = parse_file_header(buf)?;
            assert_eq!(header.spec_version, SpecVersionBin::V2);
            assert_eq!(header.file_type, expected);
            assert_eq!(header.compression, CompressionAlgorithmBin::Zstd);
            Ok(zstd::decode_all(&buf[ICECHUNK_FILE_HEADER_LEN..])?)
        };

        let snapshot = Snapshot::initial(spec)?;
        let mut payload = Vec::new();
        serialize_snapshot(&snapshot, spec, &mut payload)?;
        let file = to_file(FileTypeBin::Snapshot, &payload)?;
        let back = deserialize_snapshot(spec, from_file(&file, FileTypeBin::Snapshot)?)?;
        assert_eq!(back.bytes(), snapshot.bytes());

        let chunk = ChunkInfo {
            node: NodeId::random(),
            coord: ChunkIndices(vec![0]),
            payload: ChunkPayload::Inline(Bytes::from_static(b"container test")),
        };
        let manifest =
            Manifest::from_sorted_vec(&ManifestId::random(), vec![chunk], None)?
                .expect("non-empty manifest");
        let mut payload = Vec::new();
        serialize_manifest(&manifest, spec, &mut payload)?;
        let file = to_file(FileTypeBin::Manifest, &payload)?;
        let back = deserialize_manifest(spec, from_file(&file, FileTypeBin::Manifest)?)?;
        assert_eq!(back.bytes(), manifest.bytes());

        let tx = TransactionLog::new_from_parts(
            &Snapshot::INITIAL_SNAPSHOT_ID,
            std::iter::empty::<NodeId>(),
            std::iter::empty::<NodeId>(),
            std::iter::empty::<NodeId>(),
            std::iter::empty::<NodeId>(),
            std::iter::empty::<NodeId>(),
            std::iter::empty::<NodeId>(),
            std::iter::empty::<(NodeId, std::iter::Empty<ChunkIndices>)>(),
            std::iter::empty(),
        );
        let mut payload = Vec::new();
        serialize_transaction_log(&tx, spec, &mut payload)?;
        let file = to_file(FileTypeBin::TransactionLog, &payload)?;
        let back = deserialize_transaction_log(
            spec,
            from_file(&file, FileTypeBin::TransactionLog)?,
        )?;
        assert_eq!(back.bytes(), tx.bytes());

        let snap_info = SnapshotInfo {
            id: Snapshot::INITIAL_SNAPSHOT_ID,
            parent_id: None,
            flushed_at: DateTime::from_timestamp_micros(1_000_000)
                .expect("valid timestamp"),
            message: "initial".to_string(),
            metadata: Default::default(),
            pruned_ancestor_tx_logs: vec![],
        };
        let repo = RepoInfo::initial(spec, snap_info, 100, None::<&()>, None);
        let mut payload = Vec::new();
        serialize_repo_info(&repo, spec, &mut payload)?;
        let file = to_file(FileTypeBin::RepoInfo, &payload)?;
        let back = deserialize_repo_info(spec, from_file(&file, FileTypeBin::RepoInfo)?)?;
        assert_eq!(back.bytes(), repo.bytes());

        Ok(())
    }
}
