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
pub const REPO_INFO_FILE_PATH: &str = "repo";
pub const CHUNKS_FILE_PATH: &str = "chunks";
pub const MANIFESTS_FILE_PATH: &str = "manifests";
pub const SNAPSHOTS_FILE_PATH: &str = "snapshots";
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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Move {
    pub from: Path,
    pub to: Path,
    pub node_id: NodeId,
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

impl<const SIZE: usize, T: FileTypeTag> TryFrom<&str> for ObjectId<SIZE, T> {
    type Error = &'static str;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let bytes = base32::decode(base32::Alphabet::Crockford, value);
        let Some(bytes) = bytes else { return Err("Invalid ObjectId string") };
        Self::try_from(bytes.as_slice())
    }
}

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
        "this repository uses Icechunk format version {found}, but this library only supports up to version {max_supported}. Please upgrade the icechunk library"
    )]
    InvalidSpecVersion { found: u8, max_supported: u8 },
    #[error("this operation is not supported for Icechunk format version {version}")]
    UnsupportedOperationForVersion { version: u8 },
    #[error("Icechunk cannot read this file type, expected {expected:?} got {got}")]
    InvalidFileType { expected: FileTypeBin, got: u8 }, // TODO: add more info
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
}

pub type IcechunkFormatError = ICError<IcechunkFormatErrorKind>;

impl From<Infallible> for IcechunkFormatErrorKind {
    fn from(value: Infallible) -> Self {
        match value {}
    }
}

pub type IcechunkResult<T> = Result<T, IcechunkFormatError>;

/// Binary format constants (file types, spec versions, compression).
pub mod format_constants {
    use std::sync::LazyLock;

    use serde::{Deserialize, Serialize};

    use super::IcechunkFormatErrorKind;

    /// Binary file type identifier in the file header.
    #[repr(u8)]
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
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

    pub const ICECHUNK_FORMAT_MAGIC_BYTES: &[u8] = "ICE🧊CHUNK".as_bytes();

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
}
