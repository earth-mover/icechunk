use core::fmt;
use std::{
    convert::Infallible,
    fmt::{Debug, Display},
    hash::Hash,
    marker::PhantomData,
    ops::Range,
};

use ::flatbuffers::InvalidFlatbuffer;
use bytes::Bytes;
use flatbuffers::generated;
use format_constants::FileTypeBin;
use manifest::{VirtualReferenceError, VirtualReferenceErrorKind};
use rand::{Rng, rng};
use serde::{Deserialize, Serialize};
use serde_with::{TryFromInto, serde_as};
use thiserror::Error;
use typed_path::Utf8UnixPathBuf;

use crate::{error::ICError, private};

pub mod attributes;
pub mod manifest;

#[allow(
    dead_code,
    unused_imports,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::needless_lifetimes,
    clippy::extra_unused_lifetimes,
    clippy::missing_safety_doc,
    clippy::derivable_impls
)]
#[path = "./flatbuffers/all_generated.rs"]
pub mod flatbuffers;

pub mod serializers;
pub mod snapshot;
pub mod transaction_log;

#[serde_as]
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Serialize, Deserialize)]
pub struct Path(#[serde_as(as = "TryFromInto<String>")] Utf8UnixPathBuf);

#[allow(dead_code)]
pub trait FileTypeTag: private::Sealed {}

/// The id of a file in object store
#[derive(Hash, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ObjectId<const SIZE: usize, T: FileTypeTag>(
    #[serde(with = "serde_bytes")] pub [u8; SIZE],
    PhantomData<T>,
);

#[derive(Hash, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct SnapshotTag;

#[derive(Hash, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct ManifestTag;

#[derive(Hash, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct ChunkTag;

#[derive(Hash, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct AttributesTag;

#[derive(Hash, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct NodeTag;

impl private::Sealed for SnapshotTag {}
impl private::Sealed for ManifestTag {}
impl private::Sealed for ChunkTag {}
impl private::Sealed for AttributesTag {}
impl private::Sealed for NodeTag {}
impl FileTypeTag for SnapshotTag {}
impl FileTypeTag for ManifestTag {}
impl FileTypeTag for ChunkTag {}
impl FileTypeTag for AttributesTag {}
impl FileTypeTag for NodeTag {}

// A 1e-9 conflict probability requires 2^33 ~ 8.5 bn chunks
// using this site for the calculations: https://www.bdayprob.com/
pub type SnapshotId = ObjectId<12, SnapshotTag>;
pub type ManifestId = ObjectId<12, ManifestTag>;
pub type ChunkId = ObjectId<12, ChunkTag>;
pub type AttributesId = ObjectId<12, AttributesTag>;

// A 1e-9 conflict probability requires 2^17.55 ~ 200k nodes
/// The internal id of an array or group, unique only to a single store version
pub type NodeId = ObjectId<8, NodeTag>;

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

impl<const SIZE: usize, T: FileTypeTag> fmt::Debug for ObjectId<SIZE, T> {
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

#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
/// An ND index to an element in a chunk grid.
pub struct ChunkIndices(pub Vec<u32>);

pub type ChunkOffset = u64;
pub type ChunkLength = u64;

impl<'a> From<generated::ChunkIndices<'a>> for ChunkIndices {
    fn from(value: generated::ChunkIndices<'a>) -> Self {
        ChunkIndices(value.coords().iter().collect())
    }
}

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

    pub fn slice(&self, bytes: Bytes) -> Bytes {
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

pub type TableOffset = u32;

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum IcechunkFormatErrorKind {
    #[error(transparent)]
    VirtualReferenceError(VirtualReferenceErrorKind),
    #[error("node not found at `{path:?}`")]
    NodeNotFound { path: Path },
    #[error("chunk coordinates not found `{coords:?}`")]
    ChunkCoordinatesNotFound { coords: ChunkIndices },
    #[error("manifest information cannot be found in snapshot for id `{manifest_id}`")]
    ManifestInfoNotFound { manifest_id: ManifestId },
    #[error("invalid magic numbers in file")]
    InvalidMagicNumbers, // TODO: add more info
    #[error("Icechunk cannot read from repository written with a more modern version")]
    InvalidSpecVersion, // TODO: add more info
    #[error("Icechunk cannot read this file type, expected {expected:?} got {got}")]
    InvalidFileType { expected: FileTypeBin, got: u8 }, // TODO: add more info
    #[error("Icechunk cannot read file, invalid compression algorithm")]
    InvalidCompressionAlgorithm, // TODO: add more info
    #[error("Invalid Icechunk metadata file")]
    InvalidFlatBuffer(#[from] InvalidFlatbuffer),
    #[error("error during metadata file deserialization")]
    DeserializationError(#[from] Box<rmp_serde::decode::Error>),
    #[error("error during metadata file serialization")]
    SerializationError(#[from] Box<rmp_serde::encode::Error>),
    #[error("I/O error")]
    IO(#[from] std::io::Error),
    #[error("path error")]
    Path(#[from] PathError),
    #[error("invalid timestamp in file")]
    InvalidTimestamp,
}

pub type IcechunkFormatError = ICError<IcechunkFormatErrorKind>;

// it would be great to define this impl in error.rs, but it conflicts with the blanket
// `impl From<T> for T`
impl<E> From<E> for IcechunkFormatError
where
    E: Into<IcechunkFormatErrorKind>,
{
    fn from(value: E) -> Self {
        Self::new(value.into())
    }
}

impl From<VirtualReferenceError> for IcechunkFormatError {
    fn from(value: VirtualReferenceError) -> Self {
        Self::with_context(
            IcechunkFormatErrorKind::VirtualReferenceError(value.kind),
            value.context,
        )
    }
}

impl From<Infallible> for IcechunkFormatErrorKind {
    fn from(value: Infallible) -> Self {
        match value {}
    }
}

pub type IcechunkResult<T> = Result<T, IcechunkFormatError>;

pub mod format_constants {
    use std::sync::LazyLock;

    #[repr(u8)]
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum FileTypeBin {
        Snapshot = 1u8,
        Manifest = 2,
        Attributes = 3,
        TransactionLog = 4,
        Chunk = 5,
    }

    impl TryFrom<u8> for FileTypeBin {
        type Error = String;

        fn try_from(value: u8) -> Result<Self, Self::Error> {
            match value {
                n if n == FileTypeBin::Snapshot as u8 => Ok(FileTypeBin::Snapshot),
                n if n == FileTypeBin::Manifest as u8 => Ok(FileTypeBin::Manifest),
                n if n == FileTypeBin::Attributes as u8 => Ok(FileTypeBin::Attributes),
                n if n == FileTypeBin::TransactionLog as u8 => {
                    Ok(FileTypeBin::TransactionLog)
                }
                n if n == FileTypeBin::Chunk as u8 => Ok(FileTypeBin::Chunk),
                n => Err(format!("Bad file type code: {n}")),
            }
        }
    }

    #[repr(u8)]
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum SpecVersionBin {
        V0dot1 = 1u8,
    }

    impl TryFrom<u8> for SpecVersionBin {
        type Error = String;

        fn try_from(value: u8) -> Result<Self, Self::Error> {
            match value {
                n if n == SpecVersionBin::V0dot1 as u8 => Ok(SpecVersionBin::V0dot1),
                n => Err(format!("Bad spec version code: {n}")),
            }
        }
    }

    impl SpecVersionBin {
        pub fn current() -> Self {
            Self::V0dot1
        }
    }

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
    pub const ICECHUNK_FILE_TYPE_METADATA_KEY: &str = "ic_file_type";

    pub const ICECHUNK_COMPRESSION_METADATA_KEY: &str = "ic_comp_alg";
    pub const ICECHUNK_COMPRESSION_ZSTD: &str = "zstd";
}

impl Display for Path {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone, Error, PartialEq, Eq)]
#[non_exhaustive]
pub enum PathError {
    #[error("path must start with a `/` character")]
    NotAbsolute,
    #[error(r#"path must be cannonic, cannot include "." or "..""#)]
    NotCanonic,
}

impl Path {
    pub fn root() -> Path {
        Path(Utf8UnixPathBuf::from("/".to_string()))
    }

    pub fn new(path: &str) -> Result<Path, PathError> {
        let buf = Utf8UnixPathBuf::from(path);
        if !buf.is_absolute() {
            return Err(PathError::NotAbsolute);
        }

        if buf.normalize() != buf {
            return Err(PathError::NotCanonic);
        }
        Ok(Path(buf))
    }

    pub fn starts_with(&self, other: &Path) -> bool {
        self.0.starts_with(&other.0)
    }

    pub fn ancestors(&self) -> impl Iterator<Item = Path> + '_ {
        self.0.ancestors().map(|p| Path(p.to_owned()))
    }

    pub fn name(&self) -> Option<&str> {
        self.0.file_name()
    }
}

impl TryFrom<&str> for Path {
    type Error = PathError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<&String> for Path {
    type Error = PathError;

    fn try_from(value: &String) -> Result<Self, Self::Error> {
        value.as_str().try_into()
    }
}

impl TryFrom<String> for Path {
    type Error = PathError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.as_str().try_into()
    }
}

#[cfg(test)]
#[allow(clippy::panic, clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;

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
}
