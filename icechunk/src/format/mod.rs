use core::fmt;
use std::{
    convert::Infallible,
    fmt::{Debug, Display},
    hash::Hash,
    marker::PhantomData,
    ops::Range,
};

use bytes::Bytes;
use itertools::Itertools;
use rand::{thread_rng, Rng};
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, TryFromInto};
use thiserror::Error;
use typed_path::Utf8UnixPathBuf;

use crate::{metadata::DataType, private};

pub mod attributes;
pub mod manifest;
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
        thread_rng().fill(&mut buf[..]);
        Self(buf, PhantomData)
    }

    pub fn new(buf: [u8; SIZE]) -> Self {
        Self(buf, PhantomData)
    }

    pub const FAKE: Self = Self([0; SIZE], PhantomData);
}

impl<const SIZE: usize, T: FileTypeTag> fmt::Debug for ObjectId<SIZE, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:02x}", self.0.iter().format(""))
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

#[derive(Debug, Clone, Error, PartialEq, Eq)]
#[non_exhaustive]
pub enum IcechunkFormatError {
    #[error("error decoding fill_value from array")]
    FillValueDecodeError { found_size: usize, target_size: usize, target_type: DataType },
    #[error("error decoding fill_value from json")]
    FillValueParse { data_type: DataType, value: serde_json::Value },
    #[error("node not found at `{path:?}`")]
    NodeNotFound { path: Path },
    #[error("chunk coordinates not found `{coords:?}`")]
    ChunkCoordinatesNotFound { coords: ChunkIndices },
    #[error("manifest information cannot be found in snapshot `{manifest_id}`")]
    ManifestInfoNotFound { manifest_id: ManifestId },
    #[error("invalid magic numbers in file")]
    InvalidMagicNumbers, // TODO: add more info
    #[error("Icechunk cannot read from repository written with a more modern version")]
    InvalidSpecVersion, // TODO: add more info
    #[error("Icechunk cannot read this file type, expected {expected} got {got}")]
    InvalidFileType { expected: u8, got: u8 }, // TODO: add more info
}

pub type IcechunkResult<T> = Result<T, IcechunkFormatError>;

pub type IcechunkFormatVersion = u8;

pub mod format_constants {
    use std::sync::LazyLock;

    pub const ICECHUNK_FORMAT_MAGIC_BYTES: &[u8] = "ICE🧊CHUNK".as_bytes();
    pub const LATEST_ICECHUNK_SPEC_VERSION_BINARY: u8 = 1;

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

    pub const ICECHUNK_FILE_TYPE_BINARY_SNAPSHOT: u8 = 1;
    pub const ICECHUNK_FILE_TYPE_BINARY_MANIFEST: u8 = 2;
    pub const ICECHUNK_FILE_TYPE_BINARY_ATTRIBUTES: u8 = 3;
    pub const ICECHUNK_FILE_TYPE_BINARY_TRANSACTION_LOG: u8 = 4;

    pub const ICECHUNK_COMPRESSION_BINARY_ZSTD: u8 = 1;
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

    #[test]
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
