use core::fmt;
use std::{
    fmt::{Debug, Display},
    hash::Hash,
    marker::PhantomData,
    ops::Bound,
    path::PathBuf,
};

use bytes::Bytes;
use itertools::Itertools;
use rand::{thread_rng, Rng};
use serde::{Deserialize, Deserializer, Serialize};
use thiserror::Error;

use crate::metadata::DataType;

pub mod attributes;
pub mod manifest;
pub mod snapshot;

pub type Path = PathBuf;

#[allow(dead_code)]
pub trait FileTypeTag {}

/// The id of a file in object store
#[derive(Hash, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct ObjectId<const SIZE: usize, T: FileTypeTag>(pub [u8; SIZE], PhantomData<T>);

#[derive(Hash, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct SnapshotTag;

#[derive(Hash, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct ManifestTag;

#[derive(Hash, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct ChunkTag;

#[derive(Hash, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct AttributesTag;

impl FileTypeTag for SnapshotTag {}
impl FileTypeTag for ManifestTag {}
impl FileTypeTag for ChunkTag {}
impl FileTypeTag for AttributesTag {}

// A 1e-9 conflict probability requires 2^33 ~ 8.5 bn chunks
// using this site for the calculations: https://www.bdayprob.com/
pub type SnapshotId = ObjectId<12, SnapshotTag>;
pub type ManifestId = ObjectId<12, ManifestTag>;
pub type ChunkId = ObjectId<12, ChunkTag>;
pub type AttributesId = ObjectId<12, AttributesTag>;

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

impl<const SIZE: usize, T: FileTypeTag> Display for ObjectId<SIZE, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", String::from(self))
    }
}

impl<const SIZE: usize, T: FileTypeTag> Serialize for ObjectId<SIZE, T> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        // Just use the string representation
        serializer.serialize_str(&String::from(self))
    }
}

impl<'de, const SIZE: usize, T: FileTypeTag> Deserialize<'de> for ObjectId<SIZE, T> {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        // Because we implement TryFrom<&str> for ObjectId, we can use it here instead of
        // having to implement a custom deserializer
        let s = String::deserialize(d)?;
        ObjectId::try_from(s.as_str()).map_err(serde::de::Error::custom)
    }
}

/// The internal id of an array or group, unique only to a single store version
pub type NodeId = u32;

#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
/// An ND index to an element in a chunk grid.
pub struct ChunkIndices(pub Vec<u32>);

pub type ChunkOffset = u64;
pub type ChunkLength = u64;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ByteRange(pub Bound<ChunkOffset>, pub Bound<ChunkOffset>);

impl ByteRange {
    pub fn from_offset(offset: ChunkOffset) -> Self {
        Self(Bound::Included(offset), Bound::Unbounded)
    }

    pub fn from_offset_with_length(offset: ChunkOffset, length: ChunkOffset) -> Self {
        Self(Bound::Included(offset), Bound::Excluded(offset + length))
    }

    pub fn to_offset(offset: ChunkOffset) -> Self {
        Self(Bound::Unbounded, Bound::Excluded(offset))
    }

    pub fn bounded(start: ChunkOffset, end: ChunkOffset) -> Self {
        Self(Bound::Included(start), Bound::Excluded(end))
    }

    pub fn length(&self) -> Option<u64> {
        match (self.0, self.1) {
            (_, Bound::Unbounded) => None,
            (Bound::Unbounded, Bound::Excluded(end)) => Some(end),
            (Bound::Unbounded, Bound::Included(end)) => Some(end + 1),
            (Bound::Included(start), Bound::Excluded(end)) => Some(end - start),
            (Bound::Excluded(start), Bound::Included(end)) => Some(end - start),
            (Bound::Included(start), Bound::Included(end)) => Some(end - start + 1),
            (Bound::Excluded(start), Bound::Excluded(end)) => Some(end - start - 1),
        }
    }

    pub const ALL: Self = Self(Bound::Unbounded, Bound::Unbounded);

    pub fn slice(&self, bytes: Bytes) -> Bytes {
        match (self.0, self.1) {
            (Bound::Included(start), Bound::Excluded(end)) => {
                bytes.slice(start as usize..end as usize)
            }
            (Bound::Included(start), Bound::Unbounded) => bytes.slice(start as usize..),
            (Bound::Unbounded, Bound::Excluded(end)) => bytes.slice(..end as usize),
            (Bound::Excluded(start), Bound::Excluded(end)) => {
                bytes.slice(start as usize + 1..end as usize)
            }
            (Bound::Excluded(start), Bound::Unbounded) => {
                bytes.slice(start as usize + 1..)
            }
            (Bound::Unbounded, Bound::Included(end)) => bytes.slice(..=end as usize),
            (Bound::Included(start), Bound::Included(end)) => {
                bytes.slice(start as usize..=end as usize)
            }
            (Bound::Excluded(start), Bound::Included(end)) => {
                bytes.slice(start as usize + 1..=end as usize)
            }
            (Bound::Unbounded, Bound::Unbounded) => bytes,
        }
    }
}

impl From<(Option<ChunkOffset>, Option<ChunkOffset>)> for ByteRange {
    fn from((start, end): (Option<ChunkOffset>, Option<ChunkOffset>)) -> Self {
        match (start, end) {
            (Some(start), Some(end)) => {
                Self(Bound::Included(start), Bound::Excluded(end))
            }
            (Some(start), None) => Self(Bound::Included(start), Bound::Unbounded),
            (None, Some(end)) => Self(Bound::Unbounded, Bound::Excluded(end)),
            (None, None) => Self(Bound::Unbounded, Bound::Unbounded),
        }
    }
}

pub type TableOffset = u32;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Flags(); // FIXME: implement

#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum IcechunkFormatError {
    #[error("error decoding fill_value from array")]
    FillValueDecodeError { found_size: usize, target_size: usize, target_type: DataType },
    #[error("error decoding fill_value from json")]
    FillValueParse { data_type: DataType, value: serde_json::Value },
    #[error("node not found at `{path:?}`")]
    NodeNotFound { path: Path },
    #[error("chunk coordinates not found `{coords:?}`")]
    ChunkCoordinatesNotFound { coords: ChunkIndices },
}

pub type IcechunkResult<T> = Result<T, IcechunkFormatError>;

#[cfg(test)]
#[allow(clippy::panic, clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn test_object_id_serialization() {
        let sid = SnapshotId::new([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]);
        assert_eq!(serde_json::to_string(&sid).unwrap(), r#""000G40R40M30E209185G""#);
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
