use core::fmt;
use std::{
    fmt::{Debug, Display},
    hash::Hash,
    ops::Bound,
    path::PathBuf,
};

use bytes::Bytes;
use itertools::Itertools;
use serde::{Deserialize, Deserializer, Serialize};
use serde_with::serde_as;
use thiserror::Error;
use schemars::JsonSchema;

use crate::metadata::DataType;

pub mod attributes;
pub mod manifest;
pub mod snapshot;

pub type Path = PathBuf;

/// The id of a file in object store
// FIXME: should this be passed by ref everywhere?
#[serde_as]
#[derive(Hash, Clone, PartialEq, Eq, PartialOrd, Ord, JsonSchema)]
pub struct ObjectId(pub [u8; 16]); // FIXME: this doesn't need to be this big

impl ObjectId {
    pub fn random() -> Self {
        Self(rand::random())
    }

    pub const FAKE: Self = Self([0; 16]);
}

impl fmt::Debug for ObjectId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:02x}", self.0.iter().format(""))
    }
}

impl TryFrom<&[u8]> for ObjectId {
    type Error = &'static str;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        let buf = value.try_into();
        buf.map(ObjectId).map_err(|_| "Invalid ObjectId buffer length")
    }
}

impl TryFrom<&str> for ObjectId {
    type Error = &'static str;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let bytes = base32::decode(base32::Alphabet::Crockford, value);
        let Some(bytes) = bytes else { return Err("Invalid ObjectId string") };
        Self::try_from(bytes.as_slice())
    }
}

impl From<&ObjectId> for String {
    fn from(value: &ObjectId) -> Self {
        base32::encode(base32::Alphabet::Crockford, &value.0)
    }
}

impl Display for ObjectId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", String::from(self))
    }
}

impl Serialize for ObjectId {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        // Just use the string representation
        serializer.serialize_str(&String::from(self))
    }
}

impl<'de> Deserialize<'de> for ObjectId {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        // Because we implement TryFrom<&str> for ObjectId, we can use it here instead of
        // having to implement a custom deserializer
        let s = String::deserialize(d)?;
        ObjectId::try_from(s.as_str()).map_err(serde::de::Error::custom)
    }
}

/// The internal id of an array or group, unique only to a single store version
pub type NodeId = u32;

#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, JsonSchema)]
/// An ND index to an element in a chunk grid.
pub struct ChunkIndices(pub Vec<u64>);

pub type ChunkOffset = u64;
pub type ChunkLength = u64;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ByteRange(pub Bound<ChunkOffset>, pub Bound<ChunkOffset>);

impl ByteRange {
    pub fn from_offset(offset: ChunkOffset) -> Self {
        Self(Bound::Included(offset), Bound::Unbounded)
    }

    pub fn to_offset(offset: ChunkOffset) -> Self {
        Self(Bound::Unbounded, Bound::Excluded(offset))
    }

    pub fn bounded(start: ChunkOffset, end: ChunkOffset) -> Self {
        Self(Bound::Included(start), Bound::Excluded(end))
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
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
        let sid = ObjectId([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]);
        assert_eq!(
            serde_json::to_string(&sid).unwrap(),
            r#""000G40R40M30E209185GR38E1W""#
        );
        assert_eq!(String::from(&sid), "000G40R40M30E209185GR38E1W");
        assert_eq!(sid, ObjectId::try_from("000G40R40M30E209185GR38E1W").unwrap());
        let sid = ObjectId::random();
        assert_eq!(
            serde_json::from_slice::<ObjectId>(
                serde_json::to_vec(&sid).unwrap().as_slice()
            )
            .unwrap(),
            sid,
        );
    }
}
