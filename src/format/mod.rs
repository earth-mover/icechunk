use core::fmt;
use std::path::PathBuf;

use itertools::Itertools;
use thiserror::Error;

use crate::metadata::DataType;

pub mod attributes;
pub mod manifest;
pub mod structure;

pub type Path = PathBuf;

/// The id of a file in object store
/// FIXME: should this be passed by ref everywhere?
#[derive(Hash, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct ObjectId(pub [u8; 16]); // FIXME: this doesn't need to be this big

impl ObjectId {
    const SIZE: usize = 16;

    pub fn random() -> ObjectId {
        ObjectId(rand::random())
    }

    pub const FAKE: ObjectId = ObjectId([0; 16]);
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

/// The internal id of an array or group, unique only to a single store version
pub type NodeId = u32;

#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
/// An ND index to an element in a chunk grid.
pub struct ChunkIndices(pub Vec<u64>);

pub type ChunkOffset = u64;
pub type ChunkLength = u64;

pub type TableOffset = u32;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableRegion(pub TableOffset, pub TableOffset);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Flags(); // FIXME: implement

#[derive(Debug, Clone, Error)]
pub enum IcechunkFormatError {
    #[error("error decoding fill_value from array")]
    FillValueDecodeError { found_size: usize, target_size: usize, target_type: DataType },
    #[error("error decoding fill_value from json")]
    FillValueParse { data_type: DataType, value: serde_json::Value },
    #[error("null found decoding fill_value")]
    NullFillValueError,
}
