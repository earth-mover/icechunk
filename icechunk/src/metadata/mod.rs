use std::{collections::HashMap, num::NonZeroU64};

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use test_strategy::Arbitrary;

pub mod data_type;
pub mod fill_value;

pub use data_type::DataType;
pub use fill_value::FillValue;

/// The shape of an array.
/// 0 is a valid shape member
pub type ArrayShape = Vec<u64>;
// each dimension name can be null in Zarr
pub type DimensionName = Option<String>;
pub type DimensionNames = Vec<DimensionName>;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Codec {
    pub name: String,
    pub configuration: Option<HashMap<String, serde_json::Value>>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageTransformer {
    pub name: String,
    pub configuration: Option<HashMap<String, serde_json::Value>>,
}

#[derive(Clone, Eq, PartialEq, Debug, Serialize, Deserialize)]
pub struct ChunkShape(pub Vec<NonZeroU64>);

#[derive(Arbitrary, Copy, Clone, Eq, PartialEq, Debug, Serialize, Deserialize)]
pub enum ChunkKeyEncoding {
    Slash,
    Dot,
    Default,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UserAttributes {
    #[serde(flatten)]
    pub parsed: serde_json::Value,
}

impl UserAttributes {
    pub fn try_new(json: &[u8]) -> Result<UserAttributes, serde_json::Error> {
        serde_json::from_slice(json).map(|json| UserAttributes { parsed: json })
    }

    pub fn to_bytes(&self) -> Bytes {
        // We can unwrap because a Value is always valid json
        #[allow(clippy::expect_used)]
        serde_json::to_vec(&self.parsed)
            .expect("Bug in UserAttributes serialization")
            .into()
    }
}

impl TryFrom<u8> for ChunkKeyEncoding {
    type Error = &'static str;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            b'/' => Ok(ChunkKeyEncoding::Slash),
            b'.' => Ok(ChunkKeyEncoding::Dot),
            b'x' => Ok(ChunkKeyEncoding::Default),
            _ => Err("Invalid chunk key encoding character"),
        }
    }
}

impl From<ChunkKeyEncoding> for u8 {
    fn from(value: ChunkKeyEncoding) -> Self {
        match value {
            ChunkKeyEncoding::Slash => b'/',
            ChunkKeyEncoding::Dot => b'.',
            ChunkKeyEncoding::Default => b'x',
        }
    }
}
