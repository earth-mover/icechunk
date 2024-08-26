/// General design:
/// - Most things are async even if they don't need to be. Async propagates unfortunately. If
///   something can be async sometimes it needs to be async always. In our example: fetching from
///   storage.
/// - There is a high level interface that knows about arrays, groups, user attributes, etc.
/// - There is a low level interface that speaks zarr keys and values, and is used to provide the
///   zarr store that will be used from python
/// - There is a translation language between low and high levels. When user writes to a zarr key,
///   we need to convert that key to the language of arrays and groups.
/// - We have a facade to forward Zarr store methods to, without having to know all the complexity
///   of the Rust system. This facade is configured with appropriate information (like caching
///   mechanisms)
/// - There is an abstract type for storage of the Arrow datastructures.
///   This is the `Storage` trait. It knows how to fetch and write arrow.
///   We have:
///     - an in memory implementations
///     - an s3 implementation that writes to parquet
///     - caching decorators
/// - The datastructures are represented by concrete types (Structure|Attribute|Manifests)Table
///   Methods on those types help generating the high level interface of the library. These
///   datastructures use Arrow  RecordBatches for representation.
/// - A `Dataset` concrete type, implements the high level interface, using an Storage
///   implementation and the data tables.
pub mod dataset;
pub mod manifest;
pub mod storage;
pub mod strategies;
pub mod structure;
pub mod zarr;

use arrow::array::RecordBatch;
use async_trait::async_trait;
use bytes::Bytes;
use itertools::Itertools;
use manifest::ManifestsTable;
use parquet::errors as parquet_errors;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    fmt::{self, Display},
    io,
    num::NonZeroU64,
    ops::Range,
    path::PathBuf,
    sync::Arc,
};
use structure::StructureTable;
use test_strategy::Arbitrary;
use thiserror::Error;

#[derive(Debug, Clone, Error)]
pub enum IcechunkFormatError {
    #[error("error decoding fill_value from array")]
    FillValueDecodeError { found_size: usize, target_size: usize, target_type: DataType },
    #[error("error decoding fill_value from json")]
    FillValueParse { data_type: DataType, value: serde_json::Value },
    #[error("null found decoding fill_value")]
    NullFillValueError,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
/// An ND index to an element in an array.
pub struct ArrayIndices(pub Vec<u64>);

/// The shape of an array.
/// 0 is a valid shape member
pub type ArrayShape = Vec<u64>;
// each dimension name can be null in Zarr
pub type DimensionName = Option<String>;
pub type DimensionNames = Vec<DimensionName>;

pub type Path = PathBuf;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
#[serde(rename_all = "lowercase")]
pub enum DataType {
    Bool,
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Float16,
    Float32,
    Float64,
    Complex64,
    Complex128,
    // FIXME: serde serialization
    RawBits(usize),
}

impl DataType {
    fn fits_i64(&self, n: i64) -> bool {
        use DataType::*;
        match self {
            Int8 => n >= i8::MIN as i64 && n <= i8::MAX as i64,
            Int16 => n >= i16::MIN as i64 && n <= i16::MAX as i64,
            Int32 => n >= i32::MIN as i64 && n <= i32::MAX as i64,
            Int64 => true,
            _ => false,
        }
    }

    fn fits_u64(&self, n: u64) -> bool {
        use DataType::*;
        match self {
            UInt8 => n >= u8::MIN as u64 && n <= u8::MAX as u64,
            UInt16 => n >= u16::MIN as u64 && n <= u16::MAX as u64,
            UInt32 => n >= u32::MIN as u64 && n <= u32::MAX as u64,
            UInt64 => true,
            _ => false,
        }
    }
}

impl TryFrom<&str> for DataType {
    type Error = &'static str;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "bool" => Ok(DataType::Bool),
            "int8" => Ok(DataType::Int8),
            "int16" => Ok(DataType::Int16),
            "int32" => Ok(DataType::Int32),
            "int64" => Ok(DataType::Int64),
            "uint8" => Ok(DataType::UInt8),
            "uint16" => Ok(DataType::UInt16),
            "uint32" => Ok(DataType::UInt32),
            "uint64" => Ok(DataType::UInt64),
            "float16" => Ok(DataType::Float16),
            "float32" => Ok(DataType::Float32),
            "float64" => Ok(DataType::Float64),
            "complex64" => Ok(DataType::Complex64),
            "complex128" => Ok(DataType::Complex128),
            _ => {
                let mut it = value.chars();
                if it.next() == Some('r') {
                    it.as_str()
                        .parse()
                        .map(DataType::RawBits)
                        .map_err(|_| "Cannot parse RawBits size")
                } else {
                    Err("Unknown data type, cannot parse")
                }
            }
        }
    }
}

impl Display for DataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use DataType::*;
        match self {
            Bool => f.write_str("bool"),
            Int8 => f.write_str("int8"),
            Int16 => f.write_str("int16"),
            Int32 => f.write_str("int32"),
            Int64 => f.write_str("int64"),
            UInt8 => f.write_str("uint8"),
            UInt16 => f.write_str("uint16"),
            UInt32 => f.write_str("uint32"),
            UInt64 => f.write_str("uint64"),
            Float16 => f.write_str("float16"),
            Float32 => f.write_str("float32"),
            Float64 => f.write_str("float64"),
            Complex64 => f.write_str("complex64"),
            Complex128 => f.write_str("complex128"),
            RawBits(usize) => write!(f, "r{}", usize),
        }
    }
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub struct ChunkShape(pub Vec<NonZeroU64>);

#[derive(Arbitrary, Copy, Clone, Eq, PartialEq, Debug, Serialize, Deserialize)]
pub enum ChunkKeyEncoding {
    Slash,
    Dot,
    Default,
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

#[derive(Arbitrary, Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum FillValue {
    // FIXME: test all json (de)serializations
    Bool(bool),
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    UInt8(u8),
    UInt16(u16),
    UInt32(u32),
    UInt64(u64),
    Float16(f32),
    Float32(f32),
    Float64(f64),
    Complex64(f32, f32),
    Complex128(f64, f64),
    RawBits(Vec<u8>),
}

impl FillValue {
    fn from_data_type_and_json(
        dt: &DataType,
        value: &serde_json::Value,
    ) -> Result<Self, IcechunkFormatError> {
        match (dt, value) {
            (DataType::Bool, serde_json::Value::Bool(b)) => Ok(FillValue::Bool(*b)),
            (DataType::Int8, serde_json::Value::Number(n))
                if n.as_i64().map(|n| dt.fits_i64(n)) == Some(true) =>
            {
                Ok(FillValue::Int8(n.as_i64().unwrap() as i8))
            }
            (DataType::Int16, serde_json::Value::Number(n))
                if n.as_i64().map(|n| dt.fits_i64(n)) == Some(true) =>
            {
                Ok(FillValue::Int16(n.as_i64().unwrap() as i16))
            }
            (DataType::Int32, serde_json::Value::Number(n))
                if n.as_i64().map(|n| dt.fits_i64(n)) == Some(true) =>
            {
                Ok(FillValue::Int32(n.as_i64().unwrap() as i32))
            }
            (DataType::Int64, serde_json::Value::Number(n))
                if n.as_i64().map(|n| dt.fits_i64(n)) == Some(true) =>
            {
                Ok(FillValue::Int64(n.as_i64().unwrap()))
            }
            (DataType::UInt8, serde_json::Value::Number(n))
                if n.as_u64().map(|n| dt.fits_u64(n)) == Some(true) =>
            {
                Ok(FillValue::UInt8(n.as_u64().unwrap() as u8))
            }
            (DataType::UInt16, serde_json::Value::Number(n))
                if n.as_u64().map(|n| dt.fits_u64(n)) == Some(true) =>
            {
                Ok(FillValue::UInt16(n.as_u64().unwrap() as u16))
            }
            (DataType::UInt32, serde_json::Value::Number(n))
                if n.as_u64().map(|n| dt.fits_u64(n)) == Some(true) =>
            {
                Ok(FillValue::UInt32(n.as_u64().unwrap() as u32))
            }
            (DataType::UInt64, serde_json::Value::Number(n))
                if n.as_u64().map(|n| dt.fits_u64(n)) == Some(true) =>
            {
                Ok(FillValue::UInt64(n.as_u64().unwrap()))
            }
            (DataType::Float16, serde_json::Value::Number(n)) if n.as_f64().is_some() => {
                // FIXME: limits logic
                Ok(FillValue::Float16(n.as_f64().unwrap() as f32))
            }
            (DataType::Float32, serde_json::Value::Number(n)) if n.as_f64().is_some() => {
                // FIXME: limits logic
                Ok(FillValue::Float32(n.as_f64().unwrap() as f32))
            }
            (DataType::Float64, serde_json::Value::Number(n)) if n.as_f64().is_some() => {
                // FIXME: limits logic
                Ok(FillValue::Float64(n.as_f64().unwrap()))
            }
            (DataType::Complex64, serde_json::Value::Array(arr)) if arr.len() == 2 => {
                let r = FillValue::from_data_type_and_json(&DataType::Float32, &arr[0])?;
                let i = FillValue::from_data_type_and_json(&DataType::Float32, &arr[1])?;
                match (r, i) {
                    (FillValue::Float32(r), FillValue::Float32(i)) => {
                        Ok(FillValue::Complex64(r, i))
                    }
                    _ => Err(IcechunkFormatError::FillValueParse {
                        data_type: dt.clone(),
                        value: value.clone(),
                    }),
                }
            }
            (DataType::Complex128, serde_json::Value::Array(arr)) if arr.len() == 2 => {
                let r = FillValue::from_data_type_and_json(&DataType::Float64, &arr[0])?;
                let i = FillValue::from_data_type_and_json(&DataType::Float64, &arr[1])?;
                match (r, i) {
                    (FillValue::Float64(r), FillValue::Float64(i)) => {
                        Ok(FillValue::Complex128(r, i))
                    }
                    _ => Err(IcechunkFormatError::FillValueParse {
                        data_type: dt.clone(),
                        value: value.clone(),
                    }),
                }
            }

            (DataType::RawBits(n), serde_json::Value::Array(arr)) if arr.len() == *n => {
                let bits = arr
                    .iter()
                    .map(|b| FillValue::from_data_type_and_json(&DataType::UInt8, b))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(FillValue::RawBits(
                    bits.iter()
                        .map(|b| match b {
                            FillValue::UInt8(n) => *n,
                            _ => 0,
                        })
                        .collect(),
                ))
            }
            _ => Err(IcechunkFormatError::FillValueParse {
                data_type: dt.clone(),
                value: value.clone(),
            }),
        }
    }

    fn from_data_type_and_value(
        dt: &DataType,
        value: &[u8],
    ) -> Result<Self, IcechunkFormatError> {
        use IcechunkFormatError::FillValueDecodeError;

        match dt {
            DataType::Bool => {
                if value.len() != 1 {
                    Err(FillValueDecodeError {
                        found_size: value.len(),
                        target_size: 1,
                        target_type: DataType::Bool,
                    })
                } else {
                    Ok(FillValue::Bool(value[0] != 0))
                }
            }
            DataType::Int8 => value
                .try_into()
                .map(|x| FillValue::Int8(i8::from_be_bytes(x)))
                .map_err(|_| FillValueDecodeError {
                    found_size: value.len(),
                    target_size: 1,
                    target_type: DataType::Int8,
                }),
            DataType::Int16 => value
                .try_into()
                .map(|x| FillValue::Int16(i16::from_be_bytes(x)))
                .map_err(|_| FillValueDecodeError {
                    found_size: value.len(),
                    target_size: 2,
                    target_type: DataType::Int16,
                }),
            DataType::Int32 => value
                .try_into()
                .map(|x| FillValue::Int32(i32::from_be_bytes(x)))
                .map_err(|_| FillValueDecodeError {
                    found_size: value.len(),
                    target_size: 4,
                    target_type: DataType::Int32,
                }),
            DataType::Int64 => value
                .try_into()
                .map(|x| FillValue::Int64(i64::from_be_bytes(x)))
                .map_err(|_| FillValueDecodeError {
                    found_size: value.len(),
                    target_size: 8,
                    target_type: DataType::Int64,
                }),
            DataType::UInt8 => value
                .try_into()
                .map(|x| FillValue::UInt8(u8::from_be_bytes(x)))
                .map_err(|_| FillValueDecodeError {
                    found_size: value.len(),
                    target_size: 1,
                    target_type: DataType::UInt8,
                }),
            DataType::UInt16 => value
                .try_into()
                .map(|x| FillValue::UInt16(u16::from_be_bytes(x)))
                .map_err(|_| FillValueDecodeError {
                    found_size: value.len(),
                    target_size: 2,
                    target_type: DataType::UInt16,
                }),
            DataType::UInt32 => value
                .try_into()
                .map(|x| FillValue::UInt32(u32::from_be_bytes(x)))
                .map_err(|_| FillValueDecodeError {
                    found_size: value.len(),
                    target_size: 4,
                    target_type: DataType::UInt32,
                }),
            DataType::UInt64 => value
                .try_into()
                .map(|x| FillValue::UInt64(u64::from_be_bytes(x)))
                .map_err(|_| FillValueDecodeError {
                    found_size: value.len(),
                    target_size: 8,
                    target_type: DataType::UInt64,
                }),
            DataType::Float16 => value
                .try_into()
                .map(|x| FillValue::Float16(f32::from_be_bytes(x)))
                .map_err(|_| FillValueDecodeError {
                    found_size: value.len(),
                    target_size: 4,
                    target_type: DataType::Float16,
                }),
            DataType::Float32 => value
                .try_into()
                .map(|x| FillValue::Float32(f32::from_be_bytes(x)))
                .map_err(|_| FillValueDecodeError {
                    found_size: value.len(),
                    target_size: 4,
                    target_type: DataType::Float32,
                }),
            DataType::Float64 => value
                .try_into()
                .map(|x| FillValue::Float64(f64::from_be_bytes(x)))
                .map_err(|_| FillValueDecodeError {
                    found_size: value.len(),
                    target_size: 8,
                    target_type: DataType::Float64,
                }),
            DataType::Complex64 => {
                if value.len() != 8 {
                    Err(FillValueDecodeError {
                        found_size: value.len(),
                        target_size: 8,
                        target_type: DataType::Complex64,
                    })
                } else {
                    let r = value[..4].try_into().map(f32::from_be_bytes).map_err(|_| {
                        FillValueDecodeError {
                            found_size: value.len(),
                            target_size: 4,
                            target_type: DataType::Complex64,
                        }
                    });
                    let i = value[4..].try_into().map(f32::from_be_bytes).map_err(|_| {
                        FillValueDecodeError {
                            found_size: value.len(),
                            target_size: 4,
                            target_type: DataType::Complex64,
                        }
                    });
                    Ok(FillValue::Complex64(r?, i?))
                }
            }
            DataType::Complex128 => {
                if value.len() != 16 {
                    Err(FillValueDecodeError {
                        found_size: value.len(),
                        target_size: 16,
                        target_type: DataType::Complex128,
                    })
                } else {
                    let r = value[..8].try_into().map(f64::from_be_bytes).map_err(|_| {
                        FillValueDecodeError {
                            found_size: value.len(),
                            target_size: 8,
                            target_type: DataType::Complex128,
                        }
                    });
                    let i = value[8..].try_into().map(f64::from_be_bytes).map_err(|_| {
                        FillValueDecodeError {
                            found_size: value.len(),
                            target_size: 8,
                            target_type: DataType::Complex128,
                        }
                    });
                    Ok(FillValue::Complex128(r?, i?))
                }
            }
            DataType::RawBits(_) => Ok(FillValue::RawBits(value.to_owned())),
        }
    }

    #[allow(dead_code)]
    fn get_data_type(&self) -> DataType {
        match self {
            FillValue::Bool(_) => DataType::Bool,
            FillValue::Int8(_) => DataType::Int8,
            FillValue::Int16(_) => DataType::Int16,
            FillValue::Int32(_) => DataType::Int32,
            FillValue::Int64(_) => DataType::Int64,
            FillValue::UInt8(_) => DataType::UInt8,
            FillValue::UInt16(_) => DataType::UInt16,
            FillValue::UInt32(_) => DataType::UInt32,
            FillValue::UInt64(_) => DataType::UInt64,
            FillValue::Float16(_) => DataType::Float16,
            FillValue::Float32(_) => DataType::Float32,
            FillValue::Float64(_) => DataType::Float64,
            FillValue::Complex64(_, _) => DataType::Complex64,
            FillValue::Complex128(_, _) => DataType::Complex128,
            FillValue::RawBits(v) => DataType::RawBits(v.len()),
        }
    }

    fn to_be_bytes(&self) -> Vec<u8> {
        match self {
            FillValue::Bool(v) => vec![if v.to_owned() { 1 } else { 0 }],
            FillValue::Int8(v) => v.to_be_bytes().into(),
            FillValue::Int16(v) => v.to_be_bytes().into(),
            FillValue::Int32(v) => v.to_be_bytes().into(),
            FillValue::Int64(v) => v.to_be_bytes().into(),
            FillValue::UInt8(v) => v.to_be_bytes().into(),
            FillValue::UInt16(v) => v.to_be_bytes().into(),
            FillValue::UInt32(v) => v.to_be_bytes().into(),
            FillValue::UInt64(v) => v.to_be_bytes().into(),
            FillValue::Float16(v) => v.to_be_bytes().into(),
            FillValue::Float32(v) => v.to_be_bytes().into(),
            FillValue::Float64(v) => v.to_be_bytes().into(),
            FillValue::Complex64(r, i) => {
                r.to_be_bytes().into_iter().chain(i.to_be_bytes()).collect()
            }
            FillValue::Complex128(r, i) => {
                r.to_be_bytes().into_iter().chain(i.to_be_bytes()).collect()
            }
            FillValue::RawBits(v) => v.to_owned(),
        }
    }
}

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
        serde_json::to_vec(&self.parsed)
            .expect("Bug in UserAttributes serialization")
            .into()
    }
}

/// The internal id of an array or group, unique only to a single store version
pub type NodeId = u32;

/// The id of a file in object store
/// FIXME: should this be passed by ref everywhere?
#[derive(Hash, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct ObjectId([u8; 16]); // FIXME: this doesn't need to be this big

impl ObjectId {
    const SIZE: usize = 16;

    pub fn random() -> ObjectId {
        ObjectId(rand::random())
    }

    const FAKE: ObjectId = ObjectId([0; 16]);
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

pub type ChunkOffset = u64;
pub type ChunkLength = u64;

type TableOffset = u32;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableRegion(TableOffset, TableOffset);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Flags(); // FIXME: implement

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserAttributesRef {
    pub object_id: ObjectId,
    pub location: TableOffset,
    pub flags: Flags,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UserAttributesStructure {
    Inline(UserAttributes),
    Ref(UserAttributesRef),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManifestExtents(pub Vec<ArrayIndices>);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManifestRef {
    pub object_id: ObjectId,
    pub location: TableRegion,
    pub flags: Flags,
    pub extents: ManifestExtents,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ZarrArrayMetadata {
    pub shape: ArrayShape,
    pub data_type: DataType,
    pub chunk_shape: ChunkShape,
    pub chunk_key_encoding: ChunkKeyEncoding,
    pub fill_value: FillValue,
    pub codecs: Vec<Codec>,
    pub storage_transformers: Option<Vec<StorageTransformer>>,
    pub dimension_names: Option<DimensionNames>,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub enum NodeType {
    Group,
    Array,
}

#[derive(Debug, Clone, PartialEq)]
pub enum NodeData {
    Array(ZarrArrayMetadata, Vec<ManifestRef>),
    Group,
}

#[derive(Debug, Clone, PartialEq)]
pub struct NodeStructure {
    pub id: NodeId,
    pub path: Path,
    pub user_attributes: Option<UserAttributesStructure>,
    pub node_data: NodeData,
}

impl NodeStructure {
    pub fn node_type(&self) -> NodeType {
        match &self.node_data {
            NodeData::Group => NodeType::Group,
            NodeData::Array(_, _) => NodeType::Array,
        }
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct VirtualChunkRef {
    location: String, // FIXME: better type
    offset: u64,
    length: u64,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct ChunkRef {
    id: ObjectId,
    offset: u64,
    length: u64,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub enum ChunkPayload {
    Inline(Bytes), // FIXME: optimize copies
    Virtual(VirtualChunkRef),
    Ref(ChunkRef),
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct ChunkInfo {
    node: NodeId,
    coord: ArrayIndices,
    payload: ChunkPayload,
}

#[derive(Debug, PartialEq)]
pub struct AttributesTable {
    pub batch: RecordBatch,
}

#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub enum AddNodeError {
    #[error("node already exists at `{0}`")]
    AlreadyExists(Path),
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum DeleteNodeError {
    #[error("node not found at `{0}`")]
    NotFound(Path),
    #[error("there is not an array at `{0}`")]
    NotAnArray(Path),
    #[error("there is not a group at `{0}`")]
    NotAGroup(Path),
}

#[derive(Debug, Error)]
pub enum UpdateNodeError {
    #[error("node not found at `{0}`")]
    NotFound(Path),
    #[error("there is not an array at `{0}`")]
    NotAnArray(Path),
    #[error("error contacting storage")]
    StorageError(#[from] StorageError),
}

#[derive(Debug, Error)]
pub enum GetNodeError {
    #[error("node not found at `{0}`")]
    NotFound(Path),
    #[error("error contacting storage")]
    StorageError(#[from] StorageError),
}

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("object not found `{0:?}`")]
    NotFound(ObjectId),
    #[error("synchronization error on the Storage instance")]
    Deadlock,
    #[error("error contacting object store {0}")]
    ObjectStore(#[from] object_store::Error),
    #[error("error reading or writing to/from parquet files: {0}")]
    ParquetError(#[from] parquet_errors::ParquetError),
    #[error("error reading RecordBatch from parquet file {0}.")]
    BadRecordBatchRead(String),
    #[error("i/o error: `{0:?}`")]
    IOError(#[from] io::Error),
    #[error("bad path: {0}")]
    BadPath(Path),
}

/// Fetch and write the parquet files that represent the dataset in object store
///
/// Different implementation can cache the files differently, or not at all.
/// Implementations are free to assume files are never overwritten.
#[async_trait]
pub trait Storage: fmt::Debug {
    async fn fetch_structure(
        &self,
        id: &ObjectId,
    ) -> Result<Arc<StructureTable>, StorageError>; // FIXME: format flags
    async fn fetch_attributes(
        &self,
        id: &ObjectId,
    ) -> Result<Arc<AttributesTable>, StorageError>; // FIXME: format flags
    async fn fetch_manifests(
        &self,
        id: &ObjectId,
    ) -> Result<Arc<ManifestsTable>, StorageError>; // FIXME: format flags
    async fn fetch_chunk(
        &self,
        id: &ObjectId,
        range: &Option<Range<ChunkOffset>>,
    ) -> Result<Bytes, StorageError>; // FIXME: format flags

    async fn write_structure(
        &self,
        id: ObjectId,
        table: Arc<StructureTable>,
    ) -> Result<(), StorageError>;
    async fn write_attributes(
        &self,
        id: ObjectId,
        table: Arc<AttributesTable>,
    ) -> Result<(), StorageError>;
    async fn write_manifests(
        &self,
        id: ObjectId,
        table: Arc<ManifestsTable>,
    ) -> Result<(), StorageError>;
    async fn write_chunk(&self, id: ObjectId, bytes: Bytes) -> Result<(), StorageError>;
}

#[derive(Clone, Debug)]
pub struct DatasetConfig {
    // Chunks smaller than this will be stored inline in the manifst
    pub inline_threshold_bytes: u16,
}

impl Default for DatasetConfig {
    fn default() -> Self {
        Self { inline_threshold_bytes: 512 }
    }
}

#[derive(Clone, Debug)]
pub struct Dataset {
    config: DatasetConfig,
    storage: Arc<dyn Storage + Send + Sync>,
    structure_id: Option<ObjectId>,
    last_node_id: Option<NodeId>,
    change_set: ChangeSet,
}

#[derive(Clone, Debug, PartialEq, Default)]
pub struct ChangeSet {
    new_groups: HashMap<Path, NodeId>,
    new_arrays: HashMap<Path, (NodeId, ZarrArrayMetadata)>,
    updated_arrays: HashMap<Path, ZarrArrayMetadata>,
    // These paths may point to Arrays or Groups,
    // since both Groups and Arrays support UserAttributes
    updated_attributes: HashMap<Path, Option<UserAttributes>>,
    // FIXME: issue with too many inline chunks kept in mem
    set_chunks: HashMap<Path, HashMap<ArrayIndices, Option<ChunkPayload>>>,
    deleted_groups: HashMap<Path, NodeId>,
    deleted_arrays: HashMap<Path, NodeId>,
}
