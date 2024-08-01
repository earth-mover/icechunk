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
pub mod storage;
pub mod structure;

use async_trait::async_trait;
use std::{
    collections::{HashMap, HashSet},
    fmt::Display,
    num::NonZeroU64,
    path::PathBuf,
    sync::Arc,
};
use structure::StructureTable;

/// An ND index to an element in an array.
pub type ArrayIndices = Vec<u64>;

/// The shape of an array.
/// 0 is a valid shape member
pub type ArrayShape = Vec<u64>;

pub type Path = PathBuf;

#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
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
    RawBits(usize),
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
pub struct ChunkShape(Vec<NonZeroU64>);

#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub enum ChunkKeyEncoding {
    Slash,
    Dot,
    Default,
}

impl TryFrom<u8> for ChunkKeyEncoding {
    type Error = &'static str;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            c if { c == '/' as u8 } => Ok(ChunkKeyEncoding::Slash),
            c if { c == '.' as u8 } => Ok(ChunkKeyEncoding::Dot),
            c if { c == 'x' as u8 } => Ok(ChunkKeyEncoding::Default),
            _ => Err("Invalid chunk key encoding character"),
        }
    }
}

impl From<ChunkKeyEncoding> for u8 {
    fn from(value: ChunkKeyEncoding) -> Self {
        match value {
            ChunkKeyEncoding::Slash => '/' as u8,
            ChunkKeyEncoding::Dot => '.' as u8,
            ChunkKeyEncoding::Default => 'x' as u8,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum FillValue {
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Codecs(String); // FIXME: define

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StorageTransformers(String); // FIXME: define

pub type DimensionName = String;

pub type UserAttributes = String; // FIXME: better definition

/// The internal id of an array or group, unique only to a single store version
pub type NodeId = u32;

/// The id of a file in object store
/// FIXME: should this be passed by ref everywhere?
#[derive(Debug, Hash, Clone, PartialEq, Eq)]
pub struct ObjectId([u8; 16]); // FIXME: this doesn't need to be this big

impl ObjectId {
    const SIZE: usize = 16;

    pub fn random() -> ObjectId {
        ObjectId(rand::random())
    }
}

impl TryFrom<&[u8]> for ObjectId {
    type Error = &'static str;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        let buf = value.try_into();
        buf.map(ObjectId)
            .map_err(|_| "Invalid ObjectId buffer length")
    }
}

pub type ChunkOffset = u64;
pub type ChunkLength = u64;

type TableOffset = u32;
type TableLength = u32;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableRegion(TableOffset, TableLength);

#[derive(Debug, Clone, PartialEq, Eq)]
struct Flags(); // FIXME: implement

#[derive(Debug, Clone, PartialEq, Eq)]
struct UserAttributesRef {
    object_id: ObjectId,
    location: TableOffset,
    flags: Flags,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum UserAttributesStructure {
    Inline(UserAttributes),
    Ref(UserAttributesRef),
}

#[derive(Debug, Clone, PartialEq)]
struct ManifestExtents(Vec<ArrayIndices>);

#[derive(Debug, Clone, PartialEq)]
struct ManifestRef {
    object_id: ObjectId,
    location: TableRegion,
    flags: Flags,
    extents: ManifestExtents,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ZarrArrayMetadata {
    shape: ArrayShape,
    data_type: DataType,
    chunk_shape: ChunkShape,
    chunk_key_encoding: ChunkKeyEncoding,
    fill_value: FillValue,
    codecs: Codecs,
    storage_transformers: Option<StorageTransformers>,
    // each dimension name can be null in Zarr
    dimension_names: Option<Vec<Option<DimensionName>>>,
}

#[derive(Clone, Debug)]
pub enum NodeType {
    Group,
    Array,
}

#[derive(Debug, PartialEq)]
pub enum NodeData {
    Array(ZarrArrayMetadata), //(manifests: Vec<ManifestRef>)
    Group,
}

#[derive(Debug, PartialEq)]
pub struct NodeStructure {
    id: NodeId,
    path: Path,
    user_attributes: Option<UserAttributesStructure>,
    node_data: NodeData,
}

pub struct VirtualChunkRef {
    location: String, // FIXME: better type
    offset: u64,
    length: u64,
}

pub struct ChunkRef {
    id: ObjectId, // FIXME: better type
    offset: u64,
    length: u64,
}

pub enum ChunkPayload {
    Inline(Vec<u8>), // FIXME: optimize copies
    Virtual(VirtualChunkRef),
    Ref(ChunkRef),
}

pub struct ChunkInfo {
    node: NodeId,
    coord: ArrayIndices,
    payload: ChunkPayload,
}

// FIXME: this will hold the arrow file
pub struct AttributesTable();

// FIXME: this will hold the arrow file
pub struct ManifestsTable();

pub enum AddNodeError {
    AlreadyExists,
}

pub enum UpdateNodeError {
    NotFound,
    NotAnArray,
}

pub enum StorageError {
    NotFound,
    Deadlock,
}

/// Fetch and write the parquet files that represent the dataset in object store
///
/// Different implementation can cache the files differently, or not at all.
/// Implementations are free to assume files are never overwritten.
#[async_trait]
trait Storage {
    async fn fetch_structure(&self, id: &ObjectId) -> Result<Arc<StructureTable>, StorageError>; // FIXME: format flags
    async fn fetch_attributes(&self, id: &ObjectId) -> Result<Arc<AttributesTable>, StorageError>; // FIXME: format flags
    async fn fetch_manifests(&self, id: &ObjectId) -> Result<Arc<ManifestsTable>, StorageError>; // FIXME: format flags

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
}

pub struct Dataset {
    structure_id: ObjectId,
    storage: Box<dyn Storage>,

    new_groups: HashSet<Path>,
    new_arrays: HashMap<Path, ZarrArrayMetadata>,
    updated_arrays: HashMap<Path, ZarrArrayMetadata>,
    updated_attributes: HashMap<Path, UserAttributes>,
    // FIXME: issue with too many inline chunks kept in mem
    set_chunks: HashMap<(Path, ArrayIndices), ChunkPayload>,
}
