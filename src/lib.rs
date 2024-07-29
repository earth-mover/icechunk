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

use arrow::array::RecordBatch;
use async_trait::async_trait;
use std::{
    collections::{HashMap, HashSet},
    num::NonZeroU64,
    path::PathBuf,
    sync::Arc,
};

#[derive(Clone, Debug)]
pub enum NodeType {
    Group,
    Array,
}

/// An ND index to an element in an array.
pub type ArrayIndices = Vec<u64>;

/// The shape of an array.
/// 0 is a valid shape member
pub type ArrayShape = Vec<u64>;

pub type Path = PathBuf;

#[derive(Clone, Debug)]
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

#[derive(Clone, Eq, PartialEq, Debug)]
pub struct ChunkShape(Vec<NonZeroU64>);

#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub enum ChunkKeyEncoding {
    Slash,
    Dot,
    Default,
}

#[derive(Clone, Debug)]
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

#[derive(Clone, Debug)]
pub struct Codecs(String); // FIXME: define

#[derive(Clone, Debug)]
pub struct StorageTransformers(String); // FIXME: define

pub type DimensionName = String;

pub type UserAttributes = String; // FIXME: better definition

/// The internal id of an array or group, unique only to a single store version
pub type NodeId = u32;

/// The id of a file in object store
/// FIXME: should this be passed by ref everywhere?
pub type ObjectId = [u8; 16]; // FIXME: this doesn't need to be this big

pub type ChunkOffset = u64;
pub type ChunkLength = u64;

type TableOffset = usize;
type TableLength = usize;
pub struct TableRegion(TableOffset, TableLength);

struct Flags(); // FIXME: implement

struct UserAttributesRef {
    object_id: ObjectId,
    location: TableOffset,
    flags: Flags,
}

enum UserAttributesStructure {
    Inline(UserAttributes),
    Ref(UserAttributesRef),
}

struct ManifestExtents(Vec<ArrayIndices>);

struct ManifestRef {
    object_id: ObjectId,
    location: TableRegion,
    flags: Flags,
    extents: ManifestExtents,
}

pub struct ZarrArrayMetadata {
    shape: ArrayShape,
    data_type: DataType,
    chunk_shape: ChunkShape,
    chunk_key_encoding: ChunkKeyEncoding,
    fill_value: FillValue,
    codecs: Codecs,
    storage_transformers: StorageTransformers,
    dimension_names: Vec<DimensionName>,
}

pub struct ArrayStructure {
    id: NodeId,
    path: Path,
    zarr_metadata: ZarrArrayMetadata,
    //user_attributes: UserAttributesStructure,
    //manifests: Vec<ManifestRef>,
}

pub struct GroupStructure {
    id: NodeId,
    path: Path,
    //user_attributes: UserAttributesStructure,
}

pub enum NodeStructure {
    Array(ArrayStructure),
    Group(GroupStructure),
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
pub struct StructureTable {
    batch: RecordBatch,
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

#[cfg(test)]
mod tests {
    use super::*;
}
