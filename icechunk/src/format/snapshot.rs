use std::{num::NonZeroU64, sync::Arc};

use arrow::{
    array::{
        Array, ArrayRef, AsArray, BinaryArray, BinaryBuilder, FixedSizeBinaryArray,
        FixedSizeBinaryBuilder, ListArray, ListBuilder, RecordBatch, StringArray,
        StringBuilder, StructArray, UInt32Array, UInt32Builder, UInt8Array,
    },
    datatypes::{Field, Fields, Schema, UInt32Type, UInt64Type},
    error::ArrowError,
};
use itertools::izip;

use crate::metadata::{
    ArrayShape, ChunkKeyEncoding, ChunkShape, Codec, DataType, DimensionName,
    DimensionNames, FillValue, StorageTransformer, UserAttributes,
};

use super::{
    arrow::get_column,
    manifest::{ManifestExtents, ManifestRef},
    BatchLike, Flags, IcechunkFormatError, IcechunkResult, NodeId, ObjectId, Path,
    TableOffset, TableRegion,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserAttributesRef {
    pub object_id: ObjectId,
    pub location: TableOffset,
    pub flags: Flags,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UserAttributesSnapshot {
    Inline(UserAttributes),
    Ref(UserAttributesRef),
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub enum NodeType {
    Group,
    Array,
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

#[derive(Debug, Clone, PartialEq)]
pub enum NodeData {
    Array(ZarrArrayMetadata, Vec<ManifestRef>),
    Group,
}

#[derive(Debug, Clone, PartialEq)]
pub struct NodeSnapshot {
    pub id: NodeId,
    pub path: Path,
    pub user_attributes: Option<UserAttributesSnapshot>,
    pub node_data: NodeData,
}

impl NodeSnapshot {
    pub fn node_type(&self) -> NodeType {
        match &self.node_data {
            NodeData::Group => NodeType::Group,
            NodeData::Array(_, _) => NodeType::Array,
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct SnapshotTable {
    pub batch: RecordBatch,
}

impl BatchLike for SnapshotTable {
    fn get_batch(&self) -> &RecordBatch {
        &self.batch
    }
}

impl SnapshotTable {
    pub fn get_node(&self, path: &Path) -> IcechunkResult<NodeSnapshot> {
        // FIXME: optimize
        let paths: &StringArray = get_column(self, "path")?.as_string()?;
        let needle = path
            .to_str()
            .ok_or(IcechunkFormatError::InvalidPath { path: path.clone() })?;
        let idx = paths
            .iter()
            .position(|s| s == Some(needle))
            .ok_or(IcechunkFormatError::NodeNotFound { path: path.clone() })?;
        self.build_node_snapshot(idx)
    }

    pub fn iter(&self) -> impl Iterator<Item = IcechunkResult<NodeSnapshot>> + '_ {
        let max = self.batch.num_rows();
        (0..max).map(|idx| self.build_node_snapshot(idx))
    }

    // FIXME: do we still need this method?
    pub fn iter_arc(
        self: Arc<Self>,
    ) -> impl Iterator<Item = IcechunkResult<NodeSnapshot>> {
        let max = self.batch.num_rows();
        (0..max).map(move |idx| self.build_node_snapshot(idx))
    }

    fn build_zarr_array_metadata(&self, idx: usize) -> IcechunkResult<ZarrArrayMetadata> {
        // TODO: split this huge function
        let shape: Vec<u64> = get_column(self, "shape")?
            .list_at(idx)?
            .as_primitive_opt::<UInt64Type>()
            .ok_or(IcechunkFormatError::InvalidColumnType {
                column_name: "shape".to_string(),
                expected_column_type: "list of u64".to_string(),
            })?
            .iter()
            .collect::<Option<Vec<_>>>()
            .ok_or(IcechunkFormatError::InvalidArrayMetadata {
                index: idx,
                field: "shape".to_string(),
                message: "null dimensions sizes".to_string(),
            })?;
        let data_type = DataType::try_from(
            get_column(self, "data_type")?.string_at(idx)?,
        )
        .map_err(|err| IcechunkFormatError::InvalidArrayMetadata {
            index: idx,
            field: "data_type".to_string(),
            message: err.to_string(),
        })?;
        let chunk_shape = ChunkShape(
            get_column(self, "chunk_shape")?
                .list_at(idx)?
                .as_primitive_opt::<UInt64Type>()
                .ok_or(IcechunkFormatError::InvalidColumnType {
                    column_name: "chunk_shape".to_string(),
                    expected_column_type: "list of u64".to_string(),
                })?
                .iter()
                .collect::<Option<Vec<_>>>()
                .ok_or(IcechunkFormatError::InvalidArrayMetadata {
                    index: idx,
                    field: "chunk_shape".to_string(),
                    message: "null chunk sizes".to_string(),
                })?
                .iter()
                .map(|u64| NonZeroU64::new(*u64))
                .collect::<Option<Vec<_>>>()
                .ok_or(IcechunkFormatError::InvalidArrayMetadata {
                    index: idx,
                    field: "chunk_shape".to_string(),
                    message: "non-positive chunk sizes".to_string(),
                })?,
        );
        let key_encoding_u8 = get_column(self, "chunk_key_encoding")?.u8_at(idx)?;
        let chunk_key_encoding =
            std::convert::TryInto::<ChunkKeyEncoding>::try_into(key_encoding_u8)
                .map_err(|err| IcechunkFormatError::InvalidArrayMetadata {
                    index: idx,
                    field: "chunk_key_encoding".to_string(),
                    message: err.to_string(),
                })?;

        let codecs = get_column(self, "codecs")?
            .list_at(idx)?
            .as_binary_opt::<i32>()
            .ok_or(IcechunkFormatError::InvalidColumnType {
                column_name: "codecs".to_string(),
                expected_column_type: "list of binary".to_string(),
            })?
            .iter()
            // FIXME: handle parse error
            .map(|maybe_json| {
                maybe_json.and_then(|json| serde_json::from_slice(json).ok())
            })
            .collect::<Option<Vec<_>>>()
            .ok_or(IcechunkFormatError::InvalidArrayMetadata {
                index: idx,
                field: "codecs".to_string(),
                message: "null or unparseable codec".to_string(),
            })?;

        let storage_transformers =
            match get_column(self, "storage_transformers")?.list_at_opt(idx)? {
                Some(tr_array) => Some(
                    tr_array
                        .as_binary_opt::<i32>()
                        .ok_or(IcechunkFormatError::InvalidColumnType {
                            column_name: "storage_transformers".to_string(),
                            expected_column_type: "list of binary".to_string(),
                        })?
                        .iter()
                        .map(|maybe_json| {
                            maybe_json.and_then(|json| serde_json::from_slice(json).ok())
                        })
                        .collect::<Option<Vec<_>>>()
                        .ok_or(IcechunkFormatError::InvalidArrayMetadata {
                            index: idx,
                            field: "storage_transformers".to_string(),
                            message: "null or unparseable transformer".to_string(),
                        })?,
                ),

                None => None,
            };

        let dimension_names =
            match get_column(self, "dimension_names")?.list_at_opt(idx)? {
                Some(ds_array) => Some(
                    ds_array
                        .as_string_opt::<i32>()
                        .ok_or(IcechunkFormatError::InvalidColumnType {
                            column_name: "dimension_names".to_string(),
                            expected_column_type: "list of string".to_string(),
                        })?
                        .iter()
                        .map(|s| s.map(|s| s.to_string()))
                        .collect::<Vec<_>>(),
                ),
                None => None,
            };

        let encoded_fill_value = get_column(self, "fill_value")?.binary_at(idx)?;
        let fill_value =
            FillValue::from_data_type_and_value(&data_type, encoded_fill_value)?;

        Ok(ZarrArrayMetadata {
            shape,
            data_type,
            chunk_shape,
            chunk_key_encoding,
            fill_value,
            codecs,
            storage_transformers,
            dimension_names,
        })
    }

    fn build_manifest_refs(&self, idx: usize) -> IcechunkResult<Vec<ManifestRef>> {
        let manifest_refs_array = get_column(self, "manifest_references")?.as_struct()?;
        if manifest_refs_array.is_valid(idx) {
            let refs = manifest_refs_array
                .column_by_name("reference")
                .ok_or(IcechunkFormatError::ColumnNotFound {
                    column: "manifest_references.reference".to_string(),
                })?
                .as_list_opt::<i32>()
                .ok_or(IcechunkFormatError::InvalidColumnType {
                    column_name: "manifest_references.reference".to_string(),
                    expected_column_type: "list of fixed size binary".to_string(),
                })?
                // TODO: is ok to consider it empty if null
                .value(idx);
            let refs = refs.as_fixed_size_binary_opt().ok_or(
                IcechunkFormatError::InvalidColumnType {
                    column_name: format!("manifest_references.reference[{idx}]"),
                    expected_column_type: "fixed size binary".to_string(),
                },
            )?;

            let row_from = manifest_refs_array
                .column_by_name("start_row")
                .ok_or(IcechunkFormatError::ColumnNotFound {
                    column: "manifest_references.start_row".to_string(),
                })?
                .as_list_opt::<i32>()
                .ok_or(IcechunkFormatError::InvalidColumnType {
                    column_name: "manifest_references.start_row".to_string(),
                    expected_column_type: "list of u32".to_string(),
                })?
                // TODO: is ok to consider it empty if null
                .value(idx);
            let row_from = row_from.as_primitive_opt::<UInt32Type>().ok_or(
                IcechunkFormatError::InvalidColumnType {
                    column_name: format!("manifest_references.start_row[{idx}]"),
                    expected_column_type: "u32".to_string(),
                },
            )?;

            let row_to = manifest_refs_array
                .column_by_name("end_row")
                .ok_or(IcechunkFormatError::ColumnNotFound {
                    column: "manifest_references.end_row".to_string(),
                })?
                .as_list_opt::<i32>()
                .ok_or(IcechunkFormatError::InvalidColumnType {
                    column_name: "manifest_references.end_row".to_string(),
                    expected_column_type: "list of u32".to_string(),
                })?
                // TODO: is ok to consider it empty if null
                .value(idx);
            let row_to = row_to.as_primitive_opt::<UInt32Type>().ok_or(
                IcechunkFormatError::InvalidColumnType {
                    column_name: format!("manifest_references.end_row[{idx}]"),
                    expected_column_type: "u32".to_string(),
                },
            )?;

            // FIXME: add extents and flags

            let it = izip!(refs.iter(), row_from.iter(), row_to.iter()).map(
                |(r, f, t)| match (r.and_then(|r| r.try_into().ok()), f, t) {
                    (Some(r), Some(f), Some(t)) => Some((r, f, t)),
                    _ => None,
                },
            );
            let res = it
                .collect::<Option<Vec<_>>>()
                .ok_or(IcechunkFormatError::InvalidArrayManifest {
                    index: idx,
                    field: "manifest_references".to_string(),
                    message: "null or incorrect reference".to_string(),
                })?
                .iter()
                .map(|(r, f, t)| ManifestRef {
                    object_id: ObjectId(*r),
                    location: TableRegion(*f, *t),
                    // FIXME: flags
                    flags: Flags(),
                    // FIXME: extents
                    extents: ManifestExtents(vec![]),
                })
                .collect();
            Ok(res)
        } else {
            Ok(vec![])
        }
    }

    fn build_node_snapshot(&self, idx: usize) -> IcechunkResult<NodeSnapshot> {
        let node_type = get_column(self, "type")?.string_at(idx)?;
        let id = get_column(self, "id")?.u32_at(idx)?;
        let path = get_column(self, "path")?.string_at(idx)?;
        let user_attributes = self.build_user_attributes(idx);
        match node_type {
            "group" => Ok(NodeSnapshot {
                path: path.into(),
                id,
                user_attributes,
                node_data: NodeData::Group,
            }),
            "array" => Ok(NodeSnapshot {
                path: path.into(),
                id,
                user_attributes,
                node_data: NodeData::Array(
                    self.build_zarr_array_metadata(idx)?,
                    self.build_manifest_refs(idx)?,
                ),
            }),
            _ => Err(IcechunkFormatError::InvalidNodeType {
                index: idx,
                node_type: node_type.to_string(),
            }),
        }
    }

    fn build_user_attributes(&self, idx: usize) -> Option<UserAttributesSnapshot> {
        let inline =
            self.batch.column_by_name("user_attributes")?.as_binary_opt::<i32>()?;
        if inline.is_valid(idx) {
            // FIXME: error handling
            UserAttributes::try_new(inline.value(idx))
                .ok()
                .map(UserAttributesSnapshot::Inline)
        } else {
            self.build_user_attributes_ref(idx)
        }
    }
    fn build_user_attributes_ref(&self, idx: usize) -> Option<UserAttributesSnapshot> {
        let atts_ref = self
            .batch
            .column_by_name("user_attributes_ref")?
            .as_fixed_size_binary_opt()?;
        let atts_row = self
            .batch
            .column_by_name("user_attributes_row")?
            .as_primitive_opt::<UInt32Type>()?;
        if atts_ref.is_valid(idx) && atts_row.is_valid(idx) {
            let object_id = atts_ref.value(idx);
            let object_id = object_id.try_into().ok()?;
            let location = atts_row.value(idx);
            // FIXME: flags
            let flags = Flags();
            Some(UserAttributesSnapshot::Ref(UserAttributesRef {
                object_id,
                location,
                flags,
            }))
        } else {
            None
        }
    }
}

fn mk_id_array<T>(coll: T) -> UInt32Array
where
    T: IntoIterator,
    T::Item: Into<NodeId>,
{
    coll.into_iter().map(|x| x.into()).collect()
}

fn mk_type_array<T>(coll: T) -> StringArray
where
    T: IntoIterator<Item = NodeType>,
{
    let iter = coll.into_iter().map(|x| match x {
        NodeType::Array => "array".to_string(),
        NodeType::Group => "group".to_string(),
    });
    StringArray::from_iter_values(iter)
}

fn mk_path_array<T>(coll: T) -> StringArray
where
    T: IntoIterator,
    T::Item: ToString,
{
    let iter = coll.into_iter().map(|x| x.to_string());
    StringArray::from_iter_values(iter)
}

fn mk_shape_array<T, P>(coll: T) -> ListArray
where
    T: IntoIterator<Item = Option<P>>,
    P: IntoIterator<Item = u64>,
{
    let iter = coll.into_iter().map(|opt| opt.map(|p| p.into_iter().map(Some)));
    // I don't know how to create a ListArray that has not nullable elements
    let res = ListArray::from_iter_primitive::<UInt64Type, _, _>(iter);
    let (_, offsets, values, nulls) = res.into_parts();
    let field = Arc::new(Field::new("item", arrow::datatypes::DataType::UInt64, false));
    ListArray::new(field, offsets, values, nulls)
}

fn mk_data_type_array<T: IntoIterator<Item = Option<DataType>>>(coll: T) -> StringArray {
    let iter = coll.into_iter().map(|x| x.map(|x| x.to_string()));
    StringArray::from_iter(iter)
}

fn mk_chunk_shape_array<T>(coll: T) -> ListArray
where
    T: IntoIterator<Item = Option<ChunkShape>>,
{
    let iter = coll
        .into_iter()
        .map(|opt| opt.map(|p| p.0.iter().map(|n| Some(n.get())).collect::<Vec<_>>()));
    let res = ListArray::from_iter_primitive::<UInt64Type, _, _>(iter);
    let (_, offsets, values, nulls) = res.into_parts();
    let field = Arc::new(Field::new("item", arrow::datatypes::DataType::UInt64, false));
    ListArray::new(field, offsets, values, nulls)
}

fn mk_chunk_key_encoding_array<T>(coll: T) -> UInt8Array
where
    T: IntoIterator<Item = Option<ChunkKeyEncoding>>,
{
    let iter = coll.into_iter().map(|x| x.map(|x| x.into()));
    UInt8Array::from_iter(iter)
}

fn mk_fill_values_array<T>(coll: T) -> BinaryArray
where
    T: IntoIterator<Item = Option<FillValue>>,
{
    let iter = coll.into_iter().map(|fv| fv.as_ref().map(|f| f.to_be_bytes()));
    BinaryArray::from_iter(iter)
}

fn mk_codecs_array<T, P>(coll: T) -> ListArray
where
    T: IntoIterator<Item = Option<P>>,
    P: IntoIterator<Item = Codec>,
{
    let mut b = ListBuilder::new(BinaryBuilder::new());
    for maybe_list in coll.into_iter() {
        let it = maybe_list.map(|codecs| {
            codecs.into_iter().map(|codec| {
                Some(
                    #[allow(clippy::expect_used)]
                    serde_json::to_vec(&codec)
                        .expect("Invariant violation: Codecs are always serializable"),
                )
            })
        });
        b.append_option(it)
    }
    let res = b.finish();

    // I don't know how to create non nullabe list arrays directly
    let (_, offsets, values, nulls) = res.into_parts();
    let field = Arc::new(Field::new("item", arrow::datatypes::DataType::Binary, false));
    ListArray::new(field, offsets, values, nulls)
}

fn mk_storage_transformers_array<T, P>(coll: T) -> ListArray
where
    T: IntoIterator<Item = Option<P>>,
    P: IntoIterator<Item = StorageTransformer>,
{
    let mut b = ListBuilder::new(BinaryBuilder::new());
    for maybe_list in coll.into_iter() {
        let it = maybe_list.map(|codecs| {
            codecs.into_iter().map(|tr| {
                Some(
                    #[allow(clippy::expect_used)]
                    serde_json::to_vec(&tr)
                        .expect("Invariant violation: Codecs are always serializable"),
                )
            })
        });
        b.append_option(it)
    }
    let res = b.finish();

    // I don't know how to create non nullabe list arrays directly
    let (_, offsets, values, nulls) = res.into_parts();
    let field = Arc::new(Field::new("item", arrow::datatypes::DataType::Binary, false));
    ListArray::new(field, offsets, values, nulls)
}

fn mk_dimension_names_array<T, P>(coll: T) -> ListArray
where
    T: IntoIterator<Item = Option<P>>,
    P: IntoIterator<Item = DimensionName>,
{
    let mut b = ListBuilder::new(StringBuilder::new());
    for list in coll {
        b.append_option(list)
    }
    b.finish()
}

fn mk_user_attributes_array<T: IntoIterator<Item = Option<UserAttributes>>>(
    coll: T,
) -> BinaryArray {
    BinaryArray::from_iter(coll.into_iter().map(|ua| ua.map(|ua| ua.to_bytes())))
}

fn mk_user_attributes_ref_array<T: IntoIterator<Item = Option<ObjectId>>>(
    coll: T,
) -> FixedSizeBinaryArray {
    let iter = coll.into_iter().map(|oid| oid.map(|oid| oid.0));
    #[allow(clippy::expect_used)]
    FixedSizeBinaryArray::try_from_sparse_iter_with_size(iter, ObjectId::SIZE as i32)
        .expect("Invariant violation: bad user attributes ObjectId size")
}

fn mk_user_attributes_row_array<T: IntoIterator<Item = Option<u32>>>(
    coll: T,
) -> UInt32Array {
    UInt32Array::from_iter(coll)
}

fn mk_manifest_refs_array<T, P>(coll: T) -> StructArray
where
    T: IntoIterator<Item = Option<P>>,
    P: IntoIterator<Item = ManifestRef>,
{
    let mut ref_array =
        ListBuilder::new(FixedSizeBinaryBuilder::new(ObjectId::SIZE as i32));
    let mut from_row_array = ListBuilder::new(UInt32Builder::new());
    let mut to_row_array = ListBuilder::new(UInt32Builder::new());

    for m in coll {
        match m {
            None => {
                ref_array.append_null();
                from_row_array.append_null();
                to_row_array.append_null();
            }
            Some(manifests) => {
                for manifest in manifests {
                    #[allow(clippy::expect_used)]
                    ref_array
                        .values()
                        .append_value(manifest.object_id.0)
                        .expect("Invariant violation: bad manifest ObjectId size");
                    from_row_array.values().append_value(manifest.location.0);
                    to_row_array.values().append_value(manifest.location.1);
                }
                ref_array.append(true);
                from_row_array.append(true);
                to_row_array.append(true);
            }
        }
    }
    let ref_array = ref_array.finish();
    let from_row_array = from_row_array.finish();
    let to_row_array = to_row_array.finish();

    // I don't know how to create non nullabe list arrays directly
    let (_, offsets, values, nulls) = ref_array.into_parts();
    let field = Arc::new(Field::new(
        "item",
        arrow::datatypes::DataType::FixedSizeBinary(ObjectId::SIZE as i32),
        false,
    ));
    let ref_array = ListArray::new(field, offsets, values, nulls);

    let (_, offsets, values, nulls) = from_row_array.into_parts();
    let field = Arc::new(Field::new("item", arrow::datatypes::DataType::UInt32, false));
    let from_row_array = ListArray::new(field, offsets, values, nulls);

    let (_, offsets, values, nulls) = to_row_array.into_parts();
    let field = Arc::new(Field::new("item", arrow::datatypes::DataType::UInt32, false));
    let to_row_array = ListArray::new(field, offsets, values, nulls);

    StructArray::from(vec![
        (
            Arc::new(Field::new_list(
                "reference",
                Field::new(
                    "item",
                    arrow::datatypes::DataType::FixedSizeBinary(ObjectId::SIZE as i32),
                    false,
                ),
                true,
            )),
            Arc::new(ref_array) as ArrayRef,
        ),
        (
            Arc::new(Field::new_list(
                "start_row",
                Field::new("item", arrow::datatypes::DataType::UInt32, false),
                true,
            )),
            Arc::new(from_row_array) as ArrayRef,
        ),
        (
            Arc::new(Field::new_list(
                "end_row",
                Field::new("item", arrow::datatypes::DataType::UInt32, false),
                true,
            )),
            Arc::new(to_row_array) as ArrayRef,
        ),
    ])
}

// This is pretty awful, but Rust Try infrastructure is experimental
pub type SnapshotTableResult<E> = Result<SnapshotTable, Result<E, ArrowError>>;

// For testing only
pub fn mk_snapshot_table<E>(
    coll: impl IntoIterator<Item = Result<NodeSnapshot, E>>,
) -> SnapshotTableResult<E> {
    let mut ids = Vec::new();
    let mut types = Vec::new();
    let mut paths = Vec::new();
    let mut shapes = Vec::new();
    let mut data_types = Vec::new();
    let mut chunk_shapes = Vec::new();
    let mut chunk_key_encodings = Vec::new();
    let mut fill_values = Vec::new();
    let mut codecs = Vec::new();
    let mut storage_transformers = Vec::new();
    let mut dimension_names = Vec::new();
    let mut user_attributes_vec = Vec::new();
    let mut user_attributes_ref = Vec::new();
    let mut user_attributes_row = Vec::new();
    let mut manifest_refs_vec = Vec::new();
    // FIXME: add user_attributes_flags
    for node in coll {
        let node = match node {
            Ok(node) => node,
            Err(err) => return Err(Ok(err)),
        };
        ids.push(node.id);
        paths.push(node.path.to_string_lossy().into_owned());
        match node.user_attributes {
            Some(UserAttributesSnapshot::Inline(atts)) => {
                user_attributes_ref.push(None);
                user_attributes_row.push(None);
                user_attributes_vec.push(Some(atts));
            }
            Some(UserAttributesSnapshot::Ref(UserAttributesRef {
                object_id,
                location,
                flags: _flags,
            })) => {
                user_attributes_vec.push(None);
                user_attributes_ref.push(Some(object_id));
                user_attributes_row.push(Some(location));
            }
            None => {
                user_attributes_vec.push(None);
                user_attributes_ref.push(None);
                user_attributes_row.push(None);
            }
        }

        match node.node_data {
            NodeData::Group => {
                types.push(NodeType::Group);
                shapes.push(None);
                data_types.push(None);
                chunk_shapes.push(None);
                chunk_key_encodings.push(None);
                fill_values.push(None);
                codecs.push(None);
                storage_transformers.push(None);
                dimension_names.push(None);
                manifest_refs_vec.push(None);
            }
            NodeData::Array(zarr_metadata, manifest_refs) => {
                types.push(NodeType::Array);
                shapes.push(Some(zarr_metadata.shape));
                data_types.push(Some(zarr_metadata.data_type));
                chunk_shapes.push(Some(zarr_metadata.chunk_shape));
                chunk_key_encodings.push(Some(zarr_metadata.chunk_key_encoding));
                fill_values.push(Some(zarr_metadata.fill_value));
                codecs.push(Some(zarr_metadata.codecs));
                storage_transformers.push(zarr_metadata.storage_transformers);
                dimension_names.push(zarr_metadata.dimension_names);
                manifest_refs_vec.push(Some(manifest_refs));
            }
        }
    }

    let ids = mk_id_array(ids);
    let types = mk_type_array(types);
    let paths = mk_path_array(paths);
    let shapes = mk_shape_array(shapes);
    let data_types = mk_data_type_array(data_types);
    let chunk_shapes = mk_chunk_shape_array(chunk_shapes);
    let chunk_key_encodings = mk_chunk_key_encoding_array(chunk_key_encodings);
    let fill_values = mk_fill_values_array(fill_values);
    let codecs = mk_codecs_array(codecs);
    let storage_transformers = mk_storage_transformers_array(storage_transformers);
    let dimension_names = mk_dimension_names_array(dimension_names);
    let user_attributes_vec = mk_user_attributes_array(user_attributes_vec);
    let user_attributes_ref = mk_user_attributes_ref_array(user_attributes_ref);
    let user_attributes_row = mk_user_attributes_row_array(user_attributes_row);
    let manifest_refs = mk_manifest_refs_array(manifest_refs_vec);

    let columns: Vec<Arc<dyn Array>> = vec![
        Arc::new(ids),
        Arc::new(types),
        Arc::new(paths),
        Arc::new(shapes),
        Arc::new(data_types),
        Arc::new(chunk_shapes),
        Arc::new(chunk_key_encodings),
        Arc::new(fill_values),
        Arc::new(codecs),
        Arc::new(storage_transformers),
        Arc::new(dimension_names),
        Arc::new(user_attributes_vec),
        Arc::new(user_attributes_ref),
        Arc::new(user_attributes_row),
        Arc::new(manifest_refs),
    ];
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", arrow::datatypes::DataType::UInt32, false),
        Field::new("type", arrow::datatypes::DataType::Utf8, false),
        Field::new("path", arrow::datatypes::DataType::Utf8, false),
        Field::new_list(
            "shape",
            Field::new("item", arrow::datatypes::DataType::UInt64, false),
            true,
        ),
        Field::new("data_type", arrow::datatypes::DataType::Utf8, true),
        Field::new_list(
            "chunk_shape",
            Field::new("item", arrow::datatypes::DataType::UInt64, false),
            true,
        ),
        Field::new("chunk_key_encoding", arrow::datatypes::DataType::UInt8, true),
        Field::new("fill_value", arrow::datatypes::DataType::Binary, true),
        Field::new_list(
            "codecs",
            Field::new("item", arrow::datatypes::DataType::Binary, false),
            true,
        ),
        Field::new_list(
            "storage_transformers",
            Field::new("item", arrow::datatypes::DataType::Binary, false),
            true,
        ),
        Field::new_list(
            "dimension_names",
            Field::new("item", arrow::datatypes::DataType::Utf8, true),
            true,
        ),
        Field::new("user_attributes", arrow::datatypes::DataType::Binary, true),
        Field::new(
            "user_attributes_ref",
            arrow::datatypes::DataType::FixedSizeBinary(ObjectId::SIZE as i32),
            true,
        ),
        Field::new("user_attributes_row", arrow::datatypes::DataType::UInt32, true),
        Field::new(
            "manifest_references",
            arrow::datatypes::DataType::Struct(Fields::from(vec![
                Field::new_list(
                    "reference",
                    Field::new(
                        "item",
                        arrow::datatypes::DataType::FixedSizeBinary(
                            ObjectId::SIZE as i32,
                        ),
                        false,
                    ),
                    true,
                ),
                Field::new_list(
                    "start_row",
                    Field::new("item", arrow::datatypes::DataType::UInt32, false),
                    true,
                ),
                Field::new_list(
                    "end_row",
                    Field::new("item", arrow::datatypes::DataType::UInt32, false),
                    true,
                ),
            ])),
            true,
        ),
    ]));
    let batch = RecordBatch::try_new(schema, columns).map_err(Err)?;
    Ok(SnapshotTable { batch })
}

#[cfg(test)]
mod strategies {
    use proptest::prelude::*;
    use proptest::strategy::Strategy;

    use crate::dataset::FillValue;

    pub(crate) fn fill_values_vec_strategy(
    ) -> impl Strategy<Value = Vec<Option<FillValue>>> {
        use proptest::collection::vec;
        vec(proptest::option::of(any::<FillValue>()), 0..10)
    }
}

#[cfg(test)]
#[allow(clippy::panic, clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use crate::format::IcechunkFormatError;

    use super::*;
    use pretty_assertions::assert_eq;
    use proptest::prelude::*;
    use std::{
        collections::HashMap,
        convert::Infallible,
        iter::{self, zip},
    };

    #[test]
    fn test_get_node() -> Result<(), Box<dyn std::error::Error>> {
        let zarr_meta1 = ZarrArrayMetadata {
            shape: vec![10u64, 20, 30],
            data_type: DataType::Float32,
            chunk_shape: ChunkShape(vec![
                NonZeroU64::new(3).unwrap(),
                NonZeroU64::new(2).unwrap(),
                NonZeroU64::new(1).unwrap(),
            ]),
            chunk_key_encoding: ChunkKeyEncoding::Slash,
            fill_value: FillValue::Float32(0f32),

            codecs: vec![Codec {
                name: "mycodec".to_string(),
                configuration: Some(HashMap::from_iter(iter::once((
                    "foo".to_string(),
                    serde_json::Value::from(42),
                )))),
            }],
            storage_transformers: Some(vec![StorageTransformer {
                name: "mytransformer".to_string(),
                configuration: Some(HashMap::from_iter(iter::once((
                    "foo".to_string(),
                    serde_json::Value::from(42),
                )))),
            }]),
            dimension_names: Some(vec![
                Some("x".to_string()),
                Some("y".to_string()),
                Some("t".to_string()),
            ]),
        };
        let zarr_meta2 = ZarrArrayMetadata {
            storage_transformers: None,
            data_type: DataType::Int32,
            dimension_names: Some(vec![None, None, Some("t".to_string())]),
            fill_value: FillValue::Int32(0i32),
            ..zarr_meta1.clone()
        };
        let zarr_meta3 =
            ZarrArrayMetadata { dimension_names: None, ..zarr_meta2.clone() };
        let man_ref1 = ManifestRef {
            object_id: ObjectId::random(),
            location: TableRegion(0, 1),
            flags: Flags(),
            extents: ManifestExtents(vec![]),
        };
        let man_ref2 = ManifestRef {
            object_id: ObjectId::random(),
            location: TableRegion(0, 1),
            flags: Flags(),
            extents: ManifestExtents(vec![]),
        };

        let oid = ObjectId::random();
        let nodes = vec![
            NodeSnapshot {
                path: "/".into(),
                id: 1,
                user_attributes: None,
                node_data: NodeData::Group,
            },
            NodeSnapshot {
                path: "/a".into(),
                id: 2,
                user_attributes: None,
                node_data: NodeData::Group,
            },
            NodeSnapshot {
                path: "/b".into(),
                id: 3,
                user_attributes: None,
                node_data: NodeData::Group,
            },
            NodeSnapshot {
                path: "/b/c".into(),
                id: 4,
                user_attributes: Some(UserAttributesSnapshot::Inline(
                    UserAttributes::try_new(br#"{"foo": "some inline"}"#).unwrap(),
                )),
                node_data: NodeData::Group,
            },
            NodeSnapshot {
                path: "/b/array1".into(),
                id: 5,
                user_attributes: Some(UserAttributesSnapshot::Ref(UserAttributesRef {
                    object_id: oid.clone(),
                    location: 42,
                    flags: Flags(),
                })),
                node_data: NodeData::Array(
                    zarr_meta1.clone(),
                    vec![man_ref1.clone(), man_ref2.clone()],
                ),
            },
            NodeSnapshot {
                path: "/array2".into(),
                id: 6,
                user_attributes: None,
                node_data: NodeData::Array(zarr_meta2.clone(), vec![]),
            },
            NodeSnapshot {
                path: "/b/array3".into(),
                id: 7,
                user_attributes: None,
                node_data: NodeData::Array(zarr_meta3.clone(), vec![]),
            },
        ];
        let st = mk_snapshot_table::<Infallible>(nodes.into_iter().map(Ok)).unwrap();
        assert_eq!(
            st.get_node(&"/nonexistent".into()),
            Err(IcechunkFormatError::NodeNotFound { path: "/nonexistent".into() })
        );

        let node = st.get_node(&"/b/c".into());
        assert_eq!(
            node,
            Ok(NodeSnapshot {
                path: "/b/c".into(),
                id: 4,
                user_attributes: Some(UserAttributesSnapshot::Inline(
                    UserAttributes::try_new(br#"{"foo": "some inline"}"#).unwrap(),
                )),
                node_data: NodeData::Group,
            }),
        );
        let node = st.get_node(&"/".into());
        assert_eq!(
            node,
            Ok(NodeSnapshot {
                path: "/".into(),
                id: 1,
                user_attributes: None,
                node_data: NodeData::Group,
            }),
        );
        let node = st.get_node(&"/b/array1".into());
        assert_eq!(
            node,
            Ok(NodeSnapshot {
                path: "/b/array1".into(),
                id: 5,
                user_attributes: Some(UserAttributesSnapshot::Ref(UserAttributesRef {
                    object_id: oid,
                    location: 42,
                    flags: Flags(),
                })),
                node_data: NodeData::Array(zarr_meta1.clone(), vec![man_ref1, man_ref2]),
            }),
        );
        let node = st.get_node(&"/array2".into());
        assert_eq!(
            node,
            Ok(NodeSnapshot {
                path: "/array2".into(),
                id: 6,
                user_attributes: None,
                node_data: NodeData::Array(zarr_meta2.clone(), vec![]),
            }),
        );
        let node = st.get_node(&"/b/array3".into());
        assert_eq!(
            node,
            Ok(NodeSnapshot {
                path: "/b/array3".into(),
                id: 7,
                user_attributes: None,
                node_data: NodeData::Array(zarr_meta3.clone(), vec![]),
            }),
        );
        Ok(())
    }

    fn decode_fill_values_array(
        dtypes: Vec<Option<DataType>>,
        array: BinaryArray,
    ) -> IcechunkResult<Vec<Option<FillValue>>> {
        zip(dtypes, array.iter())
            .map(|(dt, value)| {
                dt.map(|dt| {
                    FillValue::from_data_type_and_value(
                        &dt,
                        value.ok_or(IcechunkFormatError::NullElement {
                            index: 0,
                            column_name: "unknown".to_string(),
                        })?,
                    )
                })
            })
            .map(|x| x.transpose())
            .collect()
    }

    #[test]
    fn test_fill_values_vec_roundtrip() {
        let fill_values = vec![
            Some(FillValue::Bool(true)),
            Some(FillValue::Bool(false)),
            None, // for groups
            Some(FillValue::Int8(0i8)),
            Some(FillValue::Int16(0i16)),
            Some(FillValue::Int32(0i32)),
            Some(FillValue::Int64(0i64)),
            None,
            Some(FillValue::UInt8(0u8)),
            Some(FillValue::UInt16(0u16)),
            Some(FillValue::UInt32(0u32)),
            Some(FillValue::UInt64(0u64)),
            None, // for groups
            Some(FillValue::Float16(0f32)),
            Some(FillValue::Float32(0f32)),
            Some(FillValue::Float64(0f64)),
            None, // for groups
            Some(FillValue::Complex64(0f32, 1f32)),
            Some(FillValue::Complex128(0f64, 1f64)),
            None, // for groups
            Some(FillValue::RawBits(vec![b'1'])),
        ];

        let dtypes: Vec<Option<DataType>> =
            fill_values.iter().map(|x| x.as_ref().map(|x| x.get_data_type())).collect();
        let encoded = mk_fill_values_array(fill_values.clone());
        let decoded = decode_fill_values_array(dtypes, encoded).unwrap();

        assert_eq!(fill_values, decoded);
    }

    proptest! {
        #[test]
        fn test_fill_values_vec_roundtrip_prop(
            fill_values in strategies::fill_values_vec_strategy()
        ) {
            let dtypes: Vec<Option<DataType>> = fill_values
                .iter()
                .map(|x| x.as_ref().map(|x| x.get_data_type()))
                .collect();
            let encoded = mk_fill_values_array(fill_values.clone());
            let decoded = decode_fill_values_array(dtypes, encoded).unwrap();
            prop_assert_eq!(fill_values, decoded);
        }
    }
}
