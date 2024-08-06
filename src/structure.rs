use std::{num::NonZeroU64, sync::Arc};

use std::collections::HashMap;

use arrow::array::FixedSizeListBuilder;
use arrow::array::Float32Builder;
use arrow::buffer::ScalarBuffer;
use arrow::datatypes::DataType as ArrowDataType;
use arrow::datatypes::UnionFields;
use arrow::error::ArrowError;

use arrow::{
    array::{
        Array, ArrayRef, AsArray, BinaryArray, FixedSizeBinaryArray, FixedSizeBinaryBuilder,
        FixedSizeListArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array,
        Int8Array, ListArray, ListBuilder, RecordBatch, StringArray, StringBuilder, StructArray,
        UInt32Array, UInt32Builder, UInt8Array, UnionArray,
    },
    datatypes::{
        Field, Fields, Float32Type, Float64Type, Int32Type, Int64Type, Schema, UInt32Type,
        UInt64Type, UInt8Type,
    },
};
use itertools::izip;

use crate::{
    ChunkKeyEncoding, ChunkShape, Codecs, DataType, DimensionName, FillValue, Flags,
    ManifestExtents, ManifestRef, NodeData, NodeId, NodeStructure, NodeType, ObjectId, Path,
    StorageTransformers, TableRegion, UserAttributes, UserAttributesRef, UserAttributesStructure,
    ZarrArrayMetadata,
};

// TODO: is there a better pattern
const NONE_FILL_VALUE_SENTINEL: i8 = 0i8;

fn fill_value_union_fields() -> UnionFields {
    [
        (
            NONE_FILL_VALUE_SENTINEL,
            Arc::new(Field::new("none", ArrowDataType::Int8, false)),
        ),
        (
            FillValue::Int32(0).discriminant(),
            Arc::new(Field::new("int32", ArrowDataType::Int32, true)),
        ),
        (
            FillValue::Int64(0).discriminant(),
            Arc::new(Field::new("int64", ArrowDataType::Int64, true)),
        ),
        (
            FillValue::Float32(0f32).discriminant(),
            Arc::new(Field::new("float32", ArrowDataType::Float32, true)),
        ),
        (
            FillValue::Float64(0f64).discriminant(),
            Arc::new(Field::new("float64", ArrowDataType::Float64, true)),
        ),
        (
            FillValue::Complex64(0f32, 0f32).discriminant(),
            Arc::new(Field::new(
                "complex64",
                ArrowDataType::FixedSizeList(
                    Arc::new(Field::new_list_field(ArrowDataType::Float32, true)),
                    2, // 2 element list for complex numbers
                ),
                false,
            )),
        ),
        (
            FillValue::RawBits(vec![b'1']).discriminant(),
            Arc::new(Field::new("raw", ArrowDataType::Binary, true)),
        ),
    ]
    .into_iter()
    .collect::<UnionFields>()
}

pub struct StructureTable {
    batch: RecordBatch,
}

impl StructureTable {
    pub fn get_node(&self, path: &Path) -> Option<NodeStructure> {
        // FIXME: optimize
        let paths: &StringArray = self.batch.column_by_name("path")?.as_string_opt()?;
        let needle = path.to_str();
        let idx = paths.iter().position(|s| s == needle);
        idx.and_then(|idx| self.build_node_structure(path, idx))
    }

    // FIXME: there should be a failure reason here, so return a Result
    fn build_zarr_array_metadata(&self, idx: usize) -> Option<ZarrArrayMetadata> {
        // FIXME: all of this should check for nulls in the columns and fail
        let shape = self
            .batch
            .column_by_name("shape")?
            .as_list_opt::<i32>()?
            .value(idx)
            .as_primitive_opt::<UInt64Type>()?
            .iter()
            .flatten()
            .collect();
        let data_type = DataType::try_from(
            self.batch.column_by_name("data_type")?.as_string_opt::<i32>()?.value(idx),
        )
        .ok()?;
        let chunk_shape = ChunkShape(
            self.batch
                .column_by_name("chunk_shape")?
                .as_list_opt::<i32>()?
                .value(idx)
                .as_primitive_opt::<UInt64Type>()?
                .iter()
                .filter_map(|x| x.and_then(NonZeroU64::new))
                .collect(),
        );
        let key_encoding_u8 = self
            .batch
            .column_by_name("chunk_key_encoding")?
            .as_primitive_opt::<UInt8Type>()?
            .value(idx);

        let chunk_key_encoding = key_encoding_u8.try_into().ok()?;

        let codecs = Codecs(
            self.batch
                .column_by_name("codecs")?
                .as_string_opt::<i32>()?
                .value(idx)
                .to_string(),
        );

        let storage_transformers =
            self.batch.column_by_name("storage_transformers")?.as_string_opt::<i32>()?;
        let storage_transformers = if storage_transformers.is_null(idx) {
            None
        } else {
            Some(StorageTransformers(storage_transformers.value(idx).to_string()))
        };

        let dimension_names =
            self.batch.column_by_name("dimension_names")?.as_list_opt::<i32>()?;
        let dimension_names = if dimension_names.is_null(idx) {
            None
        } else {
            Some(
                dimension_names
                    .value(idx)
                    .as_string_opt::<i32>()?
                    .iter()
                    .map(|x| x.map(|x| x.to_string()))
                    .collect(),
            )
        };

        let fill_values = decode_fill_values_array(
            self.batch.column_by_name("fill_value")?
                .as_any()
                .downcast_ref::<UnionArray>()
                .unwrap()
                .clone() // TODO: don't understand this
        );
        let fill_value = fill_values[idx].to_owned().unwrap();

        Some(ZarrArrayMetadata {
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

    // FIXME: there should be a failure reason here, so return a Result
    fn build_manifest_refs(&self, idx: usize) -> Option<Vec<ManifestRef>> {
        let manifest_refs_array =
            self.batch.column_by_name("manifest_references")?.as_struct_opt()?;
        if manifest_refs_array.is_valid(idx) {
            let refs = manifest_refs_array
                .column_by_name("reference")?
                .as_list_opt::<i32>()?
                .value(idx);
            let refs = refs.as_fixed_size_binary_opt()?;

            let row_from = manifest_refs_array
                .column_by_name("start_row")?
                .as_list_opt::<i32>()?
                .value(idx);
            let row_from = row_from.as_primitive_opt::<UInt32Type>()?;

            let row_to = manifest_refs_array
                .column_by_name("end_row")?
                .as_list_opt::<i32>()?
                .value(idx);
            let row_to = row_to.as_primitive_opt::<UInt32Type>()?;

            // FIXME: add extents and flags

            let it = izip!(refs.iter(), row_from.iter(), row_to.iter())
                .filter_map(|(r, f, t)| Some((r?.try_into().ok()?, f?, t?)));
            let res = it
                .map(|(r, f, t)| ManifestRef {
                    object_id: ObjectId(r),
                    location: TableRegion(f, t),
                    // FIXME: flags
                    flags: Flags(),
                    // FIXME: extents
                    extents: ManifestExtents(vec![]),
                })
                .collect();
            Some(res)
        } else {
            None
        }
    }

    fn build_node_structure(&self, path: &Path, idx: usize) -> Option<NodeStructure> {
        let node_type =
            self.batch.column_by_name("type")?.as_string_opt::<i32>()?.value(idx);
        let id =
            self.batch.column_by_name("id")?.as_primitive_opt::<UInt32Type>()?.value(idx);
        let user_attributes = self.build_user_attributes(idx);
        match node_type {
            "group" => Some(NodeStructure {
                path: path.clone(),
                id,
                user_attributes,
                node_data: NodeData::Group,
            }),
            "array" => Some(NodeStructure {
                path: path.clone(),
                id,
                user_attributes,
                node_data: NodeData::Array(
                    self.build_zarr_array_metadata(idx)?,
                    self.build_manifest_refs(idx)?,
                ),
            }),
            _ => None,
        }
    }

    fn build_user_attributes(&self, idx: usize) -> Option<UserAttributesStructure> {
        let inline =
            self.batch.column_by_name("user_attributes")?.as_string_opt::<i32>()?;
        if inline.is_valid(idx) {
            Some(UserAttributesStructure::Inline(inline.value(idx).to_string()))
        } else {
            self.build_user_attributes_ref(idx)
        }
    }
    fn build_user_attributes_ref(&self, idx: usize) -> Option<UserAttributesStructure> {
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
            Some(UserAttributesStructure::Ref(UserAttributesRef {
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

fn mk_codecs_array<T: IntoIterator<Item = Option<Codecs>>>(coll: T) -> StringArray {
    let iter = coll.into_iter().map(|x| x.map(|x| x.0));
    StringArray::from_iter(iter)
}

fn mk_storage_transformers_array<T: IntoIterator<Item = Option<StorageTransformers>>>(
    coll: T,
) -> StringArray {
    let iter = coll.into_iter().map(|x| x.map(|x| x.0));
    StringArray::from_iter(iter)
}

fn mk_dimension_names_array<T, P>(coll: T) -> ListArray
where
    T: IntoIterator<Item = Option<P>>,
    P: IntoIterator<Item = Option<DimensionName>>,
{
    let mut b = ListBuilder::new(StringBuilder::new());
    for list in coll {
        b.append_option(list)
    }
    b.finish()
}

fn mk_user_attributes_array<T: IntoIterator<Item = Option<UserAttributes>>>(
    coll: T,
) -> StringArray {
    StringArray::from_iter(coll)
}

fn mk_user_attributes_ref_array<T: IntoIterator<Item = Option<ObjectId>>>(
    coll: T,
) -> FixedSizeBinaryArray {
    let iter = coll.into_iter().map(|oid| oid.map(|oid| oid.0));
    FixedSizeBinaryArray::try_from_sparse_iter_with_size(iter, ObjectId::SIZE as i32)
        .expect("Bad ObjectId size")
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
                    ref_array
                        .values()
                        .append_value(manifest.object_id.0)
                        .expect("Error appending to manifest reference array");
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

fn to_fill_value(type_name: &str, value: Arc<dyn Array>) -> Option<FillValue> {
    dbg!("in to_fill_value");
    let result: Option<FillValue> = match type_name {
        "none" => None,
        "int32" => Some(FillValue::Int32(
            value.as_primitive_opt::<Int32Type>()?.value(0),
        )),
        "int64" => Some(FillValue::Int64(
            value.as_primitive_opt::<Int64Type>()?.value(0),
        )),
        "float32" => Some(FillValue::Float32(
            value.as_primitive_opt::<Float32Type>()?.value(0),
        )),
        "float64" => Some(FillValue::Float64(
            value.as_primitive_opt::<Float64Type>()?.value(0),
        )),
        "complex64" => {
            // TODO: Learn about making this a function that's generic in Float32, Float64
            let fill_value = value.as_fixed_size_list().value(0);
            let as_array = fill_value.as_primitive_opt::<Float32Type>()?;
            Some(FillValue::Complex64(as_array.value(0), as_array.value(1)))
        }
        "raw" => {
            let as_vec = value.as_binary_opt::<i32>()?.values().as_slice().to_owned();
            Some(FillValue::RawBits(as_vec))
        }
        _ => None,
    };
    result
}

fn mk_fill_values_array(fill_values: Vec<Option<FillValue>>) -> Result<UnionArray, ArrowError> {
    let mut none_vec = Vec::new();
    let mut int32_vec = Vec::new();
    let mut int64_vec = Vec::new();
    let mut float32_vec = Vec::new();
    let mut float64_vec = Vec::new();
    let mut complex64_vec = Vec::new();
    let mut rawbits_vec = Vec::new();

    let mut type_ids: Vec<i8> = Vec::new();
    let mut offsets_vec: Vec<i32> = Vec::new();

    // Construct the UnionArray, sadly UnionBuilder does not support the more complex types
    // like ListArray or Binary
    // Iterate over input; append to vecs
    for val in fill_values.iter() {
        // TODO: what if there are no matches?
        match val.to_owned() {
            None => {
                // required because type_ids.len() == offsets_vec.len() is enforced
                offsets_vec.push(0i32);
                type_ids.push(NONE_FILL_VALUE_SENTINEL);
                if none_vec.len() == 0 {
                    none_vec.push(0i8)
                }
            }
            Some(FillValue::Int32(v)) => {
                offsets_vec.push(int32_vec.len() as i32);
                int32_vec.push(v);
                type_ids.push(FillValue::Int32(0).discriminant());
            }
            Some(FillValue::Int64(v)) => {
                offsets_vec.push(int64_vec.len() as i32);
                int64_vec.push(v);
                type_ids.push(FillValue::Int64(0i64).discriminant());
            }
            Some(FillValue::Float32(v)) => {
                offsets_vec.push(float32_vec.len() as i32);
                float32_vec.push(v);
                type_ids.push(FillValue::Float32(0f32).discriminant());
            }
            Some(FillValue::Float64(v)) => {
                offsets_vec.push(float64_vec.len() as i32);
                float64_vec.push(v);
                type_ids.push(FillValue::Float64(0f64).discriminant());
            }
            Some(FillValue::Complex64(r, i)) => {
                offsets_vec.push(complex64_vec.len() as i32);
                complex64_vec.push((r, i));
                type_ids.push(FillValue::Complex64(0f32, 0f32).discriminant());
            }
            Some(FillValue::RawBits(v)) => {
                offsets_vec.push(rawbits_vec.len() as i32);
                rawbits_vec.push(v);
                type_ids.push(FillValue::RawBits(vec![b'1']).discriminant());
            }
            _ => (), // TODO:
        }
    }

    let type_ids = type_ids.into_iter().collect::<ScalarBuffer<i8>>();
    let offsets = offsets_vec.into_iter().collect::<ScalarBuffer<i32>>();
    let children: Vec<Arc<dyn Array>> = vec![
        // order here MUST match order of union_fields
        Arc::new(Int8Array::from(none_vec)),
        Arc::new(Int32Array::from(int32_vec)),
        Arc::new(Int64Array::from(int64_vec)),
        Arc::new(Float32Array::from(float32_vec)),
        Arc::new(Float64Array::from(float64_vec)),
        Arc::new(build_complex_list_array(complex64_vec)),
        Arc::new(BinaryArray::from_iter_values(
            rawbits_vec.iter().map(|x| x.as_slice()),
        )),
    ];
    // dbg!(children.clone(), type_ids.clone(), offsets.clone());

    UnionArray::try_new(fill_value_union_fields(), type_ids, Some(offsets), children)
}

fn decode_fill_values_array(array: UnionArray) -> Vec<Option<FillValue>> {
    let mut fill_values = Vec::new();
    let type_names = array.type_names();
    dbg!(type_names.clone(), array.clone(), array.len());
    for idx in 0..array.len() {
        // DC: Why can't this be outside the for?
        let type_id = array.type_id(idx);
        let type_name = type_names[type_id as usize];
        dbg!(type_name, array.value(idx).clone());
        // TODO: I think I need Result instead of Option somewhere
        fill_values.push(
            to_fill_value(type_name, array.value(idx)), //.ok_or("Error Decoding fill value")
        )
    }
    fill_values
}

fn build_complex_list_array(values: Vec<(f32, f32)>) -> FixedSizeListArray {
    let values_builder = Float32Builder::new();
    let mut builder = FixedSizeListBuilder::new(values_builder, 2);
    for val in values.iter() {
        let (real, imag) = val;
        builder.values().append_value(real.to_owned());
        builder.values().append_value(imag.to_owned());
        builder.append(true);
    }
    builder.finish()
}

// For testing only
pub fn mk_structure_table<T: IntoIterator<Item = NodeStructure>>(
    coll: T,
) -> StructureTable {
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
        ids.push(node.id);
        paths.push(node.path.to_string_lossy().into_owned());
        match node.user_attributes {
            Some(UserAttributesStructure::Inline(atts)) => {
                user_attributes_ref.push(None);
                user_attributes_row.push(None);
                user_attributes_vec.push(Some(atts));
            }
            Some(UserAttributesStructure::Ref(UserAttributesRef {
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

    // DC: Vec -> Arrow Arrays, why not directly up top?
    // Also are you allowed to change type like this
    let ids = mk_id_array(ids);
    let types = mk_type_array(types);
    let paths = mk_path_array(paths);
    let shapes = mk_shape_array(shapes);
    let data_types = mk_data_type_array(data_types);
    let chunk_shapes = mk_chunk_shape_array(chunk_shapes);
    let chunk_key_encodings = mk_chunk_key_encoding_array(chunk_key_encodings);
    let fill_values = mk_fill_values_array(fill_values).unwrap(); // TODO: remove unwrap here
    let codecs = mk_codecs_array(codecs);
    let storage_transformers = mk_storage_transformers_array(storage_transformers);
    let dimension_names = mk_dimension_names_array(dimension_names);
    let user_attributes_vec = mk_user_attributes_array(user_attributes_vec);
    let user_attributes_ref = mk_user_attributes_ref_array(user_attributes_ref);
    let user_attributes_row = mk_user_attributes_row_array(user_attributes_row);
    let manifest_refs = mk_manifest_refs_array(manifest_refs_vec);

    // TODO: DC: what is this dyn thing?
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
        Field::new(
            "fill_value",
            arrow::datatypes::DataType::Union(
                // TODO: Is dense correct here?
                fill_value_union_fields(),
                arrow::datatypes::UnionMode::Dense,
            ),
            true,
        ),
        Field::new("codecs", arrow::datatypes::DataType::Utf8, true),
        Field::new("storage_transformers", arrow::datatypes::DataType::Utf8, true),
        Field::new_list(
            "dimension_names",
            Field::new("item", arrow::datatypes::DataType::Utf8, true),
            true,
        ),
        Field::new("user_attributes", arrow::datatypes::DataType::Utf8, true),
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
    let batch =
        RecordBatch::try_new(schema, columns).expect("Error creating record batch");
    StructureTable { batch }
}

fn fixed_size_list_array_from_vec(value: Vec<f32>) -> FixedSizeListArray {
    let values_builder = Float32Builder::new();
    let mut builder = FixedSizeListBuilder::new(values_builder, 2);
    builder.values().append_value(value[0]);
    builder.values().append_value(value[1]);
    builder.append(true);
    builder.finish()
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;

    #[test]
    fn test_get_node() {
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
            codecs: Codecs("codec".to_string()),
            storage_transformers: Some(StorageTransformers("tranformers".to_string())),
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
            NodeStructure {
                path: "/".into(),
                id: 1,
                user_attributes: None,
                node_data: NodeData::Group,
            },
            NodeStructure {
                path: "/a".into(),
                id: 2,
                user_attributes: None,
                node_data: NodeData::Group,
            },
            NodeStructure {
                path: "/b".into(),
                id: 3,
                user_attributes: None,
                node_data: NodeData::Group,
            },
            NodeStructure {
                path: "/b/c".into(),
                id: 4,
                user_attributes: Some(UserAttributesStructure::Inline(
                    "some inline".to_string(),
                )),
                node_data: NodeData::Group,
            },
            NodeStructure {
                path: "/b/array1".into(),
                id: 5,
                user_attributes: Some(UserAttributesStructure::Ref(UserAttributesRef {
                    object_id: oid.clone(),
                    location: 42,
                    flags: Flags(),
                })),
                node_data: NodeData::Array(
                    zarr_meta1.clone(),
                    vec![man_ref1.clone(), man_ref2.clone()],
                ),
            },
            NodeStructure {
                path: "/array2".into(),
                id: 6,
                user_attributes: None,
                node_data: NodeData::Array(zarr_meta2.clone(), vec![]),
            },
            NodeStructure {
                path: "/b/array3".into(),
                id: 7,
                user_attributes: None,
                node_data: NodeData::Array(zarr_meta3.clone(), vec![]),
            },
        ];
        let st = mk_structure_table(nodes);
        assert_eq!(st.get_node(&"/nonexistent".into()), None);

        let node = st.get_node(&"/b/c".into());
        assert_eq!(
            node,
            Some(NodeStructure {
                path: "/b/c".into(),
                id: 4,
                user_attributes: Some(UserAttributesStructure::Inline(
                    "some inline".to_string()
                )),
                node_data: NodeData::Group,
            }),
        );
        let node = st.get_node(&"/".into());
        assert_eq!(
            node,
            Some(NodeStructure {
                path: "/".into(),
                id: 1,
                user_attributes: None,
                node_data: NodeData::Group,
            }),
        );
        let node = st.get_node(&"/b/array1".into());
        assert_eq!(
            node,
            Some(NodeStructure {
                path: "/b/array1".into(),
                id: 5,
                user_attributes: Some(UserAttributesStructure::Ref(UserAttributesRef {
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
            Some(NodeStructure {
                path: "/array2".into(),
                id: 6,
                user_attributes: None,
                node_data: NodeData::Array(zarr_meta2.clone(), vec![]),
            }),
        );
        let node = st.get_node(&"/b/array3".into());
        assert_eq!(
            node,
            Some(NodeStructure {
                path: "/b/array3".into(),
                id: 7,
                user_attributes: None,
                node_data: NodeData::Array(zarr_meta3.clone(), vec![]),
            }),
        );
    }
    #[test]
    fn test_fill_values_vec_roundtrip() {
        let fill_values = vec![
            None, // for groups
            Some(FillValue::Int32(0i32)),
            Some(FillValue::Int64(0i64)),
            Some(FillValue::Float32(0f32)),
            Some(FillValue::Float64(0f64)),
            Some(FillValue::Complex64(0f32, 1f32)),
            Some(FillValue::RawBits(vec![b'1'])),
        ];

        // TOOD: understand this clone
        let encoded =
            mk_fill_values_array(fill_values.clone()).expect("Failed to decode fill_value.");
        let decoded = decode_fill_values_array(encoded);

        assert_eq!(fill_values, decoded);
    }

    #[test]
    fn test_fill_value_decode() {
        // int32
        let value = 1i32;
        let expected = FillValue::Int32(value);
        let encoded = Int32Array::from(vec![value]);
        let actual = to_fill_value("int32", Arc::new(encoded)).unwrap();
        assert_eq!(expected, actual);

        // int64
        let value = 1i64;
        let expected = FillValue::Int64(value);
        let encoded = Int64Array::from(vec![value]);
        let actual = to_fill_value("int64", Arc::new(encoded)).unwrap();
        assert_eq!(expected, actual);

        // float64
        let value = 1f64;
        let expected = FillValue::Float64(value);
        let encoded = Float64Array::from(vec![value]);
        let actual = to_fill_value("float64", Arc::new(encoded)).unwrap();
        assert_eq!(expected, actual);

        // // complex64
        let value = vec![1.0f32, 2.0f32];
        let expected = FillValue::Complex64(value[0], value[1]);
        let encoded = fixed_size_list_array_from_vec(value);
        let actual = to_fill_value("complex64", Arc::new(encoded)).unwrap();
        assert_eq!(expected, actual);

        // // binary
        let value = b"123";
        let expected = FillValue::RawBits(value.to_vec());
        let encoded = BinaryArray::from_vec(vec![value]);
        let actual = to_fill_value("raw", Arc::new(encoded)).unwrap();
        assert_eq!(expected, actual);
    }
}
