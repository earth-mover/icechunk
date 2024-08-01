use std::{num::NonZeroU64, sync::Arc};

use arrow::{
    array::{
        Array, AsArray, ListArray, ListBuilder, RecordBatch, StringArray, StringBuilder,
        UInt32Array, UInt8Array,
    },
    datatypes::{Field, Schema, UInt32Type, UInt64Type, UInt8Type},
};

use crate::{
    ArrayStructure, ChunkKeyEncoding, ChunkShape, Codecs, DataType, DimensionName, FillValue,
    GroupStructure, NodeId, NodeStructure, NodeType, Path, StorageTransformers, ZarrArrayMetadata,
};

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
            self.batch
                .column_by_name("data_type")?
                .as_string_opt::<i32>()?
                .value(idx),
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

        let storage_transformers = self
            .batch
            .column_by_name("storage_transformers")?
            .as_string_opt::<i32>()?;
        let storage_transformers = if storage_transformers.is_null(idx) {
            None
        } else {
            Some(StorageTransformers(
                storage_transformers.value(idx).to_string(),
            ))
        };

        let dimension_names = self
            .batch
            .column_by_name("dimension_names")?
            .as_list_opt::<i32>()?;
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

        Some(ZarrArrayMetadata {
            shape,
            data_type,
            chunk_shape,
            chunk_key_encoding,
            fill_value: FillValue::Int8(0), // FIXME: implement
            codecs,
            storage_transformers,
            dimension_names,
        })
    }

    fn build_node_structure(&self, path: &Path, idx: usize) -> Option<NodeStructure> {
        let node_type = self
            .batch
            .column_by_name("type")?
            .as_string_opt::<i32>()?
            .value(idx);
        let id = self
            .batch
            .column_by_name("id")?
            .as_primitive_opt::<UInt32Type>()?
            .value(idx);
        match node_type {
            "group" => Some(NodeStructure::Group(GroupStructure {
                path: path.clone(),
                id,
            })),
            "array" => {
                let zarr_metadata = self.build_zarr_array_metadata(idx)?;
                let array = ArrayStructure {
                    path: path.clone(),
                    id,
                    zarr_metadata,
                };
                Some(NodeStructure::Array(array))
            }
            _ => None,
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
    let iter = coll
        .into_iter()
        .map(|opt| opt.map(|p| p.into_iter().map(Some)));
    // I don't know how to create a ListArray that has not nullable elements
    let res = ListArray::from_iter_primitive::<UInt64Type, _, _>(iter);
    let (_, offsets, values, nulls) = res.into_parts();
    let field = Arc::new(Field::new(
        "item",
        arrow::datatypes::DataType::UInt64,
        false,
    ));
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
    let field = Arc::new(Field::new(
        "item",
        arrow::datatypes::DataType::UInt64,
        false,
    ));
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

// For testing only
pub fn mk_structure_table<T: IntoIterator<Item = NodeStructure>>(coll: T) -> StructureTable {
    let mut ids = Vec::new();
    let mut types = Vec::new();
    let mut paths = Vec::new();
    let mut shapes = Vec::new();
    let mut data_types = Vec::new();
    let mut chunk_shapes = Vec::new();
    let mut chunk_key_encodings = Vec::new();
    let mut codecs = Vec::new();
    let mut storage_transformers = Vec::new();
    let mut dimension_names = Vec::new();
    for node in coll {
        match node {
            NodeStructure::Group(GroupStructure { id, path }) => {
                types.push(NodeType::Group);
                ids.push(id);
                paths.push(path.to_string_lossy().into_owned());
                shapes.push(None);
                data_types.push(None);
                chunk_shapes.push(None);
                chunk_key_encodings.push(None);
                codecs.push(None);
                storage_transformers.push(None);
                dimension_names.push(None);
            }
            NodeStructure::Array(ArrayStructure {
                id,
                path,
                zarr_metadata,
            }) => {
                types.push(NodeType::Array);
                ids.push(id);
                paths.push(path.to_string_lossy().into_owned());
                shapes.push(Some(zarr_metadata.shape));
                data_types.push(Some(zarr_metadata.data_type));
                chunk_shapes.push(Some(zarr_metadata.chunk_shape));
                chunk_key_encodings.push(Some(zarr_metadata.chunk_key_encoding));
                codecs.push(Some(zarr_metadata.codecs));
                storage_transformers.push(zarr_metadata.storage_transformers);
                dimension_names.push(zarr_metadata.dimension_names);
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
    let codecs = mk_codecs_array(codecs);
    let storage_transformers = mk_storage_transformers_array(storage_transformers);
    let dimension_names = mk_dimension_names_array(dimension_names);
    let columns: Vec<Arc<dyn Array>> = vec![
        Arc::new(ids),
        Arc::new(types),
        Arc::new(paths),
        Arc::new(shapes),
        Arc::new(data_types),
        Arc::new(chunk_shapes),
        Arc::new(chunk_key_encodings),
        Arc::new(codecs),
        Arc::new(storage_transformers),
        Arc::new(dimension_names),
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
        Field::new(
            "chunk_key_encoding",
            arrow::datatypes::DataType::UInt8,
            true,
        ),
        // FIXME:
        //Field::new("fill_value", todo!(), true),
        Field::new("codecs", arrow::datatypes::DataType::Utf8, true),
        Field::new(
            "storage_transformers",
            arrow::datatypes::DataType::Utf8,
            true,
        ),
        Field::new_list(
            "dimension_names",
            Field::new("item", arrow::datatypes::DataType::Utf8, true),
            true,
        ),
    ]));
    let batch = RecordBatch::try_new(schema, columns).expect("Error creating record batch");
    StructureTable { batch }
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
            dimension_names: Some(vec![None, None, Some("t".to_string())]),
            ..zarr_meta1.clone()
        };
        let zarr_meta3 = ZarrArrayMetadata {
            dimension_names: None,
            ..zarr_meta2.clone()
        };

        let nodes = vec![
            NodeStructure::Group(GroupStructure {
                path: "/".into(),
                id: 1,
            }),
            NodeStructure::Group(GroupStructure {
                path: "/a".into(),
                id: 2,
            }),
            NodeStructure::Group(GroupStructure {
                path: "/b".into(),
                id: 3,
            }),
            NodeStructure::Group(GroupStructure {
                path: "/b/c".into(),
                id: 4,
            }),
            NodeStructure::Array(ArrayStructure {
                path: "/b/array1".into(),
                id: 5,
                zarr_metadata: zarr_meta1.clone(),
            }),
            NodeStructure::Array(ArrayStructure {
                path: "/array2".into(),
                id: 5,
                zarr_metadata: zarr_meta2.clone(),
            }),
            NodeStructure::Array(ArrayStructure {
                path: "/b/array3".into(),
                id: 5,
                zarr_metadata: zarr_meta3.clone(),
            }),
        ];
        let st = mk_structure_table(nodes);
        assert_eq!(st.get_node(&"/nonexistent".into()), None);

        let node = st.get_node(&"/b/c".into());
        assert_eq!(
            node,
            Some(NodeStructure::Group(GroupStructure {
                path: "/b/c".into(),
                id: 4,
            }))
        );
        let node = st.get_node(&"/".into());
        assert_eq!(
            node,
            Some(NodeStructure::Group(GroupStructure {
                path: "/".into(),
                id: 1,
            }))
        );
        let node = st.get_node(&"/b/array1".into());
        assert_eq!(
            node,
            Some(NodeStructure::Array(ArrayStructure {
                path: "/b/array1".into(),
                id: 5,
                zarr_metadata: zarr_meta1,
            }),)
        );
        let node = st.get_node(&"/array2".into());
        assert_eq!(
            node,
            Some(NodeStructure::Array(ArrayStructure {
                path: "/array2".into(),
                id: 5,
                zarr_metadata: zarr_meta2,
            }),)
        );
        let node = st.get_node(&"/b/array3".into());
        assert_eq!(
            node,
            Some(NodeStructure::Array(ArrayStructure {
                path: "/b/array3".into(),
                id: 5,
                zarr_metadata: zarr_meta3,
            }),)
        );
    }
}
