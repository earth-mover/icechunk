use std::{num::NonZeroU64, sync::Arc};

use arrow::{
    array::{Array, AsArray, ListArray, RecordBatch, StringArray, UInt32Array},
    datatypes::{Field, Schema, UInt32Type, UInt64Type},
};

use crate::{
    ArrayStructure, ChunkKeyEncoding, ChunkShape, Codecs, DataType, FillValue, GroupStructure,
    NodeId, NodeStructure, NodeType, Path, StorageTransformers, ZarrArrayMetadata,
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

    fn build_zarr_array_metadata(&self, idx: usize) -> Option<ZarrArrayMetadata> {
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
        let encs = self
            .batch
            .column_by_name("chunk_key_encoding")?
            .as_primitive_opt::<UInt32Type>()?;

        let chunk_key_encoding_char = if encs.is_null(idx) {
            None
        } else {
            Some(encs.value(idx))
        };
        let chunk_key_encoding =
            ChunkKeyEncoding::from_char(chunk_key_encoding_char.and_then(char::from_u32))?;

        let codecs = Codecs(
            self.batch
                .column_by_name("codecs")?
                .as_string_opt::<i32>()?
                .value(idx)
                .to_string(),
        );

        let storage_transformers = StorageTransformers(
            self.batch
                .column_by_name("storage_transformers")?
                .as_string_opt::<i32>()?
                .value(idx)
                .to_string(),
        );

        let dimension_names = self
            .batch
            .column_by_name("dimension_names")?
            .as_list_opt::<i32>()?
            .value(idx)
            .as_string_opt::<i32>()?
            .iter()
            .filter_map(|x| x.map(|x| x.to_string()))
            .collect();

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
    ListArray::from_iter_primitive::<UInt64Type, _, _>(iter)
}

fn mk_data_type_array<T: IntoIterator<Item = DataType>>(coll: T) -> StringArray {
    let iter = coll.into_iter().map(|x| x.to_string());
    StringArray::from_iter_values(iter)
}

fn mk_chunk_shape_array<T, P>(coll: T) -> ListArray
where
    T: IntoIterator<Item = Option<P>>,
    P: IntoIterator<Item = u64>,
{
    let iter = coll
        .into_iter()
        .map(|opt| opt.map(|p| p.into_iter().map(Some)));
    ListArray::from_iter_primitive::<UInt64Type, _, _>(iter)
}

// For testing only
fn mk_structure_table<T: IntoIterator<Item = NodeStructure>>(coll: T) -> StructureTable {
    let mut ids = Vec::new();
    let mut types = Vec::new();
    let mut paths = Vec::new();
    for node in coll {
        match node {
            NodeStructure::Group(GroupStructure { id, path }) => {
                types.push(NodeType::Group);
                ids.push(id);
                paths.push(path.to_string_lossy().into_owned());
            }
            _ => {
                todo!()
            }
        }
    }
    let ids = mk_id_array(ids);
    let types = mk_type_array(types);
    let paths = mk_path_array(paths);
    let columns: Vec<Arc<dyn Array>> = vec![Arc::new(ids), Arc::new(types), Arc::new(paths)];
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", arrow::datatypes::DataType::UInt32, false),
        Field::new("type", arrow::datatypes::DataType::Utf8, false),
        Field::new("path", arrow::datatypes::DataType::Utf8, false),
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
        let groups = vec![
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
        ];
        let st = mk_structure_table(groups);
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
    }
}
