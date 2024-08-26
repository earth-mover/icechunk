use std::sync::Arc;

use arrow::{
    array::{
        Array, AsArray, BinaryArray, FixedSizeBinaryArray, GenericBinaryBuilder,
        RecordBatch, StringArray, UInt32Array, UInt64Array,
    },
    datatypes::{Field, Schema, UInt32Type, UInt64Type},
};
use bytes::Bytes;
use futures::{Stream, StreamExt};
use itertools::Itertools;

use crate::{
    ArrayIndices, ChunkInfo, ChunkPayload, ChunkRef, ObjectId, TableOffset, TableRegion,
    VirtualChunkRef,
};

#[derive(Debug, PartialEq)]
pub struct ManifestsTable {
    pub batch: RecordBatch,
}

impl ManifestsTable {
    pub fn get_chunk_info(
        &self,
        coords: &ArrayIndices,
        region: &TableRegion,
    ) -> Option<ChunkInfo> {
        // FIXME: make this fast, currently it's a linear search
        // FIXME: return error type
        let idx = self.get_chunk_info_index(coords, region)?;
        self.get_row(idx)
    }

    pub fn iter(
        // FIXME: I need an arc here to be able to implement node_chunk_iterator in dataset.rs
        self: Arc<Self>,
        from: Option<TableOffset>,
        to: Option<TableOffset>,
    ) -> impl Iterator<Item = ChunkInfo> {
        let from = from.unwrap_or(0);
        let to = to.unwrap_or(self.batch.num_rows() as u32);
        // FIXME: unwrap
        (from..to).map(move |idx| self.get_row(idx).unwrap())
    }

    fn get_row(&self, row: TableOffset) -> Option<ChunkInfo> {
        if row as usize >= self.batch.num_rows() {
            return None;
        }

        let id_col =
            self.batch.column_by_name("array_id")?.as_primitive_opt::<UInt32Type>()?;
        let coords_col = self.batch.column_by_name("coords")?.as_binary_opt::<i32>()?;
        let offset_col =
            self.batch.column_by_name("offset")?.as_primitive_opt::<UInt64Type>()?;
        let length_col =
            self.batch.column_by_name("length")?.as_primitive_opt::<UInt64Type>()?;
        let inline_col =
            self.batch.column_by_name("inline_data")?.as_binary_opt::<i32>()?;
        let chunk_id_col =
            self.batch.column_by_name("chunk_id")?.as_fixed_size_binary_opt()?;
        let virtual_path_col =
            self.batch.column_by_name("virtual_path")?.as_string_opt::<i32>()?;
        // FIXME: do something with extras
        let _extra_col = self.batch.column_by_name("extra")?.as_string_opt::<i32>()?;

        // These arrays cannot contain null values, we don't need to check using `is_null`
        let idx = row as usize;
        let id = id_col.value(idx);
        let coords =
            ArrayIndices::unchecked_try_from_slice(coords_col.value(idx)).ok()?;

        if inline_col.is_valid(idx) {
            // we have an inline chunk
            let data = Bytes::copy_from_slice(inline_col.value(idx));
            Some(ChunkInfo {
                node: id,
                coord: coords,
                payload: ChunkPayload::Inline(data),
            })
        } else {
            let offset = offset_col.value(idx);
            let length = length_col.value(idx);

            if virtual_path_col.is_valid(idx) {
                // we have a virtual chunk
                let location = virtual_path_col.value(idx).to_string();
                Some(ChunkInfo {
                    node: id,
                    coord: coords,
                    payload: ChunkPayload::Virtual(VirtualChunkRef {
                        location,
                        offset,
                        length,
                    }),
                })
            } else {
                // we have a materialized chunk
                let chunk_id = chunk_id_col.value(idx).try_into().ok()?;
                Some(ChunkInfo {
                    node: id,
                    coord: coords,
                    payload: ChunkPayload::Ref(ChunkRef { id: chunk_id, offset, length }),
                })
            }
        }
    }

    fn get_chunk_info_index(
        &self,
        coords: &ArrayIndices,
        TableRegion(from_row, to_row): &TableRegion,
    ) -> Option<TableOffset> {
        if *to_row as usize > self.batch.num_rows() || from_row > to_row {
            return None;
        }
        let arrray = self
            .batch
            .column_by_name("coords")?
            .as_binary_opt::<i32>()?
            .slice(*from_row as usize, (to_row - from_row) as usize);
        let binary_coord: Vec<u8> = coords.into();
        let binary_coord = binary_coord.as_slice();
        let position: TableOffset = arrray
            .iter()
            .position(|coord| coord == Some(binary_coord))?
            .try_into()
            .ok()?;
        Some(position + from_row)
    }
}

impl ArrayIndices {
    // FIXME: better error type
    pub fn try_from_slice(rank: usize, slice: &[u8]) -> Result<Self, String> {
        if slice.len() != rank * 8 {
            Err(format!("Invalid slice length {}, expecting {}", slice.len(), rank))
        } else {
            ArrayIndices::unchecked_try_from_slice(slice)
        }
    }

    pub fn unchecked_try_from_slice(slice: &[u8]) -> Result<Self, String> {
        let chunked = slice.iter().chunks(8);
        let res = chunked.into_iter().map(|chunk| {
            u64::from_be_bytes(
                chunk
                    .copied()
                    .collect::<Vec<_>>()
                    .as_slice()
                    .try_into()
                    .expect("Invalid slice size"),
            )
        });
        Ok(ArrayIndices(res.collect()))
    }
}

impl From<&ArrayIndices> for Vec<u8> {
    fn from(ArrayIndices(ref value): &ArrayIndices) -> Self {
        value.iter().flat_map(|c| c.to_be_bytes()).collect()
    }
}

pub async fn mk_manifests_table(chunks: impl Stream<Item = ChunkInfo>) -> ManifestsTable {
    let mut array_ids = Vec::new();
    let mut coords = Vec::new();
    let mut chunk_ids = Vec::new();
    let mut inline_data = Vec::new();
    let mut virtual_paths = Vec::new();
    let mut offsets = Vec::new();
    let mut lengths = Vec::new();
    let mut extras: Vec<Option<()>> = Vec::new();

    futures::pin_mut!(chunks);
    while let Some(chunk) = chunks.next().await {
        array_ids.push(chunk.node);
        coords.push(chunk.coord);
        // FIXME:
        extras.push(None);

        match chunk.payload {
            ChunkPayload::Inline(data) => {
                lengths.push(data.len() as u64);
                inline_data.push(Some(data));
                chunk_ids.push(None);
                virtual_paths.push(None);
                offsets.push(None);
            }
            ChunkPayload::Ref(ChunkRef { id, offset, length }) => {
                lengths.push(length);
                inline_data.push(None);
                chunk_ids.push(Some(id));
                virtual_paths.push(None);
                offsets.push(Some(offset));
            }
            ChunkPayload::Virtual(VirtualChunkRef { location, offset, length }) => {
                lengths.push(length);
                inline_data.push(None);
                chunk_ids.push(None);
                virtual_paths.push(Some(location));
                offsets.push(Some(offset));
            }
        }
    }

    let array_ids = mk_array_ids_array(array_ids);
    let coords = mk_coords_array(coords);
    let offsets = mk_offsets_array(offsets);
    let lengths = mk_lengths_array(lengths);
    let inline_data = mk_inline_data_array(inline_data);
    let chunk_ids = mk_chunk_ids_array(chunk_ids);
    let virtual_paths = mk_virtual_paths_array(virtual_paths);
    let extras = mk_extras_array(extras);

    let columns: Vec<Arc<dyn Array>> = vec![
        Arc::new(array_ids),
        Arc::new(coords),
        Arc::new(offsets),
        Arc::new(lengths),
        Arc::new(inline_data),
        Arc::new(chunk_ids),
        Arc::new(virtual_paths),
        Arc::new(extras),
    ];

    let schema = Arc::new(Schema::new(vec![
        Field::new("array_id", arrow::datatypes::DataType::UInt32, false),
        Field::new("coords", arrow::datatypes::DataType::Binary, false),
        Field::new("offset", arrow::datatypes::DataType::UInt64, true),
        Field::new("length", arrow::datatypes::DataType::UInt64, false),
        Field::new("inline_data", arrow::datatypes::DataType::Binary, true),
        Field::new(
            "chunk_id",
            arrow::datatypes::DataType::FixedSizeBinary(ObjectId::SIZE as i32),
            true,
        ),
        Field::new("virtual_path", arrow::datatypes::DataType::Utf8, true),
        Field::new("extra", arrow::datatypes::DataType::Utf8, true),
    ]));
    let batch =
        RecordBatch::try_new(schema, columns).expect("Error creating record batch");
    ManifestsTable { batch }
}

fn mk_offsets_array<T: IntoIterator<Item = Option<u64>>>(coll: T) -> UInt64Array {
    coll.into_iter().collect()
}

fn mk_virtual_paths_array<T: IntoIterator<Item = Option<String>>>(
    coll: T,
) -> StringArray {
    coll.into_iter().collect()
}

fn mk_array_ids_array<T: IntoIterator<Item = u32>>(coll: T) -> UInt32Array {
    coll.into_iter().collect()
}

fn mk_inline_data_array<T: IntoIterator<Item = Option<Bytes>>>(coll: T) -> BinaryArray {
    BinaryArray::from_iter(coll)
}

fn mk_lengths_array<T: IntoIterator<Item = u64>>(coll: T) -> UInt64Array {
    coll.into_iter().collect()
}

fn mk_extras_array<T: IntoIterator<Item = Option<()>>>(coll: T) -> StringArray {
    // FIXME: implement extras
    coll.into_iter().map(|_x| None as Option<String>).collect()
}

fn mk_coords_array<T: IntoIterator<Item = ArrayIndices>>(coll: T) -> BinaryArray {
    let mut builder = GenericBinaryBuilder::<i32>::new();
    for ref coords in coll {
        let vec: Vec<u8> = coords.into();
        builder.append_value(vec.as_slice());
    }
    builder.finish()
}

fn mk_chunk_ids_array<T: IntoIterator<Item = Option<ObjectId>>>(
    coll: T,
) -> FixedSizeBinaryArray {
    let iter = coll.into_iter().map(|oid| oid.map(|oid| oid.0));
    FixedSizeBinaryArray::try_from_sparse_iter_with_size(iter, ObjectId::SIZE as i32)
        .expect("Bad ObjectId size")
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;
    use proptest::prelude::*;

    proptest! {

        #[test]
        fn coordinate_encoding_roundtrip(v: Vec<u64>) {
            let arr = ArrayIndices(v);
            let as_vec: Vec<u8> = (&arr).into();
            let roundtrip = ArrayIndices::try_from_slice(arr.0.len(), as_vec.as_slice()).unwrap();
            assert_eq!(arr, roundtrip);
        }
    }

    #[tokio::test]
    async fn test_get_chunk_info() {
        let c1a = ChunkInfo {
            node: 1,
            coord: ArrayIndices(vec![0, 0, 0]),
            payload: ChunkPayload::Ref(ChunkRef {
                id: ObjectId::random(),
                offset: 0,
                length: 128,
            }),
        };
        let c2a = ChunkInfo {
            node: 1,
            coord: ArrayIndices(vec![0, 0, 1]),
            payload: ChunkPayload::Ref(ChunkRef {
                id: ObjectId::random(),
                offset: 42,
                length: 43,
            }),
        };
        let c3a = ChunkInfo {
            node: 1,
            coord: ArrayIndices(vec![1, 0, 1]),
            payload: ChunkPayload::Ref(ChunkRef {
                id: ObjectId::random(),
                offset: 9999,
                length: 1,
            }),
        };

        let c1b = ChunkInfo {
            node: 2,
            coord: ArrayIndices(vec![0, 0, 0]),
            payload: ChunkPayload::Inline("hello".into()),
        };

        let c1c = ChunkInfo {
            node: 2,
            coord: ArrayIndices(vec![0, 0, 0]),
            payload: ChunkPayload::Virtual(VirtualChunkRef {
                location: "s3://foo.bar".to_string(),
                offset: 99,
                length: 100,
            }),
        };

        let table = mk_manifests_table(futures::stream::iter(vec![
            c1a.clone(),
            c2a.clone(),
            c3a.clone(),
            c1b.clone(),
            c1c.clone(),
        ]))
        .await;
        let res = table.get_chunk_info(&ArrayIndices(vec![0, 0, 0]), &TableRegion(1, 3));
        assert_eq!(res.as_ref(), None);
        let res = table.get_chunk_info(&ArrayIndices(vec![0, 0, 0]), &TableRegion(0, 3));
        assert_eq!(res.as_ref(), Some(&c1a));
        let res = table.get_chunk_info(&ArrayIndices(vec![0, 0, 0]), &TableRegion(0, 1));
        assert_eq!(res.as_ref(), Some(&c1a));

        let res = table.get_chunk_info(&ArrayIndices(vec![0, 0, 1]), &TableRegion(2, 3));
        assert_eq!(res.as_ref(), None);
        let res = table.get_chunk_info(&ArrayIndices(vec![0, 0, 1]), &TableRegion(0, 3));
        assert_eq!(res.as_ref(), Some(&c2a));
        let res = table.get_chunk_info(&ArrayIndices(vec![0, 0, 1]), &TableRegion(0, 2));
        assert_eq!(res.as_ref(), Some(&c2a));
        let res = table.get_chunk_info(&ArrayIndices(vec![0, 0, 1]), &TableRegion(1, 3));
        assert_eq!(res.as_ref(), Some(&c2a));

        let res = table.get_chunk_info(&ArrayIndices(vec![1, 0, 1]), &TableRegion(4, 3));
        assert_eq!(res.as_ref(), None);
        let res = table.get_chunk_info(&ArrayIndices(vec![1, 0, 1]), &TableRegion(0, 3));
        assert_eq!(res.as_ref(), Some(&c3a));
        let res = table.get_chunk_info(&ArrayIndices(vec![1, 0, 1]), &TableRegion(1, 3));
        assert_eq!(res.as_ref(), Some(&c3a));
        let res = table.get_chunk_info(&ArrayIndices(vec![1, 0, 1]), &TableRegion(2, 3));
        assert_eq!(res.as_ref(), Some(&c3a));

        let res = table.get_chunk_info(&ArrayIndices(vec![0, 0, 0]), &TableRegion(3, 4));
        assert_eq!(res.as_ref(), Some(&c1b));

        let res = table.get_chunk_info(&ArrayIndices(vec![0, 0, 0]), &TableRegion(4, 5));
        assert_eq!(res.as_ref(), Some(&c1c));
    }
}
