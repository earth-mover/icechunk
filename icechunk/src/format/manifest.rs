use std::{collections::HashMap, sync::Arc};

use arrow::{
    array::{
        Array, BinaryArray, FixedSizeBinaryArray, GenericBinaryBuilder, RecordBatch,
        StringArray, UInt32Array, UInt64Array,
    },
    datatypes::{Field, Schema},
    error::ArrowError,
};
use bytes::Bytes;
use futures::{Stream, StreamExt};
use itertools::Itertools;
use rocksdb::OptimisticTransactionDB;
use serde::{Deserialize, Serialize};
use serde_with::{base64::Base64, serde_as};

use super::{
    arrow::get_column, BatchLike, ChunkIndices, Flags, IcechunkFormatError,
    IcechunkResult, NodeId, ObjectId, TableOffset, TableRegion,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManifestExtents(pub Vec<ChunkIndices>);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManifestRef {
    pub object_id: ObjectId,
    pub location: TableRegion,
    pub flags: Flags,
    pub extents: ManifestExtents,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct VirtualChunkRef {
    location: String, // FIXME: better type
    offset: u64,
    length: u64,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ChunkRef {
    pub id: ObjectId,
    pub offset: u64,
    pub length: u64,
}

#[serde_as]
#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum ChunkPayload {
    // FIXME: use something more optimal
    Inline(#[serde_as(as = "Base64")] Bytes),
    Virtual(VirtualChunkRef),
    Ref(ChunkRef),
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ChunkInfo {
    pub node: NodeId,
    pub coord: ChunkIndices,
    pub payload: ChunkPayload,
}

#[derive(Debug, PartialEq)]
pub struct ManifestsTable {
    pub batch: RecordBatch,
}

impl BatchLike for ManifestsTable {
    fn get_batch(&self) -> &RecordBatch {
        &self.batch
    }
}

#[derive(Clone, Debug, Serialize)]
struct DBKey<'a> {
    #[serde(rename = "n")]
    node: &'a NodeId,
    #[serde(rename = "c")]
    coord: &'a ChunkIndices,
}

fn db_key(node_id: &NodeId, coord: &ChunkIndices) -> Vec<u8> {
    let key = DBKey { node: node_id, coord };
    serde_json::to_vec(&key).unwrap()
}

pub fn write_chunk_info(
    db: &OptimisticTransactionDB,
    node_id: &NodeId,
    chunk: &ChunkInfo,
) {
    let key = db_key(node_id, &chunk.coord);
    // FIXME: we don't need all the data
    let value = serde_json::to_vec(&chunk).unwrap();
    db.put(key.as_slice(), value.as_slice()).unwrap();
}

pub fn get_chunk_info(
    db: &OptimisticTransactionDB,
    node_id: &NodeId,
    coord: &ChunkIndices,
) -> IcechunkResult<ChunkInfo> {
    let key = db_key(node_id, coord);
    let bytes = db
        .get(key.as_slice())
        .unwrap()
        .ok_or(IcechunkFormatError::ChunkCoordinatesNotFound { coords: coord.clone() })?;
    serde_json::from_slice(bytes.as_slice()).map_err(|_| {
        IcechunkFormatError::InvalidArrayManifest {
            index: 0,
            field: "all".to_string(),
            message: "parsing error".to_string(),
        }
    })
}

impl ManifestsTable {
    pub fn get_chunk_info(
        &self,
        coords: &ChunkIndices,
        region: &TableRegion,
    ) -> IcechunkResult<ChunkInfo> {
        // FIXME: make this fast, currently it's a linear search
        let idx = self.get_chunk_info_index(coords, region)?;
        self.get_row(idx)
    }

    pub fn iter(
        // FIXME: I need an arc here to be able to implement node_chunk_iterator in dataset.rs
        self: Arc<Self>,
        from: Option<TableOffset>,
        to: Option<TableOffset>,
    ) -> impl Iterator<Item = IcechunkResult<ChunkInfo>> {
        let from = from.unwrap_or(0);
        let to = to.unwrap_or(self.batch.num_rows() as u32);
        (from..to).map(move |idx| self.get_row(idx))
    }

    fn get_row(&self, row: TableOffset) -> IcechunkResult<ChunkInfo> {
        if row as usize >= self.batch.num_rows() {
            return Err(IcechunkFormatError::InvalidManifestIndex {
                index: row as usize,
                max_index: self.batch.num_rows() - 1,
            });
        }

        let idx = row as usize;
        // TODO: do something with extras
        let _extra_col = get_column(self, "extra")?.string_at_opt(idx)?;

        // These arrays cannot contain null values, we don't need to check using `is_null`
        let id = get_column(self, "array_id")?.u32_at(idx)?;
        let coords = get_column(self, "coords")?.binary_at(idx)?;
        // FIXME: once we define    how we want to encode coordinates we'll see if we need to
        // persist the rank of the array somewhere, for now, we fake it and we don't check
        let coords =
            ChunkIndices::try_from_slice(999_999_999, coords).map_err(|msg| {
                IcechunkFormatError::InvalidArrayManifest {
                    index: idx,
                    field: "coords".to_string(),
                    message: msg.to_string(),
                }
            })?;

        let inline_data = get_column(self, "inline_data")?.binary_at_opt(idx)?;
        if let Some(inline_data) = inline_data {
            // we have an inline chunk
            let data = Bytes::copy_from_slice(inline_data);
            Ok(ChunkInfo { node: id, coord: coords, payload: ChunkPayload::Inline(data) })
        } else {
            let offset = get_column(self, "offset")?.u64_at(idx)?;
            let length = get_column(self, "length")?.u64_at(idx)?;

            let virtual_path = get_column(self, "virtual_path")?.string_at_opt(idx)?;
            if let Some(virtual_path) = virtual_path {
                // we have a virtual chunk
                let location = virtual_path.to_string();
                Ok(ChunkInfo {
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
                let chunk_id = get_column(self, "chunk_id")?
                    .fixed_size_binary_at(idx)?
                    .try_into()
                    .map_err(|_| IcechunkFormatError::InvalidArrayManifest {
                        index: idx,
                        field: "chunk_id".to_string(),
                        message: "invalid object id".to_string(),
                    })?;
                Ok(ChunkInfo {
                    node: id,
                    coord: coords,
                    payload: ChunkPayload::Ref(ChunkRef { id: chunk_id, offset, length }),
                })
            }
        }
    }

    fn get_chunk_info_index(
        &self,
        coords: &ChunkIndices,
        TableRegion(from_row, to_row): &TableRegion,
    ) -> IcechunkResult<TableOffset> {
        if *to_row as usize > self.batch.num_rows() || from_row > to_row {
            return Err(IcechunkFormatError::InvalidManifestIndex {
                index: *to_row as usize,
                max_index: self.batch.num_rows() - 1,
            });
        }

        let arrray = get_column(self, "coords")?
            .as_binary()?
            .slice(*from_row as usize, (to_row - from_row) as usize);
        let binary_coord: Vec<u8> = coords.into();
        let binary_coord = binary_coord.as_slice();
        let position: usize =
            arrray.iter().position(|coord| coord == Some(binary_coord)).ok_or(
                IcechunkFormatError::ChunkCoordinatesNotFound { coords: coords.clone() },
            )?;
        let position_u32: u32 = position.try_into().map_err(|_| {
            IcechunkFormatError::InvalidArrayManifest {
                index: position,
                field: "coords".to_string(),
                message: "index out of range".to_string(),
            }
        })?;
        Ok(position_u32 + from_row)
    }
}

impl ChunkIndices {
    pub fn try_from_slice(_rank: usize, slice: &[u8]) -> Result<Self, &'static str> {
        let chunked = slice.chunks(8);
        let res: Vec<_> = chunked
            .map(TryInto::<&[u8; 8]>::try_into)
            .try_collect()
            .map_err(|_| "error parsing chunk indices, invalid slice length")?;
        // FIXME: reenable this check, if needed, once we have the definitive coords encoding
        //if res.len() != rank {
        //    return Err("error parsing chunk indices, invalid slice length");
        //}
        Ok(ChunkIndices(
            res.into_iter().map(|slice| u64::from_be_bytes(*slice)).collect(),
        ))
    }
}

impl From<&ChunkIndices> for Vec<u8> {
    fn from(ChunkIndices(ref value): &ChunkIndices) -> Self {
        value.iter().flat_map(|c| c.to_be_bytes()).collect()
    }
}

// This is pretty awful, but Rust Try infrastructure is experimental
pub type ManifestTableResult<E> = Result<ManifestsTable, Result<E, ArrowError>>;

pub async fn mk_manifests_table<E>(
    chunks: impl Stream<Item = Result<ChunkInfo, E>>,
) -> ManifestTableResult<E> {
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
        let chunk = match chunk {
            Ok(chunk) => chunk,
            Err(err) => return Err(Ok(err)),
        };
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
    let batch = RecordBatch::try_new(schema, columns).map_err(Err)?;
    ManifestTableResult::Ok(ManifestsTable { batch })
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

fn mk_coords_array<T: IntoIterator<Item = ChunkIndices>>(coll: T) -> BinaryArray {
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
    #[allow(clippy::expect_used)] // we are feeding objects of known and fixed size
    FixedSizeBinaryArray::try_from_sparse_iter_with_size(iter, ObjectId::SIZE as i32)
        .expect("Bad ObjectId size")
}

#[cfg(test)]
#[allow(clippy::panic, clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::convert::Infallible;

    use super::*;
    use pretty_assertions::assert_eq;
    use proptest::prelude::*;

    proptest! {

        #[test]
        fn coordinate_encoding_roundtrip(v: Vec<u64>) {
            let arr = ChunkIndices(v);
            let as_vec: Vec<u8> = (&arr).into();
            let roundtrip = ChunkIndices::try_from_slice(arr.0.len(), as_vec.as_slice()).unwrap();
            assert_eq!(arr, roundtrip);
        }
    }

    #[tokio::test]
    async fn test_get_chunk_info() -> Result<(), Box<dyn std::error::Error>> {
        let c1a = ChunkInfo {
            node: 1,
            coord: ChunkIndices(vec![0, 0, 0]),
            payload: ChunkPayload::Ref(ChunkRef {
                id: ObjectId::random(),
                offset: 0,
                length: 128,
            }),
        };
        let c2a = ChunkInfo {
            node: 1,
            coord: ChunkIndices(vec![0, 0, 1]),
            payload: ChunkPayload::Ref(ChunkRef {
                id: ObjectId::random(),
                offset: 42,
                length: 43,
            }),
        };
        let c3a = ChunkInfo {
            node: 1,
            coord: ChunkIndices(vec![1, 0, 1]),
            payload: ChunkPayload::Ref(ChunkRef {
                id: ObjectId::random(),
                offset: 9999,
                length: 1,
            }),
        };

        let c1b = ChunkInfo {
            node: 2,
            coord: ChunkIndices(vec![0, 0, 0]),
            payload: ChunkPayload::Inline("hello".into()),
        };

        let c1c = ChunkInfo {
            node: 2,
            coord: ChunkIndices(vec![0, 0, 0]),
            payload: ChunkPayload::Virtual(VirtualChunkRef {
                location: "s3://foo.bar".to_string(),
                offset: 99,
                length: 100,
            }),
        };

        let table = mk_manifests_table::<Infallible>(
            futures::stream::iter(vec![
                c1a.clone(),
                c2a.clone(),
                c3a.clone(),
                c1b.clone(),
                c1c.clone(),
            ])
            .map(Ok),
        )
        .await
        .map_err(|e| format!("error creating manifest {e:?}"))?;

        let res = table.get_chunk_info(&ChunkIndices(vec![0, 0, 0]), &TableRegion(1, 3));
        assert_eq!(
            res.as_ref(),
            Err(&IcechunkFormatError::ChunkCoordinatesNotFound {
                coords: ChunkIndices(vec![0, 0, 0])
            })
        );
        let res = table.get_chunk_info(&ChunkIndices(vec![0, 0, 0]), &TableRegion(0, 3));
        assert_eq!(res.as_ref(), Ok(&c1a));
        let res = table.get_chunk_info(&ChunkIndices(vec![0, 0, 0]), &TableRegion(0, 1));
        assert_eq!(res.as_ref(), Ok(&c1a));

        let res = table.get_chunk_info(&ChunkIndices(vec![0, 0, 1]), &TableRegion(2, 3));
        assert_eq!(
            res.as_ref(),
            Err(&IcechunkFormatError::ChunkCoordinatesNotFound {
                coords: ChunkIndices(vec![0, 0, 1])
            })
        );
        let res = table.get_chunk_info(&ChunkIndices(vec![0, 0, 1]), &TableRegion(0, 3));
        assert_eq!(res.as_ref(), Ok(&c2a));
        let res = table.get_chunk_info(&ChunkIndices(vec![0, 0, 1]), &TableRegion(0, 2));
        assert_eq!(res.as_ref(), Ok(&c2a));
        let res = table.get_chunk_info(&ChunkIndices(vec![0, 0, 1]), &TableRegion(1, 3));
        assert_eq!(res.as_ref(), Ok(&c2a));

        let res = table.get_chunk_info(&ChunkIndices(vec![1, 0, 1]), &TableRegion(0, 3));
        assert_eq!(res.as_ref(), Ok(&c3a));
        let res = table.get_chunk_info(&ChunkIndices(vec![1, 0, 1]), &TableRegion(1, 3));
        assert_eq!(res.as_ref(), Ok(&c3a));
        let res = table.get_chunk_info(&ChunkIndices(vec![1, 0, 1]), &TableRegion(2, 3));
        assert_eq!(res.as_ref(), Ok(&c3a));

        let res = table.get_chunk_info(&ChunkIndices(vec![0, 0, 0]), &TableRegion(3, 4));
        assert_eq!(res.as_ref(), Ok(&c1b));

        let res = table.get_chunk_info(&ChunkIndices(vec![0, 0, 0]), &TableRegion(4, 5));
        assert_eq!(res.as_ref(), Ok(&c1c));
        Ok(())
    }
}
