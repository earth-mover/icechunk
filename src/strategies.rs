use std::num::NonZeroU64;
use std::ops::RangeInclusive;
use std::path::PathBuf;
use std::sync::Arc;

use itertools::zip_eq;
use proptest::prelude::*;
use proptest::{collection::vec, option, strategy::Strategy};

use crate::dataset::{
    ArrayShape, ChunkIndices, ChunkKeyEncoding, ChunkPayload, ChunkShape, Codec, Dataset,
    DimensionNames, FillValue, Path, StorageTransformer, ZarrArrayMetadata,
};
use crate::format::manifest::{ChunkRef, VirtualChunkRef};
use crate::storage::InMemoryStorage;

pub fn node_paths() -> impl Strategy<Value = Path> {
    // FIXME: Add valid paths
    any::<PathBuf>()
}

pub fn empty_datasets() -> impl Strategy<Value = Dataset> {
    // FIXME: add storages strategy
    let storage = InMemoryStorage::new();
    let dataset = Dataset::create(Arc::new(storage)).build();
    prop_oneof![Just(dataset)]
}

pub fn codecs() -> impl Strategy<Value = Vec<Codec>> {
    prop_oneof![Just(vec![Codec { name: "mycodec".to_string(), configuration: None }]),]
}

pub fn storage_transformers() -> impl Strategy<Value = Option<Vec<StorageTransformer>>> {
    prop_oneof![
        Just(Some(vec![StorageTransformer {
            name: "mytransformer".to_string(),
            configuration: None,
        }])),
        Just(None),
    ]
}

#[derive(Debug)]
pub struct ShapeDim {
    shape: ArrayShape,
    chunk_shape: ChunkShape,
    dimension_names: Option<DimensionNames>,
}

pub fn shapes_and_dims(max_ndim: Option<usize>) -> impl Strategy<Value = ShapeDim> {
    // FIXME: ndim = 0
    let max_ndim = max_ndim.unwrap_or(4usize);
    vec(1u64..=25u64, 1..max_ndim)
        .prop_flat_map(|shape| {
            let ndim = shape.len();
            let chunk_shape: Vec<BoxedStrategy<NonZeroU64>> = shape
                .clone()
                .into_iter()
                .map(|size| {
                    (1u64..=size)
                        .prop_map(|chunk_size| NonZeroU64::new(chunk_size).unwrap())
                        .boxed()
                })
                .collect();
            (Just(shape), chunk_shape, option::of(vec(option::of(any::<String>()), ndim)))
        })
        .prop_map(|(shape, chunk_shape, dimension_names)| ShapeDim {
            shape,
            chunk_shape: ChunkShape(chunk_shape),
            dimension_names,
        })
}

prop_compose! {
    pub fn zarr_array_metadata()(
        chunk_key_encoding: ChunkKeyEncoding,
        fill_value: FillValue,
        shape_and_dim in shapes_and_dims(None),
        storage_transformers in storage_transformers(),
        codecs in codecs(),
    ) -> ZarrArrayMetadata {
        ZarrArrayMetadata {
            shape: shape_and_dim.shape,
            data_type: fill_value.get_data_type(),
            chunk_shape: shape_and_dim.chunk_shape,
            chunk_key_encoding,
            fill_value,
            codecs,
            storage_transformers,
            dimension_names: shape_and_dim.dimension_names,
        }
    }
}

fn indices_from_metadata(
    metadata: ZarrArrayMetadata,
) -> impl Strategy<Value = ChunkIndices> {
    let shape = metadata.shape;
    let ChunkShape(chunk_shape) = metadata.chunk_shape;

    zip_eq(shape.iter(), chunk_shape.iter())
        .map(|(size, chunksize)| 1..=(*size) / (*chunksize))
        .collect::<Vec<RangeInclusive<u64>>>()
        .prop_map(ChunkIndices)
}

prop_compose! {
    pub fn metadata_and_indices()(metadata in zarr_array_metadata())
        (
            metadata in Just(metadata.clone()),
            indices in indices_from_metadata(metadata.clone())
        )
        -> (ZarrArrayMetadata, ChunkIndices) {
            (metadata, indices)
    }
}

pub fn chunk_payloads() -> impl Strategy<Value = ChunkPayload> {
    prop_oneof![
        proptest::string::bytes_regex("")
            .unwrap()
            .prop_map(|x| ChunkPayload::Inline(x.into())),
        any::<VirtualChunkRef>().prop_map(ChunkPayload::Virtual),
        any::<ChunkRef>().prop_map(ChunkPayload::Ref),
    ]
}
