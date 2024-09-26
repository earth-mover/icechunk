use std::num::NonZeroU64;
use std::path::PathBuf;
use std::sync::Arc;

use proptest::prelude::*;
use proptest::{collection::vec, option, strategy::Strategy};

use crate::format::snapshot::ZarrArrayMetadata;
use crate::format::Path;
use crate::metadata::{ArrayShape, DimensionNames};
use crate::repository::{
    ChunkKeyEncoding, ChunkShape, Codec, FillValue, StorageTransformer,
};
use crate::{ObjectStorage, Repository};

pub fn node_paths() -> impl Strategy<Value = Path> {
    // FIXME: Add valid paths
    any::<PathBuf>()
}

prop_compose! {
    #[allow(clippy::expect_used)]
    pub fn empty_repositories()(_id in any::<u32>()) -> Repository {
    // _id is used as a hack to avoid using prop_oneof![Just(repository)]
    // Using Just requires Repository impl Clone, which we do not want

    // FIXME: add storages strategy
    let storage = ObjectStorage::new_in_memory_store(None);
    let runtime = tokio::runtime::Runtime::new().expect("Failed to create tokio runtime");

    runtime.block_on(async {
        Repository::init(Arc::new(storage), false)
            .await
            .expect("Failed to initialize repository")
            .build()
    })
}
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
    vec(1u64..26u64, 1..max_ndim)
        .prop_flat_map(|shape| {
            let ndim = shape.len();
            let chunk_shape: Vec<BoxedStrategy<NonZeroU64>> = shape
                .clone()
                .into_iter()
                .map(|size| {
                    (1u64..=size)
                        .prop_map(|chunk_size| {
                            #[allow(clippy::expect_used)] // no zeroes in the range above
                            NonZeroU64::new(chunk_size)
                                .expect("logic bug no zeros allowed")
                        })
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
