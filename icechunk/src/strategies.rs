use std::collections::HashMap;
use std::num::NonZeroU64;
use std::ops::Range;

use prop::string::string_regex;
use proptest::prelude::*;
use proptest::{collection::vec, option, strategy::Strategy};

use crate::Repository;
use crate::format::manifest::ManifestExtents;
use crate::format::snapshot::{ArrayShape, DimensionName};
use crate::format::{ChunkIndices, Path};
use crate::session::Session;
use crate::storage::new_in_memory_storage;

const MAX_NDIM: usize = 4;

pub fn node_paths() -> impl Strategy<Value = Path> {
    // FIXME: Add valid paths
    #[allow(clippy::expect_used)]
    vec(string_regex("[a-zA-Z0-9]*").expect("invalid regex"), 0..10).prop_map(|v| {
        format!("/{}", v.join("/")).try_into().expect("invalid Path string")
    })
}

prop_compose! {
    #[allow(clippy::expect_used)]
    pub fn empty_repositories()(_id in any::<u32>()) -> Repository {
    // _id is used as a hack to avoid using prop_oneof![Just(repository)]
    // Using Just requires Repository impl Clone, which we do not want

    // FIXME: add storages strategy
    let runtime = tokio::runtime::Runtime::new().expect("Failed to create tokio runtime");

    runtime.block_on(async {
        let storage = new_in_memory_storage().await.expect("Cannot create in memory storage");
        Repository::create(None, storage, HashMap::new())
            .await
            .expect("Failed to initialize repository")
    })
}
}

prop_compose! {
    #[allow(clippy::expect_used)]
    pub fn empty_writable_session()(_id in any::<u32>()) -> Session {
    // _id is used as a hack to avoid using prop_oneof![Just(repository)]
    // Using Just requires Repository impl Clone, which we do not want

    // FIXME: add storages strategy

    let runtime = tokio::runtime::Runtime::new().expect("Failed to create tokio runtime");

    runtime.block_on(async {
        let storage = new_in_memory_storage().await.expect("Cannot create in memory storage");
        let repository = Repository::create(None, storage, HashMap::new())
            .await
            .expect("Failed to initialize repository");
        repository.writable_session("main").await.expect("Failed to create session")
    })
}
}

#[derive(Debug)]
pub struct ShapeDim {
    pub shape: ArrayShape,
    pub dimension_names: Option<Vec<DimensionName>>,
}

pub fn shapes_and_dims(max_ndim: Option<usize>) -> impl Strategy<Value = ShapeDim> {
    // FIXME: ndim = 0
    let max_ndim = max_ndim.unwrap_or(MAX_NDIM);
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
            #[allow(clippy::expect_used)]
            shape: ArrayShape::new(
                shape.into_iter().zip(chunk_shape.iter().map(|n| n.get())),
            )
            .expect("Invalid array shape"),
            dimension_names: dimension_names.map(|ds| {
                ds.iter().map(|s| From::from(s.as_ref().map(|s| s.as_str()))).collect()
            }),
        })
}

pub fn manifest_extents(ndim: usize) -> impl Strategy<Value = ManifestExtents> {
    (vec(0u32..1000u32, ndim), vec(1u32..1000u32, ndim)).prop_map(|(start, delta)| {
        let stop = std::iter::zip(start.iter(), delta.iter())
            .map(|(s, d)| s + d)
            .collect::<Vec<_>>();
        ManifestExtents::new(start.as_slice(), stop.as_slice())
    })
}

//prop_compose! {
//    pub fn zarr_array_metadata()(
//        chunk_key_encoding: ChunkKeyEncoding,
//        fill_value: FillValue,
//        shape_and_dim in shapes_and_dims(None),
//        storage_transformers in storage_transformers(),
//        codecs in codecs(),
//    ) -> ZarrArrayMetadata {
//        ZarrArrayMetadata {
//            shape: shape_and_dim.shape,
//            data_type: fill_value.get_data_type(),
//            chunk_shape: shape_and_dim.chunk_shape,
//            chunk_key_encoding,
//            fill_value,
//            codecs,
//            storage_transformers,
//            dimension_names: shape_and_dim.dimension_names,
//        }
//    }
//}

prop_compose! {
    pub fn chunk_indices(dim: usize, values_in: Range<u32>)(v in proptest::collection::vec(values_in, dim..(dim+1))) -> ChunkIndices {
        ChunkIndices(v)
    }
}
