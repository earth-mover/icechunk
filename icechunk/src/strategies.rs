#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
use std::collections::HashMap;
use std::num::{NonZeroU16, NonZeroU64};
use std::ops::{Bound, Range};
use std::path::PathBuf;

use prop::string::string_regex;
use proptest::prelude::*;
use proptest::{collection::vec, option, strategy::Strategy};

use crate::config::{
    CachingConfig, CompressionAlgorithm, CompressionConfig, ManifestConfig,
    ManifestPreloadCondition, ManifestPreloadConfig, ManifestSplitCondition,
    ManifestSplitDim, ManifestSplitDimCondition, ManifestSplittingConfig, S3Options,
};
use crate::format::manifest::ManifestExtents;
use crate::format::snapshot::{ArrayShape, DimensionName};
use crate::format::{ChunkIndices, Path};
use crate::session::Session;
use crate::storage::{
    ConcurrencySettings, RetriesSettings, Settings, new_in_memory_storage,
};
use crate::virtual_chunks::VirtualChunkContainer;
use crate::{ObjectStoreConfig, Repository, RepositoryConfig};

const MAX_NDIM: usize = 4;

pub fn node_paths() -> impl Strategy<Value = Path> {
    // FIXME: Add valid paths
    vec(string_regex("[a-zA-Z0-9]*").expect("invalid regex"), 0..10).prop_map(|v| {
        format!("/{}", v.join("/")).try_into().expect("invalid Path string")
    })
}

prop_compose! {
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
                            NonZeroU64::new(chunk_size)
                                .expect("logic bug no zeros allowed")
                        })
                        .boxed()
                })
                .collect();
            (Just(shape), chunk_shape, option::of(vec(option::of(any::<String>()), ndim)))
        })
        .prop_map(|(shape, chunk_shape, dimension_names)| ShapeDim {
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

prop_compose! {
    pub fn chunk_indices(dim: usize, values_in: Range<u32>)(v in proptest::collection::vec(values_in, dim..(dim+1))) -> ChunkIndices {
        ChunkIndices(v)
    }
}

prop_compose! {
    pub fn s3_options()
    (region in option::of(string_regex("[a-zA-Z0-9\\-_]*").unwrap()),
     endpoint_url in option::of(string_regex("[a-zA-Z0-9\\-_/]*").unwrap().prop_map(|s| format!("https://{s}"))),
     network_stream_timeout_seconds in option::of(0..120u32)
    ) -> S3Options {
        S3Options{
            region,
            endpoint_url,
            anonymous: false,
            allow_http: false,
            force_path_style: false,
            network_stream_timeout_seconds
        }
    }
}

prop_compose! {
    pub fn compression_config()
    (level in option::of(1..5u8), algorithm in option::of(Just(CompressionAlgorithm::Zstd))) -> CompressionConfig {
        CompressionConfig{ algorithm, level }
    }
}

prop_compose! {
    pub fn caching_config()
    (num_snapshot_nodes in option::of(0..10_000_000u64),
     num_chunk_refs in option::of(0..10_000_000u64),
     num_transaction_changes in option::of(0..10_000u64),
     num_bytes_attributes in option::of(0..10_000u64),
     num_bytes_chunks in option::of(0..10_000_000_000u64),
) -> CachingConfig {
        CachingConfig{
            num_snapshot_nodes,
            num_chunk_refs,
            num_transaction_changes,
            num_bytes_attributes,
            num_bytes_chunks,
        }
    }
}

prop_compose! {
    pub fn virtual_chunk_container()
    (store in object_store_config()
                .prop_filter(
                    "virtual chunk containers can not point to in-memory stores",
                    |store| !matches!(store, ObjectStoreConfig::InMemory)
                )
    ) -> VirtualChunkContainer  {
        use ObjectStoreConfig::*;
        match &store {
            InMemory => panic!("assumed not to be in memory"),
            LocalFileSystem(path_buf) => {
                VirtualChunkContainer::new(format!("file:///{}/", path_buf.to_string_lossy()),store).unwrap()
            }
            Http(_) => VirtualChunkContainer::new("http://example.com/".to_string(),store).unwrap(),
            S3Compatible(_) => VirtualChunkContainer::new("s3://somebucket/".to_string(),store).unwrap(),
            S3(_) => VirtualChunkContainer::new("s3://somebucket/".to_string(),store).unwrap(),
            Gcs(_) => VirtualChunkContainer::new("gcs://somebucket/".to_string(),store).unwrap(),
            Azure(_) => VirtualChunkContainer::new("az://somebucket/".to_string(),store).unwrap(),
            Tigris(_) => VirtualChunkContainer::new("tigris://somebucket/".to_string(),store).unwrap(),
        }
    }
}

pub fn object_store_config() -> BoxedStrategy<ObjectStoreConfig> {
    use ObjectStoreConfig::*;
    prop_oneof![
        Just(InMemory),
        proptest::collection::vec(string_regex("[a-zA-Z0-9\\-_]+").unwrap(), 1..4)
            .prop_map(|s| LocalFileSystem(PathBuf::from(s.join("/")))),
        s3_options().prop_map(S3),
        s3_options().prop_map(S3Compatible),
        s3_options().prop_map(Tigris),
        any::<HashMap<String, String>>().prop_map(Gcs),
        any::<HashMap<String, String>>().prop_map(Http),
        any::<HashMap<String, String>>().prop_map(Azure),
    ]
    .boxed()
}

pub fn bound<T>(inner: impl Strategy<Value = T>) -> impl Strategy<Value = Bound<T>>
where
    T: std::fmt::Debug + Clone,
{
    inner.prop_flat_map(|t| {
        prop_oneof![
            Just(Bound::Included(t.clone())),
            Just(Bound::Excluded(t.clone())),
            Just(Bound::Unbounded)
        ]
    })
}

pub fn manifest_preload_condition() -> BoxedStrategy<ManifestPreloadCondition> {
    use ManifestPreloadCondition::*;
    let leaf = prop_oneof![
        Just(True),
        Just(False),
        ".*".prop_map(|regex| PathMatches { regex }),
        ".*".prop_map(|regex| NameMatches { regex }),
        bound(any::<u32>()).prop_map(|from| NumRefs { from, to: Bound::Unbounded }),
    ];
    leaf.prop_recursive(4, 20, 5, |inner| {
        prop_oneof![
            proptest::collection::vec(inner.clone(), 1..4).prop_map(Or),
            proptest::collection::vec(inner.clone(), 1..4).prop_map(And),
        ]
    })
    .boxed()
}

pub fn manifest_split_condition() -> BoxedStrategy<ManifestSplitCondition> {
    use ManifestSplitCondition::*;
    let leaf = prop_oneof![
        Just(AnyArray),
        ".*".prop_map(|regex| PathMatches { regex }),
        ".*".prop_map(|regex| NameMatches { regex }),
    ];
    leaf.prop_recursive(4, 20, 5, |inner| {
        prop_oneof![
            proptest::collection::vec(inner.clone(), 1..4).prop_map(Or),
            proptest::collection::vec(inner.clone(), 1..4).prop_map(And),
        ]
    })
    .boxed()
}

prop_compose! {
    pub fn manifest_preload_config()
        (max_total_refs in option::of(any::<u32>()),
        preload_if in option::of(manifest_preload_condition())
    ) -> ManifestPreloadConfig {
        ManifestPreloadConfig { max_total_refs, preload_if }
    }
}

pub fn manifest_split_dim_condition() -> BoxedStrategy<ManifestSplitDimCondition> {
    use ManifestSplitDimCondition::*;
    prop_oneof![Just(Any), any::<usize>().prop_map(Axis), ".*".prop_map(DimensionName)]
        .boxed()
}

prop_compose! {
    pub fn manifest_split_dim()
        (condition in manifest_split_dim_condition(),
        num_chunks in any::<u32>(),
    ) -> ManifestSplitDim {
        ManifestSplitDim { condition, num_chunks }
    }
}

prop_compose! {
    pub fn split_sizes()
        (condition in manifest_split_condition(), dims in proptest::collection::vec(manifest_split_dim(), 1..5))
    -> (ManifestSplitCondition, Vec<ManifestSplitDim>) {
    (condition, dims)
    }
}

prop_compose! {
    pub fn manifest_splitting_config()
        (sizes in option::of(proptest::collection::vec(split_sizes(), 1..5)))
    -> ManifestSplittingConfig {
        ManifestSplittingConfig{split_sizes: sizes}
    }
}

prop_compose! {
    pub fn manifest_config()
        (splitting in option::of(manifest_splitting_config()), preload in option::of(manifest_preload_config()))
    -> ManifestConfig {
        ManifestConfig{preload, splitting}
    }
}

prop_compose! {
    pub fn virtual_chunk_containers()
        (containers in proptest::collection::vec(virtual_chunk_container(), 0..10))
    -> HashMap<String, VirtualChunkContainer> {
        containers.into_iter().map(|cont| (cont.url_prefix().to_string(), cont)).collect()
    }
}

prop_compose! {
    pub fn concurrency_settings()
        (max_concurrent_requests_for_object in option::of(any::<NonZeroU16>()),
        ideal_concurrent_request_size in option::of(any::<NonZeroU64>())
    ) -> ConcurrencySettings  {
        ConcurrencySettings  {max_concurrent_requests_for_object, ideal_concurrent_request_size}
    }
}

prop_compose! {
    pub fn retries_settings()
        (max_tries in option::of(any::<NonZeroU16>()),
        initial_backoff_ms in option::of(any::<u32>()),
        max_backoff_ms in option::of(any::<u32>()),
    ) -> RetriesSettings  {
        RetriesSettings {initial_backoff_ms,max_backoff_ms, max_tries }
    }
}

prop_compose! {
    pub fn storage_settings()
        (
        concurrency in option::of(concurrency_settings()),
        retries in option::of(retries_settings()),
        unsafe_use_conditional_update in option::of(any::<bool>()),
        unsafe_use_conditional_create in option::of(any::<bool>()),
        unsafe_use_metadata in option::of(any::<bool>()),
        storage_class in option::of(".*"),
        metadata_storage_class in option::of(".*"),
        chunks_storage_class in option::of(".*"),
        minimum_size_for_multipart_upload in option::of(any::<u64>()),
    ) -> Settings  {
        Settings {
            concurrency,
            retries,
            unsafe_use_conditional_update,
            unsafe_use_conditional_create,
            unsafe_use_metadata,
            storage_class,
            metadata_storage_class,
            chunks_storage_class,
            minimum_size_for_multipart_upload,
        }
    }
}

prop_compose! {
    pub fn repository_config()
        (inline_chunk_threshold_bytes in option::of(any::<u16>()),
        get_partial_values_concurrency in option::of(any::<u16>()),
        compression in option::of(compression_config()),
        max_concurrent_requests in option::of(any::<u16>()),
        caching in option::of(caching_config()),
        virtual_chunk_containers in option::of(virtual_chunk_containers()),
        manifest in option::of(manifest_config()),
        storage in option::of(storage_settings()),

        )
    -> RepositoryConfig {
        RepositoryConfig{
            inline_chunk_threshold_bytes,
            get_partial_values_concurrency,
            compression,
            max_concurrent_requests,
            caching,
            manifest,
            virtual_chunk_containers,
            storage,
        }
    }
}
