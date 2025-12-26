#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
use crate::config::{
    AzureCredentials, AzureStaticCredentials, CachingConfig, CompressionAlgorithm,
    CompressionConfig, GcsBearerCredential, GcsStaticCredentials, ManifestConfig,
    ManifestPreloadCondition, ManifestPreloadConfig, ManifestSplitCondition,
    ManifestSplitDim, ManifestSplitDimCondition, ManifestSplittingConfig, S3Options,
    S3StaticCredentials,
};
use crate::format::format_constants::SpecVersionBin;
use crate::format::manifest::{
    ChunkPayload, ChunkRef, ManifestExtents, SecondsSinceEpoch, VirtualChunkLocation,
    VirtualChunkRef,
};
use crate::format::snapshot::{ArrayShape, DimensionName};
use crate::format::{ChunkId, ChunkIndices, NodeId, Path, manifest};
use crate::session::Session;
use crate::storage::{
    ConcurrencySettings, ETag, RetriesSettings, Settings, new_in_memory_storage,
};
use crate::virtual_chunks::VirtualChunkContainer;
use crate::{ObjectStoreConfig, Repository, RepositoryConfig};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use prop::string::string_regex;
use proptest::collection::{btree_map, vec};
use proptest::prelude::*;
use proptest::{
    array::{uniform8, uniform12},
    option,
    strategy::Strategy,
};
use std::collections::{BTreeMap, HashMap};
use std::num::{NonZeroU16, NonZeroU64};
use std::ops::{Bound, Range};
use std::path::PathBuf;

use crate::change_set::{ArrayData, Move};

const MAX_NDIM: usize = 4;

pub fn node_paths() -> impl Strategy<Value = Path> {
    // FIXME: Add valid paths
    vec(string_regex("[a-zA-Z0-9]*").expect("invalid regex"), 0..10).prop_map(|v| {
        format!("/{}", v.join("/")).try_into().expect("invalid Path string")
    })
}

pub fn spec_version() -> BoxedStrategy<SpecVersionBin> {
    prop_oneof![Just(SpecVersionBin::V2dot0), Just(SpecVersionBin::V1dot0)].boxed()
}

prop_compose! {
    pub fn empty_repositories()(version in spec_version()) -> Repository {
        // FIXME: add storages strategy
        let runtime = tokio::runtime::Runtime::new().expect("Failed to create tokio runtime");

        runtime.block_on(async {
            let storage = new_in_memory_storage().await.expect("Cannot create in memory storage");
            Repository::create(None, storage, HashMap::new(), Some(version))
                .await
                .expect("Failed to initialize repository")
        })
    }
}

prop_compose! {
    pub fn empty_writable_session()(version in spec_version()) -> Session {
    // _id is used as a hack to avoid using prop_oneof![Just(repository)]
    // Using Just requires Repository impl Clone, which we do not want

    // FIXME: add storages strategy

    let runtime = tokio::runtime::Runtime::new().expect("Failed to create tokio runtime");

    runtime.block_on(async {
        let storage = new_in_memory_storage().await.expect("Cannot create in memory storage");
        let repository = Repository::create(None, storage, HashMap::new(), Some(version))
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

pub fn manifest_extents2(ndim: usize) -> impl Strategy<Value = ManifestExtents> {
    (vec(0u32..1000u32, ndim), vec(1u32..1000u32, ndim)).prop_map(|(start, delta)| {
        let stop = std::iter::zip(start.iter(), delta.iter())
            .map(|(s, d)| s + d)
            .collect::<Vec<_>>();
        ManifestExtents::new(start.as_slice(), stop.as_slice())
    })
}

pub fn manifest_extents(ndim: usize) -> impl Strategy<Value = ManifestExtents> {
    vec(
        any::<Range<u32>>()
            .prop_filter("Could not construct a nonempty range", |range| {
                !range.is_empty()
            }),
        ndim,
    )
    .prop_map(ManifestExtents::from_ranges_iter)
}

prop_compose! {
    pub fn chunk_indices(dim: usize, values_in: Range<u32>)(v in vec(values_in, dim..(dim+1))) -> ChunkIndices {
        ChunkIndices(v)
    }
}

fn transfer_protocol() -> BoxedStrategy<String> {
    prop_oneof!["https", "http"].boxed()
}

prop_compose! {
    pub fn url() (protocol in transfer_protocol(),
    remaining_url in "[a-zA-Z0-9\\-_/]*") -> String {
        format!("{protocol}://{remaining_url}")
    }
}

prop_compose! {
    pub fn s3_options()
    (region in option::of(string_regex("[a-zA-Z0-9\\-_]*").unwrap()),
     endpoint_url in option::of(url()),
       is_anonymous in any::<bool>(),
       should_path_style_be_forced in any::<bool>(),
       network_stream_timeout_seconds in option::of(0..120u32),
       requester_pays in any::<bool>(),
    ) ->S3Options {
        let cpy = endpoint_url.clone();
        S3Options{
            region,
            endpoint_url,
            anonymous: is_anonymous,
            allow_http: cpy.is_none_or(|link| !link.starts_with("https")),
            force_path_style: should_path_style_be_forced,
            network_stream_timeout_seconds,
            requester_pays,
        }
    }
}

prop_compose! {
    pub fn azure_options()
    (account in string_regex("[a-zA-Z0-9\\-_]+").unwrap(),
     mut config in any::<HashMap<String, String>>()
    ) -> HashMap<String, String> {
        config.insert("account".to_string(), account.clone());
        config
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
        vec(string_regex("[a-zA-Z0-9\\-_]+").unwrap(), 1..4)
            .prop_map(|s| LocalFileSystem(PathBuf::from(s.join("/")))),
        s3_options().prop_map(S3),
        s3_options().prop_map(S3Compatible),
        s3_options().prop_map(Tigris),
        any::<HashMap<String, String>>().prop_map(Gcs),
        any::<HashMap<String, String>>().prop_map(Http),
        azure_options().prop_map(Azure),
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
            vec(inner.clone(), 1..4).prop_map(Or),
            vec(inner.clone(), 1..4).prop_map(And),
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
        preload_if in option::of(manifest_preload_condition()),
            max_arrays_to_scan in option::of(any::<u32>())
    ) -> ManifestPreloadConfig {
        ManifestPreloadConfig { max_total_refs, preload_if, max_arrays_to_scan}
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
        (condition in manifest_split_condition(), dims in vec(manifest_split_dim(), 1..5))
    -> (ManifestSplitCondition, Vec<ManifestSplitDim>) {
    (condition, dims)
    }
}

prop_compose! {
    pub fn manifest_splitting_config()
        (sizes in option::of(vec(split_sizes(), 1..5)))
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
        (containers in vec(virtual_chunk_container(), 0..10))
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
        previous_file in option::of(any::<PathBuf>().prop_map(|path| path.to_string_lossy().to_string())),
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
            previous_file,
        }
    }
}

prop_compose! {
    pub fn expiration_date() (seconds in any::<i64>()) -> Option<DateTime<Utc>> {
        DateTime::from_timestamp_secs(seconds)
    }
}

prop_compose! {
    pub fn s3_static_credentials()
    (access_key_id in any::<String>(),
        secret_access_key in any::<String>(),
    expires_after in expiration_date(),
    session_token in option::of(any::<String>())) -> S3StaticCredentials {
        S3StaticCredentials{access_key_id, secret_access_key, session_token, expires_after}
    }
}

prop_compose! {
pub fn gcs_bearer_credential()
    (bearer in any::<String>(),expires_after in  expiration_date()) -> GcsBearerCredential {
        GcsBearerCredential{bearer,expires_after}
    }
}

pub fn gcs_static_credentials() -> BoxedStrategy<GcsStaticCredentials> {
    use GcsStaticCredentials::*;
    prop_oneof![
        any::<PathBuf>().prop_map(ServiceAccount),
        any::<String>().prop_map(ServiceAccountKey),
        any::<PathBuf>().prop_map(ApplicationCredentials),
        gcs_bearer_credential().prop_map(BearerToken)
    ]
    .boxed()
}

pub fn azure_static_credentials() -> BoxedStrategy<AzureStaticCredentials> {
    use AzureStaticCredentials::*;
    prop_oneof![
        any::<String>().prop_map(AccessKey),
        any::<String>().prop_map(SASToken),
        any::<String>().prop_map(BearerToken),
    ]
    .boxed()
}

pub fn azure_credentials() -> BoxedStrategy<AzureCredentials> {
    use AzureCredentials::*;
    prop_oneof![Just(FromEnv), azure_static_credentials().prop_map(Static)].boxed()
}

fn path_component() -> impl Strategy<Value = String> {
    string_regex("[a-zA-Z0-9]{10}").expect("Could not generate a valid path component")
}

fn file_path_components() -> impl Strategy<Value = Vec<String>> {
    vec(path_component(), 8..15)
}

// Given a collection of directory names, an absolute Unix style path
// using the directory names in order is generated
fn to_abs_unix_path(path_components: Vec<String>) -> String {
    format!("/{}", path_components.join("/"))
}

// Generates Unix style absolute file paths
fn absolute_path() -> impl Strategy<Value = String> {
    file_path_components().prop_map(to_abs_unix_path)
}

pub fn path() -> impl Strategy<Value = Path> {
    absolute_path().prop_filter_map("Could not generate a valid path", |abs_path| {
        Path::new(&abs_path).ok()
    })
}

type DimensionShapeInfo = (u64, u64);

prop_compose! {
    fn dimension_shape_info()(dim_length in any::<u64>(), chunk_length in any::<NonZeroU64>()) -> DimensionShapeInfo {
        (dim_length, chunk_length.get())
    }
}

prop_compose! {
    fn array_shape()(dimensions in vec(dimension_shape_info(), 5..30)) -> ArrayShape {
        ArrayShape::new(dimensions).unwrap()
    }
}

fn dimension_name() -> impl Strategy<Value = DimensionName> {
    use DimensionName::*;
    prop_oneof![Just(NotSpecified), any::<String>().prop_map(Name)]
}

prop_compose! {
pub fn bytes()(random_data in any::<Vec<u8>>()) -> Bytes {
        Bytes::from(random_data)
    }
}

prop_compose! {
pub    fn array_data()(shape in array_shape(),
        dimension_names in option::of(vec(dimension_name(), 5..10)),
    user_data in bytes()) -> ArrayData {
        ArrayData{shape, dimension_names, user_data}
    }
}

pub fn node_id() -> impl Strategy<Value = NodeId> {
    uniform8(any::<u8>()).prop_map(NodeId::new)
}

fn chunk_id() -> impl Strategy<Value = ChunkId> {
    uniform12(any::<u8>()).prop_map(ChunkId::new)
}

prop_compose! {
    fn chunk_ref()(id in chunk_id(), offset in any::<u64>(), length in any::<u64>()) -> ChunkRef {
        ChunkRef{ id, offset, length }
    }
}

fn etag() -> impl Strategy<Value = ETag> {
    any::<String>().prop_map(ETag)
}
fn checksum() -> impl Strategy<Value = manifest::Checksum> {
    use manifest::Checksum::*;
    prop_oneof![
        any::<u32>().prop_map(SecondsSinceEpoch).prop_map(LastModified),
        etag().prop_map(ETag)
    ]
}

fn non_empty_alphanumeric_string() -> impl Strategy<Value = String> {
    string_regex("[a-zA-Z0-9]{1,}")
        .expect("Could not generate a valid nonempty alphanumeric string")
}

prop_compose! {
    fn url_with_host_and_path()(protocol in transfer_protocol(),
        host in non_empty_alphanumeric_string(),
        path in non_empty_alphanumeric_string()) -> String {
        format!("{}://{}/{}", protocol, host, path)
    }
}

fn virtual_chunk_location() -> impl Strategy<Value = VirtualChunkLocation> {
    url_with_host_and_path()
        .prop_filter_map("Could not generate url with valid host and path", |url| {
            VirtualChunkLocation::from_absolute_path(&url).ok()
        })
}

prop_compose! {
    fn virtual_chunk_ref()(location in virtual_chunk_location(),
        offset in any::<u64>(),
        length in any::<u64>(),
       checksum in option::of(checksum())) -> VirtualChunkRef {
        VirtualChunkRef{ location, offset, length, checksum }
    }
}

fn chunk_payload() -> impl Strategy<Value = ChunkPayload> {
    use ChunkPayload::*;
    prop_oneof![
        bytes().prop_map(Inline),
        virtual_chunk_ref().prop_map(Virtual),
        chunk_ref().prop_map(Ref)
    ]
}

type SplitManifest = BTreeMap<ChunkIndices, Option<ChunkPayload>>;

// pub fn chunk_indices2() -> impl Strategy<Value = ChunkIndices> {
//     (any::<u8>().prop_map(usize::from), any::<Range<u32>>())
//         .prop_flat_map(|(dim, data)| chunk_indices(dim, data))
// }

pub fn chunk_indices2(dim: usize) -> impl Strategy<Value = ChunkIndices> {
    any::<Range<u32>>()
        .prop_flat_map(move |data| chunk_indices(dim, data))
}

pub fn split_manifest() -> impl Strategy<Value = SplitManifest> {
    any::<u16>().prop_map(usize::from).prop_flat_map(|dim|
    btree_map(chunk_indices2(dim), option::of(chunk_payload()), 3..10))
}

prop_compose! {
    pub fn gen_move()(to in path(), from in path()) -> Move {
        Move{to, from}
    }
}
