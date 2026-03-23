//! Proptest strategies for format types.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, dead_code)]

use crate::format_constants::SpecVersionBin;
use crate::manifest::{
    ChunkPayload, ChunkRef, ManifestExtents, ManifestRef, ManifestSplits,
    SecondsSinceEpoch, VirtualChunkLocation, VirtualChunkRef,
};
use crate::snapshot::{
    ArrayShape, DimensionName, ManifestFileInfo, NodeData, NodeSnapshot,
};
use crate::{
    AttributesId, ChunkId, ChunkIndices, ManifestId, NodeId, Path, SnapshotId, manifest,
};
use bytes::Bytes;
use icechunk_types::ETag;
use prop::string::string_regex;
use proptest::collection::vec;
use proptest::prelude::*;
use proptest::{
    array::{uniform8, uniform12},
    option,
    strategy::Strategy,
};
use std::ops::Range;

const MAX_NDIM: usize = 4;

pub fn spec_version() -> BoxedStrategy<SpecVersionBin> {
    prop_oneof![Just(SpecVersionBin::V2), Just(SpecVersionBin::V1)].boxed()
}

pub fn attributes_id() -> impl Strategy<Value = AttributesId> {
    uniform12(any::<u8>()).prop_map(AttributesId::new)
}

#[derive(Debug)]
pub struct ShapeDim {
    pub shape: ArrayShape,
    pub dimension_names: Option<Vec<DimensionName>>,
}

pub fn shapes_and_dims(
    max_ndim: Option<usize>,
    min_dim_size: Option<usize>,
) -> impl Strategy<Value = ShapeDim> {
    let max_ndim = max_ndim.unwrap_or(MAX_NDIM);
    let min_size = min_dim_size.unwrap_or(0);
    vec((min_size as u64)..26u64, 1..max_ndim)
        .prop_flat_map(move |shape| {
            let ndim = shape.len();
            let chunk_shape: Vec<BoxedStrategy<u64>> = shape
                .clone()
                .into_iter()
                .map(move |size| ((min_size as u64)..=size).boxed())
                .collect();
            (Just(shape), chunk_shape, option::of(vec(option::of(any::<String>()), ndim)))
        })
        .prop_map(|(shape, chunk_shape, dimension_names)| ShapeDim {
            shape: ArrayShape::new(shape.into_iter().zip(chunk_shape.iter()).map(
                |(dim_length, chunk_size)| {
                    (
                        dim_length,
                        if chunk_size == &0 {
                            0
                        } else {
                            dim_length.div_ceil(*chunk_size) as u32
                        },
                    )
                },
            ))
            .expect("Invalid array shape"),
            dimension_names: dimension_names.map(|ds| {
                ds.iter().map(|s| From::from(s.as_ref().map(|s| s.as_str()))).collect()
            }),
        })
}

pub fn limited_width_manifest_extents(
    ndim: usize,
) -> impl Strategy<Value = ManifestExtents> {
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

fn path_component() -> impl Strategy<Value = String> {
    string_regex("[a-zA-Z0-9]{10}").expect("Could not generate a valid path component")
}

fn file_path_components() -> impl Strategy<Value = Vec<String>> {
    vec(path_component(), 8..15)
}

fn to_abs_unix_path(path_components: &[String]) -> String {
    format!("/{}", path_components.join("/"))
}

fn absolute_path() -> impl Strategy<Value = String> {
    file_path_components().prop_map(|c| to_abs_unix_path(&c))
}

pub fn path() -> impl Strategy<Value = Path> {
    absolute_path().prop_filter_map("Could not generate a valid path", |abs_path| {
        Path::new(&abs_path).ok()
    })
}

type DimensionShapeInfo = (u64, u32);

prop_compose! {
    fn dimension_shape_info()(dim_length in any::<u64>(), chunk_length in any::<u64>()) -> DimensionShapeInfo {
        (dim_length, dim_length.div_ceil(chunk_length) as u32)
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
        format!("{protocol}://{host}/{path}")
    }
}

fn virtual_chunk_location() -> impl Strategy<Value = VirtualChunkLocation> {
    url_with_host_and_path()
        .prop_filter_map("Could not generate url with valid host and path", |url| {
            VirtualChunkLocation::from_url(&url).ok()
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

pub fn large_chunk_indices(dim: usize) -> impl Strategy<Value = ChunkIndices> {
    any::<Range<u32>>().prop_flat_map(move |data| chunk_indices(dim, data))
}

fn snapshot_id() -> impl Strategy<Value = SnapshotId> {
    uniform12(any::<u8>()).prop_map(SnapshotId::new)
}

fn manifest_id() -> impl Strategy<Value = ManifestId> {
    uniform12(any::<u8>()).prop_map(ManifestId::new)
}

prop_compose! {
    pub fn manifest_ref()
    (ndim in any::<u8>().prop_map(usize::from))
    (object_id in manifest_id(),
     extents in manifest_extents(ndim),
    ) -> ManifestRef {
        ManifestRef{ object_id, extents }
    }
}

pub fn manifest_splits() -> impl Strategy<Value = ManifestSplits> {
    (1..10usize)
        .prop_flat_map(|ndim| {
            vec(
                proptest::collection::hash_set(0u32..1000, 2..6usize).prop_map(|s| {
                    let mut v: Vec<u32> = s.into_iter().collect();
                    v.sort();
                    v
                }),
                ndim,
            )
        })
        .prop_map(ManifestSplits::from_edges)
}

type ArrayInfo = (ArrayShape, Option<Vec<DimensionName>>, Vec<ManifestRef>);
fn array_info() -> impl Strategy<Value = ArrayInfo> {
    (array_shape(), option::of(vec(dimension_name(), 5..10)), vec(manifest_ref(), 1..8))
}

fn node_data() -> impl Strategy<Value = NodeData> {
    use NodeData::*;
    prop_oneof![
        Just(Group),
        array_info().prop_map(|(shape, dimension_names, manifests)| Array {
            shape,
            dimension_names,
            manifests
        }),
    ]
}

prop_compose! {
   pub fn node_snapshot()
    (id in node_id(),
        path in path(),
        user_data in bytes(),
        node_data in node_data()) -> NodeSnapshot {
        NodeSnapshot{id, path, user_data, node_data}
    }
}

prop_compose! {
    pub fn manifest_file_info()
    (id in manifest_id(),
    size_bytes in any::<u64>(),
    num_chunk_refs in any::<u32>()) -> ManifestFileInfo  {
        ManifestFileInfo{id, size_bytes, num_chunk_refs}
    }
}
