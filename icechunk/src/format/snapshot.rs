use std::{collections::BTreeMap, convert::Infallible, sync::Arc};

use chrono::{DateTime, Utc};
use err_into::ErrorInto;
use flatbuffers::{FlatBufferBuilder, VerifierOptions};
use itertools::Itertools as _;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::metadata::{
    ArrayShape, ChunkKeyEncoding, ChunkShape, Codec, DataType, DimensionNames, FillValue,
    StorageTransformer, UserAttributes,
};

use super::{
    flatbuffers::gen,
    manifest::{Manifest, ManifestExtents, ManifestRef},
    AttributesId, ChunkIndices, IcechunkFormatError, IcechunkFormatErrorKind,
    IcechunkResult, ManifestId, NodeId, Path, SnapshotId, TableOffset,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UserAttributesRef {
    pub object_id: AttributesId,
    pub location: TableOffset,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum UserAttributesSnapshot {
    Inline(UserAttributes),
    Ref(UserAttributesRef),
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub enum NodeType {
    Group,
    Array,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ZarrArrayMetadata {
    pub shape: ArrayShape,
    pub data_type: DataType,
    pub chunk_shape: ChunkShape,
    pub chunk_key_encoding: ChunkKeyEncoding,
    pub fill_value: FillValue,
    pub codecs: Vec<Codec>,
    pub storage_transformers: Option<Vec<StorageTransformer>>,
    pub dimension_names: Option<DimensionNames>,
}

impl ZarrArrayMetadata {
    /// Returns an iterator over the maximum permitted chunk indices for the array.
    ///
    /// This function calculates the maximum chunk indices based on the shape of the array
    /// and the chunk shape, using (shape - 1) / chunk_shape. Given integer division is truncating,
    /// this will always result in proper indices at the boundaries.
    ///
    /// # Returns
    ///
    /// A ChunkIndices type containing the max chunk index for each dimension.
    fn max_chunk_indices_permitted(&self) -> ChunkIndices {
        debug_assert_eq!(self.shape.len(), self.chunk_shape.0.len());

        ChunkIndices(
            self.shape
                .iter()
                .zip(self.chunk_shape.0.iter())
                .map(|(s, cs)| if *s == 0 { 0 } else { ((s - 1) / cs.get()) as u32 })
                .collect(),
        )
    }

    /// Validates the provided chunk coordinates for the array.
    ///
    /// This function checks if the provided chunk indices are valid for the array.
    ///
    /// # Arguments
    ///
    /// * `coord` - The chunk indices to validate.
    ///
    /// # Returns
    ///
    /// An bool indicating whether the chunk coordinates are valid.
    ///
    /// # Errors
    ///
    /// Returns false if the chunk coordinates are invalid.
    pub fn valid_chunk_coord(&self, coord: &ChunkIndices) -> bool {
        debug_assert_eq!(self.shape.len(), coord.0.len());

        coord
            .0
            .iter()
            .zip(self.max_chunk_indices_permitted().0)
            .all(|(index, index_permitted)| *index <= index_permitted)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum NodeData {
    Array(ZarrArrayMetadata, Vec<ManifestRef>),
    Group,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NodeSnapshot {
    pub id: NodeId,
    pub path: Path,
    pub user_attributes: Option<UserAttributesSnapshot>,
    pub node_data: NodeData,
}

impl NodeSnapshot {
    pub fn node_type(&self) -> NodeType {
        match &self.node_data {
            NodeData::Group => NodeType::Group,
            NodeData::Array(_, _) => NodeType::Array,
        }
    }
}

impl From<&gen::ObjectId8> for NodeId {
    fn from(value: &gen::ObjectId8) -> Self {
        NodeId::new(value.0)
    }
}

impl From<&gen::ObjectId12> for ManifestId {
    fn from(value: &gen::ObjectId12) -> Self {
        ManifestId::new(value.0)
    }
}

impl From<&gen::ObjectId12> for AttributesId {
    fn from(value: &gen::ObjectId12) -> Self {
        AttributesId::new(value.0)
    }
}

impl<'a> From<gen::ManifestRef<'a>> for ManifestRef {
    fn from(value: gen::ManifestRef<'a>) -> Self {
        let from = value.extents().iter().map(|range| range.from()).collect::<Vec<_>>();
        let to = value.extents().iter().map(|range| range.to()).collect::<Vec<_>>();
        let extents = ManifestExtents::new(from.as_slice(), to.as_slice());
        ManifestRef { object_id: value.object_id().into(), extents }
    }
}

impl<'a> TryFrom<gen::ArrayNodeData<'a>> for NodeData {
    type Error = rmp_serde::decode::Error;

    fn try_from(value: gen::ArrayNodeData<'a>) -> Result<Self, Self::Error> {
        // TODO: is it ok to call `bytes` here? Or do we need to collect an iterator
        let meta = rmp_serde::from_slice(value.zarr_metadata().bytes())?;
        let manifest_refs = value.manifests().iter().map(|m| m.into()).collect();
        Ok(Self::Array(meta, manifest_refs))
    }
}

impl<'a> From<gen::GroupNodeData<'a>> for NodeData {
    fn from(_: gen::GroupNodeData<'a>) -> Self {
        Self::Group
    }
}

impl<'a> TryFrom<gen::InlineUserAttributes<'a>> for UserAttributesSnapshot {
    type Error = rmp_serde::decode::Error;

    fn try_from(value: gen::InlineUserAttributes<'a>) -> Result<Self, Self::Error> {
        let parsed = rmp_serde::from_slice(value.data().bytes())?;
        Ok(Self::Inline(UserAttributes { parsed }))
    }
}

impl<'a> From<gen::UserAttributesRef<'a>> for UserAttributesSnapshot {
    fn from(value: gen::UserAttributesRef<'a>) -> Self {
        Self::Ref(UserAttributesRef {
            object_id: value.object_id().into(),
            location: value.location(),
        })
    }
}

impl<'a> TryFrom<gen::NodeSnapshot<'a>> for NodeSnapshot {
    type Error = IcechunkFormatError;

    fn try_from(value: gen::NodeSnapshot<'a>) -> Result<Self, Self::Error> {
        #[allow(clippy::expect_used, clippy::panic)]
        let node_data: NodeData = match value.node_data_type() {
            gen::NodeData::Array => value
                .node_data_as_array()
                .expect("Bug in flatbuffers library")
                .try_into()?,
            gen::NodeData::Group => {
                value.node_data_as_group().expect("Bug in flatbuffers library").into()
            }
            x => panic!("Invalid node data type in flatbuffers file {:?}", x),
        };
        #[allow(clippy::expect_used, clippy::panic)]
        let user_attributes: Option<UserAttributesSnapshot> =
            match value.user_attributes_type() {
                gen::UserAttributesSnapshot::Inline => Some(
                    value
                        .user_attributes_as_inline()
                        .expect("Bug in flatbuffers library")
                        .try_into()?,
                ),
                gen::UserAttributesSnapshot::Reference => Some(
                    value
                        .user_attributes_as_reference()
                        .expect("Bug in flatbuffers library")
                        .into(),
                ),
                gen::UserAttributesSnapshot::NONE => None,
                x => panic!("Invalid user attributes type in flatbuffers file {:?}", x),
            };
        let res = NodeSnapshot {
            id: value.id().into(),
            path: value.path().to_string().try_into()?,
            user_attributes,
            node_data,
        };
        Ok(res)
    }
}

impl From<&gen::ManifestFileInfo> for ManifestFileInfo {
    fn from(value: &gen::ManifestFileInfo) -> Self {
        Self {
            id: value.id().into(),
            size_bytes: value.size_bytes(),
            num_chunk_refs: value.num_chunk_refs(),
        }
    }
}

impl From<&gen::AttributeFileInfo> for AttributeFileInfo {
    fn from(value: &gen::AttributeFileInfo) -> Self {
        Self { id: value.id().into() }
    }
}

pub type SnapshotProperties = BTreeMap<String, Value>;

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone, Eq, Hash)]
pub struct ManifestFileInfo {
    pub id: ManifestId,
    pub size_bytes: u64,
    pub num_chunk_refs: u32,
}

impl ManifestFileInfo {
    pub fn new(manifest: &Manifest, size_bytes: u64) -> Self {
        Self {
            id: manifest.id().clone(),
            num_chunk_refs: manifest.len() as u32,
            size_bytes,
        }
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct AttributeFileInfo {
    pub id: AttributesId,
}

#[derive(Debug, PartialEq)]
pub struct Snapshot {
    buffer: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotInfo {
    pub id: SnapshotId,
    pub parent_id: Option<SnapshotId>,
    pub flushed_at: DateTime<chrono::Utc>,
    pub message: String,
    pub metadata: SnapshotProperties,
}

impl TryFrom<&Snapshot> for SnapshotInfo {
    type Error = IcechunkFormatError;

    fn try_from(value: &Snapshot) -> Result<Self, Self::Error> {
        Ok(Self {
            id: value.id().clone(),
            parent_id: value.parent_id().clone(),
            flushed_at: value.flushed_at()?,
            message: value.message().to_string(),
            metadata: value.metadata()?.clone(),
        })
    }
}

impl SnapshotInfo {
    pub fn is_initial(&self) -> bool {
        self.parent_id.is_none()
    }
}

static ROOT_OPTIONS: VerifierOptions = VerifierOptions {
    max_depth: 64,
    max_tables: 500_000,
    max_apparent_size: 1 << 31, // taken from the default
    ignore_missing_null_terminator: true,
};

impl Snapshot {
    pub const INITIAL_COMMIT_MESSAGE: &'static str = "Repository initialized";

    pub fn from_buffer(buffer: Vec<u8>) -> IcechunkResult<Snapshot> {
        let _ = flatbuffers::root_with_opts::<gen::Snapshot>(
            &ROOT_OPTIONS,
            buffer.as_slice(),
        )?;
        Ok(Snapshot { buffer })
    }

    pub fn bytes(&self) -> &[u8] {
        self.buffer.as_slice()
    }

    pub fn from_iter<E, I>(
        id: Option<SnapshotId>,
        parent_id: Option<SnapshotId>,
        message: String,
        properties: Option<SnapshotProperties>,
        mut manifest_files: Vec<ManifestFileInfo>,
        mut attribute_files: Vec<AttributeFileInfo>,
        sorted_iter: I,
    ) -> IcechunkResult<Self>
    where
        IcechunkFormatError: From<E>,
        I: IntoIterator<Item = Result<NodeSnapshot, E>>,
    {
        // TODO: what's a good capacity?
        let mut builder = flatbuffers::FlatBufferBuilder::with_capacity(4_096);

        manifest_files.sort_by(|a, b| a.id.cmp(&b.id));
        let manifest_files = manifest_files
            .iter()
            .map(|mfi| {
                let id = gen::ObjectId12::new(&mfi.id.0);
                gen::ManifestFileInfo::new(&id, mfi.size_bytes, mfi.num_chunk_refs)
            })
            .collect::<Vec<_>>();
        let manifest_files = builder.create_vector(&manifest_files);

        attribute_files.sort_by(|a, b| a.id.cmp(&b.id));
        let attribute_files = attribute_files
            .iter()
            .map(|att| {
                let id = gen::ObjectId12::new(&att.id.0);
                gen::AttributeFileInfo::new(&id)
            })
            .collect::<Vec<_>>();
        let attribute_files = builder.create_vector(&attribute_files);

        let metadata_items: Vec<_> = properties
            .unwrap_or_default()
            .iter()
            .map(|(k, v)| {
                let name = builder.create_shared_string(k.as_str());
                let serialized = rmp_serde::to_vec(v)?;
                let value = builder.create_vector(serialized.as_slice());
                Ok::<_, IcechunkFormatError>(gen::MetadataItem::create(
                    &mut builder,
                    &gen::MetadataItemArgs { name: Some(name), value: Some(value) },
                ))
            })
            .try_collect()?;
        let metadata_items = builder.create_vector(metadata_items.as_slice());

        let message = builder.create_string(&message);
        let parent_id = parent_id.map(|oid| gen::ObjectId12::new(&oid.0));
        let flushed_at = Utc::now().timestamp_micros() as u64;
        let id = gen::ObjectId12::new(&id.unwrap_or_else(SnapshotId::random).0);

        let nodes: Vec<_> = sorted_iter
            .into_iter()
            .map(|node| node.err_into().and_then(|node| mk_node(&mut builder, &node)))
            .try_collect()?;
        let nodes = builder.create_vector(&nodes);

        let snap = gen::Snapshot::create(
            &mut builder,
            &gen::SnapshotArgs {
                id: Some(&id),
                parent_id: parent_id.as_ref(),
                nodes: Some(nodes),
                flushed_at,
                message: Some(message),
                metadata: Some(metadata_items),
                manifest_files: Some(manifest_files),
                attribute_files: Some(attribute_files),
            },
        );

        builder.finish(snap, Some("Ichk"));
        let (mut buffer, offset) = builder.collapse();
        buffer.drain(0..offset);
        buffer.shrink_to_fit();
        Ok(Snapshot { buffer })
    }

    pub fn initial() -> IcechunkResult<Self> {
        let properties = [("__root".to_string(), serde_json::Value::from(true))].into();
        let nodes: Vec<Result<NodeSnapshot, Infallible>> = Vec::new();
        Self::from_iter(
            None,
            None,
            Self::INITIAL_COMMIT_MESSAGE.to_string(),
            Some(properties),
            Default::default(),
            Default::default(),
            nodes,
        )
    }

    fn root(&self) -> gen::Snapshot {
        // without the unsafe version this is too slow
        // if we try to keep the root in the Manifest struct, we would need a lifetime
        unsafe { flatbuffers::root_unchecked::<gen::Snapshot>(&self.buffer) }
    }

    pub fn id(&self) -> SnapshotId {
        SnapshotId::new(self.root().id().0)
    }

    pub fn parent_id(&self) -> Option<SnapshotId> {
        self.root().parent_id().map(|pid| SnapshotId::new(pid.0))
    }

    pub fn metadata(&self) -> IcechunkResult<SnapshotProperties> {
        self.root()
            .metadata()
            .iter()
            .map(|item| {
                let key = item.name().to_string();
                let value = rmp_serde::from_slice(item.value().bytes())?;
                Ok((key, value))
            })
            .try_collect()
    }

    pub fn flushed_at(&self) -> IcechunkResult<DateTime<Utc>> {
        let ts = self.root().flushed_at();
        let ts: i64 = ts.try_into().map_err(|_| {
            IcechunkFormatError::from(IcechunkFormatErrorKind::InvalidTimestamp)
        })?;
        DateTime::from_timestamp_micros(ts)
            .ok_or_else(|| IcechunkFormatErrorKind::InvalidTimestamp.into())
    }

    pub fn message(&self) -> String {
        self.root().message().to_string()
    }

    // pub fn nodes(&self) -> &BTreeMap<Path, NodeSnapshot> {
    //     &self.nodes
    // }

    pub fn get_manifest_file(&self, id: &ManifestId) -> Option<ManifestFileInfo> {
        self.root().manifest_files().iter().find(|mf| mf.id().0 == id.0.as_slice()).map(
            |mf| ManifestFileInfo {
                id: ManifestId::new(mf.id().0),
                size_bytes: mf.size_bytes(),
                num_chunk_refs: mf.num_chunk_refs(),
            },
        )
    }

    pub fn manifest_files(&self) -> impl Iterator<Item = ManifestFileInfo> + '_ {
        self.root().manifest_files().iter().map(|mf| mf.into())
    }

    pub fn attribute_files(&self) -> impl Iterator<Item = AttributeFileInfo> + '_ {
        self.root().attribute_files().iter().map(|f| f.into())
    }

    /// Cretase a new `Snapshot` with all the same data as `new_child` but `self` as parent
    pub fn adopt(&self, new_child: &Snapshot) -> IcechunkResult<Self> {
        // Rust flatbuffers implementation doesn't allow mutation of scalars, so we need to
        // create a whole new buffer and write to it in full

        Snapshot::from_iter(
            Some(new_child.id()),
            Some(self.id()),
            new_child.message().clone(),
            Some(new_child.metadata()?.clone()),
            new_child.manifest_files().collect(),
            new_child.attribute_files().collect(),
            new_child.iter(),
        )
    }

    pub fn get_node(&self, path: &Path) -> IcechunkResult<NodeSnapshot> {
        let res = self
            .root()
            .nodes()
            .lookup_by_key(path.to_string().as_str(), |node, path| node.path().cmp(path))
            .ok_or(IcechunkFormatError::from(IcechunkFormatErrorKind::NodeNotFound {
                path: path.clone(),
            }))?;
        res.try_into()
    }

    pub fn iter(&self) -> impl Iterator<Item = IcechunkResult<NodeSnapshot>> + '_ {
        self.root().nodes().iter().map(|node| node.try_into().err_into())
    }

    pub fn iter_arc(
        self: Arc<Self>,
    ) -> impl Iterator<Item = IcechunkResult<NodeSnapshot>> {
        NodeIterator { snapshot: self, last_index: 0 }
    }

    pub fn len(&self) -> usize {
        self.root().nodes().len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn manifest_info(&self, id: &ManifestId) -> Option<ManifestFileInfo> {
        self.root()
            .manifest_files()
            .iter()
            .find(|mi| mi.id().0 == id.0)
            .map(|man| man.into())
    }
}

struct NodeIterator {
    snapshot: Arc<Snapshot>,
    last_index: usize,
}

impl Iterator for NodeIterator {
    type Item = IcechunkResult<NodeSnapshot>;

    fn next(&mut self) -> Option<Self::Item> {
        let nodes = self.snapshot.root().nodes();
        if self.last_index < nodes.len() {
            let res = Some(nodes.get(self.last_index).try_into().err_into());
            self.last_index += 1;
            res
        } else {
            None
        }
    }
}

fn mk_node<'bldr>(
    builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
    node: &NodeSnapshot,
) -> IcechunkResult<flatbuffers::WIPOffset<gen::NodeSnapshot<'bldr>>> {
    let id = gen::ObjectId8::new(&node.id.0);
    let path = builder.create_string(node.path.to_string().as_str());
    let (user_attributes_type, user_attributes) =
        mk_user_attributes(builder, node.user_attributes.as_ref())?;
    let (node_data_type, node_data) = mk_node_data(builder, &node.node_data)?;
    Ok(gen::NodeSnapshot::create(
        builder,
        &gen::NodeSnapshotArgs {
            id: Some(&id),
            path: Some(path),
            user_attributes_type,
            user_attributes,
            node_data_type,
            node_data,
        },
    ))
}

fn mk_user_attributes(
    builder: &mut flatbuffers::FlatBufferBuilder<'_>,
    atts: Option<&UserAttributesSnapshot>,
) -> IcechunkResult<(
    gen::UserAttributesSnapshot,
    Option<flatbuffers::WIPOffset<flatbuffers::UnionWIPOffset>>,
)> {
    match atts {
        Some(UserAttributesSnapshot::Inline(user_attributes)) => {
            let data = builder
                .create_vector(rmp_serde::to_vec(&user_attributes.parsed)?.as_slice());
            let inl = gen::InlineUserAttributes::create(
                builder,
                &gen::InlineUserAttributesArgs { data: Some(data) },
            );
            Ok((gen::UserAttributesSnapshot::Inline, Some(inl.as_union_value())))
        }
        Some(UserAttributesSnapshot::Ref(uatts)) => {
            let id = gen::ObjectId12::new(&uatts.object_id.0);
            let reference = gen::UserAttributesRef::create(
                builder,
                &gen::UserAttributesRefArgs {
                    object_id: Some(&id),
                    location: uatts.location,
                },
            );
            Ok((gen::UserAttributesSnapshot::Reference, Some(reference.as_union_value())))
        }
        None => Ok((gen::UserAttributesSnapshot::NONE, None)),
    }
}

fn mk_node_data(
    builder: &mut FlatBufferBuilder<'_>,
    node_data: &NodeData,
) -> IcechunkResult<(
    gen::NodeData,
    Option<flatbuffers::WIPOffset<flatbuffers::UnionWIPOffset>>,
)> {
    match node_data {
        NodeData::Array(zarr, manifests) => {
            let zarr_metadata =
                Some(builder.create_vector(rmp_serde::to_vec(zarr)?.as_slice()));
            let manifests = manifests
                .iter()
                .map(|manref| {
                    let object_id = gen::ObjectId12::new(&manref.object_id.0);
                    let extents = manref
                        .extents
                        .iter()
                        .map(|range| gen::ChunkIndexRange::new(range.start, range.end))
                        .collect::<Vec<_>>();
                    let extents = builder.create_vector(&extents);
                    gen::ManifestRef::create(
                        builder,
                        &gen::ManifestRefArgs {
                            object_id: Some(&object_id),
                            extents: Some(extents),
                        },
                    )
                })
                .collect::<Vec<_>>();
            let manifests = builder.create_vector(manifests.as_slice());
            Ok((
                gen::NodeData::Array,
                Some(
                    gen::ArrayNodeData::create(
                        builder,
                        &gen::ArrayNodeDataArgs {
                            zarr_metadata,
                            manifests: Some(manifests),
                        },
                    )
                    .as_union_value(),
                ),
            ))
        }
        NodeData::Group => Ok((
            gen::NodeData::Group,
            Some(
                gen::GroupNodeData::create(builder, &gen::GroupNodeDataArgs {})
                    .as_union_value(),
            ),
        )),
    }
}

#[cfg(test)]
#[allow(clippy::panic, clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use crate::format::{IcechunkFormatError, ObjectId};

    use super::*;
    use pretty_assertions::assert_eq;
    use std::{
        collections::HashMap,
        iter::{self},
        num::NonZeroU64,
    };

    #[test]
    fn test_get_node() -> Result<(), Box<dyn std::error::Error>> {
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

            codecs: vec![Codec {
                name: "mycodec".to_string(),
                configuration: Some(HashMap::from_iter(iter::once((
                    "foo".to_string(),
                    serde_json::Value::from(42),
                )))),
            }],
            storage_transformers: Some(vec![StorageTransformer {
                name: "mytransformer".to_string(),
                configuration: Some(HashMap::from_iter(iter::once((
                    "foo".to_string(),
                    serde_json::Value::from(42),
                )))),
            }]),
            dimension_names: Some(vec![
                Some("x".to_string()),
                Some("y".to_string()),
                Some("t".to_string()),
            ]),
        };
        let zarr_meta2 = ZarrArrayMetadata {
            storage_transformers: None,
            data_type: DataType::Int32,
            dimension_names: Some(vec![None, None, Some("t".to_string())]),
            fill_value: FillValue::Int32(0i32),
            ..zarr_meta1.clone()
        };
        let zarr_meta3 =
            ZarrArrayMetadata { dimension_names: None, ..zarr_meta2.clone() };
        let man_ref1 = ManifestRef {
            object_id: ObjectId::random(),
            extents: ManifestExtents::new(&[0, 0, 0], &[100, 100, 100]),
        };
        let man_ref2 = ManifestRef {
            object_id: ObjectId::random(),
            extents: ManifestExtents::new(&[0, 0, 0], &[100, 100, 100]),
        };

        let oid = ObjectId::random();
        let node_ids = iter::repeat_with(NodeId::random).take(7).collect::<Vec<_>>();
        // nodes must be sorted by path
        let nodes = vec![
            NodeSnapshot {
                path: Path::root(),
                id: node_ids[0].clone(),
                user_attributes: None,
                node_data: NodeData::Group,
            },
            NodeSnapshot {
                path: "/a".try_into().unwrap(),
                id: node_ids[1].clone(),
                user_attributes: None,
                node_data: NodeData::Group,
            },
            NodeSnapshot {
                path: "/array2".try_into().unwrap(),
                id: node_ids[5].clone(),
                user_attributes: None,
                node_data: NodeData::Array(zarr_meta2.clone(), vec![]),
            },
            NodeSnapshot {
                path: "/b".try_into().unwrap(),
                id: node_ids[2].clone(),
                user_attributes: None,
                node_data: NodeData::Group,
            },
            NodeSnapshot {
                path: "/b/array1".try_into().unwrap(),
                id: node_ids[4].clone(),
                user_attributes: Some(UserAttributesSnapshot::Ref(UserAttributesRef {
                    object_id: oid.clone(),
                    location: 42,
                })),
                node_data: NodeData::Array(
                    zarr_meta1.clone(),
                    vec![man_ref1.clone(), man_ref2.clone()],
                ),
            },
            NodeSnapshot {
                path: "/b/array3".try_into().unwrap(),
                id: node_ids[6].clone(),
                user_attributes: None,
                node_data: NodeData::Array(zarr_meta3.clone(), vec![]),
            },
            NodeSnapshot {
                path: "/b/c".try_into().unwrap(),
                id: node_ids[3].clone(),
                user_attributes: Some(UserAttributesSnapshot::Inline(
                    UserAttributes::try_new(br#"{"foo": "some inline"}"#).unwrap(),
                )),
                node_data: NodeData::Group,
            },
        ];
        let initial = Snapshot::initial().unwrap();
        let manifests = vec![
            ManifestFileInfo {
                id: man_ref1.object_id.clone(),
                size_bytes: 1_000_000,
                num_chunk_refs: 100_000,
            },
            ManifestFileInfo {
                id: man_ref2.object_id.clone(),
                size_bytes: 1_000_000,
                num_chunk_refs: 100_000,
            },
        ];
        let st = Snapshot::from_iter(
            None,
            Some(initial.id().clone()),
            String::default(),
            Default::default(),
            manifests,
            vec![],
            nodes.into_iter().map(Ok::<NodeSnapshot, Infallible>),
        )
        .unwrap();

        assert!(matches!(
            st.get_node(&"/nonexistent".try_into().unwrap()),
            Err(IcechunkFormatError {
                kind: IcechunkFormatErrorKind::NodeNotFound {
                    path
                },
                ..
            }) if path == "/nonexistent".try_into().unwrap()
        ));

        let node = st.get_node(&"/b/c".try_into().unwrap()).unwrap();
        assert_eq!(
            node,
            NodeSnapshot {
                path: "/b/c".try_into().unwrap(),
                id: node_ids[3].clone(),
                user_attributes: Some(UserAttributesSnapshot::Inline(
                    UserAttributes::try_new(br#"{"foo": "some inline"}"#).unwrap(),
                )),
                node_data: NodeData::Group,
            },
        );
        let node = st.get_node(&Path::root()).unwrap();
        assert_eq!(
            node,
            NodeSnapshot {
                path: Path::root(),
                id: node_ids[0].clone(),
                user_attributes: None,
                node_data: NodeData::Group,
            },
        );
        let node = st.get_node(&"/b/array1".try_into().unwrap()).unwrap();
        assert_eq!(
            node,
            NodeSnapshot {
                path: "/b/array1".try_into().unwrap(),
                id: node_ids[4].clone(),
                user_attributes: Some(UserAttributesSnapshot::Ref(UserAttributesRef {
                    object_id: oid,
                    location: 42,
                })),
                node_data: NodeData::Array(zarr_meta1.clone(), vec![man_ref1, man_ref2]),
            },
        );
        let node = st.get_node(&"/array2".try_into().unwrap()).unwrap();
        assert_eq!(
            node,
            NodeSnapshot {
                path: "/array2".try_into().unwrap(),
                id: node_ids[5].clone(),
                user_attributes: None,
                node_data: NodeData::Array(zarr_meta2.clone(), vec![]),
            },
        );
        let node = st.get_node(&"/b/array3".try_into().unwrap()).unwrap();
        assert_eq!(
            node,
            NodeSnapshot {
                path: "/b/array3".try_into().unwrap(),
                id: node_ids[6].clone(),
                user_attributes: None,
                node_data: NodeData::Array(zarr_meta3.clone(), vec![]),
            },
        );
        Ok(())
    }

    #[test]
    fn test_valid_chunk_coord() {
        let zarr_meta1 = ZarrArrayMetadata {
            shape: vec![10000, 10001, 9999],
            data_type: DataType::Float32,
            chunk_shape: ChunkShape(vec![
                NonZeroU64::new(1000).unwrap(),
                NonZeroU64::new(1000).unwrap(),
                NonZeroU64::new(1000).unwrap(),
            ]),
            chunk_key_encoding: ChunkKeyEncoding::Slash,
            fill_value: FillValue::Float32(0f32),

            codecs: vec![Codec {
                name: "mycodec".to_string(),
                configuration: Some(HashMap::from_iter(iter::once((
                    "foo".to_string(),
                    serde_json::Value::from(42),
                )))),
            }],
            storage_transformers: None,
            dimension_names: None,
        };

        let zarr_meta2 = ZarrArrayMetadata {
            shape: vec![0, 0, 0],
            chunk_shape: ChunkShape(vec![
                NonZeroU64::new(1000).unwrap(),
                NonZeroU64::new(1000).unwrap(),
                NonZeroU64::new(1000).unwrap(),
            ]),
            ..zarr_meta1.clone()
        };

        let coord1 = ChunkIndices(vec![9, 10, 9]);
        let coord2 = ChunkIndices(vec![10, 11, 10]);
        let coord3 = ChunkIndices(vec![0, 0, 0]);

        assert!(zarr_meta1.valid_chunk_coord(&coord1));
        assert!(!zarr_meta1.valid_chunk_coord(&coord2));

        assert!(zarr_meta2.valid_chunk_coord(&coord3));
    }
}
