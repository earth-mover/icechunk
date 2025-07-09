use std::{collections::BTreeMap, convert::Infallible, num::NonZeroU64, sync::Arc};

use bytes::Bytes;
use chrono::{DateTime, Utc};
use err_into::ErrorInto;
use flatbuffers::{FlatBufferBuilder, VerifierOptions};
use itertools::Itertools as _;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::{
    AttributesId, ChunkIndices, IcechunkFormatError, IcechunkFormatErrorKind,
    IcechunkResult, ManifestId, NodeId, Path, SnapshotId,
    flatbuffers::generated,
    manifest::{Manifest, ManifestExtents, ManifestRef},
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DimensionShape {
    dim_length: u64,
    chunk_length: u64,
}

impl DimensionShape {
    pub fn new(array_length: u64, chunk_length: NonZeroU64) -> Self {
        Self { dim_length: array_length, chunk_length: chunk_length.get() }
    }
    pub fn array_length(&self) -> u64 {
        self.dim_length
    }
    pub fn chunk_length(&self) -> u64 {
        self.chunk_length
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArrayShape(Vec<DimensionShape>);

impl ArrayShape {
    pub fn len(&self) -> usize {
        self.0.len()
    }
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn get(&self, ax: usize) -> Option<DimensionShape> {
        if ax > self.len() - 1 { None } else { Some(self.0[ax].clone()) }
    }

    pub fn iter(&self) -> impl Iterator<Item = &DimensionShape> {
        self.0.iter()
    }

    pub fn num_chunks(&self) -> impl Iterator<Item = u32> {
        self.max_chunk_indices_permitted().map(|x| x + 1)
    }

    pub fn new<I>(it: I) -> Option<Self>
    where
        I: IntoIterator<Item = (u64, u64)>,
    {
        let v = it.into_iter().map(|(al, cl)| {
            let cl = NonZeroU64::new(cl)?;
            Some(DimensionShape::new(al, cl))
        });
        v.collect::<Option<Vec<_>>>().map(Self)
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
        coord
            .0
            .iter()
            .zip(self.max_chunk_indices_permitted())
            .all(|(index, index_permitted)| *index <= index_permitted)
    }

    /// Returns an iterator over the maximum permitted chunk indices for the array.
    ///
    /// This function calculates the maximum chunk indices based on the shape of the array
    /// and the chunk shape, using (shape - 1) / chunk_shape. Given integer division is truncating,
    /// this will always result in proper indices at the boundaries.
    fn max_chunk_indices_permitted(&self) -> impl Iterator<Item = u32> + '_ {
        self.0.iter().map(|dim_shape| {
            if dim_shape.chunk_length == 0 || dim_shape.dim_length == 0 {
                0
            } else {
                ((dim_shape.dim_length - 1) / dim_shape.chunk_length) as u32
            }
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DimensionName {
    NotSpecified,
    Name(String),
}

impl From<Option<&str>> for DimensionName {
    fn from(value: Option<&str>) -> Self {
        match value {
            Some(s) => s.into(),
            None => DimensionName::NotSpecified,
        }
    }
}

impl From<DimensionName> for Option<String> {
    fn from(value: DimensionName) -> Option<String> {
        match value {
            DimensionName::NotSpecified => None,
            DimensionName::Name(name) => Some(name),
        }
    }
}

impl From<&str> for DimensionName {
    fn from(value: &str) -> Self {
        if value.is_empty() {
            DimensionName::NotSpecified
        } else {
            DimensionName::Name(value.to_string())
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeData {
    Array {
        shape: ArrayShape,
        dimension_names: Option<Vec<DimensionName>>,
        manifests: Vec<ManifestRef>,
    },
    Group,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub enum NodeType {
    Group,
    Array,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NodeSnapshot {
    pub id: NodeId,
    pub path: Path,
    pub user_data: Bytes,
    pub node_data: NodeData,
}

impl NodeSnapshot {
    pub fn node_type(&self) -> NodeType {
        match &self.node_data {
            NodeData::Group => NodeType::Group,
            NodeData::Array { .. } => NodeType::Array,
        }
    }
}

impl From<&generated::ObjectId8> for NodeId {
    fn from(value: &generated::ObjectId8) -> Self {
        NodeId::new(value.0)
    }
}

impl From<&generated::ObjectId12> for ManifestId {
    fn from(value: &generated::ObjectId12) -> Self {
        ManifestId::new(value.0)
    }
}

impl From<&generated::ObjectId12> for AttributesId {
    fn from(value: &generated::ObjectId12) -> Self {
        AttributesId::new(value.0)
    }
}

impl<'a> From<generated::ManifestRef<'a>> for ManifestRef {
    fn from(value: generated::ManifestRef<'a>) -> Self {
        let from = value.extents().iter().map(|range| range.from()).collect::<Vec<_>>();
        let to = value.extents().iter().map(|range| range.to()).collect::<Vec<_>>();
        let extents = ManifestExtents::new(from.as_slice(), to.as_slice());
        ManifestRef { object_id: value.object_id().into(), extents }
    }
}

impl From<&generated::DimensionShape> for DimensionShape {
    fn from(value: &generated::DimensionShape) -> Self {
        DimensionShape {
            dim_length: value.array_length(),
            chunk_length: value.chunk_length(),
        }
    }
}

impl<'a> From<generated::ArrayNodeData<'a>> for NodeData {
    fn from(value: generated::ArrayNodeData<'a>) -> Self {
        let dimension_names = value
            .dimension_names()
            .map(|dn| dn.iter().map(|name| name.name().into()).collect());
        let shape = ArrayShape(value.shape().iter().map(|dim| dim.into()).collect());
        let manifests = value.manifests().iter().map(|m| m.into()).collect();
        Self::Array { shape, dimension_names, manifests }
    }
}

impl<'a> From<generated::GroupNodeData<'a>> for NodeData {
    fn from(_: generated::GroupNodeData<'a>) -> Self {
        Self::Group
    }
}

impl<'a> TryFrom<generated::NodeSnapshot<'a>> for NodeSnapshot {
    type Error = IcechunkFormatError;

    fn try_from(value: generated::NodeSnapshot<'a>) -> Result<Self, Self::Error> {
        #[allow(clippy::expect_used, clippy::panic)]
        let node_data: NodeData = match value.node_data_type() {
            generated::NodeData::Array => {
                value.node_data_as_array().expect("Bug in flatbuffers library").into()
            }
            generated::NodeData::Group => {
                value.node_data_as_group().expect("Bug in flatbuffers library").into()
            }
            x => panic!("Invalid node data type in flatbuffers file {x:?}"),
        };
        let res = NodeSnapshot {
            id: value.id().into(),
            path: value.path().to_string().try_into()?,
            node_data,
            user_data: Bytes::copy_from_slice(value.user_data().bytes()),
        };
        Ok(res)
    }
}

impl From<&generated::ManifestFileInfo> for ManifestFileInfo {
    fn from(value: &generated::ManifestFileInfo) -> Self {
        Self {
            id: value.id().into(),
            size_bytes: value.size_bytes(),
            num_chunk_refs: value.num_chunk_refs(),
        }
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

#[derive(PartialEq)]
pub struct Snapshot {
    buffer: Vec<u8>,
}

impl std::fmt::Debug for Snapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let nodes =
            self.iter().map(|n| n.map(|n| n.path.to_string())).collect::<Vec<_>>();
        f.debug_struct("Snapshot")
            .field("id", &self.id())
            .field("parent_id", &self.parent_id())
            .field("flushed_at", &self.flushed_at())
            .field("nodes", &nodes)
            .field("manifests", &self.manifest_files().collect::<Vec<_>>())
            .field("message", &self.message())
            .field("metadata", &self.metadata())
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotInfo {
    pub id: SnapshotId,
    pub parent_id: Option<SnapshotId>,
    pub flushed_at: DateTime<chrono::Utc>,
    pub message: String,
    pub metadata: SnapshotProperties,
    pub manifests: Vec<ManifestFileInfo>,
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
            manifests: value.manifest_files().collect(),
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
    pub const INITIAL_SNAPSHOT_ID: SnapshotId = SnapshotId::new([
        0x0b, 0x1c, 0xc8, 0xd6, 0x78, 0x75, 0x80, 0xf0, 0xe3, 0x3a, 0x65,
        0x34, // Decodes as 1CECHNKREP0F1RSTCMT0
    ]);

    pub fn from_buffer(buffer: Vec<u8>) -> IcechunkResult<Snapshot> {
        let _ = flatbuffers::root_with_opts::<generated::Snapshot>(
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
        flushed_at: Option<DateTime<Utc>>,
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
                let id = generated::ObjectId12::new(&mfi.id.0);
                generated::ManifestFileInfo::new(&id, mfi.size_bytes, mfi.num_chunk_refs)
            })
            .collect::<Vec<_>>();
        let manifest_files = builder.create_vector(&manifest_files);

        let metadata_items: Vec<_> = properties
            .unwrap_or_default()
            .iter()
            .map(|(k, v)| {
                let name = builder.create_shared_string(k.as_str());
                let serialized = rmp_serde::to_vec(v).map_err(Box::new)?;
                let value = builder.create_vector(serialized.as_slice());
                Ok::<_, IcechunkFormatError>(generated::MetadataItem::create(
                    &mut builder,
                    &generated::MetadataItemArgs { name: Some(name), value: Some(value) },
                ))
            })
            .try_collect()?;
        let metadata_items = builder.create_vector(metadata_items.as_slice());

        let message = builder.create_string(&message);
        let parent_id = parent_id.map(|oid| generated::ObjectId12::new(&oid.0));
        let flushed_at = flushed_at.unwrap_or_else(Utc::now).timestamp_micros() as u64;
        let id = generated::ObjectId12::new(&id.unwrap_or_else(SnapshotId::random).0);

        let nodes: Vec<_> = sorted_iter
            .into_iter()
            .map(|node| node.err_into().and_then(|node| mk_node(&mut builder, &node)))
            .try_collect()?;
        let nodes = builder.create_vector(&nodes);

        let snap = generated::Snapshot::create(
            &mut builder,
            &generated::SnapshotArgs {
                id: Some(&id),
                parent_id: parent_id.as_ref(),
                nodes: Some(nodes),
                flushed_at,
                message: Some(message),
                metadata: Some(metadata_items),
                manifest_files: Some(manifest_files),
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
            Some(Self::INITIAL_SNAPSHOT_ID),
            None,
            Self::INITIAL_COMMIT_MESSAGE.to_string(),
            Some(properties),
            Default::default(),
            None,
            nodes,
        )
    }

    fn root(&self) -> generated::Snapshot {
        // without the unsafe version this is too slow
        // if we try to keep the root in the Manifest struct, we would need a lifetime
        unsafe { flatbuffers::root_unchecked::<generated::Snapshot>(&self.buffer) }
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
                let value =
                    rmp_serde::from_slice(item.value().bytes()).map_err(Box::new)?;
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
            Some(new_child.flushed_at()?),
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
) -> IcechunkResult<flatbuffers::WIPOffset<generated::NodeSnapshot<'bldr>>> {
    let id = generated::ObjectId8::new(&node.id.0);
    let path = builder.create_string(node.path.to_string().as_str());
    let (node_data_type, node_data) = mk_node_data(builder, &node.node_data)?;
    let user_data = Some(builder.create_vector(&node.user_data));
    Ok(generated::NodeSnapshot::create(
        builder,
        &generated::NodeSnapshotArgs {
            id: Some(&id),
            path: Some(path),
            node_data_type,
            node_data,
            user_data,
        },
    ))
}

fn mk_node_data(
    builder: &mut FlatBufferBuilder<'_>,
    node_data: &NodeData,
) -> IcechunkResult<(
    generated::NodeData,
    Option<flatbuffers::WIPOffset<flatbuffers::UnionWIPOffset>>,
)> {
    match node_data {
        NodeData::Array { manifests, dimension_names, shape } => {
            let manifests = manifests
                .iter()
                .map(|manref| {
                    let object_id = generated::ObjectId12::new(&manref.object_id.0);
                    let extents = manref
                        .extents
                        .iter()
                        .map(|range| {
                            generated::ChunkIndexRange::new(range.start, range.end)
                        })
                        .collect::<Vec<_>>();
                    let extents = builder.create_vector(&extents);
                    generated::ManifestRef::create(
                        builder,
                        &generated::ManifestRefArgs {
                            object_id: Some(&object_id),
                            extents: Some(extents),
                        },
                    )
                })
                .collect::<Vec<_>>();
            let manifests = builder.create_vector(manifests.as_slice());
            let dimensions = dimension_names.as_ref().map(|dn| {
                let names = dn
                    .iter()
                    .map(|n| match n {
                        DimensionName::Name(s) => {
                            let n = builder.create_shared_string(s.as_str());
                            generated::DimensionName::create(
                                builder,
                                &generated::DimensionNameArgs { name: Some(n) },
                            )
                        }
                        DimensionName::NotSpecified => generated::DimensionName::create(
                            builder,
                            &generated::DimensionNameArgs { name: None },
                        ),
                    })
                    .collect::<Vec<_>>();
                builder.create_vector(names.as_slice())
            });
            let shape = shape
                .0
                .iter()
                .map(|ds| generated::DimensionShape::new(ds.dim_length, ds.chunk_length))
                .collect::<Vec<_>>();
            let shape = builder.create_vector(shape.as_slice());
            Ok((
                generated::NodeData::Array,
                Some(
                    generated::ArrayNodeData::create(
                        builder,
                        &generated::ArrayNodeDataArgs {
                            manifests: Some(manifests),
                            shape: Some(shape),
                            dimension_names: dimensions,
                        },
                    )
                    .as_union_value(),
                ),
            ))
        }
        NodeData::Group => Ok((
            generated::NodeData::Group,
            Some(
                generated::GroupNodeData::create(
                    builder,
                    &generated::GroupNodeDataArgs {},
                )
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
    use std::iter::{self};

    #[icechunk_macros::test]
    fn test_get_node() -> Result<(), Box<dyn std::error::Error>> {
        let shape1 = ArrayShape::new(vec![(10u64, 3), (20, 2), (30, 1)]).unwrap();
        let dim_names1 = Some(vec!["x".into(), "y".into(), "t".into()]);

        let shape2 = shape1.clone();
        let dim_names2 = Some(vec![
            DimensionName::NotSpecified,
            DimensionName::NotSpecified,
            "t".into(),
        ]);

        let shape3 = shape1.clone();
        let dim_names3 = None;

        let man_ref1 = ManifestRef {
            object_id: ObjectId::random(),
            extents: ManifestExtents::new(&[0, 0, 0], &[100, 100, 100]),
        };
        let man_ref2 = ManifestRef {
            object_id: ObjectId::random(),
            extents: ManifestExtents::new(&[0, 0, 0], &[100, 100, 100]),
        };

        let node_ids = iter::repeat_with(NodeId::random).take(7).collect::<Vec<_>>();
        // nodes must be sorted by path
        let nodes = vec![
            NodeSnapshot {
                path: Path::root(),
                id: node_ids[0].clone(),
                user_data: Bytes::new(),
                node_data: NodeData::Group,
            },
            NodeSnapshot {
                path: "/a".try_into().unwrap(),
                id: node_ids[1].clone(),
                user_data: Bytes::new(),
                node_data: NodeData::Group,
            },
            NodeSnapshot {
                path: "/array2".try_into().unwrap(),
                id: node_ids[5].clone(),
                user_data: Bytes::new(),
                node_data: NodeData::Array {
                    shape: shape2.clone(),
                    dimension_names: dim_names2.clone(),
                    manifests: vec![],
                },
            },
            NodeSnapshot {
                path: "/b".try_into().unwrap(),
                id: node_ids[2].clone(),
                user_data: Bytes::new(),
                node_data: NodeData::Group,
            },
            NodeSnapshot {
                path: "/b/array1".try_into().unwrap(),
                id: node_ids[4].clone(),
                user_data: Bytes::copy_from_slice(b"hello"),
                node_data: NodeData::Array {
                    shape: shape1.clone(),
                    dimension_names: dim_names1.clone(),
                    manifests: vec![man_ref1.clone(), man_ref2.clone()],
                },
            },
            NodeSnapshot {
                path: "/b/array3".try_into().unwrap(),
                id: node_ids[6].clone(),
                user_data: Bytes::new(),
                node_data: NodeData::Array {
                    shape: shape3.clone(),
                    dimension_names: dim_names3.clone(),
                    manifests: vec![],
                },
            },
            NodeSnapshot {
                path: "/b/c".try_into().unwrap(),
                id: node_ids[3].clone(),
                user_data: Bytes::copy_from_slice(b"bye"),
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
            None,
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
                user_data: Bytes::copy_from_slice(b"bye"),
                node_data: NodeData::Group,
            },
        );
        let node = st.get_node(&Path::root()).unwrap();
        assert_eq!(
            node,
            NodeSnapshot {
                path: Path::root(),
                id: node_ids[0].clone(),
                user_data: Bytes::new(),
                node_data: NodeData::Group,
            },
        );
        let node = st.get_node(&"/b/array1".try_into().unwrap()).unwrap();
        assert_eq!(
            node,
            NodeSnapshot {
                path: "/b/array1".try_into().unwrap(),
                id: node_ids[4].clone(),
                user_data: Bytes::copy_from_slice(b"hello"),
                node_data: NodeData::Array {
                    shape: shape1.clone(),
                    dimension_names: dim_names1.clone(),
                    manifests: vec![man_ref1, man_ref2]
                },
            },
        );
        let node = st.get_node(&"/array2".try_into().unwrap()).unwrap();
        assert_eq!(
            node,
            NodeSnapshot {
                path: "/array2".try_into().unwrap(),
                id: node_ids[5].clone(),
                user_data: Bytes::new(),
                node_data: NodeData::Array {
                    shape: shape2.clone(),
                    dimension_names: dim_names2.clone(),
                    manifests: vec![]
                },
            },
        );
        let node = st.get_node(&"/b/array3".try_into().unwrap()).unwrap();
        assert_eq!(
            node,
            NodeSnapshot {
                path: "/b/array3".try_into().unwrap(),
                id: node_ids[6].clone(),
                user_data: Bytes::new(),
                node_data: NodeData::Array {
                    shape: shape3.clone(),
                    dimension_names: dim_names3.clone(),
                    manifests: vec![]
                },
            },
        );
        Ok(())
    }

    #[icechunk_macros::test]
    fn test_valid_chunk_coord() {
        let shape1 =
            ArrayShape::new(vec![(10_000, 1_000), (10_001, 1_000), (9_999, 1_000)])
                .unwrap();
        let shape2 = ArrayShape::new(vec![(0, 1_000), (0, 1_000), (0, 1_000)]).unwrap();
        let coord1 = ChunkIndices(vec![9, 10, 9]);
        let coord2 = ChunkIndices(vec![10, 11, 10]);
        let coord3 = ChunkIndices(vec![0, 0, 0]);

        assert!(shape1.valid_chunk_coord(&coord1));
        assert!(!shape1.valid_chunk_coord(&coord2));

        assert!(shape2.valid_chunk_coord(&coord3));
    }
}
