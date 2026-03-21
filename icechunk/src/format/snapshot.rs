//! Repository state at a point in time (arrays, groups, and manifest references).

use std::{collections::BTreeMap, convert::Infallible, ops::Range, sync::Arc};

use bytes::Bytes;
use chrono::{DateTime, Utc};
use err_into::ErrorInto as _;
use flatbuffers::{
    FlatBufferBuilder, ForwardsUOffset, UnionWIPOffset, Vector, VerifierOptions,
    WIPOffset,
};
use itertools::Itertools as _;
use quick_cache::sync::{Cache, GuardResult};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::format::lookup_index_by_key;

use super::{
    AttributesId, ChunkIndices, IcechunkFormatError, IcechunkFormatErrorKind,
    IcechunkResult, ManifestId, NodeId, Path, SnapshotId,
    flatbuffers::generated,
    format_constants::SpecVersionBin,
    manifest::{Manifest, ManifestExtents, ManifestRef},
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DimensionShape {
    dim_length: u64,
    num_chunks: u32,
}

impl DimensionShape {
    pub fn new(array_length: u64, num_chunks: u32) -> Self {
        Self { dim_length: array_length, num_chunks }
    }
    pub fn array_length(&self) -> u64 {
        self.dim_length
    }
    pub fn num_chunks(&self) -> u32 {
        self.num_chunks
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
        self.0.iter().map(|x| x.num_chunks())
    }

    pub fn new<I>(it: I) -> Option<Self>
    where
        I: IntoIterator<Item = (u64, u32)>,
    {
        let v = it.into_iter().map(|(al, nc)| Some(DimensionShape::new(al, nc)));
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
            .zip(self.num_chunks())
            .all(|(index, index_permitted)| *index <= (index_permitted.max(1) - 1))
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
        let extents = ManifestExtents::from_ranges_iter(
            value
                .extents()
                .iter()
                .map(|range| Range { start: range.from(), end: range.to() }),
        );
        ManifestRef { object_id: value.object_id().into(), extents }
    }
}

impl TryFrom<&generated::DimensionShape> for DimensionShape {
    type Error = IcechunkFormatError;

    fn try_from(value: &generated::DimensionShape) -> Result<Self, Self::Error> {
        if value.chunk_length() == 0 && value.array_length() != 0 {
            return Err(IcechunkFormatErrorKind::InvalidArrayMetadata(format!(
                "Array metadata has chunk_length = 0 while array_length={:?}",
                value.array_length()
            ))
            .into());
        }
        let num_chunks = if value.chunk_length() == 0 {
            0
        } else {
            value.array_length().div_ceil(value.chunk_length()) as u32
        };
        Ok(DimensionShape { dim_length: value.array_length(), num_chunks })
    }
}

impl<'a> From<&generated::DimensionShapeV2<'a>> for DimensionShape {
    fn from(value: &generated::DimensionShapeV2<'a>) -> Self {
        DimensionShape {
            dim_length: value.array_length(),
            num_chunks: value.num_chunks(),
        }
    }
}

impl<'a> TryFrom<generated::ArrayNodeData<'a>> for NodeData {
    type Error = IcechunkFormatError;

    fn try_from(value: generated::ArrayNodeData<'a>) -> Result<Self, Self::Error> {
        let dimension_names = value
            .dimension_names()
            .map(|dn| dn.iter().map(|name| name.name().into()).collect());
        // In our flatbuffers the V1 `shape` is required and will be an empty `[]` for V2 repos
        // Note that `shape` is *also* `[]` for scalars in V1 repos.
        // So we branch on `shape_v2` which is optional, and thus None, on V1 repos.
        let shape = ArrayShape(match value.shape_v2() {
            None => value
                .shape()
                .iter()
                .map(|dim| dim.try_into())
                .collect::<Result<Vec<_>, IcechunkFormatError>>()?,
            Some(x) => x.iter().map(|dim| (&dim).into()).collect(),
        });
        let manifests = value.manifests().iter().map(|m| m.into()).collect();
        Ok(Self::Array { shape, dimension_names, manifests })
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
        #[expect(clippy::expect_used, clippy::panic)]
        let node_data: NodeData = match value.node_data_type() {
            generated::NodeData::Array => value
                .node_data_as_array()
                .expect("Bug in flatbuffers library")
                .try_into()?,
            generated::NodeData::Group => {
                value.node_data_as_group().expect("Bug in flatbuffers library").into()
            }
            x => panic!("Invalid node data type in flatbuffers file {x:?}"),
        };
        let res = NodeSnapshot {
            id: value.id().into(),
            path: Path::from_trusted(value.path()),
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

/// Insert a key-value pair into the `__icechunk` namespace object within snapshot properties.
/// Creates the `__icechunk` object if it doesn't exist.
pub fn inject_icechunk_metadata(
    properties: &mut SnapshotProperties,
    key: &str,
    value: Value,
) {
    match properties.get_mut("__icechunk") {
        Some(Value::Object(map)) => {
            map.insert(key.to_string(), value);
        }
        _ => {
            properties
                .insert("__icechunk".to_string(), serde_json::json!({ key: value }));
        }
    }
}

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

// It is _extremely_ common to set/get chunks in large batches sequentially.
// Without a cache we incur the cost of repeatedly decoding the flatbuffer.
// This cache is very small (size 2 currently), because we could cache a large number of
// snapshots. The size of 2 is fine for typically workloads that iterate sequentially
// through nodes.
// We cannot choose 1 because of a bug in the `quick_cache` dependency
// where a size-1 cache is effectively no cache
// https://github.com/arthurprs/quick-cache/issues/105
const SNAPSHOT_NODE_CACHE_SIZE: usize = 2;

pub struct Snapshot {
    buffer: Vec<u8>,
    spec_version: SpecVersionBin,
    node_cache: Cache<Path, Arc<NodeSnapshot>>,
}

impl std::fmt::Debug for Snapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let nodes =
            self.iter().map(|n| n.map(|n| n.path.to_string())).collect::<Vec<_>>();
        #[expect(deprecated)]
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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SnapshotInfo {
    pub id: SnapshotId,
    pub parent_id: Option<SnapshotId>,
    pub flushed_at: DateTime<Utc>,
    pub message: String,
    pub metadata: SnapshotProperties,
}

impl TryFrom<&Snapshot> for SnapshotInfo {
    type Error = IcechunkFormatError;

    fn try_from(value: &Snapshot) -> Result<Self, Self::Error> {
        #[expect(deprecated)]
        Ok(Self {
            id: value.id().clone(),
            parent_id: value.parent_id().clone(),
            flushed_at: value.flushed_at()?,
            message: value.message().clone(),
            metadata: value.metadata()?.clone(),
        })
    }
}

impl SnapshotInfo {
    pub fn is_initial(&self) -> bool {
        self.id == Snapshot::INITIAL_SNAPSHOT_ID
    }
}

impl SnapshotId {
    pub fn is_initial(&self) -> bool {
        *self == Snapshot::INITIAL_SNAPSHOT_ID
    }
}

static ROOT_OPTIONS: VerifierOptions = VerifierOptions {
    max_depth: 64,
    max_tables: 50_000_000,
    max_apparent_size: 1 << 31, // taken from the default
    ignore_missing_null_terminator: true,
};

impl Snapshot {
    pub const INITIAL_COMMIT_MESSAGE: &'static str = "Repository initialized";
    pub const INITIAL_SNAPSHOT_ID: SnapshotId = SnapshotId::new([
        0x0b, 0x1c, 0xc8, 0xd6, 0x78, 0x75, 0x80, 0xf0, 0xe3, 0x3a, 0x65,
        0x34, // Decodes as 1CECHNKREP0F1RSTCMT0
    ]);

    pub fn from_buffer(
        spec_version: SpecVersionBin,
        buffer: Vec<u8>,
    ) -> IcechunkResult<Snapshot> {
        let _ = flatbuffers::root_with_opts::<generated::Snapshot<'_>>(
            &ROOT_OPTIONS,
            buffer.as_slice(),
        )?;
        Ok(Snapshot {
            buffer,
            spec_version,
            // this number is low because we cache a very large number of snapshot nodes.
            node_cache: Cache::new(SNAPSHOT_NODE_CACHE_SIZE),
        })
    }

    pub fn bytes(&self) -> &[u8] {
        self.buffer.as_slice()
    }

    #[expect(clippy::too_many_arguments)]
    pub fn from_iter<E, I>(
        id: Option<SnapshotId>,
        parent_id: Option<SnapshotId>,
        spec_version: SpecVersionBin,
        message: &str,
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
        let mut builder = FlatBufferBuilder::with_capacity(4_096);

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
                let serialized = if spec_version == SpecVersionBin::V1 {
                    rmp_serde::to_vec(v).map_err(Box::new)?
                } else {
                    flexbuffers::to_vec(v).map_err(Box::new)?
                };

                let value = builder.create_vector(serialized.as_slice());
                Ok::<_, IcechunkFormatError>(generated::MetadataItem::create(
                    &mut builder,
                    &generated::MetadataItemArgs { name: Some(name), value: Some(value) },
                ))
            })
            .try_collect()?;
        let metadata_items = builder.create_vector(metadata_items.as_slice());

        let message = builder.create_string(message);
        // Icechunk 2.0 no longer uses this field
        let parent_id = parent_id.map(|oid| generated::ObjectId12::new(&oid.0));
        let flushed_at = flushed_at.unwrap_or_else(Utc::now).timestamp_micros() as u64;
        let id = generated::ObjectId12::new(&id.unwrap_or_else(SnapshotId::random).0);

        let nodes: Vec<_> = sorted_iter
            .into_iter()
            .map(|node| {
                node.err_into()
                    .and_then(|node| mk_node(&mut builder, &node, spec_version))
            })
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
                ..Default::default()
            },
        );

        builder.finish(snap, Some("Ichk"));
        let (mut buffer, offset) = builder.collapse();
        buffer.drain(0..offset);
        buffer.shrink_to_fit();
        Ok(Snapshot {
            buffer,
            spec_version,
            // this number is low because we cache a very large number of snapshot nodes.
            node_cache: Cache::new(SNAPSHOT_NODE_CACHE_SIZE),
        })
    }

    pub fn initial(spec_version: SpecVersionBin) -> IcechunkResult<Self> {
        let mut properties = SnapshotProperties::default();
        inject_icechunk_metadata(&mut properties, "is_root", Value::from(true));
        let nodes: Vec<Result<NodeSnapshot, Infallible>> = Vec::new();
        Self::from_iter(
            Some(Self::INITIAL_SNAPSHOT_ID),
            None,
            spec_version,
            Self::INITIAL_COMMIT_MESSAGE,
            Some(properties),
            Default::default(),
            None,
            nodes,
        )
    }

    #[expect(unsafe_code)]
    fn root(&self) -> generated::Snapshot<'_> {
        // SAFETY: self.buffer was serialized by our own flatbuffers serialization code.
        // We skip validation for performance; a corrupt buffer here indicates
        // file corruption or a bad Icechunk implementation, not a caller error.
        unsafe { flatbuffers::root_unchecked::<generated::Snapshot<'_>>(&self.buffer) }
    }

    pub fn id(&self) -> SnapshotId {
        SnapshotId::new(self.root().id().0)
    }

    #[deprecated(
        since = "2.0.0",
        note = "New versions of icechunk don't use this field and initialize it to None"
    )]
    pub fn parent_id(&self) -> Option<SnapshotId> {
        self.root().parent_id().map(|pid| SnapshotId::new(pid.0))
    }

    pub fn metadata(&self) -> IcechunkResult<SnapshotProperties> {
        self.root()
            .metadata()
            .iter()
            .map(|item| {
                let key = item.name().to_string();
                let value = if self.spec_version == SpecVersionBin::V1 {
                    rmp_serde::from_slice(item.value().bytes()).map_err(Box::new)?
                } else {
                    flexbuffers::from_slice(item.value().bytes()).map_err(Box::new)?
                };
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
    #[deprecated(
        since = "2.0.0",
        note = "Shouldn't be necessary after 2.0, only to support Icechunk 1 repos"
    )]
    pub fn adopt(&self, new_child: &Snapshot) -> IcechunkResult<Self> {
        // Rust flatbuffers implementation doesn't allow mutation of scalars, so we need to
        // create a whole new buffer and write to it in full

        Snapshot::from_iter(
            Some(new_child.id()),
            Some(self.id()),
            SpecVersionBin::V1, // 2.0 doesn't use adopt
            &new_child.message(),
            Some(new_child.metadata()?.clone()),
            new_child.manifest_files().collect(),
            Some(new_child.flushed_at()?),
            new_child.iter(),
        )
    }

    fn _get_node(&self, path: &Path) -> IcechunkResult<NodeSnapshot> {
        let res = self
            .root()
            .nodes()
            .lookup_by_key(path.to_string().as_str(), |node, path| node.path().cmp(path))
            .ok_or(IcechunkFormatError::from(IcechunkFormatErrorKind::NodeNotFound {
                path: path.clone(),
            }))?;
        res.try_into()
    }

    pub fn get_node(&self, path: &Path) -> IcechunkResult<Arc<NodeSnapshot>> {
        use GuardResult::*;
        match self.node_cache.get_value_or_guard(path, None) {
            Value(node) => Ok(node),
            Timeout => Ok(Arc::new(self._get_node(path)?)),
            Guard(guard) => {
                let node = self._get_node(path)?;
                let node = Arc::new(node);
                let _ = guard.insert(Arc::clone(&node));
                Ok(node)
            }
        }
    }

    pub fn get_node_index(&self, path: &Path) -> IcechunkResult<usize> {
        let path_str = path.to_string();
        let res =
            lookup_index_by_key(self.root().nodes(), path_str.as_str(), |node, path| {
                node.path().cmp(path)
            })
            .ok_or(IcechunkFormatError::from(
                IcechunkFormatErrorKind::NodeNotFound { path: path.clone() },
            ))?;
        Ok(res)
    }

    pub fn iter(&self) -> impl Iterator<Item = IcechunkResult<NodeSnapshot>> + '_ {
        self.root().nodes().iter().map(|node| node.try_into().err_into())
    }

    pub fn iter_arc(
        self: Arc<Self>,
        parent_group: &Path,
    ) -> impl Iterator<Item = IcechunkResult<NodeSnapshot>> + use<> {
        NodeIterator::new(self, parent_group)
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
    next_index: usize,
    prefix: String,
}

impl NodeIterator {
    fn new(snapshot: Arc<Snapshot>, parent_group: &Path) -> Self {
        let next_index = snapshot.get_node_index(parent_group).unwrap_or_default();
        let prefix = parent_group.to_string();
        let prefix = if prefix == "/" { String::new() } else { prefix };
        NodeIterator { snapshot, next_index, prefix }
    }
}

impl Iterator for NodeIterator {
    type Item = IcechunkResult<NodeSnapshot>;

    fn next(&mut self) -> Option<Self::Item> {
        let nodes = self.snapshot.root().nodes();
        loop {
            // we need to loop over all nodes starting from the index
            // until we are sure we'll see no more children
            if self.next_index >= nodes.len() {
                return None;
            }

            let node: IcechunkResult<NodeSnapshot> =
                nodes.get(self.next_index).try_into();

            match node {
                Ok(res) => {
                    let node_path = res.path.to_string();
                    if let Some(after_prefix) =
                        node_path.strip_prefix(self.prefix.as_str())
                        && (after_prefix.is_empty() || after_prefix.starts_with('/'))
                    {
                        self.next_index += 1;
                        return Some(Ok(res));
                    } else if node_path.as_str() > self.prefix.as_str()
                        && !node_path.starts_with(self.prefix.as_str())
                    {
                        // We've passed all possible children of the prefix
                        return None;
                    } else {
                        // Not a match but there may be matches later (foo-bar" comes before "foo/bar")
                        self.next_index += 1;
                    }
                }
                Err(err) => return Some(Err(err)),
            }
        }
    }
}

fn mk_node<'bldr>(
    builder: &mut FlatBufferBuilder<'bldr>,
    node: &NodeSnapshot,
    spec_version: SpecVersionBin,
) -> IcechunkResult<WIPOffset<generated::NodeSnapshot<'bldr>>> {
    let id = generated::ObjectId8::new(&node.id.0);
    let path = builder.create_string(node.path.to_string().as_str());
    let (node_data_type, node_data) =
        mk_node_data(builder, &node.node_data, spec_version)?;
    let user_data = Some(builder.create_vector(&node.user_data));
    Ok(generated::NodeSnapshot::create(
        builder,
        &generated::NodeSnapshotArgs {
            id: Some(&id),
            path: Some(path),
            node_data_type,
            node_data,
            user_data,
            ..Default::default()
        },
    ))
}

type ShapeV1<'a> = WIPOffset<Vector<'a, generated::DimensionShape>>;
type ShapeV2<'a> =
    WIPOffset<Vector<'a, ForwardsUOffset<generated::DimensionShapeV2<'a>>>>;

fn mk_array_shapes<'a>(
    builder: &mut FlatBufferBuilder<'a>,
    spec_version: SpecVersionBin,
    shape: &ArrayShape,
) -> (Option<ShapeV1<'a>>, Option<ShapeV2<'a>>) {
    use SpecVersionBin::*;
    match spec_version {
        V1 => {
            let shape = shape
                .0
                .iter()
                .map(|ds| {
                    let chunk_length = if ds.num_chunks == 0 {
                        debug_assert_eq!(ds.dim_length, 0);
                        0
                    } else {
                        // FIXME: this can be *very* wrong, but I'm not sure it really matters
                        //        we still preserve num_chunks, and return the proper Zarr JSON metadata
                        //        which is stored in NodeData::user_data which stores the zarr.json exactly
                        //        as provided by Zarr
                        ds.dim_length.div_ceil(ds.num_chunks as u64)
                    };

                    generated::DimensionShape::new(ds.dim_length, chunk_length)
                })
                .collect::<Vec<_>>();
            (Some(builder.create_vector(shape.as_slice())), None)
        }
        V2 => {
            let shape = shape
                .0
                .iter()
                .map(|ds| {
                    generated::DimensionShapeV2::create(
                        builder,
                        &generated::DimensionShapeV2Args {
                            array_length: ds.dim_length,
                            num_chunks: ds.num_chunks,
                        },
                    )
                })
                .collect::<Vec<_>>();
            let empty_shape_for_v1_compat =
                builder.create_vector(&[] as &[generated::DimensionShape]);
            (
                Some(empty_shape_for_v1_compat),
                Some(builder.create_vector(shape.as_slice())),
            )
        }
    }
}

fn mk_node_data(
    builder: &mut FlatBufferBuilder<'_>,
    node_data: &NodeData,
    spec_version: SpecVersionBin,
) -> IcechunkResult<(generated::NodeData, Option<WIPOffset<UnionWIPOffset>>)> {
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
            let (shape_v1, shape_v2) = mk_array_shapes(builder, spec_version, shape);
            let node_data = generated::ArrayNodeData::create(
                builder,
                &generated::ArrayNodeDataArgs {
                    manifests: Some(manifests),
                    shape: shape_v1,
                    shape_v2,
                    dimension_names: dimensions,
                },
            );
            Ok((generated::NodeData::Array, Some(node_data.as_union_value())))
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
#[allow(unused_qualifications)] // proptest macros generate fully qualified paths
mod tests {
    use crate::format::{IcechunkFormatError, ObjectId};

    use super::*;
    use crate::{
        roundtrip_serialization_tests,
        strategies::{ShapeDim, manifest_file_info, node_snapshot, shapes_and_dims},
    };
    use pretty_assertions::assert_eq;
    use proptest::prelude::*;
    use std::iter::{self};

    roundtrip_serialization_tests!(
        serialize_and_deserialize_node_snapshot - node_snapshot,
        serialize_and_deserialize_manifest_file_info - manifest_file_info
    );

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
            None,
            SpecVersionBin::current(),
            "",
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
            Arc::new(NodeSnapshot {
                path: "/b/c".try_into().unwrap(),
                id: node_ids[3].clone(),
                user_data: Bytes::copy_from_slice(b"bye"),
                node_data: NodeData::Group,
            }),
        );
        let node = st.get_node(&Path::root()).unwrap();
        assert_eq!(
            node,
            Arc::new(NodeSnapshot {
                path: Path::root(),
                id: node_ids[0].clone(),
                user_data: Bytes::new(),
                node_data: NodeData::Group,
            }),
        );
        let node = st.get_node(&"/b/array1".try_into().unwrap()).unwrap();
        assert_eq!(
            node,
            Arc::new(NodeSnapshot {
                path: "/b/array1".try_into().unwrap(),
                id: node_ids[4].clone(),
                user_data: Bytes::copy_from_slice(b"hello"),
                node_data: NodeData::Array {
                    shape: shape1.clone(),
                    dimension_names: dim_names1.clone(),
                    manifests: vec![man_ref1, man_ref2]
                },
            }),
        );
        let node = st.get_node(&"/array2".try_into().unwrap()).unwrap();
        assert_eq!(
            node,
            Arc::new(NodeSnapshot {
                path: "/array2".try_into().unwrap(),
                id: node_ids[5].clone(),
                user_data: Bytes::new(),
                node_data: NodeData::Array {
                    shape: shape2.clone(),
                    dimension_names: dim_names2.clone(),
                    manifests: vec![]
                },
            }),
        );
        let node = st.get_node(&"/b/array3".try_into().unwrap()).unwrap();
        assert_eq!(
            node,
            Arc::new(NodeSnapshot {
                path: "/b/array3".try_into().unwrap(),
                id: node_ids[6].clone(),
                user_data: Bytes::new(),
                node_data: NodeData::Array {
                    shape: shape3.clone(),
                    dimension_names: dim_names3.clone(),
                    manifests: vec![]
                },
            }),
        );
        Ok(())
    }

    #[icechunk_macros::test]
    fn test_valid_chunk_coord() {
        let shape1 =
            ArrayShape::new(vec![(10_000, 10), (10_001, 11), (9_999, 10)]).unwrap();
        let shape2 = ArrayShape::new(vec![(0, 1_000), (0, 1_000), (0, 1_000)]).unwrap();
        let coord1 = ChunkIndices(vec![9, 10, 9]);
        let coord2 = ChunkIndices(vec![10, 11, 10]);
        let coord3 = ChunkIndices(vec![0, 0, 0]);

        assert!(shape1.valid_chunk_coord(&coord1));
        assert!(!shape1.valid_chunk_coord(&coord2));
        assert!(shape2.valid_chunk_coord(&coord3));
    }

    #[icechunk_macros::test]
    fn test_valid_chunk_coord_zero_length_dim() {
        let shape = ArrayShape::new(vec![(0, 0)]).unwrap();
        assert!(shape.valid_chunk_coord(&ChunkIndices(vec![0])));
        assert!(!shape.valid_chunk_coord(&ChunkIndices(vec![1])));
    }

    #[test_strategy::proptest]
    fn test_prop_valid_chunk_coord(
        #[strategy(shapes_and_dims(None, Some(1)))] shape_dim: ShapeDim,
        axis_offset: usize,
    ) {
        let shape = &shape_dim.shape;
        let ndim = shape.len();
        let axis = axis_offset % ndim;

        // origin is always valid
        let origin = ChunkIndices(vec![0; ndim]);
        prop_assert!(shape.valid_chunk_coord(&origin));

        // max valid coord is in bounds
        let max_valid =
            ChunkIndices(shape.num_chunks().map(|nc| nc.saturating_sub(1)).collect());
        prop_assert!(shape.valid_chunk_coord(&max_valid));

        // bumping one axis out of bounds is rejected
        let mut oob = max_valid.0.clone();
        oob[axis] = shape.iter().nth(axis).unwrap().num_chunks().max(1);
        prop_assert!(!shape.valid_chunk_coord(&ChunkIndices(oob)));
    }
}
