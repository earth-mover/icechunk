use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    ops::Bound,
    sync::Arc,
};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::metadata::{
    ArrayShape, ChunkKeyEncoding, ChunkShape, Codec, DataType, DimensionNames, FillValue,
    StorageTransformer, UserAttributes,
};

use super::{
    format_constants, manifest::ManifestRef, AttributesId, IcechunkFormatError,
    IcechunkFormatVersion, IcechunkResult, ManifestId, NodeId, ObjectId, Path,
    SnapshotId, TableOffset,
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

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct SnapshotMetadata {
    pub id: SnapshotId,
    pub written_at: DateTime<Utc>,
    pub message: String,
}

pub type SnapshotProperties = HashMap<String, Value>;

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct ManifestFileInfo {
    pub id: ManifestId,
    pub format_version: IcechunkFormatVersion,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct AttributeFileInfo {
    pub id: AttributesId,
    pub format_version: IcechunkFormatVersion,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Snapshot {
    pub icechunk_snapshot_format_version: IcechunkFormatVersion,
    pub icechunk_snapshot_format_flags: BTreeMap<String, rmpv::Value>,

    pub manifest_files: Vec<ManifestFileInfo>,
    pub attribute_files: Vec<AttributeFileInfo>,

    pub total_parents: u32,
    // we denormalize this field to have it easily available in the serialized file
    pub short_term_parents: u16,
    pub short_term_history: VecDeque<SnapshotMetadata>,

    pub metadata: SnapshotMetadata,
    pub started_at: DateTime<Utc>,
    pub properties: SnapshotProperties,
    nodes: BTreeMap<Path, NodeSnapshot>,
}

impl Default for SnapshotMetadata {
    fn default() -> Self {
        Self {
            id: ObjectId::random(),
            written_at: Utc::now(),
            message: Default::default(),
        }
    }
}

impl SnapshotMetadata {
    fn with_message(msg: String) -> Self {
        Self { message: msg, ..Self::default() }
    }
}

impl Snapshot {
    pub const INITIAL_COMMIT_MESSAGE: &'static str = "Repository initialized";

    fn new(
        short_term_history: VecDeque<SnapshotMetadata>,
        total_parents: u32,
        properties: Option<SnapshotProperties>,
        nodes: BTreeMap<Path, NodeSnapshot>,
        manifest_files: Vec<ManifestFileInfo>,
        attribute_files: Vec<AttributeFileInfo>,
    ) -> Self {
        let metadata = SnapshotMetadata::default();
        let short_term_parents = short_term_history.len() as u16;
        let started_at = Utc::now();
        let properties = properties.unwrap_or_default();
        Self {
            icechunk_snapshot_format_version:
                format_constants::LATEST_ICECHUNK_SNAPSHOT_FORMAT,
            icechunk_snapshot_format_flags: Default::default(),
            manifest_files,
            attribute_files,
            total_parents,
            short_term_parents,
            short_term_history,
            metadata,
            started_at,
            properties,
            nodes,
        }
    }

    pub fn from_iter<T: IntoIterator<Item = NodeSnapshot>>(
        parent: &Snapshot,
        properties: Option<SnapshotProperties>,
        manifest_files: Vec<ManifestFileInfo>,
        attribute_files: Vec<AttributeFileInfo>,
        iter: T,
    ) -> Self {
        let nodes = iter.into_iter().map(|node| (node.path.clone(), node)).collect();
        let mut history = parent.short_term_history.clone();
        history.push_front(parent.metadata.clone());

        Self::new(
            history,
            parent.total_parents + 1,
            properties,
            nodes,
            manifest_files,
            attribute_files,
        )
    }

    pub fn empty() -> Self {
        let metadata =
            SnapshotMetadata::with_message(Self::INITIAL_COMMIT_MESSAGE.to_string());
        Self {
            metadata,
            ..Self::new(VecDeque::new(), 0, None, Default::default(), vec![], vec![])
        }
    }

    pub fn get_node(&self, path: &Path) -> IcechunkResult<&NodeSnapshot> {
        self.nodes
            .get(path)
            .ok_or(IcechunkFormatError::NodeNotFound { path: path.clone() })
    }

    pub fn iter(&self) -> impl Iterator<Item = &NodeSnapshot> + '_ {
        self.nodes.values()
    }

    pub fn iter_arc(self: Arc<Self>) -> impl Iterator<Item = NodeSnapshot> {
        NodeIterator { table: self, last_key: None }
    }

    pub fn local_ancestry(self: Arc<Self>) -> impl Iterator<Item = SnapshotMetadata> {
        (0..self.short_term_history.len())
            .map(move |ix| self.short_term_history[ix].clone())
    }

    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

// We need this complex dance because Rust makes it really hard to put together an object and a
// reference to it (in the iterator) in a single self-referential struct
struct NodeIterator {
    table: Arc<Snapshot>,
    last_key: Option<Path>,
}

impl Iterator for NodeIterator {
    type Item = NodeSnapshot;

    fn next(&mut self) -> Option<Self::Item> {
        match &self.last_key {
            None => {
                if let Some((k, v)) = self.table.nodes.first_key_value() {
                    self.last_key = Some(k.clone());
                    Some(v.clone())
                } else {
                    None
                }
            }
            Some(last_key) => {
                if let Some((k, v)) = self
                    .table
                    .nodes
                    .range::<Path, _>((Bound::Excluded(last_key), Bound::Unbounded))
                    .next()
                {
                    self.last_key = Some(k.clone());
                    Some(v.clone())
                } else {
                    None
                }
            }
        }
    }
}

#[cfg(test)]
#[allow(clippy::panic, clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use crate::format::{manifest::ManifestExtents, IcechunkFormatError};

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
            extents: ManifestExtents(vec![]),
        };
        let man_ref2 = ManifestRef {
            object_id: ObjectId::random(),
            extents: ManifestExtents(vec![]),
        };

        let oid = ObjectId::random();
        let node_ids = iter::repeat_with(NodeId::random).take(7).collect::<Vec<_>>();
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
                path: "/b".try_into().unwrap(),
                id: node_ids[2].clone(),
                user_attributes: None,
                node_data: NodeData::Group,
            },
            NodeSnapshot {
                path: "/b/c".try_into().unwrap(),
                id: node_ids[3].clone(),
                user_attributes: Some(UserAttributesSnapshot::Inline(
                    UserAttributes::try_new(br#"{"foo": "some inline"}"#).unwrap(),
                )),
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
                path: "/array2".try_into().unwrap(),
                id: node_ids[5].clone(),
                user_attributes: None,
                node_data: NodeData::Array(zarr_meta2.clone(), vec![]),
            },
            NodeSnapshot {
                path: "/b/array3".try_into().unwrap(),
                id: node_ids[6].clone(),
                user_attributes: None,
                node_data: NodeData::Array(zarr_meta3.clone(), vec![]),
            },
        ];
        let initial = Snapshot::empty();
        let manifests = vec![
            ManifestFileInfo {
                id: man_ref1.object_id.clone(),
                format_version: format_constants::LATEST_ICECHUNK_MANIFEST_FORMAT,
            },
            ManifestFileInfo {
                id: man_ref2.object_id.clone(),
                format_version: format_constants::LATEST_ICECHUNK_MANIFEST_FORMAT,
            },
        ];
        let st = Snapshot::from_iter(&initial, None, manifests, vec![], nodes);

        assert_eq!(
            st.get_node(&"/nonexistent".try_into().unwrap()),
            Err(IcechunkFormatError::NodeNotFound {
                path: "/nonexistent".try_into().unwrap()
            })
        );

        let node = st.get_node(&"/b/c".try_into().unwrap());
        assert_eq!(
            node,
            Ok(&NodeSnapshot {
                path: "/b/c".try_into().unwrap(),
                id: node_ids[3].clone(),
                user_attributes: Some(UserAttributesSnapshot::Inline(
                    UserAttributes::try_new(br#"{"foo": "some inline"}"#).unwrap(),
                )),
                node_data: NodeData::Group,
            }),
        );
        let node = st.get_node(&Path::root());
        assert_eq!(
            node,
            Ok(&NodeSnapshot {
                path: Path::root(),
                id: node_ids[0].clone(),
                user_attributes: None,
                node_data: NodeData::Group,
            }),
        );
        let node = st.get_node(&"/b/array1".try_into().unwrap());
        assert_eq!(
            node,
            Ok(&NodeSnapshot {
                path: "/b/array1".try_into().unwrap(),
                id: node_ids[4].clone(),
                user_attributes: Some(UserAttributesSnapshot::Ref(UserAttributesRef {
                    object_id: oid,
                    location: 42,
                })),
                node_data: NodeData::Array(zarr_meta1.clone(), vec![man_ref1, man_ref2]),
            }),
        );
        let node = st.get_node(&"/array2".try_into().unwrap());
        assert_eq!(
            node,
            Ok(&NodeSnapshot {
                path: "/array2".try_into().unwrap(),
                id: node_ids[5].clone(),
                user_attributes: None,
                node_data: NodeData::Array(zarr_meta2.clone(), vec![]),
            }),
        );
        let node = st.get_node(&"/b/array3".try_into().unwrap());
        assert_eq!(
            node,
            Ok(&NodeSnapshot {
                path: "/b/array3".try_into().unwrap(),
                id: node_ids[6].clone(),
                user_attributes: None,
                node_data: NodeData::Array(zarr_meta3.clone(), vec![]),
            }),
        );
        Ok(())
    }
}
