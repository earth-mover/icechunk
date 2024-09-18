use std::{collections::BTreeMap, mem::size_of, ops::Bound, sync::Arc};

use serde::{Deserialize, Serialize};

use crate::metadata::{
    ArrayShape, ChunkKeyEncoding, ChunkShape, Codec, DataType, DimensionNames, FillValue,
    StorageTransformer, UserAttributes,
};

use super::{
    manifest::ManifestRef, Flags, IcechunkFormatError, IcechunkResult, NodeId, ObjectId,
    Path, TableOffset,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UserAttributesRef {
    pub object_id: ObjectId,
    pub location: TableOffset,
    pub flags: Flags,
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

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct SnapshotTable {
    pub nodes: BTreeMap<Path, NodeSnapshot>,
}

impl SnapshotTable {
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

    pub fn estimated_size_bytes(&self) -> usize {
        // FIXME: this is really bad
        self.nodes.len()
            * (
                size_of::<Path>() //keys
                + size_of::<NodeSnapshot>()  //values
                + 20 * size_of::<char>()  // estimated size of path
        + 200
                // estimated dynamic size of metadata
            )
    }
}

// We need this complex dance because Rust makes it really hard to put together an object and a
// reference to it (in the iterator) in a single self-referential struct
struct NodeIterator {
    table: Arc<SnapshotTable>,
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

impl FromIterator<NodeSnapshot> for SnapshotTable {
    fn from_iter<T: IntoIterator<Item = NodeSnapshot>>(iter: T) -> Self {
        let nodes = iter.into_iter().map(|node| (node.path.clone(), node)).collect();
        SnapshotTable { nodes }
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
            flags: Flags(),
            extents: ManifestExtents(vec![]),
        };
        let man_ref2 = ManifestRef {
            object_id: ObjectId::random(),
            flags: Flags(),
            extents: ManifestExtents(vec![]),
        };

        let oid = ObjectId::random();
        let nodes = vec![
            NodeSnapshot {
                path: "/".into(),
                id: 1,
                user_attributes: None,
                node_data: NodeData::Group,
            },
            NodeSnapshot {
                path: "/a".into(),
                id: 2,
                user_attributes: None,
                node_data: NodeData::Group,
            },
            NodeSnapshot {
                path: "/b".into(),
                id: 3,
                user_attributes: None,
                node_data: NodeData::Group,
            },
            NodeSnapshot {
                path: "/b/c".into(),
                id: 4,
                user_attributes: Some(UserAttributesSnapshot::Inline(
                    UserAttributes::try_new(br#"{"foo": "some inline"}"#).unwrap(),
                )),
                node_data: NodeData::Group,
            },
            NodeSnapshot {
                path: "/b/array1".into(),
                id: 5,
                user_attributes: Some(UserAttributesSnapshot::Ref(UserAttributesRef {
                    object_id: oid.clone(),
                    location: 42,
                    flags: Flags(),
                })),
                node_data: NodeData::Array(
                    zarr_meta1.clone(),
                    vec![man_ref1.clone(), man_ref2.clone()],
                ),
            },
            NodeSnapshot {
                path: "/array2".into(),
                id: 6,
                user_attributes: None,
                node_data: NodeData::Array(zarr_meta2.clone(), vec![]),
            },
            NodeSnapshot {
                path: "/b/array3".into(),
                id: 7,
                user_attributes: None,
                node_data: NodeData::Array(zarr_meta3.clone(), vec![]),
            },
        ];
        let st = nodes.into_iter().collect::<SnapshotTable>();
        assert_eq!(
            st.get_node(&"/nonexistent".into()),
            Err(IcechunkFormatError::NodeNotFound { path: "/nonexistent".into() })
        );

        let node = st.get_node(&"/b/c".into());
        assert_eq!(
            node,
            Ok(&NodeSnapshot {
                path: "/b/c".into(),
                id: 4,
                user_attributes: Some(UserAttributesSnapshot::Inline(
                    UserAttributes::try_new(br#"{"foo": "some inline"}"#).unwrap(),
                )),
                node_data: NodeData::Group,
            }),
        );
        let node = st.get_node(&"/".into());
        assert_eq!(
            node,
            Ok(&NodeSnapshot {
                path: "/".into(),
                id: 1,
                user_attributes: None,
                node_data: NodeData::Group,
            }),
        );
        let node = st.get_node(&"/b/array1".into());
        assert_eq!(
            node,
            Ok(&NodeSnapshot {
                path: "/b/array1".into(),
                id: 5,
                user_attributes: Some(UserAttributesSnapshot::Ref(UserAttributesRef {
                    object_id: oid,
                    location: 42,
                    flags: Flags(),
                })),
                node_data: NodeData::Array(zarr_meta1.clone(), vec![man_ref1, man_ref2]),
            }),
        );
        let node = st.get_node(&"/array2".into());
        assert_eq!(
            node,
            Ok(&NodeSnapshot {
                path: "/array2".into(),
                id: 6,
                user_attributes: None,
                node_data: NodeData::Array(zarr_meta2.clone(), vec![]),
            }),
        );
        let node = st.get_node(&"/b/array3".into());
        assert_eq!(
            node,
            Ok(&NodeSnapshot {
                path: "/b/array3".into(),
                id: 7,
                user_attributes: None,
                node_data: NodeData::Array(zarr_meta3.clone(), vec![]),
            }),
        );
        Ok(())
    }
}
