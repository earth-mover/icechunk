use std::sync::Arc;

use bytes::Bytes;
use futures::Stream;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    ArrayIndices, ChunkOffset, Dataset, NodeData, Path, UserAttributes,
    UserAttributesStructure, ZarrArrayMetadata,
};

pub struct Store {
    pub dataset: Arc<Dataset>,
}

type ByteRange = (Option<ChunkOffset>, Option<ChunkOffset>);
type StoreResult<A> = Result<A, StoreError>;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum KeyNotFoundError {
    #[error("chunk cannot be find for key `{key}`")]
    ChunkNotFound { key: String, path: Path, coords: ArrayIndices },
    #[error("node not found at `{path}`")]
    NodeNotFound { path: Path },
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum StoreError {
    #[error("invalid zarr key format `{key}`")]
    InvalidKey { key: String },
    #[error("object not found: `{0}`")]
    NotFound(#[from] KeyNotFoundError),
}

impl Store {
    pub fn new(dataset: Arc<Dataset>) -> Self {
        Store { dataset }
    }

    pub async fn empty(&self) -> StoreResult<bool> {
        Ok(self.dataset.list_nodes().await.next().is_none())
    }

    pub async fn clear(&self) -> StoreResult<()> {
        todo!()
    }

    // TODO: prototype argument
    pub async fn get(&self, key: &str, _byte_range: &ByteRange) -> StoreResult<Bytes> {
        match Key::parse(key)? {
            Key::Metadata { node_path } => self.get_metadata(key, &node_path).await,
            Key::Chunk { node_path, coords } => {
                self.get_chunk(key, node_path, coords).await
            }
        }
    }

    // TODO: prototype argument
    pub async fn get_partial_values(
        &self,
        _key_ranges: impl IntoIterator<Item = (&str, &ByteRange)>,
    ) -> StoreResult<Bytes> {
        todo!()
    }

    // TODO: prototype argument
    pub async fn exists(&self, _key: &str) -> StoreResult<bool> {
        todo!()
    }

    pub fn supports_writes(&self) -> StoreResult<bool> {
        Ok(true)
    }

    pub async fn set(&self, _key: &str, _value: Bytes) -> StoreResult<()> {
        todo!()
    }

    pub async fn delete(&self, _key: &str) -> StoreResult<()> {
        todo!()
    }

    pub fn supports_partial_writes(&self) -> StoreResult<bool> {
        Ok(false)
    }

    pub async fn set_partial_values(
        self,
        _key_start_values: impl IntoIterator<Item = (&str, ChunkOffset, Bytes)>,
    ) -> StoreResult<()> {
        unimplemented!()
    }

    pub fn supports_listing(&self) -> StoreResult<bool> {
        Ok(true)
    }

    pub async fn list(&self) -> StoreResult<impl Stream<Item = StoreResult<String>>> {
        Ok(futures::stream::once(async { todo!() }))
    }

    pub async fn list_prefix(
        &self,
        _prefix: &str,
    ) -> StoreResult<impl Stream<Item = StoreResult<String>>> {
        Ok(futures::stream::once(async { todo!() }))
    }

    pub async fn list_dir(
        &self,
        _prefix: &str,
    ) -> StoreResult<impl Stream<Item = StoreResult<String>>> {
        Ok(futures::stream::once(async { todo!() }))
    }

    async fn get_chunk(
        &self,
        key: &str,
        path: Path,
        coords: ArrayIndices,
    ) -> StoreResult<Bytes> {
        self.dataset.get_chunk(&path, &coords).await.ok_or(StoreError::NotFound(
            KeyNotFoundError::ChunkNotFound { key: key.to_string(), path, coords },
        ))
    }

    async fn get_metadata(&self, _key: &str, path: &Path) -> StoreResult<Bytes> {
        let node = self.dataset.get_node(path).await.map_err(|_| {
            StoreError::NotFound(KeyNotFoundError::NodeNotFound { path: path.clone() })
        })?;
        let user_attributes = match node.user_attributes {
            None => None,
            Some(UserAttributesStructure::Inline(atts)) => Some(atts),
            Some(UserAttributesStructure::Ref(_)) => todo!(),
        };
        match node.node_data {
            NodeData::Group => Ok(GroupMetadata::new(user_attributes).to_bytes()),
            NodeData::Array(zarr_metadata, _) => {
                Ok(ArrayMetadata::new(user_attributes, zarr_metadata).to_bytes())
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Key {
    Metadata { node_path: Path },
    Chunk { node_path: Path, coords: ArrayIndices },
}

impl Key {
    const ROOT_KEY: &'static str = "zarr.json";
    const METADATA_SUFFIX: &'static str = "/zarr.json";
    const CHUNK_COORD_INFIX: &'static str = "c";

    fn parse(key: &str) -> Result<Self, StoreError> {
        fn parse_chunk(key: &str) -> Result<Key, StoreError> {
            if let Some((path, coords)) = key.rsplit_once(Key::CHUNK_COORD_INFIX) {
                if coords.is_empty() {
                    Ok(Key::Chunk {
                        node_path: ["/", path].iter().collect(),
                        coords: ArrayIndices(vec![]),
                    })
                } else {
                    coords
                        .strip_prefix('/')
                        .ok_or(StoreError::InvalidKey { key: key.to_string() })?
                        .split('/')
                        .map(|s| s.parse::<u64>())
                        .collect::<Result<Vec<_>, _>>()
                        .map(|coords| Key::Chunk {
                            node_path: ["/", path].iter().collect(),
                            coords: ArrayIndices(coords),
                        })
                        .map_err(|_| StoreError::InvalidKey { key: key.to_string() })
                }
            } else {
                Err(StoreError::InvalidKey { key: key.to_string() })
            }
        }

        if key == Key::ROOT_KEY {
            Ok(Key::Metadata { node_path: "/".into() })
        } else if key.starts_with('/') {
            Err(StoreError::InvalidKey { key: key.to_string() })
        } else if let Some(path) = key.strip_suffix(Key::METADATA_SUFFIX) {
            // we need to be careful indexing into utf8 strings
            Ok(Key::Metadata { node_path: ["/", path].iter().collect() })
        } else {
            parse_chunk(key)
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct ArrayMetadata {
    zarr_format: u8,
    node_type: String,
    attributes: Option<UserAttributes>,
    #[serde(flatten)]
    zarr_metadata: ZarrArrayMetadata,
}

#[derive(Debug, Serialize, Deserialize)]
struct GroupMetadata {
    zarr_format: u8,
    node_type: String,
    attributes: Option<UserAttributes>,
}

impl ArrayMetadata {
    fn new(attributes: Option<UserAttributes>, zarr_metadata: ZarrArrayMetadata) -> Self {
        Self { zarr_format: 3, node_type: "array".to_string(), attributes, zarr_metadata }
    }

    fn to_bytes(&self) -> Bytes {
        Bytes::from_iter(
            // We can unpack because it comes from controlled datastructures that can be serialized
            serde_json::to_vec(self).expect("bug in ArrayMetadata serialization"),
        )
    }
}

impl GroupMetadata {
    fn new(attributes: Option<UserAttributes>) -> Self {
        Self { zarr_format: 3, node_type: "group".to_string(), attributes }
    }

    fn to_bytes(&self) -> Bytes {
        Bytes::from_iter(
            // We can unpack because it comes from controlled datastructures that can be serialized
            serde_json::to_vec(self).expect("bug in GroupMetadata serialization"),
        )
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, iter, num::NonZeroU64};

    use crate::{
        storage::InMemoryStorage, ChunkKeyEncoding, ChunkShape, Codec, DataType,
        FillValue, Storage, StorageTransformer,
    };

    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn test_parse_key() {
        assert_eq!(Key::parse("zarr.json"), Ok(Key::Metadata { node_path: "/".into() }));
        assert_eq!(
            Key::parse("a/zarr.json"),
            Ok(Key::Metadata { node_path: "/a".into() })
        );
        assert_eq!(
            Key::parse("a/b/c/zarr.json"),
            Ok(Key::Metadata { node_path: "/a/b/c".into() })
        );
        assert_eq!(
            Key::parse("foo/c"),
            Ok(Key::Chunk { node_path: "/foo".into(), coords: ArrayIndices(vec![]) })
        );
        assert_eq!(
            Key::parse("foo/bar/c"),
            Ok(Key::Chunk { node_path: "/foo/bar".into(), coords: ArrayIndices(vec![]) })
        );
        assert_eq!(
            Key::parse("foo/c/1/2/3"),
            Ok(Key::Chunk {
                node_path: "/foo".into(),
                coords: ArrayIndices(vec![1, 2, 3])
            })
        );
        assert_eq!(
            Key::parse("foo/bar/baz/c/1/2/3"),
            Ok(Key::Chunk {
                node_path: "/foo/bar/baz".into(),
                coords: ArrayIndices(vec![1, 2, 3])
            })
        );
        assert_eq!(
            Key::parse("c"),
            Ok(Key::Chunk { node_path: "/".into(), coords: ArrayIndices(vec![]) })
        );
    }

    #[tokio::test]
    async fn test_metadata_get() -> Result<(), Box<dyn std::error::Error>> {
        // TODO: turn this test into pure Store operations once we support writes through Zarr
        let storage: Arc<dyn Storage + Send + Sync> = Arc::new(InMemoryStorage::new());
        let ds = Arc::new(Dataset::create(Arc::clone(&storage)));
        let store = Store { dataset: Arc::clone(&ds) };

        assert_eq!(
            store.get("zarr.json", &(None, None)).await,
            Err(StoreError::NotFound(KeyNotFoundError::NodeNotFound {
                path: "/".into()
            }))
        );

        let mut ds = Dataset::create(Arc::clone(&storage));
        ds.add_group("/".into()).await?;
        let store = Store { dataset: Arc::clone(&Arc::new(ds)) };
        assert_eq!(
            store.get("zarr.json", &(None, None)).await,
            Ok(Bytes::copy_from_slice(
                br#"{"zarr_format":3,"node_type":"group","attributes":null}"#
            ))
        );

        let mut ds = Dataset::create(Arc::clone(&storage));
        ds.add_group("/a/b".into()).await?;
        ds.set_user_attributes(
            "/a/b".into(),
            Some(UserAttributes::try_new(br#"{"foo": 42}"#).unwrap()),
        )
        .await?;
        let store = Store { dataset: Arc::clone(&Arc::new(ds)) };
        assert_eq!(
            store.get("a/b/zarr.json", &(None, None)).await,
            Ok(Bytes::copy_from_slice(
                br#"{"zarr_format":3,"node_type":"group","attributes":{"foo":42}}"#
            ))
        );

        let zarr_meta = ZarrArrayMetadata {
            shape: vec![2, 2, 2],
            data_type: DataType::Int32,
            chunk_shape: ChunkShape(vec![
                NonZeroU64::new(1).unwrap(),
                NonZeroU64::new(1).unwrap(),
                NonZeroU64::new(1).unwrap(),
            ]),
            chunk_key_encoding: ChunkKeyEncoding::Slash,
            fill_value: FillValue::Int32(0),
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
                    "bar".to_string(),
                    serde_json::Value::from(43),
                )))),
            }]),
            dimension_names: Some(vec![
                Some("x".to_string()),
                Some("y".to_string()),
                Some("t".to_string()),
            ]),
        };
        let mut ds = Dataset::create(Arc::clone(&storage));
        ds.add_group("/a/b".into()).await?;
        ds.add_array("/a/b/array".into(), zarr_meta.clone()).await?;
        ds.set_user_attributes(
            "/a/b/array".into(),
            Some(UserAttributes::try_new(br#"{"foo": 42}"#).unwrap()),
        )
        .await?;
        let store = Store { dataset: Arc::clone(&Arc::new(ds)) };
        assert_eq!(
            store.get("a/b/array/zarr.json", &(None, None)).await,
            Ok(Bytes::copy_from_slice(
                br#"{"zarr_format":3,"node_type":"array","attributes":{"foo":42},"shape":[2,2,2],"data_type":"int32","chunk_grid":{"name":"regular","configuration":{"chunk_shape":[1,1,1]}},"chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},"fill_value":0,"codecs":[{"name":"mycodec","configuration":{"foo":42}}],"storage_transformers":[{"name":"mytransformer","configuration":{"bar":43}}],"dimension_names":["x","y","t"]}"#
            ))
        );

        Ok(())
    }
}
