use std::sync::Arc;

use bytes::Bytes;
use futures::Stream;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::{spawn, sync::RwLock};

use crate::{
    AddNodeError, ArrayIndices, ChunkOffset, Dataset, NodeData, Path, UpdateNodeError,
    UserAttributes, UserAttributesStructure, ZarrArrayMetadata,
};

pub struct Store {
    pub dataset: Arc<RwLock<Dataset>>,
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

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("invalid zarr key format `{key}`")]
    InvalidKey { key: String },
    #[error("object not found: `{0}`")]
    NotFound(#[from] KeyNotFoundError),
    #[error("cannot update object: `{0}`")]
    CannotUpdate(#[from] UpdateNodeError),
    #[error("bad metadata: `{0}`")]
    BadMetadata(#[from] serde_json::Error),
    #[error("add node error: `{0}`")]
    AddNode(#[from] AddNodeError),
}

impl Store {
    pub fn new(dataset: Dataset) -> Self {
        Store { dataset: Arc::new(RwLock::new(dataset)) }
    }

    pub async fn empty(&self) -> StoreResult<bool> {
        let res = self.dataset.read().await.list_nodes().await.next().is_none();
        Ok(res)
    }

    pub async fn clear(&mut self) -> StoreResult<()> {
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
        // TODO: calling this argument self gives a compiler error for some reason
        this: Arc<Self>,
        key_ranges: impl IntoIterator<Item = (String, ByteRange)>,
    ) -> StoreResult<Vec<StoreResult<Bytes>>> {
        let mut tasks = Vec::new();
        for (key, range) in key_ranges {
            let this = Arc::clone(&this);
            tasks.push(spawn(async move { this.get(&key, &range).await }));
        }
        let mut outputs = Vec::with_capacity(tasks.len());
        for task in tasks {
            outputs.push(task.await.unwrap());
        }
        Ok(outputs)
    }

    // TODO: prototype argument
    pub async fn exists(&self, key: &str) -> StoreResult<bool> {
        match self.get(key, &(None, None)).await {
            Ok(_) => Ok(true),
            Err(StoreError::NotFound(_)) => Ok(false),
            Err(other_error) => Err(other_error),
        }
    }

    pub fn supports_writes(&self) -> StoreResult<bool> {
        Ok(true)
    }

    pub async fn set(&mut self, key: &str, value: Bytes) -> StoreResult<()> {
        match Key::parse(key)? {
            Key::Metadata { node_path } => {
                if let Ok(array_meta) = serde_json::from_slice(value.as_ref()) {
                    self.set_array_meta(node_path, array_meta).await
                } else {
                    match serde_json::from_slice(value.as_ref()) {
                        Ok(group_meta) => {
                            self.set_group_meta(node_path, group_meta).await
                        }
                        Err(err) => Err(StoreError::BadMetadata(err)),
                    }
                }
            }
            Key::Chunk { ref node_path, ref coords } => {
                self.dataset.write().await.set_chunk(node_path, coords, value).await?;
                Ok(())
            }
        }
    }

    pub async fn delete(&mut self, _key: &str) -> StoreResult<()> {
        todo!()
    }

    pub fn supports_partial_writes(&self) -> StoreResult<bool> {
        Ok(false)
    }

    pub async fn set_partial_values(
        &mut self,
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
        let chunk = self.dataset.read().await.get_chunk(&path, &coords).await;
        chunk.ok_or(StoreError::NotFound(KeyNotFoundError::ChunkNotFound {
            key: key.to_string(),
            path,
            coords,
        }))
    }

    async fn get_metadata(&self, _key: &str, path: &Path) -> StoreResult<Bytes> {
        let node = self.dataset.read().await.get_node(path).await.map_err(|_| {
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

    async fn set_array_meta(
        &self,
        path: Path,
        array_meta: ArrayMetadata,
    ) -> Result<(), StoreError> {
        if self.dataset.read().await.get_array(&path).await.is_ok() {
            let mut ds = self.dataset.write().await;
            // TODO: we don't necessarily need to update both
            ds.set_user_attributes(path.clone(), array_meta.attributes).await?;
            ds.update_array(path, array_meta.zarr_metadata).await?;
            Ok(())
        } else {
            let mut ds = self.dataset.write().await;
            ds.add_array(path.clone(), array_meta.zarr_metadata).await?;
            ds.set_user_attributes(path, array_meta.attributes).await?;
            Ok(())
        }
    }

    async fn set_group_meta(
        &self,
        path: Path,
        group_meta: GroupMetadata,
    ) -> Result<(), StoreError> {
        if self.dataset.read().await.get_group(&path).await.is_ok() {
            let mut ds = self.dataset.write().await;
            ds.set_user_attributes(path, group_meta.attributes).await?;
            Ok(())
        } else {
            let mut ds = self.dataset.write().await;
            ds.add_group(path.clone()).await?;
            ds.set_user_attributes(path, group_meta.attributes).await?;
            Ok(())
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
    const CHUNK_COORD_INFIX: &'static str = "/c";

    fn parse(key: &str) -> Result<Self, StoreError> {
        fn parse_chunk(key: &str) -> Result<Key, StoreError> {
            if key == "c" {
                return Ok(Key::Chunk {
                    node_path: "/".into(),
                    coords: ArrayIndices(vec![]),
                });
            }
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

    use crate::{storage::InMemoryStorage, Storage};

    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn test_parse_key() {
        assert!(matches!(
            Key::parse("zarr.json"),
            Ok(Key::Metadata { node_path}) if node_path.to_str() == Some("/")
        ));
        assert!(matches!(
            Key::parse("a/zarr.json"),
            Ok(Key::Metadata { node_path }) if node_path.to_str() == Some("/a")
        ));
        assert!(matches!(
            Key::parse("a/b/c/zarr.json"),
            Ok(Key::Metadata { node_path }) if node_path.to_str() == Some("/a/b/c")
        ));
        assert!(matches!(
            Key::parse("foo/c"),
            Ok(Key::Chunk { node_path, coords }) if node_path.to_str() == Some("/foo") && coords == ArrayIndices(vec![])
        ));
        assert!(matches!(
            Key::parse("foo/bar/c"),
            Ok(Key::Chunk { node_path, coords}) if node_path.to_str() == Some("/foo/bar") && coords == ArrayIndices(vec![])
        ));
        assert!(matches!(
            Key::parse("foo/c/1/2/3"),
            Ok(Key::Chunk {
                node_path,
                coords,
            }) if node_path.to_str() == Some("/foo") && coords == ArrayIndices(vec![1,2,3])
        ));
        assert!(matches!(
            Key::parse("foo/bar/baz/c/1/2/3"),
            Ok(Key::Chunk {
                node_path,
                coords,
            }) if node_path.to_str() == Some("/foo/bar/baz") && coords == ArrayIndices(vec![1,2,3])
        ));
        assert!(matches!(
            Key::parse("c"),
            Ok(Key::Chunk { node_path, coords}) if node_path.to_str() == Some("/") && coords == ArrayIndices(vec![])
        ));
    }

    #[tokio::test]
    async fn test_metadata_set_and_get() -> Result<(), Box<dyn std::error::Error>> {
        // TODO: turn this test into pure Store operations once we support writes through Zarr
        let storage: Arc<dyn Storage + Send + Sync> = Arc::new(InMemoryStorage::new());
        let ds = Dataset::create(Arc::clone(&storage));
        let mut store = Store::new(ds);

        assert!(matches!(
            store.get("zarr.json", &(None, None)).await,
            Err(StoreError::NotFound(KeyNotFoundError::NodeNotFound {path})) if path.to_str() == Some("/")
        ));

        store
            .set(
                "zarr.json",
                Bytes::copy_from_slice(br#"{"zarr_format":3, "node_type":"group"}"#),
            )
            .await?;
        assert_eq!(
            store.get("zarr.json", &(None, None)).await.unwrap(),
            Bytes::copy_from_slice(
                br#"{"zarr_format":3,"node_type":"group","attributes":null}"#
            )
        );

        store.set("a/b/zarr.json", Bytes::copy_from_slice(br#"{"zarr_format":3, "node_type":"group", "attributes": {"spam":"ham", "eggs":42}}"#)).await?;
        assert_eq!(
            store.get("a/b/zarr.json", &(None, None)).await.unwrap(),
            Bytes::copy_from_slice(
                br#"{"zarr_format":3,"node_type":"group","attributes":{"eggs":42,"spam":"ham"}}"#
            )
        );

        let zarr_meta = Bytes::copy_from_slice(br#"{"zarr_format":3,"node_type":"array","attributes":{"foo":42},"shape":[2,2,2],"data_type":"int32","chunk_grid":{"name":"regular","configuration":{"chunk_shape":[1,1,1]}},"chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},"fill_value":0,"codecs":[{"name":"mycodec","configuration":{"foo":42}}],"storage_transformers":[{"name":"mytransformer","configuration":{"bar":43}}],"dimension_names":["x","y","t"]}"#);
        store.set("a/b/array/zarr.json", zarr_meta.clone()).await?;
        assert_eq!(
            store.get("a/b/array/zarr.json", &(None, None)).await.unwrap(),
            zarr_meta.clone()
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_chunk_set_and_get() -> Result<(), Box<dyn std::error::Error>> {
        // TODO: turn this test into pure Store operations once we support writes through Zarr
        let in_mem_storage = Arc::new(InMemoryStorage::new());
        let storage =
            Arc::clone(&(in_mem_storage.clone() as Arc<dyn Storage + Send + Sync>));
        let ds = Dataset::create(Arc::clone(&storage));
        let mut store = Store::new(ds);

        store
            .set(
                "zarr.json",
                Bytes::copy_from_slice(br#"{"zarr_format":3, "node_type":"group"}"#),
            )
            .await?;
        let zarr_meta = Bytes::copy_from_slice(br#"{"zarr_format":3,"node_type":"array","attributes":{"foo":42},"shape":[2,2,2],"data_type":"int32","chunk_grid":{"name":"regular","configuration":{"chunk_shape":[1,1,1]}},"chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},"fill_value":0,"codecs":[{"name":"mycodec","configuration":{"foo":42}}],"storage_transformers":[{"name":"mytransformer","configuration":{"bar":43}}],"dimension_names":["x","y","t"]}"#);
        store.set("array/zarr.json", zarr_meta.clone()).await?;

        let data = Bytes::copy_from_slice(b"hello");
        store.set("array/c/0/1/0", data.clone()).await?;
        assert_eq!(store.get("array/c/0/1/0", &(None, None)).await.unwrap(), data);

        let chunk_id = in_mem_storage.chunk_ids().iter().next().cloned().unwrap();
        assert_eq!(in_mem_storage.fetch_chunk(&chunk_id, &None).await?, data);

        Ok(())
    }
}
