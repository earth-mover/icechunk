use std::{iter, num::NonZeroU64, sync::Arc};

use bytes::Bytes;
use futures::Stream;
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, TryFromInto};
use thiserror::Error;
use tokio::spawn;

use crate::{
    AddNodeError, ArrayIndices, ArrayShape, ChunkKeyEncoding, ChunkOffset, ChunkShape,
    Codec, DataType, Dataset, DimensionNames, FillValue, IcechunkFormatError, NodeData,
    Path, StorageTransformer, UpdateNodeError, UserAttributes, UserAttributesStructure,
    ZarrArrayMetadata,
};

pub struct Store {
    dataset: Dataset,
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
    #[error("store method `{0}` is not implemented by Icechunk")]
    Unimplemented(&'static str),
}

impl Store {
    pub fn new(dataset: Dataset) -> Self {
        Store { dataset }
    }

    pub fn dataset(self) -> Dataset {
        self.dataset
    }

    pub async fn empty(&self) -> StoreResult<bool> {
        let res = self.dataset.list_nodes().await.next().is_none();
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
        // We need an Arc here because otherwise we cannot spawn concurrent tasks
        self: Arc<Self>,
        key_ranges: impl IntoIterator<Item = (String, ByteRange)>,
    ) -> StoreResult<Vec<StoreResult<Bytes>>> {
        let mut tasks = Vec::new();
        for (key, range) in key_ranges {
            let this = Arc::clone(&self);
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
                self.dataset.set_chunk(node_path, coords, value).await?;
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
        Err(StoreError::Unimplemented("set_partial_values"))
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
        let chunk = self.dataset.get_chunk(&path, &coords).await;
        chunk.ok_or(StoreError::NotFound(KeyNotFoundError::ChunkNotFound {
            key: key.to_string(),
            path,
            coords,
        }))
    }

    async fn get_metadata(&self, _key: &str, path: &Path) -> StoreResult<Bytes> {
        let node = self.dataset.get_node(path).await.map_err(|_| {
            StoreError::NotFound(KeyNotFoundError::NodeNotFound { path: path.clone() })
        })?;
        let user_attributes = match node.user_attributes {
            None => None,
            Some(UserAttributesStructure::Inline(atts)) => Some(atts),
            // FIXME: implement
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
        &mut self,
        path: Path,
        array_meta: ArrayMetadata,
    ) -> Result<(), StoreError> {
        if self.dataset.get_array(&path).await.is_ok() {
            // TODO: we don't necessarily need to update both
            self.dataset.set_user_attributes(path.clone(), array_meta.attributes).await?;
            self.dataset.update_array(path, array_meta.zarr_metadata).await?;
            Ok(())
        } else {
            self.dataset.add_array(path.clone(), array_meta.zarr_metadata).await?;
            self.dataset.set_user_attributes(path, array_meta.attributes).await?;
            Ok(())
        }
    }

    async fn set_group_meta(
        &mut self,
        path: Path,
        group_meta: GroupMetadata,
    ) -> Result<(), StoreError> {
        if self.dataset.get_group(&path).await.is_ok() {
            self.dataset.set_user_attributes(path, group_meta.attributes).await?;
            Ok(())
        } else {
            self.dataset.add_group(path.clone()).await?;
            self.dataset.set_user_attributes(path, group_meta.attributes).await?;
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

#[serde_as]
#[derive(Debug, Serialize, Deserialize)]
struct ArrayMetadata {
    zarr_format: u8,
    node_type: String,
    attributes: Option<UserAttributes>,
    #[serde(flatten)]
    #[serde_as(as = "TryFromInto<ZarrArrayMetadataSerialzer>")]
    zarr_metadata: ZarrArrayMetadata,
}

#[serde_as]
#[derive(Serialize, Deserialize)]
pub struct ZarrArrayMetadataSerialzer {
    pub shape: ArrayShape,
    pub data_type: DataType,

    #[serde_as(as = "TryFromInto<NameConfigSerializer>")]
    #[serde(rename = "chunk_grid")]
    pub chunk_shape: ChunkShape,

    #[serde_as(as = "TryFromInto<NameConfigSerializer>")]
    pub chunk_key_encoding: ChunkKeyEncoding,
    pub fill_value: serde_json::Value,
    pub codecs: Vec<Codec>,
    pub storage_transformers: Option<Vec<StorageTransformer>>,
    // each dimension name can be null in Zarr
    pub dimension_names: Option<DimensionNames>,
}

impl TryFrom<ZarrArrayMetadataSerialzer> for ZarrArrayMetadata {
    type Error = IcechunkFormatError;

    fn try_from(value: ZarrArrayMetadataSerialzer) -> Result<Self, Self::Error> {
        let ZarrArrayMetadataSerialzer {
            shape,
            data_type,
            chunk_shape,
            chunk_key_encoding,
            fill_value,
            codecs,
            storage_transformers,
            dimension_names,
        } = value;
        {
            let fill_value = FillValue::from_data_type_and_json(&data_type, &fill_value)?;
            Ok(ZarrArrayMetadata {
                fill_value,
                shape,
                data_type,
                chunk_shape,
                chunk_key_encoding,
                codecs,
                storage_transformers,
                dimension_names,
            })
        }
    }
}

impl From<ZarrArrayMetadata> for ZarrArrayMetadataSerialzer {
    fn from(value: ZarrArrayMetadata) -> Self {
        let ZarrArrayMetadata {
            shape,
            data_type,
            chunk_shape,
            chunk_key_encoding,
            fill_value,
            codecs,
            storage_transformers,
            dimension_names,
        } = value;
        {
            let fill_value = serde_json::to_value(fill_value).unwrap();
            ZarrArrayMetadataSerialzer {
                shape,
                data_type,
                chunk_shape,
                chunk_key_encoding,
                codecs,
                storage_transformers,
                dimension_names,
                fill_value,
            }
        }
    }
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

#[derive(Serialize, Deserialize)]
struct NameConfigSerializer {
    name: String,
    configuration: serde_json::Value,
}

impl From<ChunkShape> for NameConfigSerializer {
    fn from(value: ChunkShape) -> Self {
        let arr = serde_json::Value::Array(
            value
                .0
                .iter()
                .map(|v| {
                    serde_json::Value::Number(serde_json::value::Number::from(v.get()))
                })
                .collect(),
        );
        let kvs = serde_json::value::Map::from_iter(iter::once((
            "chunk_shape".to_string(),
            arr,
        )));
        Self {
            name: "regular".to_string(),
            configuration: serde_json::Value::Object(kvs),
        }
    }
}

impl TryFrom<NameConfigSerializer> for ChunkShape {
    type Error = &'static str;

    fn try_from(value: NameConfigSerializer) -> Result<Self, Self::Error> {
        match value {
            NameConfigSerializer {
                name,
                configuration: serde_json::Value::Object(kvs),
            } if name == "regular" => {
                let values = kvs
                    .get("chunk_shape")
                    .and_then(|v| v.as_array())
                    .ok_or("cannot parse ChunkShape")?;
                let shape = values
                    .iter()
                    .map(|v| v.as_u64().and_then(|u64| NonZeroU64::try_from(u64).ok()))
                    .collect::<Option<Vec<_>>>()
                    .ok_or("cannot parse ChunkShape")?;
                Ok(ChunkShape(shape))
            }
            _ => Err("cannot parse ChunkShape"),
        }
    }
}

impl From<ChunkKeyEncoding> for NameConfigSerializer {
    fn from(_value: ChunkKeyEncoding) -> Self {
        let kvs = serde_json::value::Map::from_iter(iter::once((
            "separator".to_string(),
            serde_json::Value::String("/".to_string()),
        )));
        Self {
            name: "default".to_string(),
            configuration: serde_json::Value::Object(kvs),
        }
    }
}

impl TryFrom<NameConfigSerializer> for ChunkKeyEncoding {
    type Error = &'static str;

    fn try_from(value: NameConfigSerializer) -> Result<Self, Self::Error> {
        //FIXME: we are hardcoding / as the separator
        match value {
            NameConfigSerializer {
                name,
                configuration: serde_json::Value::Object(kvs),
            } if name == "default" => {
                if let Some("/") =
                    kvs.get("separator").ok_or("cannot parse ChunkKeyEncoding")?.as_str()
                {
                    Ok(ChunkKeyEncoding::Slash)
                } else {
                    Err("cannot parse ChunkKeyEncoding")
                }
            }
            _ => Err("cannot parse ChunkKeyEncoding"),
        }
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

        let mut ds = store.dataset();
        let oid = ds.flush().await?;

        let ds = Dataset::update(storage, oid);
        let store = Store::new(ds);
        assert_eq!(store.get("array/c/0/1/0", &(None, None)).await.unwrap(), data);

        Ok(())
    }
}
