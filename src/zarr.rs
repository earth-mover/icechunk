use std::sync::Arc;

use bytes::Bytes;
use futures::Stream;
use thiserror::Error;

use crate::{
    ArrayIndices, ChunkOffset, Dataset, NodeData, Path, UserAttributesStructure,
};

pub struct Store {
    dataset: Arc<Dataset>,
}

type ByteRange = (Option<ChunkOffset>, Option<ChunkOffset>);
type StoreResult<A> = Result<A, StoreError>;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum StoreError {
    #[error("invalid zarr key format `{key}`")]
    InvalidKey { key: String },
    #[error("chunk cannot be find for key `{key}`")]
    ChunkNotFound { key: String, path: Path, coords: ArrayIndices },
    #[error("node not found `{path}`")]
    NodeNotFound { path: Path },
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
        self.dataset.get_chunk(&path, &coords).await.ok_or(StoreError::ChunkNotFound {
            key: key.to_string(),
            path,
            coords,
        })
    }

    async fn get_metadata(&self, _key: &str, path: &Path) -> StoreResult<Bytes> {
        // FIXME: handle error

        let node = self
            .dataset
            .get_node(path)
            .await
            .map_err(|_| StoreError::NodeNotFound { path: path.clone() })?;
        let atts = match node.user_attributes {
            None => None,
            Some(UserAttributesStructure::Inline(atts)) => Some(atts),
            Some(UserAttributesStructure::Ref(_)) => todo!(),
        };
        match node.node_data {
            NodeData::Group => Ok(atts.unwrap_or_default()),
            NodeData::Array(_zar_meta, _) => {
                todo!()
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

#[cfg(test)]
mod tests {
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
}
