use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use aws_sdk_s3::Client;
use bytes::Bytes;
use object_store::{
    local::LocalFileSystem, path::Path, GetOptions, GetRange, ObjectStore,
};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use url::Url;

use crate::{
    format::{manifest::VirtualReferenceError, ByteRange},
    private,
    storage::s3::{mk_client, range_to_header, S3Config, S3Credentials},
};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum ObjectStorePlatform {
    // FIXME: official names
    S3,
    GoogleCloudStorage,
    Azure,
    Tigris,
    LocalFileSystem,
}

type ContainerName = String;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct VirtualChunkContainer {
    pub name: ContainerName,
    pub object_store: ObjectStorePlatform,
    pub region: Option<String>,
    pub prefix: String,
    pub endpoint_url: Option<String>,
    pub anonymous: bool,
    pub allow_http: bool,
}

pub fn mk_default_containers() -> Vec<VirtualChunkContainer> {
    vec![
        VirtualChunkContainer {
            name: "s3".to_string(),
            object_store: ObjectStorePlatform::S3,
            region: None,
            prefix: "s3:".to_string(),
            endpoint_url: None,
            anonymous: false,
            allow_http: false,
        },
        VirtualChunkContainer {
            name: "gs".to_string(),
            object_store: ObjectStorePlatform::GoogleCloudStorage,
            region: None,
            prefix: "gs:".to_string(),
            endpoint_url: None,
            anonymous: false,
            allow_http: false,
        },
        VirtualChunkContainer {
            name: "azure".to_string(),
            object_store: ObjectStorePlatform::Azure,
            region: None,
            prefix: "az:".to_string(), //FIXME: prototype
            endpoint_url: None,
            anonymous: false,
            allow_http: false,
        },
        VirtualChunkContainer {
            name: "tigris".to_string(),
            object_store: ObjectStorePlatform::Tigris,
            region: None,
            prefix: "tigris:".to_string(),
            endpoint_url: None,
            anonymous: true,
            allow_http: false,
        },
        VirtualChunkContainer {
            name: "file".to_string(),
            object_store: ObjectStorePlatform::LocalFileSystem,
            region: None,
            prefix: "file:".to_string(),
            endpoint_url: None,
            anonymous: true,
            allow_http: false,
        },
    ]
}

#[async_trait]
pub trait ChunkFetcher: std::fmt::Debug + private::Sealed + Send + Sync {
    async fn fetch_chunk(
        &self,
        chunk_location: &str,
        range: &ByteRange,
    ) -> Result<Bytes, VirtualReferenceError>;
}

/// Sort containers in reverse order of prefix length
pub(crate) fn sort_containers(containers: &mut [VirtualChunkContainer]) {
    containers.sort_by_key(|cont| -(cont.prefix.len() as i64));
}

fn find_container<'a>(
    chunk_location: &str,
    containers: &'a [VirtualChunkContainer],
) -> Option<&'a VirtualChunkContainer> {
    containers.iter().find(|cont| chunk_location.starts_with(cont.prefix.as_str()))
}

#[derive(Debug)]
pub struct VirtualChunkResolver {
    containers: Vec<VirtualChunkContainer>,
    fetchers: RwLock<HashMap<ContainerName, Arc<dyn ChunkFetcher>>>,
}

impl VirtualChunkResolver {
    pub fn new(containers: Vec<VirtualChunkContainer>) -> Self {
        VirtualChunkResolver { containers, fetchers: RwLock::new(HashMap::new()) }
    }

    pub async fn get_fetcher(
        &self,
        chunk_location: &str,
    ) -> Result<Arc<dyn ChunkFetcher>, VirtualReferenceError> {
        let cont = find_container(chunk_location, &self.containers).ok_or_else(|| {
            VirtualReferenceError::NoContainerForUrl(chunk_location.to_string())
        })?;

        // Many tasks will be fetching chunks at the same time, so it's important
        // that we don't lock the cache more than we absolutely need to. For this reason
        // we optimistically lock for reads and try to find an existing fetcher.
        // If there is no fetcher, we drop the read lock and lock for writes, then we can create
        // the new fetcher. But first we need to check for the possibility of somebody else
        // creating the fetcher between unlocking for reads and locking for writes.
        // The end result is not ideal, because every reader needs to wait the whole time while a
        // new fetcher is created (which could be expensive). But we don't expect frequent creation
        // of fetchers. Better code could could iprove on this.
        let fetchers = self.fetchers.read().await;
        match fetchers.get(&cont.name).cloned() {
            Some(fetcher) => Ok(fetcher),
            None => {
                drop(fetchers);
                let mut fetchers = self.fetchers.write().await;
                // we need to check again if somebody else created the fetcher before us
                match fetchers.get(&cont.name).cloned() {
                    Some(fetcher) => Ok(fetcher),
                    None => {
                        let fetcher = self.mk_fetcher_for(cont).await?;
                        fetchers.insert(cont.name.clone(), Arc::clone(&fetcher));
                        Ok(fetcher)
                    }
                }
            }
        }
    }

    pub async fn fetch_chunk(
        &self,
        chunk_location: &str,
        range: &ByteRange,
    ) -> Result<Bytes, VirtualReferenceError> {
        let fetcher = self.get_fetcher(chunk_location).await?;
        fetcher.fetch_chunk(chunk_location, range).await
    }

    async fn mk_fetcher_for(
        &self,
        cont: &VirtualChunkContainer,
    ) -> Result<Arc<dyn ChunkFetcher>, VirtualReferenceError> {
        match cont.object_store {
            ObjectStorePlatform::S3 => Ok(Arc::new(S3Fetcher::new(cont).await)),
            ObjectStorePlatform::GoogleCloudStorage => {
                unimplemented!("support for virtual chunks on gcs")
            }
            ObjectStorePlatform::Azure => {
                unimplemented!("support for virtual chunks on gcs")
            }
            ObjectStorePlatform::Tigris => {
                unimplemented!("support for virtual chunks on Tigris")
            }
            ObjectStorePlatform::LocalFileSystem => {
                Ok(Arc::new(LocalFSFetcher::new(cont).await))
            }
        }
    }
}

#[derive(Debug)]
pub struct S3Fetcher {
    client: Arc<Client>,
}

impl S3Fetcher {
    pub async fn new(cont: &VirtualChunkContainer) -> Self {
        let config = S3Config {
            region: cont.region.clone(),
            endpoint: cont.endpoint_url.clone(),
            // FIXME: credentials
            credentials: if cont.anonymous {
                S3Credentials::Anonymous
            } else {
                S3Credentials::FromEnv
            },
            allow_http: cont.allow_http,
        };
        let client = Arc::new(mk_client(Some(&config)).await);
        Self { client }
    }
}

impl private::Sealed for S3Fetcher {}

#[async_trait]
impl ChunkFetcher for S3Fetcher {
    async fn fetch_chunk(
        &self,
        location: &str,
        range: &ByteRange,
    ) -> Result<Bytes, VirtualReferenceError> {
        let url = Url::parse(location).map_err(VirtualReferenceError::CannotParseUrl)?;

        let bucket_name = if let Some(host) = url.host_str() {
            host.to_string()
        } else {
            Err(VirtualReferenceError::CannotParseBucketName(
                "No bucket name found".to_string(),
            ))?
        };

        let key = url.path();
        let key = key.strip_prefix('/').unwrap_or(key);
        let mut b = self.client.get_object().bucket(bucket_name).key(key);

        if let Some(header) = range_to_header(range) {
            b = b.range(header)
        };

        Ok(b.send()
            .await
            .map_err(|e| VirtualReferenceError::FetchError(Box::new(e)))?
            .body
            .collect()
            .await
            .map_err(|e| VirtualReferenceError::FetchError(Box::new(e)))?
            .into_bytes())
    }
}

#[derive(Debug)]
pub struct LocalFSFetcher {
    client: Arc<LocalFileSystem>,
}

impl LocalFSFetcher {
    pub async fn new(_: &VirtualChunkContainer) -> Self {
        Self { client: Arc::new(LocalFileSystem::new()) }
    }
}

impl private::Sealed for LocalFSFetcher {}

#[async_trait]
impl ChunkFetcher for LocalFSFetcher {
    async fn fetch_chunk(
        &self,
        location: &str,
        range: &ByteRange,
    ) -> Result<Bytes, VirtualReferenceError> {
        let url = Url::parse(location).map_err(VirtualReferenceError::CannotParseUrl)?;
        let options =
            GetOptions { range: Option::<GetRange>::from(range), ..Default::default() };
        let path = Path::parse(url.path())
            .map_err(|e| VirtualReferenceError::OtherError(Box::new(e)))?;

        self.client
            .get_opts(&path, options)
            .await
            .map_err(|e| VirtualReferenceError::FetchError(Box::new(e)))?
            .bytes()
            .await
            .map_err(|e| VirtualReferenceError::FetchError(Box::new(e)))
    }
}
