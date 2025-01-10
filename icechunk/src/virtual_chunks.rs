use std::{collections::HashMap, ops::Range, path::PathBuf, sync::Arc};

use async_trait::async_trait;
use aws_sdk_s3::{error::SdkError, operation::get_object::GetObjectError, Client};
use bytes::Bytes;
use object_store::{local::LocalFileSystem, path::Path, GetOptions, ObjectStore};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use url::Url;

use crate::{
    config::{Credentials, S3Credentials, S3Options},
    format::{
        manifest::{Checksum, SecondsSinceEpoch, VirtualReferenceError},
        ChunkOffset,
    },
    private,
    storage::s3::{mk_client, range_to_header},
    ObjectStoreConfig,
};

pub type ContainerName = String;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct VirtualChunkContainer {
    pub name: ContainerName,
    pub url_prefix: String,
    pub store: ObjectStoreConfig,
}

pub type VirtualChunkCredentialsError = String;

impl VirtualChunkContainer {
    pub fn validate_credentials(
        &self,
        cred: &Credentials,
    ) -> Result<(), VirtualChunkCredentialsError> {
        match (&self.store, cred) {
            (ObjectStoreConfig::InMemory, _) => {
                Err("in memory storage does not accept credentials".to_string())
            }
            (ObjectStoreConfig::LocalFileSystem(..), _) => {
                Err("in memory storage does not accept credentials".to_string())
            }
            (ObjectStoreConfig::S3Compatible(_), Credentials::S3(_)) => Ok(()),
            (ObjectStoreConfig::S3(_), Credentials::S3(_)) => Ok(()),
            (ObjectStoreConfig::Gcs(_), Credentials::Gcs(_)) => Ok(()),
            (ObjectStoreConfig::Azure(_), Credentials::Azure(_)) => Ok(()),
            (ObjectStoreConfig::Tigris {}, _) => Ok(()), // TODO
            _ => Err("credentials do not match store type".to_string()),
        }
    }
}

pub fn mk_default_containers() -> HashMap<ContainerName, VirtualChunkContainer> {
    [
        (
            "s3".to_string(),
            VirtualChunkContainer {
                name: "s3".to_string(),
                url_prefix: "s3".to_string(),
                store: ObjectStoreConfig::S3(S3Options {
                    region: None,
                    endpoint_url: None,
                    anonymous: false,
                    allow_http: false,
                }),
            },
        ),
        (
            "gcs".to_string(),
            VirtualChunkContainer {
                name: "gcs".to_string(),
                url_prefix: "gcs".to_string(),
                store: ObjectStoreConfig::Gcs(HashMap::new()),
            },
        ),
        (
            "az".to_string(),
            VirtualChunkContainer {
                name: "az".to_string(),
                url_prefix: "az".to_string(),
                store: ObjectStoreConfig::Azure(HashMap::new()),
            },
        ),
        (
            "tigris".to_string(),
            VirtualChunkContainer {
                name: "tigris".to_string(),
                url_prefix: "tigris".to_string(),
                store: ObjectStoreConfig::Tigris {},
            },
        ),
        (
            "file".to_string(),
            VirtualChunkContainer {
                name: "file".to_string(),
                url_prefix: "file".to_string(),
                store: ObjectStoreConfig::LocalFileSystem(PathBuf::new()),
            },
        ),
    ]
    .into()
}

#[async_trait]
pub trait ChunkFetcher: std::fmt::Debug + private::Sealed + Send + Sync {
    async fn fetch_chunk(
        &self,
        chunk_location: &str,
        range: &Range<ChunkOffset>,
        checksum: Option<&Checksum>,
    ) -> Result<Bytes, VirtualReferenceError>;
}

/// Sort containers in reverse order of prefix length
pub(crate) fn sort_containers(containers: &mut [VirtualChunkContainer]) {
    containers.sort_by_key(|cont| -(cont.url_prefix.len() as i64));
}

fn find_container<'a>(
    chunk_location: &str,
    containers: &'a [VirtualChunkContainer],
) -> Option<&'a VirtualChunkContainer> {
    containers.iter().find(|cont| chunk_location.starts_with(cont.url_prefix.as_str()))
}

#[derive(Debug, Serialize, Deserialize)]
pub struct VirtualChunkResolver {
    containers: Vec<VirtualChunkContainer>,
    credentials: HashMap<ContainerName, Credentials>,
    #[serde(skip)]
    fetchers: RwLock<HashMap<ContainerName, Arc<dyn ChunkFetcher>>>,
}

impl VirtualChunkResolver {
    pub fn new(
        containers: impl Iterator<Item = VirtualChunkContainer>,
        credentials: HashMap<ContainerName, Credentials>,
    ) -> Self {
        let mut containers = containers.collect::<Vec<_>>();
        sort_containers(&mut containers);
        VirtualChunkResolver {
            containers,
            credentials,
            fetchers: RwLock::new(HashMap::new()),
        }
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
        // of fetchers. Better code could could improve on this.
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
        range: &Range<ChunkOffset>,
        checksum: Option<&Checksum>,
    ) -> Result<Bytes, VirtualReferenceError> {
        let fetcher = self.get_fetcher(chunk_location).await?;
        fetcher.fetch_chunk(chunk_location, range, checksum).await
    }

    async fn mk_fetcher_for(
        &self,
        cont: &VirtualChunkContainer,
    ) -> Result<Arc<dyn ChunkFetcher>, VirtualReferenceError> {
        #[allow(clippy::unimplemented)]
        match &cont.store {
            ObjectStoreConfig::S3(opts) | ObjectStoreConfig::S3Compatible(opts) => {
                let creds = match self.credentials.get(&cont.name) {
                    Some(Credentials::S3(creds)) => creds,
                    Some(_) => {
                        Err(VirtualReferenceError::InvalidCredentials("S3".to_string()))?
                    }
                    None => {
                        if opts.anonymous {
                            &S3Credentials::Anonymous
                        } else {
                            &S3Credentials::FromEnv
                        }
                    }
                };
                Ok(Arc::new(S3Fetcher::new(opts, creds).await))
            }
            // FIXME: implement
            ObjectStoreConfig::Gcs { .. } => {
                unimplemented!("support for virtual chunks on gcs")
            }
            ObjectStoreConfig::Azure { .. } => {
                unimplemented!("support for virtual chunks on gcs")
            }
            ObjectStoreConfig::Tigris { .. } => {
                unimplemented!("support for virtual chunks on Tigris")
            }
            ObjectStoreConfig::LocalFileSystem { .. } => {
                Ok(Arc::new(LocalFSFetcher::new(cont).await))
            }
            ObjectStoreConfig::InMemory {} => {
                unimplemented!("support for virtual chunks on Tigris")
            }
        }
    }
}

#[derive(Debug)]
pub struct S3Fetcher {
    client: Arc<Client>,
}

impl S3Fetcher {
    pub async fn new(opts: &S3Options, credentials: &S3Credentials) -> Self {
        Self { client: Arc::new(mk_client(opts, credentials.clone()).await) }
    }
}

impl private::Sealed for S3Fetcher {}

#[async_trait]
impl ChunkFetcher for S3Fetcher {
    async fn fetch_chunk(
        &self,
        location: &str,
        range: &Range<ChunkOffset>,
        checksum: Option<&Checksum>,
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
        let mut b = self
            .client
            .get_object()
            .bucket(bucket_name)
            .key(key)
            .range(range_to_header(range));

        match checksum {
            Some(Checksum::LastModified(SecondsSinceEpoch(seconds))) => {
                b = b.if_unmodified_since(aws_sdk_s3::primitives::DateTime::from_secs(
                    *seconds as i64,
                ))
            }
            Some(Checksum::ETag(etag)) => {
                b = b.if_match(etag);
            }
            None => {}
        }

        match b.send().await {
            Ok(out) => Ok(out
                .body
                .collect()
                .await
                .map_err(|e| VirtualReferenceError::FetchError(Box::new(e)))?
                .into_bytes()),
            // minio returns this
            Err(SdkError::ServiceError(err)) => {
                if err.err().meta().code() == Some("PreconditionFailed") {
                    Err(VirtualReferenceError::ObjectModified(location.to_string()))
                } else {
                    Err(VirtualReferenceError::FetchError(Box::new(SdkError::<
                        GetObjectError,
                    >::ServiceError(
                        err
                    ))))
                }
            }
            // S3 API documents this
            Err(SdkError::ResponseError(err)) => {
                let status = err.raw().status().as_u16();
                // see https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html#API_PutObject_RequestSyntax
                if status == 409 || status == 412 {
                    Err(VirtualReferenceError::ObjectModified(location.to_string()))
                } else {
                    Err(VirtualReferenceError::FetchError(Box::new(SdkError::<
                        GetObjectError,
                    >::ResponseError(
                        err
                    ))))
                }
            }
            Err(err) => Err(VirtualReferenceError::FetchError(Box::new(err))),
        }
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
        range: &Range<ChunkOffset>,
        checksum: Option<&Checksum>,
    ) -> Result<Bytes, VirtualReferenceError> {
        let url = Url::parse(location).map_err(VirtualReferenceError::CannotParseUrl)?;
        let usize_range = range.start as usize..range.end as usize;
        let mut options =
            GetOptions { range: Some(usize_range.into()), ..Default::default() };

        match checksum {
            Some(Checksum::LastModified(SecondsSinceEpoch(seconds))) => {
                // We can unwrap here because u32 values can always construct a DateTime
                #[allow(clippy::expect_used)]
                let d = chrono::DateTime::from_timestamp(*seconds as i64, 0)
                    .expect("Bad last modified field in virtual chunk reference");
                options.if_unmodified_since = Some(d);
            }
            Some(Checksum::ETag(etag)) => options.if_match = Some(etag.clone()),
            None => {}
        }

        let path = Path::parse(url.path())
            .map_err(|e| VirtualReferenceError::OtherError(Box::new(e)))?;

        match self.client.get_opts(&path, options).await {
            Ok(res) => res
                .bytes()
                .await
                .map_err(|e| VirtualReferenceError::FetchError(Box::new(e))),
            Err(object_store::Error::Precondition { .. }) => {
                Err(VirtualReferenceError::ObjectModified(location.to_string()))
            }
            Err(err) => Err(VirtualReferenceError::FetchError(Box::new(err))),
        }
    }
}
