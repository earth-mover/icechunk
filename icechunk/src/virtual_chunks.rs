use std::{collections::HashMap, ops::Range, path::PathBuf, sync::Arc};

use async_trait::async_trait;
use aws_sdk_s3::{error::SdkError, operation::get_object::GetObjectError, Client};
use bytes::{Buf, Bytes};
use futures::{stream::FuturesOrdered, TryStreamExt};
use object_store::{local::LocalFileSystem, path::Path, GetOptions, ObjectStore};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use url::Url;

use crate::{
    config::{Credentials, S3Credentials, S3Options},
    format::{
        manifest::{
            Checksum, SecondsSinceEpoch, VirtualReferenceError, VirtualReferenceErrorKind,
        },
        ChunkOffset,
    },
    private,
    storage::{
        self,
        s3::{mk_client, range_to_header},
        split_in_multiple_requests,
    },
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
            (ObjectStoreConfig::Tigris(_), Credentials::S3(_)) => Ok(()),
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
                    force_path_style: false,
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
                store: ObjectStoreConfig::Tigris(S3Options {
                    region: None,
                    endpoint_url: Some("https://fly.storage.tigris.dev".to_string()),
                    anonymous: false,
                    allow_http: false,
                    force_path_style: false,
                }),
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
    settings: storage::Settings,
    #[serde(skip)]
    fetchers: RwLock<HashMap<ContainerName, Arc<dyn ChunkFetcher>>>,
}

impl VirtualChunkResolver {
    pub fn new(
        containers: impl Iterator<Item = VirtualChunkContainer>,
        credentials: HashMap<ContainerName, Credentials>,
        settings: storage::Settings,
    ) -> Self {
        let mut containers = containers.collect::<Vec<_>>();
        sort_containers(&mut containers);
        VirtualChunkResolver {
            containers,
            credentials,
            settings,
            fetchers: RwLock::new(HashMap::new()),
        }
    }

    pub fn matching_container(
        &self,
        chunk_location: &str,
    ) -> Option<&VirtualChunkContainer> {
        find_container(chunk_location, self.containers.as_ref())
    }

    pub async fn get_fetcher(
        &self,
        chunk_location: &str,
    ) -> Result<Arc<dyn ChunkFetcher>, VirtualReferenceError> {
        let cont = find_container(chunk_location, &self.containers).ok_or_else(|| {
            VirtualReferenceErrorKind::NoContainerForUrl(chunk_location.to_string())
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
                    Some(_) => Err(VirtualReferenceErrorKind::InvalidCredentials(
                        "S3".to_string(),
                    ))?,
                    None => {
                        if opts.anonymous {
                            &S3Credentials::Anonymous
                        } else {
                            &S3Credentials::FromEnv
                        }
                    }
                };
                Ok(Arc::new(S3Fetcher::new(opts, creds, self.settings.clone()).await))
            }
            ObjectStoreConfig::Tigris(opts) => {
                let creds = match self.credentials.get(&cont.name) {
                    Some(Credentials::S3(creds)) => creds,
                    Some(_) => Err(VirtualReferenceErrorKind::InvalidCredentials(
                        "S3".to_string(),
                    ))?,
                    None => {
                        if opts.anonymous {
                            &S3Credentials::Anonymous
                        } else {
                            &S3Credentials::FromEnv
                        }
                    }
                };
                let opts = if opts.endpoint_url.is_some() {
                    opts
                } else {
                    &S3Options {
                        endpoint_url: Some("https://fly.storage.tigris.dev".to_string()),
                        ..opts.clone()
                    }
                };
                Ok(Arc::new(S3Fetcher::new(opts, creds, self.settings.clone()).await))
            }
            ObjectStoreConfig::LocalFileSystem { .. } => {
                Ok(Arc::new(LocalFSFetcher::new(cont).await))
            }
            // FIXME: implement
            ObjectStoreConfig::Gcs { .. } => {
                unimplemented!("support for virtual chunks on gcs")
            }
            ObjectStoreConfig::Azure { .. } => {
                unimplemented!("support for virtual chunks on gcs")
            }
            ObjectStoreConfig::InMemory {} => {
                unimplemented!("support for virtual chunks in Memory")
            }
        }
    }
}

#[derive(Debug)]
pub struct S3Fetcher {
    client: Arc<Client>,
    settings: storage::Settings,
}

impl S3Fetcher {
    pub async fn new(
        opts: &S3Options,
        credentials: &S3Credentials,
        settings: storage::Settings,
    ) -> Self {
        Self {
            settings,
            client: Arc::new(
                mk_client(opts, credentials.clone(), Vec::new(), Vec::new()).await,
            ),
        }
    }

    async fn get_object_concurrently(
        &self,
        key: &str,
        location: &str, //used for errors
        bucket: &str,
        range: &Range<u64>,
        checksum: Option<&Checksum>,
    ) -> Result<Box<dyn Buf + Unpin + Send>, VirtualReferenceError> {
        let client = &self.client;
        let results =
            split_in_multiple_requests(
                range,
                self.settings.concurrency().ideal_concurrent_request_size().get(),
                self.settings.concurrency().max_concurrent_requests_for_object().get(),
            )
            .map(|range| async move {
                let key = key.to_string();
                let client = Arc::clone(client);
                let bucket = bucket.to_string();
                let mut b = client
                    .get_object()
                    .bucket(bucket)
                    .key(key)
                    .range(range_to_header(&range));

                match checksum {
                    Some(Checksum::LastModified(SecondsSinceEpoch(seconds))) => {
                        b = b.if_unmodified_since(
                            aws_sdk_s3::primitives::DateTime::from_secs(*seconds as i64),
                        )
                    }
                    Some(Checksum::ETag(etag)) => {
                        b = b.if_match(&etag.0);
                    }
                    None => {}
                };

                b.send()
                    .await
                    .map_err(|e| match e {
                        // minio returns this
                        SdkError::ServiceError(err) => {
                            if err.err().meta().code() == Some("PreconditionFailed") {
                                VirtualReferenceErrorKind::ObjectModified(
                                    location.to_string(),
                                )
                            } else {
                                VirtualReferenceErrorKind::FetchError(Box::new(
                                    SdkError::<GetObjectError>::ServiceError(err),
                                ))
                            }
                        }
                        // S3 API documents this
                        SdkError::ResponseError(err) => {
                            let status = err.raw().status().as_u16();
                            // see https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html#API_PutObject_RequestSyntax
                            if status == 409 || status == 412 {
                                VirtualReferenceErrorKind::ObjectModified(
                                    location.to_string(),
                                )
                            } else {
                                VirtualReferenceErrorKind::FetchError(Box::new(
                                    SdkError::<GetObjectError>::ResponseError(err),
                                ))
                            }
                        }
                        other_err => {
                            VirtualReferenceErrorKind::FetchError(Box::new(other_err))
                        }
                    })?
                    .body
                    .collect()
                    .await
                    .map_err(|e| {
                        VirtualReferenceError::from(
                            VirtualReferenceErrorKind::FetchError(Box::new(e)),
                        )
                    })
            })
            .collect::<FuturesOrdered<_>>();

        let init: Box<dyn Buf + Unpin + Send> = Box::new(&[][..]);
        results
            .try_fold(init, |prev, agg_bytes| async {
                let res: Box<dyn Buf + Unpin + Send> = Box::new(prev.chain(agg_bytes));
                Ok(res)
            })
            .await
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
        let url =
            Url::parse(location).map_err(VirtualReferenceErrorKind::CannotParseUrl)?;

        let bucket_name = if let Some(host) = url.host_str() {
            host.to_string()
        } else {
            Err(VirtualReferenceErrorKind::CannotParseBucketName(
                "No bucket name found".to_string(),
            ))?
        };

        let key = url.path();
        let key = key.strip_prefix('/').unwrap_or(key);
        let mut buf = self
            .get_object_concurrently(key, location, bucket_name.as_str(), range, checksum)
            .await?;
        let needed_bytes = range.end - range.start;
        let remaining = buf.remaining() as u64;
        if remaining != needed_bytes {
            Err(VirtualReferenceErrorKind::InvalidObjectSize {
                expected: needed_bytes,
                available: remaining,
            }
            .into())
        } else {
            Ok(buf.copy_to_bytes(needed_bytes as usize))
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
        let url =
            Url::parse(location).map_err(VirtualReferenceErrorKind::CannotParseUrl)?;
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
            Some(Checksum::ETag(etag)) => options.if_match = Some(etag.0.clone()),
            None => {}
        }

        let path = Path::parse(url.path())
            .map_err(|e| VirtualReferenceErrorKind::OtherError(Box::new(e)))?;

        match self.client.get_opts(&path, options).await {
            Ok(res) => res
                .bytes()
                .await
                .map_err(|e| VirtualReferenceErrorKind::FetchError(Box::new(e)).into()),
            Err(object_store::Error::Precondition { .. }) => {
                Err(VirtualReferenceErrorKind::ObjectModified(location.to_string())
                    .into())
            }
            Err(err) => Err(VirtualReferenceErrorKind::FetchError(Box::new(err)).into()),
        }
    }
}
