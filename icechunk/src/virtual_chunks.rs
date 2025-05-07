use std::{
    collections::HashMap,
    num::{NonZeroU16, NonZeroU64},
    ops::Range,
    path::PathBuf,
    str::FromStr,
    sync::Arc,
};

use async_trait::async_trait;
use aws_sdk_s3::{Client, error::SdkError, operation::get_object::GetObjectError};
use bytes::{Buf, Bytes};
use futures::{TryStreamExt, stream::FuturesOrdered};
use object_store::{
    ClientConfigKey, GetOptions, ObjectStore, gcp::GoogleConfigKey,
    local::LocalFileSystem, path::Path,
};
use quick_cache::sync::Cache;
use serde::{Deserialize, Serialize};
use url::Url;

use crate::{
    ObjectStoreConfig,
    config::{Credentials, GcsCredentials, S3Credentials, S3Options},
    format::{
        ChunkOffset,
        manifest::{
            Checksum, SecondsSinceEpoch, VirtualReferenceError, VirtualReferenceErrorKind,
        },
    },
    private,
    storage::{
        self,
        object_store::{
            GcsObjectStoreBackend, HttpObjectStoreBackend, ObjectStoreBackend as _,
        },
        s3::{mk_client, range_to_header},
        split_in_multiple_requests,
    },
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
            (ObjectStoreConfig::Http(_), _) => {
                Err("http storage does not support credentials".to_string())
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
        (
            "http".to_string(),
            VirtualChunkContainer {
                name: "http".to_string(),
                url_prefix: "http".to_string(),
                store: ObjectStoreConfig::Http(HashMap::new()),
            },
        ),
        (
            "https".to_string(),
            VirtualChunkContainer {
                name: "https".to_string(),
                url_prefix: "https".to_string(),
                store: ObjectStoreConfig::Http(HashMap::new()),
            },
        ),
    ]
    .into_iter()
    .collect()
}

#[async_trait]
pub trait ChunkFetcher: std::fmt::Debug + private::Sealed + Send + Sync {
    fn ideal_concurrent_request_size(&self) -> NonZeroU64;
    fn max_concurrent_requests_for_object(&self) -> NonZeroU16;

    async fn fetch_part(
        &self,
        chunk_location: &Url,
        range: Range<ChunkOffset>,
        checksum: Option<&Checksum>,
    ) -> Result<Box<dyn Buf + Unpin + Send>, VirtualReferenceError>;

    async fn fetch_chunk(
        &self,
        chunk_location: &Url,
        range: &Range<ChunkOffset>,
        checksum: Option<&Checksum>,
    ) -> Result<Bytes, VirtualReferenceError> {
        let results = split_in_multiple_requests(
            range,
            self.ideal_concurrent_request_size().get(),
            self.max_concurrent_requests_for_object().get(),
        )
        .map(|range| self.fetch_part(chunk_location, range, checksum))
        .collect::<FuturesOrdered<_>>();

        let init: Box<dyn Buf + Unpin + Send> = Box::new(&[][..]);
        let mut buf = results
            .try_fold(init, |prev, agg_bytes| async {
                let res: Box<dyn Buf + Unpin + Send> = Box::new(prev.chain(agg_bytes));
                Ok(res)
            })
            .await?;

        let remaining = buf.remaining() as u64;
        let needed_bytes = range.end - range.start;
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

type BucketName = String;
type CacheKey = (ContainerName, Option<BucketName>);

type ChunkFetcherCache = Cache<CacheKey, Arc<dyn ChunkFetcher>>;

#[derive(Debug, Serialize, Deserialize)]
pub struct VirtualChunkResolver {
    containers: Vec<VirtualChunkContainer>,
    credentials: HashMap<ContainerName, Credentials>,
    settings: storage::Settings,
    #[serde(skip, default = "new_cache")]
    fetchers: ChunkFetcherCache,
}

fn new_cache() -> ChunkFetcherCache {
    // TODO: configurable
    ChunkFetcherCache::new(50)
}

impl VirtualChunkResolver {
    pub fn new(
        containers: impl Iterator<Item = VirtualChunkContainer>,
        credentials: HashMap<ContainerName, Credentials>,
        settings: storage::Settings,
    ) -> Self {
        let mut containers = containers.collect::<Vec<_>>();
        sort_containers(&mut containers);
        VirtualChunkResolver { containers, credentials, settings, fetchers: new_cache() }
    }

    pub fn matching_container(
        &self,
        chunk_location: &str,
    ) -> Option<&VirtualChunkContainer> {
        find_container(chunk_location, self.containers.as_ref())
    }

    async fn get_fetcher(
        &self,
        chunk_location: &Url,
    ) -> Result<Arc<dyn ChunkFetcher>, VirtualReferenceError> {
        let cont = find_container(chunk_location.to_string().as_str(), &self.containers)
            .ok_or_else(|| {
                VirtualReferenceErrorKind::NoContainerForUrl(chunk_location.to_string())
            })?;

        let cache_key = fetcher_cache_key(cont, chunk_location)?;
        // TODO: we shouldn't need to clone the container name
        match self.fetchers.get_value_or_guard_async(&cache_key).await {
            Ok(fetcher) => Ok(Arc::clone(&fetcher)),
            Err(guard) => {
                let fetcher = self.mk_fetcher_for(cont, chunk_location).await?;
                let _fail_is_ok = guard.insert(Arc::clone(&fetcher));
                Ok(fetcher)
            }
        }
    }

    pub async fn fetch_chunk(
        &self,
        chunk_location: &str,
        range: &Range<ChunkOffset>,
        checksum: Option<&Checksum>,
    ) -> Result<Bytes, VirtualReferenceError> {
        let url = Url::parse(chunk_location)
            .map_err(VirtualReferenceErrorKind::CannotParseUrl)?;
        let fetcher = self.get_fetcher(&url).await?;
        fetcher.fetch_chunk(&url, range, checksum).await
    }

    async fn mk_fetcher_for(
        &self,
        cont: &VirtualChunkContainer,
        chunk_location: &Url,
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
                Ok(Arc::new(ObjectStoreFetcher::new_local()))
            }
            ObjectStoreConfig::Gcs(opts) => {
                let creds = match self.credentials.get(&cont.name) {
                    Some(Credentials::Gcs(creds)) => creds,
                    Some(_) => Err(VirtualReferenceErrorKind::InvalidCredentials(
                        "GCS".to_string(),
                    ))?,
                    None => &GcsCredentials::Anonymous,
                };

                let bucket_name = if let Some(host) = chunk_location.host_str() {
                    host.to_string()
                } else {
                    Err(VirtualReferenceErrorKind::CannotParseBucketName(
                        "No bucket name found".to_string(),
                    ))?
                };
                Ok(Arc::new(
                    ObjectStoreFetcher::new_gcs(
                        bucket_name,
                        None,
                        Some(creds.clone()),
                        opts.clone(),
                    )
                    .await?,
                ))
            }
            ObjectStoreConfig::Http(opts) => {
                let hostname = if let Some(host) = chunk_location.host_str() {
                    host.to_string()
                } else {
                    Err(VirtualReferenceErrorKind::CannotParseBucketName(
                        "No hostname found for HTTP store".to_string(),
                    ))?
                };

                let root_url = format!("{}://{}", chunk_location.scheme(), hostname);
                Ok(Arc::new(
                    ObjectStoreFetcher::new_http(&root_url, opts)
                        .await?,
                ))
            },
            ObjectStoreConfig::Azure { .. } => {
                unimplemented!("support for virtual chunks on azure")
            }
            ObjectStoreConfig::InMemory => {
                unimplemented!("support for virtual chunks in Memory")
            }
        }
    }
}

fn is_fetcher_bucket_constrained(store: &ObjectStoreConfig) -> bool {
    matches!(store, ObjectStoreConfig::Gcs(_) | ObjectStoreConfig::Http(_))
}

fn fetcher_cache_key(
    cont: &VirtualChunkContainer,
    location: &Url,
) -> Result<(String, Option<String>), VirtualReferenceError> {
    if is_fetcher_bucket_constrained(&cont.store) {
        if let Some(host) = location.host_str() {
            Ok((cont.name.clone(), Some(format!("{}://{}", location.scheme(), host))))
        } else {
            Err(VirtualReferenceErrorKind::CannotParseBucketName(
                "No bucket name found".to_string(),
            )
            .into())
        }
    } else {
        Ok((cont.name.clone(), None))
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
}

impl private::Sealed for S3Fetcher {}

#[async_trait]
impl ChunkFetcher for S3Fetcher {
    fn ideal_concurrent_request_size(&self) -> NonZeroU64 {
        self.settings.concurrency().ideal_concurrent_request_size()
    }
    fn max_concurrent_requests_for_object(&self) -> NonZeroU16 {
        self.settings.concurrency().max_concurrent_requests_for_object()
    }

    async fn fetch_part(
        &self,
        chunk_location: &Url,
        range: Range<ChunkOffset>,
        checksum: Option<&Checksum>,
    ) -> Result<Box<dyn Buf + Unpin + Send>, VirtualReferenceError> {
        let bucket_name = if let Some(host) = chunk_location.host_str() {
            host.to_string()
        } else {
            Err(VirtualReferenceErrorKind::CannotParseBucketName(
                "No bucket name found".to_string(),
            ))?
        };

        let key = chunk_location.path();
        let key = key.strip_prefix('/').unwrap_or(key);

        let mut b = self
            .client
            .get_object()
            .bucket(bucket_name)
            .key(key)
            .range(range_to_header(&range));

        match checksum {
            Some(Checksum::LastModified(SecondsSinceEpoch(seconds))) => {
                b = b.if_unmodified_since(aws_sdk_s3::primitives::DateTime::from_secs(
                    *seconds as i64,
                ))
            }
            Some(Checksum::ETag(etag)) => {
                b = b.if_match(&etag.0);
            }
            None => {}
        };

        let res = b
            .send()
            .await
            .map_err(|e| match e {
                // minio returns this
                SdkError::ServiceError(err) => {
                    if err.err().meta().code() == Some("PreconditionFailed") {
                        VirtualReferenceErrorKind::ObjectModified(
                            chunk_location.to_string(),
                        )
                    } else {
                        VirtualReferenceErrorKind::FetchError(Box::new(SdkError::<
                            GetObjectError,
                        >::ServiceError(
                            err
                        )))
                    }
                }
                // S3 API documents this
                SdkError::ResponseError(err) => {
                    let status = err.raw().status().as_u16();
                    // see https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html#API_PutObject_RequestSyntax
                    if status == 409 || status == 412 {
                        VirtualReferenceErrorKind::ObjectModified(
                            chunk_location.to_string(),
                        )
                    } else {
                        VirtualReferenceErrorKind::FetchError(Box::new(SdkError::<
                            GetObjectError,
                        >::ResponseError(
                            err
                        )))
                    }
                }
                other_err => VirtualReferenceErrorKind::FetchError(Box::new(other_err)),
            })?
            .body
            .collect()
            .await
            .map_err(|e| {
                VirtualReferenceError::from(VirtualReferenceErrorKind::FetchError(
                    Box::new(e),
                ))
            })?;
        Ok(Box::new(res))
    }
}

#[derive(Debug)]
pub struct ObjectStoreFetcher {
    client: Arc<dyn ObjectStore>,
    settings: storage::Settings,
}
impl private::Sealed for ObjectStoreFetcher {}

impl ObjectStoreFetcher {
    fn new_local() -> Self {
        ObjectStoreFetcher {
            client: Arc::new(LocalFileSystem::new()),
            settings: storage::Settings {
                concurrency: Some(storage::ConcurrencySettings {
                    max_concurrent_requests_for_object: Some(
                        NonZeroU16::new(5).unwrap_or(NonZeroU16::MIN),
                    ),
                    ideal_concurrent_request_size: Some(
                        NonZeroU64::new(4 * 1024).unwrap_or(NonZeroU64::MIN),
                    ),
                }),
                unsafe_use_conditional_update: Some(false),
                unsafe_use_metadata: Some(false),
                ..Default::default()
            },
        }
    }

    pub async fn new_http(
        url: &str,
        opts: &HashMap<String, String>,
    ) -> Result<Self, VirtualReferenceError> {
        let config = opts
            .iter()
            .map(|(k, v)| (ClientConfigKey::from_str(k).unwrap(), v.clone()))
            .collect();
        let backend =
            HttpObjectStoreBackend { url: url.to_string(), config: Some(config) };
        let client = backend
            .mk_object_store()
            .map_err(|e| VirtualReferenceErrorKind::OtherError(Box::new(e)))?;
        Ok(ObjectStoreFetcher { client, settings: storage::Settings::default() })
    }

    pub async fn new_gcs(
        bucket: String,
        prefix: Option<String>,
        credentials: Option<GcsCredentials>,
        config: HashMap<String, String>,
    ) -> Result<Self, VirtualReferenceError> {
        let config = config
            .into_iter()
            .map(|(k, v)| (GoogleConfigKey::from_str(&k).unwrap(), v))
            .collect();
        let backend =
            GcsObjectStoreBackend { bucket, prefix, credentials, config: Some(config) };
        let client = backend
            .mk_object_store()
            .map_err(|e| VirtualReferenceErrorKind::OtherError(Box::new(e)))?;

        Ok(ObjectStoreFetcher { client, settings: storage::Settings::default() })
    }
}

#[async_trait]
impl ChunkFetcher for ObjectStoreFetcher {
    fn ideal_concurrent_request_size(&self) -> NonZeroU64 {
        self.settings.concurrency().ideal_concurrent_request_size()
    }
    fn max_concurrent_requests_for_object(&self) -> NonZeroU16 {
        self.settings.concurrency().max_concurrent_requests_for_object()
    }

    async fn fetch_part(
        &self,
        chunk_location: &Url,
        range: Range<ChunkOffset>,
        checksum: Option<&Checksum>,
    ) -> Result<Box<dyn Buf + Unpin + Send>, VirtualReferenceError> {
        let usize_range = range.start..range.end;
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

        let path = Path::parse(chunk_location.path())
            .map_err(|e| VirtualReferenceErrorKind::OtherError(Box::new(e)))?;

        match self.client.get_opts(&path, options).await {
            Ok(res) => {
                let res = res.bytes().await.map_err(|e| {
                    VirtualReferenceError::from(VirtualReferenceErrorKind::FetchError(
                        Box::new(e),
                    ))
                })?;
                Ok(Box::new(res))
            }
            Err(object_store::Error::Precondition { .. }) => {
                Err(VirtualReferenceErrorKind::ObjectModified(chunk_location.to_string())
                    .into())
            }
            Err(err) => Err(VirtualReferenceErrorKind::FetchError(Box::new(err)).into()),
        }
    }
}
