use std::{
    collections::HashMap,
    num::{NonZeroU16, NonZeroU64},
    ops::Range,
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
    // name is no longer needed, but we keep it for compatibility with
    // old serialized configurations
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub name: Option<ContainerName>,

    url_prefix: String,
    pub store: ObjectStoreConfig,
}

pub type VirtualChunkCredentialsError = String;

impl VirtualChunkContainer {
    pub fn new(url_prefix: String, store: ObjectStoreConfig) -> Result<Self, String> {
        let url = Url::parse(url_prefix.as_str()).map_err(|e| e.to_string())?;
        match (url.scheme(), &store) {
            (
                "s3",
                ObjectStoreConfig::S3(_)
                | ObjectStoreConfig::S3Compatible(_)
                | ObjectStoreConfig::Tigris(_),
            ) => {
                if !url.has_host() {
                    return Err(
                        "Url prefix for s3:// containers must include a host".to_string()
                    );
                }
            }
            ("gcs", ObjectStoreConfig::Gcs(_)) => {
                if !url.has_host() {
                    return Err("Url prefix for gcs:// containers must include a host"
                        .to_string());
                }
            }
            ("az", ObjectStoreConfig::Azure(_)) => {
                if !url.has_host() {
                    return Err(
                        "Url prefix for az:// containers must include a host".to_string()
                    );
                }
            }
            ("tigris", ObjectStoreConfig::Tigris(_)) => {
                if !url.has_host() {
                    return Err(
                        "Url prefix for tigris:// containers must include a host"
                            .to_string(),
                    );
                }
            }
            ("http" | "https", ObjectStoreConfig::Http(_)) => {
                if !url.has_host() {
                    return Err(
                        "Url prefix for http(s):// containers must include a host"
                            .to_string(),
                    );
                }
            }
            ("file", ObjectStoreConfig::LocalFileSystem(_)) => {
                if !url.has_host() && url.path().len() < 2 {
                    return Err("Url prefix for file:// containers must include a path"
                        .to_string());
                }

                let mut segments = url.path_segments().ok_or(
                    "Url prefix for file:// containers must include a path".to_string(),
                )?;

                if segments.any(|s| s == ".." || s == ".") {
                    return Err(
                        "Url prefix for file:// containers must be a canonical url"
                            .to_string(),
                    );
                }
            }
            (scheme, _) => {
                return Err(format!(
                    "Invalid url prefix scheme ({scheme}) for this object store config"
                ));
            }
        };

        Ok(Self { url_prefix, store, name: None })
    }

    pub fn url_prefix(&self) -> &str {
        self.url_prefix.as_str()
    }

    pub fn validate_credentials(
        &self,
        cred: Option<&Credentials>,
    ) -> Result<(), VirtualChunkCredentialsError> {
        match (&self.store, cred) {
            (ObjectStoreConfig::S3Compatible(_), Some(Credentials::S3(_)) | None) => {
                Ok(())
            }
            (ObjectStoreConfig::S3(_), Some(Credentials::S3(_)) | None) => Ok(()),
            (ObjectStoreConfig::Gcs(_), Some(Credentials::Gcs(_)) | None) => Ok(()),
            (ObjectStoreConfig::Azure(_), Some(Credentials::Azure(_)) | None) => Ok(()),
            (ObjectStoreConfig::Tigris(_), Some(Credentials::S3(_)) | None) => Ok(()),
            (ObjectStoreConfig::InMemory, None) => Ok(()),
            (ObjectStoreConfig::LocalFileSystem(_), None) => Ok(()),
            (ObjectStoreConfig::Http(_), None) => Ok(()),

            (ObjectStoreConfig::InMemory, Some(_)) => {
                Err("in memory storage does not accept credentials".to_string())
            }
            (ObjectStoreConfig::LocalFileSystem(..), Some(_)) => {
                Err("in memory storage does not accept credentials".to_string())
            }
            (ObjectStoreConfig::Http(_), Some(_)) => {
                // TODO: Support basic and bearer auth
                Err("http storage does not support credentials yet".to_string())
            }
            (store, cred) => Err(format!(
                "Virtual chunk container credentials do not match store type. store={store:?}, credentials={cred:?}"
            )),
        }
    }
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
    // url_prefix to Credentials
    credentials: HashMap<String, Option<Credentials>>,
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
        credentials: HashMap<String, Option<Credentials>>,
        settings: storage::Settings,
    ) -> Self {
        // we need to validate the containers because they can come from persisted config
        // they can be manipulated to have invalid url prefixes
        // We chose to silently filter out invalid containers

        let mut containers = containers
            .filter_map(|cont| {
                VirtualChunkContainer::new(cont.url_prefix, cont.store)
                    .inspect_err(|err| {
                        tracing::warn!(
                            "Invalid virtual chunk container, ignoring it: {err}"
                        )
                    })
                    .ok()
            })
            .collect::<Vec<_>>();
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
        // this resolves things like /../
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
                let creds = match self.credentials.get(&cont.url_prefix) {
                    Some(Some(Credentials::S3(creds))) => creds,
                    Some(Some(_)) => Err(VirtualReferenceErrorKind::InvalidCredentials(
                        "S3".to_string(),
                    ))?,
                    Some(None) => {
                        if opts.anonymous {
                            &S3Credentials::Anonymous
                        } else {
                            &S3Credentials::FromEnv
                        }
                    }
                    None => Err(
                        VirtualReferenceErrorKind::UnauthorizedVirtualChunkContainer(
                            Box::new(cont.clone()),
                        ),
                    )?,
                };
                Ok(Arc::new(S3Fetcher::new(opts, creds, self.settings.clone()).await))
            }
            ObjectStoreConfig::Tigris(opts) => {
                let creds = match self.credentials.get(&cont.url_prefix) {
                    Some(Some(Credentials::S3(creds))) => creds,
                    Some(Some(_)) => Err(VirtualReferenceErrorKind::InvalidCredentials(
                        "tigris".to_string(),
                    ))?,
                    Some(None) => {
                        if opts.anonymous {
                            &S3Credentials::Anonymous
                        } else {
                            &S3Credentials::FromEnv
                        }
                    }
                    None => Err(
                        VirtualReferenceErrorKind::UnauthorizedVirtualChunkContainer(
                            Box::new(cont.clone()),
                        ),
                    )?,
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
                match self.credentials.get(&cont.url_prefix) {
                    Some(None) => Ok(Arc::new(ObjectStoreFetcher::new_local())),
                    Some(Some(_)) => Err(VirtualReferenceErrorKind::InvalidCredentials(
                        "file".to_string(),
                    ))?,
                    None => Err(
                        VirtualReferenceErrorKind::UnauthorizedVirtualChunkContainer(
                            Box::new(cont.clone()),
                        ),
                    )?,
                }
            }
            ObjectStoreConfig::Gcs(opts) => {
                let creds = match self.credentials.get(&cont.url_prefix) {
                    Some(Some(Credentials::Gcs(creds))) => creds,
                    Some(Some(_)) => Err(VirtualReferenceErrorKind::InvalidCredentials(
                        "GCS".to_string(),
                    ))?,
                    // FIXME: support from env
                    Some(None) => &GcsCredentials::Anonymous,
                    None => Err(
                        VirtualReferenceErrorKind::UnauthorizedVirtualChunkContainer(
                            Box::new(cont.clone()),
                        ),
                    )?,
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
                match self.credentials.get(&cont.url_prefix) {
                    // FIXME: support http auth
                    Some(None) => {}
                    Some(Some(_)) => Err(VirtualReferenceErrorKind::InvalidCredentials(
                        "HTTP".to_string(),
                    ))?,
                    None => Err(
                        VirtualReferenceErrorKind::UnauthorizedVirtualChunkContainer(
                            Box::new(cont.clone()),
                        ),
                    )?,
                };
                let hostname = if let Some(host) = chunk_location.host_str() {
                    host.to_string()
                } else {
                    Err(VirtualReferenceErrorKind::CannotParseBucketName(
                        "No hostname found for HTTP store".to_string(),
                    ))?
                };

                let root_url = format!("{}://{}", chunk_location.scheme(), hostname);
                Ok(Arc::new(ObjectStoreFetcher::new_http(&root_url, opts).await?))
            }
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
            Ok((
                cont.url_prefix.clone(),
                Some(format!("{}://{}", location.scheme(), host)),
            ))
        } else {
            Err(VirtualReferenceErrorKind::CannotParseBucketName(
                "No bucket name found".to_string(),
            )
            .into())
        }
    } else {
        Ok((cont.url_prefix.clone(), None))
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
        let client =
            mk_client(opts, credentials.clone(), Vec::new(), Vec::new(), &settings).await;
        Self { settings, client: Arc::new(client) }
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
            .filter_map(|(k, v)| {
                ClientConfigKey::from_str(k).ok().map(|key| (key, v.clone()))
            })
            .collect();
        let backend =
            HttpObjectStoreBackend { url: url.to_string(), config: Some(config) };
        let settings = storage::Settings::default();
        let client = backend
            .mk_object_store(&settings)
            .map_err(|e| VirtualReferenceErrorKind::OtherError(Box::new(e)))?;
        Ok(ObjectStoreFetcher { client, settings })
    }

    pub async fn new_gcs(
        bucket: String,
        prefix: Option<String>,
        credentials: Option<GcsCredentials>,
        config: HashMap<String, String>,
    ) -> Result<Self, VirtualReferenceError> {
        let config = config
            .into_iter()
            .filter_map(|(k, v)| {
                GoogleConfigKey::from_str(&k).ok().map(|key| (key, v.clone()))
            })
            .collect();
        let backend =
            GcsObjectStoreBackend { bucket, prefix, credentials, config: Some(config) };
        let settings = storage::Settings::default();
        let client = backend
            .mk_object_store(&settings)
            .map_err(|e| VirtualReferenceErrorKind::OtherError(Box::new(e)))?;

        Ok(ObjectStoreFetcher { client, settings })
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

#[cfg(test)]
#[allow(clippy::panic, clippy::unwrap_used, clippy::expect_used)]
mod tests {

    use crate::{
        ObjectStoreConfig,
        config::S3Options,
        format::manifest::{VirtualReferenceError, VirtualReferenceErrorKind},
        virtual_chunks::{VirtualChunkContainer, VirtualChunkResolver},
    };

    #[test]
    fn cannot_create_container_without_prefix() {
        assert!(
            VirtualChunkContainer::new(
                "s3://".to_string(),
                ObjectStoreConfig::S3Compatible(S3Options {
                    region: None,
                    endpoint_url: None,
                    anonymous: false,
                    allow_http: false,
                    force_path_style: false
                })
            )
            .is_err()
        );
        assert!(
            VirtualChunkContainer::new(
                "file://".to_string(),
                ObjectStoreConfig::S3Compatible(S3Options {
                    region: None,
                    endpoint_url: None,
                    anonymous: false,
                    allow_http: false,
                    force_path_style: false
                })
            )
            .is_err()
        );
        assert!(
            VirtualChunkContainer::new(
                "file:///".to_string(),
                ObjectStoreConfig::S3Compatible(S3Options {
                    region: None,
                    endpoint_url: None,
                    anonymous: false,
                    allow_http: false,
                    force_path_style: false
                })
            )
            .is_err()
        );
        assert!(
            VirtualChunkContainer::new(
                "gcs://".to_string(),
                ObjectStoreConfig::S3Compatible(S3Options {
                    region: None,
                    endpoint_url: None,
                    anonymous: false,
                    allow_http: false,
                    force_path_style: false
                })
            )
            .is_err()
        );
        assert!(
            VirtualChunkContainer::new(
                "http://".to_string(),
                ObjectStoreConfig::S3Compatible(S3Options {
                    region: None,
                    endpoint_url: None,
                    anonymous: false,
                    allow_http: false,
                    force_path_style: false
                })
            )
            .is_err()
        );
        assert!(
            VirtualChunkContainer::new(
                "https://".to_string(),
                ObjectStoreConfig::S3Compatible(S3Options {
                    region: None,
                    endpoint_url: None,
                    anonymous: false,
                    allow_http: false,
                    force_path_style: false
                })
            )
            .is_err()
        );
        assert!(
            VirtualChunkContainer::new(
                "custom://".to_string(),
                ObjectStoreConfig::S3Compatible(S3Options {
                    region: None,
                    endpoint_url: None,
                    anonymous: false,
                    allow_http: false,
                    force_path_style: false
                })
            )
            .is_err()
        );
    }

    #[tokio::test]
    async fn test_cannot_resolve_for_nonexistent_container() {
        let container = VirtualChunkContainer::new(
            "file:///foo".to_string(),
            ObjectStoreConfig::LocalFileSystem("/example".into()),
        )
        .unwrap();
        let resolver = VirtualChunkResolver::new(
            [container].into_iter(),
            Default::default(),
            Default::default(),
        );

        let path = "file:///example/foo.nc";
        let res = resolver.fetch_chunk(path, &(0..100), None).await;
        assert!(matches!(
            res,
            Err(VirtualReferenceError {
                kind: VirtualReferenceErrorKind::NoContainerForUrl(error_path),
                ..
            }) if error_path.as_str() == path
        ));
    }

    #[tokio::test]
    async fn test_cannot_resolve_for_unauthorized_container() {
        let container = VirtualChunkContainer::new(
            "file:///example".to_string(),
            ObjectStoreConfig::LocalFileSystem("/example".into()),
        )
        .unwrap();
        let resolver = VirtualChunkResolver::new(
            [container].into_iter(),
            Default::default(),
            Default::default(),
        );

        let path = "file:///example/foo.nc";
        let res = resolver.fetch_chunk(path, &(0..100), None).await;
        assert!(matches!(
            res,
            Err(VirtualReferenceError {
                kind: VirtualReferenceErrorKind::UnauthorizedVirtualChunkContainer(cont),
                ..
            }) if cont.url_prefix() == "file:///example"
        ));
    }

    #[tokio::test]
    async fn test_resolver_filters_out_invalid_containers() {
        // this may be a container manipulated on-disk to have a bad url-prefix
        // it should get filtered out when creating the resolver
        let container = VirtualChunkContainer {
            name: None,
            url_prefix: "file:///".to_string(),
            store: ObjectStoreConfig::LocalFileSystem("/example".into()),
        };
        let resolver = VirtualChunkResolver::new(
            [container].into_iter(),
            Default::default(),
            Default::default(),
        );

        let path = "file:///example/foo.nc";
        let res = resolver.fetch_chunk(path, &(0..100), None).await;
        assert!(matches!(
            res,
            Err(VirtualReferenceError {
                kind: VirtualReferenceErrorKind::NoContainerForUrl(error_path),
                ..
            }) if error_path.as_str() == path
        ));
    }
}
