//! References to external data sources.
//!
//! Virtual chunks allow arrays to reference data stored outside the Icechunk
//! repository (e.g., existing Parquet, `NetCDF`, or other files). Instead of
//! copying data, chunks store references to byte ranges in external files.

use std::{
    collections::HashMap,
    num::{NonZeroU16, NonZeroU64},
    ops::Range,
    str::FromStr as _,
    sync::Arc,
};

use async_trait::async_trait;
use bytes::{Buf, Bytes};
use futures::{TryStreamExt as _, stream::FuturesOrdered};
#[cfg(any(
    feature = "object-store-s3",
    feature = "object-store-gcs",
    feature = "object-store-azure",
    feature = "object-store-http"
))]
use icechunk_arrow_object_store::object_store::ClientConfigKey;
#[cfg(feature = "object-store-azure")]
use icechunk_arrow_object_store::object_store::azure::AzureConfigKey;
#[cfg(feature = "object-store-gcs")]
use icechunk_arrow_object_store::object_store::gcp::GoogleConfigKey;
#[cfg(feature = "object-store-fs")]
use icechunk_arrow_object_store::object_store::local::LocalFileSystem;
#[cfg(any(
    feature = "object-store-s3",
    feature = "object-store-fs",
    feature = "object-store-gcs",
    feature = "object-store-azure",
    feature = "object-store-http"
))]
use icechunk_arrow_object_store::object_store::{GetOptions, ObjectStore, path::Path};
#[cfg(feature = "s3")]
use icechunk_s3::aws_sdk_s3::{
    Client, error::SdkError, operation::get_object::GetObjectError,
};
use icechunk_types::ICResultExt as _;
use quick_cache::sync::Cache;
use serde::{Deserialize, Serialize};
use url::Url;

#[cfg(feature = "object-store-azure")]
use crate::config::AzureCredentials;
#[cfg(feature = "object-store-gcs")]
use crate::config::GcsCredentials;
use crate::config::{S3Credentials, S3Options};
#[cfg(feature = "object-store-azure")]
use crate::storage::AzureObjectStoreBackend;
#[cfg(feature = "object-store-gcs")]
use crate::storage::GcsObjectStoreBackend;
#[cfg(feature = "object-store-http")]
use crate::storage::HttpObjectStoreBackend;
#[cfg(any(
    feature = "object-store-s3",
    feature = "object-store-gcs",
    feature = "object-store-azure",
    feature = "object-store-http"
))]
use crate::storage::ObjectStoreBackend as _;
#[cfg(feature = "object-store-s3")]
use crate::storage::S3ObjectStoreBackend;
use crate::{
    ObjectStoreConfig,
    config::Credentials,
    format::{
        ChunkOffset,
        manifest::{
            Checksum, SecondsSinceEpoch, VirtualChunkLocation, VirtualReferenceError,
            VirtualReferenceErrorKind,
        },
    },
    private,
    storage::{self, split_in_multiple_requests, strip_quotes},
};
#[cfg(feature = "s3")]
use icechunk_s3::{mk_client, range_to_header};

pub type ContainerName = String;

/// Configuration for an external data source that virtual chunks can reference.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct VirtualChunkContainer {
    // name is no longer needed, but we keep it for compatibility with
    // old serialized configurations
    // We use default because for a while after 1.0 we were skipping
    // serialization if None. Then we learned rmp_serde doesn't really
    // support optional fields, so serialization was broken
    #[serde(default)]
    pub name: Option<ContainerName>,

    url_prefix: String,
    pub store: ObjectStoreConfig,
}

pub type VirtualChunkCredentialsError = String;

impl VirtualChunkContainer {
    pub fn new(url_prefix: String, store: ObjectStoreConfig) -> Result<Self, String> {
        Self::create(None, url_prefix, store)
    }

    pub fn new_named(
        name: ContainerName,
        url_prefix: String,
        store: ObjectStoreConfig,
    ) -> Result<Self, String> {
        if name.is_empty() || name.contains('/') {
            return Err(
                "VirtualChunkContainer name must be non-empty and not contain '/'"
                    .to_string(),
            );
        }
        Self::create(Some(name), url_prefix, store)
    }

    fn create(
        name: Option<ContainerName>,
        url_prefix: String,
        store: ObjectStoreConfig,
    ) -> Result<Self, String> {
        if !url_prefix.ends_with('/') {
            return Err(
                "VirtualChunkContainer url_prefix must end in a / character".to_string()
            );
        }

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
            #[cfg(feature = "object-store-gcs")]
            ("gcs" | "gs", ObjectStoreConfig::Gcs(_)) => {
                if !url.has_host() {
                    return Err(
                        "Url prefix for GCS containers must include a host".to_string()
                    );
                }
            }
            #[cfg(feature = "object-store-azure")]
            ("az" | "azure" | "abfs", ObjectStoreConfig::Azure(..)) => {
                if !url.has_host() {
                    return Err(
                        "Url prefix for Azure containers must include a host".to_string()
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
            #[cfg(feature = "object-store-http")]
            ("http" | "https", ObjectStoreConfig::Http(_)) => {
                if !url.has_host() {
                    return Err(
                        "Url prefix for http(s):// containers must include a host"
                            .to_string(),
                    );
                }
            }
            #[cfg(feature = "object-store-fs")]
            ("file", ObjectStoreConfig::LocalFileSystem(_)) => {
                if !url.has_host() && url.path().len() < 2 {
                    return Err("Url prefix for file:// containers must include a path"
                        .to_string());
                }

                let mut segments = url.path_segments().ok_or_else(|| {
                    "Url prefix for file:// containers must include a path".to_string()
                })?;

                if segments.any(|s| s == ".." || s == ".") {
                    return Err(
                        "Url prefix for file:// containers must be a canonical url"
                            .to_string(),
                    );
                }
            }
            (scheme, store) => {
                return Err(format!(
                    "Invalid url prefix scheme ({scheme}) for this object store config: ({store:?})"
                ));
            }
        };

        Ok(Self { url_prefix, store, name })
    }

    pub fn name(&self) -> Option<&str> {
        self.name.as_deref()
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
            #[cfg(feature = "object-store-gcs")]
            (ObjectStoreConfig::Gcs(_), Some(Credentials::Gcs(_)) | None) => Ok(()),
            #[cfg(feature = "object-store-azure")]
            (ObjectStoreConfig::Azure(_), Some(Credentials::Azure(_))) => Ok(()),
            (ObjectStoreConfig::Tigris(_), Some(Credentials::S3(_)) | None) => Ok(()),
            (ObjectStoreConfig::InMemory, None) => Ok(()),
            #[cfg(feature = "object-store-fs")]
            (ObjectStoreConfig::LocalFileSystem(_), None) => Ok(()),
            #[cfg(feature = "object-store-http")]
            (ObjectStoreConfig::Http(_), None) => Ok(()),

            (ObjectStoreConfig::InMemory, Some(_)) => {
                Err("in memory storage does not accept credentials".to_string())
            }
            #[cfg(feature = "object-store-fs")]
            (ObjectStoreConfig::LocalFileSystem(..), Some(_)) => {
                Err("local file storage does not accept credentials".to_string())
            }
            #[cfg(feature = "object-store-http")]
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

/// Trait for fetching byte ranges from external data sources.
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
            })
            .capture()
        } else {
            Ok(buf.copy_to_bytes(needed_bytes as usize))
        }
    }
}

/// Sort containers in reverse order of prefix length
pub(crate) fn sort_containers(containers: &mut [VirtualChunkContainer]) {
    containers.sort_by_key(|cont| -(cont.url_prefix.len() as i64));
}

type BucketName = String;
type CacheKey = (ContainerName, Option<BucketName>);

type ChunkFetcherCache = Cache<CacheKey, Arc<dyn ChunkFetcher>>;

/// Resolves virtual chunk references to actual bytes from external sources.
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
        fn add_trailing(s: String) -> String {
            if s.ends_with('/') { s } else { format!("{s}/") }
        }

        // we need to validate the containers because they can come from persisted config
        // they can be manipulated to have invalid url prefixes
        // We chose to silently filter out invalid containers

        let mut containers = containers
            .filter_map(|cont| {
                let url_prefix = add_trailing(cont.url_prefix);
                let result = match cont.name {
                    Some(name) => {
                        VirtualChunkContainer::new_named(name, url_prefix, cont.store)
                    }
                    None => VirtualChunkContainer::new(url_prefix, cont.store),
                };
                result
                    .inspect_err(|err| {
                        tracing::warn!(
                            "Invalid virtual chunk container, ignoring it: {err}"
                        );
                    })
                    .ok()
            })
            .collect::<Vec<_>>();
        sort_containers(&mut containers);
        let credentials =
            credentials.into_iter().map(|(k, v)| (add_trailing(k), v)).collect();
        VirtualChunkResolver { containers, credentials, settings, fetchers: new_cache() }
    }

    pub fn matching_container(
        &self,
        chunk_location: &VirtualChunkLocation,
    ) -> Option<&VirtualChunkContainer> {
        if let Some((name, _)) = chunk_location.parse_vcc() {
            self.matching_container_by_name(name)
        } else {
            self.matching_container_by_url(chunk_location.url())
        }
    }

    fn matching_container_by_url(
        &self,
        chunk_location: &str,
    ) -> Option<&VirtualChunkContainer> {
        self.containers
            .iter()
            .find(|cont| chunk_location.starts_with(cont.url_prefix.as_str()))
    }

    fn matching_container_by_name(&self, name: &str) -> Option<&VirtualChunkContainer> {
        self.containers.iter().find(|c| c.name.as_deref() == Some(name))
    }

    /// Expand a chunk location to an absolute URL. For `vcc://name/path` locations,
    /// resolves the name to a container's `url_prefix`. For absolute URLs, returns as-is.
    pub fn expand_location(
        &self,
        location: &str,
    ) -> Result<String, VirtualReferenceError> {
        let Some(rest) =
            location.strip_prefix(crate::format::manifest::VCC_RELATIVE_URL_SCHEME)
        else {
            return Ok(location.to_string());
        };
        let Some(slash) = rest.find('/') else {
            return Err(Err(VirtualReferenceErrorKind::NoContainerForName(
                rest.to_string(),
            ))
            .capture()?);
        };
        let name = &rest[..slash];
        let relative_path = &rest[slash + 1..];
        let cont = self
            .matching_container_by_name(name)
            .ok_or_else(|| {
                VirtualReferenceErrorKind::NoContainerForName(name.to_string())
            })
            .capture()?;
        Ok(format!("{}{}", cont.url_prefix(), relative_path))
    }

    async fn get_fetcher(
        &self,
        chunk_location: &Url,
    ) -> Result<Arc<dyn ChunkFetcher>, VirtualReferenceError> {
        let location = chunk_location.to_string();
        let location = urlencoding::decode(location.as_str()).capture()?;
        let cont = self
            .matching_container_by_url(location.as_ref())
            .ok_or_else(|| {
                VirtualReferenceErrorKind::NoContainerForUrl(chunk_location.to_string())
            })
            .capture()?;

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
        let location = self.expand_location(chunk_location)?;

        // this resolves things like /../
        let url = Url::parse(&location)
            .map_err(|e| VirtualReferenceErrorKind::CannotParseUrl {
                cause: e,
                url: location.clone(),
            })
            .capture()?;
        let fetcher = self.get_fetcher(&url).await?;
        fetcher.fetch_chunk(&url, range, checksum).await
    }

    async fn mk_fetcher_for(
        &self,
        cont: &VirtualChunkContainer,
        chunk_location: &Url,
    ) -> Result<Arc<dyn ChunkFetcher>, VirtualReferenceError> {
        #[expect(clippy::unimplemented)]
        match &cont.store {
            #[cfg(feature = "s3")]
            ObjectStoreConfig::S3(opts) | ObjectStoreConfig::S3Compatible(opts) => {
                let creds = match self.credentials.get(&cont.url_prefix) {
                    Some(Some(Credentials::S3(creds))) => creds,
                    Some(Some(_)) => {
                        Err(VirtualReferenceErrorKind::InvalidCredentials(
                            "S3".to_string(),
                        ))
                        .capture()?
                    }
                    Some(None) => {
                        if opts.anonymous {
                            &S3Credentials::Anonymous
                        } else {
                            &S3Credentials::FromEnv
                        }
                    }
                    None => {
                        Err(VirtualReferenceErrorKind::UnauthorizedVirtualChunkContainer {
                            url_prefix: cont.url_prefix().to_string(),
                            name: cont.name().map(str::to_string),
                        })
                        .capture()?
                    }
                };
                Ok(Arc::new(S3Fetcher::new(opts, creds, self.settings.clone()).await))
            }
            #[cfg(feature = "s3")]
            ObjectStoreConfig::Tigris(opts) => {
                let creds = match self.credentials.get(&cont.url_prefix) {
                    Some(Some(Credentials::S3(creds))) => creds,
                    Some(Some(_)) => {
                        Err(VirtualReferenceErrorKind::InvalidCredentials(
                            "tigris".to_string(),
                        ))
                        .capture()?
                    }
                    Some(None) => {
                        if opts.anonymous {
                            &S3Credentials::Anonymous
                        } else {
                            &S3Credentials::FromEnv
                        }
                    }
                    None => {
                        Err(VirtualReferenceErrorKind::UnauthorizedVirtualChunkContainer {
                            url_prefix: cont.url_prefix().to_string(),
                            name: cont.name().map(str::to_string),
                        })
                        .capture()?
                    }
                };
                let opts = if opts.endpoint_url.is_some() {
                    opts
                } else {
                    &S3Options {
                        endpoint_url: Some("https://t3.storage.dev".to_string()),
                        ..opts.clone()
                    }
                };
                Ok(Arc::new(S3Fetcher::new(opts, creds, self.settings.clone()).await))
            }
            #[cfg(all(not(feature = "s3"), feature = "object-store-s3"))]
            ObjectStoreConfig::S3(opts)
            | ObjectStoreConfig::S3Compatible(opts)
            | ObjectStoreConfig::Tigris(opts) => {
                let creds = match self.credentials.get(&cont.url_prefix) {
                    Some(Some(Credentials::S3(creds))) => Some(creds.clone()),
                    Some(Some(_)) => {
                        Err(VirtualReferenceErrorKind::InvalidCredentials(
                            "S3".to_string(),
                        ))
                        .capture()?
                    }
                    Some(None) => {
                        if opts.anonymous {
                            Some(S3Credentials::Anonymous)
                        } else {
                            Some(S3Credentials::FromEnv)
                        }
                    }
                    None => {
                        Err(VirtualReferenceErrorKind::UnauthorizedVirtualChunkContainer {
                            url_prefix: cont.url_prefix().to_string(),
                            name: cont.name().map(str::to_string),
                        })
                        .capture()?
                    }
                };

                let bucket_name = if let Some(host) = chunk_location.host_str() {
                    urlencoding::decode(host)?.into_owned()
                } else {
                    Err(VirtualReferenceErrorKind::CannotParseBucketName(
                        "No bucket name found".to_string(),
                    ))
                    .capture()?
                };

                Ok(Arc::new(
                    ObjectStoreFetcher::new_s3(
                        bucket_name,
                        None,
                        creds,
                        Some(opts.clone()),
                        self.settings.clone(),
                    )
                    .await?,
                ))
            }
            #[cfg(not(any(feature = "s3", feature = "object-store-s3")))]
            ObjectStoreConfig::S3(_)
            | ObjectStoreConfig::S3Compatible(_)
            | ObjectStoreConfig::Tigris(_) => {
                Err(VirtualReferenceErrorKind::OtherError(Box::new(
                    std::io::Error::new(
                        std::io::ErrorKind::Unsupported,
                        "S3/Tigris virtual chunk fetching requires the `s3` or `object-store-s3` feature",
                    ),
                )))
                .capture()
            }
            #[cfg(feature = "object-store-fs")]
            ObjectStoreConfig::LocalFileSystem { .. } => {
                match self.credentials.get(&cont.url_prefix) {
                    Some(None) => Ok(Arc::new(ObjectStoreFetcher::new_local(self.settings.clone()))),
                    Some(Some(_)) => {
                        Err(VirtualReferenceErrorKind::InvalidCredentials(
                            "file".to_string(),
                        ))
                        .capture()?
                    }
                    None => {
                        Err(VirtualReferenceErrorKind::UnauthorizedVirtualChunkContainer {
                            url_prefix: cont.url_prefix().to_string(),
                            name: cont.name().map(str::to_string),
                        })
                        .capture()?
                    }
                }
            }
            #[cfg(feature = "object-store-gcs")]
            ObjectStoreConfig::Gcs(opts) => {
                let creds = match self.credentials.get(&cont.url_prefix) {
                    Some(Some(Credentials::Gcs(creds))) => creds,
                    Some(Some(_)) => {
                        Err(VirtualReferenceErrorKind::InvalidCredentials(
                            "GCS".to_string(),
                        ))
                        .capture()?
                    }
                    // FIXME: support from env
                    Some(None) => &GcsCredentials::Anonymous,
                    None => {
                        Err(VirtualReferenceErrorKind::UnauthorizedVirtualChunkContainer {
                            url_prefix: cont.url_prefix().to_string(),
                            name: cont.name().map(str::to_string),
                        })
                        .capture()?
                    }
                };

                let bucket_name = if let Some(host) = chunk_location.host_str() {
                    urlencoding::decode(host).capture()?.into_owned()
                } else {
                    Err(VirtualReferenceErrorKind::CannotParseBucketName(
                        "No bucket name found".to_string(),
                    ))
                    .capture()?
                };
                Ok(Arc::new(
                    ObjectStoreFetcher::new_gcs(
                        bucket_name,
                        None,
                        Some(creds.clone()),
                        opts.clone(),
                        self.settings.clone(),
                    )
                    .await?,
                ))
            }
            #[cfg(feature = "object-store-http")]
            ObjectStoreConfig::Http(opts) => {
                match self.credentials.get(&cont.url_prefix) {
                    // FIXME: support http auth
                    Some(None) => {}
                    Some(Some(_)) => {
                        Err(VirtualReferenceErrorKind::InvalidCredentials(
                            "HTTP".to_string(),
                        ))
                        .capture()?;
                    }
                    None => {
                        Err(VirtualReferenceErrorKind::UnauthorizedVirtualChunkContainer {
                            url_prefix: cont.url_prefix().to_string(),
                            name: cont.name().map(str::to_string),
                        })
                        .capture()?;
                    }
                };
                let hostname = if let Some(host) = chunk_location.host_str() {
                    host.to_string()
                } else {
                    Err(VirtualReferenceErrorKind::CannotParseBucketName(
                        "No hostname found for HTTP store".to_string(),
                    ))
                    .capture()?
                };

                let root_url = format!("{}://{}", chunk_location.scheme(), hostname);
                Ok(Arc::new(ObjectStoreFetcher::new_http(&root_url, opts, self.settings.clone()).await?))
            }
            #[cfg(feature = "object-store-azure")]
            ObjectStoreConfig::Azure(config) => {
                let account = config
                    .get("account")
                    .ok_or(VirtualReferenceErrorKind::AzureConfigurationMustIncludeAccount)
                    .capture()?;

                let creds = match self.credentials.get(&cont.url_prefix) {
                    Some(Some(Credentials::Azure(creds))) => creds,
                    Some(Some(_)) => {
                        Err(VirtualReferenceErrorKind::InvalidCredentials(
                            "Azure".to_string(),
                        ))
                        .capture()?
                    }
                    // FIXME: support anonymous
                    Some(None) | None => {
                        Err(VirtualReferenceErrorKind::UnauthorizedVirtualChunkContainer {
                            url_prefix: cont.url_prefix().to_string(),
                            name: cont.name().map(str::to_string),
                        })
                        .capture()?
                    }
                };

                let container = if let Some(host) = chunk_location.host_str() {
                    urlencoding::decode(host).capture()?.into_owned()
                } else {
                    Err(VirtualReferenceErrorKind::CannotParseBucketName(
                        "No bucket name found".to_string(),
                    ))
                    .capture()?
                };
                Ok(Arc::new(
                    ObjectStoreFetcher::new_azure(
                        account.clone(),
                        container,
                        None,
                        Some(creds.clone()),
                        config.clone(),
                        self.settings.clone(),
                    )
                    .await?,
                ))
            }
            ObjectStoreConfig::InMemory => {
                unimplemented!("support for virtual chunks in Memory")
            }
        }
    }
}

fn is_fetcher_bucket_constrained(store: &ObjectStoreConfig) -> bool {
    match store {
        // When using the native S3Fetcher (feature = "s3"), the client handles any
        // bucket, so it is NOT bucket-constrained. When falling back to
        // ObjectStoreFetcher via object-store-s3, each client is bound to one bucket.
        #[cfg(all(not(feature = "s3"), feature = "object-store-s3"))]
        ObjectStoreConfig::S3(_)
        | ObjectStoreConfig::S3Compatible(_)
        | ObjectStoreConfig::Tigris(_) => true,
        #[cfg(feature = "object-store-gcs")]
        ObjectStoreConfig::Gcs(_) => true,
        #[cfg(feature = "object-store-http")]
        ObjectStoreConfig::Http(_) => true,
        #[cfg(feature = "object-store-azure")]
        ObjectStoreConfig::Azure(_) => true,
        _ => false,
    }
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
            ))
            .capture()
        }
    } else {
        Ok((cont.url_prefix.clone(), None))
    }
}

#[cfg(feature = "s3")]
#[derive(Debug)]
pub struct S3Fetcher {
    client: Arc<Client>,
    settings: storage::Settings,
}

#[cfg(feature = "s3")]
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

#[cfg(feature = "s3")]
impl private::Sealed for S3Fetcher {}

#[cfg(feature = "s3")]
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
            urlencoding::decode(host).capture()?.into_owned()
        } else {
            Err(VirtualReferenceErrorKind::CannotParseBucketName(
                "No bucket name found".to_string(),
            ))
            .capture()?
        };

        let key = urlencoding::decode(chunk_location.path()).capture()?;
        let key = key.strip_prefix('/').unwrap_or(key.as_ref());

        let mut b = self
            .client
            .get_object()
            .bucket(bucket_name)
            .key(key)
            .range(range_to_header(&range));

        match checksum {
            Some(Checksum::LastModified(SecondsSinceEpoch(seconds))) => {
                b = b.if_unmodified_since(
                    icechunk_s3::aws_sdk_s3::primitives::DateTime::from_secs(
                        *seconds as i64,
                    ),
                );
            }
            Some(Checksum::ETag(etag)) => {
                b = b.if_match(strip_quotes(&etag.0));
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
            })
            .capture()?
            .body
            .collect()
            .await
            .map_err(|e| VirtualReferenceErrorKind::FetchError(Box::new(e)))
            .capture()?;
        Ok(Box::new(res))
    }
}

#[cfg(any(
    feature = "object-store-s3",
    feature = "object-store-fs",
    feature = "object-store-gcs",
    feature = "object-store-azure",
    feature = "object-store-http"
))]
#[derive(Debug)]
pub struct ObjectStoreFetcher {
    client: Arc<dyn ObjectStore>,
    settings: storage::Settings,
}

#[cfg(any(
    feature = "object-store-s3",
    feature = "object-store-fs",
    feature = "object-store-gcs",
    feature = "object-store-azure",
    feature = "object-store-http"
))]
impl private::Sealed for ObjectStoreFetcher {}

#[cfg(any(
    feature = "object-store-s3",
    feature = "object-store-fs",
    feature = "object-store-gcs",
    feature = "object-store-azure",
    feature = "object-store-http"
))]
impl ObjectStoreFetcher {
    #[cfg(feature = "object-store-fs")]
    fn new_local(settings: storage::Settings) -> Self {
        ObjectStoreFetcher {
            client: Arc::new(LocalFileSystem::new()),
            settings: storage::Settings {
                unsafe_use_conditional_update: Some(false),
                unsafe_use_metadata: Some(false),
                ..settings
            },
        }
    }

    #[cfg(feature = "object-store-s3")]
    pub async fn new_s3(
        bucket: String,
        prefix: Option<String>,
        credentials: Option<S3Credentials>,
        config: Option<S3Options>,
        settings: storage::Settings,
    ) -> Result<Self, VirtualReferenceError> {
        let backend = S3ObjectStoreBackend { bucket, prefix, credentials, config };
        let client = backend
            .mk_object_store(&settings)
            .map_err(|e| VirtualReferenceErrorKind::OtherError(Box::new(e)))
            .capture()?;
        Ok(ObjectStoreFetcher { client, settings })
    }

    #[cfg(feature = "object-store-http")]
    pub async fn new_http(
        url: &str,
        opts: &HashMap<String, String>,
        settings: storage::Settings,
    ) -> Result<Self, VirtualReferenceError> {
        let config = opts
            .iter()
            .filter_map(|(k, v)| {
                ClientConfigKey::from_str(k).ok().map(|key| (key, v.clone()))
            })
            .collect();
        let backend =
            HttpObjectStoreBackend { url: url.to_string(), config: Some(config) };
        let client = backend
            .mk_object_store(&settings)
            .map_err(|e| VirtualReferenceErrorKind::OtherError(Box::new(e)))
            .capture()?;
        Ok(ObjectStoreFetcher { client, settings })
    }

    #[cfg(feature = "object-store-gcs")]
    pub async fn new_gcs(
        bucket: String,
        prefix: Option<String>,
        credentials: Option<GcsCredentials>,
        config: HashMap<String, String>,
        settings: storage::Settings,
    ) -> Result<Self, VirtualReferenceError> {
        let config = config
            .into_iter()
            .filter_map(|(k, v)| {
                GoogleConfigKey::from_str(&k).ok().map(|key| (key, v.clone()))
            })
            .collect();
        let backend =
            GcsObjectStoreBackend { bucket, prefix, credentials, config: Some(config) };
        let client = backend
            .mk_object_store(&settings)
            .map_err(|e| VirtualReferenceErrorKind::OtherError(Box::new(e)))
            .capture()?;

        Ok(ObjectStoreFetcher { client, settings })
    }

    #[cfg(feature = "object-store-azure")]
    pub async fn new_azure(
        account: String,
        container: String,
        prefix: Option<String>,
        credentials: Option<AzureCredentials>,
        config: HashMap<String, String>,
        settings: storage::Settings,
    ) -> Result<Self, VirtualReferenceError> {
        let config = config
            .into_iter()
            .filter_map(|(k, v)| {
                AzureConfigKey::from_str(&k).ok().map(|key| (key, v.clone()))
            })
            .collect();
        let backend = AzureObjectStoreBackend {
            account,
            container,
            prefix,
            credentials,
            config: Some(config),
        };

        let client = backend
            .mk_object_store(&settings)
            .map_err(|e| VirtualReferenceErrorKind::OtherError(Box::new(e)))
            .capture()?;

        Ok(ObjectStoreFetcher { client, settings })
    }
}

#[cfg(any(
    feature = "object-store-s3",
    feature = "object-store-fs",
    feature = "object-store-gcs",
    feature = "object-store-azure",
    feature = "object-store-http"
))]
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
                #[expect(clippy::expect_used)]
                let d = chrono::DateTime::from_timestamp(*seconds as i64, 0)
                    .expect("Bad last modified field in virtual chunk reference");
                options.if_unmodified_since = Some(d);
            }
            Some(Checksum::ETag(etag)) => {
                options.if_match = Some(strip_quotes(&etag.0).to_string());
            }
            None => {}
        }

        let path = Path::parse(urlencoding::decode(chunk_location.path()).capture()?)
            .map_err(|e| VirtualReferenceErrorKind::OtherError(Box::new(e)))
            .capture()?;

        match self.client.get_opts(&path, options).await {
            Ok(res) => {
                let res = res
                    .bytes()
                    .await
                    .map_err(|e| VirtualReferenceErrorKind::FetchError(Box::new(e)))
                    .capture()?;
                Ok(Box::new(res))
            }
            Err(icechunk_arrow_object_store::object_store::Error::Precondition {
                ..
            }) => {
                Err(VirtualReferenceErrorKind::ObjectModified(chunk_location.to_string()))
                    .capture()
            }
            Err(err) => Err(VirtualReferenceError::capture(
                VirtualReferenceErrorKind::FetchError(Box::new(err)),
            )),
        }
    }
}

#[cfg(test)]
mod tests {

    use crate::{
        ObjectStoreConfig,
        config::S3Options,
        format::manifest::{
            VirtualChunkLocation, VirtualReferenceError, VirtualReferenceErrorKind,
        },
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
                    force_path_style: false,
                    network_stream_timeout_seconds: None,
                    requester_pays: false,
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
                    force_path_style: false,
                    network_stream_timeout_seconds: None,
                    requester_pays: false,
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
                    force_path_style: false,
                    network_stream_timeout_seconds: None,
                    requester_pays: false,
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
                    force_path_style: false,
                    network_stream_timeout_seconds: None,
                    requester_pays: false,
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
                    force_path_style: false,
                    network_stream_timeout_seconds: None,
                    requester_pays: false,
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
                    force_path_style: false,
                    network_stream_timeout_seconds: None,
                    requester_pays: false,
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
                    force_path_style: false,
                    network_stream_timeout_seconds: None,
                    requester_pays: false,
                })
            )
            .is_err()
        );
    }

    #[cfg(feature = "object-store-fs")]
    #[tokio::test]
    async fn test_cannot_resolve_for_nonexistent_container() {
        let container = VirtualChunkContainer::new(
            "file:///foo/".to_string(),
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

    #[cfg(feature = "object-store-fs")]
    #[tokio::test]
    async fn test_cannot_resolve_for_unauthorized_container() {
        let container = VirtualChunkContainer::new(
            "file:///example/".to_string(),
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
                kind: VirtualReferenceErrorKind::UnauthorizedVirtualChunkContainer { url_prefix, .. },
                ..
            }) if url_prefix == "file:///example/"
        ));
    }

    #[cfg(feature = "object-store-fs")]
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

    fn s3_store_config() -> ObjectStoreConfig {
        ObjectStoreConfig::S3(S3Options {
            region: Some("us-east-1".to_string()),
            endpoint_url: None,
            anonymous: false,
            allow_http: false,
            force_path_style: false,
            network_stream_timeout_seconds: None,
            requester_pays: false,
        })
    }

    #[test]
    fn test_new_named_container() {
        let cont = VirtualChunkContainer::new_named(
            "my-data".to_string(),
            "s3://bucket/prefix/".to_string(),
            s3_store_config(),
        )
        .unwrap();
        assert_eq!(cont.name(), Some("my-data"));
        assert_eq!(cont.url_prefix(), "s3://bucket/prefix/");
    }

    #[test]
    fn test_new_named_container_invalid_name() {
        // Empty name
        assert!(
            VirtualChunkContainer::new_named(
                "".to_string(),
                "s3://bucket/prefix/".to_string(),
                s3_store_config(),
            )
            .is_err()
        );

        // Name with slash
        assert!(
            VirtualChunkContainer::new_named(
                "a/b".to_string(),
                "s3://bucket/prefix/".to_string(),
                s3_store_config(),
            )
            .is_err()
        );
    }

    #[test]
    fn test_expand_location() {
        let cont = VirtualChunkContainer::new_named(
            "my-data".to_string(),
            "s3://bucket/prefix/".to_string(),
            s3_store_config(),
        )
        .unwrap();
        let resolver = VirtualChunkResolver::new(
            [cont].into_iter(),
            Default::default(),
            Default::default(),
        );

        // vcc:// expands to absolute
        let expanded =
            resolver.expand_location("vcc://my-data/path/to/chunk.nc").unwrap();
        assert_eq!(expanded, "s3://bucket/prefix/path/to/chunk.nc");

        // Absolute URL passes through as-is
        let expanded = resolver.expand_location("s3://bucket/prefix/chunk.nc").unwrap();
        assert_eq!(expanded, "s3://bucket/prefix/chunk.nc");
    }

    #[test]
    fn test_expand_location_unknown_name() {
        let cont = VirtualChunkContainer::new_named(
            "my-data".to_string(),
            "s3://bucket/prefix/".to_string(),
            s3_store_config(),
        )
        .unwrap();
        let resolver = VirtualChunkResolver::new(
            [cont].into_iter(),
            Default::default(),
            Default::default(),
        );

        let result = resolver.expand_location("vcc://unknown/chunk.nc");
        assert!(matches!(
            result,
            Err(VirtualReferenceError {
                kind: VirtualReferenceErrorKind::NoContainerForName(name),
                ..
            }) if name == "unknown"
        ));

        // No slash after the name
        let result = resolver.expand_location("vcc://chunk.nc");
        assert!(matches!(
            result,
            Err(VirtualReferenceError {
                kind: VirtualReferenceErrorKind::NoContainerForName(name),
                ..
            }) if name == "chunk.nc"
        ));
    }

    #[cfg(feature = "object-store-fs")]
    #[tokio::test]
    async fn test_resolver_passes_settings_to_fetcher() {
        use std::collections::HashMap;
        use std::num::{NonZeroU16, NonZeroU64};
        use url::Url;

        let custom_settings = crate::storage::Settings {
            concurrency: Some(crate::storage::ConcurrencySettings {
                max_concurrent_requests_for_object: Some(
                    NonZeroU16::new(42).unwrap(),
                ),
                ideal_concurrent_request_size: Some(
                    NonZeroU64::new(8192).unwrap(),
                ),
            }),
            ..Default::default()
        };

        let container = VirtualChunkContainer::new(
            "file:///example/".to_string(),
            ObjectStoreConfig::LocalFileSystem("/example".into()),
        )
        .unwrap();

        let mut credentials: HashMap<String, Option<crate::config::Credentials>> =
            HashMap::new();
        credentials.insert("file:///example/".to_string(), None);

        let resolver = VirtualChunkResolver::new(
            [container].into_iter(),
            credentials,
            custom_settings,
        );

        let url = Url::parse("file:///example/foo.nc").unwrap();
        let fetcher = resolver.get_fetcher(&url).await.unwrap();

        // These should reflect the custom settings, not defaults or hardcoded values
        assert_eq!(fetcher.max_concurrent_requests_for_object().get(), 42);
        assert_eq!(fetcher.ideal_concurrent_request_size().get(), 8192);
    }

    #[test]
    fn test_resolver_preserves_names() {
        let cont = VirtualChunkContainer::new_named(
            "my-data".to_string(),
            "s3://bucket/prefix/".to_string(),
            s3_store_config(),
        )
        .unwrap();
        let resolver = VirtualChunkResolver::new(
            [cont].into_iter(),
            Default::default(),
            Default::default(),
        );

        let loc = VirtualChunkLocation::from_vcc_path("my-data", "chunk.nc").unwrap();
        let found = resolver.matching_container(&loc);
        assert!(found.is_some());
        assert_eq!(found.unwrap().name(), Some("my-data"));
    }
}
