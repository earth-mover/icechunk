use std::{ops::Range, pin::Pin, sync::Arc};

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::{Stream, stream::BoxStream};
use reqwest as rw;
use serde::{Deserialize, Serialize};
use tokio::sync::OnceCell;

use tracing::{debug, trace};
use url::Url;

use crate::{
    config::{GcsCredentials, S3Credentials, S3Options},
    new_s3_storage, private,
    storage::{
        StorageErrorKind, new_gcs_storage, new_http_storage, new_r2_storage,
        new_tigris_storage,
    },
};

use super::{
    DeleteObjectsResult, ListInfo, Settings, Storage, StorageError, StorageResult,
    VersionInfo, VersionedUpdateResult,
};

#[derive(Debug, Serialize, Deserialize)]
pub struct RedirectStorage {
    url: Url,

    #[serde(skip)]
    backend: OnceCell<Arc<dyn Storage>>,
}

const HANDLED_SCHEMES: [&str; 9] = [
    "s3",
    "gs",
    "gcs",
    "http+icechunk",
    "http+ic",
    "https+icechunk",
    "https+ic",
    "r2",
    "tigris",
];

impl RedirectStorage {
    pub fn new(url: Url) -> Self {
        Self { url, backend: OnceCell::new() }
    }

    async fn backend(&self) -> StorageResult<&dyn Storage> {
        self.backend.get_or_try_init(|| self.mk_backend()).await.map(|arc| arc.as_ref())
    }

    async fn mk_backend(&self) -> StorageResult<Arc<dyn Storage>> {
        let redirect = |attempt: rw::redirect::Attempt| {
            // TODO: make configurable
            if attempt.previous().len() > 10 {
                attempt.error("too many redirects")
            } else if HANDLED_SCHEMES.contains(&attempt.url().scheme()) {
                trace!(url = attempt.url().as_str(), "Found destination");
                attempt.stop()
            } else {
                trace!(url = attempt.url().as_str(), "Following redirect");
                attempt.follow()
            }
        };

        let client = rw::Client::builder()
            .user_agent("icechunk")
            .redirect(rw::redirect::Policy::custom(redirect))
            .build()
            .map_err(|e| {
                StorageError::from(StorageErrorKind::BadRedirect(format!(
                    "Cannot build http client for redirect Storage instance: {e}"
                )))
            })?;

        let req = client.get(self.url.clone()).build().map_err(|e| {
            StorageError::from(StorageErrorKind::BadRedirect(format!(
                "Cannot build http request for redirect Storage instance: {e}"
            )))
        })?;
        let res = client.execute(req).await.map_err(|e| {
            StorageError::from(StorageErrorKind::BadRedirect(format!(
                "Request to redirect url ({}) failed, cannot find target Storage instance: {e}", &self.url
            )))
        })?;
        let storage_url = res.headers().get("location").ok_or_else(|| {
            StorageError::from(StorageErrorKind::BadRedirect(
                "Redirect Storage response must be a redirect, no location header detected".to_string()
            ))
        })?.to_str().map_err(|e| {
            StorageError::from(StorageErrorKind::BadRedirect(format!(
                "Request to redirect url ({}) doesn't return a proper redirect to a known Storage protocol: {e}", &self.url
            )))
        })?;

        self.mk_storage(storage_url).await
    }

    async fn mk_storage(&self, url: &str) -> StorageResult<Arc<dyn Storage>> {
        let url = Url::parse(url).map_err(|e| {
            StorageError::from(StorageErrorKind::BadRedirect(format!(
                "Storage url cannot be parsed ({url}): {e}"
            )))
        })?;
        match url.scheme() {
            "s3" => {
                let (bucket, prefix) = repo_location(&url)?;
                // TODO: figure out the region for the bucket using HeadBucket or something
                let region = repo_region(&url)?;
                debug!(bucket, prefix, region, "Creating S3 Storage from redirect");

                // TODO: make more parameters configurable using the query
                new_s3_storage(
                    S3Options {
                        region: Some(region),
                        endpoint_url: None,
                        anonymous: true,
                        allow_http: false,
                        force_path_style: false,
                        network_stream_timeout_seconds: None,
                        requester_pays: false,
                    },
                    bucket,
                    Some(prefix),
                    Some(S3Credentials::Anonymous),
                )
            }
            "r2" => {
                let (bucket, prefix) = repo_location(&url)?;
                let region = repo_region(&url).ok();
                let account_id = repo_account_id(&url)?;
                debug!(bucket, prefix, region, "Creating R2 Storage from redirect");
                // TODO: make more parameters configurable using the query
                new_r2_storage(
                    S3Options {
                        region,
                        endpoint_url: None,
                        anonymous: true,
                        allow_http: false,
                        force_path_style: false,
                        network_stream_timeout_seconds: None,
                        requester_pays: false,
                    },
                    Some(bucket),
                    Some(prefix),
                    Some(account_id),
                    Some(S3Credentials::Anonymous),
                )
            }
            "tigris" => {
                let (bucket, prefix) = repo_location(&url)?;
                let region = repo_region(&url).ok();
                debug!(bucket, prefix, region, "Creating R2 Storage from redirect");
                // TODO: make more parameters configurable using the query
                new_tigris_storage(
                    S3Options {
                        region,
                        endpoint_url: None,
                        anonymous: true,
                        allow_http: false,
                        force_path_style: false,
                        network_stream_timeout_seconds: None,
                        requester_pays: false,
                    },
                    bucket,
                    Some(prefix),
                    Some(S3Credentials::Anonymous),
                    true,
                )
            }

            "http+icechunk" | "http+ic" | "https+icechunk" | "https+ic" => {
                let mut base_url = url.clone();
                // we can expect here because the scheme is already matched as http[s]
                #[allow(clippy::expect_used)]
                let new_scheme = base_url
                    .scheme()
                    .split_once('+')
                    .map(|(x, _)| x)
                    .expect("Internal error, bad url scheme")
                    .to_string();
                #[allow(clippy::expect_used)]
                base_url
                    .set_scheme(new_scheme.as_str())
                    .expect("Internal error, cannot set url scheme");
                new_http_storage(base_url.to_string().as_str(), None)
            }
            "gs" | "gcs" => {
                let (bucket, prefix) = repo_location(&url)?;
                new_gcs_storage(
                    bucket,
                    Some(prefix),
                    Some(GcsCredentials::Anonymous),
                    None,
                )
            }
            _ => Err(StorageErrorKind::BadRedirect(format!(
                "Bad URL for redirect Storage, unknown scheme: {url}"
            ))
            .into()),
        }
    }
}

fn repo_location(url: &Url) -> StorageResult<(String, String)> {
    let bucket = url
        .host()
        .ok_or_else(|| {
            StorageError::from(StorageErrorKind::BadRedirect(format!(
                "Storage url doesn't have a host to indicate the bucket ({url})"
            )))
        })?
        .to_string();
    let path = url.path();
    if !path.starts_with('/') {
        return Err(StorageErrorKind::BadRedirect(format!(
            "Invalid Storage URL, must have a path to indicate bucket prefix: {url}"
        ))
        .into());
    }
    let prefix = path[1..].to_string();
    Ok((bucket, prefix))
}

fn repo_region(url: &Url) -> StorageResult<String> {
    let res = url
        .query_pairs()
        .find(|(name, _)| name == "region")
        .ok_or_else(|| {
            StorageErrorKind::BadRedirect(format!(
                "Invalid Storage URL, must have a region query parameter: {url}"
            ))
        })?
        .1
        .to_string();
    Ok(res)
}

fn repo_account_id(url: &Url) -> StorageResult<String> {
    let res = url
        .query_pairs()
        .find(|(name, _)| name == "account_id")
        .ok_or_else(|| {
            StorageErrorKind::BadRedirect(format!(
                "Invalid Storage URL, must have an account_id query parameter: {url}"
            ))
        })?
        .1
        .to_string();
    Ok(res)
}

impl private::Sealed for RedirectStorage {}

impl std::fmt::Display for RedirectStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RedirectStorage(url={})", self.url)
    }
}

#[async_trait]
#[typetag::serde]
impl Storage for RedirectStorage {
    async fn can_write(&self) -> StorageResult<bool> {
        self.backend().await?.can_write().await
    }

    async fn default_settings(&self) -> StorageResult<Settings> {
        self.backend().await?.default_settings().await
    }

    async fn get_object_range(
        &self,
        settings: &Settings,
        path: &str,
        range: Option<&Range<u64>>,
    ) -> StorageResult<(
        Pin<Box<dyn Stream<Item = Result<Bytes, StorageError>> + Send>>,
        VersionInfo,
    )> {
        self.backend().await?.get_object_range(settings, path, range).await
    }

    async fn put_object(
        &self,
        settings: &Settings,
        path: &str,
        bytes: Bytes,
        content_type: Option<&str>,
        metadata: Vec<(String, String)>,
        previous_version: Option<&VersionInfo>,
    ) -> StorageResult<VersionedUpdateResult> {
        self.backend()
            .await?
            .put_object(settings, path, bytes, content_type, metadata, previous_version)
            .await
    }

    async fn copy_object(
        &self,
        settings: &Settings,
        from: &str,
        to: &str,
        content_type: Option<&str>,
        version: &VersionInfo,
    ) -> StorageResult<VersionedUpdateResult> {
        self.backend().await?.copy_object(settings, from, to, content_type, version).await
    }

    async fn list_objects<'a>(
        &'a self,
        settings: &Settings,
        prefix: &str,
    ) -> StorageResult<BoxStream<'a, StorageResult<ListInfo<String>>>> {
        self.backend().await?.list_objects(settings, prefix).await
    }

    async fn delete_batch(
        &self,
        settings: &Settings,
        prefix: &str,
        batch: Vec<(String, u64)>,
    ) -> StorageResult<DeleteObjectsResult> {
        self.backend().await?.delete_batch(settings, prefix, batch).await
    }

    async fn get_object_last_modified(
        &self,
        path: &str,
        settings: &Settings,
    ) -> StorageResult<DateTime<Utc>> {
        self.backend().await?.get_object_last_modified(path, settings).await
    }
}
