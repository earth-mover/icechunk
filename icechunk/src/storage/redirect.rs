//! Read-only storage that follows HTTP redirects to the underlying backend.

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

use crate::config::{S3Credentials, S3Options};
use crate::storage::StorageErrorKind;
#[cfg(feature = "object-store-http")]
use crate::storage::new_http_storage;
#[cfg(feature = "object-store-gcs")]
use crate::{config::GcsCredentials, storage::new_gcs_storage};
#[cfg(feature = "s3")]
use crate::{
    new_s3_storage,
    storage::{new_r2_storage, new_tigris_storage},
};
use icechunk_storage::sealed;
use icechunk_types::ICResultExt as _;

use super::{
    DeleteObjectsResult, GetModifiedResult, ListInfo, RepositoryCreation, Settings,
    Storage, StorageError, StorageInfo, StorageResult, VersionInfo,
    VersionedUpdateResult,
};

#[derive(Debug, Serialize, Deserialize)]
pub struct RedirectStorage {
    url: Url,

    #[serde(skip)]
    backend: OnceCell<Arc<dyn Storage + Send + Sync>>,
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

    async fn backend(&self) -> StorageResult<&(dyn Storage + Send + Sync)> {
        self.backend.get_or_try_init(|| self.mk_backend()).await.map(|arc| arc.as_ref())
    }

    async fn mk_backend(&self) -> StorageResult<Arc<dyn Storage + Send + Sync>> {
        let redirect = |attempt: rw::redirect::Attempt<'_>| {
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
            .user_agent(crate::user_agent())
            .redirect(rw::redirect::Policy::custom(redirect))
            .build()
            .map_err(|e| {
                StorageErrorKind::BadRedirect(format!(
                    "Cannot build http client for redirect Storage instance: {e}"
                ))
            })
            .capture()?;

        let req = client
            .get(self.url.clone())
            .build()
            .map_err(|e| {
                StorageErrorKind::BadRedirect(format!(
                    "Cannot build http request for redirect Storage instance: {e}"
                ))
            })
            .capture()?;
        let res = client.execute(req).await.map_err(|e| {
            StorageErrorKind::BadRedirect(format!(
                "Request to redirect url ({}) failed, cannot find target Storage instance: {e}",
                &self.url
            ))
        }).capture()?;
        let storage_url = res.headers().get("location").ok_or_else(|| {
            StorageErrorKind::BadRedirect(
                "Redirect Storage response must be a redirect, no location header detected".to_string()
            )
        }).capture()?.to_str().map_err(|e| {
            StorageErrorKind::BadRedirect(format!(
                "Request to redirect url ({}) doesn't return a proper redirect to a known Storage protocol: {e}", &self.url
            ))
        }).capture()?;

        self.mk_storage(storage_url).await
    }

    async fn mk_storage(
        &self,
        url: &str,
    ) -> StorageResult<Arc<dyn Storage + Send + Sync>> {
        let url = Url::parse(url)
            .map_err(|e| {
                StorageErrorKind::BadRedirect(format!(
                    "Storage url cannot be parsed ({url}): {e}"
                ))
            })
            .capture()?;
        match url.scheme() {
            #[cfg(feature = "s3")]
            "s3" => {
                let (bucket, prefix) = repo_location(&url)?;
                // TODO: figure out the region for the bucket using HeadBucket or something
                let region = repo_region(&url)?;
                debug!(bucket, prefix, region, "Creating S3 Storage from redirect");

                // TODO: make more parameters configurable using the query
                new_s3_storage(
                    S3Options::default().with_region(region).with_anonymous(true),
                    bucket,
                    Some(prefix),
                    Some(S3Credentials::Anonymous),
                    Vec::new(),
                    Vec::new(),
                    None, // auto-detect key layout
                )
            }
            #[cfg(not(feature = "s3"))]
            "s3" => Err(StorageErrorKind::BadRedirect(
                "Redirect target uses `s3://` but the `s3` feature is disabled"
                    .to_string(),
            ))
            .capture(),
            #[cfg(feature = "s3")]
            "r2" => {
                let (bucket, prefix) = repo_location(&url)?;
                let region = repo_region(&url).ok();
                let account_id = repo_account_id(&url)?;
                debug!(bucket, prefix, region, "Creating R2 Storage from redirect");
                // TODO: make more parameters configurable using the query
                let mut opts = S3Options::default().with_anonymous(true);
                if let Some(region) = region {
                    opts = opts.with_region(region);
                }
                new_r2_storage(
                    opts,
                    Some(bucket),
                    Some(prefix),
                    Some(account_id),
                    Some(S3Credentials::Anonymous),
                    Vec::new(),
                    Vec::new(),
                    None, // auto-detect key layout
                )
            }
            #[cfg(not(feature = "s3"))]
            "r2" => Err(StorageErrorKind::BadRedirect(
                "Redirect target uses `r2://` but the `s3` feature is disabled"
                    .to_string(),
            ))
            .capture(),
            #[cfg(feature = "s3")]
            "tigris" => {
                let (bucket, prefix) = repo_location(&url)?;
                let region = repo_region(&url).ok();
                debug!(bucket, prefix, region, "Creating R2 Storage from redirect");
                // TODO: make more parameters configurable using the query
                let mut opts = S3Options::default().with_anonymous(true);
                if let Some(region) = region {
                    opts = opts.with_region(region);
                }
                new_tigris_storage(
                    opts,
                    bucket,
                    Some(prefix),
                    Some(S3Credentials::Anonymous),
                    true,
                    Vec::new(),
                    Vec::new(),
                    None, // auto-detect key layout
                )
            }
            #[cfg(not(feature = "s3"))]
            "tigris" => Err(StorageErrorKind::BadRedirect(
                "Redirect target uses `tigris://` but the `s3` feature is disabled"
                    .to_string(),
            ))
            .capture(),

            #[cfg(feature = "object-store-http")]
            "http+icechunk" | "http+ic" | "https+icechunk" | "https+ic" => {
                // `Url::set_scheme` refuses conversions between "special"
                // (http/https) and non-special (http+icechunk) schemes, so we
                // can't mutate the scheme in place. Rebuild the URL string with
                // the `+icechunk`/`+ic` tag stripped from the scheme instead.
                let scheme = url.scheme();
                let base_scheme =
                    scheme.split_once('+').map_or(scheme, |(base, _)| base);
                let http_url = [base_scheme, &url.as_str()[scheme.len()..]].concat();
                new_http_storage(http_url.as_str(), None, None)
            }
            #[cfg(not(feature = "object-store-http"))]
            "http+icechunk" | "http+ic" | "https+icechunk" | "https+ic" => Err(
                StorageErrorKind::BadRedirect(
                    "Redirect target uses `http+icechunk://` or `https+icechunk://`, but the `http-store` feature is disabled".to_string(),
                ),
            )
            .capture(),
            #[cfg(feature = "object-store-gcs")]
            "gs" | "gcs" => {
                let (bucket, prefix) = repo_location(&url)?;
                new_gcs_storage(
                    bucket,
                    Some(prefix),
                    Some(GcsCredentials::Anonymous),
                    None,
                    Vec::new(),
                    Vec::new(),
                )
            }
            #[cfg(not(feature = "object-store-gcs"))]
            "gs" | "gcs" => Err(StorageErrorKind::BadRedirect(
                "Redirect target uses `gs://`/`gcs://` but the `gcs` feature is disabled"
                    .to_string(),
            ))
            .capture(),
            _ => Err(StorageErrorKind::BadRedirect(format!(
                "Bad URL for redirect Storage, unknown scheme: {url}"
            )))
            .capture(),
        }
    }
}

fn repo_location(url: &Url) -> StorageResult<(String, String)> {
    let bucket = url
        .host()
        .ok_or_else(|| {
            StorageErrorKind::BadRedirect(format!(
                "Storage url doesn't have a host to indicate the bucket ({url})"
            ))
        })
        .capture()?
        .to_string();
    let path = url.path();
    if !path.starts_with('/') {
        return Err(StorageErrorKind::BadRedirect(format!(
            "Invalid Storage URL, must have a path to indicate bucket prefix: {url}"
        )))
        .capture();
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
        })
        .capture()?
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
        })
        .capture()?
        .1
        .to_string();
    Ok(res)
}

impl sealed::Sealed for RedirectStorage {}

impl std::fmt::Display for RedirectStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RedirectStorage(url={})", self.url)
    }
}

#[async_trait]
#[typetag::serde]
impl Storage for RedirectStorage {
    fn storage_info(&self) -> StorageInfo {
        StorageInfo {
            backend_type: "redirect",
            fields: vec![("url", self.url.to_string())],
        }
    }

    async fn can_write(&self) -> StorageResult<bool> {
        self.backend().await?.can_write().await
    }

    async fn can_create_repository(&self) -> StorageResult<RepositoryCreation> {
        self.backend().await?.can_create_repository().await
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

    async fn sum_object_sizes(
        &self,
        settings: &Settings,
        prefix: &str,
        shardable: bool,
    ) -> StorageResult<u64> {
        self.backend().await?.sum_object_sizes(settings, prefix, shardable).await
    }

    async fn sum_object_sizes_many(
        &self,
        settings: &Settings,
        prefixes: &[(&str, bool)],
    ) -> StorageResult<u64> {
        self.backend().await?.sum_object_sizes_many(settings, prefixes).await
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

    async fn get_object_conditional(
        &self,
        settings: &Settings,
        path: &str,
        previous_version: Option<&VersionInfo>,
    ) -> StorageResult<GetModifiedResult> {
        self.backend()
            .await?
            .get_object_conditional(settings, path, previous_version)
            .await
    }
}

#[cfg(all(test, feature = "object-store-http"))]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_mk_storage_strips_icechunk_scheme_tag() {
        let redirect =
            RedirectStorage::new(Url::parse("http://redirect.example.com/").unwrap());
        for (tagged, expected) in [
            ("http+icechunk", "http"),
            ("http+ic", "http"),
            ("https+icechunk", "https"),
            ("https+ic", "https"),
        ] {
            let target = format!("{tagged}://storage.example.com/repo/prefix");
            let storage = redirect
                .mk_storage(&target)
                .await
                .unwrap_or_else(|e| panic!("mk_storage failed for {tagged}: {e:?}"));
            let info = storage.storage_info();
            assert_eq!(info.backend_type, "HTTP");
            let url = info
                .fields
                .iter()
                .find_map(|(k, v)| (*k == "url").then_some(v))
                .expect("HTTP storage_info must expose a url field");
            assert_eq!(url, &format!("{expected}://storage.example.com/repo/prefix"));
        }
    }
}
