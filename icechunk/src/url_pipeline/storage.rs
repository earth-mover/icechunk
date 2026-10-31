//! Mapping the root sub-URL of a URL pipeline to an Icechunk [`Storage`].
//!
//! | Root URL                                   | Storage                               |
//! |--------------------------------------------|---------------------------------------|
//! | `file:/abs/path`, `file:///abs/path`, `file://localhost/abs/path` | local filesystem |
//! | `memory:`, `memory:name`, `memory://name`  | in-memory, shared per name in the process |
//! | `s3://bucket[/prefix]`                     | S3                                    |
//! | `s3+http(s)://endpoint/bucket[/prefix]`    | S3-compatible endpoint, path-style    |
//! | `s3+https://bucket.s3[.region].amazonaws.com[/prefix]` | S3, virtual-hosted AWS    |
//! | `gs://bucket[/prefix]`                     | Google Cloud Storage                  |
//! | `http(s)://host[:port][/path]`             | HTTP (read-only)                      |

use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{Arc, LazyLock, Mutex},
};

use icechunk_types::ICResultExt as _;
use regex::Regex;
use thiserror::Error;

use crate::{
    Storage,
    config::{Credentials, GcsCredentials, S3Credentials, S3Options},
    storage::{self, StorageResult},
};

use super::{UrlPipelineErrorKind, UrlPipelineResult};

/// A parsed root sub-URL.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum RootLocation {
    /// `memory:` storage. Storages are shared, per `name`, for the lifetime of
    /// the process, so a repository created at `memory:x` can be reopened by
    /// a later `memory:x|icechunk:` URL.
    Memory { name: String },
    /// `file:` storage on an absolute local path.
    LocalFileSystem { path: PathBuf },
    /// `s3:`, or an `s3+https:` URL that names an AWS endpoint.
    S3 { bucket: String, prefix: Option<String>, region: Option<String> },
    /// `s3+http:`/`s3+https:` on a non-AWS endpoint. The first path component
    /// is the bucket and requests are path-style.
    S3Compatible {
        endpoint_url: String,
        allow_http: bool,
        bucket: String,
        prefix: Option<String>,
    },
    /// `gs:` storage.
    Gcs { bucket: String, prefix: Option<String> },
    /// `http:`/`https:` storage (read-only).
    Http { url: String },
}

impl RootLocation {
    /// The lowercase scheme this location was parsed from.
    pub fn scheme(&self) -> &'static str {
        match self {
            RootLocation::Memory { .. } => "memory",
            RootLocation::LocalFileSystem { .. } => "file",
            RootLocation::S3 { .. } => "s3",
            RootLocation::S3Compatible { allow_http: true, .. } => "s3+http",
            RootLocation::S3Compatible { allow_http: false, .. } => "s3+https",
            RootLocation::Gcs { .. } => "gs",
            RootLocation::Http { .. } => "http",
        }
    }

    /// Whether storage at this location can only be read.
    pub fn is_read_only(&self) -> bool {
        matches!(self, RootLocation::Http { .. })
    }
}

/// The root URL schemes [`parse_root_url`] understands.
pub const SUPPORTED_ROOT_SCHEMES: &[&str] =
    &["file", "memory", "s3", "s3+http", "s3+https", "gs", "http", "https"];

/// Options used to build [`Storage`] from a root URL.
///
/// Nothing is read implicitly from these options: an unset field means
/// "use the backend default". For S3 and GCS the default credentials are
/// `FromEnv` (the same default as the explicit storage constructors), and
/// only apply when neither `credentials` nor `anonymous` is set.
#[derive(Debug, Clone, Default)]
pub struct UrlStorageOptions {
    /// Credentials for the root storage. Must match the root scheme: S3
    /// credentials for `s3:`/`s3+http(s):`, GCS credentials for `gs:`.
    pub credentials: Option<Credentials>,
    /// Use anonymous (unsigned) access. Conflicts with `credentials`.
    pub anonymous: bool,
    /// S3 region. Only valid for S3 roots.
    pub region: Option<String>,
    /// Override the S3 endpoint (path-style requests are used). Only valid
    /// for S3 roots.
    pub endpoint_url: Option<String>,
}

fn invalid_root<T>(url: &str, reason: impl Into<String>) -> UrlPipelineResult<T> {
    Err(UrlPipelineErrorKind::InvalidRootUrl {
        url: url.to_string(),
        reason: reason.into(),
    })
    .capture()
}

fn invalid_options<T>(reason: impl Into<String>) -> UrlPipelineResult<T> {
    Err(UrlPipelineErrorKind::InvalidStorageOptions(reason.into())).capture()
}

/// Split `scheme:body`, lowercasing the scheme.
fn split_scheme(url: &str) -> Option<(String, &str)> {
    let (scheme, body) = url.split_once(':')?;
    let mut chars = scheme.chars();
    let first = chars.next()?;
    if !first.is_ascii_alphabetic()
        || !chars.all(|c| c.is_ascii_alphanumeric() || matches!(c, '+' | '-' | '.'))
    {
        return None;
    }
    Some((scheme.to_ascii_lowercase(), body))
}

/// Split `//authority[/path]` into `(authority, path)`, path without the
/// leading `/`.
fn split_authority(body: &str) -> Option<(&str, &str)> {
    let rest = body.strip_prefix("//")?;
    Some(rest.split_once('/').unwrap_or((rest, "")))
}

fn non_empty_prefix(path: &str) -> Option<String> {
    let path = path.trim_matches('/');
    if path.is_empty() { None } else { Some(path.to_string()) }
}

/// Split `bucket[/prefix]` from a `//bucket[/prefix]` body.
fn bucket_and_prefix(
    url: &str,
    scheme: &str,
    body: &str,
) -> UrlPipelineResult<(String, Option<String>)> {
    let Some((bucket, path)) = split_authority(body) else {
        return invalid_root(url, format!("expected `{scheme}://bucket[/path]`"));
    };
    if bucket.is_empty() {
        return invalid_root(url, "missing bucket name");
    }
    Ok((bucket.to_string(), non_empty_prefix(path)))
}

// `bucket.s3.amazonaws.com`, `bucket.s3.REGION.amazonaws.com`,
// `bucket.s3-REGION.amazonaws.com`: virtual-hosted AWS endpoints.
#[expect(clippy::expect_used)]
static AWS_VIRTUAL_HOST: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"^(?P<bucket>.+)\.s3(?:[.-](?P<region>[a-z0-9-]+))?\.amazonaws\.com$")
        .expect("valid regex")
});

// `s3.amazonaws.com`, `s3.REGION.amazonaws.com`, `s3-REGION.amazonaws.com`:
// path-style AWS endpoints.
#[expect(clippy::expect_used)]
static AWS_PATH_HOST: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"^s3(?:[.-](?P<region>[a-z0-9-]+))?\.amazonaws\.com$")
        .expect("valid regex")
});

fn parse_s3_http(url: &str, scheme: &str, body: &str) -> UrlPipelineResult<RootLocation> {
    let Some((authority, path)) = split_authority(body) else {
        return invalid_root(
            url,
            format!("expected `{scheme}://endpoint/bucket[/path]`"),
        );
    };
    if authority.is_empty() {
        return invalid_root(url, "missing endpoint host");
    }
    let host =
        authority.rsplit_once(':').map_or(authority, |(h, _)| h).to_ascii_lowercase();

    // The spec leaves it ambiguous whether the endpoint addresses a single
    // bucket (`https://bucket.example.com/path`) or many
    // (`https://example.com/bucket/path`). AWS hostnames say which form is
    // meant; for any other endpoint the path-style form is assumed, since S3
    // clients cannot address a bucket without knowing its name.
    if scheme == "s3+https" {
        if let Some(caps) = AWS_PATH_HOST.captures(&host) {
            let region = caps.name("region").map(|r| r.as_str().to_string());
            let (bucket, prefix) = path.split_once('/').unwrap_or((path, ""));
            if bucket.is_empty() {
                return invalid_root(url, "missing bucket name after the AWS endpoint");
            }
            return Ok(RootLocation::S3 {
                bucket: bucket.to_string(),
                prefix: non_empty_prefix(prefix),
                region,
            });
        }
        if let Some(caps) = AWS_VIRTUAL_HOST.captures(&host) {
            let bucket = caps.name("bucket").map_or("", |b| b.as_str());
            let region = caps.name("region").map(|r| r.as_str().to_string());
            return Ok(RootLocation::S3 {
                bucket: bucket.to_string(),
                prefix: non_empty_prefix(path),
                region,
            });
        }
    }

    let allow_http = scheme == "s3+http";
    let protocol = if allow_http { "http" } else { "https" };
    let (bucket, prefix) = path.split_once('/').unwrap_or((path, ""));
    if bucket.is_empty() {
        return invalid_root(
            url,
            "missing bucket name: the first path component after the endpoint \
             names the bucket",
        );
    }
    Ok(RootLocation::S3Compatible {
        endpoint_url: format!("{protocol}://{authority}"),
        allow_http,
        bucket: bucket.to_string(),
        prefix: non_empty_prefix(prefix),
    })
}

fn parse_file(url: &str, body: &str) -> UrlPipelineResult<RootLocation> {
    let path = if let Some(rest) = body.strip_prefix("//") {
        let (authority, path) = rest.split_once('/').map_or((rest, ""), |(a, p)| (a, p));
        if !(authority.is_empty() || authority.eq_ignore_ascii_case("localhost")) {
            return invalid_root(
                url,
                format!(
                    "unsupported authority `{authority}`; only an empty authority or \
                     `localhost` is allowed"
                ),
            );
        }
        if path.is_empty() && !rest.ends_with('/') {
            return invalid_root(url, "missing path");
        }
        format!("/{path}")
    } else {
        body.to_string()
    };
    if !path.starts_with('/') {
        return invalid_root(
            url,
            "`file:` URLs must carry an absolute path (`file:relative/path` is not allowed)",
        );
    }
    // a file URL spells a Windows drive path as `/C:/...`
    #[cfg(windows)]
    let path = {
        let bytes = path.as_bytes();
        if bytes.len() >= 3 && bytes[1].is_ascii_alphabetic() && bytes[2] == b':' {
            path[1..].to_string()
        } else {
            path
        }
    };
    Ok(RootLocation::LocalFileSystem { path: PathBuf::from(path) })
}

/// Parse the root sub-URL of a URL pipeline.
///
/// Root URLs carry no query or fragment. Percent-escapes are not decoded,
/// matching zarr-python's handling of pipeline roots.
pub fn parse_root_url(url: &str) -> UrlPipelineResult<RootLocation> {
    let Some((scheme, body)) = split_scheme(url) else {
        return invalid_root(
            url,
            "expected an absolute URL with a scheme, e.g. `file:/path`",
        );
    };
    if body.contains('?') || body.contains('#') {
        return invalid_root(url, "query strings and fragments are not supported");
    }
    match scheme.as_str() {
        "memory" => {
            // up to two `/` may follow `memory:`; they are not part of the path
            let name = body
                .strip_prefix("//")
                .or_else(|| body.strip_prefix('/'))
                .unwrap_or(body);
            Ok(RootLocation::Memory { name: name.trim_end_matches('/').to_string() })
        }
        "file" => parse_file(url, body),
        "s3" => {
            let (bucket, prefix) = bucket_and_prefix(url, &scheme, body)?;
            Ok(RootLocation::S3 { bucket, prefix, region: None })
        }
        "s3+http" | "s3+https" => parse_s3_http(url, &scheme, body),
        "gs" => {
            let (bucket, prefix) = bucket_and_prefix(url, &scheme, body)?;
            Ok(RootLocation::Gcs { bucket, prefix })
        }
        "http" | "https" => {
            let Some((authority, _)) = split_authority(body) else {
                return invalid_root(url, format!("expected `{scheme}://host[/path]`"));
            };
            if authority.is_empty() {
                return invalid_root(url, "missing host");
            }
            Ok(RootLocation::Http { url: format!("{scheme}:{body}") })
        }
        _ => Err(UrlPipelineErrorKind::UnsupportedRootScheme { scheme }).capture(),
    }
}

/// Validate `options` against `location`, producing the storage to build.
///
/// Errors here are user-input errors ([`UrlPipelineErrorKind`]); building
/// the storage itself happens in [`storage_from_root`].
fn s3_options_and_credentials(
    options: &UrlStorageOptions,
    region: Option<String>,
) -> UrlPipelineResult<(S3Options, Option<S3Credentials>)> {
    let credentials = match (&options.credentials, options.anonymous) {
        (Some(_), true) => {
            return invalid_options("pass either credentials or anonymous, not both");
        }
        (Some(Credentials::S3(c)), false) => Some(c.clone()),
        (Some(_), false) => {
            return invalid_options("S3 roots require S3 credentials");
        }
        (None, true) => Some(S3Credentials::Anonymous),
        (None, false) => None,
    };
    let mut config = S3Options::default();
    config.region = options.region.clone().or(region);
    config.anonymous = options.anonymous;
    if let Some(endpoint) = &options.endpoint_url {
        config.allow_http = endpoint.starts_with("http://");
        config.endpoint_url = Some(endpoint.clone());
        config.force_path_style = true;
    }
    Ok((config, credentials))
}

fn reject_s3_only_options(
    options: &UrlStorageOptions,
    scheme: &str,
) -> UrlPipelineResult<()> {
    if options.region.is_some() {
        return invalid_options(format!(
            "`region` is only valid for S3 roots, not `{scheme}:`"
        ));
    }
    if options.endpoint_url.is_some() {
        return invalid_options(format!(
            "`endpoint_url` is only valid for `s3:` roots, not `{scheme}:`"
        ));
    }
    Ok(())
}

fn reject_credentials(
    options: &UrlStorageOptions,
    scheme: &str,
) -> UrlPipelineResult<()> {
    if options.credentials.is_some() || options.anonymous {
        return invalid_options(format!(
            "`{scheme}:` roots take no credentials or anonymous flag"
        ));
    }
    Ok(())
}

/// Validated arguments for one storage constructor.
#[derive(Debug)]
enum StoragePlan {
    Memory(String),
    LocalFileSystem(PathBuf),
    S3 {
        config: S3Options,
        bucket: String,
        prefix: Option<String>,
        credentials: Option<S3Credentials>,
    },
    Gcs {
        bucket: String,
        prefix: Option<String>,
        credentials: Option<GcsCredentials>,
    },
    Http(String),
}

fn plan(
    location: &RootLocation,
    options: &UrlStorageOptions,
) -> UrlPipelineResult<StoragePlan> {
    let scheme = location.scheme();
    match location {
        RootLocation::Memory { name } => {
            reject_credentials(options, scheme)?;
            reject_s3_only_options(options, scheme)?;
            Ok(StoragePlan::Memory(name.clone()))
        }
        RootLocation::LocalFileSystem { path } => {
            reject_credentials(options, scheme)?;
            reject_s3_only_options(options, scheme)?;
            Ok(StoragePlan::LocalFileSystem(path.clone()))
        }
        RootLocation::Http { url } => {
            reject_credentials(options, scheme)?;
            reject_s3_only_options(options, scheme)?;
            Ok(StoragePlan::Http(url.clone()))
        }
        RootLocation::S3 { bucket, prefix, region } => {
            let (config, credentials) =
                s3_options_and_credentials(options, region.clone())?;
            Ok(StoragePlan::S3 {
                config,
                bucket: bucket.clone(),
                prefix: prefix.clone(),
                credentials,
            })
        }
        RootLocation::S3Compatible { endpoint_url, allow_http, bucket, prefix } => {
            if options.endpoint_url.is_some() {
                return invalid_options(format!(
                    "`endpoint_url` cannot be combined with an `{scheme}:` root, which \
                     already names the endpoint"
                ));
            }
            let (mut config, credentials) = s3_options_and_credentials(options, None)?;
            config.endpoint_url = Some(endpoint_url.clone());
            config.allow_http = *allow_http;
            config.force_path_style = true;
            Ok(StoragePlan::S3 {
                config,
                bucket: bucket.clone(),
                prefix: prefix.clone(),
                credentials,
            })
        }
        RootLocation::Gcs { bucket, prefix } => {
            reject_s3_only_options(options, scheme)?;
            let credentials = match (&options.credentials, options.anonymous) {
                (Some(_), true) => {
                    return invalid_options(
                        "pass either credentials or anonymous, not both",
                    );
                }
                (Some(Credentials::Gcs(c)), false) => Some(c.clone()),
                (Some(_), false) => {
                    return invalid_options("`gs:` roots require GCS credentials");
                }
                (None, true) => Some(GcsCredentials::Anonymous),
                (None, false) => None,
            };
            Ok(StoragePlan::Gcs {
                bucket: bucket.clone(),
                prefix: prefix.clone(),
                credentials,
            })
        }
    }
}

type SharedStorage = Arc<dyn Storage + Send + Sync>;

static MEMORY_STORAGES: LazyLock<Mutex<HashMap<String, SharedStorage>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

fn lookup_memory(name: &str) -> Option<SharedStorage> {
    let guard = MEMORY_STORAGES.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    guard.get(name).map(Arc::clone)
}

async fn memory_storage(name: &str) -> StorageResult<SharedStorage> {
    if let Some(storage) = lookup_memory(name) {
        return Ok(storage);
    }
    // create outside the lock; if another task won the race, use its storage
    let created = storage::new_in_memory_storage().await?;
    let mut guard =
        MEMORY_STORAGES.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    Ok(Arc::clone(guard.entry(name.to_string()).or_insert(created)))
}

/// Errors from building storage for, or opening a repository at, a URL.
#[derive(Debug, Error)]
pub enum OpenFromUrlError {
    /// The URL or options are invalid.
    #[error(transparent)]
    Url(#[from] super::UrlPipelineError),
    /// The storage backend failed.
    #[error(transparent)]
    Storage(#[from] crate::StorageError),
    /// Opening the repository or session failed.
    #[error(transparent)]
    Repository(#[from] crate::repository::RepositoryError),
}

/// Validate a root URL and options without building any storage.
///
/// This performs every user-input check [`storage_from_url`] performs, so
/// callers can report URL errors before starting asynchronous work.
pub fn validate_storage_url(
    url: &str,
    options: &UrlStorageOptions,
) -> UrlPipelineResult<RootLocation> {
    let location = parse_root_url(url)?;
    plan(&location, options)?;
    Ok(location)
}

/// Build the [`Storage`] for a parsed root location.
pub async fn storage_from_root(
    location: &RootLocation,
    options: &UrlStorageOptions,
) -> Result<SharedStorage, OpenFromUrlError> {
    Ok(match plan(location, options)? {
        StoragePlan::Memory(name) => memory_storage(&name).await?,
        StoragePlan::LocalFileSystem(path) => {
            storage::new_local_filesystem_storage(&path).await?
        }
        StoragePlan::S3 { config, bucket, prefix, credentials } => {
            storage::new_s3_storage(
                config,
                bucket,
                prefix,
                credentials,
                vec![],
                vec![],
                None,
            )?
        }
        StoragePlan::Gcs { bucket, prefix, credentials } => {
            storage::new_gcs_storage(bucket, prefix, credentials, None, vec![], vec![])?
        }
        StoragePlan::Http(url) => {
            let config = url
                .starts_with("http:")
                .then(|| HashMap::from([("allow_http".to_string(), "true".to_string())]));
            storage::new_http_storage(&url, config, None)?
        }
    })
}

/// Build the [`Storage`] for a root sub-URL such as `s3://bucket/prefix`.
///
/// See the [module docs](self) for the supported schemes.
pub async fn storage_from_url(
    url: &str,
    options: &UrlStorageOptions,
) -> Result<SharedStorage, OpenFromUrlError> {
    let location = parse_root_url(url)?;
    storage_from_root(&location, options).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    fn s3(bucket: &str, prefix: Option<&str>) -> RootLocation {
        RootLocation::S3 {
            bucket: bucket.to_string(),
            prefix: prefix.map(str::to_string),
            region: None,
        }
    }

    fn file(path: &str) -> RootLocation {
        RootLocation::LocalFileSystem { path: PathBuf::from(path) }
    }

    fn memory(name: &str) -> RootLocation {
        RootLocation::Memory { name: name.to_string() }
    }

    #[rstest]
    // file: examples from `schemes/file.md`
    #[case("file:/", file("/"))]
    #[case("file:/a", file("/a"))]
    #[case("file:/path/to/file.txt", file("/path/to/file.txt"))]
    #[case("file://localhost/path/to/file.txt", file("/path/to/file.txt"))]
    #[case("file://LOCALHOST/path/to/file.txt", file("/path/to/file.txt"))]
    #[case("file:///path/to/file.txt", file("/path/to/file.txt"))]
    #[case("file:///path/to/repo.zarr.icechunk/", file("/path/to/repo.zarr.icechunk/"))]
    #[case("FILE:///x", file("/x"))]
    #[case("file:///", file("/"))]
    // memory: examples from `schemes/memory.md`
    #[case("memory:", memory(""))]
    #[case("memory:/", memory(""))]
    #[case("memory://", memory(""))]
    #[case("memory:path/to/resource", memory("path/to/resource"))]
    #[case("memory:a", memory("a"))]
    #[case(
        "memory:another@path+with,lots&of(special;characters)*_!-$'",
        memory("another@path+with,lots&of(special;characters)*_!-$'")
    )]
    #[case("memory:/path/to/resource", memory("path/to/resource"))]
    #[case("memory://path/to/resource", memory("path/to/resource"))]
    #[case("memory:a/", memory("a"))]
    // s3: examples from `schemes/s3.md`
    #[case("s3://bucket", s3("bucket", None))]
    #[case("s3://bucket/", s3("bucket", None))]
    #[case("s3://bucket/path/within/bucket", s3("bucket", Some("path/within/bucket")))]
    #[case("s3://aaa", s3("aaa", None))]
    #[case("s3://bucket/path/", s3("bucket", Some("path")))]
    // s3+http(s): examples from `schemes/s3+http.md`
    #[case(
        "s3+https://mybucket.s3.amazonaws.com/path/to/file",
        s3("mybucket", Some("path/to/file"))
    )]
    #[case(
        "s3+https://s3.amazonaws.com/mybucket/path/to/file",
        s3("mybucket", Some("path/to/file"))
    )]
    #[case(
        "s3+https://mybucket.s3.us-west-2.amazonaws.com/p",
        RootLocation::S3 { bucket: "mybucket".into(), prefix: Some("p".into()), region: Some("us-west-2".into()) }
    )]
    #[case(
        "s3+https://s3.eu-west-1.amazonaws.com/b/p",
        RootLocation::S3 { bucket: "b".into(), prefix: Some("p".into()), region: Some("eu-west-1".into()) }
    )]
    #[case(
        "s3+http://localhost:9000/testbucket/some/prefix",
        RootLocation::S3Compatible {
            endpoint_url: "http://localhost:9000".into(),
            allow_http: true,
            bucket: "testbucket".into(),
            prefix: Some("some/prefix".into()),
        }
    )]
    #[case(
        "s3+https://example.com/path/to/database.icechunk/",
        RootLocation::S3Compatible {
            endpoint_url: "https://example.com".into(),
            allow_http: false,
            bucket: "path".into(),
            prefix: Some("to/database.icechunk".into()),
        }
    )]
    #[case(
        "s3+http://example.com/bucket",
        RootLocation::S3Compatible {
            endpoint_url: "http://example.com".into(),
            allow_http: true,
            bucket: "bucket".into(),
            prefix: None,
        }
    )]
    // gs: examples from `schemes/gs.md`
    #[case("gs://bucket", RootLocation::Gcs { bucket: "bucket".into(), prefix: None })]
    #[case("gs://bucket/", RootLocation::Gcs { bucket: "bucket".into(), prefix: None })]
    #[case(
        "gs://bucket/path/within/bucket",
        RootLocation::Gcs { bucket: "bucket".into(), prefix: Some("path/within/bucket".into()) }
    )]
    #[case("gs://aaa", RootLocation::Gcs { bucket: "aaa".into(), prefix: None })]
    // http(s): examples from `schemes/http.md`
    #[case(
        "https://example.com/path/to/resource%20name",
        RootLocation::Http { url: "https://example.com/path/to/resource%20name".into() }
    )]
    #[case("http://a", RootLocation::Http { url: "http://a".into() })]
    #[case("http://example.com", RootLocation::Http { url: "http://example.com".into() })]
    #[case(
        "https://server.example.com:1234/path/to/array",
        RootLocation::Http { url: "https://server.example.com:1234/path/to/array".into() }
    )]
    #[case("http://192.168.10.1:1234", RootLocation::Http { url: "http://192.168.10.1:1234".into() })]
    #[case("http://[::1]", RootLocation::Http { url: "http://[::1]".into() })]
    #[case("HTTP://example.com/", RootLocation::Http { url: "http://example.com/".into() })]
    fn test_parse_root_valid(#[case] url: &str, #[case] expected: RootLocation) {
        assert_eq!(parse_root_url(url).unwrap(), expected, "url: {url:?}");
    }

    #[rstest]
    // relative or missing file paths
    #[case("file:relative/path")]
    #[case("file:")]
    #[case("file://")]
    #[case("file://somehost/share/path")]
    // queries and fragments
    #[case("file:///x?y=1")]
    #[case("s3://bucket/x#frag")]
    #[case("https://example.com/path/to/resource?query=value")]
    // missing buckets and hosts
    #[case("s3://")]
    #[case("s3:bucket")]
    #[case("s3:///prefix")]
    #[case("gs://")]
    #[case("s3+http://example.com")]
    #[case("s3+http://example.com/")]
    #[case("s3+https://s3.amazonaws.com/")]
    #[case("s3+https:///bucket")]
    #[case("http://")]
    #[case("https:/no-authority")]
    // no scheme
    #[case("/abs/path")]
    #[case("relative/path")]
    fn test_parse_root_invalid(#[case] url: &str) {
        let err = parse_root_url(url).unwrap_err();
        assert!(
            matches!(err.kind, UrlPipelineErrorKind::InvalidRootUrl { .. }),
            "url: {url:?}, err: {err}"
        );
    }

    #[rstest]
    #[case("zip://x", "zip")]
    #[case("az://container/path", "az")]
    #[case("arraylake://org/repo", "arraylake")]
    #[case("vendor.scheme://org/repo", "vendor.scheme")]
    #[case("Unknown:x", "unknown")]
    fn test_parse_root_unsupported_scheme(#[case] url: &str, #[case] scheme: &str) {
        let err = parse_root_url(url).unwrap_err();
        match err.kind {
            UrlPipelineErrorKind::UnsupportedRootScheme { scheme: s } => {
                assert_eq!(s, scheme);
            }
            other => panic!("unexpected error {other:?}"),
        }
    }

    #[test]
    fn test_supported_schemes_parse() {
        for scheme in SUPPORTED_ROOT_SCHEMES {
            let url = format!("{scheme}://example.com/bucket/path");
            let err = parse_root_url(&url).err();
            assert!(
                !matches!(
                    err.map(|e| e.kind),
                    Some(UrlPipelineErrorKind::UnsupportedRootScheme { .. })
                ),
                "{scheme} should be supported"
            );
        }
    }

    #[test]
    fn test_options_validation() {
        let static_s3 = Credentials::S3(S3Credentials::Anonymous);
        let gcs = Credentials::Gcs(GcsCredentials::Anonymous);
        let ok =
            |url: &str, options: UrlStorageOptions| validate_storage_url(url, &options);

        // credentials must match the scheme
        assert!(
            ok(
                "s3://b",
                UrlStorageOptions {
                    credentials: Some(static_s3.clone()),
                    ..Default::default()
                }
            )
            .is_ok()
        );
        assert!(
            ok(
                "s3://b",
                UrlStorageOptions {
                    credentials: Some(gcs.clone()),
                    ..Default::default()
                }
            )
            .is_err()
        );
        assert!(
            ok(
                "gs://b",
                UrlStorageOptions {
                    credentials: Some(gcs.clone()),
                    ..Default::default()
                }
            )
            .is_ok()
        );
        assert!(
            ok(
                "gs://b",
                UrlStorageOptions {
                    credentials: Some(static_s3.clone()),
                    ..Default::default()
                }
            )
            .is_err()
        );
        // credentials and anonymous conflict
        assert!(
            ok(
                "s3://b",
                UrlStorageOptions {
                    credentials: Some(static_s3.clone()),
                    anonymous: true,
                    ..Default::default()
                }
            )
            .is_err()
        );
        // local and memory roots take no credentials or S3 options
        for url in ["file:///x", "memory:x", "https://example.com/x"] {
            assert!(ok(url, UrlStorageOptions::default()).is_ok(), "{url}");
            assert!(
                ok(url, UrlStorageOptions { anonymous: true, ..Default::default() })
                    .is_err(),
                "{url}"
            );
            assert!(
                ok(
                    url,
                    UrlStorageOptions {
                        credentials: Some(static_s3.clone()),
                        ..Default::default()
                    }
                )
                .is_err(),
                "{url}"
            );
            assert!(
                ok(
                    url,
                    UrlStorageOptions { region: Some("r".into()), ..Default::default() }
                )
                .is_err(),
                "{url}"
            );
        }
        // endpoint overrides apply to s3: but not to s3+http(s):, which names one
        assert!(
            ok(
                "s3://b",
                UrlStorageOptions {
                    endpoint_url: Some("http://localhost:9000".into()),
                    ..Default::default()
                }
            )
            .is_ok()
        );
        assert!(
            ok(
                "s3+http://h/b",
                UrlStorageOptions {
                    endpoint_url: Some("http://localhost:9000".into()),
                    ..Default::default()
                }
            )
            .is_err()
        );
        assert!(
            ok(
                "gs://b",
                UrlStorageOptions { region: Some("r".into()), ..Default::default() }
            )
            .is_err()
        );
    }

    #[test]
    fn test_s3_plan() {
        let options = UrlStorageOptions {
            endpoint_url: Some("http://localhost:9000".into()),
            region: Some("us-east-1".into()),
            anonymous: true,
            ..Default::default()
        };
        let StoragePlan::S3 { config, bucket, prefix, credentials } =
            plan(&parse_root_url("s3://b/p").unwrap(), &options).unwrap()
        else {
            panic!("expected S3")
        };
        assert_eq!(bucket, "b");
        assert_eq!(prefix.as_deref(), Some("p"));
        assert!(matches!(credentials, Some(S3Credentials::Anonymous)));
        assert_eq!(config.endpoint_url.as_deref(), Some("http://localhost:9000"));
        assert!(config.allow_http && config.force_path_style && config.anonymous);
        assert_eq!(config.region.as_deref(), Some("us-east-1"));

        // no credentials given: left unset, the constructor defaults to FromEnv
        let StoragePlan::S3 { config, credentials, .. } = plan(
            &parse_root_url("s3+https://minio.example.com/b").unwrap(),
            &UrlStorageOptions::default(),
        )
        .unwrap() else {
            panic!("expected S3")
        };
        assert!(credentials.is_none());
        assert_eq!(config.endpoint_url.as_deref(), Some("https://minio.example.com"));
        assert!(!config.allow_http && config.force_path_style);
    }

    #[icechunk_macros::tokio_test]
    async fn test_memory_storage_is_shared_per_name() {
        let opts = UrlStorageOptions::default();
        let a1 = storage_from_url("memory:shared-test-a", &opts).await.unwrap();
        let a2 = storage_from_url("memory://shared-test-a/", &opts).await.unwrap();
        let b = storage_from_url("memory:shared-test-b", &opts).await.unwrap();
        assert!(Arc::ptr_eq(&a1, &a2));
        assert!(!Arc::ptr_eq(&a1, &b));
    }

    #[icechunk_macros::tokio_test]
    async fn test_local_storage_from_url() {
        let dir = tempfile::TempDir::new().unwrap();
        let url = format!("file://{}", dir.path().display());
        storage_from_url(&url, &UrlStorageOptions::default()).await.unwrap();
    }
}
