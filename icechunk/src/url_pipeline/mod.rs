//! Support for the `icechunk:` [URL pipeline](https://github.com/jbms/url-pipeline)
//! adapter scheme (ZEP 8).
//!
//! A URL pipeline such as
//! `s3://bucket/repo|icechunk://tag.v1/path/to/group|zarr3:` names a root
//! location (`s3://bucket/repo`), the Icechunk ref to read (`tag.v1`) and a
//! path inside the repository (`path/to/group`).
//!
//! - [`parse_icechunk_ref`] parses the body of an `icechunk:` segment.
//! - [`parse_root_url`] and [`storage_from_url`] map the root sub-URL to a
//!   [`Storage`](crate::Storage).
//! - [`open_from_url`] does both and opens a session.

mod reference;
mod storage;

use std::collections::HashMap;

pub use reference::{DEFAULT_BRANCH, parse_icechunk_ref};
pub use storage::{
    OpenFromUrlError, RootLocation, SUPPORTED_ROOT_SCHEMES, UrlStorageOptions,
    parse_root_url, storage_from_root, storage_from_url, validate_storage_url,
};

use thiserror::Error;

use crate::{
    Repository, RepositoryConfig, config::Credentials, error::ICError,
    repository::VersionInfo, session::Session,
};

/// Errors in the text of a URL pipeline, or in how it was asked to be opened.
///
/// These are user-input errors. Language bindings map them to their
/// "invalid URL" error (in Python, `ValueError`, which the zarr adapter
/// turns into `zarr.errors.URLPipelineError`).
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum UrlPipelineErrorKind {
    #[error("invalid `icechunk:` URL segment `icechunk:{body}`: {reason}")]
    InvalidIcechunkSegment { body: String, reason: String },
    #[error("invalid root URL `{url}`: {reason}")]
    InvalidRootUrl { url: String, reason: String },
    #[error(
        "unsupported root URL scheme `{scheme}:`; supported schemes are file:, memory:, \
         s3:, s3+http:, s3+https:, gs:, http: and https:"
    )]
    UnsupportedRootScheme { scheme: String },
    #[error("invalid storage options: {0}")]
    InvalidStorageOptions(String),
}

pub type UrlPipelineError = ICError<UrlPipelineErrorKind>;
pub type UrlPipelineResult<T> = Result<T, UrlPipelineError>;

/// Options for [`open_from_url`].
#[derive(Debug, Clone, Default)]
pub struct OpenFromUrlOptions {
    /// How to build storage for the root URL.
    pub storage: UrlStorageOptions,
    /// Repository configuration, layered over the persisted config.
    pub config: Option<RepositoryConfig>,
    /// Credentials for virtual chunk containers, passed to
    /// [`RepositoryBuilder::authorize_virtual_chunk_access`](crate::repository::RepositoryBuilder::authorize_virtual_chunk_access).
    pub authorize_virtual_chunk_access: HashMap<String, Option<Credentials>>,
}

/// A parsed `<root>|icechunk:<body>` pair, validated but not yet opened.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IcechunkUrl {
    pub root: RootLocation,
    pub version: VersionInfo,
    pub path: String,
}

/// Parse and validate a root URL and `icechunk:` segment body.
///
/// Performs every user-input check [`open_from_url`] performs, without
/// any I/O.
pub fn parse_icechunk_url(
    root_url: &str,
    icechunk_body: &str,
    options: &OpenFromUrlOptions,
) -> UrlPipelineResult<IcechunkUrl> {
    let root = validate_storage_url(root_url, &options.storage)?;
    let (version, path) = parse_icechunk_ref(icechunk_body)?;
    Ok(IcechunkUrl { root, version, path })
}

/// Open a session for a `<root_url>|icechunk:<icechunk_body>` URL pipeline.
///
/// `root_url` is the root sub-URL (for example `s3://bucket/prefix`) and
/// `icechunk_body` is the text after `icechunk:` (for example
/// `//tag.v1/path`). Returns a read-only session on the selected ref and
/// the residual path inside the repository.
pub async fn open_from_url(
    root_url: &str,
    icechunk_body: &str,
    options: OpenFromUrlOptions,
) -> Result<(Session, String), OpenFromUrlError> {
    let parsed = parse_icechunk_url(root_url, icechunk_body, &options)?;
    open_parsed(parsed, options).await
}

/// Open a session for an already-parsed [`IcechunkUrl`].
pub async fn open_parsed(
    parsed: IcechunkUrl,
    options: OpenFromUrlOptions,
) -> Result<(Session, String), OpenFromUrlError> {
    let storage = storage_from_root(&parsed.root, &options.storage).await?;
    let builder = Repository::open(storage)
        .authorize_virtual_chunk_access(options.authorize_virtual_chunk_access);
    let builder = match options.config {
        Some(config) => builder.config(config),
        None => builder,
    };
    let repo = builder.execute().await?;
    let session = repo.readonly_session(&parsed.version).await?;
    Ok((session, parsed.path))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::format::Path;
    use bytes::Bytes;

    async fn make_repo(url: &str) -> Repository {
        let storage = storage_from_url(url, &UrlStorageOptions::default()).await.unwrap();
        Repository::create(storage).execute().await.unwrap()
    }

    #[icechunk_macros::tokio_test]
    async fn test_open_from_url_refs_and_paths() {
        let dir = tempfile::TempDir::new().unwrap();
        let root = format!("file://{}/", dir.path().display());
        let repo = make_repo(&root).await;

        let mut session = repo.writable_session("main").await.unwrap();
        session.add_group(Path::root(), Bytes::new()).await.unwrap();
        session.add_group("/a".try_into().unwrap(), Bytes::new()).await.unwrap();
        let snap = session.commit("first").execute().await.unwrap();
        repo.create_tag("v1", &snap).await.unwrap();
        repo.create_branch("dev", &snap).await.unwrap();

        for (body, expected_snapshot, expected_path) in [
            ("", &snap, ""),
            ("a/b/", &snap, "a/b"),
            ("//branch.main/a", &snap, "a"),
            ("//branch.dev", &snap, ""),
            ("//tag.v1/a/", &snap, "a"),
        ] {
            let (session, path) =
                open_from_url(&root, body, OpenFromUrlOptions::default()).await.unwrap();
            assert!(session.read_only(), "{body}");
            assert_eq!(session.snapshot_id(), expected_snapshot, "{body}");
            assert_eq!(path, expected_path, "{body}");
        }

        let (session, path) =
            open_from_url(&root, &format!("//{snap}/a"), OpenFromUrlOptions::default())
                .await
                .unwrap();
        assert_eq!(session.snapshot_id(), &snap);
        assert_eq!(path, "a");

        // missing refs are repository errors, not URL errors
        let err = open_from_url(&root, "//tag.nope", OpenFromUrlOptions::default())
            .await
            .unwrap_err();
        assert!(matches!(err, OpenFromUrlError::Repository(_)), "{err}");
        let err = open_from_url(&root, "//branch.nope", OpenFromUrlOptions::default())
            .await
            .unwrap_err();
        assert!(matches!(err, OpenFromUrlError::Repository(_)), "{err}");
    }

    #[icechunk_macros::tokio_test]
    async fn test_open_from_url_memory() {
        let root = "memory:open-from-url-test";
        let repo = make_repo(root).await;
        let snap = repo.lookup_branch("main").await.unwrap();
        let (session, path) = open_from_url(
            "memory://open-from-url-test/",
            "x",
            OpenFromUrlOptions::default(),
        )
        .await
        .unwrap();
        assert_eq!(session.snapshot_id(), &snap);
        assert_eq!(path, "x");
    }

    #[icechunk_macros::tokio_test]
    async fn test_open_from_url_user_errors() {
        let opts = OpenFromUrlOptions::default;
        for (root, body) in [
            ("file:relative/path", ""),
            ("zip:x", ""),
            ("file:///x", "//branch.a%2Fb"),
            ("file:///x", "//not-a-snapshot"),
        ] {
            let err = open_from_url(root, body, opts()).await.unwrap_err();
            assert!(matches!(err, OpenFromUrlError::Url(_)), "{root} {body}: {err}");
        }
    }
}
