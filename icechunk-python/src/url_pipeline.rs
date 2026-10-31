//! Python bindings for `icechunk::url_pipeline` (the `icechunk:` URL
//! pipeline adapter, ZEP 8).
//!
//! URL and option errors are raised as `ValueError` synchronously, when the
//! function is called; only I/O happens in the returned awaitable.

use std::{collections::HashMap, sync::Arc};

use icechunk::{
    repository::VersionInfo,
    url_pipeline::{
        self, OpenFromUrlError, OpenFromUrlOptions, UrlPipelineError, UrlStorageOptions,
    },
};
use pyo3::{exceptions::PyValueError, prelude::*};
use tokio::sync::RwLock;

use crate::{
    config::{PyCredentials, PyRepositoryConfig, PyStorage},
    errors::PyIcechunkStoreError,
    session::PySession,
};

fn url_error(err: &UrlPipelineError) -> PyErr {
    PyValueError::new_err(err.kind.to_string())
}

fn open_error(err: OpenFromUrlError) -> PyErr {
    match err {
        OpenFromUrlError::Url(e) => url_error(&e),
        OpenFromUrlError::Storage(e) => PyIcechunkStoreError::StorageError(e).into(),
        OpenFromUrlError::Repository(e) => {
            PyIcechunkStoreError::RepositoryError(e).into()
        }
    }
}

fn storage_options(
    credentials: Option<PyCredentials>,
    anonymous: bool,
    region: Option<String>,
    endpoint_url: Option<String>,
) -> UrlStorageOptions {
    UrlStorageOptions {
        credentials: credentials.map(Into::into),
        anonymous,
        region,
        endpoint_url,
    }
}

/// Parse the body of an `icechunk:` URL segment.
///
/// Returns `(kind, name, path)`, where `kind` is `"branch"`, `"tag"` or
/// `"snapshot"`.
#[pyfunction]
pub(crate) fn _parse_url_ref(body: &str) -> PyResult<(String, String, String)> {
    let (version, path) =
        url_pipeline::parse_icechunk_ref(body).map_err(|e| url_error(&e))?;
    let (kind, name) = match version {
        VersionInfo::BranchTipRef(name) => ("branch", name),
        VersionInfo::TagRef(name) => ("tag", name),
        VersionInfo::SnapshotId(id) => ("snapshot", String::from(&id)),
        _ => return Err(PyValueError::new_err("unexpected ref kind")),
    };
    Ok((kind.to_string(), name, path))
}

/// The root URL schemes the `icechunk:` adapter maps to storage natively.
#[pyfunction]
pub(crate) fn _url_root_schemes() -> Vec<&'static str> {
    url_pipeline::SUPPORTED_ROOT_SCHEMES.to_vec()
}

/// Build a `Storage` from a root URL such as `s3://bucket/prefix`.
#[pyfunction]
#[pyo3(signature = (url, *, credentials = None, anonymous = false, region = None, endpoint_url = None))]
pub(crate) fn _storage_from_url(
    py: Python<'_>,
    url: &str,
    credentials: Option<PyCredentials>,
    anonymous: bool,
    region: Option<String>,
    endpoint_url: Option<String>,
) -> PyResult<PyStorage> {
    let options = storage_options(credentials, anonymous, region, endpoint_url);
    let location =
        url_pipeline::validate_storage_url(url, &options).map_err(|e| url_error(&e))?;
    // This function calls block_on, so we need to allow other thread python to make progress
    py.detach(move || {
        pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
            let storage = url_pipeline::storage_from_root(&location, &options)
                .await
                .map_err(open_error)?;
            Ok(PyStorage(storage))
        })
    })
}

/// Open a read-only session for `<root_url>|icechunk:<icechunk_body>`.
///
/// Returns an awaitable of `(session, residual_path)`. URL and option
/// errors raise `ValueError` immediately, before any I/O.
#[pyfunction]
#[pyo3(signature = (
    root_url,
    icechunk_body,
    *,
    credentials = None,
    anonymous = false,
    region = None,
    endpoint_url = None,
    config = None,
    authorize_virtual_chunk_access = None,
))]
#[expect(clippy::too_many_arguments)]
pub(crate) fn _open_session_from_url_async<'py>(
    py: Python<'py>,
    root_url: &str,
    icechunk_body: &str,
    credentials: Option<PyCredentials>,
    anonymous: bool,
    region: Option<String>,
    endpoint_url: Option<String>,
    config: Option<&PyRepositoryConfig>,
    authorize_virtual_chunk_access: Option<HashMap<String, Option<PyCredentials>>>,
) -> PyResult<Bound<'py, PyAny>> {
    let options = OpenFromUrlOptions {
        storage: storage_options(credentials, anonymous, region, endpoint_url),
        config: config
            .map(|c| c.try_into().map_err(PyValueError::new_err))
            .transpose()?,
        authorize_virtual_chunk_access: authorize_virtual_chunk_access
            .unwrap_or_default()
            .into_iter()
            .map(|(prefix, cred)| (prefix, cred.map(Into::into)))
            .collect(),
    };
    let parsed = url_pipeline::parse_icechunk_url(root_url, icechunk_body, &options)
        .map_err(|e| url_error(&e))?;
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let (session, path) =
            url_pipeline::open_parsed(parsed, options).await.map_err(open_error)?;
        Ok((PySession(Arc::new(RwLock::new(session))), path))
    })
}
