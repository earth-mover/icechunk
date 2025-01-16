use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use chrono::{DateTime, Utc};
use futures::TryStreamExt;
use icechunk::{
    config::Credentials,
    format::{snapshot::SnapshotMetadata, SnapshotId},
    repository::{RepositoryError, VersionInfo},
    Repository,
};
use pyo3::{exceptions::PyValueError, prelude::*, types::PyType};
use tokio::sync::RwLock;

use crate::{
    config::{
        datetime_repr, PyCredentials, PyRepositoryConfig, PyStorage, PyStorageSettings,
    },
    errors::PyIcechunkStoreError,
    session::PySession,
};

#[pyclass(name = "SnapshotMetadata", eq)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PySnapshotMetadata {
    #[pyo3(get)]
    id: String,
    #[pyo3(get)]
    written_at: DateTime<Utc>,
    #[pyo3(get)]
    message: String,
}

impl From<SnapshotMetadata> for PySnapshotMetadata {
    fn from(val: SnapshotMetadata) -> Self {
        PySnapshotMetadata {
            id: val.id.to_string(),
            written_at: val.written_at,
            message: val.message,
        }
    }
}

#[pymethods]
impl PySnapshotMetadata {
    pub fn __repr__(&self) -> String {
        // TODO: escape
        format!(
            r#"SnapshotMetadata(id="{id}",written_at={at},message="{message}")"#,
            id = self.id,
            at = datetime_repr(&self.written_at),
            message = self.message.chars().take(10).collect::<String>() + "...",
        )
    }
}

#[pyclass]
pub struct PyRepository(Repository);

#[pymethods]
/// Most functions in this class call `Runtime.block_on` so they need to `allow_threads` so other
/// python threads can make progress in the case of an actual block
impl PyRepository {
    #[classmethod]
    #[pyo3(signature = (storage, *, config = None, virtual_chunk_credentials = None))]
    fn create(
        _cls: &Bound<'_, PyType>,
        py: Python<'_>,
        storage: PyStorage,
        config: Option<&PyRepositoryConfig>,
        virtual_chunk_credentials: Option<HashMap<String, PyCredentials>>,
    ) -> PyResult<Self> {
        // This function calls block_on, so we need to allow other thread python to make progress
        py.allow_threads(move || {
            let repository =
                pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
                    Repository::create(
                        config.map(|c| c.into()),
                        storage.0,
                        map_credentials(virtual_chunk_credentials),
                    )
                    .await
                    .map_err(PyIcechunkStoreError::RepositoryError)
                })?;

            Ok(Self(repository))
        })
    }

    #[classmethod]
    #[pyo3(signature = (storage, *, config = None, virtual_chunk_credentials = None))]
    fn open(
        _cls: &Bound<'_, PyType>,
        py: Python<'_>,
        storage: PyStorage,
        config: Option<&PyRepositoryConfig>,
        virtual_chunk_credentials: Option<HashMap<String, PyCredentials>>,
    ) -> PyResult<Self> {
        // This function calls block_on, so we need to allow other thread python to make progress
        py.allow_threads(move || {
            let repository =
                pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
                    Repository::open(
                        config.map(|c| c.into()),
                        storage.0,
                        map_credentials(virtual_chunk_credentials),
                    )
                    .await
                    .map_err(PyIcechunkStoreError::RepositoryError)
                })?;

            Ok(Self(repository))
        })
    }

    #[classmethod]
    #[pyo3(signature = (storage, *, config = None, virtual_chunk_credentials = None))]
    fn open_or_create(
        _cls: &Bound<'_, PyType>,
        py: Python<'_>,
        storage: PyStorage,
        config: Option<&PyRepositoryConfig>,
        virtual_chunk_credentials: Option<HashMap<String, PyCredentials>>,
    ) -> PyResult<Self> {
        // This function calls block_on, so we need to allow other thread python to make progress
        py.allow_threads(move || {
            let repository =
                pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
                    Ok::<_, PyErr>(
                        Repository::open_or_create(
                            config.map(|c| c.into()),
                            storage.0,
                            map_credentials(virtual_chunk_credentials),
                        )
                        .await
                        .map_err(PyIcechunkStoreError::RepositoryError)?,
                    )
                })?;

            Ok(Self(repository))
        })
    }

    #[staticmethod]
    fn exists(py: Python<'_>, storage: PyStorage) -> PyResult<bool> {
        // This function calls block_on, so we need to allow other thread python to make progress
        py.allow_threads(move || {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
                let exists = Repository::exists(storage.0.as_ref())
                    .await
                    .map_err(PyIcechunkStoreError::RepositoryError)?;
                Ok(exists)
            })
        })
    }

    /// Reopen the repository changing its config and or virtual chunk credentials
    ///
    /// If config is None, it will use the same value as self
    /// If virtual_chunk_credentials is None, it will use the same value as self
    /// If virtual_chunk_credentials is Some(x), it will override with x
    #[pyo3(signature = (*, config = None, virtual_chunk_credentials = None::<Option<HashMap<String, PyCredentials>>>))]
    pub fn reopen(
        &self,
        py: Python<'_>,
        config: Option<&PyRepositoryConfig>,
        virtual_chunk_credentials: Option<Option<HashMap<String, PyCredentials>>>,
    ) -> PyResult<Self> {
        py.allow_threads(move || {
            Ok(Self(
                self.0
                    .reopen(
                        config.map(|c| c.into()),
                        virtual_chunk_credentials.map(map_credentials),
                    )
                    .map_err(PyIcechunkStoreError::RepositoryError)?,
            ))
        })
    }

    #[staticmethod]
    fn fetch_config(
        py: Python<'_>,
        storage: PyStorage,
    ) -> PyResult<Option<PyRepositoryConfig>> {
        // This function calls block_on, so we need to allow other thread python to make progress
        py.allow_threads(move || {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
                let res = Repository::fetch_config(storage.0.as_ref())
                    .await
                    .map_err(PyIcechunkStoreError::RepositoryError)?;
                Ok(res.map(|res| res.0.into()))
            })
        })
    }

    fn save_config(&self, py: Python<'_>) -> PyResult<()> {
        // This function calls block_on, so we need to allow other thread python to make progress
        py.allow_threads(move || {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
                let _etag = self
                    .0
                    .save_config()
                    .await
                    .map_err(PyIcechunkStoreError::RepositoryError)?;
                Ok(())
            })
        })
    }

    pub fn config(&self) -> PyRepositoryConfig {
        self.0.config().clone().into()
    }

    pub fn storage_settings(&self) -> PyStorageSettings {
        self.0.storage_settings().clone().into()
    }

    #[pyo3(signature = (*, branch = None, tag = None, snapshot = None))]
    pub fn ancestry(
        &self,
        py: Python<'_>,
        branch: Option<String>,
        tag: Option<String>,
        snapshot: Option<String>,
    ) -> PyResult<Vec<PySnapshotMetadata>> {
        // This function calls block_on, so we need to allow other thread python to make progress
        py.allow_threads(move || {
            let version = args_to_version_info(branch, tag, snapshot)?;

            // TODO: this holds everything in memory
            pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
                let ancestry = self
                    .0
                    .ancestry(&version)
                    .await
                    .map_err(PyIcechunkStoreError::RepositoryError)?
                    .map_ok(Into::<PySnapshotMetadata>::into)
                    .try_collect::<Vec<_>>()
                    .await
                    .map_err(PyIcechunkStoreError::RepositoryError)?;
                Ok(ancestry)
            })
        })
    }

    pub fn create_branch(
        &self,
        py: Python<'_>,
        branch_name: &str,
        snapshot_id: &str,
    ) -> PyResult<()> {
        // This function calls block_on, so we need to allow other thread python to make progress
        py.allow_threads(move || {
            let snapshot_id = SnapshotId::try_from(snapshot_id).map_err(|_| {
                PyIcechunkStoreError::RepositoryError(RepositoryError::InvalidSnapshotId(
                    snapshot_id.to_owned(),
                ))
            })?;

            pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
                self.0
                    .create_branch(branch_name, &snapshot_id)
                    .await
                    .map_err(PyIcechunkStoreError::RepositoryError)?;
                Ok(())
            })
        })
    }

    pub fn list_branches(&self, py: Python<'_>) -> PyResult<HashSet<String>> {
        // This function calls block_on, so we need to allow other thread python to make progress
        py.allow_threads(move || {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
                let branches = self
                    .0
                    .list_branches()
                    .await
                    .map_err(PyIcechunkStoreError::RepositoryError)?;
                Ok(branches)
            })
        })
    }

    pub fn lookup_branch(&self, py: Python<'_>, branch_name: &str) -> PyResult<String> {
        // This function calls block_on, so we need to allow other thread python to make progress
        py.allow_threads(move || {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
                let tip = self
                    .0
                    .lookup_branch(branch_name)
                    .await
                    .map_err(PyIcechunkStoreError::RepositoryError)?;
                Ok(tip.to_string())
            })
        })
    }

    pub fn reset_branch(
        &self,
        py: Python<'_>,
        branch_name: &str,
        snapshot_id: &str,
    ) -> PyResult<()> {
        // This function calls block_on, so we need to allow other thread python to make progress
        py.allow_threads(move || {
            let snapshot_id = SnapshotId::try_from(snapshot_id).map_err(|_| {
                PyIcechunkStoreError::RepositoryError(RepositoryError::InvalidSnapshotId(
                    snapshot_id.to_owned(),
                ))
            })?;

            pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
                self.0
                    .reset_branch(branch_name, &snapshot_id)
                    .await
                    .map_err(PyIcechunkStoreError::RepositoryError)?;
                Ok(())
            })
        })
    }

    pub fn delete_branch(&self, py: Python<'_>, branch: &str) -> PyResult<()> {
        // This function calls block_on, so we need to allow other thread python to make progress
        py.allow_threads(move || {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
                self.0
                    .delete_branch(branch)
                    .await
                    .map_err(PyIcechunkStoreError::RepositoryError)?;
                Ok(())
            })
        })
    }

    pub fn create_tag(
        &self,
        py: Python<'_>,
        tag_name: &str,
        snapshot_id: &str,
    ) -> PyResult<()> {
        // This function calls block_on, so we need to allow other thread python to make progress
        py.allow_threads(move || {
            let snapshot_id = SnapshotId::try_from(snapshot_id).map_err(|_| {
                PyIcechunkStoreError::RepositoryError(RepositoryError::InvalidSnapshotId(
                    snapshot_id.to_owned(),
                ))
            })?;

            pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
                self.0
                    .create_tag(tag_name, &snapshot_id)
                    .await
                    .map_err(PyIcechunkStoreError::RepositoryError)?;
                Ok(())
            })
        })
    }

    pub fn list_tags(&self, py: Python<'_>) -> PyResult<HashSet<String>> {
        // This function calls block_on, so we need to allow other thread python to make progress
        py.allow_threads(move || {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
                let tags = self
                    .0
                    .list_tags()
                    .await
                    .map_err(PyIcechunkStoreError::RepositoryError)?;
                Ok(tags)
            })
        })
    }

    pub fn lookup_tag(&self, py: Python<'_>, tag: &str) -> PyResult<String> {
        // This function calls block_on, so we need to allow other thread python to make progress
        py.allow_threads(move || {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
                let tag = self
                    .0
                    .lookup_tag(tag)
                    .await
                    .map_err(PyIcechunkStoreError::RepositoryError)?;
                Ok(tag.to_string())
            })
        })
    }

    #[pyo3(signature = (*, branch = None, tag = None, snapshot = None))]
    pub fn readonly_session(
        &self,
        py: Python<'_>,
        branch: Option<String>,
        tag: Option<String>,
        snapshot: Option<String>,
    ) -> PyResult<PySession> {
        // This function calls block_on, so we need to allow other thread python to make progress
        py.allow_threads(move || {
            let version = args_to_version_info(branch, tag, snapshot)?;
            let session =
                pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
                    self.0
                        .readonly_session(&version)
                        .await
                        .map_err(PyIcechunkStoreError::RepositoryError)
                })?;

            Ok(PySession(Arc::new(RwLock::new(session))))
        })
    }

    pub fn writable_session(&self, py: Python<'_>, branch: &str) -> PyResult<PySession> {
        // This function calls block_on, so we need to allow other thread python to make progress
        py.allow_threads(move || {
            let session =
                pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
                    self.0
                        .writable_session(branch)
                        .await
                        .map_err(PyIcechunkStoreError::RepositoryError)
                })?;

            Ok(PySession(Arc::new(RwLock::new(session))))
        })
    }
}

fn map_credentials(
    cred: Option<HashMap<String, PyCredentials>>,
) -> HashMap<String, Credentials> {
    cred.map(|cred| {
        cred.iter().map(|(name, cred)| (name.clone(), cred.clone().into())).collect()
    })
    .unwrap_or_default()
}

fn args_to_version_info(
    branch: Option<String>,
    tag: Option<String>,
    snapshot: Option<String>,
) -> PyResult<VersionInfo> {
    let n = [&branch, &tag, &snapshot].iter().filter(|r| !r.is_none()).count();
    if n > 1 {
        return Err(PyValueError::new_err(
            "Must provide one of branch_name, tag_name, or snapshot_id",
        ));
    }

    if let Some(branch_name) = branch {
        Ok(VersionInfo::BranchTipRef(branch_name))
    } else if let Some(tag_name) = tag {
        Ok(VersionInfo::TagRef(tag_name))
    } else if let Some(snapshot_id) = snapshot {
        let snapshot_id = SnapshotId::try_from(snapshot_id.as_str()).map_err(|_| {
            PyIcechunkStoreError::RepositoryError(RepositoryError::InvalidSnapshotId(
                snapshot_id.to_owned(),
            ))
        })?;

        Ok(VersionInfo::SnapshotId(snapshot_id))
    } else {
        return Err(PyValueError::new_err(
            "Must provide either branch_name, tag_name, or snapshot_id",
        ));
    }
}
