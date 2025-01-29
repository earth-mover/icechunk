use std::{
    collections::{BTreeSet, HashMap, HashSet},
    sync::Arc,
};

use chrono::{DateTime, Utc};
use futures::{StreamExt, TryStreamExt};
use icechunk::{
    config::Credentials,
    format::{snapshot::SnapshotInfo, SnapshotId},
    ops::gc::{expire, garbage_collect, GCConfig, GCSummary},
    repository::{RepositoryError, VersionInfo},
    Repository,
};
use pyo3::{exceptions::PyValueError, prelude::*, types::PyType};
use tokio::sync::{Mutex, RwLock};

use crate::{
    config::{
        datetime_repr, format_option_string, PyCredentials, PyRepositoryConfig,
        PyStorage, PyStorageSettings,
    },
    errors::PyIcechunkStoreError,
    session::PySession,
    streams::PyAsyncGenerator,
};

#[pyclass(name = "SnapshotInfo", eq)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PySnapshotInfo {
    #[pyo3(get)]
    id: String,
    #[pyo3(get)]
    parent_id: Option<String>,
    #[pyo3(get)]
    written_at: DateTime<Utc>,
    #[pyo3(get)]
    message: String,
}

impl From<SnapshotInfo> for PySnapshotInfo {
    fn from(val: SnapshotInfo) -> Self {
        PySnapshotInfo {
            id: val.id.to_string(),
            parent_id: val.parent_id.map(|id| id.to_string()),
            written_at: val.flushed_at,
            message: val.message,
        }
    }
}

#[pymethods]
impl PySnapshotInfo {
    pub fn __repr__(&self) -> String {
        // TODO: escape
        format!(
            r#"SnapshotInfo(id="{id}", parent_id={parent}, written_at={at}, message="{message}")"#,
            id = self.id,
            parent = format_option_string(self.parent_id.as_ref()),
            at = datetime_repr(&self.written_at),
            message = self.message.chars().take(10).collect::<String>() + "...",
        )
    }
}

#[pyclass(name = "GCSummary", eq)]
#[derive(Debug, PartialEq, Eq, Default)]
pub struct PyGCSummary {
    #[pyo3(get)]
    pub chunks_deleted: usize,
    #[pyo3(get)]
    pub manifests_deleted: usize,
    #[pyo3(get)]
    pub snapshots_deleted: usize,
    #[pyo3(get)]
    pub attributes_deleted: usize,
    #[pyo3(get)]
    pub transaction_logs_deleted: usize,
}

impl From<GCSummary> for PyGCSummary {
    fn from(value: GCSummary) -> Self {
        Self {
            chunks_deleted: value.chunks_deleted,
            manifests_deleted: value.manifests_deleted,
            snapshots_deleted: value.snapshots_deleted,
            attributes_deleted: value.attributes_deleted,
            transaction_logs_deleted: value.transaction_logs_deleted,
        }
    }
}

#[pyclass]
pub struct PyRepository(Arc<Repository>);

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

            Ok(Self(Arc::new(repository)))
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

            Ok(Self(Arc::new(repository)))
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

            Ok(Self(Arc::new(repository)))
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
            Ok(Self(Arc::new(
                self.0
                    .reopen(
                        config.map(|c| c.into()),
                        virtual_chunk_credentials.map(map_credentials),
                    )
                    .map_err(PyIcechunkStoreError::RepositoryError)?,
            )))
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

    pub fn storage(&self) -> PyStorage {
        PyStorage(Arc::clone(self.0.storage()))
    }

    #[pyo3(signature = (*, branch = None, tag = None, snapshot = None))]
    pub fn ancestry(
        &self,
        py: Python<'_>,
        branch: Option<String>,
        tag: Option<String>,
        snapshot: Option<String>,
    ) -> PyResult<Vec<PySnapshotInfo>> {
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
                    .map_ok(Into::<PySnapshotInfo>::into)
                    .try_collect::<Vec<_>>()
                    .await
                    .map_err(PyIcechunkStoreError::RepositoryError)?;
                Ok(ancestry)
            })
        })
    }

    #[pyo3(signature = (*, branch = None, tag = None, snapshot = None))]
    pub fn async_ancestry(
        &self,
        py: Python<'_>,
        branch: Option<String>,
        tag: Option<String>,
        snapshot: Option<String>,
    ) -> PyResult<PyAsyncGenerator> {
        let repo = Arc::clone(&self.0);
        // This function calls block_on, so we need to allow other thread python to make progress
        py.allow_threads(move || {
            let version = args_to_version_info(branch, tag, snapshot)?;
            let ancestry = pyo3_async_runtimes::tokio::get_runtime()
                .block_on(async move { repo.ancestry_arc(&version).await })
                .map_err(PyIcechunkStoreError::RepositoryError)?
                .map_err(PyIcechunkStoreError::RepositoryError);

            let parents = ancestry.and_then(|info| async move {
                Python::with_gil(|py| {
                    let info = PySnapshotInfo::from(info);
                    Ok(Bound::new(py, info)?.into_any().unbind())
                })
            });

            let prepared_list = Arc::new(Mutex::new(parents.err_into().boxed()));
            Ok(PyAsyncGenerator::new(prepared_list))
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

    pub fn list_branches(&self, py: Python<'_>) -> PyResult<BTreeSet<String>> {
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

    pub fn delete_tag(&self, py: Python<'_>, tag: &str) -> PyResult<()> {
        // This function calls block_on, so we need to allow other thread python to make progress
        py.allow_threads(move || {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
                self.0
                    .delete_tag(tag)
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

    pub fn list_tags(&self, py: Python<'_>) -> PyResult<BTreeSet<String>> {
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

    pub fn expire_snapshots(
        &self,
        py: Python<'_>,
        older_than: DateTime<Utc>,
    ) -> PyResult<HashSet<String>> {
        // This function calls block_on, so we need to allow other thread python to make progress
        py.allow_threads(move || {
            let result =
                pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
                    let result = expire(
                        self.0.storage().as_ref(),
                        self.0.storage_settings(),
                        self.0.asset_manager().clone(),
                        older_than,
                    )
                    .await
                    .map_err(PyIcechunkStoreError::GCError)?;
                    Ok::<_, PyIcechunkStoreError>(
                        result.iter().map(|id| id.to_string()).collect(),
                    )
                })?;

            Ok(result)
        })
    }

    pub fn garbage_collect(
        &self,
        py: Python<'_>,
        delete_object_older_than: DateTime<Utc>,
    ) -> PyResult<PyGCSummary> {
        // This function calls block_on, so we need to allow other thread python to make progress
        py.allow_threads(move || {
            let result =
                pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
                    let gc_config = GCConfig::clean_all(
                        delete_object_older_than,
                        delete_object_older_than,
                        Default::default(),
                    );
                    let result = garbage_collect(
                        self.0.storage().as_ref(),
                        self.0.storage_settings(),
                        self.0.asset_manager().clone(),
                        &gc_config,
                    )
                    .await
                    .map_err(PyIcechunkStoreError::GCError)?;
                    Ok::<_, PyIcechunkStoreError>(result.into())
                })?;

            Ok(result)
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
